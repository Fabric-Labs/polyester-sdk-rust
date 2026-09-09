//! HTTP/Connect transport factory and API-key request signing.

use crate::auth::{self, Credentials};
use crate::errors::{Error, Result, map_connect_error};
use crate::user_agent::user_agent;
use buffa::Message;
use connectrpc::ConnectError;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::rustls;
use http::{HeaderValue, Uri, header::USER_AGENT};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_API_URL: &str = "https://api-devnet.polyester.ai";
pub const DEFAULT_WS_URL: &str = "wss://api-devnet.polyester.ai";
/// Maximum decompressed ConnectRPC response message accepted by the SDK.
///
/// This is set explicitly instead of relying on the transport dependency's
/// default so catalog and other unary responses remain allocation-bounded
/// across dependency upgrades.
pub const MAX_CONNECT_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Wire encoding for Connect unary calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireFormat {
    #[default]
    Binary,
    Json,
}

impl WireFormat {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            _ => Self::Binary,
        }
    }
}

/// Transport configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub api_url: String,
    pub ws_url: String,
    pub timeout: Duration,
    pub wire_format: WireFormat,
    /// Allow non-loopback `http://` / `ws://` endpoints. Loopback plaintext
    /// stays allowed without this flag.
    pub allow_insecure_http: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.to_owned(),
            ws_url: DEFAULT_WS_URL.to_owned(),
            timeout: Duration::from_secs(10),
            wire_format: WireFormat::Binary,
            allow_insecure_http: false,
        }
    }
}

/// Environment variable that opts `Client::from_env` into remote plaintext.
pub const ALLOW_INSECURE_HTTP_ENV: &str = "POLYESTER_ALLOW_INSECURE_HTTP";

/// True when `POLYESTER_ALLOW_INSECURE_HTTP` is `1`, `true`, or `yes`.
pub fn env_allow_insecure_http() -> bool {
    match std::env::var(ALLOW_INSECURE_HTTP_ENV) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        ),
        Err(_) => false,
    }
}

/// Whether `url` targets localhost / 127.0.0.0/8 / ::1.
pub fn host_is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => {
            let host = host.trim_end_matches('.').to_ascii_lowercase();
            host == "localhost"
        }
        Some(url::Host::Ipv4(addr)) => addr.is_loopback(),
        Some(url::Host::Ipv6(addr)) => addr.is_loopback(),
        None => false,
    }
}

fn redact_url_for_log(url: &url::Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.to_string()
}

fn accept_plaintext(url: &url::Url, allow_insecure: bool, kind: &str) -> Result<()> {
    if allow_insecure || host_is_loopback(url) {
        tracing::warn!(
            url = %redact_url_for_log(url),
            "{kind} is plaintext; credentials or tokens may travel in the clear"
        );
        return Ok(());
    }
    Err(Error::validation(format!(
        "{kind} must use a TLS scheme unless the host is loopback or allow_insecure_http is set"
    )))
}

/// Validate an HTTP(S) endpoint. Remote `http://` requires `allow_insecure`.
pub fn validate_http_url(raw: &str, allow_insecure: bool) -> Result<url::Url> {
    let parsed =
        url::Url::parse(raw).map_err(|e| Error::validation(format!("invalid URL: {e}")))?;
    match parsed.scheme() {
        "https" => Ok(parsed),
        "http" => {
            accept_plaintext(&parsed, allow_insecure, "http URL")?;
            Ok(parsed)
        }
        other => Err(Error::validation(format!(
            "URL must start with http:// or https://, got {other}"
        ))),
    }
}

/// Validate a WebSocket endpoint. Remote `ws://` requires `allow_insecure`.
pub fn validate_ws_url(raw: &str, allow_insecure: bool) -> Result<url::Url> {
    let parsed =
        url::Url::parse(raw).map_err(|e| Error::validation(format!("invalid ws_url: {e}")))?;
    match parsed.scheme() {
        "wss" => Ok(parsed),
        "ws" => {
            accept_plaintext(&parsed, allow_insecure, "ws_url")?;
            Ok(parsed)
        }
        other => Err(Error::validation(format!(
            "ws_url must start with ws:// or wss://, got {other}"
        ))),
    }
}

/// Shared transport handle used by all generated Connect clients.
pub type SharedTransport = HttpClient;

/// Owns HTTP client, Connect config, and optional credentials.
#[derive(Clone)]
pub struct Factory {
    pub config: Config,
    pub credentials: Option<Credentials>,
    transport: SharedTransport,
    connect_config: ClientConfig,
}

impl Factory {
    pub fn new(config: Config, credentials: Option<Credentials>) -> Result<Self> {
        let parsed = validate_http_url(&config.api_url, config.allow_insecure_http)?;
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::validation(
                "api_url must not contain a query string or fragment",
            ));
        }
        validate_ws_url(&config.ws_url, config.allow_insecure_http)?;
        let uri: Uri = config
            .api_url
            .parse()
            .map_err(|e| Error::validation(format!("invalid api_url: {e}")))?;

        let transport = build_http_client(&config.api_url)?;

        let ua = HeaderValue::from_str(&user_agent())
            .map_err(|e| Error::validation(format!("invalid User-Agent header value: {e}")))?;
        let mut connect_config = ClientConfig::new(uri)
            .with_default_timeout(config.timeout)
            .with_default_max_message_size(MAX_CONNECT_RESPONSE_BYTES)
            .with_default_header(USER_AGENT, ua);

        if config.wire_format == WireFormat::Json {
            connect_config = connect_config.json();
        }

        Ok(Self {
            config,
            credentials,
            transport,
            connect_config,
        })
    }

    pub(crate) fn transport(&self) -> SharedTransport {
        self.transport.clone()
    }

    pub(crate) fn connect_config(&self) -> ClientConfig {
        self.connect_config.clone()
    }

    pub fn require_credentials(&self) -> Result<&Credentials> {
        self.credentials
            .as_ref()
            .ok_or_else(|| Error::auth("This endpoint requires Polyester API-key credentials"))
    }

    pub fn map_error(err: ConnectError) -> Error {
        map_connect_error(err)
    }

    /// Build `CallOptions` with API-key signatures over the exact bytes that
    /// Connect will send for the configured wire format.
    pub fn sign_options<M: Message + Serialize>(
        &self,
        procedure: &str,
        request: &M,
    ) -> Result<CallOptions> {
        let creds = self.require_credentials()?;
        let body = match self.config.wire_format {
            WireFormat::Binary => request.encode_to_bytes(),
            WireFormat::Json => connectrpc::JsonCodec::encode(request).map_err(Self::map_error)?,
        };
        let sign_url = auth::request_url(&self.config.api_url, procedure);
        let headers = creds.sign_request("POST", &sign_url, &body, None)?;
        let mut opts = CallOptions::default().with_header(USER_AGENT, user_agent());
        for (k, v) in headers {
            opts = opts.with_header(k, v);
        }
        Ok(opts)
    }

    /// Async variant used by SDK network calls so timestamp-capacity
    /// backpressure never blocks a Tokio worker thread.
    pub async fn sign_options_async<M: Message + Serialize>(
        &self,
        procedure: &str,
        request: &M,
    ) -> Result<CallOptions> {
        let creds = self.require_credentials()?;
        let body = match self.config.wire_format {
            WireFormat::Binary => request.encode_to_bytes(),
            WireFormat::Json => connectrpc::JsonCodec::encode(request).map_err(Self::map_error)?,
        };
        let sign_url = auth::request_url(&self.config.api_url, procedure);
        let headers = creds
            .sign_request_async("POST", &sign_url, &body, None)
            .await?;
        let mut opts = CallOptions::default().with_header(USER_AGENT, user_agent());
        for (k, v) in headers {
            opts = opts.with_header(k, v);
        }
        Ok(opts)
    }
}

fn build_http_client(api_url: &str) -> Result<HttpClient> {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    if api_url.starts_with("https://") {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        Ok(HttpClient::with_tls(tls))
    } else if api_url.starts_with("http://") {
        Ok(HttpClient::plaintext())
    } else {
        Err(Error::validation(
            "api_url must start with http:// or https://",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_agent::user_agent;

    #[test]
    fn connect_config_sets_polyester_user_agent() {
        let factory = Factory::new(
            Config {
                api_url: "http://127.0.0.1:9".into(),
                ..Default::default()
            },
            None,
        )
        .expect("factory");
        let config = factory.connect_config();
        let ua = config
            .default_headers()
            .get(USER_AGENT)
            .expect("User-Agent default header")
            .to_str()
            .expect("ascii");
        assert_eq!(ua, user_agent());
    }

    #[test]
    fn poly_4693_api_base_rejects_query_and_fragment_but_allows_local_http() {
        for api_url in [
            "https://api.example.test?x=1",
            "https://api.example.test/base?x=1",
            "https://api.example.test#fragment",
            "https://api.example.test/base#fragment",
        ] {
            let err = match Factory::new(
                Config {
                    api_url: api_url.into(),
                    ..Default::default()
                },
                None,
            ) {
                Ok(_) => panic!("query/fragment API base must fail before Connect joining"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::Validation(_)), "{api_url}: {err}");
        }

        Factory::new(
            Config {
                api_url: "http://127.0.0.1:9".into(),
                ..Default::default()
            },
            None,
        )
        .expect("localhost HTTP remains supported");
    }

    #[test]
    fn factory_rejects_remote_plaintext_unless_opted_in() {
        let err = match Factory::new(
            Config {
                api_url: "http://api.example.test".into(),
                ..Default::default()
            },
            None,
        ) {
            Ok(_) => panic!("remote http must fail"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::Validation(_)), "{err}");

        let err = match Factory::new(
            Config {
                api_url: "https://api.example.test".into(),
                ws_url: "ws://api.example.test".into(),
                ..Default::default()
            },
            None,
        ) {
            Ok(_) => panic!("remote ws must fail"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::Validation(_)), "{err}");

        Factory::new(
            Config {
                api_url: "http://api.example.test".into(),
                ws_url: "ws://api.example.test".into(),
                allow_insecure_http: true,
                ..Default::default()
            },
            None,
        )
        .expect("explicit insecure opt-in");
    }
}
