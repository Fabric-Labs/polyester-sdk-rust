//! HTTP/Connect transport factory and API-key request signing.

use crate::auth::{self, Credentials};
use crate::errors::{Error, Result, map_connect_error};
use crate::user_agent::user_agent;
use buffa::Message;
use bytes::Bytes;
use connectrpc::client::{
    BoxFuture, CallOptions, ClientBody, ClientConfig, ClientTransport, HttpClient,
};
use connectrpc::rustls;
use connectrpc::{ConnectError, ErrorCode, ErrorDetail};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE, USER_AGENT};
use http::{HeaderValue, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Either, Full, Limited};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_API_URL: &str = "https://api.devnet.polyester.com";
pub const DEFAULT_WS_URL: &str = "wss://api.devnet.polyester.com";
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
pub type SharedTransport = GatewayTransport;

/// Type URL of the SDK-local Connect error detail that marks an API-gateway
/// rejection. It never travels on the wire; [`GatewayTransport`] attaches it
/// and [`crate::errors::map_connect_error`] reads it back.
pub(crate) const GATEWAY_REJECTION_TYPE: &str = "polyester.sdk.GatewayRejection";

/// Type URL of the SDK-local Connect error detail that marks a bare HTTP 404
/// or 501: a failed response whose body is not a Connect error. The error's
/// HTTP status is the response's.
pub(crate) const BARE_HTTP_ERROR_TYPE: &str = "polyester.sdk.BareHttpError";

/// Longest body excerpt kept in a bare HTTP error message.
const BARE_HTTP_ERROR_EXCERPT_CHARS: usize = 256;

/// [`HttpClient`] that keeps what Connect would lose from a failed response
/// that is not a Connect error.
///
/// - The API gateway rejects API-key requests before any service sees them,
///   with a plain JSON (or problem JSON) body such as
///   `401 {"code":"TIMESTAMP_SKEW"}`. Connect would keep only the HTTP status
///   and drop the code. This transport reports the code, so the rejection
///   maps to [`Error::TimestampSkew`] or [`Error::ApiKeyRejected`].
/// - A bare HTTP 404 or 501 (any body that is not a Connect error) keeps its
///   real status, so it maps to [`Error::RouteNotFound`] or
///   [`Error::NotImplemented`] rather than Connect's guess from the status.
#[derive(Clone)]
pub struct GatewayTransport {
    inner: HttpClient,
}

type GatewayResponseBody = Either<Incoming, Full<Bytes>>;

impl ClientTransport for GatewayTransport {
    type ResponseBody = GatewayResponseBody;
    type Error = ConnectError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, std::result::Result<Response<Self::ResponseBody>, ConnectError>> {
        let response = self.inner.send(request);
        Box::pin(async move {
            let response = response.await?;
            let Some(is_json) = inspected_failure(&response) else {
                return Ok(response.map(Either::Left));
            };
            let (parts, body) = response.into_parts();
            let body = Limited::new(body, MAX_CONNECT_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|e| ConnectError::unavailable(format!("read error response: {e}")))?
                .to_bytes();
            if let Some(err) = classify_failed_response(parts.status, is_json, &body) {
                return Err(err);
            }
            Ok(Response::from_parts(parts, Either::Right(Full::new(body))))
        })
    }
}

/// For a failed, uncompressed response that may need classifying, whether
/// its body is JSON; `None` for everything else, which streams through
/// untouched. Gateway rejections are JSON; bare 404 and 501 have any body.
fn inspected_failure<B>(response: &Response<B>) -> Option<bool> {
    let status = response.status();
    if status.is_success() || response.headers().contains_key(CONTENT_ENCODING) {
        return None;
    }
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_ascii_lowercase)
        .is_some_and(|value| {
            value.starts_with("application/json") || value.starts_with("application/problem+json")
        });
    (is_json || status == StatusCode::NOT_FOUND || status == StatusCode::NOT_IMPLEMENTED)
        .then_some(is_json)
}

/// The Connect error for a failed response that Connect would misreport, or
/// `None` for a Connect error body or any other failure, which Connect then
/// maps as usual.
fn classify_failed_response(
    status: StatusCode,
    is_json: bool,
    body: &[u8],
) -> Option<ConnectError> {
    #[derive(Deserialize)]
    struct Body {
        code: Option<String>,
    }
    let code = is_json
        .then(|| serde_json::from_slice::<Body>(body).ok())
        .flatten()
        .and_then(|body| body.code)
        .filter(|code| !code.is_empty());
    if let Some(code) = code {
        // Connect error codes are lowercase names such as `unimplemented`.
        if code.parse::<ErrorCode>().is_ok() {
            return None;
        }
        if let Some(err) = gateway_rejection(status, code) {
            return Some(err);
        }
    }
    if status != StatusCode::NOT_FOUND && status != StatusCode::NOT_IMPLEMENTED {
        return None;
    }
    let text = String::from_utf8_lossy(body);
    let excerpt: String = text
        .trim()
        .chars()
        .take(BARE_HTTP_ERROR_EXCERPT_CHARS)
        .collect();
    let message = if excerpt.is_empty() {
        format!("HTTP {}", status.as_u16())
    } else {
        format!("HTTP {}: {excerpt}", status.as_u16())
    };
    Some(sdk_marked_error(
        ErrorCode::Unimplemented,
        message,
        status,
        BARE_HTTP_ERROR_TYPE,
    ))
}

/// The Connect error for an API-gateway rejection `code`, or `None` when the
/// status is not one the gateway uses for it.
fn gateway_rejection(status: StatusCode, code: String) -> Option<ConnectError> {
    let connect_code = if code == crate::errors::auth_codes::TIMESTAMP_SKEW {
        ErrorCode::Unauthenticated
    } else {
        match status {
            StatusCode::UNAUTHORIZED => ErrorCode::Unauthenticated,
            StatusCode::FORBIDDEN => ErrorCode::PermissionDenied,
            _ => return None,
        }
    };
    Some(sdk_marked_error(
        connect_code,
        code,
        status,
        GATEWAY_REJECTION_TYPE,
    ))
}

fn sdk_marked_error(
    code: ErrorCode,
    message: String,
    status: StatusCode,
    marker: &str,
) -> ConnectError {
    ConnectError::new(code, message)
        .with_http_status(status)
        .with_detail(ErrorDetail {
            type_url: marker.to_owned(),
            value: None,
            debug: None,
        })
}

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

fn build_http_client(api_url: &str) -> Result<GatewayTransport> {
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
        Ok(GatewayTransport {
            inner: HttpClient::with_tls(tls),
        })
    } else if api_url.starts_with("http://") {
        Ok(GatewayTransport {
            inner: HttpClient::plaintext(),
        })
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
    fn failed_responses_keep_their_status_and_gateway_code() {
        let classify = |status: u16, is_json: bool, body: &str| {
            classify_failed_response(
                StatusCode::from_u16(status).unwrap(),
                is_json,
                body.as_bytes(),
            )
        };
        let marker = |err: &ConnectError| {
            err.details
                .iter()
                .map(|d| d.type_url.clone())
                .collect::<Vec<_>>()
        };

        // Connect error bodies are left to Connect, with or without a message.
        assert!(classify(501, true, r#"{"code":"unimplemented"}"#).is_none());
        assert!(classify(404, true, r#"{"code":"not_found","message":"x"}"#).is_none());
        // Other failures Connect maps correctly by status are left alone.
        assert!(classify(500, true, "{}").is_none());
        assert!(classify(503, true, r#"{"code":"MAINTENANCE"}"#).is_none());

        for (status, is_json, body, message) in [
            (404, false, "", "HTTP 404"),
            (404, true, "{}", "HTTP 404: {}"),
            (501, false, "Not Implemented", "HTTP 501: Not Implemented"),
            (
                501,
                true,
                r#"{"message":"Not Implemented"}"#,
                r#"HTTP 501: {"message":"Not Implemented"}"#,
            ),
        ] {
            let err = classify(status, is_json, body).expect("bare HTTP error");
            assert_eq!(err.code, ErrorCode::Unimplemented);
            assert_eq!(err.http_status().as_u16(), status);
            assert_eq!(err.message.as_deref(), Some(message));
            assert_eq!(marker(&err), [BARE_HTTP_ERROR_TYPE]);
        }

        let skew = classify(401, true, r#"{"code":"TIMESTAMP_SKEW"}"#).unwrap();
        assert_eq!(
            (
                skew.code,
                skew.http_status().as_u16(),
                skew.message.as_deref()
            ),
            (ErrorCode::Unauthenticated, 401, Some("TIMESTAMP_SKEW"))
        );
        assert_eq!(marker(&skew), [GATEWAY_REJECTION_TYPE]);
        let denied = classify(403, true, r#"{"code":"IP_NOT_ALLOWED"}"#).unwrap();
        assert_eq!(denied.code, ErrorCode::PermissionDenied);
        // A JSON body is only read for its code when it is JSON.
        assert!(classify(401, false, r#"{"code":"TIMESTAMP_SKEW"}"#).is_none());
    }

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
