//! HTTP/Connect transport factory and API-key request signing.

use crate::auth::{self, Credentials};
use crate::errors::{Error, Result, map_connect_error};
use buffa::Message;
use connectrpc::ConnectError;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::rustls;
use http::Uri;
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.to_owned(),
            ws_url: DEFAULT_WS_URL.to_owned(),
            timeout: Duration::from_secs(10),
            wire_format: WireFormat::Binary,
        }
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
        let uri: Uri = config
            .api_url
            .parse()
            .map_err(|e| Error::validation(format!("invalid api_url: {e}")))?;

        let transport = build_http_client(&config.api_url)?;

        let mut connect_config = ClientConfig::new(uri)
            .with_default_timeout(config.timeout)
            .with_default_max_message_size(MAX_CONNECT_RESPONSE_BYTES);

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
        let mut opts = CallOptions::default();
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
        let mut opts = CallOptions::default();
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
