//! Minimal JSON-RPC helpers for chain RPC / bundler / paymaster.

use crate::errors::{Error, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;

type HyperClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Full<Bytes>,
>;

static HTTP: OnceCell<HyperClient> = OnceCell::const_new();

async fn http_client() -> Result<&'static HyperClient> {
    HTTP.get_or_try_init(|| async {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let mut roots = connectrpc::rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = connectrpc::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .build();
        Ok::<_, Error>(
            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .build(https),
        )
    })
    .await
}

/// JSON-RPC HTTP client (async; matches the SDK's tokio/hyper stack).
#[derive(Debug)]
pub struct JsonRpcClient {
    url: String,
    timeout: Duration,
    next_id: AtomicU64,
}

impl JsonRpcClient {
    pub fn new(url: impl Into<String>, timeout: Duration) -> Self {
        Self {
            url: url.into(),
            timeout,
            next_id: AtomicU64::new(0),
        }
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let body = serde_json::to_vec(&payload)
            .map_err(|e| Error::transport(format!("jsonrpc encode: {e}")))?;

        let uri: hyper::Uri = self
            .url
            .parse()
            .map_err(|e| Error::transport(format!("jsonrpc invalid url: {e}")))?;
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| Error::transport(format!("jsonrpc request build: {e}")))?;

        let client = http_client().await?;
        let resp = tokio::time::timeout(self.timeout, client.request(req))
            .await
            .map_err(|_| Error::transport(format!("jsonrpc timeout calling {method}")))?
            .map_err(|e| Error::transport(format!("jsonrpc HTTP request failed: {e}")))?;

        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::transport(format!("jsonrpc read body: {e}")))?
            .to_bytes();

        if !status.is_success() {
            return Err(Error::transport(format!(
                "jsonrpc HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }

        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::transport(format!("jsonrpc invalid JSON: {e}")))?;
        if let Some(err) = body.get("error").filter(|e| !e.is_null()) {
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("jsonrpc error");
            return Err(Error::transport(format!("jsonrpc {method}: {message}")));
        }
        Ok(body.get("result").cloned().unwrap_or(Value::Null))
    }
}
