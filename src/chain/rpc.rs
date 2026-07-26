//! Minimal JSON-RPC helpers for chain RPC / bundler / paymaster.

use crate::errors::{Error, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;

type HyperClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Full<Bytes>,
>;

/// Maximum accepted JSON-RPC HTTP response body.
pub const MAX_JSONRPC_RESPONSE_BYTES: usize = 1024 * 1024;

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
    next_id: Arc<AtomicU64>,
}

impl Clone for JsonRpcClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            timeout: self.timeout,
            next_id: self.next_id.clone(),
        }
    }
}

impl JsonRpcClient {
    pub fn new(url: impl Into<String>, timeout: Duration) -> Self {
        Self {
            url: url.into(),
            timeout,
            next_id: Arc::new(AtomicU64::new(0)),
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
        let timeout = self.timeout;
        let (status, bytes) = tokio::time::timeout(timeout, async {
            let resp = client
                .request(req)
                .await
                .map_err(|e| Error::transport(format!("jsonrpc HTTP request failed: {e}")))?;
            let status = resp.status();
            if content_length_exceeds_limit(resp.headers(), MAX_JSONRPC_RESPONSE_BYTES) {
                return Err(Error::transport(format!(
                    "jsonrpc response exceeds {MAX_JSONRPC_RESPONSE_BYTES} bytes"
                )));
            }
            let bytes = Limited::new(resp.into_body(), MAX_JSONRPC_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|e| Error::transport(format!("jsonrpc read body: {e}")))?
                .to_bytes();
            Ok::<_, Error>((status, bytes))
        })
        .await
        .map_err(|_| Error::transport(format!("jsonrpc timeout calling {method}")))??;

        if !status.is_success() {
            return Err(Error::transport(format!(
                "jsonrpc HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }

        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::transport(format!("jsonrpc invalid JSON: {e}")))?;
        parse_jsonrpc_result(&body, id, method)
    }
}

fn content_length_exceeds_limit(headers: &http::HeaderMap, max_bytes: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max_bytes)
}

/// Validate a JSON-RPC 2.0 success/error envelope and extract `result`.
pub fn parse_jsonrpc_result(body: &Value, expected_id: u64, method: &str) -> Result<Value> {
    let obj = body
        .as_object()
        .ok_or_else(|| Error::transport("jsonrpc response must be a JSON object".to_owned()))?;

    match obj.get("jsonrpc").and_then(|v| v.as_str()) {
        Some("2.0") => {}
        Some(other) => {
            return Err(Error::transport(format!(
                "jsonrpc unsupported version: {other}"
            )));
        }
        None => {
            return Err(Error::transport(
                "jsonrpc response missing jsonrpc version".to_owned(),
            ));
        }
    }

    let id_ok = match obj.get("id") {
        Some(Value::Number(n)) => n.as_u64() == Some(expected_id),
        Some(Value::String(s)) => s.parse::<u64>().ok() == Some(expected_id),
        _ => false,
    };
    if !id_ok {
        return Err(Error::transport(format!(
            "jsonrpc response id mismatch (expected {expected_id})"
        )));
    }

    let has_result = obj.contains_key("result");
    let error = obj.get("error").filter(|e| !e.is_null());
    match (has_result, error) {
        (true, None) => Ok(obj.get("result").cloned().unwrap_or(Value::Null)),
        (false, Some(err)) => {
            if !err.is_object() {
                return Err(Error::transport(format!(
                    "jsonrpc {method}: error must be an object"
                )));
            }
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("jsonrpc error");
            Err(Error::transport(format!("jsonrpc {method}: {message}")))
        }
        (true, Some(_)) => Err(Error::transport(format!(
            "jsonrpc {method}: response must not include both result and error"
        ))),
        (false, None) => Err(Error::transport(format!(
            "jsonrpc {method}: response must include result or error"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_rejects_both_result_and_error() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": 1,
            "error": {"code": -1, "message": "nope"},
        });
        let err = parse_jsonrpc_result(&body, 1, "eth_call").unwrap_err();
        assert!(err.to_string().contains("both result and error"));
    }

    #[test]
    fn envelope_rejects_neither_result_nor_error() {
        let body = json!({"jsonrpc": "2.0", "id": 1});
        assert!(parse_jsonrpc_result(&body, 1, "eth_call").is_err());
    }

    #[test]
    fn envelope_rejects_id_mismatch() {
        let body = json!({"jsonrpc": "2.0", "id": 9, "result": true});
        assert!(parse_jsonrpc_result(&body, 1, "eth_call").is_err());
    }

    #[test]
    fn envelope_accepts_null_result() {
        let body = json!({"jsonrpc": "2.0", "id": 1, "result": null});
        assert_eq!(
            parse_jsonrpc_result(&body, 1, "eth_call").unwrap(),
            Value::Null
        );
    }
}
