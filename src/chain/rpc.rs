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

static HTTPS: OnceCell<HyperClient> = OnceCell::const_new();
static HTTP_OR_HTTPS: OnceCell<HyperClient> = OnceCell::const_new();

async fn http_client(allow_http: bool) -> Result<&'static HyperClient> {
    let cell = if allow_http { &HTTP_OR_HTTPS } else { &HTTPS };
    cell.get_or_try_init(|| async move {
        let tls = (*crate::tls::client_config()?).clone();
        let builder = hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(tls);
        let https = if allow_http {
            builder.https_or_http().enable_http1().build()
        } else {
            builder.https_only().enable_http1().build()
        };
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
    pub fn new(url: impl Into<String>, timeout: Duration) -> Result<Self> {
        Self::new_with_policy(url, timeout, false)
    }

    /// Same as [`Self::new`] but permits a remote `http://` endpoint.
    pub fn new_insecure(url: impl Into<String>, timeout: Duration) -> Result<Self> {
        Self::new_with_policy(url, timeout, true)
    }

    fn new_with_policy(
        url: impl Into<String>,
        timeout: Duration,
        allow_insecure: bool,
    ) -> Result<Self> {
        let url = url.into();
        crate::transport::validate_http_url(&url, allow_insecure)?;
        Ok(Self {
            url,
            timeout,
            next_id: Arc::new(AtomicU64::new(0)),
        })
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

        let allow_http = self.url.starts_with("http://");
        let client = http_client(allow_http).await?;
        let timeout = self.timeout;
        // A failed answer's metadata, read from its headers as soon as they
        // arrive: every error from that answer carries it, one its body
        // causes (cut short, too large, or still arriving at the deadline)
        // included. The client never follows a redirect: it is a failed
        // answer too.
        let mut failed_answer = None;
        let (status, bytes, answer) = tokio::time::timeout(timeout, async {
            let resp = client
                .request(req)
                .await
                .map_err(|e| Error::transport(format!("jsonrpc HTTP request failed: {e}")))?;
            let status = resp.status();
            let answer = crate::errors::answer_from_headers(status.as_u16(), resp.headers());
            if !status.is_success() {
                failed_answer = Some(answer.clone());
            }
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
            Ok::<_, Error>((status, bytes, answer))
        })
        .await
        .unwrap_or_else(|_| {
            Err(Error::transport(format!(
                "jsonrpc timeout calling {method}"
            )))
        })
        .map_err(|err| match failed_answer.take() {
            Some(answer) => err.with_answer(answer),
            None => err,
        })?;

        if !status.is_success() {
            // The endpoint's failed answer: its status and delay ride along.
            return Err(Error::transport(format!(
                "jsonrpc HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            ))
            .with_answer(answer));
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
            // Only a well-formed error object, an integer code and a string
            // message, says what happened; any other proves nothing.
            match (
                err.get("code").and_then(Value::as_i64),
                err.get("message").and_then(Value::as_str),
            ) {
                (Some(code), Some(message)) => Err(Error::JsonRpc {
                    method: method.to_owned(),
                    code,
                    message: message.to_owned(),
                }),
                (_, message) => Err(Error::transport(format!(
                    "jsonrpc {method}: malformed error object (want an integer code and a string message): {}",
                    message.unwrap_or("no message")
                ))),
            }
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

    /// A JSON-RPC endpoint's failed answer keeps its status and Retry-After.
    #[tokio::test]
    async fn failed_answer_carries_its_metadata() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 16 * 1024];
                let _ = stream.read(&mut buf).await;
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 503 Busy\r\nRetry-After: 30\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbusy",
                    )
                    .await;
            }
        });
        let client =
            JsonRpcClient::new_insecure(format!("http://{addr}"), Duration::from_secs(5)).unwrap();
        let err = client.request("eth_chainId", json!([])).await.unwrap_err();
        assert!(err.to_string().contains("jsonrpc HTTP 503"), "{err:?}");
        assert_eq!(
            err.answer()
                .map(|answer| (answer.status, answer.retry_after)),
            Some((503, Some(30.0))),
            "{err:?}"
        );
    }

    /// A failed answer's metadata is read from its headers before its body: a
    /// body cut short, too large or still arriving at the deadline, and a
    /// redirect the client never follows (a valid or a malformed `Location`),
    /// all leave an error carrying the answer's status and Retry-After, after
    /// one request. A refused connection carries no answer.
    #[tokio::test]
    async fn body_failures_and_redirects_carry_their_answer() {
        use crate::answer_test_server::{
            Answer, AnswerBody, Expected, MALFORMED_LOCATION, answering, counting_target,
        };
        // A JSON-RPC call: a POST of JSON to the URL's path.
        let call = Expected {
            method: "POST",
            path: "/",
            header: "content-type",
            value: Some("application/json"),
        };
        // Any request a followed redirect makes counts there.
        let (target, target_hits) = counting_target().await;
        let too_large = MAX_JSONRPC_RESPONSE_BYTES + 1;
        let cases = [
            ("truncated", Answer::unavailable(AnswerBody::Truncated)),
            (
                "oversized",
                Answer::unavailable(AnswerBody::Sized(too_large)),
            ),
            (
                "oversized-chunked",
                Answer::unavailable(AnswerBody::Chunked(too_large)),
            ),
            ("body-deadline", Answer::unavailable(AnswerBody::Stalled)),
            ("redirect", Answer::redirect(target)),
            (
                "redirect-malformed",
                Answer::redirect(MALFORMED_LOCATION.to_owned()),
            ),
        ];
        for (name, answer) in cases {
            let status = answer.status;
            let (host, requests) = answering(answer, false, call).await;
            let client =
                JsonRpcClient::new_insecure(format!("http://{host}"), Duration::from_millis(400))
                    .unwrap();
            let started = std::time::Instant::now();
            let err = client
                .request("eth_chainId", json!([]))
                .await
                .expect_err(name);
            assert!(started.elapsed() < Duration::from_secs(3), "{name}");
            assert_eq!(requests.load(Ordering::SeqCst), 1, "{name}: {err:?}");
            assert_eq!(
                err.answer()
                    .map(|answer| (answer.status, answer.retry_after)),
                Some((status, Some(30.0))),
                "{name}: {err:?}"
            );
        }
        assert_eq!(
            target_hits.load(Ordering::SeqCst),
            0,
            "a redirect was followed"
        );
        // Port 1 on the loopback refuses the connection: no answer at all.
        let client =
            JsonRpcClient::new_insecure("http://127.0.0.1:1", Duration::from_millis(400)).unwrap();
        let err = client
            .request("eth_chainId", json!([]))
            .await
            .expect_err("refused");
        assert!(err.answer().is_none(), "{err:?}");
    }

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

    #[test]
    fn new_rejects_remote_plaintext() {
        let err = JsonRpcClient::new("http://rpc.example.test", Duration::from_secs(1))
            .expect_err("remote http");
        assert!(matches!(err, Error::Validation(_)), "{err}");
        JsonRpcClient::new("http://127.0.0.1:8545", Duration::from_secs(1)).expect("loopback");
        JsonRpcClient::new("https://rpc.example.test", Duration::from_secs(1)).expect("https");
        JsonRpcClient::new_insecure("http://rpc.example.test", Duration::from_secs(1))
            .expect("opt-in");
    }
}
