//! Signed HTTP token exchange for private Centrifugo channels.

use crate::auth::{Credentials, encode_query_component};
use crate::errors::{Error, Result};
use crate::user_agent::{cloudflare_1010_message, is_cloudflare_browser_ban, user_agent};
use http_body_util::{BodyExt, Empty, Limited};
use hyper::header::{CONTENT_LENGTH, HeaderName, HeaderValue, USER_AGENT};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use std::time::Duration;

type HyperClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Empty<bytes::Bytes>,
>;

/// Default when callers do not pass an SDK config timeout.
pub const DEFAULT_TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TOKEN_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ERROR_BODY_CHARS: usize = 256;

pub fn connection_token_url(api_url: &str) -> String {
    format!("{}/v1/rt/token", api_url.trim_end_matches('/'))
}

pub fn subscription_token_url(api_url: &str, channel: &str) -> String {
    // Must use the same encoder as API-key canonical_query so the signed query
    // string matches the request URL (preserve RFC 3986 unreserved chars).
    format!(
        "{}/v1/rt/subscribe?channel={}",
        api_url.trim_end_matches('/'),
        encode_query_component(channel)
    )
}

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

pub async fn fetch_connection_token(
    creds: &Credentials,
    api_url: &str,
    timeout: Duration,
) -> Result<String> {
    let url = connection_token_url(api_url);
    fetch_rt_token(creds, &url, "realtime connection token", timeout).await
}

pub async fn fetch_subscription_token(
    creds: &Credentials,
    api_url: &str,
    channel: &str,
    timeout: Duration,
) -> Result<String> {
    let url = subscription_token_url(api_url, channel);
    fetch_rt_token(
        creds,
        &url,
        &format!("realtime subscription token for {channel}"),
        timeout,
    )
    .await
}

async fn fetch_rt_token(
    creds: &Credentials,
    url: &str,
    label: &str,
    timeout: Duration,
) -> Result<String> {
    let timeout = if timeout.is_zero() {
        DEFAULT_TOKEN_REQUEST_TIMEOUT
    } else {
        timeout
    };
    let client = build_http_client()?;
    let headers = creds.sign_request_async("GET", url, b"", None).await?;
    let uri: hyper::Uri = url
        .parse()
        .map_err(|e| Error::realtime(format!("{label}: invalid url: {e}")))?;
    let mut req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Empty::<bytes::Bytes>::new())
        .map_err(|e| Error::realtime(format!("{label}: request build: {e}")))?;
    let ua = HeaderValue::from_str(&user_agent())
        .map_err(|e| Error::realtime(format!("{label}: User-Agent: {e}")))?;
    req.headers_mut().insert(USER_AGENT, ua);
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::realtime(format!("{label}: header name: {e}")))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|e| Error::realtime(format!("{label}: header value: {e}")))?;
        req.headers_mut().insert(name, value);
    }

    // One deadline covers headers + bounded body collect (F-18).
    let outcome = tokio::time::timeout(timeout, async {
        let resp = client
            .request(req)
            .await
            .map_err(|e| Error::realtime(format!("{label}: HTTP request failed: {e}")))?;
        let status = resp.status();
        if content_length_exceeds_limit(resp.headers(), MAX_TOKEN_RESPONSE_BYTES) {
            return Err(Error::realtime(format!(
                "{label}: response exceeds {MAX_TOKEN_RESPONSE_BYTES} bytes"
            )));
        }
        let body = Limited::new(resp.into_body(), MAX_TOKEN_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| Error::realtime(format!("{label}: read bounded body: {e}")))?
            .to_bytes();
        Ok::<_, Error>((status, body))
    })
    .await
    .map_err(|_| Error::realtime(format!("{label}: HTTP request timed out after {timeout:?}")))??;

    let (status, body) = outcome;
    let status_code = status.as_u16();
    if status_code == 401 {
        return Err(Error::auth(format!(
            "{label}: authentication failed (HTTP 401)"
        )));
    }
    if status_code == 403 {
        let text = String::from_utf8_lossy(&body);
        if is_cloudflare_browser_ban(&text) {
            return Err(Error::Transport(cloudflare_1010_message()));
        }
        return Err(map_permission_denied(label, url, status_code, &body));
    }
    if !status.is_success() {
        return Err(Error::realtime(format!(
            "{label}: HTTP {status_code}: {}",
            truncate_body(&body)
        )));
    }
    let payload: TokenResponse = serde_json::from_slice(&body)
        .map_err(|e| Error::realtime(format!("{label}: invalid token response: {e}")))?;
    if payload.token.is_empty() {
        return Err(Error::realtime(format!("{label}: response missing token")));
    }
    Ok(payload.token)
}

fn map_permission_denied(label: &str, endpoint: &str, status: u16, body: &[u8]) -> Error {
    let parsed = serde_json::from_slice::<serde_json::Value>(body).ok();
    let code = parsed
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("permission_denied")
        .to_owned();
    let message = parsed
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| truncate_body(body));
    Error::PermissionDenied {
        message,
        status,
        code,
        context: label.to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

fn truncate_body(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let trimmed = text.trim();
    if trimmed.chars().count() <= MAX_ERROR_BODY_CHARS {
        return trimmed.to_owned();
    }
    let mut out = trimmed
        .chars()
        .take(MAX_ERROR_BODY_CHARS)
        .collect::<String>();
    out.push('…');
    out
}

fn content_length_exceeds_limit(headers: &http::HeaderMap, max_bytes: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max_bytes)
}

fn build_http_client() -> Result<HyperClient> {
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
    Ok(Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(https))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::canonical_query;

    #[test]
    fn subscription_token_url_preserves_hyphens_for_signing() {
        let url = subscription_token_url(
            "https://api.example.test",
            "private:auth:api-keys:account:proto",
        );
        assert_eq!(
            url,
            "https://api.example.test/v1/rt/subscribe?channel=private%3Aauth%3Aapi-keys%3Aaccount%3Aproto"
        );
        assert_eq!(
            canonical_query(&url).unwrap(),
            "channel=private%3Aauth%3Aapi-keys%3Aaccount%3Aproto"
        );
        assert!(
            !canonical_query(&url).unwrap().contains("%2D"),
            "hyphens must not be percent-encoded in the signed query"
        );
    }

    #[test]
    fn realtime_token_exchange_has_finite_limits() {
        assert_eq!(DEFAULT_TOKEN_REQUEST_TIMEOUT, Duration::from_secs(10));
        assert_eq!(MAX_TOKEN_RESPONSE_BYTES, 64 * 1024);
    }

    #[test]
    fn content_length_above_cap_is_rejected() {
        let mut headers = http::HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("65537"));
        assert!(content_length_exceeds_limit(
            &headers,
            MAX_TOKEN_RESPONSE_BYTES
        ));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1024"));
        assert!(!content_length_exceeds_limit(
            &headers,
            MAX_TOKEN_RESPONSE_BYTES
        ));
    }

    #[test]
    fn http_403_maps_to_structured_permission_denied() {
        let err = map_permission_denied(
            "realtime connection token",
            "https://api.example/v1/rt/token",
            403,
            br#"{"code":"permission_denied","message":"missing transfer:read"}"#,
        );
        match err {
            Error::PermissionDenied {
                message,
                status,
                code,
                context,
                endpoint,
            } => {
                assert_eq!(message, "missing transfer:read");
                assert_eq!(status, 403);
                assert_eq!(code, "permission_denied");
                assert_eq!(context, "realtime connection token");
                assert_eq!(endpoint, "https://api.example/v1/rt/token");
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
    }
}
