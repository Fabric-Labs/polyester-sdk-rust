//! Signed HTTP token exchange for private Centrifugo channels.

use crate::auth::{Credentials, encode_query_component};
use crate::errors::{Error, Result};
use http_body_util::{BodyExt, Empty, Limited};
use hyper::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use std::time::Duration;

type HyperClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Empty<bytes::Bytes>,
>;

const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TOKEN_RESPONSE_BYTES: usize = 64 * 1024;

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

pub async fn fetch_connection_token(creds: &Credentials, api_url: &str) -> Result<String> {
    let url = connection_token_url(api_url);
    fetch_rt_token(creds, &url, "realtime connection token").await
}

pub async fn fetch_subscription_token(
    creds: &Credentials,
    api_url: &str,
    channel: &str,
) -> Result<String> {
    let url = subscription_token_url(api_url, channel);
    fetch_rt_token(
        creds,
        &url,
        &format!("realtime subscription token for {channel}"),
    )
    .await
}

async fn fetch_rt_token(creds: &Credentials, url: &str, label: &str) -> Result<String> {
    let client = build_http_client()?;
    let headers = creds.sign_request("GET", url, b"", None);
    let uri: hyper::Uri = url
        .parse()
        .map_err(|e| Error::realtime(format!("{label}: invalid url: {e}")))?;
    let mut req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Empty::<bytes::Bytes>::new())
        .map_err(|e| Error::realtime(format!("{label}: request build: {e}")))?;
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::realtime(format!("{label}: header name: {e}")))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|e| Error::realtime(format!("{label}: header value: {e}")))?;
        req.headers_mut().insert(name, value);
    }

    let resp = tokio::time::timeout(TOKEN_REQUEST_TIMEOUT, client.request(req))
        .await
        .map_err(|_| {
            Error::realtime(format!(
                "{label}: HTTP request timed out after {TOKEN_REQUEST_TIMEOUT:?}"
            ))
        })?
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

    if status.as_u16() == 401 {
        return Err(Error::auth(format!("{label}: authentication failed")));
    }
    if !status.is_success() {
        return Err(Error::realtime(format!(
            "{label}: HTTP {}",
            status.as_u16()
        )));
    }
    let payload: TokenResponse = serde_json::from_slice(&body)
        .map_err(|e| Error::realtime(format!("{label}: invalid token response: {e}")))?;
    if payload.token.is_empty() {
        return Err(Error::realtime(format!("{label}: response missing token")));
    }
    Ok(payload.token)
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
            canonical_query(&url),
            "channel=private%3Aauth%3Aapi-keys%3Aaccount%3Aproto"
        );
        assert!(
            !canonical_query(&url).contains("%2D"),
            "hyphens must not be percent-encoded in the signed query"
        );
    }

    #[test]
    fn realtime_token_exchange_has_finite_limits() {
        assert_eq!(TOKEN_REQUEST_TIMEOUT, Duration::from_secs(10));
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
}
