//! Signed HTTP token exchange for private Centrifugo channels.

use crate::auth::Credentials;
use crate::errors::{Error, Result};
use http_body_util::Empty;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use std::time::Duration;

type HyperClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Empty<bytes::Bytes>,
>;

pub fn connection_token_url(api_url: &str) -> String {
    format!("{}/v1/rt/token", api_url.trim_end_matches('/'))
}

pub fn subscription_token_url(api_url: &str, channel: &str) -> String {
    let encoded = utf8_percent_encode(channel, NON_ALPHANUMERIC).to_string();
    format!(
        "{}/v1/rt/subscribe?channel={}",
        api_url.trim_end_matches('/'),
        encoded
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

    let resp = client
        .request(req)
        .await
        .map_err(|e| Error::realtime(format!("{label}: HTTP request failed: {e}")))?;
    let status = resp.status();
    let body = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .map_err(|e| Error::realtime(format!("{label}: read body: {e}")))?
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
