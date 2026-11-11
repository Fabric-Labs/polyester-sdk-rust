//! POLY-9228: no member of an error's chain prints a credential.
//!
//! An answer can quote a URL with userinfo, such as a proxy's. The SDK error
//! copied that text, so `Display`, `Debug` and every `source()` could print
//! the credential. Every Connect error is now redacted where it is mapped,
//! keeping the error's variant and its source chain. The URL and credentials
//! here are synthetic.

use super::catalog_startup_recovery::{Action, Api, FUNDING, plan};
use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::{Client, Config, Error};
use std::time::Duration;

const USER: &str = "sdk-proxy-user";
const SECRET: &str = "proxy-secret-9228";
const QUERY: &str = "token=query-secret";

fn proxy_url() -> String {
    format!("http://{USER}:{SECRET}@proxy.example.test:3128/tunnel?{QUERY}")
}

fn answers() -> Vec<(&'static str, u16, String)> {
    vec![
        (
            "connect-error",
            503,
            format!(
                r#"{{"code":"unavailable","message":"upstream {} refused"}}"#,
                proxy_url()
            ),
        ),
        (
            "gateway-problem",
            502,
            format!(
                r#"{{"code":"EDGE_PROXY_FAILED","detail":"upstream {} refused"}}"#,
                proxy_url()
            ),
        ),
    ]
}

fn assert_chain_clean(name: &str, err: &Error) {
    let mut printed = format!("{err} {err:?} {err:#?}");
    let mut source = std::error::Error::source(err);
    while let Some(level) = source {
        printed.push_str(&format!(" {level} {level:?}"));
        source = level.source();
    }
    for secret in [USER, SECRET, QUERY] {
        assert!(
            !printed.contains(secret),
            "{name}: prints {secret}: {printed}"
        );
    }
}

#[tokio::test]
async fn a_unary_call_chains_no_credential() {
    for (name, status, body) in answers() {
        let http = MockHttpServer::spawn(move |_| HttpScript::Json {
            status,
            body: body.clone().into_bytes(),
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            timeout: Duration::from_secs(2),
            hydrate_catalogs: false,
            allow_insecure_http: true,
            ..Default::default()
        })
        .expect("client");
        let err = client
            .market_data
            .get_spot_config()
            .await
            .expect_err("the stub refuses");
        assert!(
            err.to_string().contains("proxy.example.test"),
            "{name}: {err}"
        );
        assert_chain_clean(name, &err);
    }
}

#[tokio::test]
async fn catalog_startup_chains_no_credential() {
    for (name, status, body) in answers() {
        let api = Api::spawn_with_funding_answer(
            plan(|op, _| {
                if op == FUNDING {
                    Action::Unavailable
                } else {
                    Action::Answer
                }
            }),
            status,
            body,
        )
        .await;
        let client = Client::new(api.config(Duration::from_secs(2))).expect("client");
        client.wait_for_catalogs().await.expect("spot config");
        let err = client
            .wait_for_funding_catalog()
            .await
            .expect_err("the funding config is refused");
        assert!(matches!(err, Error::CatalogLoad(_)), "{name}: {err:?}");
        assert!(
            std::error::Error::source(&err).is_some(),
            "{name}: the cause is kept"
        );
        assert_chain_clean(name, &err);
    }
}

// Round 2: URLs whose userinfo or query holds a parenthesis or an
// apostrophe, and structured diagnostics.

async fn spot_config_error(status: u16, content_type: &'static str, body: String) -> Error {
    let http = MockHttpServer::spawn(move |_| HttpScript::Raw {
        status,
        headers: vec![("Content-Type".into(), content_type.into())],
        body: body.clone().into_bytes(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: false,
        allow_insecure_http: true,
        ..Default::default()
    })
    .expect("client");
    client
        .market_data
        .get_spot_config()
        .await
        .expect_err("the stub refuses")
}

#[tokio::test]
async fn a_url_with_parentheses_or_quotes_is_redacted() {
    let urls = [
        format!("http://{USER}:one(two-{SECRET})@proxy.example.test/x"),
        format!("http://{USER}:one'two-{SECRET}@proxy.example.test/x"),
        format!("http://proxy.example.test/x?{QUERY}one(two-{SECRET})"),
        format!("http://proxy.example.test/x?{QUERY}one'two-{SECRET}"),
        format!("http://{USER}:{SECRET}?bad@proxy.example.test/x"),
        format!("http://{USER}:{SECRET}#bad@proxy.example.test/x"),
    ];
    for url in urls {
        let body = serde_json::json!({"code": "unavailable", "message": format!("upstream {url} refused")})
            .to_string();
        let err = spot_config_error(503, "application/json", body).await;
        // A URL the parser refuses is dropped whole.
        let want = if url.contains("bad@") {
            "[REDACTED INVALID URL]"
        } else {
            "proxy.example.test"
        };
        assert!(err.to_string().contains(want), "{url}: {err}");
        assert_chain_clean(&url, &err);
    }
}

/// Client-held secrets: the userinfo of the client's own API and WebSocket
/// URLs and its API private key never reach an error's text, however the
/// answer quotes them.
#[tokio::test]
async fn client_held_secrets_never_reach_an_error() {
    const API: (&str, &str) = ("sdk-api-user", "api-secret-9228");
    const WS: (&str, &str) = ("sdk-ws-user", "ws-secret-9228");
    const KEY: &str = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    let message = format!(
        "auth {}:{} via http//{}:{}@api ws {}:{} and {} key {KEY}",
        API.0, API.1, API.0, API.1, WS.0, WS.1, WS.1
    );
    let body = serde_json::json!({"code": "unavailable", "message": message}).to_string();
    let http = MockHttpServer::spawn(move |_| HttpScript::Json {
        status: 503,
        body: body.clone().into_bytes(),
    })
    .await;
    let host = http.base_url().trim_start_matches("http://").to_owned();
    let client = Client::new(Config {
        api_url: format!("http://{}:{}@{host}", API.0, API.1),
        ws_url: format!("ws://{}:{}@127.0.0.1:9/unused", WS.0, WS.1),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(KEY.into()),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: false,
        allow_insecure_http: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .market_data
        .get_spot_config()
        .await
        .expect_err("the stub refuses");
    let printed = format!("{err} {err:?} {err:#?}");
    assert!(printed.contains("[REDACTED]"), "{printed}");
    for secret in [
        format!("{}:{}", API.0, API.1),
        API.1.to_owned(),
        format!("{}:{}", WS.0, WS.1),
        WS.1.to_owned(),
        KEY.to_owned(),
    ] {
        assert!(!printed.contains(&secret), "prints {secret}: {printed}");
    }
}

#[tokio::test]
async fn structured_diagnostics_are_redacted() {
    let cases = [
        (
            "timestamp-skew-problem",
            401,
            serde_json::json!({"code": "TIMESTAMP_SKEW", "detail": format!("upstream {} refused", proxy_url())}),
        ),
        (
            "rate-limit-detail",
            429,
            serde_json::json!({
                "code": "RATE_LIMIT_EXCEEDED",
                "detail": format!("upstream {}", proxy_url()),
                "rate_limit": {"reason": "ip", "operation_id": proxy_url()},
            }),
        ),
    ];
    for (name, status, body) in cases {
        let err = spot_config_error(status, "application/problem+json", body.to_string()).await;
        let structured = match &err {
            Error::TimestampSkew { problem, .. } => format!("{problem:?} {problem:#?}"),
            Error::RateLimit { detail, .. } => format!("{detail:?} {detail:#?}"),
            other => panic!("{name}: unexpected {other:?}"),
        };
        assert!(structured.len() > 10, "{name}: no diagnostic in {err:?}");
        for secret in [USER, SECRET, QUERY] {
            assert!(
                !structured.contains(secret),
                "{name}: prints {secret}: {structured}"
            );
        }
        assert_chain_clean(name, &err);
    }
}
