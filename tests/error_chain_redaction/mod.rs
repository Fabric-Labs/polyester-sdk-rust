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
