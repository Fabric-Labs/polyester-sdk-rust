//! A dropped client forgets its secrets: once every client holding a private
//! key or URL userinfo is gone, the process no longer keeps a copy of it for
//! redaction. While a client holds one, it stays redacted.

use crate::transport::redact_text;
use crate::{Client, Config};

fn keyed_client(private_key_hex: &str) -> Client {
    Client::new(Config {
        api_key_id: Some("ak_test_0".into()),
        api_private_key: Some(private_key_hex.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
}

#[tokio::test]
async fn dropping_clients_releases_their_private_keys() {
    let key_a = "c3".repeat(32);
    let key_b = "d4".repeat(32);
    let line = format!("leak {key_a} and {key_b}");
    assert_eq!(redact_text(&line), line, "redacted before any client");

    let client_a = keyed_client(&key_a);
    let client_b = keyed_client(&key_b);
    assert_eq!(redact_text(&line), "leak [REDACTED] and [REDACTED]");

    drop(client_a);
    assert_eq!(redact_text(&line), format!("leak {key_a} and [REDACTED]"));
    drop(client_b);
    assert_eq!(
        redact_text(&line),
        line,
        "still registered after both dropped"
    );
}

#[tokio::test]
async fn a_shared_private_key_stays_redacted_while_one_client_holds_it() {
    let key = "e5".repeat(32);
    let line = format!("leak {key}");
    let first = keyed_client(&key);
    let second = keyed_client(&key);
    drop(first);
    assert_eq!(redact_text(&line), "leak [REDACTED]");
    drop(second);
    assert_eq!(redact_text(&line), line);
}

#[tokio::test]
async fn dropping_a_client_releases_its_url_userinfo() {
    let client = Client::new(Config {
        api_url: "https://dropuser:drop-url-secret-Q7@example.test".into(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    let line = "saw drop-url-secret-Q7";
    assert_eq!(redact_text(line), "saw [REDACTED]");
    drop(client);
    assert_eq!(redact_text(line), line);
}
