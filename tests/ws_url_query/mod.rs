//! `ws_url` follows the `api_url` rule: a query string or fragment is refused
//! with the same validation error, so nothing put in one by mistake is sent on
//! every connect and reconnect.

use polyester::{Client, Config, Error};

fn new_client(api_url: Option<&str>, ws_url: Option<&str>) -> polyester::Result<Client> {
    let mut config = Config {
        hydrate_catalogs: false,
        ..Default::default()
    };
    if let Some(api_url) = api_url {
        config.api_url = api_url.into();
    }
    if let Some(ws_url) = ws_url {
        config.ws_url = ws_url.into();
    }
    Client::new(config)
}

#[tokio::test]
async fn ws_url_with_a_query_or_fragment_is_refused_like_api_url() {
    for ws_url in [
        "wss://example.test/connection/websocket?token=SECRET123",
        "wss://example.test?",
        "wss://example.test/connection/websocket#frag",
        "wss://example.test#",
    ] {
        match new_client(None, Some(ws_url)) {
            Err(Error::Validation(message)) => assert_eq!(
                message, "ws_url must not contain a query string or fragment",
                "{ws_url}"
            ),
            Err(other) => panic!("{ws_url}: want Error::Validation, got {other:?}"),
            Ok(_) => panic!("{ws_url}: accepted"),
        }
    }
    match new_client(Some("https://example.test/api?token=SECRET123"), None) {
        Err(Error::Validation(message)) => {
            assert_eq!(
                message,
                "api_url must not contain a query string or fragment"
            )
        }
        Err(other) => panic!("api_url control: want Error::Validation, got {other:?}"),
        Ok(_) => panic!("api_url control: accepted"),
    }
}

#[tokio::test]
async fn plain_ws_urls_are_accepted() {
    for ws_url in [
        "wss://example.test",
        "wss://example.test/connection/websocket",
    ] {
        if let Err(err) = new_client(None, Some(ws_url)) {
            panic!("{ws_url}: {err:?}");
        }
    }
}
