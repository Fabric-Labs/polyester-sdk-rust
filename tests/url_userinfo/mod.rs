//! REST errors must not carry the userinfo (password) of the API URL.

use super::hardening_support::{HttpScript, MockHttpServer, RefusedPort};
use polyester::{Client, Config};

/// An obviously fake password placed in URL userinfo.
const FAKE_URL_PASSWORD: &str = "fake-rest-url-password-marker";
const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

fn with_userinfo(base_url: &str) -> String {
    base_url.replacen(
        "http://",
        &format!("http://fake-user:{FAKE_URL_PASSWORD}@"),
        1,
    )
}

async fn read_error(api_url: String) -> polyester::Error {
    let client = Client::new(Config {
        api_url,
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    client
        .api_keys
        .get("ak_missing")
        .await
        .expect_err("the server refuses the read")
}

fn assert_no_userinfo(label: &str, err: &polyester::Error) {
    for rendered in [err.to_string(), format!("{err:?}"), format!("{err:#?}")] {
        assert!(
            !rendered.contains(FAKE_URL_PASSWORD),
            "{label}: error leaked URL userinfo: {rendered}"
        );
    }
}

#[tokio::test]
async fn rest_errors_do_not_leak_url_userinfo() {
    for status in [401u16, 403, 404, 500] {
        let http = MockHttpServer::spawn(move |_| HttpScript::Json {
            status,
            body: br#"{"code":"permission_denied","message":"refused"}"#.to_vec(),
        })
        .await;
        let err = read_error(with_userinfo(&http.base_url())).await;
        assert_no_userinfo(&format!("HTTP {status}"), &err);
    }
    let refused = RefusedPort::bind();
    let err = read_error(with_userinfo(&refused.http_url())).await;
    assert_no_userinfo("connection refused", &err);
}
