//! Receive-window, replay-capacity and request-deadline refusals are typed by
//! their retry rule over ConnectRPC and REST alike (contract c8d9827).

use super::hardening_support::{HttpScript, MockHttpServer};
use base64::Engine as _;
use buffa::Message as _;
use polyester::proto::auth::v1::{AuthErrorCode, AuthErrorDetail};
use polyester::proto::orders::v1::{ErrorCode as OrderErrorCode, ErrorDetail as OrderErrorDetail};
use polyester::{Client, Config, Error};
use serde_json::json;

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const PROBLEM: &str = "application/problem+json";
const JSON: &str = "application/json";

fn response(status: u16, content_type: &str, body: &str, extra: &[(&str, &str)]) -> HttpScript {
    let mut headers = vec![
        ("Content-Type".to_owned(), content_type.to_owned()),
        ("Content-Length".to_owned(), body.len().to_string()),
    ];
    headers.extend(
        extra
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
    );
    HttpScript::Raw {
        status,
        headers,
        body: body.as_bytes().to_vec(),
    }
}

async fn me_error(script: impl Fn() -> HttpScript + Send + Sync + 'static) -> Error {
    let http = MockHttpServer::spawn(move |_| script()).await;
    Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
    .auth
    .me()
    .await
    .expect_err("refused")
}

fn connect_body(code: &str, message: &str, type_name: &str, value: Vec<u8>) -> String {
    json!({
        "code": code,
        "message": message,
        "details": [{
            "type": type_name,
            "value": base64::engine::general_purpose::STANDARD_NO_PAD.encode(value),
        }],
    })
    .to_string()
}

const REPLAY: &str = "replay capacity exhausted for this API key";

/// Valid credentials and nothing processed: the key's share of the replay
/// record frees as earlier windows end, so it is a retryable rate refusal,
/// never an authentication failure or an unknown outcome.
#[tokio::test]
async fn replay_capacity_exhausted_is_a_retryable_rate_refusal() {
    let detail = AuthErrorDetail {
        code: AuthErrorCode::AUTH_REPLAY_CAPACITY_EXHAUSTED.into(),
        message: REPLAY.into(),
        ..Default::default()
    };
    let connect = connect_body(
        "resource_exhausted",
        REPLAY,
        "auth.v1.AuthErrorDetail",
        detail.encode_to_vec(),
    );
    let rest =
        json!({"status": 429, "code": "REPLAY_CAPACITY_EXHAUSTED", "detail": REPLAY}).to_string();
    let bare = json!({"status": 429, "code": "REPLAY_CAPACITY_EXHAUSTED"}).to_string();
    for (content_type, body, headers, retry_after) in [
        (JSON, connect, &[][..], None),
        (PROBLEM, rest, &[][..], None),
        (PROBLEM, bare, &[("Retry-After", "1")][..], Some(1.0)),
    ] {
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let err = me_error(move || {
            let extra: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            response(429, content_type, &body, &extra)
        })
        .await;
        assert!(
            matches!(&err, Error::RateLimit { code: Some(code), .. } if code == "AUTH_REPLAY_CAPACITY_EXHAUSTED"),
            "{err:?}"
        );
        assert_eq!(
            err.auth_code(),
            Some(AuthErrorCode::AUTH_REPLAY_CAPACITY_EXHAUSTED)
        );
        assert_eq!(err.retry_after(), retry_after, "{err:?}");
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.mutation_outcome_unknown(), "{err:?}");
    }
}

/// AUTH_RECV_WINDOW_INVALID refuses an X-API-RECV-WINDOW header that is not
/// one whole number from 1 to 60000: an API-key refusal the caller must fix.
#[tokio::test]
async fn recv_window_invalid_is_a_terminal_api_key_refusal() {
    let message = "X-API-RECV-WINDOW must be an integer from 1 to 60000";
    let detail = AuthErrorDetail {
        code: AuthErrorCode::AUTH_RECV_WINDOW_INVALID.into(),
        message: message.into(),
        ..Default::default()
    };
    let connect = connect_body(
        "unauthenticated",
        message,
        "auth.v1.AuthErrorDetail",
        detail.encode_to_vec(),
    );
    let rest = json!({"status": 401, "code": "RECV_WINDOW_INVALID", "detail": message}).to_string();
    for (content_type, body) in [(JSON, connect), (PROBLEM, rest)] {
        let err = me_error(move || response(401, content_type, &body, &[])).await;
        assert!(
            matches!(&err, Error::ApiKeyRejected { status: 401, .. }),
            "{err:?}"
        );
        assert_eq!(
            err.auth_code(),
            Some(AuthErrorCode::AUTH_RECV_WINDOW_INVALID)
        );
        assert!(!err.is_retryable(), "{err:?}");
        assert!(!err.mutation_outcome_unknown(), "{err:?}");
    }
}

/// ERROR_CODE_REQUEST_DEADLINE_EXCEEDED (308): the write's window ended before
/// admission. Nothing changed, so a retry (signed again) may succeed, and it is
/// never an unknown outcome.
#[tokio::test]
async fn request_deadline_exceeded_is_retryable_and_known() {
    let message = "request deadline exceeded before admission";
    let detail = OrderErrorDetail {
        code: OrderErrorCode::ERROR_CODE_REQUEST_DEADLINE_EXCEEDED.into(),
        ..Default::default()
    };
    let connect = connect_body(
        "failed_precondition",
        message,
        "orders.v1.ErrorDetail",
        detail.encode_to_vec(),
    );
    let rest =
        json!({"status": 400, "code": "REQUEST_DEADLINE_EXCEEDED", "detail": message}).to_string();
    for (content_type, body) in [(JSON, connect), (PROBLEM, rest)] {
        let err = me_error(move || response(400, content_type, &body, &[])).await;
        assert!(
            matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_REQUEST_DEADLINE_EXCEEDED"),
            "{err:?}"
        );
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.mutation_outcome_unknown(), "{err:?}");
    }
}
