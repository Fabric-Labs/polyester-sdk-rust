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
        assert!(err.is_replay_capacity_exhausted());
        assert_eq!(
            err.auth_error_code(),
            Some(polyester::auth_codes::REPLAY_CAPACITY_EXHAUSTED)
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
            matches!(&err, Error::Api { code, .. } if code == polyester::order_codes::REQUEST_DEADLINE_EXCEEDED),
            "{err:?}"
        );
        assert!(err.is_request_deadline_exceeded());
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.mutation_outcome_unknown(), "{err:?}");
    }
}

/// One HTTP request's head, captured by [`capture_one`].
async fn capture_one(response: &'static [u8]) -> (String, tokio::task::JoinHandle<String>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let join = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut buf).await.expect("read");
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        stream.write_all(response).await.expect("write");
        String::from_utf8_lossy(&raw).into_owned()
    });
    (url, join)
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

const EMPTY_PROTO_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// A configured receive window reaches the wire as the signed
/// X-API-RECV-WINDOW header, verifiable over the payload with the window line;
/// without one, no header is sent.
#[tokio::test]
async fn configured_recv_window_is_sent_and_signed() {
    use ed25519_dalek::{Signature, SigningKey, Verifier as _};
    let seed: [u8; 32] = hex::decode(TEST_KEY).unwrap().try_into().unwrap();
    let public = SigningKey::from_bytes(&seed).verifying_key();
    for window in [Some(2500u32), None] {
        let (url, join) = capture_one(EMPTY_PROTO_OK).await;
        let client = Client::new(Config {
            api_url: url.clone(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            recv_window_ms: window,
            ..Default::default()
        })
        .expect("client");
        let _ = client.auth.me().await;
        let head = join.await.expect("captured");
        assert_eq!(
            header(&head, "X-API-RECV-WINDOW").map(str::to_owned),
            window.map(|ms| ms.to_string()),
            "{head}"
        );
        let payload = polyester::auth::canonical_signing_string_with_recv_window(
            header(&head, "X-API-TIMESTAMP").expect("timestamp"),
            window,
            "POST",
            &format!("{url}/auth.v1.AuthService/Me"),
            b"",
        )
        .expect("payload");
        let sig: [u8; 64] = hex::decode(header(&head, "X-API-SIGNATURE").expect("signature"))
            .unwrap()
            .try_into()
            .unwrap();
        public
            .verify(payload.as_bytes(), &Signature::from_bytes(&sig))
            .expect("signature covers the receive window");
    }
}

#[test]
fn recv_window_payload_and_bounds() {
    let body_hash = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
    let url = "https://api.example.test/foo/bar?b=2&a=1";
    assert_eq!(
        polyester::auth::canonical_signing_string_with_recv_window(
            "123",
            Some(5000),
            "post",
            url,
            b"{}"
        )
        .unwrap(),
        format!("123\n5000\nPOST\n/foo/bar\na=1&b=2\n{body_hash}")
    );
    assert_eq!(
        polyester::auth::canonical_signing_string_with_recv_window("123", None, "post", url, b"{}")
            .unwrap(),
        polyester::auth::canonical_signing_string("123", "post", url, b"{}").unwrap()
    );
    let creds = polyester::auth::Credentials::new("ak_recv_window_bounds", TEST_KEY).unwrap();
    for bad in [0u32, 60_001, u32::MAX] {
        assert!(matches!(
            creds.sign_request_with_recv_window(
                "GET",
                "https://api.example.test/x",
                b"",
                None,
                Some(bad)
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Client::new(Config {
                recv_window_ms: Some(bad),
                hydrate_catalogs: false,
                ..Default::default()
            }),
            Err(Error::Validation(_))
        ));
    }
    for ok in [
        polyester::auth::MIN_RECV_WINDOW_MS,
        polyester::auth::MAX_RECV_WINDOW_MS,
    ] {
        let headers = creds
            .sign_request_with_recv_window("GET", "https://api.example.test/x", b"", None, Some(ok))
            .unwrap();
        assert_eq!(headers.get("X-API-RECV-WINDOW"), Some(&ok.to_string()));
    }
}

/// An account's flow list carries received transfers: each row says how the
/// account takes part, with the addresses that name the counterparty, and the
/// request's participation filter reaches the wire.
#[tokio::test]
async fn flow_rows_carry_participation_and_the_filter_is_sent() {
    use polyester::proto::chain::lifecycle::v1::__buffa::oneof::list_flows_request::AccountSelector;
    use polyester::proto::chain::lifecycle::v1::{
        FlowParticipation, FlowSummaryView, ListFlowsRequest, ListFlowsResponse,
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
    let record = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        let sent = ListFlowsRequest::decode_from_slice(&req.body).expect("request");
        assert!(
            sent.account_selector.is_some(),
            "the account selector is sent"
        );
        record.lock().unwrap().push(sent.participation.to_i32());
        let body = ListFlowsResponse {
            flows: vec![
                FlowSummaryView {
                    flow_id: "sent".into(),
                    participation: FlowParticipation::PARTICIPATION_OWNER.into(),
                    destination_address: "0x00000000000000000000000000000000000000b2".into(),
                    ..Default::default()
                },
                FlowSummaryView {
                    flow_id: "received".into(),
                    participation: FlowParticipation::PARTICIPATION_RECIPIENT.into(),
                    source_address: "0x00000000000000000000000000000000000000c3".into(),
                    ..Default::default()
                },
                FlowSummaryView {
                    flow_id: "future".into(),
                    participation: buffa::EnumValue::from(9),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
        .encode_to_vec();
        HttpScript::Raw {
            status: 200,
            headers: vec![
                ("Content-Type".to_owned(), "application/proto".to_owned()),
                ("Content-Length".to_owned(), body.len().to_string()),
            ],
            body,
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    let page = client
        .lifecycle
        .list_flows(ListFlowsRequest {
            account_selector: Some(AccountSelector::OwnerAccountId(8)),
            participation: FlowParticipation::PARTICIPATION_RECIPIENT.into(),
            ..Default::default()
        })
        .await
        .expect("page");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![FlowParticipation::PARTICIPATION_RECIPIENT as i32]
    );
    let rows: Vec<_> = page
        .flows
        .iter()
        .map(|f| {
            (
                f.participation.as_str(),
                f.source_address.as_str(),
                f.destination_address.as_str(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("owner", "", "0x00000000000000000000000000000000000000b2"),
            (
                "recipient",
                "0x00000000000000000000000000000000000000c3",
                ""
            ),
            ("unknown_participation_9", "", ""),
        ]
    );
}
