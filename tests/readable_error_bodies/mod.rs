//! An HTTP error with no recognised code proves nothing about a mutation.
//!
//! Each case goes through a public read and a public create against a local
//! server. Only the requested procedure is counted and every expectation needs
//! at least one request, so a case that never reached the server fails.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::orders::v1::{ErrorCode as OrderErrorCode, ErrorDetail, FieldViolation};
use polyester::proto::polyester::ratelimit::v1::RateLimitDetail;
use polyester::proto::triggers::v1::{GetTriggerResponse, Trigger};
use polyester::{Client, Config, Error, Price, Quantity, QuantityDomain};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const JSON: &str = "application/json";
const PROBLEM: &str = "application/problem+json";
const READ_PATH: &str = "/triggers.v1.TriggersService/GetTrigger";
const WRITE_PATH: &str = "/orders.v1.OrdersService/CreateOrder";

type Body = (&'static str, &'static str, &'static str);

const NO_CODE_BODIES: [Body; 10] = [
    ("no-code", JSON, r#"{"message":"m"}"#),
    ("empty-code", JSON, r#"{"code":"","message":"m"}"#),
    ("null-code", JSON, r#"{"code":null,"message":"m"}"#),
    ("int-code", JSON, r#"{"code":5,"message":"m"}"#),
    ("empty-object", JSON, "{}"),
    ("problem-no-code", PROBLEM, r#"{"title":"t","detail":"d"}"#),
    ("json-array", JSON, "[1]"),
    ("broken-json", JSON, "{broken"),
    (
        "unknown-detail",
        JSON,
        r#"{"message":"m","details":[{"type":"x.y.Unknown","value":"AA"}]}"#,
    ),
    ("lowercase-code", JSON, r#"{"code":"nope","message":"m"}"#),
];
const BARE_BODIES: [Body; 4] = [
    ("html", "text/html", "<html>oops</html>"),
    ("text", "text/plain", "oops"),
    ("empty-text", "text/plain", ""),
    ("empty-json", JSON, ""),
];
const UNRECOGNISED: Body = (
    "unrecognised",
    PROBLEM,
    r#"{"code":"NOPE_NOT_A_CODE","detail":"d"}"#,
);
const CONNECT_UNKNOWN: Body = (
    "connect-unknown",
    JSON,
    r#"{"code":"unknown","message":"m"}"#,
);
const INTERNAL_STATUSES: [u16; 15] = [
    400, 402, 405, 409, 410, 412, 413, 418, 422, 431, 500, 501, 505, 520, 599,
];
const OPS: [&str; 2] = ["read", "write"];

/// Answer one public call with this response. A read gets a success after
/// `fail_for` failures. Returns its result and the requests made.
async fn call(
    op: &str,
    status: u16,
    content_type: &str,
    body: &str,
    fail_for: Option<usize>,
) -> (Option<Error>, usize) {
    let path = if op == "read" { READ_PATH } else { WRITE_PATH };
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let (content_type, body) = (content_type.to_owned(), body.to_owned());
    let http = MockHttpServer::spawn(move |req| {
        if req.path != path {
            return HttpScript::NotFound;
        }
        let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
        if fail_for.is_some_and(|limit| n > limit) {
            return connect_proto_ok(&GetTriggerResponse {
                trigger: Trigger {
                    trigger_id: 9,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            });
        }
        HttpScript::Raw {
            status,
            headers: vec![
                ("Content-Type".into(), content_type.clone()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body: body.as_bytes().to_vec(),
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
    client
        .catalogs
        .hydrate_spot_config_json(
            json!({"pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]}),
        )
        .expect("catalog");
    let error = if op == "read" {
        client.triggers.get_by_id("9", None).await.err()
    } else {
        let mut params =
            CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
        params.quantity = Some(
            Quantity::from_scaled(
                1,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .expect("quantity"),
        );
        params.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).expect("price"));
        params.time_in_force = Some(CreateTimeInForce::Gtc);
        client.orders.create(params).await.err()
    };
    (error, calls.load(Ordering::SeqCst))
}

fn assert_flags(err: &Error, retryable: bool, unknown: bool, case: &str) {
    assert_eq!(err.is_retryable(), retryable, "{case}: {err:?}");
    assert_eq!(err.mutation_outcome_unknown(), unknown, "{case}: {err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_recognised_code_is_a_server_error_with_outcome_unknown() {
    let extra: [Body; 4] = [
        UNRECOGNISED,
        CONNECT_UNKNOWN,
        (
            "connect-data-loss",
            JSON,
            r#"{"code":"data_loss","message":"m"}"#,
        ),
        (
            "connect-canceled",
            JSON,
            r#"{"code":"canceled","message":"m"}"#,
        ),
    ];
    for status in INTERNAL_STATUSES {
        for (name, content_type, body) in NO_CODE_BODIES.iter().chain(&BARE_BODIES).chain(&extra) {
            for op in OPS {
                let case = format!("{status}/{name}/{op}");
                let (err, calls) = call(op, status, content_type, body, None).await;
                let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
                assert!(matches!(err, Error::Server(_)), "{case}: {err:?}");
                assert_flags(&err, false, true, &case);
                // Not unavailable, so a read doesn't retry it either.
                assert_eq!(calls, 1, "{case}");
                if *name == "unrecognised" {
                    assert!(err.to_string().contains("NOPE_NOT_A_CODE"), "{case}: {err}");
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn json_404_without_a_code_is_not_a_missing_route() {
    for (name, content_type, body) in NO_CODE_BODIES.iter().chain(&[UNRECOGNISED]) {
        for op in OPS {
            let case = format!("{name}/{op}");
            let (err, calls) = call(op, 404, content_type, body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(matches!(err, Error::Server(_)), "{case}: {err:?}");
            assert!(!err.is_not_implemented(), "{case}: {err:?}");
            assert_flags(&err, false, true, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bare_404_stays_a_missing_route_with_outcome_unknown() {
    let extra: [Body; 2] = [
        ("route-message", JSON, r#"{"message":"404 page not found"}"#),
        ("route-phrase", JSON, r#"{"message":"Not Found"}"#),
    ];
    for (name, content_type, body) in BARE_BODIES.iter().chain(&extra) {
        for op in OPS {
            let case = format!("{name}/{op}");
            let (err, calls) = call(op, 404, content_type, body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(err, Error::RouteNotFound { .. }),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, true, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn status_401_and_403_stay_definite_auth_refusals() {
    for status in [401, 403] {
        for (name, content_type, body) in NO_CODE_BODIES
            .iter()
            .chain(&BARE_BODIES)
            .chain(&[UNRECOGNISED, CONNECT_UNKNOWN])
        {
            for op in OPS {
                let case = format!("{status}/{name}/{op}");
                let (err, calls) = call(op, status, content_type, body, None).await;
                let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
                assert!(
                    matches!(err, Error::Auth(_) | Error::ApiKeyRejected { .. }),
                    "{case}: {err:?}"
                );
                assert_flags(&err, false, false, &case);
                assert_eq!(calls, 1, "{case}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unavailable_statuses_stay_unknown_send_a_create_once_and_let_reads_retry() {
    for status in [429, 502, 503, 504] {
        for (name, content_type, body) in NO_CODE_BODIES.iter().chain(&BARE_BODIES) {
            let case = format!("{status}/{name}");
            let (err, calls) = call("write", status, content_type, body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(err, Error::Server(_) | Error::GatewayTimeout { .. }),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, true, &case);
            assert_eq!(calls, 1, "{case}: a create is sent once");
            let (err, calls) = call("read", status, content_type, body, Some(2)).await;
            assert!(err.is_none(), "{case}: {err:?}");
            assert_eq!(calls, 3, "{case}: a read succeeds on its third request");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn well_formed_unrecognised_429_stays_a_known_refusal() {
    for code in ["FUTURE_CAPACITY", "NOPE_NOT_A_CODE"] {
        let body = json!({"code": code, "detail": "d"}).to_string();
        for op in OPS {
            let case = format!("{code}/{op}");
            let (err, calls) = call(op, 429, PROBLEM, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(&err, Error::Api { code: found, .. } if found == code),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn trigger_not_found_keeps_its_typed_error() {
    for (body, expected) in [
        (
            json!({"code": "TRIGGER_NOT_FOUND", "detail": "d"}).to_string(),
            "ERROR_CODE_TRIGGER_NOT_FOUND",
        ),
        (
            json!({"code": "not_found", "message": "m"}).to_string(),
            "NotFound",
        ),
    ] {
        for op in OPS {
            let case = format!("{expected}/{op}");
            let (err, calls) = call(op, 404, JSON, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(&err, Error::Api { code, .. } if code == expected),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

/// Every published REST code is sent once and keeps its meaning.
#[tokio::test(flavor = "multi_thread")]
async fn every_contract_code_keeps_its_meaning_on_a_create() {
    let catalog: Value =
        serde_json::from_str(include_str!("../fixtures/rest_error_codes.json")).expect("catalog");
    let entries = catalog["codes"].as_array().expect("codes");
    assert!(!entries.is_empty());
    for entry in entries {
        let code = entry["code"].as_str().expect("code");
        let status = entry["status"].as_u64().expect("status") as u16;
        let case = format!("{status}/{code}");
        let body = json!({"code": code, "detail": "d"}).to_string();
        let (err, calls) = call("write", status, PROBLEM, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
        assert_eq!(calls, 1, "{case}");
        assert!(
            !matches!(&err, Error::Api { code, .. } if code == "Unknown"),
            "{case}: code lost: {err:?}"
        );
        match status {
            501 => {
                assert!(
                    matches!(err, Error::NotImplemented { .. }),
                    "{case}: {err:?}"
                );
                assert_flags(&err, false, false, &case);
            }
            500 => {
                assert!(matches!(err, Error::Server(_)), "{case}: {err:?}");
                assert_flags(&err, false, true, &case);
            }
            504 => assert_flags(&err, false, true, &case),
            s if s < 500 => {
                assert!(
                    !matches!(err, Error::Server(_) | Error::RouteNotFound { .. }),
                    "{case}: {err:?}"
                );
                assert!(!err.mutation_outcome_unknown(), "{case}: {err:?}");
            }
            _ => {}
        }
    }
}

const PLACEHOLDER_CODES: [&str; 3] = ["UNSPECIFIED", "ERROR_CODE_UNSPECIFIED", "AUTH_UNSPECIFIED"];
const DETAIL_TYPES: [&str; 6] = [
    "orders.v1.ErrorDetail",
    "auth.v1.AuthErrorDetail",
    "chain.withdraw.v1.ErrorDetail",
    "transfer.v1.ErrorDetail",
    "ledger.read.v1.ErrorDetail",
    "polyester.ratelimit.v1.RateLimitDetail",
];
const NO_CODE_STATUSES: [u16; 16] = [
    400, 402, 404, 405, 409, 410, 412, 413, 418, 422, 431, 500, 501, 505, 520, 599,
];

fn detail_body(detail_type: &str, value: &[u8], code: Option<&str>) -> String {
    let mut body = json!({
        "message": "m",
        "details": [{
            "type": detail_type,
            "value": base64::engine::general_purpose::STANDARD.encode(value),
        }],
    });
    if let Some(code) = code {
        body["code"] = json!(code);
    }
    body.to_string()
}

async fn expect_server(op: &str, status: u16, content_type: &str, body: &str, case: &str) {
    let (err, calls) = call(op, status, content_type, body, None).await;
    let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
    assert!(
        matches!(err, Error::Server(_) | Error::GatewayTimeout { .. }),
        "{case}: {err:?}"
    );
    assert_flags(&err, false, true, case);
    assert_eq!(calls, 1, "{case}");
}

async fn expect_auth(status: u16, content_type: &str, body: &str, case: &str) {
    let (err, calls) = call("write", status, content_type, body, None).await;
    let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
    assert!(
        matches!(err, Error::Auth(_) | Error::ApiKeyRejected { .. }),
        "{case}: {err:?}"
    );
    assert_flags(&err, false, false, case);
    assert_eq!(calls, 1, "{case}");
}

#[tokio::test(flavor = "multi_thread")]
async fn placeholder_code_is_no_code() {
    for code in PLACEHOLDER_CODES {
        let body = json!({"code": code, "detail": "d"}).to_string();
        for status in NO_CODE_STATUSES {
            for op in OPS {
                let case = format!("{code}/{status}/{op}");
                expect_server(op, status, PROBLEM, &body, &case).await;
            }
        }
        for status in [401, 403] {
            expect_auth(status, PROBLEM, &body, &format!("{code}/{status}")).await;
        }
        for status in [429, 502, 503, 504] {
            let case = format!("{code}/{status}");
            expect_server("write", status, PROBLEM, &body, &case).await;
            let (err, calls) = call("read", status, PROBLEM, &body, Some(2)).await;
            assert!(err.is_none(), "{case}: {err:?}");
            assert_eq!(calls, 3, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn detail_with_no_code_is_no_code() {
    for detail_type in DETAIL_TYPES {
        for (name, value) in [("empty", &b""[..]), ("undecodable", &[0u8][..])] {
            let body = detail_body(detail_type, value, None);
            for status in NO_CODE_STATUSES {
                for op in OPS {
                    let case = format!("{detail_type}/{name}/{status}/{op}");
                    expect_server(op, status, JSON, &body, &case).await;
                }
            }
            for status in [401, 403] {
                expect_auth(
                    status,
                    JSON,
                    &body,
                    &format!("{detail_type}/{name}/{status}"),
                )
                .await;
            }
            for status in [429, 502, 503, 504] {
                let case = format!("{detail_type}/{name}/{status}");
                expect_server("write", status, JSON, &body, &case).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_detail_does_not_turn_a_server_failure_into_a_refusal() {
    for detail_type in DETAIL_TYPES {
        for (status, code) in [
            (500, "internal"),
            (500, "unknown"),
            (500, "data_loss"),
            (503, "unavailable"),
        ] {
            let body = detail_body(detail_type, b"", Some(code));
            let case = format!("{detail_type}/{code}");
            expect_server("write", status, JSON, &body, &case).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recognised_connect_code_stands_without_its_empty_detail() {
    for detail_type in DETAIL_TYPES {
        for (status, code, expected) in [
            (400, "invalid_argument", "InvalidArgument"),
            (404, "not_found", "NotFound"),
            (409, "already_exists", "AlreadyExists"),
        ] {
            let body = detail_body(detail_type, b"", Some(code));
            let case = format!("{detail_type}/{code}");
            let (err, calls) = call("write", status, JSON, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(&err, Error::Api { code, .. } if code == expected),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn placeholder_does_not_name_a_resource_refusal() {
    for body in [
        detail_body("orders.v1.ErrorDetail", b"", Some("resource_exhausted")),
        json!({"code": "resource_exhausted", "message": "ERROR_CODE_UNSPECIFIED"}).to_string(),
        json!({"code": "resource_exhausted", "message": "UNSPECIFIED"}).to_string(),
    ] {
        let (err, calls) = call("write", 429, JSON, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{body}: expected an error"));
        assert!(
            matches!(&err, Error::RateLimit { code: None, .. }),
            "{body}: {err:?}"
        );
        assert_eq!(calls, 1, "{body}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_typed_detail_is_still_a_code_control() {
    let raw = ErrorDetail {
        code: OrderErrorCode::ERROR_CODE_BAD_PRICE.into(),
        ..Default::default()
    }
    .encode_to_bytes();
    for status in [400, 500] {
        let body = detail_body("orders.v1.ErrorDetail", &raw, None);
        let (err, calls) = call("write", status, JSON, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{status}: expected an error"));
        assert!(
            matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_BAD_PRICE"),
            "{status}: {err:?}"
        );
        assert_flags(&err, false, false, "typed");
        assert_eq!(calls, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_refusal_code_at_500_stays_known_control() {
    let body = json!({"code": "invalid_argument", "message": "m"}).to_string();
    let (err, calls) = call("write", 500, JSON, &body, None).await;
    let err = err.expect("error");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "InvalidArgument"),
        "{err:?}"
    );
    assert_flags(&err, false, false, "connect at 500");
    assert_eq!(calls, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn backoff_detail_without_a_code_stays_a_rate_limit_control() {
    let backoff = RateLimitDetail {
        retry_after_ms: Some(1250),
        ..Default::default()
    };
    let nested = ErrorDetail {
        rate_limit: backoff.clone().into(),
        ..Default::default()
    }
    .encode_to_bytes();
    let standalone = backoff.encode_to_bytes();
    for body in [
        detail_body("orders.v1.ErrorDetail", &nested, Some("resource_exhausted")),
        detail_body(
            "polyester.ratelimit.v1.RateLimitDetail",
            &standalone,
            Some("resource_exhausted"),
        ),
    ] {
        let (err, calls) = call("write", 429, JSON, &body, None).await;
        let err = err.expect("error");
        assert!(matches!(err, Error::RateLimit { .. }), "{err:?}");
        assert_eq!(err.retry_after(), Some(1.25), "{err:?}");
        assert_flags(&err, true, false, "backoff");
        assert_eq!(calls, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn placeholder_detail_keeps_field_violations_on_a_recognised_code() {
    let raw = ErrorDetail {
        violations: vec![FieldViolation {
            field_path: "x".into(),
            rule_id: "r".into(),
            message: "v".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_bytes();
    let body = detail_body("orders.v1.ErrorDetail", &raw, Some("invalid_argument"));
    let (err, calls) = call("write", 400, JSON, &body, None).await;
    let err = err.expect("error");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "InvalidArgument"),
        "{err:?}"
    );
    assert_eq!(err.violations().len(), 1, "{err:?}");
    assert_eq!(err.violations()[0].field_path, "x");
    assert_flags(&err, false, false, "violations");
    assert_eq!(calls, 1);
    let body = detail_body("orders.v1.ErrorDetail", &raw, None);
    expect_server("write", 500, JSON, &body, "violations at 500").await;
}

/// An encoding this client can't decode leaves no readable error. Whatever the
/// body says, only a 401 or 403 is known, on its status alone.
#[tokio::test(flavor = "multi_thread")]
async fn body_the_client_cannot_decode_is_not_a_known_failure() {
    for encoding in ["br", "zstd", "deflate", "bogus"] {
        for status in [
            400, 401, 403, 404, 409, 412, 422, 429, 500, 501, 502, 503, 504, 520,
        ] {
            let case = format!("{encoding}/{status}");
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let http = MockHttpServer::spawn(move |req| {
                if req.path != WRITE_PATH {
                    return HttpScript::NotFound;
                }
                seen.fetch_add(1, Ordering::SeqCst);
                let body = br#"{"code":"BAD_PRICE"}"#.to_vec();
                HttpScript::Raw {
                    status,
                    headers: vec![
                        ("Content-Type".into(), PROBLEM.into()),
                        ("Content-Encoding".into(), encoding.into()),
                        ("Content-Length".into(), body.len().to_string()),
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
            client
                .catalogs
                .hydrate_spot_config_json(json!({"pairs": [
                    {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}
                ]}))
                .expect("catalog");
            let mut params =
                CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
            params.quantity = Some(
                Quantity::from_scaled(
                    1,
                    Some(8),
                    QuantityDomain::OrderBase,
                    Some("BTC-USDT".into()),
                    Some(1),
                )
                .expect("quantity"),
            );
            params.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).expect("price"));
            params.time_in_force = Some(CreateTimeInForce::Gtc);
            let err = client
                .orders
                .create(params)
                .await
                .expect_err("a failed create");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{case}");
            assert!(!err.is_retryable(), "{case}: {err:?}");
            let definite = matches!(status, 401 | 403)
                && matches!(err, Error::Auth(_) | Error::ApiKeyRejected { .. });
            assert!(
                definite || err.mutation_outcome_unknown(),
                "{case}: {err:?}"
            );
        }
    }
}
