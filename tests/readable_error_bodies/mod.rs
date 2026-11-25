//! An HTTP error with no recognised code proves nothing about a mutation.
//!
//! Each case goes through a public read and a public create against a local
//! server. Only the requested procedure is counted and every expectation needs
//! at least one request, so a case that never reached the server fails.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::models::SubAccountScope;
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
    call_with_headers(op, status, content_type, body, fail_for, &[]).await
}

/// [`call`], with these extra response headers on each failure.
async fn call_with_headers(
    op: &str,
    status: u16,
    content_type: &str,
    body: &str,
    fail_for: Option<usize>,
    extra_headers: &[(&str, &str)],
) -> (Option<Error>, usize) {
    let extra_headers: Vec<(String, String)> = extra_headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
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
        let mut headers = vec![
            ("Content-Type".into(), content_type.clone()),
            ("Content-Length".into(), body.len().to_string()),
        ];
        headers.extend(extra_headers.iter().cloned());
        HttpScript::Raw {
            status,
            headers,
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
        client
            .triggers
            .get_by_id("9", SubAccountScope::Default)
            .await
            .err()
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
                assert!(matches!(err, Error::Server { .. }), "{case}: {err:?}");
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
            assert!(matches!(err, Error::Server { .. }), "{case}: {err:?}");
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
                    matches!(err, Error::Auth { .. } | Error::ApiKeyRejected { .. }),
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
                matches!(err, Error::Server { .. } | Error::GatewayTimeout { .. }),
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

/// A temporary failure answers REST 503 with its problem `code` (it was 502
/// or 504 on many routes): it reads as ConnectRPC `unavailable` does, and a
/// 503 whose code names a deadline as ConnectRPC `deadline_exceeded` does.
/// Either way a create is sent once and its outcome is unknown, and a read
/// is retried. A 504, whatever its body, stays an unknown outcome.
#[tokio::test(flavor = "multi_thread")]
async fn rest_503_problems_read_as_connect_unavailable_and_504_stays_unknown() {
    let connect_unavailable = r#"{"code":"unavailable","message":"m"}"#;
    let connect_deadline = r#"{"code":"deadline_exceeded","message":"m"}"#;
    let cases = [
        (503, "SERVICE_UNAVAILABLE", connect_unavailable),
        (503, "UPSTREAM_ERROR", connect_unavailable),
        (503, "WALLET_RESOLUTION_UNAVAILABLE", connect_unavailable),
        (503, "DEADLINE_EXCEEDED", connect_deadline),
        (503, "deadline_exceeded", connect_deadline),
        (504, "SERVICE_UNAVAILABLE", connect_deadline),
        (504, "UPSTREAM_ERROR", connect_deadline),
    ];
    for (status, code, connect_body) in cases {
        let case = format!("{status}/{code}");
        let body = json!({
            "type": "https://polyester.dev/problems/service-unavailable",
            "title": "Service Unavailable",
            "status": status,
            "code": code,
            "detail": "request_deadline_exceeded: d",
        })
        .to_string();
        let (err, calls) = call("write", status, PROBLEM, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
        assert!(
            matches!(
                err,
                Error::Server { .. } | Error::Transport { .. } | Error::GatewayTimeout { .. }
            ),
            "{case}: {err:?}"
        );
        assert_flags(&err, false, true, &case);
        assert_eq!(calls, 1, "{case}: a create is sent once");
        let (connect_err, _) = call("write", status, JSON, connect_body, None).await;
        let connect_err = connect_err.expect("connect error");
        assert_eq!(
            (err.is_retryable(), err.mutation_outcome_unknown()),
            (
                connect_err.is_retryable(),
                connect_err.mutation_outcome_unknown()
            ),
            "{case}: reads as {connect_err:?}"
        );
        let (err, calls) = call("read", status, PROBLEM, &body, Some(2)).await;
        assert!(err.is_none(), "{case}: {err:?}");
        assert_eq!(calls, 3, "{case}: a read succeeds on its third request");
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
                assert!(matches!(err, Error::Server { .. }), "{case}: {err:?}");
                assert_flags(&err, false, true, &case);
            }
            504 => assert_flags(&err, false, true, &case),
            s if s < 500 => {
                assert!(
                    !matches!(err, Error::Server { .. } | Error::RouteNotFound { .. }),
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
        matches!(err, Error::Server { .. } | Error::GatewayTimeout { .. }),
        "{case}: {err:?}"
    );
    assert_flags(&err, false, true, case);
    assert_eq!(calls, 1, "{case}");
}

async fn expect_auth(status: u16, content_type: &str, body: &str, case: &str) {
    let (err, calls) = call("write", status, content_type, body, None).await;
    let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
    assert!(
        matches!(err, Error::Auth { .. } | Error::ApiKeyRejected { .. }),
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
                && matches!(err, Error::Auth { .. } | Error::ApiKeyRejected { .. });
            assert!(
                definite || err.mutation_outcome_unknown(),
                "{case}: {err:?}"
            );
        }
    }
}

/// Code 999999 in the first field of every coded detail: an enum this SDK can't
/// name. It could be a future refusal or a future unknown outcome.
const FUTURE_CODE: [u8; 4] = [0x08, 0xBF, 0x84, 0x3D];
const CODED_DETAIL_TYPES: [&str; 5] = [
    "orders.v1.ErrorDetail",
    "auth.v1.AuthErrorDetail",
    "chain.withdraw.v1.ErrorDetail",
    "transfer.v1.ErrorDetail",
    "ledger.read.v1.ErrorDetail",
];

#[tokio::test(flavor = "multi_thread")]
async fn future_typed_code_with_no_connect_code_proves_nothing() {
    for detail_type in CODED_DETAIL_TYPES {
        let body = detail_body(detail_type, &FUTURE_CODE, None);
        for status in [400, 404, 409, 500, 501, 520] {
            for op in OPS {
                let case = format!("{detail_type}/{status}/{op}");
                expect_server(op, status, JSON, &body, &case).await;
                let (err, _) = call(op, status, JSON, &body, None).await;
                let err = err.expect("error");
                assert!(err.to_string().contains("999999"), "{case}: {err}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_of_a_future_typed_code_on_a_server_failure() {
    for detail_type in CODED_DETAIL_TYPES {
        for code in ["internal", "unknown", "data_loss"] {
            let body = detail_body(detail_type, &FUTURE_CODE, Some(code));
            let case = format!("{detail_type}/{code}");
            expect_server("read", 500, JSON, &body, &case).await;
        }
        let body = detail_body(detail_type, &FUTURE_CODE, Some("unavailable"));
        let (err, calls) = call("read", 503, JSON, &body, Some(2)).await;
        assert!(err.is_none(), "{detail_type}: {err:?}");
        assert_eq!(calls, 3, "{detail_type}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn future_typed_code_does_not_turn_a_server_failure_into_a_refusal() {
    for detail_type in CODED_DETAIL_TYPES {
        for (status, code) in [
            (500, "internal"),
            (500, "unknown"),
            (500, "data_loss"),
            (500, "canceled"),
            (503, "unavailable"),
            (504, "deadline_exceeded"),
        ] {
            let body = detail_body(detail_type, &FUTURE_CODE, Some(code));
            let case = format!("{detail_type}/{code}");
            let (err, calls) = call("write", status, JSON, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(!matches!(err, Error::Api { .. }), "{case}: {err:?}");
            if matches!(err, Error::Server { .. }) {
                assert!(err.to_string().contains("999999"), "{case}: {err}");
            }
            assert_flags(&err, false, true, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn future_typed_code_with_backoff_is_not_retryable_on_a_server_failure() {
    let backoff = RateLimitDetail {
        retry_after_ms: Some(1250),
        ..Default::default()
    }
    .encode_to_bytes();
    // Field 3 of orders.v1.ErrorDetail carries the backoff.
    let mut raw = FUTURE_CODE.to_vec();
    raw.extend([0x1A, backoff.len() as u8]);
    raw.extend_from_slice(&backoff);
    for code in [None, Some("internal")] {
        let body = detail_body("orders.v1.ErrorDetail", &raw, code);
        expect_server("write", 500, JSON, &body, &format!("backoff/{code:?}")).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn future_typed_code_keeps_its_label_on_a_recognised_refusal_control() {
    for detail_type in CODED_DETAIL_TYPES {
        for (status, code) in [
            (400, "failed_precondition"),
            (400, "invalid_argument"),
            (409, "already_exists"),
        ] {
            let body = detail_body(detail_type, &FUTURE_CODE, Some(code));
            let case = format!("{detail_type}/{code}");
            let (err, calls) = call("write", status, JSON, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert!(
                matches!(&err, Error::Api { code, .. } if code.contains("999999")),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

/// A REST 403 that names a service refusal denies the operation,
/// so it is a permission refusal, not a rejected API key.
#[tokio::test(flavor = "multi_thread")]
async fn rest_service_refusal_is_a_permission_refusal() {
    for code in ["SUBACCOUNT_READ_FORBIDDEN", "QUICKSWAP_NOT_PERMITTED"] {
        for op in OPS {
            let case = format!("{code}/{op}");
            let body = json!({"code": code, "detail": "not allowed"}).to_string();
            let (err, calls) = call(op, 403, PROBLEM, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            let path = if op == "read" { READ_PATH } else { WRITE_PATH };
            match &err {
                Error::PermissionDenied {
                    status,
                    code: denied,
                    message,
                    context,
                    endpoint,
                    ..
                } => {
                    assert_eq!(
                        (*status, denied.as_str(), message.as_str()),
                        (403, code, "not allowed"),
                        "{case}"
                    );
                    assert_eq!(endpoint, path, "{case}");
                    assert_eq!(context, path.trim_start_matches('/'), "{case}");
                }
                other => panic!("{case}: expected PermissionDenied, got {other:?}"),
            }
            assert_eq!(err.auth_code(), None, "{case}");
            assert_eq!(
                err.http_problem().map(|problem| problem.code),
                Some(code.to_owned()),
                "{case}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
}

/// Control: a credential refusal stays a rejected API key, at 401
/// or 403.
#[tokio::test(flavor = "multi_thread")]
async fn credential_refusal_stays_api_key_rejected_control() {
    for (status, code) in [
        (401, "IP_NOT_ALLOWED"),
        (403, "IP_NOT_ALLOWED"),
        (401, "SIGNATURE_INVALID"),
        (403, "KEY_DISABLED"),
    ] {
        let case = format!("{status}/{code}");
        let body = json!({"code": code, "detail": "d"}).to_string();
        let (err, calls) = call("write", status, PROBLEM, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
        assert!(
            matches!(&err, Error::ApiKeyRejected { status: s, code: c, .. } if *s == status && c == code),
            "{case}: {err:?}"
        );
        assert_eq!(
            err.auth_code(),
            polyester::errors::auth_codes::from_rest_code(code),
            "{case}"
        );
        assert_flags(&err, false, false, &case);
        assert_eq!(calls, 1, "{case}");
    }
}

/// The documented HTTP 400 `MISSING_HEADERS` problem keeps its
/// auth reason, as the HTTP 401 one does.
#[tokio::test(flavor = "multi_thread")]
async fn missing_headers_keeps_its_auth_reason_at_400() {
    use polyester::proto::auth::v1::AuthErrorCode;

    for status in [400, 401] {
        for op in OPS {
            let case = format!("{status}/{op}");
            let body = json!({"code": "MISSING_HEADERS", "detail": "d"}).to_string();
            let (err, calls) = call(op, status, PROBLEM, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert_eq!(
                err.auth_code(),
                Some(AuthErrorCode::AUTH_MISSING_HEADERS),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
            assert_eq!(calls, 1, "{case}");
        }
    }
    // Control: the documented 400 skew problem keeps its typed reason too.
    let body = json!({"code": "TIMESTAMP_SKEW", "detail": "d"}).to_string();
    let (err, _) = call("write", 400, PROBLEM, &body, None).await;
    let err = err.expect("skew error");
    assert_eq!(err.auth_code(), Some(AuthErrorCode::AUTH_TIMESTAMP_SKEW));
}

/// An order permission refusal is the same `Error::PermissionDenied`, with
/// the same code spelling, status, message and endpoint, whether the API
/// answers it as a REST problem or as a ConnectRPC `permission_denied` with
/// an `orders.v1.ErrorDetail`: the API key's own refusals (78, 27, 29)
/// exactly as the sub-account policy's (23, 20) and the other order
/// permission codes. None is retryable or leaves an outcome unknown.
#[tokio::test(flavor = "multi_thread")]
async fn order_permission_refusals_match_across_transports() {
    let message = "The API key's policy does not allow spot trading.";
    for (name, number) in [
        ("POLICY_MARKET_DENY", 20),
        ("POLICY_SPOT_TRADE_DENY", 23),
        ("API_KEY_POLICY_REQUIRED", 27),
        ("API_KEY_MARKET_DENY", 29),
        ("API_KEY_SPOT_TRADE_DENY", 78),
        ("POLICY_SPOT_READ_DENY", 71),
        ("SUBACCOUNT_READ_FORBIDDEN", 70),
        ("QUICKSWAP_NOT_PERMITTED", 203),
    ] {
        let code =
            <OrderErrorCode as buffa::Enumeration>::from_proto_name(&format!("ERROR_CODE_{name}"))
                .unwrap_or_else(|| panic!("{name}"));
        assert_eq!(buffa::Enumeration::to_i32(&code), number, "{name}");
        let raw = ErrorDetail {
            code: code.into(),
            ..Default::default()
        }
        .encode_to_bytes();
        let connect = json!({
            "code": "permission_denied",
            "message": message,
            "details": [{
                "type": "orders.v1.ErrorDetail",
                "value": base64::engine::general_purpose::STANDARD.encode(&raw),
            }],
        })
        .to_string();
        let rest = json!({"code": name, "detail": message}).to_string();
        for op in OPS {
            let mut seen = Vec::new();
            for (transport, content_type, body) in
                [("connect", JSON, &connect), ("rest", PROBLEM, &rest)]
            {
                let case = format!("{name}/{op}/{transport}");
                let (err, calls) = call(op, 403, content_type, body, None).await;
                let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
                assert_eq!(calls, 1, "{case}");
                assert_flags(&err, false, false, &case);
                match err {
                    Error::PermissionDenied {
                        message,
                        status,
                        code,
                        context,
                        endpoint,
                        ..
                    } => seen.push((code, status, message, context, endpoint)),
                    other => panic!("{case}: expected PermissionDenied, got {other:?}"),
                }
            }
            let path = if op == "read" { READ_PATH } else { WRITE_PATH };
            assert_eq!(
                seen[0],
                (
                    name.to_owned(),
                    403,
                    message.to_owned(),
                    path.trim_start_matches('/').to_owned(),
                    path.to_owned()
                ),
                "{name}/{op}"
            );
            assert_eq!(seen[0], seen[1], "{name}/{op}: transports differ");
        }
    }
}

/// A trigger change refused because the trigger has already ended is
/// reported as ended over either transport; a trigger that is not found is
/// not, since that code also means a wrong ID.
#[tokio::test(flavor = "multi_thread")]
async fn trigger_already_ended_over_both_transports() {
    for (name, status, connect_code, ended) in [
        ("TRIGGER_STATUS_INVALID", 400, "failed_precondition", true),
        ("TRIGGER_NOT_FOUND", 404, "not_found", false),
    ] {
        let code =
            <OrderErrorCode as buffa::Enumeration>::from_proto_name(&format!("ERROR_CODE_{name}"))
                .unwrap();
        let raw = ErrorDetail {
            code: code.into(),
            ..Default::default()
        }
        .encode_to_bytes();
        let mut connect: Value = serde_json::from_str(&detail_body(
            "orders.v1.ErrorDetail",
            &raw,
            Some(connect_code),
        ))
        .unwrap();
        connect["message"] = json!("Trigger has already ended");
        let rest = json!({"code": name, "detail": "Trigger has already ended"}).to_string();
        for (transport, content_type, body) in [
            ("connect", JSON, connect.to_string()),
            ("rest", PROBLEM, rest),
        ] {
            let case = format!("{name}/{transport}");
            let (err, calls) = call("write", status, content_type, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert_eq!(calls, 1, "{case}");
            assert_eq!(err.is_trigger_already_ended(), ended, "{case}: {err:?}");
            assert!(
                matches!(&err, Error::Api { code: found, .. } if *found == format!("ERROR_CODE_{name}")),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
        }
    }
}

/// A credential refusal reads the same whichever REST endpoint answers it:
/// the bare documented code at its documented status, never an `AUTH_`
/// prefixed alias or a reason hidden in `detail`. Authentication that is
/// temporarily unavailable (503) and a full replay record (429) refused the
/// request before it was processed, so both are retryable and certain.
#[tokio::test(flavor = "multi_thread")]
async fn credential_refusals_read_the_same_on_every_rest_endpoint() {
    use polyester::proto::auth::v1::AuthErrorCode as Auth;
    let cases: [(u16, &str, Auth, bool); 10] = [
        (
            401,
            "UNAUTHENTICATED",
            Auth::AUTH_AUTHENTICATION_REQUIRED,
            false,
        ),
        (401, "MISSING_HEADERS", Auth::AUTH_MISSING_HEADERS, false),
        (
            401,
            "SIGNATURE_INVALID",
            Auth::AUTH_SIGNATURE_INVALID,
            false,
        ),
        (401, "TIMESTAMP_SKEW", Auth::AUTH_TIMESTAMP_SKEW, true),
        (
            401,
            "RECV_WINDOW_INVALID",
            Auth::AUTH_RECV_WINDOW_INVALID,
            false,
        ),
        (401, "KEY_EXPIRED", Auth::AUTH_KEY_EXPIRED, false),
        (401, "KEY_DISABLED", Auth::AUTH_KEY_DISABLED, false),
        (401, "IP_NOT_ALLOWED", Auth::AUTH_IP_NOT_ALLOWED, false),
        (
            503,
            "AUTH_TEMPORARILY_UNAVAILABLE",
            Auth::AUTH_TEMPORARILY_UNAVAILABLE,
            true,
        ),
        (
            429,
            "REPLAY_CAPACITY_EXHAUSTED",
            Auth::AUTH_REPLAY_CAPACITY_EXHAUSTED,
            true,
        ),
    ];
    for (status, code, auth, retryable) in cases {
        for op in OPS {
            let case = format!("{op} {status}/{code}");
            let body =
                json!({"code": code, "title": "t", "detail": "d", "status": status}).to_string();
            let (err, calls) = call(op, status, PROBLEM, &body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            // A trigger read retries an unavailable answer until its budget
            // is spent; every other refusal, and any write, is sent once.
            if op == "read" && code == "AUTH_TEMPORARILY_UNAVAILABLE" {
                assert!(calls > 1, "{case}: calls={calls}");
            } else {
                assert_eq!(calls, 1, "{case}");
            }
            assert_eq!(err.auth_code(), Some(auth), "{case}: {err:?}");
            assert_flags(&err, retryable, false, &case);
        }
    }
}

/// Authentication that was briefly unavailable refused a trigger read before
/// it ran: the read retries it, over REST and ConnectRPC, and succeeds once
/// authentication answers. A write gets the retryable refusal after one
/// request.
#[tokio::test(flavor = "multi_thread")]
async fn trigger_reads_recover_from_transient_auth_unavailability() {
    use polyester::proto::auth::v1::{AuthErrorCode as Auth, AuthErrorDetail};
    let rest =
        json!({"code": "AUTH_TEMPORARILY_UNAVAILABLE", "title": "t", "detail": "d", "status": 503})
            .to_string();
    let detail = AuthErrorDetail {
        code: Auth::AUTH_TEMPORARILY_UNAVAILABLE.into(),
        ..Default::default()
    };
    let connect = json!({
        "code": "unavailable",
        "message": "authentication temporarily unavailable",
        "details": [{
            "type": "auth.v1.AuthErrorDetail",
            "value": base64::engine::general_purpose::STANDARD.encode(detail.encode_to_bytes()),
        }],
    })
    .to_string();
    for (shape, content_type, body) in [
        ("rest", PROBLEM, rest.as_str()),
        ("connect", JSON, connect.as_str()),
    ] {
        let (err, calls) = call("read", 503, content_type, body, Some(1)).await;
        assert!(err.is_none(), "{shape}: {err:?}");
        assert_eq!(calls, 2, "{shape}");
        let (err, calls) = call("write", 503, content_type, body, None).await;
        let err = err.unwrap_or_else(|| panic!("{shape}: expected an error"));
        assert_eq!(calls, 1, "{shape} write");
        assert_eq!(
            err.auth_code(),
            Some(Auth::AUTH_TEMPORARILY_UNAVAILABLE),
            "{shape}: {err:?}"
        );
        assert_flags(&err, true, false, shape);
    }
}

/// A REST path the API does not serve answers a `NOT_FOUND` problem: the
/// request was refused, not lost.
#[tokio::test(flavor = "multi_thread")]
async fn not_found_problem_keeps_its_code() {
    for op in OPS {
        let body = json!({"code": "NOT_FOUND", "title": "Not Found", "detail": "No API operation matches this method and path.", "status": 404}).to_string();
        let (err, calls) = call(op, 404, PROBLEM, &body, None).await;
        let err = err.expect("error");
        assert_eq!(calls, 1, "{op}");
        assert!(
            matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_NOT_FOUND"),
            "{op}: {err:?}"
        );
        assert_flags(&err, false, false, op);
    }
}

/// A trailing stop that already tracks refuses a new activation price with
/// `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE` (315) on both
/// transports: a final refusal that changed nothing.
#[tokio::test(flavor = "multi_thread")]
async fn trigger_activation_price_not_modifiable_is_final_on_both_transports() {
    const CODE: &str = "ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE";
    let detail = ErrorDetail {
        code: OrderErrorCode::ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE.into(),
        ..Default::default()
    };
    let connect = detail_body(
        "orders.v1.ErrorDetail",
        &detail.encode_to_bytes(),
        Some("failed_precondition"),
    );
    let rest =
        json!({"code": "TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE", "detail": "d", "status": 400})
            .to_string();
    for op in OPS {
        for (transport, content_type, body) in
            [("connect", JSON, &connect), ("rest", PROBLEM, &rest)]
        {
            let case = format!("{op} {transport}");
            let (err, calls) = call(op, 400, content_type, body, None).await;
            let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
            assert_eq!(calls, 1, "{case}");
            assert!(
                matches!(&err, Error::Api { code, .. } if code == CODE),
                "{case}: {err:?}"
            );
            assert_flags(&err, false, false, &case);
        }
    }
    assert_eq!(
        OrderErrorCode::ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE as i32,
        315
    );
}

/// A 504 is an unknown outcome whatever retryable code its body names; safe
/// reads still retry it. At 429 the same codes stay retryable refusals.
#[tokio::test(flavor = "multi_thread")]
async fn gateway_timeout_with_a_retryable_code_stays_unknown() {
    let bodies: [Body; 10] = [
        (
            "rate-limit",
            PROBLEM,
            r#"{"code":"RATE_LIMIT_EXCEEDED","rateLimit":{"reason":"QUOTA_EXCEEDED","retryAfterMs":"1500"}}"#,
        ),
        (
            "rate-limit-authority",
            PROBLEM,
            r#"{"code":"RATE_LIMIT_AUTHORITY_UNAVAILABLE"}"#,
        ),
        ("overloaded", PROBLEM, r#"{"code":"OVERLOADED"}"#),
        (
            "system-rate-limited",
            PROBLEM,
            r#"{"code":"SYSTEM_RATE_LIMITED"}"#,
        ),
        ("replay", PROBLEM, r#"{"code":"REPLAY_CAPACITY_EXHAUSTED"}"#),
        (
            "auth-replay",
            PROBLEM,
            r#"{"code":"AUTH_REPLAY_CAPACITY_EXHAUSTED"}"#,
        ),
        (
            "auth-unavailable",
            PROBLEM,
            r#"{"code":"AUTH_TEMPORARILY_UNAVAILABLE"}"#,
        ),
        ("skew", PROBLEM, r#"{"code":"TIMESTAMP_SKEW"}"#),
        (
            "sequence",
            PROBLEM,
            r#"{"code":"ORDER_SEQUENCE_EXHAUSTED"}"#,
        ),
        (
            "connect-resource-exhausted",
            JSON,
            r#"{"code":"resource_exhausted","message":"rate limit exceeded, retry later"}"#,
        ),
    ];
    for (case, content_type, body) in bodies {
        let (err, calls) = call_with_headers(
            "write",
            504,
            content_type,
            body,
            None,
            &[("Retry-After", "2")],
        )
        .await;
        let err = err.expect(case);
        assert!(
            matches!(err, Error::GatewayTimeout { .. }),
            "{case}: {err:?}"
        );
        assert_flags(&err, false, true, case);
        assert_eq!(calls, 1, "{case}");
        let (err, calls) = call("read", 504, content_type, body, Some(1)).await;
        assert!(err.is_none(), "{case}: a safe read retries it: {err:?}");
        assert_eq!(calls, 2, "{case}");
    }
    for body in [
        r#"{"code":"RATE_LIMIT_EXCEEDED","rateLimit":{"reason":"QUOTA_EXCEEDED"}}"#,
        r#"{"code":"AUTH_REPLAY_CAPACITY_EXHAUSTED"}"#,
    ] {
        let (err, calls) = call("write", 429, PROBLEM, body, None).await;
        let err = err.expect("429");
        assert!(
            matches!(err, Error::RateLimit { .. }),
            "429 {body}: {err:?}"
        );
        assert_flags(&err, true, false, body);
        assert_eq!(calls, 1);
    }
    let (err, _) = call(
        "write",
        504,
        PROBLEM,
        r#"{"code":"ORDER_CREATE_OUTCOME_UNKNOWN","detail":"d"}"#,
        None,
    )
    .await;
    let err = err.expect("outcome unknown");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN"),
        "{err:?}"
    );
    assert!(err.mutation_outcome_unknown());
}

/// A 503 keeps its code and Retry-After, on a REST problem and a ConnectRPC
/// unavailable answer.
#[tokio::test(flavor = "multi_thread")]
async fn service_unavailable_keeps_its_code_and_retry_after() {
    let (err, _) = call_with_headers(
        "write",
        503,
        PROBLEM,
        r#"{"code":"UPSTREAM_ERROR","detail":"upstream_unavailable","status":503}"#,
        None,
        &[("Retry-After", "30")],
    )
    .await;
    let err = err.expect("503");
    assert!(
        matches!(
            &err,
            Error::Server { code: Some(code), answer: Some(answer), .. }
                if code == "UPSTREAM_ERROR" && answer.status == 503 && answer.retry_after == Some(30.0)
        ),
        "{err:?}"
    );
    assert_eq!(err.retry_after(), Some(30.0));
    assert_flags(&err, false, true, "503");
    let (err, _) = call_with_headers(
        "write",
        503,
        JSON,
        r#"{"code":"unavailable","message":"temporarily unavailable"}"#,
        None,
        &[("Retry-After", "7")],
    )
    .await;
    let err = err.expect("connect unavailable");
    assert!(matches!(err, Error::Server { .. }), "{err:?}");
    assert_eq!(err.retry_after(), Some(7.0), "{err:?}");
    let (err, _) = call(
        "write",
        503,
        PROBLEM,
        r#"{"code":"SERVICE_UNAVAILABLE"}"#,
        None,
    )
    .await;
    assert_eq!(err.expect("no header").retry_after(), None);
}

/// A safe read waits at least the Retry-After it was asked for, and returns
/// at once when that would end past its retry budget.
#[tokio::test(flavor = "multi_thread")]
async fn read_retry_honours_retry_after() {
    let body = r#"{"code":"UPSTREAM_ERROR","status":503}"#;
    let started = std::time::Instant::now();
    let (err, calls) = call_with_headers(
        "read",
        503,
        PROBLEM,
        body,
        Some(1),
        &[("Retry-After", "30")],
    )
    .await;
    assert!(started.elapsed() < std::time::Duration::from_millis(1500));
    assert_eq!(calls, 1);
    assert_eq!(err.expect("past the budget").retry_after(), Some(30.0));
    let started = std::time::Instant::now();
    let (err, calls) = call_with_headers(
        "read",
        503,
        PROBLEM,
        body,
        Some(1),
        &[("Retry-After", "0.4")],
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(calls, 2);
    assert!(started.elapsed() >= std::time::Duration::from_millis(400));
}

/// The HTTP status decides outcome certainty first: a 504 is an unknown
/// outcome before any message, code or typed detail is read. The review's
/// probes: ConnectRPC bodies at 504 whose message or detail names a retryable
/// refusal, and plain server failures.
#[tokio::test(flavor = "multi_thread")]
async fn round3_connect_bodies_at_504_are_unknown() {
    let rate = ErrorDetail {
        code: OrderErrorCode::ERROR_CODE_RATE_LIMIT_EXCEEDED.into(),
        ..Default::default()
    }
    .encode_to_bytes();
    let limit = RateLimitDetail {
        retry_after_ms: Some(1500),
        ..Default::default()
    }
    .encode_to_bytes();
    let bodies = [
        (
            "unavailable+skew",
            json!({"code": "unavailable", "message": "TIMESTAMP_SKEW"}).to_string(),
        ),
        (
            "unavailable+rate detail",
            detail_body("orders.v1.ErrorDetail", &rate, Some("unavailable")),
        ),
        (
            "unavailable+limit detail",
            detail_body(
                "polyester.ratelimit.v1.RateLimitDetail",
                &limit,
                Some("unavailable"),
            ),
        ),
        (
            "resource_exhausted+rate detail",
            detail_body("orders.v1.ErrorDetail", &rate, Some("resource_exhausted")),
        ),
        (
            "unavailable+auth unavailable",
            json!({"code": "unavailable", "message": "AUTH_TEMPORARILY_UNAVAILABLE"}).to_string(),
        ),
        (
            "unavailable",
            json!({"code": "unavailable", "message": "temporarily unavailable"}).to_string(),
        ),
        (
            "internal",
            json!({"code": "internal", "message": "boom"}).to_string(),
        ),
        (
            "unknown",
            json!({"code": "unknown", "message": "?"}).to_string(),
        ),
    ];
    for (case, body) in &bodies {
        let (err, calls) = call("write", 504, JSON, body, None).await;
        let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
        assert_flags(&err, false, true, case);
        assert_eq!(calls, 1, "{case}");
        let (err, calls) = call("read", 504, JSON, body, Some(1)).await;
        assert!(err.is_none(), "{case}: a safe read retries it: {err:?}");
        assert_eq!(calls, 2, "{case}");
    }
    // An outcome-unknown code at 504 keeps its code even beside a rate-limit
    // detail, and stays unknown.
    let mut body: Value = serde_json::from_str(&detail_body(
        "polyester.ratelimit.v1.RateLimitDetail",
        &limit,
        Some("resource_exhausted"),
    ))
    .expect("json");
    body["message"] = json!("ORDER_CREATE_OUTCOME_UNKNOWN");
    let (err, _) = call("write", 504, JSON, &body.to_string(), None).await;
    let err = err.expect("outcome unknown");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN"),
        "{err:?}"
    );
    assert_flags(
        &err,
        false,
        true,
        "outcome unknown beside a rate-limit detail",
    );
}

/// Retry-After is captured once for every answer and carried by whatever
/// error it maps to, an authentication refusal included.
#[tokio::test(flavor = "multi_thread")]
async fn round3_retry_after_on_every_answer() {
    for (status, content_type, body) in [
        (
            503,
            PROBLEM,
            r#"{"code":"AUTH_TEMPORARILY_UNAVAILABLE","status":503}"#,
        ),
        (
            503,
            PROBLEM,
            r#"{"code":"ACCOUNT_ROUTING_INVALID","status":503}"#,
        ),
        (503, PROBLEM, r#"{"code":"PAIR_BACKLOGGED","status":503}"#),
        (
            503,
            JSON,
            r#"{"code":"unavailable","message":"AUTH_TEMPORARILY_UNAVAILABLE"}"#,
        ),
        (
            429,
            PROBLEM,
            r#"{"code":"RATE_LIMIT_EXCEEDED","status":429}"#,
        ),
        (
            504,
            PROBLEM,
            r#"{"code":"ORDER_CREATE_OUTCOME_UNKNOWN","status":504}"#,
        ),
    ] {
        let (err, _) = call_with_headers(
            "write",
            status,
            content_type,
            body,
            None,
            &[("Retry-After", "30")],
        )
        .await;
        let err = err.unwrap_or_else(|| panic!("{status} {body}: expected an error"));
        assert_eq!(err.retry_after(), Some(30.0), "{status} {body}: {err:?}");
    }
}

/// An authentication refusal that changed nothing waits for its Retry-After
/// too, or gives up when that would end past the budget.
#[tokio::test(flavor = "multi_thread")]
async fn round3_auth_unavailable_read_retry_honours_retry_after() {
    let body = r#"{"code":"AUTH_TEMPORARILY_UNAVAILABLE","status":503}"#;
    let started = std::time::Instant::now();
    let (err, calls) = call_with_headers(
        "read",
        503,
        PROBLEM,
        body,
        Some(1),
        &[("Retry-After", "30")],
    )
    .await;
    assert!(started.elapsed() < std::time::Duration::from_millis(1500));
    assert_eq!(calls, 1);
    assert_eq!(err.expect("past the budget").retry_after(), Some(30.0));
    let started = std::time::Instant::now();
    let (err, calls) = call_with_headers(
        "read",
        503,
        PROBLEM,
        body,
        Some(1),
        &[("Retry-After", "0.4")],
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(calls, 2);
    assert!(started.elapsed() >= std::time::Duration::from_millis(400));
}

/// ConnectRPC answers a server-side deadline_exceeded with HTTP 504: the
/// request's deadline, whatever its message says, never a refusal.
#[tokio::test(flavor = "multi_thread")]
async fn round3_connect_deadline_at_504_is_the_deadline() {
    for message in ["context deadline exceeded", "TIMESTAMP_SKEW"] {
        let body = json!({"code": "deadline_exceeded", "message": message}).to_string();
        let (err, calls) = call("write", 504, JSON, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{message}: expected an error"));
        assert!(matches!(err, Error::Transport { .. }), "{message}: {err:?}");
        assert_flags(&err, false, true, message);
        assert_eq!(calls, 1, "{message}");
        let (err, calls) = call("read", 504, JSON, &body, Some(1)).await;
        assert!(err.is_none(), "{message}: a safe read retries it: {err:?}");
        assert_eq!(calls, 2, "{message}");
    }
}

const ROUND4_ANSWERS: [(&str, u16, &str, &str, bool); 8] = [
    (
        "deadline 504",
        504,
        JSON,
        r#"{"code":"deadline_exceeded","message":"context deadline exceeded"}"#,
        true,
    ),
    (
        "auth 503",
        503,
        PROBLEM,
        r#"{"code":"AUTH_TEMPORARILY_UNAVAILABLE","status":503}"#,
        true,
    ),
    (
        "permission denied 403",
        403,
        PROBLEM,
        r#"{"code":"PERMISSION_DENIED","status":403,"detail":"denied"}"#,
        false,
    ),
    (
        "malformed message 503",
        503,
        JSON,
        r#"{"code":"unavailable","message":5}"#,
        true,
    ),
    (
        "malformed message 504",
        504,
        JSON,
        r#"{"code":"unavailable","message":5}"#,
        true,
    ),
    (
        "malformed detail 503",
        503,
        JSON,
        r#"{"code":"unavailable","message":"m","details":"nope"}"#,
        true,
    ),
    (
        "malformed detail 504",
        504,
        JSON,
        r#"{"code":"unavailable","message":"m","details":[{"type":"orders.v1.ErrorDetail","value":"!!"}]}"#,
        true,
    ),
    ("broken json 503", 503, JSON, "{broken", true),
];

/// Every error that came from an answer carries the answer's Retry-After,
/// whatever variant it maps to.
#[tokio::test(flavor = "multi_thread")]
async fn round4_every_answer_carries_its_retry_after() {
    for (name, status, content_type, body, _) in ROUND4_ANSWERS {
        let (err, _) = call_with_headers(
            "write",
            status,
            content_type,
            body,
            None,
            &[("Retry-After", "30")],
        )
        .await;
        let err = err.unwrap_or_else(|| panic!("{name}: expected an error"));
        assert_eq!(err.retry_after(), Some(30.0), "{name}: {err:?}");
    }
}

/// A read retried after any of these answers waits for its Retry-After
/// (0.4 s), and returns at once with the delay on its error when the delay is
/// past the budget (30 s). A refusal is never retried.
#[tokio::test(flavor = "multi_thread")]
async fn round4_read_retry_honours_every_answers_delay() {
    for (name, status, content_type, body, retried) in ROUND4_ANSWERS {
        for delay in ["30", "0.4"] {
            let started = std::time::Instant::now();
            let (err, calls) = call_with_headers(
                "read",
                status,
                content_type,
                body,
                Some(1),
                &[("Retry-After", delay)],
            )
            .await;
            let elapsed = started.elapsed();
            if !retried || delay == "30" {
                assert_eq!(calls, 1, "{name}/{delay}");
                assert!(
                    elapsed < std::time::Duration::from_millis(1500),
                    "{name}/{delay}"
                );
                let err = err.unwrap_or_else(|| panic!("{name}/{delay}: expected an error"));
                assert_eq!(
                    err.retry_after(),
                    Some(delay.parse().expect("delay")),
                    "{name}/{delay}: {err:?}"
                );
            } else {
                assert!(err.is_none(), "{name}/{delay}: {err:?}");
                assert_eq!(calls, 2, "{name}/{delay}");
                assert!(
                    elapsed >= std::time::Duration::from_millis(400),
                    "{name}/{delay}"
                );
            }
        }
    }
}

/// Every error that came from an answer carries the answer's status too; an
/// error with no answer behind it, a refused connection, carries none.
#[tokio::test(flavor = "multi_thread")]
async fn round4_answer_status_and_no_answer() {
    for (name, status, content_type, body, _) in ROUND4_ANSWERS {
        let (err, _) = call_with_headers(
            "write",
            status,
            content_type,
            body,
            None,
            &[("Retry-After", "30")],
        )
        .await;
        let err = err.unwrap_or_else(|| panic!("{name}: expected an error"));
        let answer = err
            .answer()
            .unwrap_or_else(|| panic!("{name}: no answer on {err:?}"));
        assert_eq!(answer.status, status, "{name}: {err:?}");
        assert_eq!(answer.retry_after, Some(30.0), "{name}: {err:?}");
    }
    // Port 1 on the loopback refuses the connection: no answer at all.
    let client = Client::new(Config {
        api_url: "http://127.0.0.1:1".into(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .triggers
        .get_by_id("9", SubAccountScope::Default)
        .await
        .expect_err("refused connection");
    assert!(err.answer().is_none(), "{err:?}");
    assert_eq!(err.retry_after(), None, "{err:?}");
}

/// A final refusal is still an HTTP answer: an authentication refusal at 503
/// and a permission refusal at 403 keep the answer's status and Retry-After.
#[tokio::test(flavor = "multi_thread")]
async fn round4_auth_and_permission_refusals_keep_their_answer() {
    let (err, calls) = call_with_headers(
        "write",
        503,
        JSON,
        r#"{"code":"unauthenticated","message":"temporarily unavailable"}"#,
        None,
        &[("Retry-After", "30")],
    )
    .await;
    let err = err.expect("auth refusal");
    assert!(matches!(err, Error::Auth { .. }), "{err:?}");
    assert_eq!(
        err.answer()
            .map(|answer| (answer.status, answer.retry_after)),
        Some((503, Some(30.0))),
        "{err:?}"
    );
    assert_eq!(err.retry_after(), Some(30.0));
    assert_eq!(calls, 1);
    let (err, calls) = call_with_headers(
        "write",
        403,
        PROBLEM,
        r#"{"code":"SUBACCOUNT_READ_FORBIDDEN","status":403,"detail":"denied"}"#,
        None,
        &[("Retry-After", "30")],
    )
    .await;
    let err = err.expect("permission refusal");
    assert!(matches!(err, Error::PermissionDenied { .. }), "{err:?}");
    assert_eq!(
        err.answer()
            .map(|answer| (answer.status, answer.retry_after)),
        Some((403, Some(30.0))),
        "{err:?}"
    );
    assert_eq!(calls, 1);
}

/// The status an error reports is the answer's own HTTP status, never one its
/// ConnectRPC code implies; the delay rides along.
#[tokio::test(flavor = "multi_thread")]
async fn round5_status_is_the_answers_own() {
    for (status, body) in [
        (503, r#"{"code":"unavailable","message":"busy"}"#),
        (429, r#"{"code":"unavailable","message":"busy"}"#),
        (
            503,
            r#"{"code":"unauthenticated","message":"temporarily unavailable"}"#,
        ),
    ] {
        let (err, _) =
            call_with_headers("write", status, JSON, body, None, &[("Retry-After", "30")]).await;
        let err = err.unwrap_or_else(|| panic!("{status} {body}: expected an error"));
        assert_eq!(
            err.answer()
                .map(|answer| (answer.status, answer.retry_after)),
            Some((status, Some(30.0))),
            "{status} {body}: {err:?}"
        );
    }
}

/// A variant's own status field is the answer's real status as well.
#[tokio::test(flavor = "multi_thread")]
async fn round5_variant_status_is_the_answers_own() {
    let (err, _) = call(
        "write",
        503,
        JSON,
        r#"{"code":"unauthenticated","message":"TIMESTAMP_SKEW"}"#,
        None,
    )
    .await;
    let err = err.expect("skew");
    assert!(
        matches!(err, Error::TimestampSkew { status: 503, .. }),
        "{err:?}"
    );
}

/// One public call (a read or a create) against a server answering its
/// procedure with `script`, with this client timeout. Returns the error, the
/// requests the procedure got and the time the call took.
async fn scripted_call(
    op: &str,
    timeout: std::time::Duration,
    script: impl Fn() -> HttpScript + Send + Sync + 'static,
) -> (Option<Error>, usize, std::time::Duration) {
    let path = if op == "read" { READ_PATH } else { WRITE_PATH };
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path != path {
            return HttpScript::NotFound;
        }
        seen.fetch_add(1, Ordering::SeqCst);
        script()
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout,
        ..Default::default()
    })
    .expect("client");
    client
        .catalogs
        .hydrate_spot_config_json(
            json!({"pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]}),
        )
        .expect("catalog");
    let started = std::time::Instant::now();
    let error = if op == "read" {
        client
            .triggers
            .get_by_id("9", SubAccountScope::Default)
            .await
            .err()
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
    (error, calls.load(Ordering::SeqCst), started.elapsed())
}

fn round6_headers(content_length: usize) -> Vec<(String, String)> {
    vec![
        ("Content-Type".into(), JSON.into()),
        ("Content-Length".into(), content_length.to_string()),
        ("Retry-After".into(), "30".into()),
    ]
}

/// A failed answer's metadata is read from its headers before its body. A
/// 503 with Retry-After 30 whose body is cut short (Content-Length 100, then
/// one byte and a close), is larger than the SDK reads, or is still arriving
/// at the client's deadline leaves an error that carries 503 and 30: one
/// request, and a read returns at once since the delay is past its budget. A
/// refused connection is no answer and carries none.
#[tokio::test(flavor = "multi_thread")]
async fn round6_unreadable_bodies_keep_their_answer() {
    let too_large = polyester::transport::MAX_CONNECT_RESPONSE_BYTES + 1;
    for op in OPS {
        for case in ["truncated", "oversized", "body-deadline"] {
            let script = move || match case {
                "truncated" => HttpScript::Raw {
                    status: 503,
                    headers: round6_headers(100),
                    body: b"{".to_vec(),
                },
                "oversized" => HttpScript::Raw {
                    status: 503,
                    headers: round6_headers(too_large),
                    body: vec![b' '; too_large],
                },
                _ => HttpScript::HeadersThenStall {
                    status: 503,
                    headers: round6_headers(100),
                    stall: std::time::Duration::from_secs(10),
                },
            };
            let (err, calls, elapsed) =
                scripted_call(op, std::time::Duration::from_millis(400), script).await;
            let err = err.unwrap_or_else(|| panic!("{op}/{case}: expected an error"));
            assert_eq!(calls, 1, "{op}/{case}: {err:?}");
            assert!(
                elapsed < std::time::Duration::from_secs(3),
                "{op}/{case}: {elapsed:?}"
            );
            assert_eq!(
                err.answer()
                    .map(|answer| (answer.status, answer.retry_after)),
                Some((503, Some(30.0))),
                "{op}/{case}: {err:?}"
            );
        }
    }
    let refused = super::hardening_support::RefusedPort::bind();
    let client = Client::new(Config {
        api_url: refused.http_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: std::time::Duration::from_millis(400),
        ..Default::default()
    })
    .expect("client");
    let err = client
        .triggers
        .get_by_id("9", SubAccountScope::Default)
        .await
        .expect_err("refused connection");
    assert!(err.answer().is_none(), "{err:?}");
}

/// A redirect is never followed. A 307 with Retry-After 30, to a valid or a
/// malformed `Location`, leaves an error that carries 307 and 30, after one
/// request; the redirect's target gets nothing.
#[tokio::test(flavor = "multi_thread")]
async fn round6_refused_redirects_keep_their_answer() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("target");
    let target_addr = target.local_addr().expect("addr");
    let target_hits = Arc::new(AtomicUsize::new(0));
    let hits = target_hits.clone();
    tokio::spawn(async move {
        while let Ok((_stream, _)) = target.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    for op in OPS {
        for location in [
            format!("http://{target_addr}{READ_PATH}"),
            "http://[::1".to_owned(),
        ] {
            let case = location.clone();
            let script = move || HttpScript::Raw {
                status: 307,
                headers: vec![
                    ("Location".into(), location.clone()),
                    ("Retry-After".into(), "30".into()),
                    ("Content-Length".into(), "0".into()),
                ],
                body: Vec::new(),
            };
            let (err, calls, elapsed) =
                scripted_call(op, std::time::Duration::from_secs(5), script).await;
            let err = err.unwrap_or_else(|| panic!("{op} {case}: expected an error"));
            assert_eq!(calls, 1, "{op} {case}: {err:?}");
            assert!(
                elapsed < std::time::Duration::from_secs(3),
                "{op} {case}: {elapsed:?}"
            );
            assert_eq!(
                err.answer()
                    .map(|answer| (answer.status, answer.retry_after)),
                Some((307, Some(30.0))),
                "{op} {case}: {err:?}"
            );
        }
    }
    assert_eq!(
        target_hits.load(Ordering::SeqCst),
        0,
        "a redirect was followed"
    );
}

/// The fixture is the published catalog, every catalog but Quick Swap's, as
/// distinct (code, status, Connect code) tuples: a code the catalogs answer
/// two ways keeps both, and codes the contract removed are gone.
#[test]
fn rest_error_fixture_keeps_every_catalog_variant() {
    let catalog: Value =
        serde_json::from_str(include_str!("../fixtures/rest_error_codes.json")).expect("catalog");
    let entries = catalog["codes"].as_array().expect("codes");
    let mut seen = std::collections::BTreeSet::new();
    for entry in entries {
        let tuple = (
            entry["code"].as_str().expect("code").to_owned(),
            entry["status"].as_u64().expect("status"),
            entry["connect"].as_str().expect("connect").to_owned(),
        );
        let catalogs = entry["catalogs"].as_array().expect("catalogs");
        assert!(!catalogs.is_empty(), "{tuple:?}");
        assert!(
            catalogs
                .iter()
                .all(|c| !c.as_str().expect("catalog id").starts_with("swap-api:")),
            "{tuple:?}"
        );
        assert!(seen.insert(tuple.clone()), "duplicate {tuple:?}");
    }
    let has = |code: &str, status: u64, connect: &str| {
        seen.contains(&(code.to_owned(), status, connect.to_owned()))
    };
    assert!(has("UNKNOWN_SYMBOL", 400, "invalid_argument"));
    assert!(has("UNKNOWN_SYMBOL", 404, "not_found"));
    assert!(has("REQUEST_TOO_LARGE", 413, "resource_exhausted"));
    assert!(has("UPSTREAM_ERROR", 503, "unavailable"));
    assert!(has("WALLET_RESOLUTION_UNAVAILABLE", 503, "unavailable"));
    assert!(has("ATTACHED_LEGS_EXCEEDED", 400, "failed_precondition"));
    assert!(has("INSUFFICIENT_FUNDS", 400, "failed_precondition"));
    assert!(has("BATCH_TOO_MANY_SOURCES", 400, "invalid_argument"));
    assert!(has("CLIENT_LIST_ID_INVALID", 400, "invalid_argument"));
    for removed in [
        "API_KEY_ACTION_DENY",
        "ROLE_ACTION_DENY",
        "SUBACCOUNT_POLICY_DENY",
    ] {
        assert!(seen.iter().all(|(code, ..)| code != removed), "{removed}");
    }
}

/// Every catalog tuple sent as a REST problem reads as the SDK error its
/// Connect code means, so a REST refusal and its ConnectRPC form are handled
/// alike: the refusal keeps its code, a final refusal is never retried, and
/// only a temporary or unknown outcome asks for a retry or a read-back.
#[tokio::test(flavor = "multi_thread")]
async fn every_catalog_tuple_reads_as_its_connect_code() {
    let catalog: Value =
        serde_json::from_str(include_str!("../fixtures/rest_error_codes.json")).expect("catalog");
    for entry in catalog["codes"].as_array().expect("codes") {
        let code = entry["code"].as_str().expect("code");
        let status = entry["status"].as_u64().expect("status") as u16;
        let connect = entry["connect"].as_str().expect("connect");
        let case = format!("{status}/{code}/{connect}");
        let body = json!({"code": code, "detail": "d"}).to_string();
        let (err, _) = call("write", status, PROBLEM, &body, None).await;
        let err = err.unwrap_or_else(|| panic!("{case}: expected an error"));
        let keeps_code = err
            .auth_error_code()
            .is_some_and(|got| got == code || got == format!("ERROR_CODE_{code}"));
        match connect {
            "invalid_argument"
            | "failed_precondition"
            | "not_found"
            | "aborted"
            | "already_exists" => {
                assert!(matches!(err, Error::Api { .. }), "{case}: {err:?}");
                assert!(keeps_code, "{case}: {err:?}");
                assert!(!err.mutation_outcome_unknown(), "{case}: {err:?}");
            }
            "permission_denied" => assert!(
                matches!(
                    err,
                    Error::PermissionDenied { .. } | Error::SessionRequired { .. }
                ),
                "{case}: {err:?}"
            ),
            "unauthenticated" => assert!(
                matches!(
                    err,
                    Error::ApiKeyRejected { .. } | Error::TimestampSkew { .. }
                ),
                "{case}: {err:?}"
            ),
            "resource_exhausted" if code == "REQUEST_TOO_LARGE" => {
                assert!(err.is_request_too_large(), "{case}: {err:?}");
                assert!(!err.is_retryable(), "{case}: {err:?}");
            }
            "resource_exhausted" => {
                assert!(
                    matches!(err, Error::RateLimit { .. }) || keeps_code,
                    "{case}: {err:?}"
                );
                assert!(!err.mutation_outcome_unknown(), "{case}: {err:?}");
            }
            "unavailable" => assert!(
                err.is_retryable()
                    || err.mutation_outcome_unknown()
                    || code == "ACCOUNT_ROUTING_INVALID",
                "{case}: {err:?}"
            ),
            "deadline_exceeded" | "internal" => {
                assert!(err.mutation_outcome_unknown(), "{case}: {err:?}")
            }
            "unimplemented" => {
                assert!(
                    matches!(err, Error::NotImplemented { .. }),
                    "{case}: {err:?}"
                )
            }
            other => panic!("{case}: unexpected Connect code {other}"),
        }
    }
}

/// A read refused for a missing policy action answers `403`
/// `PERMISSION_DENIED` naming the action, where ledger reads answered
/// `API_KEY_ACTION_DENY` and its siblings: it reads as a permission refusal
/// whose message names the action to grant, and is not retried.
#[tokio::test(flavor = "multi_thread")]
async fn permission_denied_names_the_missing_action() {
    let body = json!({
        "type": "https://polyester.dev/problems/permission-denied",
        "title": "Permission Denied",
        "status": 403,
        "detail": "api_key_action_deny: API key policy does not grant READ_BALANCES",
        "code": "PERMISSION_DENIED",
    })
    .to_string();
    let (err, calls) = call("read", 403, PROBLEM, &body, None).await;
    let err = err.expect("refusal");
    assert_eq!(calls, 1);
    assert!(matches!(err, Error::PermissionDenied { .. }), "{err:?}");
    assert!(err.to_string().contains("READ_BALANCES"), "{err}");
    assert!(
        !err.is_retryable() && !err.mutation_outcome_unknown(),
        "{err:?}"
    );
}
