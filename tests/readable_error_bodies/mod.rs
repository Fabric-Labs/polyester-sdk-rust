//! An HTTP error with no recognised code proves nothing about a mutation.
//!
//! Each case goes through a public read and a public create against a local
//! server. Only the requested procedure is counted and every expectation needs
//! at least one request, so a case that never reached the server fails.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
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
