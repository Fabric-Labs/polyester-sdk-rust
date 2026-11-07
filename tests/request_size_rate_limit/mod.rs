//! Uniform request-size and rate-limit answers, and the wallet-already-linked
//! login refusal, read the same over ConnectRPC and REST (contract 1bf038e).

use super::hardening_support::{HttpScript, MockHttpServer};
use base64::Engine as _;
use buffa::Message as _;
use polyester::models::RateLimitDetail;
use polyester::proto::auth::v1::{AuthErrorCode, AuthErrorDetail};
use polyester::proto::polyester::ratelimit::v1::{
    FailureReason, LimiterScope, PolicyClass, RateLimitDetail as ProtoRateLimitDetail,
};
use polyester::{Client, Config, Error};
use serde_json::json;

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const PROBLEM: &str = "application/problem+json";
const JSON: &str = "application/json";

fn api_key_client(http: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
}

fn public_client(http: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: http.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
}

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

/// The same body without a Content-Length, in two chunks.
fn chunked(status: u16, content_type: &str, body: &str) -> HttpScript {
    let (head, tail) = body.split_at(20);
    let framed = format!(
        "{:x}\r\n{head}\r\n{:x}\r\n{tail}\r\n0\r\n\r\n",
        head.len(),
        tail.len()
    );
    HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".to_owned(), content_type.to_owned()),
            ("Transfer-Encoding".to_owned(), "chunked".to_owned()),
        ],
        body: framed.into_bytes(),
    }
}

async fn me_error(script: impl Fn() -> HttpScript + Send + Sync + 'static) -> Error {
    let http = MockHttpServer::spawn(move |_| script()).await;
    api_key_client(&http).auth.me().await.expect_err("refused")
}

const REQUEST_TOO_LARGE: &str = r#"{"type":"https://polyester.dev/problems/request-too-large","title":"Request Entity Too Large","status":413,"detail":"request too large","code":"REQUEST_TOO_LARGE"}"#;

fn assert_request_too_large(err: &Error) {
    assert!(
        matches!(err, Error::Api { code, .. } if code == "REQUEST_TOO_LARGE"),
        "{err:?}"
    );
    assert!(!matches!(err, Error::RateLimit { .. }));
    assert!(err.is_request_too_large());
    assert!(
        matches!(err, Error::Api { code, .. } if code == polyester::problem_codes::REQUEST_TOO_LARGE)
    );
    let problem = err.http_problem().expect("problem");
    assert_eq!(problem.status, 413);
    assert_eq!(problem.code, "REQUEST_TOO_LARGE");
    assert_eq!(
        problem.problem_type,
        "https://polyester.dev/problems/request-too-large"
    );
    assert_eq!(problem.detail, "request too large");
    // Refused before it was read: nothing changed, and the same body is
    // refused again.
    assert!(!err.is_retryable(), "{err:?}");
    assert!(!err.mutation_outcome_unknown(), "{err:?}");
}

#[tokio::test]
async fn request_too_large_problem_is_a_typed_refusal() {
    for is_chunked in [false, true] {
        let err = me_error(move || {
            if is_chunked {
                chunked(413, PROBLEM, REQUEST_TOO_LARGE)
            } else {
                response(413, PROBLEM, REQUEST_TOO_LARGE, &[])
            }
        })
        .await;
        assert_request_too_large(&err);
    }
}

/// An oversized request is refused before the server reads its body; the
/// refusal, not the interrupted upload, is what the caller sees.
#[tokio::test]
async fn oversized_request_refused_before_its_body_is_read() {
    for is_chunked in [false, true] {
        let http = MockHttpServer::spawn(move |_| {
            let script = if is_chunked {
                chunked(413, PROBLEM, REQUEST_TOO_LARGE)
            } else {
                response(413, PROBLEM, REQUEST_TOO_LARGE, &[])
            };
            let HttpScript::Raw {
                status,
                headers,
                body,
            } = script
            else {
                unreachable!()
            };
            HttpScript::RawLingering {
                status,
                headers,
                body,
            }
        })
        .await;
        let err = public_client(&http)
            .changelog
            .get_post(&"x".repeat(4 << 20))
            .await
            .expect_err("refused");
        assert_request_too_large(&err);
    }
}

const RATE_LIMIT_PROBLEM_TAIL: &str = r#""status":429,"title":"Rate limit exceeded","type":"https://polyester.dev/problems/rate-limit-exceeded"}"#;

fn rate_limit_problem(rate_limit: serde_json::Value) -> String {
    format!(
        r#"{{"code":"RATE_LIMIT_EXCEEDED","detail":"Rate limit exceeded","rate_limit":{rate_limit},{RATE_LIMIT_PROBLEM_TAIL}"#
    )
}

/// A REST rate-limit problem: its name, body and headers, and the detail and
/// delay it must read as.
type RateLimitCase = (
    &'static str,
    String,
    Vec<(&'static str, &'static str)>,
    RateLimitDetail,
    Option<f64>,
);

/// The gateway's own refusal names only the reason and the scope.
fn gateway_detail() -> RateLimitDetail {
    RateLimitDetail {
        reason: "QUOTA_EXCEEDED".into(),
        limit: None,
        remaining: None,
        retry_after_ms: None,
        policy_version: None,
        operation_id: String::new(),
        policy_class: String::new(),
        scope: "CLIENT_IP".into(),
        refill_model: String::new(),
    }
}

#[tokio::test]
async fn rate_limit_problem_carries_its_detail() {
    let cases: Vec<RateLimitCase> = vec![
        (
            "service quota",
            rate_limit_problem(json!({
                "reason": "QUOTA_EXCEEDED", "limit": "30", "remaining": "0", "retry_after_ms": "1409",
                "policy_version": "5", "operation_id": "auth.wallet_challenge", "policy_class": "AUTH_PUBLIC",
                "scope": "AUTH_SUBJECT", "refill_model": "CONTINUOUS"
            })),
            vec![
                ("Retry-After", "2"),
                ("RateLimit-Limit", "30"),
                ("RateLimit-Remaining", "0"),
                ("RateLimit-Reset", "2"),
            ],
            RateLimitDetail {
                reason: "QUOTA_EXCEEDED".into(),
                limit: Some(30),
                remaining: Some(0),
                retry_after_ms: Some(1409),
                policy_version: Some(5),
                operation_id: "auth.wallet_challenge".into(),
                policy_class: "AUTH_PUBLIC".into(),
                scope: "AUTH_SUBJECT".into(),
                refill_model: "CONTINUOUS".into(),
            },
            Some(1.409),
        ),
        (
            "gateway window",
            rate_limit_problem(json!({"reason": "QUOTA_EXCEEDED", "scope": "CLIENT_IP"})),
            vec![
                ("Retry-After", "59"),
                ("X-RateLimit-Limit", "30"),
                ("X-RateLimit-Remaining", "0"),
                ("X-RateLimit-Reset", "59"),
            ],
            gateway_detail(),
            Some(59.0),
        ),
        (
            "gateway concurrency",
            rate_limit_problem(json!({"reason": "QUOTA_EXCEEDED", "scope": "CLIENT_IP"})),
            vec![],
            gateway_detail(),
            None,
        ),
        (
            "numbers and a newer enum value",
            rate_limit_problem(
                json!({"reason": "SOME_NEW_REASON", "limit": 30, "retry_after_ms": 250}),
            ),
            vec![],
            RateLimitDetail {
                reason: "SOME_NEW_REASON".into(),
                limit: Some(30),
                remaining: None,
                retry_after_ms: Some(250),
                policy_version: None,
                operation_id: String::new(),
                policy_class: String::new(),
                scope: String::new(),
                refill_model: String::new(),
            },
            Some(0.25),
        ),
    ];
    for (name, body, headers, want, retry_after) in cases {
        let headers: Vec<(String, String)> = headers
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let err = me_error(move || {
            let extra: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            response(429, PROBLEM, &body, &extra)
        })
        .await;
        let Error::RateLimit { code, detail, .. } = &err else {
            panic!("{name}: {err:?}");
        };
        assert_eq!(
            code.as_deref(),
            Some("ERROR_CODE_RATE_LIMIT_EXCEEDED"),
            "{name}"
        );
        assert_eq!(detail.as_deref(), Some(&want), "{name}: rate_limit detail");
        assert_eq!(err.retry_after(), retry_after, "{name}");
        assert!(err.is_retryable(), "{name}");
        assert!(!err.mutation_outcome_unknown(), "{name}");
    }
}

/// AUTH_RESOURCE_EXHAUSTED is a capacity refusal that waiting does not
/// resolve, not a rate limit.
#[tokio::test]
async fn auth_resource_exhausted_problem_is_not_a_rate_limit() {
    let err = me_error(|| {
        response(
            429,
            PROBLEM,
            r#"{"code":"AUTH_RESOURCE_EXHAUSTED","detail":"no new account can be created","status":429,"title":"Resource Exhausted","type":"https://polyester.dev/problems/auth-resource-exhausted"}"#,
            &[],
        )
    })
    .await;
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "AUTH_RESOURCE_EXHAUSTED"),
        "{err:?}"
    );
    assert!(!err.is_retryable());
    assert!(!err.mutation_outcome_unknown());
}

/// The gateway's own Connect-shaped refusal has no details: fall back to
/// Retry-After when present, and give no server hint when absent.
#[tokio::test]
async fn gateway_connect_refusal_falls_back_to_retry_after() {
    for (headers, want) in [
        (
            vec![("Retry-After", "59"), ("X-RateLimit-Limit", "30")],
            Some(59.0),
        ),
        (vec![], None),
    ] {
        let err = me_error(move || {
            response(
                429,
                JSON,
                r#"{"code":"resource_exhausted","message":"rate limit exceeded, retry later"}"#,
                &headers,
            )
        })
        .await;
        assert!(
            matches!(&err, Error::RateLimit { detail: None, .. }),
            "{err:?}"
        );
        assert_eq!(err.retry_after(), want);
        assert!(err.is_retryable());
    }
}

/// A service's ConnectRPC refusal carries RateLimitDetail as typed retry
/// information, ahead of the headers.
#[tokio::test]
async fn connect_rate_limit_detail_is_typed_retry_information() {
    let detail = ProtoRateLimitDetail {
        reason: FailureReason::QUOTA_EXCEEDED.into(),
        limit: Some(30),
        retry_after_ms: Some(1409),
        operation_id: "auth.wallet_challenge".into(),
        policy_class: PolicyClass::AUTH_PUBLIC.into(),
        scope: LimiterScope::AUTH_SUBJECT.into(),
        ..Default::default()
    };
    let body = json!({
        "code": "resource_exhausted",
        "message": "rate limit exceeded",
        "details": [{
            "type": "polyester.ratelimit.v1.RateLimitDetail",
            "value": base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_vec()),
        }],
    })
    .to_string();
    let err = me_error(move || response(429, JSON, &body, &[("Retry-After", "2")])).await;
    let Error::RateLimit {
        detail: Some(detail),
        ..
    } = &err
    else {
        panic!("{err:?}");
    };
    assert_eq!(detail.scope, "AUTH_SUBJECT");
    assert_eq!(detail.operation_id, "auth.wallet_challenge");
    assert_eq!(err.retry_after(), Some(1.409));
}

const WALLET_LINKED: &str = "this wallet is already linked to another account; log in with the account address it already uses";

/// AUTH_WALLET_ALREADY_LINKED is a terminal login refusal on both protocols.
#[tokio::test]
async fn wallet_already_linked_is_a_terminal_auth_refusal() {
    let detail = AuthErrorDetail {
        code: AuthErrorCode::AUTH_WALLET_ALREADY_LINKED.into(),
        message: WALLET_LINKED.into(),
        ..Default::default()
    };
    let connect = json!({
        "code": "already_exists",
        "message": WALLET_LINKED,
        "details": [{
            "type": "auth.v1.AuthErrorDetail",
            "value": base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_vec()),
        }],
    })
    .to_string();
    let rest = json!({
        "status": 409,
        "code": "AUTH_WALLET_ALREADY_LINKED",
        "title": "Wallet Already Linked",
        "type": "https://polyester.dev/problems/auth-wallet-already-linked",
        "detail": WALLET_LINKED,
    })
    .to_string();
    for (content_type, body) in [(JSON, connect), (PROBLEM, rest)] {
        let err = me_error(move || response(409, content_type, &body, &[])).await;
        assert_eq!(
            err.auth_error_code(),
            Some("AUTH_WALLET_ALREADY_LINKED"),
            "{err:?}"
        );
        assert_eq!(
            err.auth_code(),
            Some(AuthErrorCode::AUTH_WALLET_ALREADY_LINKED)
        );
        assert!(err.to_string().ends_with(WALLET_LINKED), "{err}");
        assert!(err.is_wallet_already_linked());
        assert_eq!(
            err.auth_error_code(),
            Some(polyester::auth_codes::WALLET_ALREADY_LINKED)
        );
        assert!(!err.is_retryable(), "{err:?}");
        assert!(!err.mutation_outcome_unknown(), "{err:?}");
    }
}

#[test]
fn request_size_and_wallet_predicates_read_only_their_codes() {
    let api = |code: &str| Error::Api {
        message: "m".into(),
        code: code.into(),
        metadata: Vec::new(),
        violations: Vec::new(),
    };
    assert!(!api("PAYLOAD_TOO_LARGE").is_request_too_large());
    assert!(
        !Error::RateLimit {
            code: Some("REQUEST_TOO_LARGE".into()),
            message: "m".into(),
            retry_after: None,
            detail: None,
        }
        .is_request_too_large()
    );
    assert!(!api("AUTH_WALLET_LOGIN_FAILED").is_wallet_already_linked());
    assert!(api("AUTH_WALLET_ALREADY_LINKED").is_wallet_already_linked());
}
