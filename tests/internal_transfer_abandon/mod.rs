//! Abandoning an internal transfer batch's idempotency key
//! (`AbandonInternalTransferBatch`) and the refusal of a batch under an
//! abandoned key, through the public services against a local server.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::models::{
    BatchCreateInternalTransfersParams, InternalTransferBatchLeg, SubAccountScope,
};
use polyester::proto::transfer::v1::{
    AbandonInternalTransferBatchRequest, AbandonInternalTransferBatchResponse,
    BatchCreateInternalTransfersResponse, ErrorCode, ErrorDetail, GetInternalTransferBatchResponse,
    InternalTransferBatchLegResult, InternalTransferBatchLegStatus, InternalTransferBatchStatus,
};
use polyester::{AssetAmount, Client, Config, Error, QuantityDomain};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const ABANDON: &str = "/transfer.v1.InternalTransferService/AbandonInternalTransferBatch";

/// Each request a server saw: its path, whether it was signed, and its body.
type Seen = Arc<Mutex<Vec<(String, bool, Vec<u8>)>>>;

/// A client against a server that answers each request with the next of
/// `scripts`, and not found once they are spent.
async fn server(scripts: Vec<HttpScript>) -> (Client, Seen) {
    let seen: Seen = Arc::default();
    let log = seen.clone();
    let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
    let http = MockHttpServer::spawn(move |request| {
        log.lock().unwrap().push((
            request.path.clone(),
            request.has_header("X-API-KEY-ID"),
            request.body.clone(),
        ));
        scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(HttpScript::NotFound)
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    // The server lives as long as the test: leak it with the client.
    std::mem::forget(http);
    (client, seen)
}

fn transfer_error(connect: &str, status: u16, code: ErrorCode) -> HttpScript {
    let detail = ErrorDetail {
        code: code.into(),
        ..Default::default()
    };
    json_body(
        status,
        "application/json",
        json!({"code": connect, "message": "refused", "details": [{
            "type": "transfer.v1.ErrorDetail",
            "value": base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec()),
        }]}),
    )
}

fn json_body(status: u16, content_type: &str, body: serde_json::Value) -> HttpScript {
    let body = body.to_string();
    HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".into(), content_type.into()),
            ("Content-Length".into(), body.len().to_string()),
        ],
        body: body.into_bytes(),
    }
}

fn abandoned() -> AbandonInternalTransferBatchResponse {
    AbandonInternalTransferBatchResponse {
        status: InternalTransferBatchStatus::BATCH_ABANDONED.into(),
        ..Default::default()
    }
}

fn not_moved() -> InternalTransferBatchLegResult {
    InternalTransferBatchLegResult {
        leg_index: 0,
        status: InternalTransferBatchLegStatus::LEG_NOT_MOVED.into(),
        ..Default::default()
    }
}

fn batch_params(key: &str) -> BatchCreateInternalTransfersParams {
    BatchCreateInternalTransfersParams {
        legs: vec![InternalTransferBatchLeg {
            asset_id: 1,
            quantity: AssetAmount::from_decimal_str("1", 18, QuantityDomain::LedgerE18, Some(1))
                .unwrap(),
            quantity_scale: None,
            subaccount_id: SubAccountScope::Main,
            destination_account_id: None,
            destination_subaccount_id: None,
            destination_smart_account_address: Some(
                "0x2222222222222222222222222222222222222222".into(),
            ),
        }],
        idempotency_key: key.into(),
    }
}

/// Abandoning a key no batch was submitted under answers `abandoned`, with
/// no request ID and no legs, from one signed request naming the key.
#[tokio::test]
async fn abandon_batch_of_an_unused_key_answers_abandoned() {
    let (client, seen) = server(vec![connect_proto_ok(&abandoned())]).await;
    let result = client
        .internal_transfers
        .abandon_batch("rebalance-1")
        .await
        .unwrap();
    assert_eq!(
        (result.status.as_str(), result.request_id.as_str()),
        ("abandoned", "")
    );
    assert!(result.legs.is_empty());
    let sent = seen.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    let (path, signed, body) = &sent[0];
    assert_eq!(path, ABANDON);
    assert!(signed);
    let request = AbandonInternalTransferBatchRequest::decode_from_slice(body).unwrap();
    assert_eq!(request.idempotency_key, "rebalance-1");
}

/// Abandoning a key a batch was submitted under answers that batch, checked
/// as a read is; a batch refused before processing may have no request ID.
#[tokio::test]
async fn abandon_batch_answers_the_batch_submitted_under_the_key() {
    let refused = AbandonInternalTransferBatchResponse {
        status: InternalTransferBatchStatus::BATCH_REFUSED.into(),
        legs: vec![InternalTransferBatchLegResult {
            error_code: ErrorCode::ERROR_CODE_INSUFFICIENT_FUNDS.into(),
            ..not_moved()
        }],
        ..Default::default()
    };
    let (client, _) = server(vec![connect_proto_ok(&refused)]).await;
    let result = client.internal_transfers.abandon_batch("k").await.unwrap();
    assert_eq!(result.status, "refused");
    assert_eq!(result.legs.len(), 1);
    assert_eq!(result.legs[0].status, "not_moved");

    let processing = AbandonInternalTransferBatchResponse {
        request_id: "1234".into(),
        status: InternalTransferBatchStatus::BATCH_PROCESSING.into(),
        legs: vec![InternalTransferBatchLegResult {
            leg_index: 0,
            status: InternalTransferBatchLegStatus::LEG_PROCESSING.into(),
            transfer_id: "t0".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let (client, _) = server(vec![connect_proto_ok(&processing)]).await;
    let result = client.internal_transfers.abandon_batch("k").await.unwrap();
    assert_eq!(
        (result.request_id.as_str(), result.status.as_str()),
        ("1234", "processing")
    );
}

/// An idempotency key that is empty or over 128 characters is refused
/// before sending; one of 128 characters is sent.
#[tokio::test]
async fn abandon_batch_refuses_an_out_of_contract_key_before_sending() {
    let (client, seen) = server(vec![connect_proto_ok(&abandoned())]).await;
    for key in [String::new(), "k".repeat(129)] {
        let err = client
            .internal_transfers
            .abandon_batch(&key)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "{} chars: {err:?}",
            key.len()
        );
    }
    assert!(seen.lock().unwrap().is_empty());
    client
        .internal_transfers
        .abandon_batch(&"k".repeat(128))
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// An abandoned answer with a request ID or legs, an unknown or unset
/// status, or a batch without legs breaks the contract.
#[tokio::test]
async fn abandon_batch_refuses_an_answer_that_breaks_the_contract() {
    let cases = [
        (
            "abandoned with request ID",
            AbandonInternalTransferBatchResponse {
                request_id: "1".into(),
                ..abandoned()
            },
        ),
        (
            "abandoned with legs",
            AbandonInternalTransferBatchResponse {
                legs: vec![not_moved()],
                ..abandoned()
            },
        ),
        (
            "unknown status",
            AbandonInternalTransferBatchResponse {
                request_id: "1".into(),
                status: buffa::EnumValue::from(99),
                legs: vec![not_moved()],
                ..Default::default()
            },
        ),
        (
            "unset status",
            AbandonInternalTransferBatchResponse::default(),
        ),
        (
            "accepted without legs",
            AbandonInternalTransferBatchResponse {
                request_id: "1".into(),
                status: InternalTransferBatchStatus::BATCH_ACCEPTED.into(),
                ..Default::default()
            },
        ),
    ];
    for (name, reply) in cases {
        let (client, _) = server(vec![connect_proto_ok(&reply)]).await;
        let err = client
            .internal_transfers
            .abandon_batch("k")
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::ResponseContract { .. }),
            "{name}: {err:?}"
        );
    }
}

/// An unavailable answer leaves the abandon's outcome unknown, as for any
/// write; abandoning again with the same key resolves it.
#[tokio::test]
async fn abandon_batch_unknown_outcome_is_resolved_by_abandoning_again() {
    let (client, seen) = server(vec![
        transfer_error(
            "unavailable",
            503,
            ErrorCode::ERROR_CODE_SERVICE_UNAVAILABLE,
        ),
        connect_proto_ok(&abandoned()),
    ])
    .await;
    let err = client
        .internal_transfers
        .abandon_batch("k")
        .await
        .unwrap_err();
    assert!(err.mutation_outcome_unknown(), "{err:?}");
    assert!(!err.is_idempotency_key_abandoned());
    let result = client.internal_transfers.abandon_batch("k").await.unwrap();
    assert_eq!(result.status, "abandoned");
    assert_eq!(seen.lock().unwrap().len(), 2);
}

/// Only abandoning answers an abandoned key: a create or a read by request
/// ID that answers `BATCH_ABANDONED` breaks the contract.
#[tokio::test]
async fn only_abandon_batch_answers_an_abandoned_key() {
    let (client, _) = server(vec![connect_proto_ok(
        &BatchCreateInternalTransfersResponse {
            status: InternalTransferBatchStatus::BATCH_ABANDONED.into(),
            legs: vec![not_moved()],
            ..Default::default()
        },
    )])
    .await;
    let err = client
        .internal_transfers
        .batch_create(batch_params("k"))
        .await
        .unwrap_err();
    assert!(err.mutation_outcome_unknown(), "{err:?}");
    assert!(!err.is_idempotency_key_abandoned());

    let (client, _) = server(vec![connect_proto_ok(&GetInternalTransferBatchResponse {
        status: InternalTransferBatchStatus::BATCH_ABANDONED.into(),
        legs: vec![not_moved()],
        ..Default::default()
    })])
    .await;
    let err = client.internal_transfers.get_batch("1").await.unwrap_err();
    assert!(matches!(err, Error::ResponseContract { .. }), "{err:?}");
}

/// A batch under an abandoned key is refused for good, over ConnectRPC and
/// REST: the one batch create failure that is final, not retryable and not
/// an unknown outcome, sent once.
#[tokio::test]
async fn batch_under_an_abandoned_key_is_final() {
    let rest = json_body(
        400,
        "application/problem+json",
        json!({"code": "IDEMPOTENCY_KEY_ABANDONED", "status": 400, "detail": "abandoned"}),
    );
    let connect = transfer_error(
        "failed_precondition",
        400,
        ErrorCode::ERROR_CODE_IDEMPOTENCY_KEY_ABANDONED,
    );
    for (shape, script) in [("connect", connect), ("rest", rest)] {
        let (client, seen) = server(vec![script]).await;
        let err = client
            .internal_transfers
            .batch_create(batch_params("abandoned-key"))
            .await
            .unwrap_err();
        assert!(err.is_idempotency_key_abandoned(), "{shape}: {err:?}");
        assert!(
            matches!(&err, Error::Api { code, .. } if code == polyester::errors::transfer_codes::IDEMPOTENCY_KEY_ABANDONED),
            "{shape}: {err:?}"
        );
        assert!(!err.is_retryable(), "{shape}: {err:?}");
        assert!(!err.mutation_outcome_unknown(), "{shape}: {err:?}");
        assert_eq!(seen.lock().unwrap().len(), 1, "{shape}");
    }

    // Any other refusal of a batch create stays an unknown outcome.
    let (client, _) = server(vec![transfer_error(
        "already_exists",
        409,
        ErrorCode::ERROR_CODE_IDEMPOTENCY_CONFLICT,
    )])
    .await;
    let err = client
        .internal_transfers
        .batch_create(batch_params("used-key"))
        .await
        .unwrap_err();
    assert!(!err.is_idempotency_key_abandoned(), "{err:?}");
    assert!(err.mutation_outcome_unknown(), "{err:?}");
}
