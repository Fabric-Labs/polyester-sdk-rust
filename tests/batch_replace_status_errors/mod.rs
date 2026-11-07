//! GetBatchReplaceStatus errors and the exact 64-bit batch identity
//! (contract 1bf038e): UNAVAILABLE is retryable, NOT_FOUND is a not-found
//! error and never a rejected batch.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::proto::orders::v1::{
    BatchReplaceAdmissionStatus, ErrorCode, ErrorDetail, GetBatchReplaceStatusRequest,
    GetBatchReplaceStatusResponse,
};
use polyester::{Client, Config, Error};
use serde_json::json;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

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

fn connect_error(status: u16, body: serde_json::Value) -> HttpScript {
    let body = body.to_string();
    HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Content-Length".into(), body.len().to_string()),
        ],
        body: body.into_bytes(),
    }
}

#[tokio::test]
async fn get_batch_replace_status_errors_and_exact_batch_id() {
    let detail = ErrorDetail {
        code: ErrorCode::ERROR_CODE_UNAVAILABLE.into(),
        ..Default::default()
    };
    let unavailable = json!({
        "code": "unavailable",
        "message": "The batch status is temporarily unavailable; retry the request.",
        "details": [{
            "type": "orders.v1.ErrorDetail",
            "value": base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_vec()),
        }],
    });
    let http = MockHttpServer::spawn(move |_| connect_error(503, unavailable.clone())).await;
    let err = api_key_client(&http)
        .orders
        .get_batch_replace_status("18446744073709551615", None)
        .await
        .expect_err("unavailable");
    assert!(err.is_retryable(), "{err:?}");
    assert!(!err.mutation_outcome_unknown(), "{err:?}");

    let http = MockHttpServer::spawn(|_| {
        connect_error(
            404,
            json!({"code": "not_found", "message": "batch replace status not found"}),
        )
    })
    .await;
    let err = api_key_client(&http)
        .orders
        .get_batch_replace_status("18446744073709551615", None)
        .await
        .expect_err("not found");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "NotFound"),
        "{err:?}"
    );
    assert!(!err.is_retryable());

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let http = MockHttpServer::spawn(move |request| {
        let req = GetBatchReplaceStatusRequest::decode_from_slice(&request.body).unwrap();
        seen_handler.lock().unwrap().push(req.batch_request_id);
        connect_proto_ok(&GetBatchReplaceStatusResponse {
            batch_request_id: req.batch_request_id,
            admission_status: BatchReplaceAdmissionStatus::Admitted.into(),
            ..Default::default()
        })
    })
    .await;
    let status = api_key_client(&http)
        .orders
        .get_batch_replace_status("18446744073709551615", None)
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [u64::MAX]);
    assert_eq!(
        status.batch_request_id,
        polyester::codecs::scalars::format_uint64_id(u64::MAX)
    );
}
