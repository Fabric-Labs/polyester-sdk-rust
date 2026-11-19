//! Public contract 1945423c (exchange release 0.93.3), through the public
//! services against a local server: account flows, batch internal transfers,
//! dust conversion, statement export files, exact balance history and the
//! new refusals.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::models::{
    BatchCreateInternalTransfersParams, InternalTransferBatchLeg, SubAccountScope,
};
use polyester::proto::chain::lifecycle::v1::{
    FlowDetailView, FlowParticipation, FlowSummaryView, GetAccountFlowRequest,
    GetAccountFlowResponse, ListAccountFlowsRequest, ListAccountFlowsResponse,
};
use polyester::proto::ledger::read::v1::{
    BalanceSeries, CreateStatementExportRequest, CreateStatementExportResponse,
    GetBalanceHistoryRequest, GetBalanceHistoryResponse, ListStatementExportsRequest,
    ListStatementExportsResponse, StatementExport, StatementExportStatus,
};
use polyester::proto::polyester::r#type::v1::U128;
use polyester::proto::transfer::v1::{
    BatchCreateInternalTransfersRequest, BatchCreateInternalTransfersResponse,
    CreateDustConversionRequest, CreateDustConversionResponse, DustConversion, DustConversionState,
    GetInternalTransferBatchRequest, GetInternalTransferBatchResponse,
    InternalTransferBatchLegResult, InternalTransferBatchLegStatus, InternalTransferBatchStatus,
    ListDustConversionsRequest, ListDustConversionsResponse, QuoteDustConversionRequest,
    QuoteDustConversionResponse, ResolvedDestination, internal_transfer_batch_leg,
};
use polyester::services::CreateStatementExportOptions;
use polyester::{AssetAmount, Client, Config, Error, QuantityDomain};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const SIGNED_HEADER: &str = "X-API-KEY-ID";
const DEFAULT_SUB: &str = "Bht3tfQZv8E";

/// Each request a server saw: its path, whether it was signed, and its body.
type Seen = Arc<Mutex<Vec<(String, bool, Vec<u8>)>>>;

async fn server(reply: impl Fn(&str) -> HttpScript + Send + Sync + 'static) -> (Client, Seen) {
    let seen: Seen = Arc::default();
    let log = seen.clone();
    let http = MockHttpServer::spawn(move |request| {
        log.lock().unwrap().push((
            request.path.clone(),
            request.has_header(SIGNED_HEADER),
            request.body.clone(),
        ));
        reply(&request.path)
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some(DEFAULT_SUB.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    // The server lives as long as the test: leak it with the client.
    std::mem::forget(http);
    (client, seen)
}

fn default_sub() -> u64 {
    polyester::codecs::scalars::parse_account_id(DEFAULT_SUB, "s").unwrap()
}

fn requests(seen: &Seen) -> Vec<(String, bool, Vec<u8>)> {
    std::mem::take(&mut *seen.lock().unwrap())
}

fn u128(value: u128) -> U128 {
    U128 {
        hi: (value >> 64) as u64,
        lo: value as u64,
        ..Default::default()
    }
}

fn json_body(status: u16, body: serde_json::Value) -> HttpScript {
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

fn connect_error(code: &str, detail_type: &str, detail: Vec<u8>) -> HttpScript {
    let status = match code {
        "resource_exhausted" => 429,
        "invalid_argument" | "failed_precondition" => 400,
        _ => 500,
    };
    json_body(
        status,
        json!({"code": code, "message": "refused", "details": [{
            "type": detail_type,
            "value": base64::engine::general_purpose::STANDARD.encode(detail),
        }]}),
    )
}

/// The account flow reads are signed, take the default sub-account, send the
/// request's filters, and refuse a page size above the contract's 500 before
/// sending.
#[tokio::test]
async fn account_flow_reads_are_signed_and_scoped() {
    let (client, seen) = server(|path| match path {
        "/chain.lifecycle.v1.LifecycleReadService/ListAccountFlows" => {
            connect_proto_ok(&ListAccountFlowsResponse {
                flows: vec![FlowSummaryView {
                    flow_id: "flow_a".into(),
                    participation: FlowParticipation::PARTICIPATION_RECIPIENT.into(),
                    ..Default::default()
                }],
                next_page_token: "next".into(),
                ..Default::default()
            })
        }
        _ => connect_proto_ok(&GetAccountFlowResponse {
            flow: FlowDetailView {
                summary: FlowSummaryView {
                    flow_id: "flow_a".into(),
                    participation: FlowParticipation::PARTICIPATION_OWNER.into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }),
    })
    .await;

    let page = client
        .lifecycle
        .list_account_flows(ListAccountFlowsRequest {
            limit: 500,
            participation: FlowParticipation::PARTICIPATION_RECIPIENT.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.flows[0].participation, "recipient");
    assert_eq!(page.next_page_token, "next");
    let flow = client
        .lifecycle
        .get_account_flow("flow_a", SubAccountScope::Id(77))
        .await
        .unwrap();
    assert_eq!(flow.participation, "owner");

    let sent = requests(&seen);
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|(_, signed, _)| *signed));
    let list = ListAccountFlowsRequest::decode_from_slice(&sent[0].2).unwrap();
    assert_eq!(list.subaccount_id, Some(default_sub()));
    assert_eq!(list.limit, 500);
    assert_eq!(
        list.participation,
        FlowParticipation::PARTICIPATION_RECIPIENT
    );
    let get = GetAccountFlowRequest::decode_from_slice(&sent[1].2).unwrap();
    assert_eq!(
        (get.subaccount_id, get.flow_id.as_str()),
        (Some(77), "flow_a")
    );

    let too_many = client
        .lifecycle
        .list_account_flows(ListAccountFlowsRequest {
            limit: 501,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(too_many, Error::Validation(_)), "{too_many:?}");
    assert!(matches!(
        client
            .lifecycle
            .get_account_flow(" ", SubAccountScope::Default)
            .await,
        Err(Error::Validation(_))
    ));
    assert!(requests(&seen).is_empty(), "refused calls were sent");
}

/// Each ID filter of `list_account_flows` follows its contract rule (at most
/// 100 unique, positive IDs): a breaking filter is refused before anything
/// is sent, and exactly 100 distinct IDs are sent as given.
#[tokio::test]
async fn account_flow_id_filters_follow_the_contract_rule() {
    let (client, seen) = server(|_| connect_proto_ok(&ListAccountFlowsResponse::default())).await;
    type Setter = fn(&mut ListAccountFlowsRequest, Vec<u32>);
    let filters: [(&str, Setter); 3] = [
        ("polyester_chain_ids", |r, ids| r.polyester_chain_ids = ids),
        ("zipped_asset_ids", |r, ids| r.zipped_asset_ids = ids),
        ("unified_asset_ids", |r, ids| r.unified_asset_ids = ids),
    ];
    for (field, set) in filters {
        for (ids, want) in [
            (
                (1..=101).collect::<Vec<u32>>(),
                format!("{field} holds at most 100 IDs, not 101"),
            ),
            (vec![3, 0], format!("{field} IDs must be positive")),
            (vec![5, 7, 5], format!("{field} repeats 5")),
        ] {
            let mut req = ListAccountFlowsRequest::default();
            set(&mut req, ids);
            let err = client.lifecycle.list_account_flows(req).await.unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{field}: {err:?}");
            assert!(err.to_string().contains(&want), "{field}: {err}");
        }
        assert!(
            requests(&seen).is_empty(),
            "{field}: a refused filter was sent"
        );
        let mut req = ListAccountFlowsRequest::default();
        set(&mut req, (1..=100).collect());
        client.lifecycle.list_account_flows(req).await.unwrap();
        let sent = requests(&seen);
        assert_eq!(sent.len(), 1, "{field}");
        let got = ListAccountFlowsRequest::decode_from_slice(&sent[0].2).unwrap();
        let ids = match field {
            "polyester_chain_ids" => got.polyester_chain_ids,
            "zipped_asset_ids" => got.zipped_asset_ids,
            _ => got.unified_asset_ids,
        };
        assert_eq!(ids, (1..=100).collect::<Vec<u32>>(), "{field}");
    }
}

fn leg(asset_id: u32, amount: &str) -> InternalTransferBatchLeg {
    InternalTransferBatchLeg {
        asset_id,
        quantity: AssetAmount::from_decimal_str(
            amount,
            18,
            QuantityDomain::LedgerE18,
            Some(asset_id),
        )
        .unwrap(),
        quantity_scale: None,
        subaccount_id: SubAccountScope::Default,
        destination_account_id: None,
        destination_subaccount_id: None,
        destination_smart_account_address: Some(
            "0x2222222222222222222222222222222222222222".into(),
        ),
    }
}

const BATCH_ID: &str = "92640625967207661803151833376978068587";

/// A batch sends every leg exactly, in order, with the source sub-account of
/// each leg and one idempotency key; the reply reads leg by leg, and the
/// batch reads back by its request ID.
/// A batch whose checks could not be decided yet is refused `unavailable`
/// before anything is recorded under its key: only an HTTP 503 ConnectRPC
/// `unavailable` with a typed `transfer.v1.ErrorDetail` naming one of the
/// documented codes is retryable with the same key and legs, and it keeps the
/// answer's status and Retry-After. A 502 or 504, another detail type,
/// generic JSON, the code in the message only, or another transfer code stays
/// an unknown outcome that is not retried.
#[tokio::test]
async fn batch_transfer_pre_admission_unavailable_is_retryable() {
    use polyester::proto::chain::withdraw::v1::{
        ErrorCode as WithdrawCode, ErrorDetail as WithdrawDetail,
    };
    use polyester::proto::transfer::v1::{ErrorCode as TransferCode, ErrorDetail};
    let b64 = |bytes: Vec<u8>| base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes);
    let typed = |connect: &str, code: TransferCode| {
        json!({
            "code": connect,
            "message": "refused",
            "details": [{
                "type": "transfer.v1.ErrorDetail",
                "value": b64(ErrorDetail { code: code.into(), ..Default::default() }.encode_to_vec()),
            }],
        })
        .to_string()
    };
    let problem = r#"{"type":"https://polyester.dev/problems/service-unavailable","title":"Service Unavailable","status":503,"code":"SERVICE_UNAVAILABLE"}"#;
    let transfer = |code: TransferCode| {
        json!({
            "code": "unavailable",
            "message": "try again",
            "details": [{
                "type": "transfer.v1.ErrorDetail",
                "value": b64(ErrorDetail { code: code.into(), ..Default::default() }.encode_to_vec()),
            }],
        })
        .to_string()
    };
    let withdraw = json!({
        "code": "unavailable",
        "message": "try again",
        "details": [{
            "type": "chain.withdraw.v1.ErrorDetail",
            "value": b64(WithdrawDetail {
                code: WithdrawCode::ERROR_CODE_SERVICE_UNAVAILABLE.into(),
                ..Default::default()
            }
            .encode_to_vec()),
        }],
    })
    .to_string();
    let json = "application/json";
    let cases: Vec<(&str, u16, &str, String, bool)> = vec![
        (
            "typed service",
            503,
            json,
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed shard",
            503,
            json,
            transfer(TransferCode::ERROR_CODE_ACCOUNT_SHARD_UNAVAILABLE),
            true,
        ),
        (
            "other transfer code",
            503,
            json,
            transfer(TransferCode::ERROR_CODE_CAPITAL_VIEW_UNAVAILABLE),
            false,
        ),
        (
            "504",
            504,
            json,
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            false,
        ),
        (
            "502",
            502,
            json,
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            false,
        ),
        ("foreign detail type", 503, json, withdraw, false),
        (
            "generic JSON",
            503,
            json,
            r#"{"error":"ERROR_CODE_SERVICE_UNAVAILABLE"}"#.to_owned(),
            false,
        ),
        (
            "code in the message only",
            503,
            json,
            r#"{"code":"unavailable","message":"ERROR_CODE_SERVICE_UNAVAILABLE"}"#.to_owned(),
            false,
        ),
        // The typed ConnectRPC refusal is read from its body alone: the
        // answer's Content-Type plays no part.
        (
            "typed, json with charset",
            503,
            "application/json; charset=utf-8",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, JSON in capitals",
            503,
            "Application/JSON",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, problem+json",
            503,
            "application/problem+json",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, jsonx",
            503,
            "application/jsonx",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, text/plain",
            503,
            "text/plain",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, vendor JSON",
            503,
            "application/vnd.polyester+json",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, proto",
            503,
            "application/proto",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        (
            "typed, combined Content-Type",
            503,
            "application/json, application/problem+json",
            transfer(TransferCode::ERROR_CODE_SERVICE_UNAVAILABLE),
            true,
        ),
        // Every other failure is an unknown outcome, whatever it maps to.
        (
            "rate limit",
            429,
            json,
            typed(
                "resource_exhausted",
                TransferCode::ERROR_CODE_RATE_LIMIT_EXCEEDED,
            ),
            false,
        ),
        (
            "typed refusal",
            400,
            json,
            typed("invalid_argument", TransferCode::ERROR_CODE_INVALID_REQUEST),
            false,
        ),
        (
            "idempotency conflict",
            409,
            json,
            typed(
                "already_exists",
                TransferCode::ERROR_CODE_IDEMPOTENCY_CONFLICT,
            ),
            false,
        ),
        // A REST problem never counts as the pre-admission refusal, whatever
        // its media type.
        (
            "problem+json",
            503,
            "application/problem+json",
            problem.to_owned(),
            false,
        ),
        (
            "problem+json with parameters",
            503,
            "Application/Problem+JSON; charset=utf-8",
            problem.to_owned(),
            false,
        ),
        (
            "problem+json-gateway",
            503,
            "application/problem+json-gateway",
            problem.to_owned(),
            false,
        ),
        ("plain JSON problem", 503, json, problem.to_owned(), false),
        (
            "vendor JSON",
            503,
            "application/vnd.polyester+json",
            problem.to_owned(),
            false,
        ),
    ];
    for (case, status, content_type, body, retryable) in cases {
        let content_type = content_type.to_owned();
        let (client, seen) = server(move |_| HttpScript::Raw {
            status,
            headers: vec![
                ("Content-Type".into(), content_type.clone()),
                ("Content-Length".into(), body.len().to_string()),
                ("Retry-After".into(), "2".into()),
            ],
            body: body.as_bytes().to_vec(),
        })
        .await;
        let err = client
            .internal_transfers
            .batch_create(BatchCreateInternalTransfersParams {
                legs: vec![leg(1, "0.5")],
                idempotency_key: "rebalance-2".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.is_retryable(), retryable, "{case}: {err:?}");
        assert_eq!(
            err.mutation_outcome_unknown(),
            !retryable,
            "{case}: {err:?}"
        );
        if retryable {
            assert!(
                matches!(&err, Error::AdmissionUnavailable { .. }),
                "{case}: {err:?}"
            );
            assert_eq!(err.answer().expect("the answer").status, 503, "{case}");
            assert_eq!(err.retry_after(), Some(2.0), "{case}");
        } else {
            assert!(
                !matches!(&err, Error::AdmissionUnavailable { .. }),
                "{case}: {err:?}"
            );
        }
        assert_eq!(seen.lock().unwrap().len(), 1, "{case}: sent once");
    }
}

#[tokio::test]
async fn batch_transfer_sends_exact_legs_and_reads_each_outcome() {
    let leg_result = |index: u32, status: InternalTransferBatchLegStatus, flow: &str| {
        InternalTransferBatchLegResult {
            leg_index: index,
            status: status.into(),
            transfer_id: format!("9264062596720766180315183337697806858{index}"),
            flow_id: flow.into(),
            asset_id: 1,
            amount_e18: u128(500_000_000_000_000_000).into(),
            ..Default::default()
        }
    };
    let (client, seen) = server(move |path| match path {
        "/transfer.v1.InternalTransferService/BatchCreateInternalTransfers" => {
            connect_proto_ok(&BatchCreateInternalTransfersResponse {
                request_id: BATCH_ID.into(),
                status: InternalTransferBatchStatus::BATCH_PROCESSING.into(),
                legs: vec![
                    leg_result(0, InternalTransferBatchLegStatus::LEG_PROCESSING, ""),
                    leg_result(1, InternalTransferBatchLegStatus::LEG_PROCESSING, ""),
                ],
                ..Default::default()
            })
        }
        _ => connect_proto_ok(&GetInternalTransferBatchResponse {
            request_id: BATCH_ID.into(),
            status: InternalTransferBatchStatus::BATCH_ACCEPTED.into(),
            legs: vec![
                InternalTransferBatchLegResult {
                    credited_at_ts_ns: 1_763_330_000_000_000_000,
                    asset_code: "BTC".into(),
                    destination: ResolvedDestination {
                        root_account_public_id: "AAo8yKqygTg".into(),
                        smart_account_address: "0x2222222222222222222222222222222222222222".into(),
                        ..Default::default()
                    }
                    .into(),
                    ..leg_result(0, InternalTransferBatchLegStatus::LEG_CREDITED, "flow_t0")
                },
                InternalTransferBatchLegResult {
                    credited_at_ts_ns: 1_763_330_000_000_000_001,
                    asset_code: "BTC".into(),
                    destination: ResolvedDestination {
                        root_account_public_id: "AAo8yKqygTg".into(),
                        smart_account_address: "0x2222222222222222222222222222222222222222".into(),
                        ..Default::default()
                    }
                    .into(),
                    ..leg_result(1, InternalTransferBatchLegStatus::LEG_CREDITED, "flow_t1")
                },
            ],
            ..Default::default()
        }),
    })
    .await;
    let mut second = leg(3, "0.000000000000000001");
    second.subaccount_id = SubAccountScope::Main;
    second.destination_smart_account_address = None;
    second.destination_subaccount_id = Some("Bht3tfQZv8E".into());
    let result = client
        .internal_transfers
        .batch_create(BatchCreateInternalTransfersParams {
            legs: vec![leg(1, "0.5"), second],
            idempotency_key: "rebalance-1".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        (result.request_id.as_str(), result.status.as_str()),
        (BATCH_ID, "processing")
    );
    assert_eq!(result.legs[0].status, "processing");
    assert_eq!(result.legs[1].flow_id, "");
    assert_eq!(
        result.legs[0].quantity.as_ref().map(AssetAmount::as_scaled),
        Some(500_000_000_000_000_000)
    );

    let read = client
        .internal_transfers
        .get_batch(&result.request_id)
        .await
        .unwrap();
    assert_eq!(read.status, "accepted");
    let legs: Vec<_> = read
        .legs
        .iter()
        .map(|l| (l.status.as_str(), l.flow_id.as_str(), l.credited_at_ts_ns))
        .collect();
    assert_eq!(
        legs,
        [
            ("credited", "flow_t0", 1_763_330_000_000_000_000),
            ("credited", "flow_t1", 1_763_330_000_000_000_001)
        ]
    );

    let sent = requests(&seen);
    assert_eq!(sent.len(), 2);
    let (path, signed, body) = &sent[0];
    assert_eq!(
        path,
        "/transfer.v1.InternalTransferService/BatchCreateInternalTransfers"
    );
    assert!(signed);
    let req = BatchCreateInternalTransfersRequest::decode_from_slice(body).unwrap();
    assert_eq!(req.idempotency_key, "rebalance-1");
    assert_eq!(req.legs.len(), 2);
    let amount = |i: usize| {
        let a = req.legs[i].amount_e18.as_option().unwrap();
        (u128::from(a.hi) << 64) | u128::from(a.lo)
    };
    assert_eq!(amount(0), 500_000_000_000_000_000);
    assert_eq!(amount(1), 1);
    assert_eq!(req.legs[0].subaccount_id, default_sub());
    assert_eq!(req.legs[1].subaccount_id, 0);
    assert!(matches!(
        req.legs[1].destination,
        Some(internal_transfer_batch_leg::Destination::DestinationSubaccountId(id)) if id == default_sub()
    ));
    let (path, signed, body) = &sent[1];
    assert_eq!(
        path,
        "/transfer.v1.InternalTransferService/GetInternalTransferBatch"
    );
    assert!(signed);
    let req = GetInternalTransferBatchRequest::decode_from_slice(body).unwrap();
    assert_eq!(req.request_id, BATCH_ID);
}

/// A batch the contract refuses is refused before sending: no legs, more than
/// 500, a bad idempotency key, a leg without an asset or with other than one
/// destination, and an amount finer than 18 decimals. A batch read refuses a
/// request ID that breaks the contract's `^[1-9][0-9]*$` (1 to 39 digits).
#[tokio::test]
async fn batch_transfer_refuses_out_of_contract_input_before_sending() {
    let (client, seen) = server(|_| HttpScript::NotFound).await;
    let params =
        |legs: Vec<InternalTransferBatchLeg>, key: &str| BatchCreateInternalTransfersParams {
            legs,
            idempotency_key: key.into(),
        };
    let mut no_asset = leg(1, "1");
    no_asset.asset_id = 0;
    let mut two_destinations = leg(1, "1");
    two_destinations.destination_account_id = Some("Bht3tfQZv8E".into());
    let mut none = leg(1, "1");
    none.destination_smart_account_address = None;
    let mut fine = leg(1, "1");
    fine.quantity =
        AssetAmount::from_scaled(1, Some(19), QuantityDomain::LedgerE18, Some(1)).unwrap();
    let cases = vec![
        ("no legs", params(vec![], "k")),
        ("501 legs", params(vec![leg(1, "1"); 501], "k")),
        ("empty key", params(vec![leg(1, "1")], "")),
        ("long key", params(vec![leg(1, "1")], &"k".repeat(129))),
        ("no asset", params(vec![no_asset], "k")),
        ("two destinations", params(vec![two_destinations], "k")),
        ("no destination", params(vec![none], "k")),
        ("19 decimals", params(vec![fine], "k")),
    ];
    for (label, params) in cases {
        let err = client
            .internal_transfers
            .batch_create(params)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{label}: {err:?}");
    }
    let forty_digits = "1".repeat(40);
    for request_id in [
        "",
        "0",
        "01",
        " 1",
        "1 ",
        "12a",
        "-1",
        "١",
        forty_digits.as_str(),
    ] {
        let err = client
            .internal_transfers
            .get_batch(request_id)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "{request_id:?}: {err:?}"
        );
    }
    assert!(requests(&seen).is_empty(), "refused calls were sent");

    // 500 legs and a 128-character key reach the wire (the server 404s), and
    // so does a 39-digit request ID.
    let most = params(vec![leg(1, "1"); 500], &"k".repeat(128));
    assert!(client.internal_transfers.batch_create(most).await.is_err());
    assert!(
        client
            .internal_transfers
            .get_batch(&"9".repeat(39))
            .await
            .is_err()
    );
    assert_eq!(requests(&seen).len(), 2);
}

/// Quote, convert and list dust: signed, scoped, and refused before sending
/// where the contract refuses.
#[tokio::test]
async fn dust_conversion_round_trip() {
    let (client, seen) = server(|path| match path {
        "/transfer.v1.DustConversionService/QuoteDustConversion" => {
            connect_proto_ok(&QuoteDustConversionResponse {
                quote_id: "dq_1".into(),
                target_asset_id: 3,
                payout_e18: u128(5_390_000_000_000_000_000).into(),
                ..Default::default()
            })
        }
        "/transfer.v1.DustConversionService/CreateDustConversion" => {
            connect_proto_ok(&CreateDustConversionResponse {
                conversion: DustConversion {
                    conversion_id: "42".into(),
                    quote_id: "dq_1".into(),
                    state: DustConversionState::DUST_CONVERSION_STATE_PROCESSING.into(),
                    target_asset_id: 3,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            })
        }
        _ => connect_proto_ok(&ListDustConversionsResponse::default()),
    })
    .await;
    let quote = client
        .dust
        .quote(3, SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(quote.quote_id.as_deref(), Some("dq_1"));
    assert_eq!(quote.payout.as_scaled(), 5_390_000_000_000_000_000);
    let conversion = client
        .dust
        .convert("dq_1", "dust-1", SubAccountScope::Id(5))
        .await
        .unwrap();
    assert_eq!(
        (conversion.conversion_id.as_str(), conversion.state.as_str()),
        ("42", "processing")
    );
    client
        .dust
        .list_conversions(100, Some("p"), SubAccountScope::Main)
        .await
        .unwrap();

    let sent = requests(&seen);
    assert_eq!(sent.len(), 3);
    assert!(sent.iter().all(|(_, signed, _)| *signed));
    let q = QuoteDustConversionRequest::decode_from_slice(&sent[0].2).unwrap();
    assert_eq!((q.subaccount_id, q.target_asset_id), (default_sub(), 3));
    let c = CreateDustConversionRequest::decode_from_slice(&sent[1].2).unwrap();
    assert_eq!(
        (
            c.subaccount_id,
            c.quote_id.as_str(),
            c.idempotency_key.as_str()
        ),
        (5, "dq_1", "dust-1")
    );
    let l = ListDustConversionsRequest::decode_from_slice(&sent[2].2).unwrap();
    assert_eq!(
        (l.subaccount_id, l.limit, l.page_token.as_str()),
        (0, 100, "p")
    );

    for err in [
        client
            .dust
            .quote(0, SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .convert("", "k", SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .convert(&"q".repeat(65), "k", SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .convert("dq", &"k".repeat(129), SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .get_conversion("042", SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .get_conversion("4x", SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .get_conversion(&"9".repeat(40), SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .list_conversions(101, None, SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .list_conversions(0, Some(&"p".repeat(257)), SubAccountScope::Default)
            .await
            .map(|_| ()),
        client
            .dust
            .list_balances(0, SubAccountScope::Id(0))
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(err, Err(Error::Validation(_))), "{err:?}");
    }
    assert!(requests(&seen).is_empty(), "refused calls were sent");
}

fn export(id: u64) -> StatementExport {
    StatementExport {
        export_id: id,
        status: StatementExportStatus::STATEMENT_EXPORT_STATUS_QUEUED.into(),
        idempotency_key: "statement-2026".into(),
        ..Default::default()
    }
}

/// A statement export is created and listed with signed requests; its period
/// and idempotency key are refused before sending where the contract refuses.
#[tokio::test]
async fn statement_export_files_are_created_and_listed() {
    let (client, seen) = server(|path| match path {
        "/ledger.read.v1.StatementExportService/CreateStatementExport" => {
            connect_proto_ok(&CreateStatementExportResponse {
                export: export(7).into(),
                ..Default::default()
            })
        }
        _ => connect_proto_ok(&ListStatementExportsResponse {
            exports: vec![export(7)],
            in_progress_exports: 1,
            max_in_progress_exports: 5,
            ..Default::default()
        }),
    })
    .await;
    let start = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
    let opts = |end_secs: u64, end_nanos: u32, key: &str| CreateStatementExportOptions {
        start_time: Some(start),
        end_time: Some(start + Duration::new(end_secs, end_nanos)),
        idempotency_key: key.into(),
        ..Default::default()
    };
    let max = 366 * 86_400;
    let created = client
        .statements
        .create_export(opts(max, 0, "statement-2026:a_b.c"))
        .await
        .unwrap();
    assert_eq!(
        (created.status.as_str(), created.export_id.is_empty()),
        ("queued", false)
    );
    let list = client
        .statements
        .list_exports(SubAccountScope::Main, 100, None)
        .await
        .unwrap();
    assert_eq!(list.exports.len(), 1);
    assert_eq!(
        (list.in_progress_exports, list.max_in_progress_exports),
        (1, 5)
    );
    let sent = requests(&seen);
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|(_, signed, _)| *signed));
    let c = CreateStatementExportRequest::decode_from_slice(&sent[0].2).unwrap();
    assert_eq!(c.subaccount_id, Some(default_sub()));
    assert_eq!(c.idempotency_key, "statement-2026:a_b.c");
    let end = c.end_time.as_option().unwrap();
    assert_eq!(end.seconds, 1_767_225_600 + max as i64);
    let l = ListStatementExportsRequest::decode_from_slice(&sent[1].2).unwrap();
    assert_eq!((l.subaccount_id, l.limit), (None, 100));

    for (label, opts) in [
        ("longer than 366 days", opts(max, 1, "k")),
        ("empty period", opts(0, 0, "k")),
        ("empty key", opts(60, 0, "")),
        ("long key", opts(60, 0, &"k".repeat(65))),
        ("slash in key", opts(60, 0, "a/b")),
        ("space in key", opts(60, 0, "a b")),
        (
            "no start",
            CreateStatementExportOptions {
                start_time: None,
                ..opts(60, 0, "k")
            },
        ),
    ] {
        let err = client.statements.create_export(opts).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{label}: {err:?}");
    }
    for err in [
        client
            .statements
            .list_exports(SubAccountScope::Default, 101, None)
            .await
            .map(|_| ()),
        client
            .statements
            .list_exports(SubAccountScope::Default, 0, Some(&"p".repeat(513)))
            .await
            .map(|_| ()),
        client.statements.get_export("1").await.map(|_| ()),
    ] {
        assert!(matches!(err, Err(Error::Validation(_))), "{err:?}");
    }
    assert!(requests(&seen).is_empty(), "refused calls were sent");
}

/// Balance history reads each point's exact 18-decimal balance, and a server
/// that still sends the removed 7-decimal field (an empty `balance`) is
/// refused instead of read as no balances.
#[tokio::test]
async fn balance_history_is_exact_and_refuses_the_previous_contract() {
    for (balance, ok) in [
        (
            vec![u128(1_844_674_407_370_955_161_549_999_999_999_999)],
            true,
        ),
        (vec![], false),
    ] {
        let reply = GetBalanceHistoryResponse {
            points: 1,
            series: vec![BalanceSeries {
                asset_id: 2,
                balance,
                ..Default::default()
            }],
            ..Default::default()
        };
        let (client, _seen) = server(move |_| connect_proto_ok(&reply)).await;
        let result = client
            .balances
            .get_balance_history(GetBalanceHistoryRequest::default())
            .await;
        if ok {
            let history = result.unwrap();
            let value = &history.series[0].balance[0];
            assert_eq!(
                value.as_scaled(),
                1_844_674_407_370_955_161_549_999_999_999_999
            );
            assert_eq!(value.scale(), Some(18));
        } else {
            assert!(
                matches!(result, Err(Error::ResponseContract { .. })),
                "{result:?}"
            );
        }
    }
}

/// `ROUTE_SUSPENDED` is a final refusal over ConnectRPC and REST: not
/// retryable and not an unknown outcome.
#[tokio::test]
async fn route_suspended_withdrawal_is_final() {
    use polyester::proto::chain::withdraw::v1::{ErrorCode, ErrorDetail};
    let connect = connect_error(
        "failed_precondition",
        "chain.withdraw.v1.ErrorDetail",
        ErrorDetail {
            code: ErrorCode::ERROR_CODE_ROUTE_SUSPENDED.into(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let rest = || {
        let body =
            json!({"code": "ROUTE_SUSPENDED", "status": 400, "detail": "suspended"}).to_string();
        HttpScript::Raw {
            status: 400,
            headers: vec![
                ("Content-Type".into(), "application/problem+json".into()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body: body.into_bytes(),
        }
    };
    let connect = Arc::new(Mutex::new(Some(connect)));
    for shape in ["connect", "rest"] {
        let script = if shape == "connect" {
            connect.lock().unwrap().take().unwrap()
        } else {
            rest()
        };
        let script = Arc::new(Mutex::new(Some(script)));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert!(err.is_withdraw_route_suspended(), "{shape}: {err:?}");
        assert!(!err.is_retryable(), "{shape}: {err:?}");
        assert!(!err.mutation_outcome_unknown(), "{shape}: {err:?}");
        assert!(!err.is_withdraw_destination_cooling_down());
    }
}

/// Dust and statement export refusals are classified by their retry rule:
/// only `DUST_BUSY` is retried; the window, daily and export limits are
/// final although they are `resource_exhausted`.
#[tokio::test]
async fn dust_and_export_refusals_follow_their_retry_rule() {
    use polyester::proto::ledger::read::v1::{
        ErrorCode as LedgerCode, ErrorDetail as LedgerDetail,
    };
    use polyester::proto::transfer::v1::{ErrorCode, ErrorDetail};
    let transfer = |code: ErrorCode| {
        ErrorDetail {
            code: code.into(),
            ..Default::default()
        }
        .encode_to_vec()
    };
    let cases: Vec<(&str, &str, Vec<u8>, &str, bool)> = vec![
        (
            "resource_exhausted",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_BUSY),
            "ERROR_CODE_DUST_BUSY",
            true,
        ),
        (
            "resource_exhausted",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_WINDOW_ACTIVE),
            "ERROR_CODE_DUST_WINDOW_ACTIVE",
            false,
        ),
        (
            "resource_exhausted",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_DAILY_LIMIT),
            "ERROR_CODE_DUST_DAILY_LIMIT",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_QUOTE_EXPIRED),
            "ERROR_CODE_DUST_QUOTE_EXPIRED",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_PRICE_MOVED),
            "ERROR_CODE_DUST_PRICE_MOVED",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_BALANCE_CHANGED),
            "ERROR_CODE_DUST_BALANCE_CHANGED",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_NOT_ELIGIBLE),
            "ERROR_CODE_DUST_NOT_ELIGIBLE",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_CONVERSION_FAILED),
            "ERROR_CODE_DUST_CONVERSION_FAILED",
            false,
        ),
        (
            "failed_precondition",
            "transfer.v1.ErrorDetail",
            transfer(ErrorCode::ERROR_CODE_DUST_TARGET_UNAVAILABLE),
            "ERROR_CODE_DUST_TARGET_UNAVAILABLE",
            false,
        ),
        (
            "resource_exhausted",
            "ledger.read.v1.ErrorDetail",
            LedgerDetail {
                code: LedgerCode::ERROR_CODE_STATEMENT_EXPORT_LIMIT_REACHED.into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "ERROR_CODE_STATEMENT_EXPORT_LIMIT_REACHED",
            false,
        ),
        (
            "failed_precondition",
            "ledger.read.v1.ErrorDetail",
            LedgerDetail {
                code: LedgerCode::ERROR_CODE_IDEMPOTENCY_KEY_REUSED.into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "ERROR_CODE_IDEMPOTENCY_KEY_REUSED",
            false,
        ),
    ];
    for (code, detail_type, detail, want, retryable) in cases {
        let script = Arc::new(Mutex::new(Some(connect_error(code, detail_type, detail))));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert!(
            matches!(&err, Error::Api { code, .. } if code == want),
            "{want}: {err:?}"
        );
        assert_eq!(err.is_retryable(), retryable, "{want}: {err:?}");
        assert!(!err.mutation_outcome_unknown(), "{want}: {err:?}");
    }
    // The same codes as REST problems.
    for (status, code, want, retryable) in [
        (429, "DUST_BUSY", "ERROR_CODE_DUST_BUSY", true),
        (
            429,
            "DUST_WINDOW_ACTIVE",
            "ERROR_CODE_DUST_WINDOW_ACTIVE",
            false,
        ),
        (
            429,
            "DUST_DAILY_LIMIT",
            "ERROR_CODE_DUST_DAILY_LIMIT",
            false,
        ),
        (
            429,
            "STATEMENT_EXPORT_LIMIT_REACHED",
            "ERROR_CODE_STATEMENT_EXPORT_LIMIT_REACHED",
            false,
        ),
    ] {
        let body = json!({"code": code, "status": status, "detail": "refused"}).to_string();
        let script = Arc::new(Mutex::new(Some(HttpScript::Raw {
            status,
            headers: vec![
                ("Content-Type".into(), "application/problem+json".into()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body: body.into_bytes(),
        })));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert!(
            matches!(&err, Error::Api { code, .. } if code == want),
            "{code}: {err:?}"
        );
        assert_eq!(err.is_retryable(), retryable, "{code}: {err:?}");
        assert!(!err.mutation_outcome_unknown(), "{code}: {err:?}");
    }
}

/// A ladder order refused for the account's rate limit is a rate-limit
/// error that still names the refused order's cause and zero-based index;
/// index 0 is kept as `Some(0)`.
#[tokio::test]
async fn rate_limited_ladder_keeps_cause_and_level() {
    use polyester::proto::orders::v1::{ErrorCode, ErrorDetail};
    use polyester::proto::polyester::ratelimit::v1::RateLimitDetail;
    for index in [2u32, 0] {
        let detail = ErrorDetail {
            code: ErrorCode::ERROR_CODE_LADDER_LEVEL_REJECTED.into(),
            cause_code: ErrorCode::ERROR_CODE_RATE_LIMIT_EXCEEDED.into(),
            level_index: Some(index),
            rate_limit: RateLimitDetail {
                retry_after_ms: Some(1_500),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .encode_to_vec();
        let script = Arc::new(Mutex::new(Some(connect_error(
            "resource_exhausted",
            "orders.v1.ErrorDetail",
            detail,
        ))));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert!(
            matches!(&err, Error::RateLimit { code: Some(code), .. } if code == "ERROR_CODE_LADDER_LEVEL_REJECTED"),
            "{err:?}"
        );
        assert_eq!(err.retry_after(), Some(1.5), "{err:?}");
        assert!(err.is_retryable(), "{err:?}");
        assert_eq!(
            err.cause_code(),
            Some("ERROR_CODE_RATE_LIMIT_EXCEEDED"),
            "{err:?}"
        );
        assert_eq!(err.level_index(), Some(index), "{err:?}");
    }
}

/// A ladder refused for one of its orders names that order's own code and
/// its zero-based index; 0 is a real index. Other refusals carry neither.
#[tokio::test]
async fn ladder_refusal_names_the_cause_and_level() {
    use polyester::proto::orders::v1::{ErrorCode, ErrorDetail};
    for (cause, index) in [
        (
            Some(ErrorCode::ERROR_CODE_MAX_OPEN_ORDERS_PER_PAIR),
            Some(0),
        ),
        (Some(ErrorCode::ERROR_CODE_PRICE_BAND_VIOLATION), Some(3)),
        (None, None),
    ] {
        let detail = ErrorDetail {
            code: if cause.is_some() {
                ErrorCode::ERROR_CODE_LADDER_LEVEL_REJECTED.into()
            } else {
                ErrorCode::ERROR_CODE_INVALID_SIDE.into()
            },
            cause_code: cause.map(Into::into).unwrap_or_default(),
            level_index: index,
            ..Default::default()
        }
        .encode_to_vec();
        let script = Arc::new(Mutex::new(Some(connect_error(
            "invalid_argument",
            "orders.v1.ErrorDetail",
            detail,
        ))));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert_eq!(
            err.cause_code(),
            cause.map(|c| buffa::Enumeration::proto_name(&c)),
            "{err:?}"
        );
        assert_eq!(err.level_index(), index, "{err:?}");
    }
}

/// A social link refusal says when the same request may succeed.
#[tokio::test]
async fn social_cooldown_carries_retry_at() {
    use polyester::proto::auth::v1::{AuthErrorCode, AuthErrorDetail};
    for code in [
        AuthErrorCode::AUTH_SOCIAL_ACCOUNT_COOLDOWN,
        AuthErrorCode::AUTH_SOCIAL_CHANGE_LIMIT_REACHED,
    ] {
        let detail = AuthErrorDetail {
            code: code.into(),
            retry_after: buffa_types::google::protobuf::Timestamp {
                seconds: 1_795_000_000,
                nanos: 250,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .encode_to_vec();
        let script = Arc::new(Mutex::new(Some(connect_error(
            "failed_precondition",
            "auth.v1.AuthErrorDetail",
            detail,
        ))));
        let (client, _) = server(move |_| {
            script
                .lock()
                .unwrap()
                .take()
                .unwrap_or(HttpScript::NotFound)
        })
        .await;
        let err = client.auth.me().await.unwrap_err();
        assert_eq!(
            err.auth_error_code(),
            Some(buffa::Enumeration::proto_name(&code))
        );
        assert_eq!(
            err.retry_at(),
            Some(UNIX_EPOCH + Duration::new(1_795_000_000, 250)),
            "{err:?}"
        );
        assert!(!err.is_retryable());
    }
}
