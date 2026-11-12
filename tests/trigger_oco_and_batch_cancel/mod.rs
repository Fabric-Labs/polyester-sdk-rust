//! OCO create and cancel and the batch trigger cancel through the public
//! `triggers` service against a local server, in binary and JSON: what each
//! call sends, what it returns, that a retry sends the identical request, and
//! that a refused call sends nothing.

use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::models::{CreateOcoParams, CreateSide, OcoLegParams};
use polyester::proto::orders::v1::{
    ErrorDetail, FieldViolation, Side, TriggerPriceSource, risk_execution,
};
use polyester::proto::polyester::ratelimit::v1::RateLimitDetail;
use polyester::proto::triggers::v1::{
    BatchCancelTriggersRequest, BatchCancelTriggersResponse, BatchCancelTriggersResultItem,
    CancelOcoRequest, CancelOcoResponse, CreateOcoRequest, CreateOcoResponse, TriggerStatus,
    batch_cancel_triggers_result_item,
};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error, Price, Quantity};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const CREATE_OCO: &str = "/triggers.v1.TriggersService/CreateOco";
const CANCEL_OCO: &str = "/triggers.v1.TriggersService/CancelOco";
const BATCH_CANCEL: &str = "/triggers.v1.TriggersService/BatchCancelTriggers";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];

/// A Connect answer in the client's wire format.
fn answer<M: buffa::Message + serde::Serialize>(wire: WireFormat, msg: &M) -> HttpScript {
    let (content_type, body) = match wire {
        WireFormat::Binary => ("application/proto", msg.encode_to_vec()),
        WireFormat::Json => ("application/json", serde_json::to_vec(msg).unwrap()),
    };
    HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), content_type.into())],
        body,
    }
}

/// A Connect request body in the client's wire format.
fn request<M: buffa::Message + serde::de::DeserializeOwned + Default>(
    wire: WireFormat,
    body: &[u8],
) -> M {
    match wire {
        WireFormat::Binary => M::decode_from_slice(body).expect("binary request"),
        WireFormat::Json => serde_json::from_slice(body).expect("JSON request"),
    }
}

fn rejected_item() -> BatchCancelTriggersResultItem {
    BatchCancelTriggersResultItem {
        status: batch_cancel_triggers_result_item::Status::Rejected.into(),
        trigger_id: 40,
        code: "RATE_LIMIT_EXCEEDED".into(),
        error: ErrorDetail {
            rate_limit: RateLimitDetail {
                retry_after_ms: Some(250),
                ..Default::default()
            }
            .into(),
            violations: vec![FieldViolation {
                field_path: "trigger_ids[1]".into(),
                rule_id: "rate_limit".into(),
                message: "Too many trigger cancels.".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

struct Server {
    client: Client,
    http: MockHttpServer,
    /// Raw request bodies, so a retry can be compared byte for byte.
    creates: Arc<Mutex<Vec<Vec<u8>>>>,
    cancels: Arc<Mutex<Vec<CancelOcoRequest>>>,
    batches: Arc<Mutex<Vec<BatchCancelTriggersRequest>>>,
}

impl Server {
    fn sent(&self) -> usize {
        self.http.requests.load(Ordering::SeqCst)
    }
}

async fn server(wire: WireFormat) -> Server {
    let creates = Arc::new(Mutex::new(Vec::new()));
    let cancels = Arc::new(Mutex::new(Vec::new()));
    let batches = Arc::new(Mutex::new(Vec::new()));
    let (c, x, b) = (creates.clone(), cancels.clone(), batches.clone());
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        CREATE_OCO => {
            c.lock().unwrap().push(req.body.clone());
            let sent: CreateOcoRequest = request(wire, &req.body);
            answer(
                wire,
                &CreateOcoResponse {
                    order_list_id: 70,
                    client_list_id: sent.oco.as_option().unwrap().client_list_id.clone(),
                    take_profit_trigger_id: 71,
                    stop_loss_trigger_id: 72,
                    accepted_at_ts_ns: 1_700_000_000_000_000_000,
                    ..Default::default()
                },
            )
        }
        CANCEL_OCO => {
            let sent: CancelOcoRequest = request(wire, &req.body);
            let order_list_id = sent.order_list_id;
            x.lock().unwrap().push(sent);
            answer(
                wire,
                &CancelOcoResponse {
                    order_list_id,
                    status: TriggerStatus::StatusCanceled.into(),
                    ..Default::default()
                },
            )
        }
        BATCH_CANCEL => {
            b.lock().unwrap().push(request(wire, &req.body));
            answer(
                wire,
                &BatchCancelTriggersResponse {
                    results: vec![
                        BatchCancelTriggersResultItem {
                            status: batch_cancel_triggers_result_item::Status::Accepted.into(),
                            trigger_id: 42,
                            ..Default::default()
                        },
                        rejected_item(),
                    ],
                    accepted_count: 1,
                    rejected_count: 1,
                    ..Default::default()
                },
            )
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: wire,
        ..Default::default()
    })
    .expect("client");
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 5,
                "quote_quantity_scale": 6
            }]
        }))
        .expect("catalog");
    Server {
        client,
        http,
        creates,
        cancels,
        batches,
    }
}

fn id(raw: u64) -> String {
    polyester::codecs::format_uint64_id(raw)
}

/// A sell OCO over 0.002 BTC: a limit take-profit at 65000 (limit 64990) and
/// a market stop-loss at 58000 watching the index price.
fn sell_oco() -> CreateOcoParams {
    let qty = Quantity::from_decimal_str("0.002", 5, Some("BTC-USDT".into()), Some(1)).unwrap();
    let mut stop_loss = OcoLegParams::market(Price::from_decimal_str("58000", None).unwrap());
    stop_loss.trigger_price_source = Some("index".into());
    let mut params = CreateOcoParams::new(
        "BTC-USDT",
        CreateSide::Sell,
        qty,
        OcoLegParams::limit(
            Price::from_decimal_str("65000", None).unwrap(),
            Price::from_decimal_str("64990", None).unwrap(),
        ),
        stop_loss,
    );
    params.client_list_id = "oco-btc-1".into();
    params
}

/// `create_oco` sends the OCO's market, side, quantity, client OCO ID and both
/// legs, and returns the OCO's ID and its legs' trigger IDs. A retry with the
/// same params and `client_list_id` sends the identical request, which the
/// server answers with the same OCO.
#[tokio::test]
async fn create_oco_sends_both_legs_and_a_retry_sends_the_same_request() {
    for wire in WIRES {
        let s = server(wire).await;
        let first = s
            .client
            .triggers
            .create_oco(sell_oco())
            .await
            .expect("create");
        assert_eq!(first.order_list_id, id(70), "{wire:?}");
        assert_eq!(first.client_list_id, "oco-btc-1", "{wire:?}");
        assert_eq!(first.take_profit_trigger_id, id(71), "{wire:?}");
        assert_eq!(first.stop_loss_trigger_id, id(72), "{wire:?}");

        let creates = s.creates.lock().unwrap().clone();
        let sent: CreateOcoRequest = request(wire, &creates[0]);
        assert_eq!(sent.subaccount_id, None, "{wire:?}");
        let oco = sent.oco.as_option().expect("oco");
        assert_eq!(oco.symbol_id, 1, "{wire:?}");
        assert_eq!(oco.side, Side::Sell, "{wire:?}");
        assert_eq!(oco.qty_scaled, 200, "{wire:?}");
        assert_eq!(oco.client_list_id, "oco-btc-1", "{wire:?}");
        let tp = oco.take_profit.as_option().expect("take_profit");
        assert_eq!(tp.trigger_price_ticks, 65_000_000_000_000, "{wire:?}");
        match tp.child.as_option().and_then(|c| c.execution.as_ref()) {
            Some(risk_execution::Execution::LimitGtc(limit)) => {
                assert_eq!(limit.price_ticks, 64_990_000_000_000, "{wire:?}");
            }
            other => panic!("{wire:?} take_profit child: {other:?}"),
        }
        let sl = oco.stop_loss.as_option().expect("stop_loss");
        assert_eq!(sl.trigger_price_ticks, 58_000_000_000_000, "{wire:?}");
        assert_eq!(
            sl.trigger_price_source,
            TriggerPriceSource::IndexPrice,
            "{wire:?}"
        );
        assert!(
            matches!(
                sl.child.as_option().and_then(|c| c.execution.as_ref()),
                Some(risk_execution::Execution::MarketIoc(_))
            ),
            "{wire:?} stop_loss child: {:?}",
            sl.child
        );

        let retry = s
            .client
            .triggers
            .create_oco(sell_oco())
            .await
            .expect("retry");
        assert_eq!(retry, first, "{wire:?}");
        let creates = s.creates.lock().unwrap().clone();
        assert_eq!(creates.len(), 2, "{wire:?}");
        assert_eq!(
            creates[1], creates[0],
            "{wire:?}: a retry must send the same OCO"
        );
    }
}

/// A create the SDK can refuse is refused with `Error::Validation` before
/// anything is sent: legs the wrong way round for the side, a missing client
/// OCO ID, a limit leg without its limit price.
#[tokio::test]
async fn create_oco_refuses_invalid_params_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        let mut swapped = sell_oco();
        swapped.side = CreateSide::Buy;
        let mut no_id = sell_oco();
        no_id.client_list_id = String::new();
        let mut no_limit = sell_oco();
        no_limit.take_profit.limit_price = None;
        for (params, want) in [
            (swapped, "below its stop_loss"),
            (no_id, "client_list_id"),
            (no_limit, "limit_price"),
        ] {
            let err = s
                .client
                .triggers
                .create_oco(params)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            assert!(err.to_string().contains(want), "{wire:?}: {err}");
        }
        assert_eq!(s.sent(), 0, "{wire:?}: a refused create must send nothing");
    }
}

/// `cancel_oco_by_id` sends the OCO's ID, in base58 or decimal, and returns
/// it with both legs' status; an ID that is not one is refused before
/// sending.
#[tokio::test]
async fn cancel_oco_sends_the_oco_id() {
    for wire in WIRES {
        let s = server(wire).await;
        for raw in [id(70), "70".into()] {
            let canceled = s
                .client
                .triggers
                .cancel_oco_by_id(&raw, Some(3))
                .await
                .expect("cancel");
            assert_eq!(canceled.order_list_id, id(70), "{wire:?} {raw}");
            assert_eq!(canceled.status, "canceled", "{wire:?} {raw}");
            let sent = s.cancels.lock().unwrap().last().cloned().unwrap();
            assert_eq!(sent.order_list_id, 70, "{wire:?} {raw}");
            assert_eq!(sent.subaccount_id, Some(3), "{wire:?} {raw}");
        }
        let sent = s.sent();
        for bad in ["", "0", "not an id"] {
            let err = s
                .client
                .triggers
                .cancel_oco_by_id(bad, None)
                .await
                .expect_err("invalid OCO ID");
            assert!(
                matches!(err, Error::Validation(_)),
                "{wire:?} {bad:?}: {err}"
            );
            assert!(err.to_string().contains("order_list_id"), "{err}");
        }
        assert_eq!(
            s.sent(),
            sent,
            "{wire:?}: a refused cancel must send nothing"
        );
    }
}

/// `batch_cancel` sends each trigger ID with the caller's `request_id`, and
/// returns one result per trigger in request order: an accepted one, and a
/// rejected one with its code, rate-limit guidance and field violations. A
/// retry with the same `request_id` sends the same request.
#[tokio::test]
async fn batch_cancel_returns_each_triggers_result() {
    for wire in WIRES {
        let s = server(wire).await;
        // A `0` keeps a decimal ID from reading as base58.
        let ids = vec![id(42), "40".to_string()];
        let result = s
            .client
            .triggers
            .batch_cancel(ids.clone(), None, Some("cancel-run-1".into()))
            .await
            .expect("batch cancel");
        assert_eq!((result.accepted_count, result.rejected_count), (1, 1));
        let [accepted, rejected] = result.results.as_slice() else {
            panic!("{wire:?}: {:?}", result.results);
        };
        assert_eq!(accepted.status, "accepted", "{wire:?}");
        assert_eq!(accepted.trigger_id, id(42), "{wire:?}");
        assert_eq!(accepted.code, "", "{wire:?}");
        assert!(accepted.rate_limit.is_none() && accepted.violations.is_empty());
        assert_eq!(rejected.status, "rejected", "{wire:?}");
        assert_eq!(rejected.trigger_id, id(40), "{wire:?}");
        assert_eq!(rejected.code, "RATE_LIMIT_EXCEEDED", "{wire:?}");
        assert_eq!(
            rejected.rate_limit.as_ref().and_then(|r| r.retry_after_ms),
            Some(250),
            "{wire:?}"
        );
        assert_eq!(rejected.violations.len(), 1, "{wire:?}");
        assert_eq!(rejected.violations[0].field_path, "trigger_ids[1]");

        s.client
            .triggers
            .batch_cancel(ids, None, Some("cancel-run-1".into()))
            .await
            .expect("retry");
        let batches = s.batches.lock().unwrap().clone();
        assert_eq!(batches.len(), 2, "{wire:?}");
        assert_eq!(batches[0].trigger_ids, vec![42, 40], "{wire:?}");
        assert_eq!(batches[0].request_id, "cancel-run-1", "{wire:?}");
        assert_eq!(batches[0].subaccount_id, None, "{wire:?}");
        assert_eq!(
            batches[1], batches[0],
            "{wire:?}: a retry must send the same batch"
        );
    }
}

/// A batch the SDK can refuse is refused with `Error::Validation` before
/// anything is sent: no trigger, more than 50, the same trigger twice.
#[tokio::test]
async fn batch_cancel_refuses_invalid_batches_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        let fifty_one: Vec<String> = (1..=51).map(|n: u64| (n * 1000).to_string()).collect();
        for (ids, want) in [
            (Vec::new(), "at least one trigger_id"),
            (fifty_one, "at most 50"),
            (vec![id(40), "40".into()], "duplicate trigger_id"),
        ] {
            let err = s
                .client
                .triggers
                .batch_cancel(ids, None, None)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            assert!(err.to_string().contains(want), "{wire:?}: {err}");
        }
        assert_eq!(s.sent(), 0, "{wire:?}: a refused batch must send nothing");
    }
}
