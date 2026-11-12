//! OCO create and cancel, the batch trigger cancel, and the OCO fields and
//! filters of trigger reads, through the public `triggers` service against a
//! local server, in binary and JSON: what each call sends, what it returns,
//! that a retry sends the identical request, and that a refused call sends
//! nothing.

use super::hardening_support::{HttpScript, MockHttpServer, MockWsServer};
use buffa::Message as _;
use polyester::models::{CreateOcoParams, CreateSide, ListTriggersOpts, OcoLegParams, Trigger};
use polyester::proto::orders::v1::{
    ErrorDetail, FieldViolation, Side, TriggerPriceSource, risk_execution,
};
use polyester::proto::polyester::ratelimit::v1::RateLimitDetail;
use polyester::proto::triggers::v1::{
    BatchCancelTriggersRequest, BatchCancelTriggersResponse, BatchCancelTriggersResultItem,
    CancelOcoRequest, CancelOcoResponse, CreateOcoRequest, CreateOcoResponse, GetTriggerResponse,
    ListTriggersRequest, ListTriggersResponse, Trigger as ProtoTrigger, TriggerStatus,
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
const LIST: &str = "/triggers.v1.TriggersService/ListTriggers";
const GET: &str = "/triggers.v1.TriggersService/GetTrigger";

/// An OCO leg as trigger reads send it: it carries the OCO's ID and client
/// OCO ID, and has no parent order.
fn oco_leg(trigger_id: u64) -> ProtoTrigger {
    ProtoTrigger {
        trigger_id,
        symbol_id: 1,
        status: TriggerStatus::StatusArmed.into(),
        order_list_id: Some(70),
        client_list_id: "oco-btc-1".into(),
        ..Default::default()
    }
}

/// A trigger of no OCO.
fn plain_trigger() -> ProtoTrigger {
    ProtoTrigger {
        trigger_id: 73,
        symbol_id: 1,
        status: TriggerStatus::StatusArmed.into(),
        ..Default::default()
    }
}

fn listed() -> Vec<ProtoTrigger> {
    vec![oco_leg(71), oco_leg(72), plain_trigger()]
}
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
    lists: Arc<Mutex<Vec<ListTriggersRequest>>>,
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
    let lists = Arc::new(Mutex::new(Vec::new()));
    let (c, x, b, l) = (
        creates.clone(),
        cancels.clone(),
        batches.clone(),
        lists.clone(),
    );
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        LIST => {
            l.lock().unwrap().push(request(wire, &req.body));
            answer(
                wire,
                &ListTriggersResponse {
                    triggers: listed(),
                    ..Default::default()
                },
            )
        }
        GET => answer(
            wire,
            &GetTriggerResponse {
                trigger: oco_leg(71).into(),
                ..Default::default()
            },
        ),
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
        lists,
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
/// anything is sent: a missing client OCO ID, a limit leg without its limit
/// price, a price source label trigger reads do not use.
#[tokio::test]
async fn create_oco_refuses_invalid_params_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        let mut unknown_source = sell_oco();
        unknown_source.stop_loss.trigger_price_source = Some("oracle".into());
        let mut no_id = sell_oco();
        no_id.client_list_id = String::new();
        let mut no_limit = sell_oco();
        no_limit.take_profit.limit_price = None;
        for (params, want) in [
            (unknown_source, "trigger_price_source"),
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

/// Which way round the legs must be for the side, and which price sources a
/// market supports, are the server's to refuse: a BUY OCO with a sell OCO's
/// legs, and `mark` legs, are sent as given.
#[tokio::test]
async fn create_oco_sends_leg_order_and_sources_for_the_server_to_judge() {
    for wire in WIRES {
        let s = server(wire).await;
        let mut params = sell_oco();
        params.side = CreateSide::Buy;
        params.take_profit.trigger_price_source = Some("mark".into());
        params.stop_loss.trigger_price_source = Some("mark".into());
        s.client
            .triggers
            .create_oco(params)
            .await
            .expect("sent to the server");
        let creates = s.creates.lock().unwrap().clone();
        let sent: CreateOcoRequest = request(wire, &creates[0]);
        let oco = sent.oco.as_option().unwrap();
        assert_eq!(oco.side, Side::Buy, "{wire:?}");
        let tp = oco.take_profit.as_option().unwrap();
        let sl = oco.stop_loss.as_option().unwrap();
        assert!(
            tp.trigger_price_ticks > sl.trigger_price_ticks,
            "{wire:?}: legs sent as given"
        );
        assert_eq!(tp.trigger_price_source, TriggerPriceSource::MarkPrice);
        assert_eq!(sl.trigger_price_source, TriggerPriceSource::MarkPrice);
    }
}

/// Trigger reads report the OCO a take-profit or stop-loss belongs to: both
/// legs carry its public `order_list_id` and its `client_list_id`, and have
/// no parent order; any other trigger reads `None` and empty.
#[tokio::test]
async fn trigger_reads_report_the_oco_of_each_leg() {
    for wire in WIRES {
        let s = server(wire).await;
        let got = s
            .client
            .triggers
            .get_by_id(&id(71), None)
            .await
            .expect("get")
            .expect("trigger");
        assert_eq!(got.order_list_id, Some(id(70)), "{wire:?} get");
        assert_eq!(got.client_list_id, "oco-btc-1", "{wire:?} get");
        assert_eq!(got.parent_order_id, None, "{wire:?} get");

        let list = s
            .client
            .triggers
            .list_with(ListTriggersOpts::default())
            .await
            .expect("list");
        let read: Vec<_> = list
            .triggers
            .iter()
            .map(|t| (t.order_list_id.clone(), t.client_list_id.as_str()))
            .collect();
        assert_eq!(
            read,
            vec![
                (Some(id(70)), "oco-btc-1"),
                (Some(id(70)), "oco-btc-1"),
                (None, ""),
            ],
            "{wire:?} list"
        );
    }

    let ws = MockWsServer::spawn_centrifugo_publications_after_handshake(
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        vec![oco_leg(72).encode_to_vec(), plain_trigger().encode_to_vec()],
    )
    .await;
    let (client, _http) = super::streaming_client(&ws).await;
    let mut sub = client
        .triggers
        .subscribe(Some("acct"))
        .await
        .expect("subscribe");
    let streamed: Vec<Trigger> = super::recv_n(&mut sub, 2).await;
    assert_eq!(streamed[0].order_list_id, Some(id(70)), "stream");
    assert_eq!(streamed[0].client_list_id, "oco-btc-1", "stream");
    assert_eq!(streamed[1].order_list_id, None, "stream");
    assert_eq!(streamed[1].client_list_id, "", "stream");
    sub.close();
}

/// `list_with` sends the `order_list_id` and `client_list_id` filters as the
/// contract's `ListTriggersRequest` fields; without them the request is as
/// before. An OCO ID that is not one is refused before sending.
#[tokio::test]
async fn list_sends_the_oco_filters() {
    for wire in WIRES {
        let s = server(wire).await;
        s.client
            .triggers
            .list_with(ListTriggersOpts::default())
            .await
            .unwrap();
        let unfiltered = s.lists.lock().unwrap()[0].clone();
        assert_eq!(unfiltered.order_list_id, None, "{wire:?}");
        assert_eq!(unfiltered.client_list_id, None, "{wire:?}");

        // A `0` keeps the decimal ID from reading as base58.
        for raw in [id(70), "70".into()] {
            s.client
                .triggers
                .list_with(ListTriggersOpts {
                    order_list_id: Some(raw.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            let sent = s.lists.lock().unwrap().last().cloned().unwrap();
            assert_eq!(sent.order_list_id, Some(70), "{wire:?} {raw}");
            assert_eq!(sent.client_list_id, None, "{wire:?} {raw}");
        }

        s.client
            .triggers
            .list_with(ListTriggersOpts {
                client_list_id: Some("oco-btc-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let sent = s.lists.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            sent.client_list_id.as_deref(),
            Some("oco-btc-1"),
            "{wire:?}"
        );
        assert_eq!(sent.order_list_id, None, "{wire:?}");

        let sent_before = s.sent();
        for bad in ["", "0", "not an id"] {
            let err = s
                .client
                .triggers
                .list_with(ListTriggersOpts {
                    order_list_id: Some(bad.into()),
                    ..Default::default()
                })
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
            sent_before,
            "{wire:?}: a refused filter must send nothing"
        );
    }
}

/// A client OCO ID is any 1 to 36 bytes of UTF-8, matched by the server byte
/// for byte: the create and the recovery listing send the caller's key
/// unchanged, untrimmed, non-ASCII included, so the listing finds the OCO the
/// create made. A key over 36 bytes is refused before anything is sent.
#[tokio::test]
async fn create_and_list_send_the_same_client_oco_id_bytes() {
    let key = " oco-\u{00e9}t\u{00e9} 1 ";
    for wire in WIRES {
        let s = server(wire).await;
        let mut params = sell_oco();
        params.client_list_id = key.into();
        let created = s.client.triggers.create_oco(params).await.expect("create");
        assert_eq!(created.client_list_id, key, "{wire:?}");
        assert_eq!(
            created.accepted_ts_ns, 1_700_000_000_000_000_000,
            "{wire:?}"
        );
        let creates = s.creates.lock().unwrap().clone();
        let sent: CreateOcoRequest = request(wire, &creates[0]);
        let created_key = sent.oco.as_option().expect("oco").client_list_id.clone();
        assert_eq!(created_key, key, "{wire:?}");

        s.client
            .triggers
            .list_with(ListTriggersOpts {
                client_list_id: Some(key.into()),
                ..Default::default()
            })
            .await
            .expect("list");
        let listed = s.lists.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            listed.client_list_id.as_deref(),
            Some(created_key.as_str()),
            "{wire:?}"
        );

        let sent_before = s.sent();
        for bad in [String::new(), "x".repeat(37), "\u{30b9}".repeat(13)] {
            let err = s
                .client
                .triggers
                .list_with(ListTriggersOpts {
                    client_list_id: Some(bad.clone()),
                    ..Default::default()
                })
                .await
                .expect_err("invalid client OCO ID");
            assert!(
                matches!(err, Error::Validation(_)),
                "{wire:?} {bad:?}: {err}"
            );
            assert!(err.to_string().contains("client_list_id"), "{err}");
        }
        assert_eq!(
            s.sent(),
            sent_before,
            "{wire:?}: a refused filter must send nothing"
        );
    }
}
