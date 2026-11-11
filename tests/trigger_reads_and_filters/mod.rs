//! Trigger reads, the list filters and the create/modify refusals through the
//! public `triggers` service against a local server, in binary and JSON:
//! POLY-9396 (a market TWAP's slice cap), POLY-9424 (the pair's symbol),
//! POLY-9425 (a trailing stop's cap), POLY-9433 (`trigger_type` and
//! `parent_order_id` filters), POLY-9423 (a ladder takes no market order type)
//! and POLY-9426 (modify takes a buy stop's 10000 bps cap).

use super::hardening_support::{HttpScript, MockHttpServer, MockWsServer};
use buffa::Message as _;
use polyester::models::{
    CreateOrderType, CreateSide, CreateTriggerParams, CreateTriggerType, ListTriggersOpts,
    MaxSlippage, ModifyTriggerParams, Trigger,
};
use polyester::proto::orders::v1::Side;
use polyester::proto::triggers::v1::{
    CreateTriggerRequest, CreateTriggerResponse, GetTriggerResponse, ListTriggersRequest,
    ListTriggersResponse, ModifyTriggerRequest, ModifyTriggerResponse, TrailingStopTrigger,
    Trigger as ProtoTrigger, TriggerStatus, TriggerType, TwapMarketIoc, TwapTrigger,
    modify_trigger_request, trailing_stop_trigger, trigger, twap_market_ioc, twap_trigger,
};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Price, Quantity};
use serde_json::json;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const LIST: &str = "/triggers.v1.TriggersService/ListTriggers";
const GET: &str = "/triggers.v1.TriggersService/GetTrigger";
const CREATE: &str = "/triggers.v1.TriggersService/CreateTrigger";
const MODIFY: &str = "/triggers.v1.TriggersService/ModifyTrigger";

fn twap(id: u64, cap: Option<twap_market_ioc::MaxSlippage>) -> ProtoTrigger {
    ProtoTrigger {
        trigger_id: id,
        symbol_id: 1,
        qty_scaled: 100_000,
        status: TriggerStatus::StatusRunning.into(),
        configuration: Some(trigger::Configuration::Twap(Box::new(TwapTrigger {
            side: Side::Sell.into(),
            duration_ms: 60_000,
            slice_interval_ms: 5_000,
            execution: Some(twap_trigger::Execution::MarketIoc(Box::new(
                TwapMarketIoc {
                    max_slippage: cap,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }))),
        ..Default::default()
    }
}

fn trailing(id: u64, cap: Option<trailing_stop_trigger::MaxSlippage>) -> ProtoTrigger {
    ProtoTrigger {
        trigger_id: id,
        symbol_id: 1,
        qty_scaled: 100_000,
        status: TriggerStatus::StatusArmed.into(),
        configuration: Some(trigger::Configuration::TrailingStop(Box::new(
            TrailingStopTrigger {
                side: Side::Sell.into(),
                trailing_distance: Some(
                    trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(200),
                ),
                max_slippage: cap,
                ..Default::default()
            },
        ))),
        ..Default::default()
    }
}

/// The triggers the server lists, each with the cap its read must report.
fn listed() -> Vec<(ProtoTrigger, Option<MaxSlippage>)> {
    vec![
        (
            twap(1, Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(10))),
            Some(MaxSlippage::Bps(10)),
        ),
        (
            twap(2, Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(50))),
            Some(MaxSlippage::Bps(50)),
        ),
        (
            twap(
                3,
                Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(
                    25_000_000_000,
                )),
            ),
            Some(MaxSlippage::Ticks(25_000_000_000)),
        ),
        (twap(4, None), None),
        (
            trailing(
                5,
                Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(100)),
            ),
            Some(MaxSlippage::Bps(100)),
        ),
        (
            trailing(
                6,
                Some(trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(
                    7_000_000_000,
                )),
            ),
            Some(MaxSlippage::Ticks(7_000_000_000)),
        ),
        (trailing(7, None), None),
    ]
}

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

struct Server {
    client: Client,
    http: MockHttpServer,
    lists: Arc<Mutex<Vec<ListTriggersRequest>>>,
    creates: Arc<Mutex<Vec<CreateTriggerRequest>>>,
    modifies: Arc<Mutex<Vec<ModifyTriggerRequest>>>,
}

async fn server(wire: WireFormat, catalogs: bool) -> Server {
    let lists = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(Mutex::new(Vec::new()));
    let modifies = Arc::new(Mutex::new(Vec::new()));
    let (l, c, m) = (lists.clone(), creates.clone(), modifies.clone());
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        LIST => {
            l.lock().unwrap().push(request(wire, &req.body));
            answer(
                wire,
                &ListTriggersResponse {
                    triggers: listed().into_iter().map(|(t, _)| t).collect(),
                    ..Default::default()
                },
            )
        }
        GET => answer(
            wire,
            &GetTriggerResponse {
                trigger: twap(
                    3,
                    Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(
                        25_000_000_000,
                    )),
                )
                .into(),
                ..Default::default()
            },
        ),
        CREATE => {
            c.lock().unwrap().push(request(wire, &req.body));
            answer(
                wire,
                &CreateTriggerResponse {
                    trigger_id: 9,
                    client_trigger_id: "ladder".into(),
                    ..Default::default()
                },
            )
        }
        MODIFY => {
            m.lock().unwrap().push(request(wire, &req.body));
            answer(
                wire,
                &ModifyTriggerResponse {
                    trigger_id: 9,
                    status: TriggerStatus::StatusArmed.into(),
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
    if catalogs {
        hydrate(&client);
    }
    Server {
        client,
        http,
        lists,
        creates,
        modifies,
    }
}

fn hydrate(client: &Client) {
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
}

const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];

/// POLY-9396 and POLY-9425: `triggers.get` and `triggers.list` report a
/// market TWAP's slice cap and a trailing stop's cap in `max_slippage`, in
/// the variant the server sent, and `None` when it sent none.
#[tokio::test]
async fn poly_9396_9425_trigger_reads_report_the_twap_and_trailing_caps() {
    let mut failures = Vec::new();
    for wire in WIRES {
        let s = server(wire, true).await;
        let got = s
            .client
            .triggers
            .get_by_id("3", None)
            .await
            .expect("get")
            .expect("trigger");
        if got.max_slippage != Some(MaxSlippage::Ticks(25_000_000_000)) {
            failures.push(format!("{wire:?} get: {:?}", got.max_slippage));
        }
        let list = s
            .client
            .triggers
            .list_with(ListTriggersOpts::default())
            .await
            .expect("list");
        for (read, (_, want)) in list.triggers.iter().zip(listed()) {
            if read.max_slippage != want {
                failures.push(format!(
                    "{wire:?} list {} {}: got {:?}, want {want:?}",
                    read.trigger_type, read.trigger_id, read.max_slippage
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// POLY-9424: with catalogs loaded, `triggers.get`, `triggers.list` and
/// `triggers.subscribe` name the trigger's pair; without them `symbol` stays
/// empty, as before.
#[tokio::test]
async fn poly_9424_trigger_reads_and_stream_name_the_pair() {
    for wire in WIRES {
        for (catalogs, want) in [(true, "BTC-USDT"), (false, "")] {
            let s = server(wire, catalogs).await;
            let got = s
                .client
                .triggers
                .get_by_id("3", None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got.symbol, want, "{wire:?} get, catalogs {catalogs}");
            let list = s
                .client
                .triggers
                .list_with(ListTriggersOpts::default())
                .await
                .unwrap();
            for t in &list.triggers {
                assert_eq!(t.symbol, want, "{wire:?} list, catalogs {catalogs}");
            }
        }
    }

    for (catalogs, want) in [(true, "BTC-USDT"), (false, "")] {
        let ws = MockWsServer::spawn_centrifugo_publications_after_handshake(
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            vec![twap(3, None).encode_to_vec()],
        )
        .await;
        let (client, _http) = super::streaming_client(&ws).await;
        if catalogs {
            hydrate(&client);
        }
        let mut sub = client
            .triggers
            .subscribe(Some("acct"))
            .await
            .expect("subscribe");
        let streamed: Vec<Trigger> = super::recv_n(&mut sub, 1).await;
        assert_eq!(streamed[0].symbol, want, "stream, catalogs {catalogs}");
        sub.close();
    }
}

/// POLY-9433: `list_with` sends `trigger_type` and `parent_order_id` as the
/// contract's `ListTriggersRequest` filters; without them the request is as
/// before. A parent order ID that is not one is refused before sending.
#[tokio::test]
async fn poly_9433_list_sends_the_trigger_type_and_parent_order_filters() {
    // The ID a trigger read reports, and the same ID in decimal (a `0` keeps
    // it from reading as base58).
    let parent = polyester::codecs::format_uint64_id(10_203);
    for wire in WIRES {
        let s = server(wire, true).await;
        s.client
            .triggers
            .list_with(ListTriggersOpts::default())
            .await
            .unwrap();
        for (trigger_type, want) in [
            (CreateTriggerType::StopLoss, TriggerType::STOP_LOSS),
            (CreateTriggerType::TakeProfit, TriggerType::TAKE_PROFIT),
            (CreateTriggerType::TrailingStop, TriggerType::TRAILING_STOP),
            (CreateTriggerType::Twap, TriggerType::TWAP),
            (CreateTriggerType::Ladder, TriggerType::LADDER),
        ] {
            s.client
                .triggers
                .list_with(ListTriggersOpts {
                    trigger_type: Some(trigger_type),
                    ..Default::default()
                })
                .await
                .unwrap();
            let sent = s.lists.lock().unwrap().last().cloned().unwrap();
            assert_eq!(sent.trigger_type, want, "{wire:?} {trigger_type:?}");
            assert_eq!(sent.parent_order_id, None);
        }
        for id in [parent.as_str(), "10203"] {
            s.client
                .triggers
                .list_with(ListTriggersOpts {
                    parent_order_id: Some(id.into()),
                    trigger_type: Some(CreateTriggerType::TakeProfit),
                    ..Default::default()
                })
                .await
                .unwrap();
            let sent = s.lists.lock().unwrap().last().cloned().unwrap();
            assert_eq!(sent.parent_order_id, Some(10_203), "{wire:?} {id}");
            assert_eq!(sent.trigger_type, TriggerType::TAKE_PROFIT);
        }
        let unfiltered = s.lists.lock().unwrap()[0].clone();
        assert_eq!(unfiltered, ListTriggersRequest::default(), "{wire:?}");

        let sent_before = s.http.requests.load(std::sync::atomic::Ordering::SeqCst);
        for bad in ["", "0", "not an id"] {
            let err = s
                .client
                .triggers
                .list_with(ListTriggersOpts {
                    parent_order_id: Some(bad.into()),
                    ..Default::default()
                })
                .await
                .expect_err("invalid parent order ID");
            assert!(
                matches!(err, polyester::Error::Validation(_)),
                "{wire:?} {bad:?}: {err}"
            );
            assert!(err.to_string().contains("parent_order_id"), "{err}");
        }
        assert_eq!(
            s.http.requests.load(std::sync::atomic::Ordering::SeqCst),
            sent_before,
            "{wire:?}: a refused filter must send nothing"
        );
    }
}

fn ladder(order_type: CreateOrderType) -> CreateTriggerParams {
    let qty = Quantity::from_decimal_str("0.0002", 5, Some("BTC-USDT".into()), Some(1)).unwrap();
    let mut params = CreateTriggerParams::new(
        "BTC-USDT",
        CreateTriggerType::Ladder,
        CreateSide::Buy,
        order_type,
        qty,
    );
    params.client_trigger_id = "ladder".into();
    params.ladder_price_min = Some(Price::from_decimal_str("60000", None).unwrap());
    params.ladder_price_max = Some(Price::from_decimal_str("61000", None).unwrap());
    params.ladder_levels = Some(2);
    params.post_only = true;
    params
}

/// POLY-9423: a ladder places limit GTC orders only, so an explicit market
/// order type is refused before anything is sent, never remapped to limit.
/// A ladder with a limit order type is still sent.
#[tokio::test]
async fn poly_9423_ladder_refuses_a_market_order_type_before_sending() {
    for wire in WIRES {
        let s = server(wire, true).await;
        let err = s
            .client
            .triggers
            .create(ladder(CreateOrderType::Market))
            .await
            .expect_err("a market ladder must be refused");
        assert!(
            matches!(err, polyester::Error::Validation(_)),
            "{wire:?}: {err}"
        );
        assert!(err.to_string().contains("ladder"), "{err}");
        assert!(s.creates.lock().unwrap().is_empty(), "{wire:?}: sent");

        s.client
            .triggers
            .create(ladder(CreateOrderType::Limit))
            .await
            .expect("a limit ladder");
        assert_eq!(s.creates.lock().unwrap().len(), 1, "{wire:?}");
    }
}

fn modify_bps(bps: i32) -> ModifyTriggerParams {
    ModifyTriggerParams {
        trigger_id: "9".into(),
        subaccount_id: None,
        symbol: Some("BTC-USDT".into()),
        symbol_id: None,
        trigger_price: None,
        activation_price: None,
        trailing_distance_ticks: None,
        trailing_distance_bps: None,
        max_slippage_ticks: None,
        max_slippage_bps: Some(bps),
    }
}

/// POLY-9426: modify accepts `max_slippage_bps` from 0 (clear) to 10000, as
/// the contract does, and leaves the side and type checks to the server; a
/// buy stop-loss or take-profit market child may take 10000, as on create.
/// 10001 and negatives are still refused before sending.
#[tokio::test]
async fn poly_9426_modify_sends_a_10000_bps_cap() {
    for wire in WIRES {
        let s = server(wire, true).await;
        for bps in [0, 1, 9_999, 10_000] {
            s.client
                .triggers
                .modify(modify_bps(bps))
                .await
                .unwrap_or_else(|e| panic!("{wire:?} modify {bps} bps: {e}"));
            let sent = s.modifies.lock().unwrap().last().cloned().unwrap();
            assert_eq!(
                sent.max_slippage,
                Some(modify_trigger_request::MaxSlippage::MaxSlippageBps(bps)),
                "{wire:?}"
            );
        }
        let sent = s.modifies.lock().unwrap().len();
        for bps in [10_001, -1] {
            let err = s
                .client
                .triggers
                .modify(modify_bps(bps))
                .await
                .expect_err("out of range");
            assert!(
                err.to_string()
                    .contains("max_slippage_bps must be between 1 and 10000"),
                "{err}"
            );
        }
        assert_eq!(s.modifies.lock().unwrap().len(), sent, "{wire:?}");
    }
}
