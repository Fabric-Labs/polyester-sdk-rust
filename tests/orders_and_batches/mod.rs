//! Batch rejection details, unresolved batch cancel IDs, TWAP fire prices,
//! sell TWAP slippage and contradictory market selectors, through the public
//! services against a local socket.
//!
//! POLY-8960: each rejected batch item keeps the server's field violations.
//! POLY-8920: a rejected batch cancel that resolved no order keeps its ID empty.
//! POLY-8959: a TWAP slice sent without a fire price keeps it absent.
//! POLY-8961: a sell TWAP market slice takes at most 9999 bps; a buy, 10000.
//! POLY-8886: a market name that contradicts the market ID is refused before
//! anything is sent.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{
    BatchCancelItem, BatchReplaceItem, CreateOrderParams, CreateOrderType, CreateSide,
    CreateTimeInForce, CreateTriggerParams, CreateTriggerType, GetCandlesOpts, GetTradesOpts,
    ListOrderHistoryOpts, OrderKey,
};
use polyester::proto::orders::v1::{
    BatchCancelOrdersResponse, BatchCancelResultItem, BatchCreateAccepted,
    BatchCreateOrdersResponse, BatchCreateRejected, BatchCreateResultItem,
    BatchReplaceAdmissionItem, BatchReplaceAdmissionStatus, BatchReplaceItemAdmissionStatus,
    BatchReplaceOrdersResponse, ErrorCode, ErrorDetail, FieldViolation, ModifyActionTaken,
    batch_cancel_result_item, batch_create_result_item,
};
use polyester::proto::triggers::v1::{
    CreateTriggerResponse, ListTriggerEventsRequest, ListTriggerEventsResponse, TriggerEvent,
    TriggerEventBatch, TriggerEventType, TriggerType,
};
use polyester::{Client, Config, Error, Price, Quantity};
use serde_json::json;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TICK_FIELD: &str = "order.attached_risk.take_profit.trigger_price_ticks";
const TICK_MESSAGE: &str = "Take-profit trigger price must be a multiple of the tick size.";
const QTY_FIELD: &str = "order.base_qty_scaled";
const QTY_MESSAGE: &str = "Quantity is below the minimum.";

/// A client whose server answers `path` with `response` and records every
/// request path it receives.
async fn client_answering(
    path: &'static str,
    response: HttpScriptFactory,
) -> (Client, Arc<Mutex<Vec<String>>>, MockHttpServer) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_server = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        seen_server.lock().unwrap().push(req.path.clone());
        if req.path == path {
            response()
        } else {
            HttpScript::NotFound
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
        .hydrate_spot_config_json(json!({
            "assets": [
                {"asset": "BTC", "market_data_volume_scale": 8},
                {"asset": "LTC", "market_data_volume_scale": 8},
            ],
            "pairs": [
                {"symbol": "BTC-USDT", "symbol_id": 1, "base_asset": "BTC", "base_quantity_scale": 8},
                {"symbol": "LTC-USDT", "symbol_id": 2, "base_asset": "LTC", "base_quantity_scale": 8},
            ]
        }))
        .expect("catalog");
    (client, seen, http)
}

type HttpScriptFactory = Box<dyn Fn() -> HttpScript + Send + Sync>;

fn answer<M: buffa::Message + Clone + Send + Sync + 'static>(msg: M) -> HttpScriptFactory {
    Box::new(move || connect_proto_ok(&msg))
}

fn violation(field: &str, rule: &str, message: &str) -> FieldViolation {
    FieldViolation {
        field_path: field.into(),
        rule_id: rule.into(),
        message: message.into(),
        ..Default::default()
    }
}

/// The public item as a caller prints it. Field violations show as
/// `field_path: "...", rule_id: "...", message: "..."`.
fn shows_violation(item: &impl std::fmt::Debug, field: &str, rule: &str, message: &str) -> bool {
    format!("{item:?}").contains(&format!(
        "field_path: {field:?}, rule_id: {rule:?}, message: {message:?}"
    ))
}

fn limit_buy(client_order_id: &str) -> CreateOrderParams {
    let mut params = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
    params.quantity =
        Some(Quantity::from_decimal_str("0.001", 8, Some("BTC-USDT".into()), Some(1)).unwrap());
    params.price = Some(Price::from_decimal_str("100", Some("BTC-USDT".into())).unwrap());
    params.time_in_force = Some(CreateTimeInForce::Gtc);
    params.client_order_id = Some(client_order_id.into());
    params
}

fn id(raw: u64) -> String {
    polyester::codecs::format_uint64_id(raw)
}

#[tokio::test]
async fn batch_create_keeps_each_rejections_field_violations() {
    let rejected = |client_order_id: &str, code: ErrorCode, violations: Vec<FieldViolation>| {
        BatchCreateResultItem {
            client_order_id: client_order_id.into(),
            outcome: Some(batch_create_result_item::Outcome::Rejected(Box::new(
                BatchCreateRejected {
                    error: ErrorDetail {
                        code: code.into(),
                        violations,
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }
    };
    let response = BatchCreateOrdersResponse {
        results: vec![
            BatchCreateResultItem {
                client_order_id: "passive".into(),
                outcome: Some(batch_create_result_item::Outcome::Accepted(Box::new(
                    BatchCreateAccepted {
                        order_id: 42,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            },
            rejected(
                "off-tick",
                ErrorCode::ERROR_CODE_PRICE_TICK_SIZE,
                vec![violation(TICK_FIELD, "tick_size", TICK_MESSAGE)],
            ),
            rejected(
                "tiny",
                ErrorCode::ERROR_CODE_MIN_QTY,
                vec![violation(QTY_FIELD, "minimum", QTY_MESSAGE)],
            ),
            rejected("plain", ErrorCode::ERROR_CODE_BAD_PRICE, vec![]),
        ],
        accepted_count: 1,
        rejected_count: 3,
        ..Default::default()
    };
    let (client, seen, _http) = client_answering(
        "/orders.v1.OrdersService/BatchCreateOrders",
        answer(response),
    )
    .await;
    let items = ["passive", "off-tick", "tiny", "plain"]
        .into_iter()
        .map(limit_buy)
        .collect();
    let result = client
        .orders
        .batch_create(items, None, None)
        .await
        .expect("batch create");

    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!((result.accepted_count, result.rejected_count), (1, 3));
    let summary: Vec<_> = result
        .results
        .iter()
        .map(|item| {
            (
                item.status.as_str(),
                item.order_id.clone(),
                item.client_order_id.as_str(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("accepted", id(42), "passive"),
            ("rejected", String::new(), "off-tick"),
            ("rejected", String::new(), "tiny"),
            ("rejected", String::new(), "plain"),
        ]
    );
    let [accepted, off_tick, tiny, plain] = &result.results[..] else {
        panic!("four items");
    };
    assert!(
        shows_violation(off_tick, TICK_FIELD, "tick_size", TICK_MESSAGE),
        "{off_tick:?}"
    );
    assert!(
        shows_violation(tiny, QTY_FIELD, "minimum", QTY_MESSAGE),
        "{tiny:?}"
    );
    assert!(!format!("{accepted:?}{plain:?}").contains("field_path"));
}

#[tokio::test]
async fn batch_cancel_and_replace_keep_each_rejections_field_violations() {
    let response = BatchCancelOrdersResponse {
        results: vec![
            BatchCancelResultItem {
                status: batch_cancel_result_item::Status::Accepted.into(),
                order_id: 42,
                ..Default::default()
            },
            BatchCancelResultItem {
                status: batch_cancel_result_item::Status::Rejected.into(),
                order_id: 45,
                code: "INVALID_ARGUMENT".into(),
                error: ErrorDetail {
                    code: ErrorCode::ERROR_CODE_MIN_QTY.into(),
                    violations: vec![violation(QTY_FIELD, "minimum", QTY_MESSAGE)],
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            },
        ],
        accepted_count: 1,
        rejected_count: 1,
        ..Default::default()
    };
    let (client, _seen, _http) = client_answering(
        "/orders.v1.OrdersService/BatchCancelOrders",
        answer(response),
    )
    .await;
    let canceled = client
        .orders
        .batch_cancel(
            vec![
                BatchCancelItem {
                    key: OrderKey::OrderId(id(42)),
                    symbol_id: None,
                },
                BatchCancelItem {
                    key: OrderKey::OrderId(id(45)),
                    symbol_id: None,
                },
            ],
            None,
            None,
        )
        .await
        .expect("batch cancel");
    assert_eq!(canceled.results[1].order_id, id(45));
    assert!(
        shows_violation(&canceled.results[1], QTY_FIELD, "minimum", QTY_MESSAGE),
        "{:?}",
        canceled.results[1]
    );
    assert!(!format!("{:?}", canceled.results[0]).contains("field_path"));

    let response = BatchReplaceOrdersResponse {
        batch_request_id: 9,
        status: BatchReplaceAdmissionStatus::BATCH_REPLACE_ADMISSION_STATUS_PARTIALLY_ADMITTED
            .into(),
        results: vec![
            BatchReplaceAdmissionItem {
                item_index: 0,
                status:
                    BatchReplaceItemAdmissionStatus::BATCH_REPLACE_ITEM_ADMISSION_STATUS_ADMITTED
                        .into(),
                old_order_id: 42,
                replacement_order_id: 43,
                action_taken: ModifyActionTaken::REPLACED.into(),
                ..Default::default()
            },
            BatchReplaceAdmissionItem {
                item_index: 1,
                status:
                    BatchReplaceItemAdmissionStatus::BATCH_REPLACE_ITEM_ADMISSION_STATUS_REJECTED
                        .into(),
                old_order_id: 44,
                code: "PRICE_TICK_SIZE".into(),
                error: ErrorDetail {
                    code: ErrorCode::ERROR_CODE_PRICE_TICK_SIZE.into(),
                    violations: vec![violation(TICK_FIELD, "tick_size", TICK_MESSAGE)],
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            },
        ],
        accepted_count: 1,
        rejected_count: 1,
        ..Default::default()
    };
    let (client, _seen, _http) = client_answering(
        "/orders.v1.OrdersService/BatchReplaceOrders",
        answer(response),
    )
    .await;
    let item = |order_id: u64| BatchReplaceItem {
        key: OrderKey::OrderId(id(order_id)),
        new_price: Some(Price::from_decimal_str("101", Some("BTC-USDT".into())).unwrap()),
        new_qty: None,
        new_attached_risk: None,
        new_client_order_id: None,
    };
    let replaced = client
        .orders
        .batch_replace(vec![item(42), item(44)], "BTC-USDT", None, None)
        .await
        .expect("batch replace");
    assert_eq!(replaced.results[1].old_order_id, id(44));
    assert!(
        shows_violation(&replaced.results[1], TICK_FIELD, "tick_size", TICK_MESSAGE),
        "{:?}",
        replaced.results[1]
    );
    assert!(!format!("{:?}", replaced.results[0]).contains("field_path"));
}

#[tokio::test]
async fn batch_cancel_keeps_an_unresolved_order_id_empty() {
    let response = BatchCancelOrdersResponse {
        results: vec![
            BatchCancelResultItem {
                status: batch_cancel_result_item::Status::Accepted.into(),
                order_id: 42,
                ..Default::default()
            },
            BatchCancelResultItem {
                status: batch_cancel_result_item::Status::Rejected.into(),
                client_order_id: "missing".into(),
                code: "ORDER_UNKNOWN".into(),
                ..Default::default()
            },
            BatchCancelResultItem {
                status: batch_cancel_result_item::Status::Rejected.into(),
                order_id: 77,
                code: "ORDER_NOT_OPEN".into(),
                ..Default::default()
            },
        ],
        accepted_count: 1,
        rejected_count: 2,
        ..Default::default()
    };
    let (client, seen, _http) = client_answering(
        "/orders.v1.OrdersService/BatchCancelOrders",
        answer(response),
    )
    .await;
    let result = client
        .orders
        .batch_cancel(
            vec![
                BatchCancelItem {
                    key: OrderKey::OrderId(id(42)),
                    symbol_id: None,
                },
                BatchCancelItem {
                    key: OrderKey::ClientOrderId("missing".into()),
                    symbol_id: None,
                },
                BatchCancelItem {
                    key: OrderKey::OrderId(id(77)),
                    symbol_id: None,
                },
            ],
            None,
            None,
        )
        .await
        .expect("batch cancel");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!((result.accepted_count, result.rejected_count), (1, 2));
    let ids: Vec<_> = result
        .results
        .iter()
        .map(|item| item.order_id.as_str())
        .collect();
    assert_eq!(ids, [id(42).as_str(), "", id(77).as_str()]);
    assert_eq!(
        (
            result.results[1].client_order_id.as_str(),
            result.results[1].code.as_str()
        ),
        ("missing", "ORDER_UNKNOWN")
    );
}

fn scheduled_slices() -> Vec<TriggerEvent> {
    let fired = |child_seq, child_order_id, fire_price_ticks| TriggerEvent {
        trigger_id: 9,
        trigger_type: TriggerType::TWAP.into(),
        event_type: TriggerEventType::EVENT_FIRED.into(),
        child_seq,
        child_order_id,
        fire_price_ticks,
        ..Default::default()
    };
    vec![fired(1, 101, None), fired(2, 102, Some(123))]
}

#[tokio::test]
async fn list_events_and_stream_keep_an_absent_fire_price_absent() {
    let (client, _seen, _http) = client_answering(
        "/triggers.v1.TriggersService/ListTriggerEvents",
        answer(ListTriggerEventsResponse {
            events: scheduled_slices(),
            ..Default::default()
        }),
    )
    .await;
    let listed = client
        .triggers
        .list_events(ListTriggerEventsRequest {
            trigger_id: 9,
            ..Default::default()
        })
        .await
        .expect("list events")
        .events;
    let streamed: Vec<_> = polyester::codecs::decode::trigger_event_batch_from_bytes(
        &TriggerEventBatch {
            events: scheduled_slices(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .expect("batch")
    .into_iter()
    .map(|event| event.expect("event"))
    .collect();
    for events in [listed, streamed] {
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].child_order_id, id(101));
        assert!(events[0].fire_price.is_none(), "{:?}", events[0]);
        assert_eq!(
            events[1].fire_price.as_ref().map(|p| p.as_ticks()),
            Some(123)
        );
    }
}

#[tokio::test]
async fn twap_market_slippage_caps_by_side() {
    let cases = [
        (CreateSide::Buy, 50, None),
        (CreateSide::Buy, 9_999, None),
        (CreateSide::Buy, 10_000, None),
        (CreateSide::Buy, 10_001, Some("between 1 and")),
        (CreateSide::Sell, 50, None),
        (CreateSide::Sell, 9_999, None),
        (CreateSide::Sell, 10_000, Some("between 1 and 9999")),
        (CreateSide::Sell, 10_001, Some("between 1 and")),
    ];
    for (side, bps, refusal) in cases {
        let (client, seen, _http) = client_answering(
            "/triggers.v1.TriggersService/CreateTrigger",
            answer(CreateTriggerResponse {
                trigger_id: 9,
                client_trigger_id: "twap-cap".into(),
                ..Default::default()
            }),
        )
        .await;
        let mut params = CreateTriggerParams::new(
            "BTC-USDT",
            CreateTriggerType::Twap,
            side,
            CreateOrderType::Market,
            Quantity::from_decimal_str("0.00036", 8, Some("BTC-USDT".into()), Some(1)).unwrap(),
        );
        params.client_trigger_id = "twap-cap".into();
        params.twap_duration_ms = Some(60_000);
        params.twap_slice_interval_ms = Some(30_000);
        params.max_slippage_bps = Some(bps);
        let result = client.triggers.create(params).await;
        let sent = seen.lock().unwrap().len();
        match refusal {
            None => {
                result.unwrap_or_else(|err| panic!("{side:?} {bps}: {err}"));
                assert_eq!(sent, 1, "{side:?} {bps}");
            }
            Some(refusal) => {
                let Err(err) = result else {
                    panic!("{side:?} {bps}: a refused cap was accepted");
                };
                assert!(
                    matches!(err, Error::Validation(_)) && err.to_string().contains(refusal),
                    "{side:?} {bps}: {err}"
                );
                assert_eq!(sent, 0, "{side:?} {bps}: a refused cap was sent");
            }
        }
    }
}

/// Each read that takes a market name and a market ID, called with `LTC-USDT`
/// and the ID of BTC-USDT, and with agreeing selectors as a control.
#[tokio::test]
async fn reads_refuse_a_market_name_that_contradicts_the_market_id() {
    type Read = for<'a> fn(&'a Client, Option<&'a str>, Option<u32>) -> futures_like::Call<'a>;
    let reads: [(&str, &'static str, Read); 3] = [
        (
            "get_trades_with",
            "/marketdata.v1.MarketDataService/GetTrades",
            |client, symbol, symbol_id| {
                Box::pin(async move {
                    client
                        .market_data
                        .get_trades_with(GetTradesOpts {
                            symbol: symbol.map(Into::into),
                            symbol_id,
                            ..Default::default()
                        })
                        .await
                        .map(drop)
                })
            },
        ),
        (
            "get_candles_with",
            "/marketdata.v1.MarketDataService/GetCandles",
            |client, symbol, symbol_id| {
                Box::pin(async move {
                    client
                        .market_data
                        .get_candles_with(GetCandlesOpts {
                            symbol: symbol.map(Into::into),
                            symbol_id,
                            ..Default::default()
                        })
                        .await
                        .map(drop)
                })
            },
        ),
        (
            "list_history_with",
            "/orders.v1.OrdersReadService/GetOrderHistory",
            |client, symbol, symbol_id| {
                Box::pin(async move {
                    client
                        .orders
                        .list_history_with(ListOrderHistoryOpts {
                            symbol: symbol.map(Into::into),
                            symbol_id,
                            ..Default::default()
                        })
                        .await
                        .map(drop)
                })
            },
        ),
    ];
    for (name, path, read) in reads {
        let ok: HttpScriptFactory = match path {
            "/marketdata.v1.MarketDataService/GetTrades" => {
                answer(polyester::proto::marketdata::v1::GetTradesResponse::default())
            }
            "/marketdata.v1.MarketDataService/GetCandles" => {
                answer(polyester::proto::marketdata::v1::GetCandlesResponse::default())
            }
            _ => answer(polyester::proto::orders::v1::GetOrderHistoryResponse::default()),
        };
        let (client, seen, _http) = client_answering(path, ok).await;

        let err = read(&client, Some("LTC-USDT"), Some(1))
            .await
            .expect_err("contradictory selectors");
        assert!(
            matches!(err, Error::Validation(_)) && err.to_string().contains("LTC-USDT"),
            "{name}: {err}"
        );
        // The catalog does not know the name, but knows the ID as another market.
        let err = read(&client, Some("DOGE-USDT"), Some(1))
            .await
            .expect_err("a name that is not the ID's");
        assert!(matches!(err, Error::Validation(_)), "{name}: {err}");
        assert!(
            seen.lock().unwrap().is_empty(),
            "{name}: a contradiction was sent"
        );

        for (symbol, symbol_id) in [
            (Some("BTC-USDT"), Some(1)),
            (Some(" BTC-USDT "), Some(1)),
            (None, Some(1)),
            (Some("BTC-USDT"), None),
        ] {
            read(&client, symbol, symbol_id)
                .await
                .unwrap_or_else(|err| panic!("{name} {symbol:?} {symbol_id:?}: {err}"));
        }
        assert_eq!(seen.lock().unwrap().len(), 4, "{name}");
    }
}

mod futures_like {
    use std::future::Future;
    use std::pin::Pin;

    pub type Call<'a> = Pin<Box<dyn Future<Output = Result<(), polyester::Error>> + Send + 'a>>;
}
