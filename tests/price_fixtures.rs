//! Q9 price acceptance fixtures through the public SDK surface.
//!
//! Every expected integer here is written out by hand. None is derived from
//! the SDK's own scale constant, so a scale regression fails these tests
//! instead of moving the expectation with it.

#[allow(dead_code)]
mod hardening_support;

use hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use polyester::models::{
    AttachedRisk, CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce,
    CreateTriggerParams, CreateTriggerType, GetCandlesOpts, GetOrderOpts, ModifyOrderParams,
    OrderKey, PreviewOrderParams, RiskLeg, TrailingDistance, TrailingStop,
};
use polyester::proto::marketdata::v1::{
    CandlePoint, GetCandlesResponse, GetTradesResponse, MarketTrade, Timeframe,
};
use polyester::proto::marketoverview::v1::{ListMarketOverviewResponse, MarketOverview};
use polyester::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};
use polyester::proto::orders::v1::{
    CreateOrderRequest, CreateOrderResponse, GetOrderResponse, GetUserTradesResponse,
    ModifyOrderRequest, Order as ProtoOrder, OrderIntent, PreviewOrderRequest,
    UserTrade as ProtoUserTrade, order_intent, risk_policy,
};
use polyester::proto::triggers::v1::{CreateTriggerRequest, trigger_intent};
use polyester::{Client, Config, Error, Price, Quantity};
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// BTC 83589, ETH 2420 and SOL 185.25 in Q9 price ticks.
const BTC_83589_TICKS: i64 = 83_589_000_000_000;
const ETH_2420_TICKS: i64 = 2_420_000_000_000;
const SOL_185_25_TICKS: i64 = 185_250_000_000;

/// Captured request bodies, keyed by Connect procedure path.
type Captured = Arc<Mutex<HashMap<String, Vec<u8>>>>;

fn catalog() -> serde_json::Value {
    json!({
        "assets": [
            {"asset": "BTC", "quantity_scale": 8, "market_data_volume_scale": 8},
            {"asset": "ETH", "quantity_scale": 8, "market_data_volume_scale": 8},
            {"asset": "SOL", "quantity_scale": 6, "market_data_volume_scale": 6}
        ],
        "pairs": [
            {
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_asset": "BTC",
                "base_quantity_scale": 8,
                "quote_quantity_scale": 6,
                "reference_price_scale": 8
            },
            {
                "symbol": "ETH-USDT",
                "symbol_id": 2,
                "base_asset": "ETH",
                "base_quantity_scale": 8,
                "quote_quantity_scale": 6,
                "reference_price_scale": 8
            },
            {
                "symbol": "SOL-USDT",
                "symbol_id": 3,
                "base_asset": "SOL",
                "base_quantity_scale": 6,
                "quote_quantity_scale": 6,
                "reference_price_scale": 8
            }
        ]
    })
}

/// A client against a local Connect server that records every request body
/// and answers with `respond`.
async fn fixture_client(
    respond: impl Fn(&str) -> HttpScript + Send + Sync + 'static,
) -> (Client, Captured, MockHttpServer) {
    let captured: Captured = Arc::new(Mutex::new(HashMap::new()));
    let sink = captured.clone();
    let server = MockHttpServer::spawn(move |request| {
        sink.lock()
            .unwrap()
            .insert(request.path.clone(), request.body);
        respond(&request.path)
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("price-fixtures".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client.catalogs.hydrate_spot_config_json(catalog()).unwrap();
    (client, captured, server)
}

fn captured<M: buffa::Message + Default>(captured: &Captured, path: &str) -> M {
    let body = captured
        .lock()
        .unwrap()
        .get(path)
        .cloned()
        .unwrap_or_else(|| panic!("no request captured for {path}"));
    M::decode_from_slice(&body).unwrap()
}

fn eth_qty(raw: &str) -> Quantity {
    Quantity::from_decimal_str(raw, 8, Some("ETH-USDT".into()), Some(2)).unwrap()
}

fn eth_price(raw: &str) -> Price {
    Price::from_decimal_str(raw, Some("ETH-USDT".into())).unwrap()
}

fn limit_gtc_price(intent: &OrderIntent) -> i64 {
    match intent.execution.as_ref() {
        Some(order_intent::Execution::LimitGtc(limit)) => limit.price_ticks,
        other => panic!("expected a limit GTC intent, got {other:?}"),
    }
}

fn base_qty(intent: &OrderIntent) -> i64 {
    match intent.sizing.as_ref() {
        Some(order_intent::Sizing::BaseQtyScaled(qty)) => *qty,
        other => panic!("expected base quantity sizing, got {other:?}"),
    }
}

// --- Decimal codec -------------------------------------------------------

#[test]
fn decimal_2420_encodes_to_2420000000000_and_back() {
    let price = Price::from_decimal_str("2420", None).unwrap();
    assert_eq!(price.as_ticks(), 2_420_000_000_000);
    assert_eq!(price.format(), "2420");

    let decoded = Price::from_ticks(2_420_000_000_000, None).unwrap();
    assert_eq!(decoded.format(), "2420");
    assert_eq!(decoded.as_decimal(), Decimal::from(2420));
    assert_eq!(decoded.as_decimal().to_string(), "2420.000000000");

    let from_decimal = Price::from_decimal(Decimal::from(2420), None).unwrap();
    assert_eq!(from_decimal.as_ticks(), 2_420_000_000_000);
}

#[test]
fn asset_prices_round_trip_at_q9() {
    for (decimal, ticks) in [
        ("83589", 83_589_000_000_000_i64),
        ("2420", 2_420_000_000_000),
        ("185.25", 185_250_000_000),
    ] {
        let encoded = Price::from_decimal_str(decimal, None).unwrap();
        assert_eq!(encoded.as_ticks(), ticks, "{decimal} encodes");
        let decoded = Price::from_ticks(ticks, None).unwrap();
        assert_eq!(decoded.format(), decimal, "{ticks} decodes");
        assert_eq!(decoded.as_decimal(), Decimal::from_str(decimal).unwrap());
        let via_decimal = Price::from_decimal(Decimal::from_str(decimal).unwrap(), None).unwrap();
        assert_eq!(
            via_decimal.as_ticks(),
            ticks,
            "{decimal} encodes from Decimal"
        );
    }
}

#[test]
fn one_tick_is_one_billionth() {
    assert_eq!(
        Price::from_decimal_str("0.000000001", None)
            .unwrap()
            .as_ticks(),
        1
    );
    assert_eq!(Price::from_ticks(1, None).unwrap().format(), "0.000000001");
    assert_eq!(
        Price::from_decimal(Decimal::from_str("0.000000001").unwrap(), None)
            .unwrap()
            .as_ticks(),
        1
    );
}

#[test]
fn a_tenth_decimal_place_is_rejected_not_rounded() {
    for raw in ["0.0000000001", "2420.0000000005", "185.2500000001"] {
        let err = Price::from_decimal_str(raw, None).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{raw}: {err:?}");
        assert!(
            err.to_string()
                .contains("price supports at most 9 decimal places"),
            "{raw}: {err}"
        );
        let err = Price::from_decimal(Decimal::from_str(raw).unwrap(), None).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{raw}: {err:?}");
        assert!(
            err.to_string().contains("at most 9 decimal places"),
            "{raw}"
        );
    }
    // Trailing zeros past the ninth place are padding, not precision.
    assert_eq!(
        Price::from_decimal_str("2420.0000000000", None)
            .unwrap()
            .as_ticks(),
        2_420_000_000_000
    );
}

#[test]
fn prices_beyond_i64_ticks_are_rejected() {
    // i64::MAX ticks is 9223372036.854775807.
    let max = Price::from_decimal_str("9223372036.854775807", None).unwrap();
    assert_eq!(max.as_ticks(), i64::MAX);
    for raw in ["9223372036.854775808", "9223372037", "100000000000"] {
        let err = Price::from_decimal_str(raw, None).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{raw}: {err:?}");
        assert!(
            err.to_string().contains("exceeds int64 range"),
            "{raw}: {err}"
        );
    }
    let err =
        Price::from_decimal(Decimal::from_str("9223372036.854775808").unwrap(), None).unwrap_err();
    assert!(err.to_string().contains("exceeds int64 range"), "{err}");
}

// --- Encode paths --------------------------------------------------------

#[tokio::test]
async fn order_create_and_preview_send_q9_limit_price_and_catalog_scaled_quantity() {
    let (client, captured_requests, _server) = fixture_client(|path| match path {
        "/orders.v1.OrdersService/CreateOrder" => connect_proto_ok(&CreateOrderResponse {
            order_id: 9,
            client_order_id: "q9-create".into(),
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;

    client
        .orders
        .create(CreateOrderParams {
            symbol: "ETH-USDT".into(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: Some(eth_qty("0.002315")),
            max_quote_debit_scaled: None,
            price: Some(eth_price("2420")),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some("q9-create".into()),
            subaccount_id: None,
            post_only: None,
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
            expire_at: None,
        })
        .await
        .unwrap();
    let create: CreateOrderRequest =
        captured(&captured_requests, "/orders.v1.OrdersService/CreateOrder");
    let intent = create.order.as_option().unwrap();
    assert_eq!(intent.symbol_id, 2);
    assert_eq!(limit_gtc_price(intent), ETH_2420_TICKS);
    // 0.002315 ETH at base_quantity_scale 8.
    assert_eq!(base_qty(intent), 231_500);

    // Preview carries the same intent; its response is not under test.
    let _ = client
        .orders
        .preview(PreviewOrderParams {
            symbol: "ETH-USDT".into(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: Some(eth_qty("0.002315")),
            max_quote_debit_scaled: None,
            price: Some(eth_price("2420")),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: None,
            subaccount_id: None,
            post_only: None,
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
            expire_at: None,
        })
        .await;
    let preview: PreviewOrderRequest =
        captured(&captured_requests, "/orders.v1.OrdersService/PreviewOrder");
    let intent = preview.order.as_option().unwrap();
    assert_eq!(limit_gtc_price(intent), ETH_2420_TICKS);
    assert_eq!(base_qty(intent), 231_500);
}

#[tokio::test]
async fn order_modify_sends_q9_new_price() {
    let (client, captured_requests, _server) = fixture_client(|_| HttpScript::NotFound).await;
    let _ = client
        .orders
        .modify(ModifyOrderParams {
            symbol: "SOL-USDT".into(),
            key: OrderKey::ClientOrderId("q9-modify".into()),
            subaccount_id: None,
            request_id: Some("q9-modify-req".into()),
            new_price: Some(Price::from_decimal_str("185.25", Some("SOL-USDT".into())).unwrap()),
            new_qty: None,
            new_attached_risk: None,
            behavior: None,
            new_client_order_id: None,
        })
        .await;
    let modify: ModifyOrderRequest =
        captured(&captured_requests, "/orders.v1.OrdersService/ModifyOrder");
    assert_eq!(modify.new_price_ticks, Some(SOL_185_25_TICKS));
}

#[allow(deprecated)]
#[tokio::test]
async fn attached_risk_sends_q9_trigger_and_activation_prices() {
    let (client, captured_requests, _server) = fixture_client(|_| HttpScript::NotFound).await;
    let params = |risk: AttachedRisk| CreateOrderParams {
        symbol: "BTC-USDT".into(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Limit,
        quantity: Some(
            Quantity::from_decimal_str("0.01", 8, Some("BTC-USDT".into()), Some(1)).unwrap(),
        ),
        max_quote_debit_scaled: None,
        price: Some(Price::from_decimal_str("83589", Some("BTC-USDT".into())).unwrap()),
        time_in_force: Some(CreateTimeInForce::Gtc),
        client_order_id: Some("q9-risk".into()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: None,
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: Some(risk),
        expire_at: None,
    };
    let leg = |raw: &str| RiskLeg {
        trigger_price: Some(Price::from_decimal_str(raw, Some("BTC-USDT".into())).unwrap()),
        trigger_price_source: None,
        order_type: None,
        limit_price: None,
        state: None,
    };

    let _ = client
        .orders
        .create(params(AttachedRisk {
            take_profit: Some(leg("90000.5")),
            stop_loss: Some(leg("80000.125")),
            trailing_stop: None,
            oco: true,
        }))
        .await;
    let create: CreateOrderRequest =
        captured(&captured_requests, "/orders.v1.OrdersService/CreateOrder");
    let intent = create.order.as_option().unwrap();
    assert_eq!(limit_gtc_price(intent), BTC_83589_TICKS);
    let risk = intent.attached_risk.as_option().unwrap();
    assert_eq!(
        risk.take_profit.as_option().unwrap().trigger_price_ticks,
        90_000_500_000_000
    );
    match risk.stop_leg.as_ref() {
        Some(risk_policy::StopLeg::StopLoss(stop)) => {
            assert_eq!(stop.trigger_price_ticks, 80_000_125_000_000);
        }
        other => panic!("expected a stop-loss leg, got {other:?}"),
    }

    let _ = client
        .orders
        .create(params(AttachedRisk {
            take_profit: None,
            stop_loss: None,
            trailing_stop: Some(TrailingStop {
                distance: Some(TrailingDistance::Bps(100)),
                activation_price: Some(
                    Price::from_decimal_str("85000.25", Some("BTC-USDT".into())).unwrap(),
                ),
                trigger_price_source: None,
                order_type: None,
                max_slippage: None,
                state: None,
            }),
            oco: false,
        }))
        .await;
    let create: CreateOrderRequest =
        captured(&captured_requests, "/orders.v1.OrdersService/CreateOrder");
    let risk = create
        .order
        .as_option()
        .unwrap()
        .attached_risk
        .as_option()
        .unwrap();
    match risk.stop_leg.as_ref() {
        Some(risk_policy::StopLeg::TrailingStop(trailing)) => {
            assert_eq!(trailing.activation_price_ticks, 85_000_250_000_000);
        }
        other => panic!("expected a trailing-stop leg, got {other:?}"),
    }
}

fn trigger_params(trigger_type: CreateTriggerType) -> CreateTriggerParams {
    CreateTriggerParams {
        symbol: "ETH-USDT".into(),
        trigger_type,
        side: CreateSide::Sell,
        order_type: CreateOrderType::Market,
        qty: eth_qty("0.002315"),
        trigger_price: None,
        limit_price: None,
        trigger_price_source: None,
        time_in_force: None,
        subaccount_id: None,
        client_trigger_id: "q9-trigger".into(),
        post_only: false,
        activation_price: None,
        trailing_distance_ticks: None,
        trailing_distance_bps: None,
        max_slippage_ticks: None,
        max_slippage_bps: None,
        twap_duration_ms: None,
        twap_slice_interval_ms: None,
        ladder_price_min: None,
        ladder_price_max: None,
        ladder_levels: None,
        ladder_distribution: None,
        fee_asset: None,
        self_trade_prevention_mode: None,
    }
}

#[tokio::test]
async fn trigger_create_sends_q9_trigger_and_activation_prices() {
    const PATH: &str = "/triggers.v1.TriggersService/CreateTrigger";
    let (client, captured_requests, _server) = fixture_client(|_| HttpScript::NotFound).await;

    let _ = client
        .triggers
        .create(CreateTriggerParams {
            trigger_price: Some(eth_price("2420")),
            ..trigger_params(CreateTriggerType::StopLoss)
        })
        .await;
    let create: CreateTriggerRequest = captured(&captured_requests, PATH);
    let intent = create.trigger.as_option().unwrap();
    assert_eq!(intent.qty_scaled, 231_500);
    match intent.strategy.as_ref() {
        Some(trigger_intent::Strategy::StopLoss(stop)) => {
            assert_eq!(stop.trigger_price_ticks, ETH_2420_TICKS);
        }
        other => panic!("expected a stop-loss trigger, got {other:?}"),
    }

    let _ = client
        .triggers
        .create(CreateTriggerParams {
            activation_price: Some(eth_price("2500.5")),
            trailing_distance_bps: Some(50),
            ..trigger_params(CreateTriggerType::TrailingStop)
        })
        .await;
    let create: CreateTriggerRequest = captured(&captured_requests, PATH);
    match create.trigger.as_option().unwrap().strategy.as_ref() {
        Some(trigger_intent::Strategy::TrailingStop(trailing)) => {
            assert_eq!(trailing.activation_price_ticks, 2_500_500_000_000);
        }
        other => panic!("expected a trailing-stop trigger, got {other:?}"),
    }
}

// --- Decode paths --------------------------------------------------------

#[tokio::test]
async fn user_and_public_trade_prices_decode_from_q9() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/orders.v1.OrdersReadService/GetUserTrades" => connect_proto_ok(&GetUserTradesResponse {
            trades: vec![ProtoUserTrade {
                symbol_id: 2,
                match_id: 77,
                order_id: 9,
                price_ticks: ETH_2420_TICKS,
                qty_scaled: 231_500,
                ts_ns: 1_700_000_000_000_000_000,
                ..Default::default()
            }],
            ..Default::default()
        }),
        "/marketdata.v1.MarketDataService/GetTrades" => connect_proto_ok(&GetTradesResponse {
            trades: vec![MarketTrade {
                symbol_id: 1,
                match_id: 78,
                price_ticks: BTC_83589_TICKS,
                qty_scaled: 1_000_000,
                ts_ns: 1_700_000_000_000_000_000,
                ..Default::default()
            }],
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;

    let user = client.trades.list(None, None).await.unwrap();
    let price = user.trades[0].price.as_ref().unwrap();
    assert_eq!(price.format(), "2420");
    assert_eq!(price.as_decimal(), Decimal::from(2420));

    let public = client
        .market_data
        .get_trades("BTC-USDT", None)
        .await
        .unwrap();
    let trade = &public.trades[0];
    assert_eq!(trade.price.as_ref().unwrap().format(), "83589");
    // Quantity keeps the pair's base_quantity_scale 8: 1000000 is 0.01 BTC.
    assert_eq!(trade.qty.as_ref().unwrap().format(None).unwrap(), "0.01");
}

#[tokio::test]
async fn execution_candles_use_q9_and_reference_candles_keep_reference_scale() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/marketdata.v1.MarketDataService/GetCandles" => connect_proto_ok(&GetCandlesResponse {
            symbol_id: 3,
            timeframe: Timeframe::Min1.into(),
            candles: vec![CandlePoint {
                ts_sec: 1_700_000_040,
                open: SOL_185_25_TICKS,
                high: 186_000_000_001,
                low: 185_000_000_000,
                close: 185_500_000_000,
                // 12.5 SOL at market_data_volume_scale 6.
                volume: 12_500_000,
                ..Default::default()
            }],
            reference_candles: vec![CandlePoint {
                ts_sec: 1_700_000_040,
                // reference_price_scale 8: 18525000000 is 185.25.
                open: 18_525_000_000,
                high: 18_600_000_000,
                low: 18_500_000_000,
                close: 18_550_000_000,
                volume: 12_500_000,
                ..Default::default()
            }],
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;

    let result = client
        .market_data
        .get_candles_with(GetCandlesOpts {
            symbol: Some("SOL-USDT".into()),
            timeframe: "1m".into(),
            include_reference: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let candle = &result.candles[0];
    assert_eq!(candle.open, "185.25");
    assert_eq!(candle.high, "186.000000001");
    assert_eq!(candle.low, "185");
    assert_eq!(candle.close, "185.5");
    assert_eq!(candle.volume, "12.5");
    let reference = &result.reference_candles[0];
    assert_eq!(reference.open, "185.25");
    assert_eq!(reference.high, "186");
    assert_eq!(reference.low, "185");
    assert_eq!(reference.close, "185.5");
}

#[tokio::test]
async fn btc_reference_candle_at_scale_8_decodes_83589() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/marketdata.v1.MarketDataService/GetCandles" => connect_proto_ok(&GetCandlesResponse {
            symbol_id: 1,
            timeframe: Timeframe::Min1.into(),
            candles: vec![CandlePoint {
                ts_sec: 1_700_000_040,
                open: BTC_83589_TICKS,
                high: BTC_83589_TICKS,
                low: BTC_83589_TICKS,
                close: BTC_83589_TICKS,
                volume: 1,
                ..Default::default()
            }],
            reference_candles: vec![CandlePoint {
                ts_sec: 1_700_000_040,
                open: 8_358_900_000_000,
                high: 8_358_900_000_000,
                low: 8_358_900_000_000,
                close: 8_358_900_000_000,
                volume: 1,
                ..Default::default()
            }],
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;
    let result = client
        .market_data
        .get_candles_with(GetCandlesOpts {
            symbol: Some("BTC-USDT".into()),
            timeframe: "1m".into(),
            include_reference: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.candles[0].open, "83589");
    assert_eq!(result.reference_candles[0].open, "83589");
    assert_eq!(result.reference_candles[0].close, "83589");
}

#[tokio::test]
async fn orderbook_level_price_decodes_from_q9() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/orderbook.v1.OrderbookService/GetOrderBook" => connect_proto_ok(&GetOrderBookResponse {
            book_seq: 5,
            bids: vec![PriceLevel {
                price_ticks: ETH_2420_TICKS,
                // 0.002315 ETH at base_quantity_scale 8.
                qty_scaled: 231_500,
                ..Default::default()
            }],
            asks: vec![PriceLevel {
                price_ticks: 2_420_000_000_001,
                qty_scaled: 100_000_000,
                ..Default::default()
            }],
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;
    let book = client.orderbook.get("ETH-USDT", Some(5)).await.unwrap();
    let bid = &book.bids[0];
    assert_eq!(bid.price.as_ref().unwrap().format(), "2420");
    assert_eq!(bid.qty.as_ref().unwrap().format(None).unwrap(), "0.002315");
    assert_eq!(
        book.asks[0].price.as_ref().unwrap().format(),
        "2420.000000001"
    );
    assert_eq!(
        book.asks[0].qty.as_ref().unwrap().format(None).unwrap(),
        "1"
    );
}

#[tokio::test]
async fn order_read_price_decodes_from_q9() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/orders.v1.OrdersReadService/GetOrder" => connect_proto_ok(&GetOrderResponse {
            order: buffa::MessageField::some(ProtoOrder {
                order_id: 9,
                symbol_id: 3,
                price_ticks: SOL_185_25_TICKS,
                avg_price_ticks: 185_200_000_000,
                orig_qty_scaled: 2_000_000,
                version: 1,
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
    })
    .await;
    let result = client
        .orders
        .get_with(GetOrderOpts {
            key: OrderKey::OrderId("A".into()),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(false),
            limit: None,
            page_token: None,
        })
        .await
        .unwrap();
    let order = result.order.unwrap();
    assert_eq!(order.price.as_ref().unwrap().format(), "185.25");
    assert_eq!(order.avg_px.as_ref().unwrap().format(), "185.2");
    assert_eq!(order.price.as_ref().unwrap().as_ticks(), SOL_185_25_TICKS);
}

#[tokio::test]
async fn market_overview_prices_are_q9_and_usd_volume_stays_raw_1e6() {
    let (client, _, _server) = fixture_client(|path| match path {
        "/marketoverview.v1.MarketOverviewService/ListMarketOverview" => {
            connect_proto_ok(&ListMarketOverviewResponse {
                markets: vec![MarketOverview {
                    symbol_id: 1,
                    last_price_ticks: BTC_83589_TICKS,
                    index_price_ticks: 83_588_500_000_000,
                    // 2.5 BTC at market_data_volume_scale 8.
                    volume_24h_base_scaled: Some(250_000_000),
                    // 208972.5 USD at the fixed USD scale 1e6.
                    volume_24h_usd_scaled: Some(208_972_500_000),
                    ..Default::default()
                }],
                ..Default::default()
            })
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let list = client
        .market_overview
        .list(polyester::services::ListMarketOverviewOptions::default())
        .await
        .unwrap();
    let market = &list.markets[0];
    assert_eq!(market.symbol, "BTC-USDT");
    assert_eq!(market.last_price.as_ref().unwrap().format(), "83589");
    assert_eq!(market.index_price.as_ref().unwrap().format(), "83588.5");
    assert_eq!(market.volume_24h_base.as_deref(), Some("2.5"));
    // The USD volume is the raw 1e6-scaled integer, never rescaled by the price scale.
    assert_eq!(
        market.volume_24h_usd_scaled.as_deref(),
        Some("208972500000")
    );
}

#[test]
fn quantity_keeps_the_pair_base_quantity_scale() {
    let qty = eth_qty("0.002315");
    assert_eq!(qty.as_scaled(), 231_500);
    assert_eq!(qty.format(None).unwrap(), "0.002315");
    let sol = Quantity::from_decimal_str("12.5", 6, Some("SOL-USDT".into()), Some(3)).unwrap();
    assert_eq!(sol.as_scaled(), 12_500_000);
}
