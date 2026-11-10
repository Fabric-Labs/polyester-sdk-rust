//! POLY-8563: order create, preview and batch create take a market ID, as the
//! Go and Python SDKs do, through the public services against a local socket.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, PreviewOrderParams,
};
use polyester::proto::orders::v1::{
    BatchCreateOrdersRequest, BatchCreateOrdersResponse, CreateOrderRequest, CreateOrderResponse,
    PreviewOrderRequest, PreviewOrderResponse,
};
use polyester::{Client, Config, Error, Price, Quantity};
use serde_json::json;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The market ID each order request carried, in order.
type Sent = Arc<Mutex<Vec<u32>>>;

async fn client() -> (Client, Sent, MockHttpServer) {
    let sent = Sent::default();
    let seen = sent.clone();
    let http = MockHttpServer::spawn(move |req| {
        let body = &req.body;
        let rpc = req.path.rsplit('/').next().unwrap_or_default().to_owned();
        let mut seen = seen.lock().unwrap();
        match rpc.as_str() {
            "CreateOrder" => {
                let req = CreateOrderRequest::decode_from_slice(body).unwrap();
                seen.push(req.order.as_option().map_or(0, |o| o.symbol_id));
                connect_proto_ok(&CreateOrderResponse {
                    order_id: 7,
                    ..Default::default()
                })
            }
            "PreviewOrder" => {
                let req = PreviewOrderRequest::decode_from_slice(body).unwrap();
                seen.push(req.order.as_option().map_or(0, |o| o.symbol_id));
                connect_proto_ok(&PreviewOrderResponse::default())
            }
            "BatchCreateOrders" => {
                let req = BatchCreateOrdersRequest::decode_from_slice(body).unwrap();
                seen.extend(req.items.iter().map(|o| o.symbol_id));
                connect_proto_ok(&BatchCreateOrdersResponse::default())
            }
            _ => HttpScript::NotFound,
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
            "pairs": [
                {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8},
                {"symbol": "LTC-USDT", "symbol_id": 2, "base_quantity_scale": 8},
            ]
        }))
        .expect("catalog");
    (client, sent, http)
}

fn ltc_buy(params: &mut CreateOrderParams) {
    params.quantity =
        Some(Quantity::from_decimal_str("0.5", 8, Some("LTC-USDT".into()), Some(2)).unwrap());
    params.price = Some(Price::from_decimal_str("80", Some("LTC-USDT".into())).unwrap());
    params.time_in_force = Some(CreateTimeInForce::Gtc);
}

fn by_id(symbol_id: u32) -> CreateOrderParams {
    let mut params =
        CreateOrderParams::for_symbol_id(symbol_id, CreateSide::Buy, CreateOrderType::Limit);
    ltc_buy(&mut params);
    params
}

fn preview_by_id(symbol_id: u32) -> PreviewOrderParams {
    let mut params =
        PreviewOrderParams::for_symbol_id(symbol_id, CreateSide::Buy, CreateOrderType::Limit);
    params.quantity =
        Some(Quantity::from_decimal_str("0.5", 8, Some("LTC-USDT".into()), Some(2)).unwrap());
    params.price = Some(Price::from_decimal_str("80", Some("LTC-USDT".into())).unwrap());
    params.time_in_force = Some(CreateTimeInForce::Gtc);
    params
}

#[tokio::test]
async fn create_preview_and_batch_create_send_the_market_id_given() {
    let (client, sent, _http) = client().await;
    client
        .orders
        .create(by_id(2))
        .await
        .expect("create by market ID");
    // The canned answers are minimal: only what was sent is checked.
    let _ = client.orders.preview(preview_by_id(2)).await;
    let _ = client.orders.batch_create(vec![by_id(2)], None, None).await;

    // The name and the ID together, when they agree, are the same order.
    let mut both = by_id(2);
    both.symbol = "LTC-USDT".into();
    client
        .orders
        .create(both)
        .await
        .expect("create by name and ID");
    assert_eq!(*sent.lock().unwrap(), vec![2, 2, 2, 2]);
}

#[tokio::test]
async fn a_market_id_the_name_or_catalog_contradicts_is_refused_before_sending() {
    let (client, sent, _http) = client().await;
    let mut contradicted = by_id(2);
    contradicted.symbol = "BTC-USDT".into();
    let mut preview_contradicted = preview_by_id(2);
    preview_contradicted.symbol = "BTC-USDT".into();
    for (label, result) in [
        (
            "create, name contradicts ID",
            client.orders.create(contradicted).await.map(|_| ()),
        ),
        (
            "preview, name contradicts ID",
            client
                .orders
                .preview(preview_contradicted)
                .await
                .map(|_| ()),
        ),
        (
            "create, unknown ID",
            client.orders.create(by_id(9)).await.map(|_| ()),
        ),
        (
            "create, zero ID",
            client.orders.create(by_id(0)).await.map(|_| ()),
        ),
    ] {
        let err = result.expect_err(label);
        assert!(matches!(err, Error::Validation(_)), "{label}: {err:?}");
        assert!(err.to_string().contains("symbol_id"), "{label}: {err}");
    }
    assert!(
        sent.lock().unwrap().is_empty(),
        "a refused order reached the server"
    );
}
