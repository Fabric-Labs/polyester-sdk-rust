//! POLY-9092: `market_client_ref_price` is a market-order field. The public
//! contract carries `client_ref_price_ticks` on `MarketIoc` only, so a limit
//! order with it is refused before anything is sent instead of having the
//! reference price silently dropped.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, PreviewOrderParams,
};
use polyester::proto::orders::v1::{CreateOrderRequest, CreateOrderResponse};
use polyester::{Client, Config, Error, Price, Quantity};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const CREATE_PATH: &str = "/orders.v1.OrdersService/CreateOrder";
const REFUSAL: &str = "market_client_ref_price is only valid for market orders";

/// A client whose server answers CreateOrder and records every request.
async fn recording_client() -> (Client, Arc<Mutex<Vec<(String, Vec<u8>)>>>, MockHttpServer) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_server = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        seen_server
            .lock()
            .unwrap()
            .push((req.path.clone(), req.body.clone()));
        if req.path == CREATE_PATH {
            connect_proto_ok(&CreateOrderResponse {
                order_id: 42,
                ..Default::default()
            })
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
            "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .expect("catalog");
    (client, seen, http)
}

fn price(decimal: &str) -> Price {
    Price::from_decimal_str(decimal, Some("BTC-USDT".into())).unwrap()
}

fn order(
    side: CreateSide,
    order_type: CreateOrderType,
    tif: Option<CreateTimeInForce>,
) -> CreateOrderParams {
    let mut params = CreateOrderParams::new("BTC-USDT", side, order_type);
    params.quantity =
        Some(Quantity::from_decimal_str("0.001", 8, Some("BTC-USDT".into()), Some(1)).unwrap());
    params.market_client_ref_price = Some(price("100"));
    if order_type == CreateOrderType::Limit {
        params.price = Some(price("100"));
        params.time_in_force = tif;
        if tif == Some(CreateTimeInForce::Gtd) {
            params.expire_at = Some(SystemTime::now() + Duration::from_secs(3600));
        }
    }
    params
}

fn preview(params: &CreateOrderParams) -> PreviewOrderParams {
    let mut preview = PreviewOrderParams::new(&params.symbol, params.side, params.order_type);
    preview.quantity = params.quantity.clone();
    preview.price = params.price.clone();
    preview.time_in_force = params.time_in_force;
    preview.expire_at = params.expire_at;
    preview.market_client_ref_price = params.market_client_ref_price.clone();
    preview
}

#[tokio::test]
async fn limit_orders_refuse_market_client_ref_price_before_sending() {
    let tifs = [
        CreateTimeInForce::Gtc,
        CreateTimeInForce::Gtd,
        CreateTimeInForce::Ioc,
        CreateTimeInForce::Fok,
    ];
    let mut failures = Vec::new();
    for tif in tifs {
        for side in [CreateSide::Buy, CreateSide::Sell] {
            for method in ["create", "preview", "batch_create"] {
                let (client, seen, _http) = recording_client().await;
                let params = order(side, CreateOrderType::Limit, Some(tif));
                let result = match method {
                    "create" => client.orders.create(params).await.map(drop),
                    "preview" => client.orders.preview(preview(&params)).await.map(drop),
                    _ => client
                        .orders
                        .batch_create(vec![params], None, None)
                        .await
                        .map(drop),
                };
                let sent = seen.lock().unwrap().len();
                let case = format!("{tif:?} {side:?} {method}");
                match result {
                    Err(Error::Validation(msg)) if msg.contains(REFUSAL) && sent == 0 => {}
                    Err(err) => failures.push(format!("{case}: {err}; {sent} request(s) sent")),
                    Ok(()) => failures.push(format!("{case}: accepted; {sent} request(s) sent")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn market_orders_send_market_client_ref_price() {
    for side in [CreateSide::Buy, CreateSide::Sell] {
        let (client, seen, _http) = recording_client().await;
        client
            .orders
            .create(order(side, CreateOrderType::Market, None))
            .await
            .unwrap_or_else(|err| panic!("{side:?}: {err}"));
        let seen = seen.lock().unwrap();
        let (_, body) = seen
            .iter()
            .find(|(path, _)| path == CREATE_PATH)
            .unwrap_or_else(|| panic!("{side:?}: never sent"));
        let req = CreateOrderRequest::decode_from_slice(body).unwrap();
        let ticks = match req.order.as_option().and_then(|o| o.execution.as_ref()) {
            Some(polyester::proto::orders::v1::order_intent::Execution::MarketIoc(market)) => {
                market.client_ref_price_ticks
            }
            other => panic!("{side:?}: expected market_ioc, got {other:?}"),
        };
        assert_eq!(ticks, 100_000_000_000, "{side:?}");
    }
}
