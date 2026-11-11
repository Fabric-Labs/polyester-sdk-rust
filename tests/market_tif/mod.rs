//! POLY-9301: the public contract executes a market order only as
//! `market_ioc` ("Market orders always execute as immediate-or-cancel").
//! A market order or a market / trailing-stop trigger child with any other
//! time in force is refused before anything is sent, instead of being sent
//! as IOC.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use buffa_types::google::protobuf::Timestamp;
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, CreateTriggerParams,
    CreateTriggerType, PreviewOrderParams,
};
use polyester::proto::orders::v1::{
    BatchCreateOrdersRequest, BatchCreateOrdersResponse, CreateOrderRequest, CreateOrderResponse,
    PreviewOrderRequest, PreviewOrderResponse, order_intent,
};
use polyester::proto::triggers::v1::{
    CreateTriggerRequest, CreateTriggerResponse, conditional_child_execution, trigger_intent,
    twap_trigger,
};
use polyester::{Client, Config, Error, Price, Quantity};
use serde_json::json;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const CREATE_PATH: &str = "/orders.v1.OrdersService/CreateOrder";
const PREVIEW_PATH: &str = "/orders.v1.OrdersService/PreviewOrder";
const BATCH_PATH: &str = "/orders.v1.OrdersService/BatchCreateOrders";
const TRIGGER_PATH: &str = "/triggers.v1.TriggersService/CreateTrigger";

type Seen = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A client whose server answers every order and trigger write and records
/// each request.
async fn recording_client() -> (Client, Seen, MockHttpServer) {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let seen_server = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        seen_server
            .lock()
            .unwrap()
            .push((req.path.clone(), req.body.clone()));
        match req.path.as_str() {
            CREATE_PATH => connect_proto_ok(&CreateOrderResponse {
                order_id: 42,
                ..Default::default()
            }),
            PREVIEW_PATH => connect_proto_ok(&PreviewOrderResponse {
                admissible: Some(true),
                evaluated_at: Timestamp {
                    seconds: 1,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }),
            BATCH_PATH => connect_proto_ok(&BatchCreateOrdersResponse::default()),
            TRIGGER_PATH => connect_proto_ok(&CreateTriggerResponse {
                trigger_id: 7,
                client_trigger_id: "market-tif".into(),
                ..Default::default()
            }),
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
            "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .expect("catalog");
    (client, seen, http)
}

fn qty() -> Quantity {
    Quantity::from_decimal_str("0.001", 8, Some("BTC-USDT".into()), Some(1)).unwrap()
}

fn price(decimal: &str) -> Price {
    Price::from_decimal_str(decimal, Some("BTC-USDT".into())).unwrap()
}

fn market_order(side: CreateSide, tif: Option<CreateTimeInForce>) -> CreateOrderParams {
    let mut params = CreateOrderParams::new("BTC-USDT", side, CreateOrderType::Market);
    params.quantity = Some(qty());
    params.time_in_force = tif;
    params
}

fn preview(params: &CreateOrderParams) -> PreviewOrderParams {
    let mut preview = PreviewOrderParams::new(&params.symbol, params.side, params.order_type);
    preview.quantity = params.quantity.clone();
    preview.time_in_force = params.time_in_force;
    preview
}

async fn send_order(client: &Client, method: &str, params: CreateOrderParams) -> Result<(), Error> {
    match method {
        "create" => client.orders.create(params).await.map(drop),
        "preview" => client.orders.preview(preview(&params)).await.map(drop),
        _ => client
            .orders
            .batch_create(vec![params], None, None)
            .await
            .map(drop),
    }
}

const SIDES: [CreateSide; 2] = [CreateSide::Buy, CreateSide::Sell];
const METHODS: [&str; 3] = ["create", "preview", "batch_create"];

#[tokio::test]
async fn market_orders_refuse_every_tif_but_ioc_before_sending() {
    let mut failures = Vec::new();
    for (tif, name) in [
        (CreateTimeInForce::Fok, "fok"),
        (CreateTimeInForce::Gtc, "gtc"),
        (CreateTimeInForce::Gtd, "gtd"),
    ] {
        // Same refusal as gtd: "tif=<value> ... is only valid for limit orders".
        let named = format!("tif={name}");
        for side in SIDES {
            for method in METHODS {
                let (client, seen, _http) = recording_client().await;
                let result = send_order(&client, method, market_order(side, Some(tif))).await;
                let sent = seen.lock().unwrap().len();
                let case = format!("{tif:?} {side:?} {method}");
                match result {
                    Err(Error::Validation(msg))
                        if msg.starts_with(&named)
                            && msg.ends_with("is only valid for limit orders")
                            && sent == 0 => {}
                    Err(err) => failures.push(format!("{case}: {err}; {sent} request(s) sent")),
                    Ok(()) => failures.push(format!("{case}: accepted; {sent} request(s) sent")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn sent_execution(path: &str, body: &[u8]) -> Option<order_intent::Execution> {
    let intent = match path {
        CREATE_PATH => CreateOrderRequest::decode_from_slice(body)
            .unwrap()
            .order
            .into_option(),
        PREVIEW_PATH => PreviewOrderRequest::decode_from_slice(body)
            .unwrap()
            .order
            .into_option(),
        BATCH_PATH => BatchCreateOrdersRequest::decode_from_slice(body)
            .unwrap()
            .items
            .into_iter()
            .next(),
        _ => None,
    };
    intent.and_then(|intent| intent.execution)
}

#[tokio::test]
async fn market_orders_with_ioc_or_no_tif_send_market_ioc() {
    for tif in [None, Some(CreateTimeInForce::Ioc)] {
        for side in SIDES {
            for method in METHODS {
                let (client, seen, _http) = recording_client().await;
                let case = format!("{tif:?} {side:?} {method}");
                send_order(&client, method, market_order(side, tif))
                    .await
                    .unwrap_or_else(|err| panic!("{case}: {err}"));
                let seen = seen.lock().unwrap();
                let execution = seen
                    .iter()
                    .find_map(|(path, body)| sent_execution(path, body))
                    .unwrap_or_else(|| panic!("{case}: never sent"));
                assert!(
                    matches!(execution, order_intent::Execution::MarketIoc(_)),
                    "{case}: sent {execution:?}"
                );
            }
        }
    }
}

fn trigger(
    trigger_type: CreateTriggerType,
    side: CreateSide,
    order_type: CreateOrderType,
    tif: Option<CreateTimeInForce>,
) -> CreateTriggerParams {
    let mut params = CreateTriggerParams::new("BTC-USDT", trigger_type, side, order_type, qty());
    params.time_in_force = tif;
    params.client_trigger_id = "market-tif".into();
    match trigger_type {
        CreateTriggerType::TrailingStop => {
            params.trailing_distance_bps = Some(100);
            if side == CreateSide::Buy {
                params.limit_price = Some(price("120"));
            }
        }
        CreateTriggerType::Twap => {
            params.twap_duration_ms = Some(60_000);
            params.twap_slice_interval_ms = Some(10_000);
        }
        _ => params.trigger_price = Some(price("100")),
    }
    params
}

/// Market and trailing-stop children: the strategies whose child always
/// executes IOC.
fn ioc_children() -> Vec<(CreateTriggerType, CreateOrderType)> {
    vec![
        (CreateTriggerType::StopLoss, CreateOrderType::Market),
        (CreateTriggerType::TakeProfit, CreateOrderType::Market),
        (CreateTriggerType::Twap, CreateOrderType::Market),
        (CreateTriggerType::TrailingStop, CreateOrderType::Market),
    ]
}

#[tokio::test]
async fn market_and_trailing_trigger_children_refuse_gtc_and_fok_before_sending() {
    let mut failures = Vec::new();
    for (trigger_type, order_type) in ioc_children() {
        for (tif, name) in [
            (CreateTimeInForce::Gtc, "Gtc"),
            (CreateTimeInForce::Fok, "Fok"),
        ] {
            for side in SIDES {
                let (client, seen, _http) = recording_client().await;
                let result = client
                    .triggers
                    .create(trigger(trigger_type, side, order_type, Some(tif)))
                    .await;
                let sent = seen.lock().unwrap().len();
                let case = format!("{trigger_type:?} {order_type:?} {tif:?} {side:?}");
                let refusal = format!("time_in_force={name} applies only to");
                match result {
                    Err(Error::Validation(msg)) if msg.contains(&refusal) && sent == 0 => {}
                    Err(err) => failures.push(format!("{case}: {err}; {sent} request(s) sent")),
                    Ok(_) => failures.push(format!("{case}: accepted; {sent} request(s) sent")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn sent_trigger_is_ioc(body: &[u8]) -> bool {
    let req = CreateTriggerRequest::decode_from_slice(body).unwrap();
    let intent = req.trigger.into_option().expect("trigger intent");
    match intent.strategy {
        Some(trigger_intent::Strategy::StopLoss(cond))
        | Some(trigger_intent::Strategy::TakeProfit(cond)) => matches!(
            cond.child.into_option().and_then(|child| child.execution),
            Some(conditional_child_execution::Execution::MarketIoc(_))
        ),
        Some(trigger_intent::Strategy::Twap(twap)) => {
            matches!(twap.execution, Some(twap_trigger::Execution::MarketIoc(_)))
        }
        // A trailing stop has no execution choice: its child is always IOC.
        Some(trigger_intent::Strategy::TrailingStop(_)) => true,
        other => panic!("unexpected strategy {other:?}"),
    }
}

#[tokio::test]
async fn market_and_trailing_trigger_children_with_ioc_or_no_tif_are_sent() {
    for (trigger_type, order_type) in ioc_children() {
        for tif in [None, Some(CreateTimeInForce::Ioc)] {
            for side in SIDES {
                let (client, seen, _http) = recording_client().await;
                let case = format!("{trigger_type:?} {order_type:?} {tif:?} {side:?}");
                client
                    .triggers
                    .create(trigger(trigger_type, side, order_type, tif))
                    .await
                    .unwrap_or_else(|err| panic!("{case}: {err}"));
                let seen = seen.lock().unwrap();
                let (_, body) = seen
                    .iter()
                    .find(|(path, _)| path == TRIGGER_PATH)
                    .unwrap_or_else(|| panic!("{case}: never sent"));
                assert!(sent_trigger_is_ioc(body), "{case}: not sent as IOC");
            }
        }
    }
}

#[tokio::test]
async fn limit_trigger_children_keep_their_tifs() {
    for (trigger_type, tifs) in [
        (
            CreateTriggerType::StopLoss,
            vec![
                None,
                Some(CreateTimeInForce::Gtc),
                Some(CreateTimeInForce::Ioc),
                Some(CreateTimeInForce::Fok),
            ],
        ),
        (
            CreateTriggerType::Twap,
            vec![None, Some(CreateTimeInForce::Gtc)],
        ),
    ] {
        for tif in tifs {
            for side in SIDES {
                let (client, _seen, _http) = recording_client().await;
                let mut params = trigger(trigger_type, side, CreateOrderType::Limit, tif);
                params.limit_price = Some(price("100"));
                client
                    .triggers
                    .create(params)
                    .await
                    .unwrap_or_else(|err| panic!("{trigger_type:?} {tif:?} {side:?}: {err}"));
            }
        }
    }
}
