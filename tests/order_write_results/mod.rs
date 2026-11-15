//! Order write results keep what the server returns: the acceptance time of a
//! create, the response time of a batch create and of a modify, and the base
//! quantity resolved for each accepted batch item, at that item's pair scale
//! as a single create returns it.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, ModifyOrderParams, OrderKey,
};
use polyester::proto::orders::v1::{
    BatchCreateAccepted, BatchCreateOrdersResponse, BatchCreateRejected, BatchCreateResultItem,
    CreateOrderResponse, ErrorCode, ErrorDetail, ModifyActionTaken, ModifyOrderResponse,
    batch_create_result_item,
};
use polyester::{Client, Config, Price, Quantity};
use serde_json::json;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ACCEPTED_TS_NS: u64 = 1_763_164_800_123_456_789;

/// A client whose server answers create, batch create and modify, with
/// BTC-USDT (base scale 8) and LTC-USDT (base scale 5) in its catalogs.
async fn client() -> (Client, MockHttpServer) {
    let http = MockHttpServer::spawn(|req| match req.path.as_str() {
        "/orders.v1.OrdersService/CreateOrder" => connect_proto_ok(&CreateOrderResponse {
            order_id: 41,
            accepted_at_ts_ns: ACCEPTED_TS_NS,
            resolved_base_qty_scaled: 776_079,
            ..Default::default()
        }),
        "/orders.v1.OrdersService/BatchCreateOrders" => {
            let accepted = |order_id: u64, resolved: i64| BatchCreateResultItem {
                outcome: Some(batch_create_result_item::Outcome::Accepted(Box::new(
                    BatchCreateAccepted {
                        order_id,
                        resolved_base_qty_scaled: resolved,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            };
            connect_proto_ok(&BatchCreateOrdersResponse {
                results: vec![
                    accepted(42, 1_000_000),
                    accepted(43, 77_607),
                    BatchCreateResultItem {
                        outcome: Some(batch_create_result_item::Outcome::Rejected(Box::new(
                            BatchCreateRejected {
                                error: ErrorDetail {
                                    code: ErrorCode::ERROR_CODE_MIN_QTY.into(),
                                    ..Default::default()
                                }
                                .into(),
                                ..Default::default()
                            },
                        ))),
                        ..Default::default()
                    },
                ],
                accepted_count: 2,
                rejected_count: 1,
                ts_ns: ACCEPTED_TS_NS + 1,
                ..Default::default()
            })
        }
        "/orders.v1.OrdersService/ModifyOrder" => connect_proto_ok(&ModifyOrderResponse {
            action_taken: ModifyActionTaken::Amended.into(),
            old_order_id: 42,
            final_order_id: 42,
            ts_ns: ACCEPTED_TS_NS + 2,
            ..Default::default()
        }),
        _ => HttpScript::NotFound,
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
                {"asset": "LTC", "market_data_volume_scale": 5},
            ],
            "pairs": [
                {"symbol": "BTC-USDT", "symbol_id": 1, "base_asset": "BTC", "base_quantity_scale": 8},
                {"symbol": "LTC-USDT", "symbol_id": 2, "base_asset": "LTC", "base_quantity_scale": 5},
            ]
        }))
        .expect("catalog");
    (client, http)
}

fn limit_buy(symbol: &str, symbol_id: u32, scale: u32) -> CreateOrderParams {
    let mut params = CreateOrderParams::new(symbol, CreateSide::Buy, CreateOrderType::Limit);
    params.quantity = Some(
        Quantity::from_decimal_str("0.01", scale, Some(symbol.into()), Some(symbol_id)).unwrap(),
    );
    params.price = Some(Price::from_decimal_str("100", Some(symbol.into())).unwrap());
    params.time_in_force = Some(CreateTimeInForce::Gtc);
    params
}

#[tokio::test]
async fn create_keeps_the_acceptance_time_and_resolved_quantity() {
    let (client, _http) = client().await;
    let created = client
        .orders
        .create(limit_buy("BTC-USDT", 1, 8))
        .await
        .expect("create");
    assert_eq!(created.accepted_ts_ns, ACCEPTED_TS_NS);
    let resolved = created.resolved_base_qty.expect("resolved base quantity");
    assert_eq!(resolved.format(None).unwrap(), "0.00776079");
}

#[tokio::test]
async fn batch_create_items_carry_the_resolved_quantity_at_their_pair_scale() {
    let (client, _http) = client().await;
    let result = client
        .orders
        .batch_create(
            vec![
                limit_buy("BTC-USDT", 1, 8),
                limit_buy("LTC-USDT", 2, 5),
                limit_buy("BTC-USDT", 1, 8),
            ],
            None,
            None,
        )
        .await
        .expect("batch create");
    assert_eq!(result.ts_ns, ACCEPTED_TS_NS + 1);
    // (formatted, scale, symbol_id) of each item's resolved quantity.
    type Resolved = (String, Option<u32>, Option<u32>);
    let resolved: Vec<Option<Resolved>> = result
        .results
        .iter()
        .map(|item| {
            item.resolved_base_qty
                .as_ref()
                .map(|qty| (qty.format(None).unwrap(), qty.scale(), qty.symbol_id()))
        })
        .collect();
    assert_eq!(
        resolved,
        [
            Some(("0.01".to_owned(), Some(8), Some(1))),
            Some(("0.77607".to_owned(), Some(5), Some(2))),
            None,
        ]
    );
}

#[tokio::test]
async fn modify_keeps_the_response_time() {
    let (client, _http) = client().await;
    let modified = client
        .orders
        .modify(ModifyOrderParams {
            symbol: "BTC-USDT".into(),
            key: OrderKey::OrderId(polyester::codecs::format_uint64_id(42)),
            subaccount_id: None,
            request_id: None,
            new_price: Some(Price::from_decimal_str("101", Some("BTC-USDT".into())).unwrap()),
            new_qty: None,
            new_attached_risk: None,
            behavior: None,
            new_client_order_id: None,
        })
        .await
        .expect("modify");
    assert_eq!(modified.ts_ns, ACCEPTED_TS_NS + 2);
}
