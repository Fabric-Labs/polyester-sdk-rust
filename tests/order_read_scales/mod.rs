//! POLY-9302: order and trigger reads carry the pair's base quantity scale
//! from the loaded catalogs, as the order book does, so their quantities
//! format without the caller passing the scale. Without catalogs a read
//! decodes as before: quantities without a scale.

use super::hardening_support::{HttpScript, MockHttpServer, MockWsServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{ListTriggersOpts, Order, OrderKey, TriggerDetails, UserTrade};
use polyester::proto::orders::v1::{
    GetOpenOrdersResponse, GetOrderHistoryResponse, GetOrderResponse, GetUserTradesResponse,
    Order as ProtoOrder, OrderFeeTotal, UserTrade as ProtoUserTrade,
};
use polyester::proto::triggers::v1::{
    GetTriggerResponse, LadderDetails, ListTriggersResponse, Trigger as ProtoTrigger, TwapDetails,
    trigger,
};
use polyester::{Client, Config, Quantity};
use serde_json::json;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn proto_order() -> ProtoOrder {
    ProtoOrder {
        order_id: 9,
        symbol_id: 1,
        orig_qty_scaled: 8_000,
        cum_qty_scaled: 5_000,
        leaves_qty_scaled: 3_000,
        inherited_cum_qty_scaled: 1_000,
        fee_total: OrderFeeTotal {
            qty_scaled: 5_000,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn proto_trade() -> ProtoUserTrade {
    ProtoUserTrade {
        symbol_id: 1,
        order_id: 9,
        match_id: 3,
        qty_scaled: 5_000,
        ..Default::default()
    }
}

fn twap() -> ProtoTrigger {
    ProtoTrigger {
        trigger_id: 11,
        symbol_id: 1,
        qty_scaled: 8_000,
        client_trigger_id: "twap".into(),
        runtime_details: Some(trigger::RuntimeDetails::TwapState(Box::new(TwapDetails {
            executed_qty_scaled: 5_000,
            ..Default::default()
        }))),
        ..Default::default()
    }
}

fn ladder() -> ProtoTrigger {
    ProtoTrigger {
        trigger_id: 12,
        symbol_id: 1,
        qty_scaled: 8_000,
        client_trigger_id: "ladder".into(),
        runtime_details: Some(trigger::RuntimeDetails::LadderState(Box::new(
            LadderDetails {
                executed_qty_scaled: 5_000,
                ..Default::default()
            },
        ))),
        ..Default::default()
    }
}

async fn client(catalogs: bool) -> (Client, MockHttpServer) {
    let http = MockHttpServer::spawn(|req| match req.path.as_str() {
        "/orders.v1.OrdersReadService/GetOrder" => connect_proto_ok(&GetOrderResponse {
            order: proto_order().into(),
            trades: vec![proto_trade()],
            ..Default::default()
        }),
        "/orders.v1.OrdersReadService/GetOpenOrders" => connect_proto_ok(&GetOpenOrdersResponse {
            orders: vec![proto_order()],
            ..Default::default()
        }),
        "/orders.v1.OrdersReadService/GetOrderHistory" => {
            connect_proto_ok(&GetOrderHistoryResponse {
                orders: vec![proto_order()],
                ..Default::default()
            })
        }
        "/orders.v1.OrdersReadService/GetUserTrades" => connect_proto_ok(&GetUserTradesResponse {
            trades: vec![proto_trade()],
            ..Default::default()
        }),
        "/triggers.v1.TriggersService/GetTrigger" => connect_proto_ok(&GetTriggerResponse {
            trigger: twap().into(),
            ..Default::default()
        }),
        "/triggers.v1.TriggersService/ListTriggers" => connect_proto_ok(&ListTriggersResponse {
            triggers: vec![twap(), ladder()],
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
    if catalogs {
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
    (client, http)
}

/// Each named quantity's scale, or `"missing"` when the field is absent.
fn scales<'a>(
    quantities: impl IntoIterator<Item = (&'a str, Option<&'a Quantity>)>,
) -> Vec<(String, Option<u32>)> {
    quantities
        .into_iter()
        .map(|(name, qty)| {
            let qty = qty.unwrap_or_else(|| panic!("{name} missing"));
            (name.to_owned(), qty.scale())
        })
        .collect()
}

fn order_scales(read: &str, order: &Order) -> Vec<(String, Option<u32>)> {
    scales([
        ("orig_qty", order.orig_qty.as_ref()),
        ("cum_qty", order.cum_qty.as_ref()),
        ("leaves_qty", order.leaves_qty.as_ref()),
        ("inherited_cum_qty", order.inherited_cum_qty.as_ref()),
        (
            "fee_total.qty",
            order.fee_total.as_ref().and_then(|fee| fee.qty.as_ref()),
        ),
    ])
    .into_iter()
    .map(|(name, scale)| (format!("{read} {name}"), scale))
    .collect()
}

fn trade_scales(read: &str, trades: &[UserTrade]) -> Vec<(String, Option<u32>)> {
    trades
        .iter()
        .map(|trade| {
            (
                format!("{read} trade qty"),
                trade.qty.as_ref().and_then(Quantity::scale),
            )
        })
        .collect()
}

/// Every quantity the order and trigger reads return, with its scale.
async fn read_all(client: &Client) -> Vec<(String, Option<u32>)> {
    let mut seen = Vec::new();
    let got = client
        .orders
        .get(OrderKey::OrderId("9".into()), None)
        .await
        .expect("get");
    seen.extend(order_scales("get", got.order.as_ref().expect("order")));
    seen.extend(trade_scales("get", &got.trades));
    for order in client
        .orders
        .list_open(None)
        .await
        .expect("list_open")
        .orders
    {
        seen.extend(order_scales("list_open", &order));
    }
    for order in client
        .orders
        .list_history(None, None)
        .await
        .expect("list_history")
        .orders
    {
        seen.extend(order_scales("list_history", &order));
    }
    let trades = client.trades.list(None, None).await.expect("trades.list");
    seen.extend(trade_scales("trades.list", &trades.trades));
    let one = client
        .triggers
        .get_by_id("11", None)
        .await
        .expect("trigger get")
        .expect("trigger");
    let listed = client
        .triggers
        .list_with(ListTriggersOpts::default())
        .await
        .expect("trigger list");
    for (read, trigger) in std::iter::once(("triggers.get", &one))
        .chain(listed.triggers.iter().map(|t| ("triggers.list", t)))
    {
        let executed = match trigger.details.as_ref() {
            Some(TriggerDetails::Twap(twap)) => twap.executed_qty.as_ref(),
            Some(TriggerDetails::Ladder(ladder)) => ladder.executed_qty.as_ref(),
            other => panic!("{read}: unexpected details {other:?}"),
        };
        seen.push((
            format!("{read} {} qty", trigger.client_trigger_id),
            trigger.qty.as_ref().and_then(Quantity::scale),
        ));
        seen.push((
            format!("{read} {} executed_qty", trigger.client_trigger_id),
            executed.and_then(Quantity::scale),
        ));
    }
    seen
}

#[tokio::test]
async fn order_and_trigger_reads_carry_the_catalog_base_scale() {
    let (client, _http) = client(true).await;
    let seen = read_all(&client).await;
    let unscaled: Vec<_> = seen.iter().filter(|(_, scale)| *scale != Some(5)).collect();
    assert!(
        unscaled.is_empty(),
        "read without the pair's scale: {unscaled:#?}"
    );

    let order = client
        .orders
        .get(OrderKey::OrderId("9".into()), None)
        .await
        .unwrap()
        .order
        .unwrap();
    assert_eq!(order.cum_qty.unwrap().format(None).unwrap(), "0.05");
}

#[tokio::test]
async fn reads_without_catalogs_still_decode_without_a_scale() {
    let (client, _http) = client(false).await;
    let seen = read_all(&client).await;
    let scaled: Vec<_> = seen.iter().filter(|(_, scale)| scale.is_some()).collect();
    assert!(
        scaled.is_empty(),
        "scale invented without catalogs: {scaled:#?}"
    );
}

/// Catalogs listing BTC-USDT (scale 5) and a pair whose base quantity scale
/// is zero; symbol 99 is listed by neither.
fn hydrate_stream_catalogs(client: &Client) {
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [
                {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 5, "quote_quantity_scale": 6},
                {"symbol": "ZERO-USDT", "symbol_id": 2, "base_quantity_scale": 0, "quote_quantity_scale": 0}
            ]
        }))
        .expect("catalog");
}

async fn stream_client(
    publications: Vec<Vec<u8>>,
    catalogs: bool,
) -> (
    Client,
    MockHttpServer,
    MockWsServer,
) {
    let ws = MockWsServer::spawn_centrifugo_publications_after_handshake(
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        publications,
    )
    .await;
    let (client, http) = super::streaming_client(&ws).await;
    if catalogs {
        hydrate_stream_catalogs(&client);
    }
    (client, http, ws)
}

/// `trades.subscribe` gives each fill its pair's base quantity scale from the
/// loaded catalogs, as `trades.list` does: a zero scale is kept and a pair
/// the catalogs don't list stays unscaled. Without catalogs nothing is scaled.
#[tokio::test]
async fn trade_stream_carries_the_catalog_base_scale() {
    use polyester::proto::orders::v1::UserTradeBatch;
    let batch = UserTradeBatch {
        events: [(1, 10), (2, 11), (99, 12)]
            .into_iter()
            .map(|(symbol_id, match_id)| ProtoUserTrade {
                symbol_id,
                match_id,
                qty_scaled: 5_000,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    for (catalogs, want) in [
        (true, [Some(5), Some(0), None]),
        (false, [None, None, None]),
    ] {
        let (client, _http, _ws) = stream_client(vec![batch.encode_to_vec()], catalogs).await;
        let mut sub = client.trades.subscribe(Some("acct")).await.expect("subscribe");
        let got: Vec<_> = super::recv_n(&mut sub, 3)
            .await
            .into_iter()
            .map(|t| (t.match_id, t.qty.as_ref().and_then(Quantity::scale)))
            .collect();
        let expected: Vec<_> = ["10", "11", "12"]
            .into_iter()
            .map(str::to_owned)
            .zip(want)
            .collect();
        assert_eq!(got, expected, "catalogs loaded: {catalogs}");
        sub.close();
    }
}

/// `triggers.subscribe` gives a trigger's `qty` and its TWAP or ladder
/// `executed_qty` the pair's base quantity scale, as `triggers.get` does.
#[tokio::test]
async fn trigger_stream_carries_the_catalog_base_scale() {
    let mut zero = ladder();
    zero.symbol_id = 2;
    let mut unknown = twap();
    unknown.symbol_id = 99;
    let publications = vec![
        twap().encode_to_vec(),
        zero.encode_to_vec(),
        unknown.encode_to_vec(),
    ];
    for (catalogs, want) in [
        (true, [Some(5), Some(0), None]),
        (false, [None, None, None]),
    ] {
        let (client, _http, _ws) = stream_client(publications.clone(), catalogs).await;
        let mut sub = client.triggers.subscribe(Some("acct")).await.expect("subscribe");
        let got: Vec<_> = super::recv_n(&mut sub, 3)
            .await
            .into_iter()
            .map(|trigger| {
                let executed = match trigger.details.as_ref() {
                    Some(TriggerDetails::Twap(twap)) => twap.executed_qty.as_ref(),
                    Some(TriggerDetails::Ladder(ladder)) => ladder.executed_qty.as_ref(),
                    other => panic!("unexpected details {other:?}"),
                };
                (
                    trigger.symbol_id,
                    trigger.qty.as_ref().and_then(Quantity::scale),
                    executed.and_then(Quantity::scale),
                )
            })
            .collect();
        let expected: Vec<_> = [1, 2, 99]
            .into_iter()
            .zip(want)
            .map(|(symbol_id, scale)| (symbol_id, scale, scale))
            .collect();
        assert_eq!(got, expected, "catalogs loaded: {catalogs}");
        sub.close();
    }
}
