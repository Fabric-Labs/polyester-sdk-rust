//! An order book bucket is empty (no bucketing) or a positive price; any other
//! bucket is refused, saying what is valid, before connecting and by
//! `set_bucket`, which keeps the current bucket.

use super::*;
use polyester::orderbook::{Subscription, parse_bucket_ticks};
use polyester::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};
use polyester::services::CreateSubscriptionOptions;

/// A whitespace-only bucket is not empty: like a whitespace-only price, it is
/// refused. Whitespace around a valid price is ignored, as for every price.
const INVALID: [&str; 9] = [
    "abc",
    "-1",
    "0",
    "0.000",
    " ",
    "\t \n",
    "0.0000000001",
    "1e3",
    "NaN",
];

fn assert_bucket_refused(result: Result<(), Error>, bucket: &str) {
    match result {
        Err(Error::Validation(message)) => assert!(
            message.contains("positive price") && message.contains("empty for no bucketing"),
            "bucket {bucket:?}: {message}"
        ),
        other => panic!("bucket {bucket:?}: {other:?}"),
    }
}

#[test]
fn parse_bucket_ticks_accepts_empty_and_positive_prices_only() {
    for (bucket, ticks) in [
        ("", 0),
        ("0.01", 10_000_000),
        (" 0.01\t", 10_000_000),
        ("1", 1_000_000_000),
        ("0.000000001", 1),
    ] {
        assert_eq!(parse_bucket_ticks(bucket).unwrap(), ticks, "{bucket:?}");
    }
    for bucket in INVALID {
        assert_bucket_refused(parse_bucket_ticks(bucket).map(|_| ()), bucket);
    }
}

async fn book_client() -> (MockHttpServer, MockWsServer, Client) {
    let ws = MockWsServer::spawn_centrifugo_public(Arc::default()).await;
    let http = MockHttpServer::spawn(move |request| {
        if request.path == "/orderbook.v1.OrderbookService/GetOrderBook" {
            connect_proto_ok(&GetOrderBookResponse {
                depth: polyester::proto::polyester::r#type::v1::DepthTier::DEPTH_50.into(),
                symbol_id: 1,
                book_seq: 1,
                available: true,
                bids: vec![PriceLevel {
                    price_ticks: 71_600_000,
                    qty_scaled: 3,
                    ..Default::default()
                }],
                ..Default::default()
            })
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol": "DOGE-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .unwrap();
    (http, ws, client)
}

fn options(bucket: &str) -> CreateSubscriptionOptions {
    CreateSubscriptionOptions {
        symbol: "DOGE-USDT".into(),
        symbol_id: Some(1),
        depth: Some(50),
        bucket: Some(bucket.into()),
    }
}

#[tokio::test]
async fn create_subscription_refuses_an_invalid_bucket_before_connecting() {
    let (http, _ws, client) = book_client().await;
    for bucket in ["abc", "-1", "0"] {
        let result = client.orderbook.create_subscription(options(bucket)).await;
        assert_bucket_refused(result.map(|_| ()), bucket);
    }
    assert_eq!(
        http.requests.load(Ordering::SeqCst),
        0,
        "fetched a snapshot"
    );
}

async fn next(subscription: &mut Subscription) -> polyester::models::OrderbookData {
    tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
        .await
        .expect("a view")
        .expect("stream must remain open")
}

#[tokio::test]
async fn set_bucket_refuses_an_invalid_bucket_and_keeps_the_current_one() {
    let (_http, _ws, client) = book_client().await;
    let mut subscription = client
        .orderbook
        .create_subscription(options("0.01"))
        .await
        .unwrap();
    let first = next(&mut subscription).await;
    assert_eq!(first.bids[0].price.as_ref().unwrap().as_ticks(), 70_000_000);
    for bucket in INVALID {
        assert_bucket_refused(subscription.set_bucket(bucket), bucket);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.updates().recv())
            .await
            .is_err(),
        "a refused bucket added a view"
    );
    subscription.set_bucket("").unwrap();
    let plain = next(&mut subscription).await;
    assert_eq!(plain.bids[0].price.as_ref().unwrap().as_ticks(), 71_600_000);
    subscription.close();
}
