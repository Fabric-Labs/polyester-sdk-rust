//! An order book subscription selected by `symbol_id` alone takes its symbol
//! and quantity scale from the spot catalog.

use super::*;
use polyester::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};
use polyester::services::CreateSubscriptionOptions;

async fn book_client() -> (MockHttpServer, MockWsServer, Client) {
    let ws = MockWsServer::spawn_centrifugo_public(Arc::default()).await;
    let http = MockHttpServer::spawn(move |request| {
        if request.path == "/orderbook.v1.OrderbookService/GetOrderBook" {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 2,
                book_seq: 1,
                available: true,
                bids: vec![PriceLevel {
                    price_ticks: 1_000_000_000,
                    qty_scaled: 1_000_000,
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
        .hydrate_spot_config_json(json!({"pairs": [
            {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8},
            {"symbol": "ETH-USDT", "symbol_id": 2, "base_quantity_scale": 6}
        ]}))
        .unwrap();
    (http, ws, client)
}

#[tokio::test]
async fn symbol_id_alone_uses_the_catalog_symbol_and_scale() {
    let (_http, _ws, client) = book_client().await;
    let mut subscription = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol_id: Some(2),
            ..Default::default()
        })
        .await
        .expect("symbol_id alone with a hydrated catalog");
    let view = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
        .await
        .expect("a view")
        .expect("stream must remain open");
    assert_eq!(view.symbol, "ETH-USDT");
    // ETH-USDT has quantity scale 6: 1_000_000 scaled is 1.
    assert_eq!(
        view.bids[0].qty.as_ref().unwrap().format(None).unwrap(),
        "1"
    );
    subscription.close();
}

#[tokio::test]
async fn unknown_symbol_id_alone_is_refused_before_connecting() {
    let (http, _ws, client) = book_client().await;
    let err = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol_id: Some(99),
            ..Default::default()
        })
        .await
        .err()
        .expect("unknown symbol_id");
    match err {
        Error::Validation(message) => assert!(message.contains("symbol_id 99"), "{message}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0);
}
