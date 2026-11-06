//! Subscriptions the server ends, through public client paths against real
//! local sockets: no silent stall, consumers released, bounded recovery.

#[allow(dead_code)]
#[path = "hardening_support/centrifugo.rs"]
mod centrifugo;
#[allow(dead_code)]
mod hardening_support;

use buffa::Message;
use centrifugo::{Centrifugo, centrifugo_publication, unsubscribe_frame};
use hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use polyester::proto::marketoverview::v1::{ListMarketOverviewResponse, MarketOverview};
use polyester::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};
use polyester::services::{CreateSubscriptionOptions, MarketOverviewCreateSubscriptionOptions};
use polyester::{Client, Config};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BOOK_CHANNEL: &str = "public:spot:orderbook:deltas:depth:10:1:proto";
const OVERVIEW_CHANNEL: &str = "public:spot:market_overview:updates:proto";
const WAIT: Duration = Duration::from_secs(10);

fn test_client(api_url: String, ws_url: String) -> Client {
    let client = Client::new(Config {
        api_url,
        ws_url,
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [
            {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}
        ]}))
        .unwrap();
    client
}

fn available_book(seq: u64) -> GetOrderBookResponse {
    GetOrderBookResponse {
        symbol_id: 1,
        book_seq: seq,
        available: true,
        bids: vec![PriceLevel {
            price_ticks: 100,
            qty_scaled: 5,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn unavailable_book() -> GetOrderBookResponse {
    GetOrderBookResponse {
        symbol_id: 1,
        ..Default::default()
    }
}

/// Book service answering from `respond(call)`, counting only snapshot reads.
async fn book_api(
    respond: impl Fn(usize) -> GetOrderBookResponse + Send + Sync + 'static,
) -> (MockHttpServer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let http = MockHttpServer::spawn(move |request| {
        if request.method == "POST" && request.path.ends_with("/GetOrderBook") {
            connect_proto_ok(&respond(count.fetch_add(1, Ordering::SeqCst) + 1))
        } else {
            HttpScript::NotFound
        }
    })
    .await;
    (http, calls)
}

async fn create_book(client: &Client) -> polyester::orderbook::Subscription {
    client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            depth: Some(10),
            ..Default::default()
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn server_unsubscribe_ends_subscription_instead_of_stalling() {
    const CHANNEL: &str = "public:spot:trades:1:proto";
    let ws = Centrifugo::spawn().await;
    let client = test_client("http://127.0.0.1:1".into(), ws.url.clone());
    let mut sub = client.realtime.subscribe_raw(CHANNEL).await.unwrap();
    let reported = Arc::new(Mutex::new(Vec::new()));
    let sink = reported.clone();
    sub.set_on_error(move |err| sink.lock().unwrap().push(err));
    ws.await_subscribed(CHANNEL).await;

    ws.push(CHANNEL, centrifugo_publication(b"live"));
    let live = tokio::time::timeout(WAIT, sub.recv()).await.unwrap();
    assert_eq!(live.as_deref(), Some(&b"live"[..]), "live control");
    assert!(sub.err().is_none(), "live control: {:?}", sub.err());

    ws.push(CHANNEL, centrifugo_publication(b"last"));
    ws.push(
        CHANNEL,
        unsubscribe_frame(CHANNEL, 2000, "server unsubscribe"),
    );
    let mut drained = Vec::new();
    let finished = tokio::time::timeout(WAIT, async {
        while let Some(row) = sub.recv().await {
            drained.push(row);
        }
    })
    .await;
    assert!(
        finished.is_ok(),
        "subscription stayed open and silent after the server ended it"
    );
    assert_eq!(drained, vec![b"last".to_vec()]);
    let ended = sub.err().expect("ended without an error");
    assert!(
        matches!(
            &ended,
            polyester::Error::SubscriptionEnded {
                channel,
                code: 2000,
                server_reason,
                reason: polyester::errors::SubscriptionEndReason::NotResubscribable,
                cause: None,
            } if channel == CHANNEL && server_reason == "server unsubscribe"
        ),
        "{ended:?}"
    );
    assert_eq!(reported.lock().unwrap().len(), 1);
    // recv_result preserves the same terminal error.
    assert!(matches!(
        sub.recv_result().await,
        Err(polyester::Error::SubscriptionEnded { .. })
    ));
}

#[tokio::test]
async fn unsubscribe_of_another_channel_is_ignored() {
    const CHANNEL: &str = "public:spot:trades:1:proto";
    let ws = Centrifugo::spawn().await;
    let client = test_client("http://127.0.0.1:1".into(), ws.url.clone());
    let mut sub = client.realtime.subscribe_raw(CHANNEL).await.unwrap();
    ws.await_subscribed(CHANNEL).await;
    ws.push(
        CHANNEL,
        unsubscribe_frame("public:spot:trades:2:proto", 2000, "other"),
    );
    ws.push(CHANNEL, centrifugo_publication(b"after"));
    let row = tokio::time::timeout(WAIT, sub.recv()).await.unwrap();
    assert_eq!(row.as_deref(), Some(&b"after"[..]));
}

#[tokio::test]
async fn managed_book_the_server_ends_releases_consumers() {
    let ws = Centrifugo::spawn().await;
    let (http, _) = book_api(|_| available_book(10)).await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    let reported = Arc::new(Mutex::new(Vec::new()));
    let sink = reported.clone();
    sub.set_on_error(move |err| sink.lock().unwrap().push(err));
    ws.await_subscribed(BOOK_CHANNEL).await;
    assert_eq!(sub.updates().recv().await.unwrap().book_seq, "10");

    ws.push(
        BOOK_CHANNEL,
        unsubscribe_frame(BOOK_CHANNEL, 2000, "server unsubscribe"),
    );
    let released = tokio::time::timeout(WAIT, sub.updates().recv()).await;
    assert!(
        matches!(released, Ok(None)),
        "consumers not released after the server ended the book: {released:?}"
    );
    assert!(matches!(
        sub.err(),
        Some(polyester::Error::SubscriptionEnded { .. })
    ));
    assert_eq!(reported.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn first_view_is_the_fetched_snapshot() {
    let ws = Centrifugo::spawn().await;
    let (http, _) = book_api(|_| available_book(10)).await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    let first = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.book_seq, "10");
    assert!(first.available);
    assert_eq!(first.bids.len(), 1);
}

#[tokio::test]
async fn unavailable_snapshot_is_requested_again() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = book_api(|call| {
        if call < 3 {
            unavailable_book()
        } else {
            available_book(10)
        }
    })
    .await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    let first = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!first.available);
    assert!(first.bids.is_empty());
    // A repeated unavailable answer adds no view: the next one is the book.
    let second = tokio::time::timeout(Duration::from_secs(15), sub.updates().recv())
        .await
        .expect("the unavailable snapshot was never requested again")
        .unwrap();
    assert!(second.available);
    assert_eq!(second.book_seq, "10");
    assert_eq!(second.bids.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "requests stop once available"
    );
}

#[tokio::test]
async fn available_snapshot_is_not_requested_again() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = book_api(|_| available_book(10)).await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    sub.updates().recv().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A newer available snapshot makes the pending request obsolete: it must not
/// fire, or a failure there could end a healthy stream.
#[tokio::test]
async fn available_snapshot_cancels_pending_request() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = book_api(|call| {
        if call == 1 {
            unavailable_book()
        } else {
            available_book(10)
        }
    })
    .await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    assert!(!sub.updates().recv().await.unwrap().available);
    sub.refresh_snapshot().await.unwrap();
    let view = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .unwrap()
        .unwrap();
    assert!(view.available);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "before the old timer is due"
    );
    // Past the 1 s delay the unavailable snapshot scheduled.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2, "an obsolete request fired");
    assert!(sub.err().is_none(), "{:?}", sub.err());
}

#[tokio::test]
async fn reconnect_with_an_available_snapshot_cancels_pending_request() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = book_api(|call| {
        if call == 1 {
            unavailable_book()
        } else {
            available_book(10)
        }
    })
    .await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    ws.await_subscribed(BOOK_CHANNEL).await;
    assert!(!sub.updates().recv().await.unwrap().available);
    ws.drop_connection(BOOK_CHANNEL);
    let view = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .unwrap()
        .unwrap();
    assert!(view.available);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "before the old timer is due"
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2, "an obsolete request fired");
}

#[tokio::test]
async fn close_stops_requests_for_an_unavailable_book() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = book_api(|_| unavailable_book()).await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_book(&client).await;
    assert!(!sub.updates().recv().await.unwrap().available);
    sub.close();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

fn overview_response(price: i64) -> ListMarketOverviewResponse {
    ListMarketOverviewResponse {
        markets: vec![MarketOverview {
            symbol_id: 1,
            last_price_ticks: price,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Overview API: the first snapshot succeeds; later ones answer 503.
async fn overview_api() -> (MockHttpServer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let http = MockHttpServer::spawn(move |request| {
        if !(request.method == "POST" && request.path.ends_with("/ListMarketOverview")) {
            return HttpScript::NotFound;
        }
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            connect_proto_ok(&overview_response(1))
        } else {
            HttpScript::Json {
                status: 503,
                body: br#"{"code":"unavailable","message":"snapshot unavailable"}"#.to_vec(),
            }
        }
    })
    .await;
    (http, calls)
}

fn overview_publication(price: i64) -> Vec<u8> {
    use polyester::proto::marketoverview::v1::MarketOverviewBatch;
    centrifugo_publication(
        &MarketOverviewBatch {
            markets: overview_response(price).markets,
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

async fn create_overview(client: &Client) -> polyester::marketoverview::Subscription {
    client
        .market_overview
        .create_subscription(MarketOverviewCreateSubscriptionOptions::default())
        .await
        .unwrap()
}

/// A dropped socket whose snapshot refresh keeps failing is terminal: consumers
/// drain what is queued, then finish, keeping the typed error and one callback.
#[tokio::test]
async fn overview_terminal_failure_releases_consumers_after_queued_rows() {
    let ws = Centrifugo::spawn().await;
    let (http, calls) = overview_api().await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_overview(&client).await;
    let reported = Arc::new(Mutex::new(Vec::new()));
    let sink = reported.clone();
    sub.set_on_error(move |err| sink.lock().unwrap().push(err));
    ws.await_subscribed(OVERVIEW_CHANNEL).await;
    let healthy = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(healthy[0].symbol_id, 1, "healthy control");
    assert!(sub.err().is_none(), "healthy control");

    // Published before the socket drops: it must still reach the consumer.
    ws.push(OVERVIEW_CHANNEL, overview_publication(8));
    ws.drop_connection(OVERVIEW_CHANNEL);

    let mut rows = Vec::new();
    let finished = tokio::time::timeout(WAIT, async {
        while let Some(row) = sub.updates().recv().await {
            rows.push(row);
        }
    })
    .await;
    assert!(
        finished.is_ok(),
        "market overview left consumers waiting after terminal failure"
    );
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "the failing snapshot refresh must have happened"
    );
    assert_eq!(
        rows.len(),
        1,
        "queued row must drain before the stream ends"
    );
    let err = sub.err().expect("typed terminal error");
    let reported = reported.lock().unwrap();
    // The dropped socket, then one report per failed refresh, the last being
    // the terminal error: nothing reports it twice.
    assert_eq!(
        reported.len(),
        calls.load(Ordering::SeqCst),
        "callbacks: {reported:?}"
    );
    assert_eq!(reported.last().unwrap().to_string(), err.to_string());
}

#[tokio::test]
async fn overview_the_server_ends_releases_consumers() {
    let ws = Centrifugo::spawn().await;
    let (http, _) = overview_api().await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_overview(&client).await;
    ws.await_subscribed(OVERVIEW_CHANNEL).await;
    sub.updates().recv().await.unwrap();
    ws.push(
        OVERVIEW_CHANNEL,
        unsubscribe_frame(OVERVIEW_CHANNEL, 2000, "server unsubscribe"),
    );
    let released = tokio::time::timeout(WAIT, sub.updates().recv()).await;
    assert!(matches!(released, Ok(None)), "{released:?}");
    assert!(matches!(
        sub.err(),
        Some(polyester::Error::SubscriptionEnded { .. })
    ));
}

#[tokio::test]
async fn overview_explicit_close_releases_consumers_without_error() {
    let ws = Centrifugo::spawn().await;
    let (http, _) = overview_api().await;
    let client = test_client(http.base_url(), ws.url.clone());
    let mut sub = create_overview(&client).await;
    sub.updates().recv().await.unwrap();
    sub.close();
    let released = tokio::time::timeout(WAIT, sub.updates().recv()).await;
    assert!(matches!(released, Ok(None)), "{released:?}");
    assert!(sub.err().is_none(), "{:?}", sub.err());
}
