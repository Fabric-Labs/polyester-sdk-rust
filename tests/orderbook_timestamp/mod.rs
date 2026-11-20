//! Order book snapshots and updates carry the server's best-effort timestamp
//! (`GetOrderBookResponse.ts`, `OrderBookDelta.ts`) through the public paths,
//! against real local sockets serving a fixed response: the exact instant
//! when the server sends one, `None` when it sends none, never the local
//! receipt time.

use super::centrifugo::{Centrifugo, centrifugo_publication};
use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use buffa_types::google::protobuf::Timestamp;
use polyester::Client;
use polyester::Config;
use polyester::models::OrderbookData;
use polyester::orderbook::Subscription;
use polyester::proto::orderbook::v1::{
    GetOrderBookRequest, GetOrderBookResponse, OrderBookDelta, PriceLevel,
};
use polyester::services::CreateSubscriptionOptions;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const WAIT: Duration = Duration::from_secs(5);
const CHANNEL: &str = "public:spot:orderbook:deltas:depth:20:1:proto";

/// 2026-11-19T18:40:33.060385485Z, a snapshot time as a server sends it, at the
/// contract's nanosecond precision.
const SNAPSHOT_TS: (i64, i32) = (1_795_113_633, 60_385_485);
/// 2026-11-19T18:40:34.123456789Z.
const DELTA_TS: (i64, i32) = (1_795_113_634, 123_456_789);

fn proto_ts((seconds, nanos): (i64, i32)) -> buffa::MessageField<Timestamp> {
    buffa::MessageField::some(Timestamp {
        seconds,
        nanos,
        ..Default::default()
    })
}

fn instant((seconds, nanos): (i64, i32)) -> SystemTime {
    UNIX_EPOCH + Duration::new(seconds as u64, nanos as u32)
}

fn later((seconds, nanos): (i64, i32), by: i64) -> (i64, i32) {
    (seconds + by, nanos)
}

fn level(price_ticks: i64, qty_scaled: i64) -> PriceLevel {
    PriceLevel {
        price_ticks,
        qty_scaled,
        ..Default::default()
    }
}

/// Serves one fixed snapshot at sequence 7, epoch 2, stamped `ts` (none for
/// `None`), at whatever tier was asked for.
async fn api(ts: Option<(i64, i32)>) -> MockHttpServer {
    MockHttpServer::spawn(move |request| {
        if request.path != "/orderbook.v1.OrderbookService/GetOrderBook" {
            return HttpScript::NotFound;
        }
        let asked = GetOrderBookRequest::decode_from_slice(&request.body).unwrap();
        connect_proto_ok(&GetOrderBookResponse {
            symbol_id: asked.symbol_id,
            depth: asked.depth,
            book_seq: 7,
            book_epoch: 2,
            available: true,
            bids: vec![level(100_000_000_000, 5)],
            asks: vec![level(101_000_000_000, 6)],
            ts: ts.map_or_else(buffa::MessageField::none, proto_ts),
            ..Default::default()
        })
    })
    .await
}

fn client(http: &MockHttpServer, ws: Option<&Centrifugo>) -> Client {
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.map_or_else(|| "ws://127.0.0.1:9".to_owned(), |ws| ws.url.clone()),
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

fn delta(start: u64, end: u64, reset: bool, ts: Option<(i64, i32)>) -> Vec<u8> {
    centrifugo_publication(
        &OrderBookDelta {
            symbol_id: 1,
            book_seq_start: start,
            book_seq_end: end,
            reset,
            bids: vec![level(99_000_000_000, i64::try_from(end).unwrap())],
            ts: ts.map_or_else(buffa::MessageField::none, proto_ts),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

/// `orderbook.get` reports the snapshot's timestamp when the server supplies
/// one, and `None` when it does not; the rest of the snapshot is unchanged.
#[tokio::test]
async fn get_carries_the_snapshot_timestamp_or_none() {
    for ts in [Some(SNAPSHOT_TS), None] {
        let http = api(ts).await;
        let book = client(&http, None)
            .orderbook
            .get("BTC-USDT", Some(1))
            .await
            .unwrap();
        assert_eq!(book.ts, ts.map(instant), "{ts:?}");
        assert_eq!(
            (
                book.symbol.as_str(),
                book.depth,
                book.book_seq.as_str(),
                book.book_epoch.as_str(),
                book.available,
                book.bids.len(),
                book.asks.len(),
            ),
            ("BTC-USDT", 1, "7", "2", true, 1, 1),
            "{ts:?}"
        );
    }
}

/// Raw deltas carry each update's timestamp when the server supplies one,
/// and `None` when it does not.
#[tokio::test]
async fn raw_deltas_carry_the_update_timestamp_or_none() {
    for ts in [Some(DELTA_TS), None] {
        let http = api(None).await;
        let ws = Centrifugo::spawn().await;
        let client = client(&http, Some(&ws));
        let mut sub = client
            .orderbook
            .subscribe_deltas(1, Some(20))
            .await
            .unwrap();
        ws.await_subscribed(CHANNEL).await;
        ws.push(CHANNEL, delta(7, 8, false, ts));
        let update = tokio::time::timeout(WAIT, sub.recv())
            .await
            .expect("no delta delivered")
            .unwrap();
        assert_eq!(update.ts, ts.map(instant), "{ts:?}");
        assert_eq!(
            (
                update.book_seq_start,
                update.book_seq_end,
                update.bids.len()
            ),
            (7, 8, 1)
        );
    }
}

async fn view_at(sub: &mut Subscription, seq: &str) -> OrderbookData {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let view = tokio::time::timeout_at(deadline, sub.updates().recv())
            .await
            .unwrap_or_else(|_| panic!("no view at book sequence {seq}: {:?}", sub.err()))
            .expect("subscription ended");
        if view.book_seq == seq {
            return view;
        }
    }
}

/// A managed book's view carries the timestamp of the latest snapshot or
/// update applied to it, `None` when that one carried none. An update that is
/// not applied, stale or out of sequence, leaves it unchanged.
#[tokio::test]
async fn managed_book_carries_the_latest_applied_timestamp() {
    for snapshot in [Some(SNAPSHOT_TS), None] {
        let http = api(snapshot).await;
        let ws = Centrifugo::spawn().await;
        let client = client(&http, Some(&ws));
        let mut sub = client
            .orderbook
            .create_subscription(CreateSubscriptionOptions {
                symbol: "BTC-USDT".into(),
                symbol_id: Some(1),
                depth: Some(10),
                bucket: None,
            })
            .await
            .unwrap();
        ws.await_subscribed(CHANNEL).await;

        let view = view_at(&mut sub, "7").await;
        assert_eq!(view.ts, snapshot.map(instant), "snapshot view");

        ws.push(CHANNEL, delta(7, 8, false, Some(DELTA_TS)));
        let view = view_at(&mut sub, "8").await;
        assert_eq!(view.ts, Some(instant(DELTA_TS)), "applied update");

        // Stale: ends at or before the book's sequence, so it is dropped.
        ws.push(CHANNEL, delta(6, 7, false, Some(later(SNAPSHOT_TS, -3600))));
        // Out of sequence: does not start at the book's sequence, so the
        // book waits for a reset and reads unavailable meanwhile.
        ws.push(CHANNEL, delta(9, 10, false, Some(later(DELTA_TS, 60))));
        let view = loop {
            let view = view_at(&mut sub, "8").await;
            if !view.available {
                break view;
            }
            assert_eq!(view.ts, Some(instant(DELTA_TS)), "stale update");
        };
        assert_eq!(view.ts, Some(instant(DELTA_TS)), "out-of-sequence update");

        // A reset applies and brings its own time.
        ws.push(CHANNEL, delta(0, 12, true, Some(later(DELTA_TS, 1))));
        let view = view_at(&mut sub, "12").await;
        assert!(view.available);
        assert_eq!(view.ts, Some(instant(later(DELTA_TS, 1))), "reset");

        // An applied update without a time leaves the view without one.
        ws.push(CHANNEL, delta(12, 13, false, None));
        let view = view_at(&mut sub, "13").await;
        assert_eq!(view.ts, None, "untimestamped update");
        sub.close();
    }
}
