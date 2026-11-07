//! Managed order books at any depth, through public paths against real local sockets.
//!
//! The server here behaves like the public stream contract: it accepts a subscribe
//! at any depth, but publishes only on the tiers 1, 20, 50, 200 and 500, each as a
//! SET diff of that tier's own top-N view. Snapshots answer at any depth the
//! snapshot enum names, up to 1000.

#[allow(dead_code)]
#[path = "../hardening_support/centrifugo.rs"]
mod centrifugo;

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use centrifugo::{Centrifugo, centrifugo_publication};
use polyester::models::{OrderbookData, OrderbookLevel};
use polyester::orderbook::Subscription;
use polyester::proto::orderbook::v1::{
    GetOrderBookRequest, GetOrderBookResponse, OrderBookDelta, PriceLevel,
};
use polyester::services::CreateSubscriptionOptions;
use polyester::{Client, Config, Error};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TIERS: [u32; 5] = [1, 20, 50, 200, 500];
const SNAPSHOT_DEPTHS: [u32; 9] = [1, 5, 10, 20, 50, 100, 200, 500, 1000];
const UNIT: i64 = 1_000_000_000; // One quote unit, in price ticks.
const BEST_BID: i64 = 2000 * UNIT;
const BEST_ASK: i64 = 2001 * UNIT;
const WAIT: Duration = Duration::from_secs(5);

fn channel_for(depth: u32) -> String {
    format!("public:spot:orderbook:deltas:depth:{depth}:1:proto")
}

fn tier_for(depth: u32) -> u32 {
    *TIERS.iter().find(|tier| **tier >= depth).unwrap()
}

type Side = BTreeMap<i64, i64>;

fn top_bids(side: &Side, depth: u32) -> Side {
    side.iter()
        .rev()
        .take(depth as usize)
        .map(|(price, qty)| (*price, *qty))
        .collect()
}

fn top_asks(side: &Side, depth: u32) -> Side {
    side.iter()
        .take(depth as usize)
        .map(|(price, qty)| (*price, *qty))
        .collect()
}

fn proto_levels(side: &Side, descending: bool) -> Vec<PriceLevel> {
    let level = |(price, qty): (&i64, &i64)| PriceLevel {
        price_ticks: *price,
        qty_scaled: *qty,
        ..Default::default()
    };
    if descending {
        side.iter().rev().map(level).collect()
    } else {
        side.iter().map(level).collect()
    }
}

fn level_diff(old: &Side, new: &Side) -> Side {
    let mut changes: Side = new
        .iter()
        .filter(|(price, qty)| old.get(price) != Some(qty))
        .map(|(price, qty)| (*price, *qty))
        .collect();
    for price in old.keys() {
        if !new.contains_key(price) {
            changes.insert(*price, 0);
        }
    }
    changes
}

#[derive(Clone)]
struct Published {
    bids: Side,
    asks: Side,
    seq: u64,
}

struct Book {
    bids: Side,
    asks: Side,
    seq: u64,
    available: bool,
    snapshots: Vec<u32>,
    published: HashMap<u32, Published>,
}

impl Book {
    fn new(levels: i64) -> Self {
        let bids: Side = (0..levels).map(|i| (BEST_BID - i * UNIT, 5)).collect();
        let asks: Side = (0..levels).map(|i| (BEST_ASK + i * UNIT, 5)).collect();
        let published = TIERS
            .iter()
            .map(|tier| {
                let view = Published {
                    bids: top_bids(&bids, *tier),
                    asks: top_asks(&asks, *tier),
                    seq: 100,
                };
                (*tier, view)
            })
            .collect();
        Self {
            bids,
            asks,
            seq: 100,
            available: true,
            snapshots: Vec::new(),
            published,
        }
    }

    fn snapshot(&mut self, request: &GetOrderBookRequest) -> GetOrderBookResponse {
        let depth = SNAPSHOT_DEPTHS[usize::try_from(request.depth.to_i32() - 1).unwrap()];
        self.snapshots.push(depth);
        let (bids, asks) = if self.available {
            (
                proto_levels(&top_bids(&self.bids, depth), true),
                proto_levels(&top_asks(&self.asks, depth), false),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        GetOrderBookResponse {
            symbol_id: 1,
            book_seq: self.seq,
            available: self.available,
            bids,
            asks,
            ..Default::default()
        }
    }
}

/// A book published per tier as SET diffs, and read through the snapshot API.
struct Market {
    book: Arc<Mutex<Book>>,
    ws: Centrifugo,
    http: MockHttpServer,
    delivered: Mutex<HashMap<String, usize>>,
}

impl Market {
    async fn spawn(levels: i64) -> Self {
        let book = Arc::new(Mutex::new(Book::new(levels)));
        let answering = book.clone();
        let http = MockHttpServer::spawn(move |request| {
            if request.method == "POST" && request.path.ends_with("/GetOrderBook") {
                let message = GetOrderBookRequest::decode_from_slice(&request.body).unwrap();
                connect_proto_ok(&answering.lock().unwrap().snapshot(&message))
            } else {
                HttpScript::NotFound
            }
        })
        .await;
        Self {
            book,
            ws: Centrifugo::spawn().await,
            http,
            delivered: Mutex::default(),
        }
    }

    fn client(&self) -> Client {
        let client = Client::new(Config {
            api_url: self.http.base_url(),
            ws_url: self.ws.url.clone(),
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

    async fn await_subscribed(&self, depth: u32) {
        self.ws.await_subscribed(&channel_for(depth)).await;
    }

    fn delivered(&self, depth: u32) -> usize {
        *self
            .delivered
            .lock()
            .unwrap()
            .get(&channel_for(depth))
            .unwrap_or(&0)
    }

    fn snapshots(&self) -> Vec<u32> {
        self.book.lock().unwrap().snapshots.clone()
    }

    fn connections(&self) -> usize {
        self.ws.connects.load(Ordering::SeqCst)
    }

    /// Delivers one delta to whatever subscribed at `depth`, as given.
    fn send(&self, depth: u32, start: u64, end: u64, bids: &Side, asks: &Side, reset: bool) {
        let channel = channel_for(depth);
        let delta = OrderBookDelta {
            symbol_id: 1,
            book_seq_start: start,
            book_seq_end: end,
            reset,
            bids: proto_levels(bids, true),
            asks: proto_levels(asks, false),
            ..Default::default()
        };
        if self
            .ws
            .try_push(&channel, centrifugo_publication(&delta.encode_to_vec()))
        {
            *self.delivered.lock().unwrap().entry(channel).or_insert(0) += 1;
        }
    }

    /// Advances the sequence and sends each tier's diff, on the tiers only.
    fn publish(&self, change: impl FnOnce(&mut Book)) {
        let mut book = self.book.lock().unwrap();
        change(&mut book);
        book.seq += 1;
        let seq = book.seq;
        for tier in TIERS {
            let bids = top_bids(&book.bids, tier);
            let asks = top_asks(&book.asks, tier);
            let last = book.published[&tier].clone();
            let bid_diff = level_diff(&last.bids, &bids);
            let ask_diff = level_diff(&last.asks, &asks);
            if bid_diff.is_empty() && ask_diff.is_empty() {
                continue;
            }
            self.send(tier, last.seq, seq, &bid_diff, &ask_diff, false);
            book.published.insert(tier, Published { bids, asks, seq });
        }
    }

    fn update(&self, change: impl FnOnce(&mut Book)) {
        change(&mut self.book.lock().unwrap());
    }
}

async fn book(client: &Client, depth: Option<u32>, bucket: Option<&str>) -> Subscription {
    client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            depth,
            bucket: bucket.map(str::to_owned),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn next_view(sub: &mut Subscription, what: &str) -> OrderbookData {
    match tokio::time::timeout(WAIT, sub.updates().recv()).await {
        Ok(Some(view)) => view,
        Ok(None) => panic!("no {what}: stream ended: {:?}", sub.err()),
        Err(_) => panic!("no {what} within {WAIT:?}: the managed book went silent"),
    }
}

fn ticks(levels: &[OrderbookLevel]) -> Vec<i64> {
    levels
        .iter()
        .map(|level| level.price.as_ref().unwrap().as_ticks())
        .collect()
}

fn qtys(levels: &[OrderbookLevel]) -> Vec<i64> {
    levels
        .iter()
        .map(|level| level.qty.as_ref().unwrap().as_scaled())
        .collect()
}

fn ladder(from: i64, step: i64, count: u32) -> Vec<i64> {
    (0..i64::from(count)).map(|i| from + i * step).collect()
}

// Without this, a server that publishes everywhere would make every test here pass.
#[tokio::test]
async fn control_an_unpublished_channel_is_accepted_and_silent() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut silent = client
        .realtime
        .subscribe_raw(&channel_for(10))
        .await
        .unwrap();
    market.await_subscribed(10).await;
    let mut live = client
        .realtime
        .subscribe_raw(&channel_for(20))
        .await
        .unwrap();
    market.await_subscribed(20).await;

    market.publish(|book| {
        book.bids.insert(BEST_BID, 7);
    });

    let row = tokio::time::timeout(WAIT, live.recv()).await.unwrap();
    assert!(row.is_some(), "the published tier delivered nothing");
    let quiet = tokio::time::timeout(Duration::from_millis(300), silent.recv()).await;
    assert!(
        quiet.is_err(),
        "the unpublished channel delivered {quiet:?}"
    );
    assert!(silent.err().is_none(), "the server accepted the subscribe");
    assert_eq!(market.delivered(10), 0);
    assert_eq!(market.delivered(20), 1);
}

#[tokio::test]
async fn depth_measured_silent_now_updates_on_deltas() {
    for depth in [5u32, 10, 25, 100] {
        let market = Market::spawn(600).await;
        let client = market.client();
        let mut sub = book(&client, Some(depth), None).await;
        let first = next_view(&mut sub, "first view").await;
        market.ws.await_subscribes(1).await;

        market.publish(|book| {
            book.bids.insert(BEST_BID, 7);
        });
        let updated = next_view(&mut sub, "view after a market move").await;

        assert_eq!(
            market.delivered(tier_for(depth)),
            1,
            "no delta was delivered"
        );
        assert_eq!(qtys(&first.bids)[0], 5);
        assert_eq!(qtys(&updated.bids)[0], 7);
        assert_eq!(
            (first.book_seq.as_str(), updated.book_seq.as_str()),
            ("100", "101")
        );
        assert_eq!(updated.bids.len(), depth as usize);
        assert_eq!(updated.asks.len(), depth as usize);
    }
}

#[tokio::test]
async fn every_requested_depth_rides_its_tier_and_trims() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut seen = std::collections::BTreeSet::new();
    let mut qty = 5;
    for depth in 1..=500u32 {
        let tier = tier_for(depth);
        seen.insert(tier);
        let mut sub = book(&client, Some(depth), None).await;
        market.await_subscribed(tier).await;
        let first = next_view(&mut sub, "first view").await;
        assert_eq!(*market.snapshots().last().unwrap(), tier, "depth {depth}");
        assert_eq!(first.depth, depth);
        assert_eq!(ticks(&first.bids), ladder(BEST_BID, -UNIT, depth));
        assert_eq!(ticks(&first.asks), ladder(BEST_ASK, UNIT, depth));

        let before = market.delivered(tier);
        qty += 1;
        market.publish(|book| {
            book.bids.insert(BEST_BID, qty);
        });
        let updated = next_view(&mut sub, "view after a delta").await;
        assert_eq!(market.delivered(tier), before + 1, "depth {depth}");
        assert_eq!(updated.depth, depth);
        assert_eq!(qtys(&updated.bids)[0], qty, "depth {depth}");
        assert_eq!(updated.bids.len(), depth as usize);
        assert_eq!(updated.asks.len(), depth as usize);
        sub.close();
    }
    assert_eq!(seen.into_iter().collect::<Vec<_>>(), TIERS);
    assert_eq!(market.connections(), 500);
}

#[tokio::test]
async fn each_tier_uses_itself_for_channel_snapshot_and_view() {
    for tier in TIERS {
        let market = Market::spawn(600).await;
        let client = market.client();
        let mut sub = book(&client, Some(tier), None).await;
        market.await_subscribed(tier).await;
        let view = next_view(&mut sub, "first view").await;
        assert_eq!(market.snapshots(), [tier]);
        assert_eq!(view.depth, tier);
        assert_eq!(view.bids.len(), tier as usize);
        assert_eq!(view.asks.len(), tier as usize);
        market.publish(|book| {
            book.asks.insert(BEST_ASK, 9);
        });
        let updated = next_view(&mut sub, "view after a delta").await;
        assert_eq!(qtys(&updated.asks)[0], 9);
    }
}

#[tokio::test]
async fn default_depth_stays_fifty() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, None, None).await;
    market.await_subscribed(50).await;
    assert_eq!(next_view(&mut sub, "first view").await.depth, 50);
}

#[tokio::test]
async fn unservable_depth_is_refused_before_connecting() {
    for depth in [501u32, 1000, 5000, 0] {
        let market = Market::spawn(600).await;
        let client = market.client();
        let refused = client
            .orderbook
            .create_subscription(CreateSubscriptionOptions {
                symbol: "BTC-USDT".into(),
                depth: Some(depth),
                ..Default::default()
            })
            .await
            .err()
            .unwrap_or_else(|| panic!("depth {depth} was accepted"));
        assert!(
            matches!(&refused, Error::Validation(_)) && refused.to_string().contains("depth"),
            "{refused:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(market.connections(), 0, "depth {depth} reached the server");
        assert!(market.snapshots().is_empty());

        // Control: the same client does reach this server at the top tier.
        let mut sub = book(&client, Some(500), None).await;
        next_view(&mut sub, "first view").await;
        assert_eq!(market.connections(), 1);
        assert_eq!(market.snapshots(), [500]);
    }
}

#[tokio::test]
async fn refusal_above_the_top_tier_names_the_limit_and_the_snapshot() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let refused = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            depth: Some(1000),
            ..Default::default()
        })
        .await
        .err()
        .expect("depth 1000 was accepted");
    let text = refused.to_string();
    assert!(
        text.contains("500") && text.contains("orderbook.get"),
        "{text}"
    );
}

// Fails if the snapshot is taken at the requested depth: level 11 would be unknown.
#[tokio::test]
async fn trimmed_view_promotes_the_next_level_after_a_removal() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    let first = next_view(&mut sub, "first view").await;
    assert_eq!(*ticks(&first.bids).last().unwrap(), BEST_BID - 9 * UNIT);

    market.publish(|book| {
        book.bids.remove(&BEST_BID);
    });
    let updated = next_view(&mut sub, "view after a removal").await;

    assert_eq!(ticks(&updated.bids), ladder(BEST_BID - UNIT, -UNIT, 10));
}

#[tokio::test]
async fn levels_outside_the_view_stay_tracked() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    next_view(&mut sub, "first view").await;
    let deep = BEST_BID - 14 * UNIT;

    market.publish(|book| {
        book.bids.insert(deep, 33);
    });
    let quiet = next_view(&mut sub, "view after a deep change").await;
    assert!(!ticks(&quiet.bids).contains(&deep), "trimmed to the depth");
    assert_eq!(quiet.book_seq, "101");

    market.publish(|book| {
        for i in 0..5 {
            book.bids.remove(&(BEST_BID - i * UNIT));
        }
    });
    let promoted = next_view(&mut sub, "view after removals").await;
    let index = ticks(&promoted.bids)
        .iter()
        .position(|price| *price == deep)
        .expect("the deep level moved into the view");
    assert_eq!(qtys(&promoted.bids)[index], 33);
}

#[tokio::test]
async fn gap_in_a_delta_outside_the_view_refreshes_at_the_tier() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    next_view(&mut sub, "first view").await;
    let deep = BEST_BID - 14 * UNIT;

    // Sequences from the snapshot's 100, skipping ahead; only level 15 changes.
    market.update(|book| {
        book.bids.insert(deep, 44);
        book.seq = 200;
    });
    market.send(20, 150, 200, &Side::from([(deep, 44)]), &Side::new(), false);
    let refreshed = next_view(&mut sub, "view after the gap refresh").await;

    assert_eq!(market.snapshots(), [20, 20]);
    assert_eq!(refreshed.book_seq, "200");
    assert_eq!(refreshed.bids.len(), 10);
}

#[tokio::test]
async fn sequence_zero_snapshot_still_detects_a_gap_at_a_trimmed_depth() {
    let market = Market::spawn(600).await;
    market.update(|book| book.seq = 0);
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    let first = next_view(&mut sub, "first view").await;
    assert_eq!(first.book_seq, "0");
    assert_eq!(first.bids.len(), 10);

    market.update(|book| book.seq = 6);
    market.send(20, 5, 6, &Side::from([(BEST_BID, 8)]), &Side::new(), false);
    let refreshed = next_view(&mut sub, "view after the gap refresh").await;

    assert_eq!(market.snapshots(), [20, 20]);
    assert_eq!(refreshed.book_seq, "6");
    assert_eq!(
        qtys(&refreshed.bids)[0],
        5,
        "the gapped delta must not apply"
    );
}

#[tokio::test]
async fn sequence_zero_snapshot_applies_the_next_contiguous_delta() {
    let market = Market::spawn(600).await;
    market.update(|book| book.seq = 0);
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    next_view(&mut sub, "first view").await;

    market.send(20, 1, 1, &Side::from([(BEST_BID, 8)]), &Side::new(), false);
    let updated = next_view(&mut sub, "view after a delta").await;

    assert_eq!(updated.book_seq, "1");
    assert_eq!(qtys(&updated.bids)[0], 8);
    assert_eq!(market.snapshots(), [20]);
}

#[tokio::test]
async fn unavailable_book_stays_unknown_then_recovers_trimmed() {
    let market = Market::spawn(600).await;
    market.update(|book| book.available = false);
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    let unknown = next_view(&mut sub, "first view").await;
    assert!(!unknown.available);
    assert_eq!(unknown.depth, 10);
    assert!(unknown.bids.is_empty());

    // A reset delta carries no availability: the view stays unavailable.
    market.send(20, 0, 101, &Side::from([(BEST_BID, 5)]), &Side::new(), true);
    let still = next_view(&mut sub, "view after a reset").await;
    assert!(!still.available);

    market.update(|book| book.available = true);
    let recovered = next_view(&mut sub, "view after the book came back").await;
    assert!(recovered.available);
    assert_eq!(recovered.bids.len(), 10);
    assert_eq!(recovered.asks.len(), 10);
    assert_eq!(market.snapshots(), [20, 20]);
}

#[tokio::test]
async fn reset_delta_replaces_the_book_and_stays_trimmed() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(2), None).await;
    market.await_subscribed(20).await;
    next_view(&mut sub, "first view").await;
    let prices: Side = (0..20).map(|i| (BEST_BID - i * UNIT, 3)).collect();
    market.send(20, 0, 500, &prices, &Side::new(), true);
    let view = next_view(&mut sub, "view after a reset").await;
    assert_eq!(ticks(&view.bids), [BEST_BID, BEST_BID - UNIT]);
    assert!(view.asks.is_empty());
    assert_eq!(view.book_seq, "500");
}

#[tokio::test]
async fn coarse_bucket_aggregates_the_tier_then_trims_to_the_depth() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(2), Some("5")).await;
    market.await_subscribed(20).await;
    let view = next_view(&mut sub, "first view").await;
    // Bids floor to 2000 (one level) and 1995 (five levels); asks ceil to 2005 and 2010.
    assert_eq!(ticks(&view.bids), [2000 * UNIT, 1995 * UNIT]);
    assert_eq!(qtys(&view.bids), [5, 25]);
    assert_eq!(ticks(&view.asks), [2005 * UNIT, 2010 * UNIT]);
    assert_eq!(qtys(&view.asks), [25, 25]);

    market.publish(|book| {
        book.bids.insert(BEST_BID - UNIT, 15);
    });
    let updated = next_view(&mut sub, "view after a delta").await;
    assert_eq!(qtys(&updated.bids), [5, 35]);
}

#[tokio::test]
async fn coarse_bucket_above_the_price_shows_the_zero_bid_at_a_trimmed_depth() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), Some("100000")).await;
    let view = next_view(&mut sub, "first view").await;
    assert_eq!(ticks(&view.bids), [0]);
    assert_eq!(
        qtys(&view.bids),
        [5 * 20],
        "every tier level floors into it"
    );
    assert_eq!(ticks(&view.asks), [100000 * UNIT]);
}

// Refused before it reaches the book: rebuilt from a fresh tier snapshot, still open.
#[tokio::test]
async fn zero_price_outside_the_trimmed_view_resyncs_at_the_tier() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    let first = next_view(&mut sub, "first view").await;

    market.send(20, 100, 101, &Side::from([(0, 5)]), &Side::new(), false);
    let refreshed = next_view(&mut sub, "view after the refused delta").await;
    assert_eq!(refreshed.book_seq, first.book_seq);
    assert_eq!(ticks(&refreshed.bids), ticks(&first.bids));
    assert_eq!(market.snapshots(), [20, 20]);
    assert!(sub.err().is_none(), "{:?}", sub.err());
}

#[tokio::test]
async fn reconnect_stays_on_the_tier_and_refetches_at_it() {
    let market = Market::spawn(600).await;
    let client = market.client();
    let mut sub = book(&client, Some(10), None).await;
    market.await_subscribed(20).await;
    next_view(&mut sub, "first view").await;

    market.ws.drop_connection(&channel_for(20));
    market.await_subscribed(20).await;
    let refreshed = next_view(&mut sub, "view after the reconnect").await;

    assert_eq!(market.snapshots(), [20, 20]);
    assert_eq!(refreshed.bids.len(), 10);
}

#[tokio::test]
async fn raw_deltas_at_a_published_tier_are_delivered() {
    for tier in TIERS {
        let market = Market::spawn(600).await;
        let client = market.client();
        let mut sub = client
            .orderbook
            .subscribe_deltas(1, Some(tier))
            .await
            .unwrap();
        market.await_subscribed(tier).await;
        market.publish(|book| {
            book.bids.insert(BEST_BID, 7);
        });
        let delta = tokio::time::timeout(WAIT, sub.recv())
            .await
            .expect("no raw delta on a published tier")
            .unwrap();
        assert_eq!(market.delivered(tier), 1);
        assert_eq!((delta.book_seq_start, delta.book_seq_end), (100, 101));
        assert_eq!(delta.bids.len(), 1);
        assert_eq!(delta.bids[0].qty_scaled, 7);
    }
}

#[tokio::test]
async fn raw_deltas_at_an_unpublished_depth_are_refused() {
    for depth in [5u32, 10, 25, 100, 501, 1000, 0] {
        let market = Market::spawn(600).await;
        let client = market.client();
        let refused = client
            .orderbook
            .subscribe_deltas(1, Some(depth))
            .await
            .err()
            .unwrap_or_else(|| panic!("depth {depth} was accepted"));
        assert!(
            matches!(&refused, Error::Validation(_))
                && refused.to_string().contains("1, 20, 50, 200, 500"),
            "{refused:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(market.connections(), 0, "depth {depth} reached the server");
    }
}

#[tokio::test]
async fn raw_snapshot_serves_every_enum_depth() {
    for depth in SNAPSHOT_DEPTHS {
        let market = Market::spawn(1100).await;
        let client = market.client();
        let snapshot = client.orderbook.get("BTC-USDT", Some(depth)).await.unwrap();
        assert_eq!(market.snapshots(), [depth]);
        assert_eq!(snapshot.depth, depth);
        assert_eq!(snapshot.bids.len(), depth as usize);
        assert_eq!(snapshot.asks.len(), depth as usize);
        assert_eq!(market.connections(), 0);
    }
}

// POLY-8586: a snapshot reports the depth it requested on the wire. A depth
// between snapshot depths rides the smallest one covering it, `None` and 0 are
// the default 50, and a depth above 1000 is refused before any request.
#[tokio::test]
async fn snapshot_reports_the_depth_requested_on_the_wire() {
    for (depth, want) in [
        (Some(1), 1),
        (Some(2), 5),
        (Some(5), 5),
        (Some(10), 10),
        (Some(11), 20),
        (Some(15), 20),
        (Some(20), 20),
        (None, 50),
        (Some(0), 50),
        (Some(21), 50),
        (Some(101), 200),
        (Some(501), 1000),
        (Some(1000), 1000),
    ] {
        let market = Market::spawn(1100).await;
        let client = market.client();
        let snapshot = client.orderbook.get("BTC-USDT", depth).await.unwrap();
        assert_eq!(market.snapshots(), [want], "depth {depth:?}");
        assert_eq!(snapshot.depth, want, "depth {depth:?}");
        assert_eq!(snapshot.bids.len(), want as usize, "depth {depth:?}");
        // POLY-8588 parity: snapshot levels keep the market they were read for.
        let price = snapshot.bids[0].price.as_ref().unwrap();
        assert_eq!(price.symbol(), Some("BTC-USDT"));
        assert_eq!(
            snapshot.asks[0].qty.as_ref().unwrap().symbol(),
            Some("BTC-USDT")
        );
    }
    for depth in [1001, 5000, u32::MAX] {
        let market = Market::spawn(10).await;
        let client = market.client();
        let refused = client.orderbook.get("BTC-USDT", Some(depth)).await;
        assert!(
            matches!(refused, Err(Error::Validation(_))),
            "depth {depth}: {refused:?}"
        );
        assert!(
            market.snapshots().is_empty(),
            "depth {depth} reached the server"
        );
    }
}
