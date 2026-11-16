//! The fixtures are synthetic frames derived from captured server traffic by
//! the `derive_marketstream_fixtures` test: every identifier, price, quantity
//! and time is rewritten, and the frames are re-encoded with the generated
//! codecs. `testdata/marketstream/fixtures/index.tsv` lists them by group; a
//! row with no file is a message not yet captured.

use std::sync::Arc;

use super::derive::fixtures_dir;
use crate::marketstream::client::{handle, offline_stream, record_subscribe, recovery_requested};
use crate::marketstream::codec::{self, Msg};
use crate::marketstream::sbe::{ReadBuf, message_header_codec, ping_codec, subscribe_codec};
use crate::marketstream::{
    Config, Event, FeedState, GapCause, MarketStream, Pairs, Stream, Topic, TradesGap,
};

struct Fixture {
    file: String,
    direction: String,
    message: String,
    group: String,
    data: Vec<u8>,
}

fn load() -> (Vec<Fixture>, Vec<String>) {
    let dir = fixtures_dir();
    let index = std::fs::read_to_string(dir.join("index.tsv")).expect("fixtures index");
    let (mut frames, mut todo) = (Vec::new(), Vec::new());
    for line in index.lines() {
        let cols: Vec<&str> = line.split('\t').collect();
        if line.starts_with('#') || cols.len() != 5 || cols[0] == "file" {
            continue;
        }
        if cols[0] == "-" {
            todo.push(format!("{}: {}", cols[3], cols[4]));
            continue;
        }
        frames.push(Fixture {
            file: cols[0].to_owned(),
            direction: cols[1].to_owned(),
            message: cols[2].to_owned(),
            group: cols[3].to_owned(),
            data: std::fs::read(dir.join(cols[0])).expect("fixture"),
        });
    }
    assert!(!frames.is_empty(), "no fixtures");
    (frames, todo)
}

fn group<'a>(frames: &'a [Fixture], name: &str) -> Vec<&'a Fixture> {
    let out: Vec<&Fixture> = frames.iter().filter(|f| f.group == name).collect();
    assert!(!out.is_empty(), "no fixtures of group {name}");
    out
}

fn decode_subscribe(data: &[u8]) -> (u64, Vec<Topic>) {
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(data), 0);
    let d = subscribe_codec::SubscribeDecoder::default().header(h, 0);
    let id = d.request_id();
    let mut items = d.items_decoder();
    let mut topics = Vec::new();
    while items.advance().unwrap().is_some() {
        topics.push(Topic::new(
            items.symbol_id(),
            Stream::from_wire(items.stream()).unwrap(),
        ));
    }
    (id, topics)
}

fn ping_id(data: &[u8]) -> u64 {
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(data), 0);
    ping_codec::PingDecoder::default().header(h, 0).request_id()
}

fn kind(msg: &Msg) -> &'static str {
    match msg {
        Msg::Trades { .. } => "Trades",
        Msg::TradesSkipped { .. } => "TradesSkipped",
        Msg::TradesThrough { .. } => "TradesThrough",
        Msg::Bbo { .. } => "BestBidOffer",
        Msg::DepthSnapshot { .. } => "DepthSnapshot",
        Msg::DepthUpdate { .. } => "DepthUpdate",
        Msg::MarketStatus { .. } => "MarketStatus",
        Msg::SubscribeResponse { .. } => "SubscribeResponse",
        Msg::Pong { .. } => "Pong",
    }
}

fn decode(f: &Fixture) -> Msg {
    codec::decode(&f.data)
        .unwrap_or_else(|e| panic!("{}: {e}", f.file))
        .expect("a known template")
}

/// Every server fixture decodes to its message; every client fixture is what
/// this client encodes for the same request; no pair ID is a captured one.
#[test]
fn fixtures_decode_and_encode() {
    let (frames, todo) = load();
    for item in &todo {
        eprintln!("TODO fixture {item}");
    }
    let mut seen = std::collections::HashSet::new();
    for f in &frames {
        seen.insert(format!("{} {}", f.direction, f.message));
        if f.direction == "client" {
            let again = match f.message.as_str() {
                "Subscribe" => {
                    let (id, topics) = decode_subscribe(&f.data);
                    assert!(
                        topics.iter().all(|t| t.symbol_id >= 101),
                        "{}: a pair ID is not synthetic",
                        f.file
                    );
                    codec::encode_subscribe(id, &topics)
                }
                "Ping" => codec::encode_ping(ping_id(&f.data)),
                other => panic!("{}: client message {other}", f.file),
            };
            assert_eq!(again, f.data, "{}", f.file);
            continue;
        }
        assert_eq!(kind(&decode(f)), f.message, "{}", f.file);
    }
    for m in [
        "client Subscribe",
        "client Ping",
        "server SubscribeResponse",
        "server Pong",
        "server Trades",
        "server TradesThrough",
        "server BestBidOffer",
        "server DepthSnapshot",
        "server DepthUpdate",
        "server MarketStatus",
    ] {
        assert!(seen.contains(m), "no {m} fixture");
    }
}

struct AnyPairs;

impl Pairs for AnyPairs {
    fn base_quantity_scale(&self, _: u32) -> Option<u32> {
        Some(8)
    }
    fn symbol(&self, _: u32) -> Option<String> {
        None
    }
}

/// A stream with the fixture Subscribe sent and its response applied, as a
/// connection that subscribed.
fn subscribed(frames: &[Fixture]) -> MarketStream {
    let mut config = Config::new("ws://127.0.0.1/ws/market.sbe", Arc::new(AnyPairs));
    config.queue_size = 1 << 12;
    let stream = offline_stream(config);
    let (id, topics) = decode_subscribe(&group(frames, "client-subscribe")[0].data);
    record_subscribe(stream.shared(), id, topics);
    replay(&stream, &group(frames, "subscribe-response"));
    stream
}

fn replay(stream: &MarketStream, frames: &[&Fixture]) {
    for f in frames {
        handle(stream.shared(), &f.data).unwrap_or_else(|e| panic!("{}: {e}", f.file));
    }
}

#[tokio::test]
async fn fixture_depth_chain_follows_exact_base() {
    let (frames, _) = load();
    let mut stream = subscribed(&frames);
    let chain = group(&frames, "depth-chain");
    replay(&stream, &chain);
    let Msg::DepthUpdate {
        symbol_id, last, ..
    } = decode(chain[chain.len() - 1])
    else {
        panic!("not an update")
    };
    let book = stream
        .order_book(symbol_id, Stream::Depth, 0)
        .expect("a book");
    assert!(
        !book.stale && book.update_id == last && stream.live(Topic::new(symbol_id, Stream::Depth)),
        "{book:?}"
    );
    let updates = stream
        .queued()
        .into_iter()
        .filter(|e| matches!(e, Event::DepthUpdate(_)))
        .count();
    assert_eq!(updates, chain.len() - 1);
}

#[tokio::test]
async fn fixture_bridging_range_advances_the_position() {
    let (frames, _) = load();
    let stream = subscribed(&frames);
    let bridge = group(&frames, "depth-bridge");
    replay(&stream, &bridge);
    let Msg::DepthUpdate {
        symbol_id,
        first,
        last,
        bids,
        asks,
        ..
    } = decode(bridge[1])
    else {
        panic!("not an update")
    };
    assert!(
        last > first && bids.is_empty() && asks.is_empty(),
        "not a bridging range"
    );
    let book = stream
        .order_book(symbol_id, Stream::Depth, 0)
        .expect("a book");
    assert!(book.update_id == last && !book.stale, "{book:?}");
}

#[tokio::test]
async fn fixture_depth20_and_quote() {
    let (frames, _) = load();
    let stream = subscribed(&frames);
    let snap = group(&frames, "depth20-snapshot");
    let quote = group(&frames, "best-bid-offer");
    replay(&stream, &snap);
    replay(&stream, &quote);
    let Msg::DepthSnapshot {
        symbol_id,
        bids,
        asks,
        ..
    } = decode(snap[0])
    else {
        panic!("not a snapshot")
    };
    let book = stream
        .order_book(symbol_id, Stream::Depth20, 0)
        .expect("a depth20 book");
    assert!(
        book.bids.len() == bids.len()
            && book.asks.len() == asks.len()
            && !bids.is_empty()
            && !book.stale,
        "{book:?}"
    );
    let Msg::Bbo { symbol_id, .. } = decode(quote[0]) else {
        panic!("not a quote")
    };
    assert!(stream.best_bid_offer(symbol_id).is_some_and(|q| !q.stale));
}

#[tokio::test]
async fn fixture_trades_after_their_baseline() {
    let (frames, _) = load();
    let mut stream = subscribed(&frames);
    replay(&stream, &group(&frames, "trades"));
    let events = stream.queued();
    assert!(
        !events.iter().any(|e| matches!(e, Event::TradesGap(_))),
        "a gap between the baseline and its trades"
    );
    let trades: Vec<_> = events
        .into_iter()
        .filter_map(|e| {
            if let Event::Trades(t) = e {
                Some(t)
            } else {
                None
            }
        })
        .collect();
    let t = trades.last().expect("trades after the baseline");
    assert!(!t.trades.is_empty());
    assert!(
        t.trades
            .iter()
            .all(|x| x.price.as_ticks() > 0 && x.qty.as_scaled() > 0)
    );
}

/// A stale pair recovers in the server's order: its replacement snapshot,
/// then LIVE. LIVE before the snapshot is a gap of the depth stream: it alone
/// recovers.
#[tokio::test]
async fn fixture_recovery_order() {
    let (frames, _) = load();
    let snapshot = group(&frames, "depth-chain")[0];
    let stale = group(&frames, "market-status-stale")[0];
    let live = group(&frames, "market-status-live")[0];
    let stream = subscribed(&frames);
    replay(&stream, &[snapshot, stale, snapshot, live]);
    let Msg::MarketStatus { symbol_id, .. } = decode(live) else {
        panic!("not a status")
    };
    assert_eq!(stream.feed(symbol_id), FeedState::Live);
    assert!(stream.live(Topic::new(symbol_id, Stream::Depth)));
    let other = subscribed(&frames);
    replay(&other, &[snapshot, stale, live]);
    let topic = Topic::new(symbol_id, Stream::Depth);
    assert!(
        recovery_requested(other.shared(), topic) && !other.live(topic),
        "LIVE before the replacement snapshot did not start the depth stream's recovery"
    );
}

#[tokio::test]
async fn fixture_pong_answers_its_ping() {
    let (frames, _) = load();
    let mut stream = subscribed(&frames);
    let id = ping_id(&group(&frames, "client-ping")[0].data);
    stream.owe_ping(id);
    replay(&stream, &group(&frames, "pong"));
    assert!(
        stream
            .queued()
            .iter()
            .any(|e| matches!(e, Event::Pong(p) if p.request_id == id))
    );
}

/// A depth snapshot holding levels, then its contiguous updates: the window
/// is the snapshot's, each update applies at its base, and the last one, a
/// range without levels, advances the position to its end.
#[tokio::test]
async fn fixture_depth_with_levels() {
    let (frames, _) = load();
    let stream = subscribed(&frames);
    let chain = group(&frames, "depth-levels-chain");
    let Msg::DepthSnapshot {
        symbol_id,
        levels_per_side,
        bids,
        asks,
        ..
    } = decode(chain[0])
    else {
        panic!("not a snapshot")
    };
    assert!(
        !bids.is_empty() && !asks.is_empty(),
        "the snapshot holds no levels"
    );
    replay(&stream, &chain[..1]);
    let book = stream
        .order_book(symbol_id, Stream::Depth, 0)
        .expect("a book");
    assert!(
        book.levels_per_side == levels_per_side
            && book.bids.len() == bids.len()
            && book.asks.len() == asks.len()
            && !book.stale,
        "{book:?}"
    );
    replay(&stream, &chain[1..]);
    let Msg::DepthUpdate {
        first,
        last,
        bids,
        asks,
        ..
    } = decode(chain[chain.len() - 1])
    else {
        panic!("not an update")
    };
    assert!(
        last > first && bids.is_empty() && asks.is_empty(),
        "the chain's last update is not a bridging range"
    );
    let book = stream
        .order_book(symbol_id, Stream::Depth, 0)
        .expect("a book");
    let window = book.levels_per_side as usize;
    assert!(
        book.update_id == last
            && !book.stale
            && book.bids.len() <= window
            && book.asks.len() <= window,
        "{book:?}"
    );
    assert!(
        book.bids
            .windows(2)
            .all(|w| w[1].price.as_ticks() < w[0].price.as_ticks()),
        "bids out of order"
    );
    assert!(
        book.asks
            .windows(2)
            .all(|w| w[1].price.as_ticks() > w[0].price.as_ticks()),
        "asks out of order"
    );
}

/// An update missing from the chain is a gap: the book is stale and not live,
/// the depth stream alone recovers by an Unsubscribe, and further updates are
/// dropped meanwhile.
#[tokio::test]
async fn fixture_depth_gap_marks_the_book_stale() {
    let (frames, _) = load();
    let stream = subscribed(&frames);
    let chain = group(&frames, "depth-levels-chain");
    replay(&stream, &chain[..2]);
    handle(stream.shared(), &chain[3].data).expect("a gap ended the connection");
    let Msg::DepthSnapshot { symbol_id, .. } = decode(chain[0]) else {
        panic!("not a snapshot")
    };
    let topic = Topic::new(symbol_id, Stream::Depth);
    assert!(
        stream
            .order_book(symbol_id, Stream::Depth, 0)
            .expect("a book")
            .stale
    );
    assert!(!stream.live(topic));
    assert!(
        recovery_requested(stream.shared(), topic),
        "no Unsubscribe of the depth stream after its gap"
    );
    handle(stream.shared(), &chain[4].data).expect("an update during the recovery");
    assert!(
        stream
            .order_book(symbol_id, Stream::Depth, 0)
            .expect("a book")
            .stale,
        "an update applied during the recovery"
    );
}

/// TradesSkipped after the pair's baseline names exactly the trades the
/// subscription will not receive live.
#[tokio::test]
async fn fixture_trades_skipped_after_the_baseline() {
    let (frames, _) = load();
    let mut stream = subscribed(&frames);
    let skipped = group(&frames, "trades-skipped");
    replay(&stream, &skipped);
    let Msg::TradesSkipped {
        symbol_id,
        first,
        last,
        ..
    } = decode(skipped[1])
    else {
        panic!("not a TradesSkipped")
    };
    let gaps: Vec<TradesGap> = stream
        .queued()
        .into_iter()
        .filter_map(|e| {
            if let Event::TradesGap(g) = e {
                Some(g)
            } else {
                None
            }
        })
        .collect();
    let want = TradesGap {
        symbol_id,
        after_trade_id: first - 1,
        through_trade_id: last.expect("a bounded range"),
        tail_epoch: 0,
        cause: GapCause::Skipped,
    };
    assert_eq!(gaps, vec![want]);
}
