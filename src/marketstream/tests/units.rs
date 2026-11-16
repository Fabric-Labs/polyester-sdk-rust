//! Decoding, trade accounting, the depth book and trade backfill.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use super::frames::*;
use crate::errors::Error;
use crate::marketstream::backfill::{Pacing, backfill_paced};
use crate::marketstream::book::{Book, Refusal};
use crate::marketstream::codec::{self, Msg, WireLevel};
use crate::marketstream::sbe::{
    self, ReadBuf, message_header_codec, subscribe_result::SubscribeResult as Wire,
};
use crate::marketstream::trades::{Tracker, epoch_base, epoch_of, in_epoch};
use crate::marketstream::{
    EpochEnd, FeedState, GapCause, HistoryPage, Side, Stream, StreamError, Topic, Trade,
    TradeHistory, TradesGap, url,
};
use crate::types::{Price, Quantity, QuantityDomain};

fn decode(frame: &[u8]) -> Msg {
    codec::decode(frame)
        .expect("decodes")
        .expect("known template")
}

fn frame_error(frame: &[u8]) -> bool {
    matches!(
        codec::decode(frame),
        Err(Error::MarketStream(StreamError::Frame { .. }))
    )
}

#[test]
fn decodes_every_server_template() {
    match decode(&trades(7, 1, 42, &[fill(1), fill(2)])) {
        Msg::Trades {
            symbol_id: 7,
            epoch: 1,
            update_id: 42,
            fills,
            ..
        } => assert_eq!(fills, vec![fill(1), fill(2)]),
        m => panic!("{m:?}"),
    }
    assert!(
        matches!(decode(&trades_through(7, 2, 1 << 40)), Msg::TradesThrough { symbol_id: 7, epoch: 2, last, .. } if last == 1 << 40)
    );
    assert!(matches!(
        decode(&trades_skipped(7, 1, 5, 0)),
        Msg::TradesSkipped {
            first: 5,
            last: None,
            ..
        }
    ));
    assert!(matches!(
        decode(&trades_skipped(7, 1, 5, 9)),
        Msg::TradesSkipped {
            first: 5,
            last: Some(9),
            ..
        }
    ));
    assert!(matches!(
        decode(&bbo(7, 1, 9, (100, 2), (NO_PRICE, 0))),
        Msg::Bbo {
            bid_price: Some(100),
            bid_qty: 2,
            ask_price: None,
            ask_qty: 0,
            ..
        }
    ));
    match decode(&depth_snapshot(
        7,
        Stream::Depth20,
        1,
        9,
        &[lv(100, 1)],
        &[lv(101, 2), lv(102, 3)],
    )) {
        Msg::DepthSnapshot {
            stream: Some(Stream::Depth20),
            levels_per_side: 20,
            bids,
            asks,
            update_id: 9,
            ..
        } => {
            assert_eq!(bids, vec![lv(100, 1)]);
            assert_eq!(asks, vec![lv(101, 2), lv(102, 3)]);
        }
        m => panic!("{m:?}"),
    }
    match decode(&depth_update(7, 1, 10, 12, &[lv(100, 0)], &[])) {
        Msg::DepthUpdate {
            first: 10,
            last: 12,
            bids,
            asks,
            ..
        } => {
            assert_eq!(bids, vec![lv(100, 0)]);
            assert!(asks.is_empty());
        }
        m => panic!("{m:?}"),
    }
    assert!(matches!(
        decode(&market_status(7, FeedState::Stale)),
        Msg::MarketStatus {
            feed: sbe::feed_state::FeedState::STALE,
            ..
        }
    ));
    match decode(&subscribe_response(
        5,
        &[
            (7, Stream::Trades, Wire::OK),
            (8, Stream::Depth20, Wire::NOT_SUBSCRIBED),
        ],
    )) {
        Msg::SubscribeResponse {
            request_id: 5,
            results,
        } => {
            assert_eq!(
                results,
                vec![
                    (7, Some(Stream::Trades), Wire::OK),
                    (8, Some(Stream::Depth20), Wire::NOT_SUBSCRIBED)
                ]
            );
        }
        m => panic!("{m:?}"),
    }
    assert!(matches!(
        decode(&pong(11)),
        Msg::Pong {
            request_id: 11,
            send_ns: 4
        }
    ));
}

#[test]
fn unknown_templates_are_skipped() {
    let mut unknown = pong(1);
    unknown.extend([1, 2, 3]);
    let unknown = with_header(
        &unknown,
        19,
        99,
        codec::SCHEMA_ID,
        codec::SCHEMA_VERSION + 1,
    );
    assert_eq!(codec::decode(&unknown).unwrap(), None);
    assert_eq!(codec::decode(&codec::encode_ping(3)).unwrap(), None);
}

#[test]
fn another_schema_is_refused() {
    let frame = with_header(&pong(1), 16, sbe::pong_codec::SBE_TEMPLATE_ID, 7, 0);
    assert!(matches!(
        codec::decode(&frame),
        Err(Error::MarketStream(StreamError::Schema {
            schema_id: 7,
            version: 0
        }))
    ));
}

#[test]
fn a_newer_version_is_read_by_its_block_lengths() {
    let cases: Vec<(Vec<u8>, usize)> = vec![
        (trades(7, 1, 42, &[fill(1), fill(2)]), 1),
        (bbo(7, 1, 9, (100, 2), (101, 3)), 0),
        (
            depth_snapshot(
                7,
                Stream::Depth,
                1,
                9,
                &[lv(100, 1), lv(99, 4)],
                &[lv(101, 2)],
            ),
            2,
        ),
        (depth_update(7, 1, 10, 12, &[lv(100, 0)], &[lv(101, 5)]), 2),
        (subscribe_response(5, &[(7, Stream::Trades, Wire::OK)]), 1),
        (trades_through(7, 2, 1 << 40), 0),
    ];
    for (frame, groups) in cases {
        let want = decode(&frame);
        assert_eq!(decode(&newer_version(&frame, 13, 5, groups)), want);
    }
}

#[test]
fn layouts_this_version_cannot_read_are_refused() {
    let t = trades(7, 1, 42, &[fill(1), fill(2)]);
    let block = sbe::trades_codec::SBE_BLOCK_LENGTH;
    let mut short_entry = t.clone();
    short_entry[message_header_codec::ENCODED_LENGTH + block as usize] = 24;
    let mut trailing = t.clone();
    trailing.push(0);
    for frame in [
        with_header(
            &t,
            block - 1,
            sbe::trades_codec::SBE_TEMPLATE_ID,
            codec::SCHEMA_ID,
            codec::SCHEMA_VERSION + 1,
        ),
        short_entry,
        trailing,
        t[..8].to_vec(),
        t[..18].to_vec(),
    ] {
        assert!(frame_error(&frame), "{frame:x?} accepted");
    }
}

#[test]
fn every_truncation_is_refused_without_panic() {
    for frame in [
        trades(7, 1, 42, &[fill(1), fill(2)]),
        depth_snapshot(7, Stream::Depth, 1, 9, &[lv(100, 1)], &[lv(101, 2)]),
        depth_update(7, 1, 10, 12, &[lv(100, 0)], &[lv(101, 5)]),
        subscribe_response(5, &[(7, Stream::Trades, Wire::OK)]),
        bbo(7, 1, 9, (100, 2), (101, 3)),
    ] {
        for n in 0..frame.len() {
            assert!(
                codec::decode(&frame[..n]).is_err(),
                "a {n}-byte prefix decoded"
            );
        }
    }
}

#[test]
fn arbitrary_bytes_never_panic() {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let seeds = [
        trades(7, 1, 42, &[fill(1)]),
        depth_snapshot(7, Stream::Depth20, 1, 9, &[lv(100, 1)], &[]),
    ];
    for _ in 0..20_000 {
        let mut frame = seeds[(next() % 2) as usize].clone();
        for _ in 0..(next() % 4 + 1) {
            let at = (next() as usize) % frame.len();
            frame[at] = next() as u8;
        }
        frame.truncate((next() as usize) % (frame.len() + 1));
        let _ = codec::decode(&frame);
    }
}

fn header_of(frame: &[u8]) -> (u16, u16, u16, u16) {
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0);
    (
        h.block_length(),
        h.template_id(),
        h.schema_id(),
        h.version(),
    )
}

#[test]
fn client_messages_encode_the_schema() {
    use sbe::subscribe_codec::SubscribeDecoder;
    use sbe::unsubscribe_codec::UnsubscribeDecoder;
    let topics = [Topic::new(7, Stream::Trades), Topic::new(8, Stream::Depth)];
    let frame = codec::encode_subscribe(99, &topics);
    assert_eq!(
        header_of(&frame),
        (
            8,
            sbe::subscribe_codec::SBE_TEMPLATE_ID,
            20,
            codec::SCHEMA_VERSION
        )
    );
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(&frame), 0);
    let d = SubscribeDecoder::default().header(h, 0);
    assert_eq!(d.request_id(), 99);
    let mut items = d.items_decoder();
    let mut got = Vec::new();
    while items.advance().unwrap().is_some() {
        got.push(Topic::new(
            items.symbol_id(),
            Stream::from_wire(items.stream()).unwrap(),
        ));
    }
    assert_eq!(got, topics);
    assert_eq!(frame.len(), 8 + 8 + 4 + 2 * 5);

    let frame = codec::encode_unsubscribe(98, &topics[..1]);
    assert_eq!(header_of(&frame).1, sbe::unsubscribe_codec::SBE_TEMPLATE_ID);
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(&frame), 0);
    let d = UnsubscribeDecoder::default().header(h, 0);
    assert_eq!(d.request_id(), 98);
    let mut items = d.items_decoder();
    assert!(items.advance().unwrap().is_some());
    assert_eq!(
        (items.symbol_id(), Stream::from_wire(items.stream())),
        (7, Some(Stream::Trades))
    );
    assert_eq!(frame.len(), 8 + 8 + 4 + 5);

    let ping = codec::encode_ping(5);
    assert_eq!(
        header_of(&ping),
        (
            8,
            sbe::ping_codec::SBE_TEMPLATE_ID,
            20,
            codec::SCHEMA_VERSION
        )
    );
    assert_eq!(&ping[8..], &5u64.to_le_bytes());
}

#[test]
fn stream_urls() {
    for (base, want) in [
        (
            "wss://api.testnet.polyester.com",
            "wss://api.testnet.polyester.com/ws/market.sbe",
        ),
        (
            "wss://api.testnet.polyester.com/",
            "wss://api.testnet.polyester.com/ws/market.sbe",
        ),
        (
            "wss://api.testnet.polyester.com/connection/websocket",
            "wss://api.testnet.polyester.com/ws/market.sbe",
        ),
        (
            "wss://api.testnet.polyester.com/ws/market.sbe",
            "wss://api.testnet.polyester.com/ws/market.sbe",
        ),
        (
            "ws://127.0.0.1:8080/edge",
            "ws://127.0.0.1:8080/edge/ws/market.sbe",
        ),
    ] {
        assert_eq!(url(base).unwrap(), want, "{base}");
    }
    for bad in [
        "https://api.polyester.com",
        "wss://h/?x=1",
        "wss://h/#f",
        "wss://",
        ":",
    ] {
        assert!(url(bad).is_err(), "{bad} accepted");
    }
}

// Trade accounting.

const E2: u64 = 1 << 40;
const E3: u64 = 2 << 40;

fn gap(after: u64, through: u64, tail: u64, cause: GapCause) -> TradesGap {
    TradesGap {
        symbol_id: 7,
        after_trade_id: after,
        through_trade_id: through,
        tail_epoch: tail,
        cause,
    }
}

fn ids(fills: &[codec::Fill]) -> Vec<u64> {
    fills.iter().map(|f| f.trade_id).collect()
}

fn fills(ids: &[u64]) -> Vec<codec::Fill> {
    ids.iter().map(|id| fill(*id)).collect()
}

#[test]
fn trades_follow_their_baseline_and_report_gaps() {
    use GapCause::*;
    let mut t = Tracker::new(7);
    assert!(
        t.trades(1, &fills(&[1])).is_err(),
        "trades before the baseline"
    );
    assert!(
        t.skipped(1, 1, Some(2)).is_err(),
        "skipped before the baseline"
    );
    assert_eq!(t.through(1, 100).unwrap(), vec![]);
    let (fresh, gaps) = t.trades(1, &fills(&[101, 102])).unwrap();
    assert_eq!((ids(&fresh), gaps), (vec![101, 102], vec![]));
    assert_eq!(
        t.skipped(1, 103, Some(107)).unwrap(),
        vec![gap(102, 107, 0, Skipped)]
    );
    assert_eq!(
        t.skipped(1, 101, Some(105)).unwrap(),
        vec![],
        "a duplicate range"
    );
    let (fresh, _) = t.trades(1, &fills(&[107, 108])).unwrap();
    assert_eq!(
        ids(&fresh),
        vec![108],
        "a duplicate trade is not delivered again"
    );
    let (fresh, gaps) = t.trades(1, &fills(&[111, 113])).unwrap();
    assert_eq!(ids(&fresh), vec![111, 113]);
    assert_eq!(
        gaps,
        vec![gap(108, 110, 0, Detected), gap(111, 112, 0, Detected)]
    );
    assert_eq!(t.through(1, 115).unwrap(), vec![gap(113, 115, 0, Detected)]);
    assert_eq!(
        t.skipped(1, 118, Some(119)).unwrap(),
        vec![gap(115, 117, 0, Detected), gap(117, 119, 0, Skipped)]
    );
}

#[test]
fn trades_across_epochs() {
    use GapCause::*;
    let mut t = Tracker::new(7);
    t.through(1, 100).unwrap();
    assert_eq!(
        t.skipped(1, 101, None).unwrap(),
        vec![gap(100, 0, 1, Skipped)]
    );
    assert!(
        t.trades(1, &fills(&[101])).unwrap().0.is_empty(),
        "covered by the tail's recovery"
    );
    let (fresh, gaps) = t.trades(2, &fills(&[E2 + 1])).unwrap();
    assert_eq!((ids(&fresh), gaps), (vec![E2 + 1], vec![]));
    // No tail for epoch 2: everything through epoch 3's base is unaccounted.
    let (_, gaps) = t.trades(3, &fills(&[E3 + 1])).unwrap();
    assert_eq!(gaps, vec![gap(E2 + 1, E3, 0, Detected)]);
    // A reported tail, then an epoch skipped entirely.
    t.skipped(3, E3 + 2, None).unwrap();
    let e5 = 4u64 << 40;
    let e4 = 3u64 << 40;
    assert_eq!(
        t.through(5, e5 + 5).unwrap(),
        vec![gap(e4, e5, 0, Detected), gap(e5, e5 + 5, 0, Detected)]
    );
    // Older epochs are stale.
    assert_eq!(t.trades(4, &fills(&[e4 + 1])).unwrap().0, vec![]);
}

#[test]
fn trades_reconcile_a_new_baseline() {
    let mut t = Tracker::new(7);
    t.through(1, 100).unwrap();
    t.trades(1, &fills(&[101])).unwrap();
    t.restart();
    assert_eq!(
        t.through(2, E2 + 3).unwrap(),
        vec![gap(101, E2 + 3, 0, GapCause::Reconnect)]
    );
    t.restart();
    assert_eq!(
        t.through(2, E2 + 2).unwrap(),
        vec![],
        "a replica behind the last connection"
    );
    let (fresh, _) = t.trades(2, &fills(&[E2 + 3, E2 + 4])).unwrap();
    assert_eq!(ids(&fresh), vec![E2 + 4]);
}

#[test]
fn trades_out_of_their_epoch_are_refused() {
    let mut t = Tracker::new(7);
    assert!(t.through(2, 5).is_err());
    t.through(1, 100).unwrap();
    for f in [fills(&[E2 + 1]), fills(&[102, 101]), vec![]] {
        assert!(t.trades(1, &f).is_err(), "{f:?}");
    }
    assert!(t.trades(0, &fills(&[5])).is_err());
    assert!(t.skipped(1, 105, Some(103)).is_err());
}

#[test]
fn epoch_arithmetic() {
    assert_eq!(epoch_base(1), Some(0));
    assert_eq!(epoch_base(3), Some(E3));
    assert_eq!(epoch_base(0), None);
    assert_eq!(epoch_base((1 << 24) + 1), None);
    assert_eq!(
        (
            epoch_of(E2),
            epoch_of(E2 - 1),
            epoch_of(E2 + 1),
            epoch_of(0)
        ),
        (2, 1, 2, 1)
    );
    assert!(!in_epoch(E2, 2) && in_epoch(E2 + 1, 2) && !in_epoch(E3, 2) && in_epoch(E3 - 1, 2));
}

// The depth book.

fn book_at(
    stream: Stream,
    update_id: u64,
    window: u16,
    bids: &[WireLevel],
    asks: &[WireLevel],
) -> Book {
    let mut b = Book::new(stream);
    assert!(b.snapshot(1, update_id, window, bids, asks).unwrap());
    b
}

#[test]
fn depth_snapshot_then_exact_base_updates() {
    let mut b = book_at(
        Stream::Depth,
        10,
        1000,
        &[lv(100, 5), lv(99, 3)],
        &[lv(101, 2), lv(103, 4)],
    );
    b.update(
        1,
        11,
        14,
        &[lv(100, 6), lv(98, 1), lv(99, 0)],
        &[lv(102, 7), lv(101, 0)],
    )
    .unwrap();
    assert_eq!(
        (b.update_id, b.bids.clone(), b.asks.clone()),
        (
            14,
            vec![lv(100, 6), lv(98, 1)],
            vec![lv(102, 7), lv(103, 4)]
        )
    );
    b.update(1, 15, 20, &[], &[]).unwrap();
    assert_eq!(b.update_id, 20);
    // A replacement snapshot replaces the book, whatever it held, an older
    // one included.
    assert!(
        b.snapshot(1, 15, 1000, &[lv(97, 1)], &[]).unwrap(),
        "a depth snapshot always replaces"
    );
    assert_eq!(
        (b.update_id, b.bids.clone(), b.asks.len()),
        (15, vec![lv(97, 1)], 0)
    );
}

#[test]
fn depth_updates_the_book_cannot_apply() {
    let mut fresh = Book::new(Stream::Depth);
    assert!(matches!(
        fresh.update(1, 11, 11, &[], &[]),
        Err(Refusal::Gap(_))
    ));
    let mut b = book_at(Stream::Depth, 10, 1000, &[lv(100, 5)], &[lv(101, 2)]);
    for (epoch, first, last) in [(1, 12, 13), (1, 10, 13), (1, 9, 10), (2, 11, 11)] {
        assert!(
            matches!(b.update(epoch, first, last, &[], &[]), Err(Refusal::Gap(_))),
            "{first}..{last}"
        );
    }
    assert_eq!(b.update_id, 10);
    assert!(matches!(
        b.update(1, 11, 10, &[], &[]),
        Err(Refusal::Invalid(_))
    ));
    assert!(matches!(
        b.update(1, 11, 11, &[lv(100, -1)], &[]),
        Err(Refusal::Invalid(_))
    ));
    assert!(matches!(
        b.update(1, 11, 11, &[], &[lv(0, 1)]),
        Err(Refusal::Invalid(_))
    ));
}

#[test]
fn depth_window_truncates_each_side() {
    let mut b = book_at(
        Stream::Depth,
        1,
        3,
        &[lv(100, 1), lv(99, 1), lv(98, 1)],
        &[lv(101, 1), lv(102, 1), lv(103, 1)],
    );
    b.update(1, 2, 2, &[lv(105, 1)], &[lv(100, 9)]).unwrap();
    assert_eq!(b.bids, vec![lv(105, 1), lv(100, 1), lv(99, 1)]);
    assert_eq!(b.asks, vec![lv(100, 9), lv(101, 1), lv(102, 1)]);
}

#[test]
fn depth20_keeps_the_newest_snapshot() {
    let mut b = Book::new(Stream::Depth20);
    assert!(b.snapshot(2, 50, 20, &[lv(100, 1)], &[]).unwrap());
    for (epoch, update) in [(2, 50), (2, 49), (1, 90)] {
        assert!(!b.snapshot(epoch, update, 20, &[lv(3, 1)], &[]).unwrap());
    }
    assert!(b.snapshot(3, 1, 20, &[lv(4, 1)], &[]).unwrap());
    assert_eq!(b.bids, vec![lv(4, 1)]);
}

#[test]
fn snapshot_levels_must_be_ordered_positive_and_within_their_window() {
    let cases: [(&[WireLevel], &[WireLevel], u16); 7] = [
        (&[lv(99, 1), lv(100, 1)], &[], 1000),
        (&[], &[lv(101, 1), lv(100, 1)], 1000),
        (&[lv(99, 1), lv(99, 2)], &[], 1000),
        (&[lv(99, 0)], &[], 1000),
        (&[], &[lv(0, 1)], 1000),
        (&[], &[], 0),
        (&[lv(100, 1), lv(99, 1)], &[], 1),
    ];
    for (bids, asks, window) in cases {
        assert!(
            Book::new(Stream::Depth)
                .snapshot(1, 1, window, bids, asks)
                .is_err(),
            "{bids:?} {asks:?} {window}"
        );
    }
}

// Backfill.

/// Rewrites a page to break the walk's contract.
type Broken = Box<dyn Fn(&mut HistoryPage) + Send + Sync>;

pub(super) struct FakeHistory {
    trades: Vec<u64>,
    ends: HashMap<u64, u64>,
    w: Box<dyn Fn(usize) -> u64 + Send + Sync>,
    limit: Option<u32>,
    afters: Mutex<Vec<u64>>,
    broken: Option<Broken>,
}

impl FakeHistory {
    pub(super) fn new(
        trades: Vec<u64>,
        ends: &[(u64, u64)],
        w: impl Fn(usize) -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            trades,
            ends: ends.iter().copied().collect(),
            w: Box::new(w),
            limit: None,
            afters: Mutex::new(Vec::new()),
            broken: None,
        }
    }
    fn calls(&self) -> usize {
        self.afters.lock().unwrap().len()
    }
}

fn history_trade(id: u64) -> Trade {
    Trade {
        trade_id: id,
        price: Price::from_ticks(1, None).unwrap(),
        qty: Quantity::from_scaled(1, Some(3), QuantityDomain::OrderBase, None, None).unwrap(),
        taker_side: Side::Buy,
    }
}

impl TradeHistory for FakeHistory {
    fn trades_after(
        &self,
        symbol_id: u32,
        after: u64,
        limit: u32,
    ) -> crate::marketstream::backfill::HistoryFuture<'_> {
        assert_eq!(symbol_id, 7);
        let limit = self.limit.unwrap_or(limit) as usize;
        let call = {
            let mut afters = self.afters.lock().unwrap();
            afters.push(after);
            afters.len() - 1
        };
        let w = (self.w)(call);
        let mut page = HistoryPage {
            complete_through_trade_id: w,
            ..Default::default()
        };
        page.trades = self
            .trades
            .iter()
            .copied()
            .filter(|id| *id > after && *id <= w)
            .take(limit)
            .map(history_trade)
            .collect();
        page.next_after_trade_id = if page.trades.len() == limit {
            page.trades.last().unwrap().trade_id
        } else {
            after.max(w)
        };
        let mut ends: Vec<_> = self
            .ends
            .iter()
            .filter(|(_, last)| **last >= after && **last <= page.next_after_trade_id)
            .collect();
        ends.sort();
        page.epoch_ends = ends
            .into_iter()
            .map(|(e, l)| EpochEnd {
                epoch: *e,
                last_trade_id: *l,
            })
            .collect();
        if let Some(broken) = &self.broken {
            broken(&mut page);
        }
        Box::pin(async move { Ok(page) })
    }
}

pub(super) fn span(from: u64, to: u64) -> Vec<u64> {
    (from..=to).collect()
}

const FAST: Pacing = Pacing {
    first: Duration::from_millis(1),
    max: Duration::from_millis(4),
};

fn bounded(after: u64, through: u64) -> TradesGap {
    gap(after, through, 0, GapCause::Detected)
}

#[tokio::test]
async fn backfill_termination_cases() {
    let e4 = 3u64 << 40;
    let cases: Vec<(&str, FakeHistory, TradesGap, Vec<u64>, usize)> = vec![
        (
            "same epoch",
            FakeHistory::new(span(1, 110), &[], |_| 110),
            bounded(100, 110),
            span(101, 110),
            1,
        ),
        (
            "reconnect into an empty epoch",
            FakeHistory::new(span(1, 105), &[(1, 105)], |_| E2),
            bounded(100, E2),
            span(101, 105),
            1,
        ),
        (
            "a full page ends on the epoch's last trade",
            FakeHistory {
                limit: Some(5),
                ..FakeHistory::new(span(1, 105), &[(1, 105)], |_| E2)
            },
            bounded(100, E2),
            span(101, 105),
            2,
        ),
        (
            "an epoch base behind an empty epoch",
            FakeHistory::new(span(1, 105), &[(1, 105), (2, E2)], |_| E3),
            bounded(100, E3),
            span(101, 105),
            1,
        ),
        (
            "across epochs with trades",
            FakeHistory::new(
                [span(1, 105), span(E2 + 1, E2 + 4), span(E3 + 1, E3 + 3)].concat(),
                &[(1, 105), (2, E2 + 4)],
                |_| E3 + 3,
            ),
            bounded(100, E3 + 3),
            [span(101, 105), span(E2 + 1, E2 + 4), span(E3 + 1, E3 + 3)].concat(),
            1,
        ),
        (
            "empty old tail",
            FakeHistory::new(span(1, 105), &[(1, 105)], |_| E2),
            gap(105, 0, 1, GapCause::Skipped),
            vec![],
            1,
        ),
        (
            "a tail with trades",
            FakeHistory::new([span(1, 105), vec![E2 + 1]].concat(), &[(1, 105)], |_| {
                E2 + 1
            }),
            gap(100, 0, 1, GapCause::Skipped),
            span(101, 105),
            1,
        ),
        (
            "the store behind the client",
            FakeHistory::new(span(1, 105), &[], |c| if c < 2 { 90 } else { 105 }),
            bounded(100, 105),
            span(101, 105),
            3,
        ),
        (
            "the store behind by an epoch",
            FakeHistory::new(
                [span(1, 100), span(E2 + 1, E2 + 5)].concat(),
                &[(1, 100)],
                |c| if c < 1 { 90 } else { E2 + 5 },
            ),
            bounded(E2 + 3, E2 + 5),
            span(E2 + 4, E2 + 5),
            2,
        ),
        (
            "more than two pages",
            FakeHistory::new(span(1, 3000), &[], |_| 3000),
            bounded(400, 2900),
            span(401, 2900),
            3,
        ),
        (
            "several empty epochs",
            FakeHistory::new(span(1, 5), &[(1, 5), (2, E2), (3, E3)], move |_| e4),
            bounded(5, e4),
            vec![],
            1,
        ),
    ];
    for (name, history, gap, want, pages) in cases {
        let got = backfill_paced(&history, gap, FAST)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            got.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
            want,
            "{name}"
        );
        assert_eq!(
            history.calls(),
            pages,
            "{name}: cursors {:?}",
            history.afters.lock().unwrap()
        );
        let afters = history.afters.lock().unwrap().clone();
        assert!(
            afters.windows(2).all(|w| w[1] >= w[0]),
            "{name}: cursor moved backwards {afters:?}"
        );
    }
}

#[tokio::test]
async fn backfill_refuses_a_broken_contract() {
    let brokens: Vec<Broken> = vec![
        Box::new(|p| p.trades = vec![history_trade(102)]),
        Box::new(|p| {
            p.trades.clear();
            p.next_after_trade_id = 99;
        }),
        Box::new(|p| {
            p.trades.truncate(1);
            p.next_after_trade_id = 105;
        }),
        Box::new(|p| {
            p.trades.clear();
            p.next_after_trade_id = E2;
            p.epoch_ends.clear();
        }),
    ];
    for broken in brokens {
        let history = FakeHistory {
            broken: Some(broken),
            ..FakeHistory::new(span(1, 105), &[], |_| 105)
        };
        let err = backfill_paced(&history, bounded(100, 105), FAST)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ResponseContract { .. }), "{err}");
    }
}

struct Failing(Mutex<u32>);

impl TradeHistory for Failing {
    fn trades_after(
        &self,
        _: u32,
        _: u64,
        _: u32,
    ) -> crate::marketstream::backfill::HistoryFuture<'_> {
        *self.0.lock().unwrap() += 1;
        Box::pin(async { Err(Error::transport("unavailable")) })
    }
}

#[tokio::test]
async fn backfill_gives_up_after_repeated_failures() {
    let history = Failing(Mutex::new(0));
    assert!(backfill_paced(&history, bounded(1, 2), FAST).await.is_err());
    assert_eq!(*history.0.lock().unwrap(), 10);
}
