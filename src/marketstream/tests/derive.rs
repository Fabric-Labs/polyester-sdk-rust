//! Derives the market-data stream's test fixtures from captured frames.
//!
//! It reads a directory of captures, each a directory with a frames.tsv
//! (columns: file, direction, message) naming one WebSocket binary frame per
//! file. It selects one frame, or one sequence, per role, rewrites every
//! identifier, price, quantity and time to synthetic values, re-encodes the
//! frames with the generated codecs, and writes them with an index to
//! `testdata/marketstream/fixtures`. Captures stay where they are; only the
//! derived frames are written.
//!
//! ```text
//! POLYESTER_MARKETSTREAM_CAPTURES=DIR cargo test --lib derive_marketstream_fixtures -- --ignored
//! ```
//!
//! The rewrite keeps what the tests rely on: the order and contiguity of
//! update IDs and trade IDs, the epoch each trade ID encodes, the order of
//! prices, which quantities are zero, the order of times, and the pairing of
//! requests with their responses. It is the same rewrite the Go SDK's
//! fixtures are derived with, so both derive the same frames.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::marketstream::sbe::{
    Encoder, ReadBuf, WriteBuf, best_bid_offer_codec, depth_snapshot_codec, depth_update_codec,
    market_status_codec, message_header_codec, ping_codec, pong_codec, subscribe_codec,
    subscribe_response_codec, trades_codec, trades_skipped_codec, trades_through_codec,
};

const SYMBOL_BASE: u32 = 101;
const UPDATE_BASE: u64 = 1000;
const TRADE_OFFSET: u64 = 500;
const PRICE_BASE: i64 = 100_000_000_000;
const TIME_BASE: u64 = 1_700_000_000_000_000_000;
const NULL_PRICE: i64 = i64::MIN;
const HEADER: usize = message_header_codec::ENCODED_LENGTH;

const STREAM_DEPTH20: u8 = 3;
const STREAM_DEPTH: u8 = 4;
const FEED_LIVE: u8 = 1;
const FEED_STALE: u8 = 2;
const MAX_CHAIN_UPDATES: usize = 8;

#[derive(Debug, Clone, Copy, Default)]
struct Level {
    price: i64,
    qty: i64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Fill {
    id: u64,
    price: i64,
    qty: i64,
    side: u8,
}

#[derive(Debug, Clone, Copy, Default)]
struct Item {
    symbol: u32,
    stream: u8,
    result: u8,
}

/// One decoded message, in wire units.
#[derive(Debug, Clone, Default)]
struct M {
    template: u16,
    request_id: u64,
    items: Vec<Item>,
    symbol: u32,
    stream: u8,
    feed: u8,
    epoch: u64,
    update: u64,
    first: u64,
    last: u64,
    last_trade: u64,
    transact: u64,
    send: u64,
    levels_per_side: u16,
    bids: Vec<Level>,
    asks: Vec<Level>,
    fills: Vec<Fill>,
    bid_price: i64,
    bid_qty: i64,
    ask_price: i64,
    ask_qty: i64,
}

#[derive(Debug, Clone)]
struct Frame {
    capture: String,
    direction: String,
    message: String,
    m: M,
}

fn header(frame: &[u8]) -> message_header_codec::MessageHeaderDecoder<ReadBuf<'_>> {
    message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0)
}

fn levels_of(v: Vec<(i64, i64)>) -> Vec<Level> {
    v.into_iter()
        .map(|(price, qty)| Level { price, qty })
        .collect()
}

/// Reads a frame with the generated decoders.
fn decode(b: &[u8]) -> M {
    let h = header(b);
    let template = h.template_id();
    let mut m = M {
        template,
        ..M::default()
    };
    match template {
        subscribe_codec::SBE_TEMPLATE_ID => {
            let d = subscribe_codec::SubscribeDecoder::default().header(h, 0);
            m.request_id = d.request_id();
            let mut g = d.items_decoder();
            while g.advance().unwrap().is_some() {
                m.items.push(Item {
                    symbol: g.symbol_id(),
                    stream: g.stream().into(),
                    result: 0,
                });
            }
        }
        ping_codec::SBE_TEMPLATE_ID => {
            m.request_id = ping_codec::PingDecoder::default().header(h, 0).request_id()
        }
        subscribe_response_codec::SBE_TEMPLATE_ID => {
            let d = subscribe_response_codec::SubscribeResponseDecoder::default().header(h, 0);
            m.request_id = d.request_id();
            let mut g = d.results_decoder();
            while g.advance().unwrap().is_some() {
                m.items.push(Item {
                    symbol: g.symbol_id(),
                    stream: g.stream().into(),
                    result: g.result().into(),
                });
            }
        }
        pong_codec::SBE_TEMPLATE_ID => {
            let d = pong_codec::PongDecoder::default().header(h, 0);
            (m.request_id, m.send) = (d.request_id(), d.send_ts_ns());
        }
        trades_codec::SBE_TEMPLATE_ID => {
            let d = trades_codec::TradesDecoder::default().header(h, 0);
            (m.symbol, m.epoch, m.update, m.transact, m.send) = (
                d.symbol_id(),
                d.book_epoch(),
                d.update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
            );
            let mut g = d.entries_decoder();
            while g.advance().unwrap().is_some() {
                m.fills.push(Fill {
                    id: g.trade_id(),
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                    side: g.taker_side().into(),
                });
            }
        }
        trades_skipped_codec::SBE_TEMPLATE_ID => {
            let d = trades_skipped_codec::TradesSkippedDecoder::default().header(h, 0);
            (m.symbol, m.epoch, m.first, m.last, m.send) = (
                d.symbol_id(),
                d.book_epoch(),
                d.first_trade_id(),
                d.last_trade_id().unwrap_or(0),
                d.send_ts_ns(),
            );
        }
        trades_through_codec::SBE_TEMPLATE_ID => {
            let d = trades_through_codec::TradesThroughDecoder::default().header(h, 0);
            (m.symbol, m.epoch, m.last_trade, m.send) = (
                d.symbol_id(),
                d.book_epoch(),
                d.last_trade_id(),
                d.send_ts_ns(),
            );
        }
        best_bid_offer_codec::SBE_TEMPLATE_ID => {
            let d = best_bid_offer_codec::BestBidOfferDecoder::default().header(h, 0);
            (m.symbol, m.epoch, m.update, m.transact, m.send) = (
                d.symbol_id(),
                d.book_epoch(),
                d.update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
            );
            m.bid_price = d.bid_price_ticks().unwrap_or(NULL_PRICE);
            m.ask_price = d.ask_price_ticks().unwrap_or(NULL_PRICE);
            (m.bid_qty, m.ask_qty) = (d.bid_qty_scaled(), d.ask_qty_scaled());
        }
        depth_snapshot_codec::SBE_TEMPLATE_ID => {
            let d = depth_snapshot_codec::DepthSnapshotDecoder::default().header(h, 0);
            (m.symbol, m.stream, m.epoch, m.update) = (
                d.symbol_id(),
                d.stream().into(),
                d.book_epoch(),
                d.update_id(),
            );
            (m.transact, m.send, m.levels_per_side) =
                (d.transact_ts_ns(), d.send_ts_ns(), d.levels_per_side());
            let mut g = d.bids_decoder();
            let mut bids = Vec::new();
            while g.advance().unwrap().is_some() {
                bids.push((g.price_ticks(), g.qty_scaled()));
            }
            let d = g.parent().unwrap();
            let mut g = d.asks_decoder();
            let mut asks = Vec::new();
            while g.advance().unwrap().is_some() {
                asks.push((g.price_ticks(), g.qty_scaled()));
            }
            (m.bids, m.asks) = (levels_of(bids), levels_of(asks));
        }
        depth_update_codec::SBE_TEMPLATE_ID => {
            let d = depth_update_codec::DepthUpdateDecoder::default().header(h, 0);
            (m.symbol, m.epoch, m.first, m.last, m.transact, m.send) = (
                d.symbol_id(),
                d.book_epoch(),
                d.first_update_id(),
                d.last_update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
            );
            let mut g = d.bids_decoder();
            let mut bids = Vec::new();
            while g.advance().unwrap().is_some() {
                bids.push((g.price_ticks(), g.qty_scaled()));
            }
            let d = g.parent().unwrap();
            let mut g = d.asks_decoder();
            let mut asks = Vec::new();
            while g.advance().unwrap().is_some() {
                asks.push((g.price_ticks(), g.qty_scaled()));
            }
            (m.bids, m.asks) = (levels_of(bids), levels_of(asks));
        }
        market_status_codec::SBE_TEMPLATE_ID => {
            let d = market_status_codec::MarketStatusDecoder::default().header(h, 0);
            (m.symbol, m.feed, m.send) = (d.symbol_id(), d.feed().into(), d.send_ts_ns());
        }
        other => panic!("template {other}"),
    }
    m
}

/// Writes a message with the generated encoders.
fn encode(m: &M) -> Vec<u8> {
    let mut b = vec![0u8; 1 << 20];
    let len = match m.template {
        subscribe_codec::SBE_TEMPLATE_ID => {
            let mut x = subscribe_codec::SubscribeEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.request_id(m.request_id);
            let mut g = x.items_encoder(m.items.len() as u16, Default::default());
            for it in &m.items {
                g.advance().unwrap();
                g.symbol_id(it.symbol);
                g.stream(it.stream.into());
            }
            g.parent().unwrap().get_limit()
        }
        ping_codec::SBE_TEMPLATE_ID => {
            let mut x = ping_codec::PingEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.request_id(m.request_id);
            x.get_limit()
        }
        subscribe_response_codec::SBE_TEMPLATE_ID => {
            let mut x = subscribe_response_codec::SubscribeResponseEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.request_id(m.request_id);
            let mut g = x.results_encoder(m.items.len() as u16, Default::default());
            for it in &m.items {
                g.advance().unwrap();
                g.symbol_id(it.symbol);
                g.stream(it.stream.into());
                g.result(it.result.into());
            }
            g.parent().unwrap().get_limit()
        }
        pong_codec::SBE_TEMPLATE_ID => {
            let mut x = pong_codec::PongEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.request_id(m.request_id);
            x.send_ts_ns(m.send);
            x.get_limit()
        }
        trades_codec::SBE_TEMPLATE_ID => {
            let mut x = trades_codec::TradesEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.book_epoch(m.epoch);
            x.update_id(m.update);
            x.transact_ts_ns(m.transact);
            x.send_ts_ns(m.send);
            let mut g = x.entries_encoder(m.fills.len() as u16, Default::default());
            for f in &m.fills {
                g.advance().unwrap();
                g.trade_id(f.id);
                g.price_ticks(f.price);
                g.qty_scaled(f.qty);
                g.taker_side(f.side.into());
            }
            g.parent().unwrap().get_limit()
        }
        trades_skipped_codec::SBE_TEMPLATE_ID => {
            let mut x = trades_skipped_codec::TradesSkippedEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.book_epoch(m.epoch);
            x.first_trade_id(m.first);
            x.last_trade_id(m.last);
            x.send_ts_ns(m.send);
            x.get_limit()
        }
        trades_through_codec::SBE_TEMPLATE_ID => {
            let mut x = trades_through_codec::TradesThroughEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.book_epoch(m.epoch);
            x.last_trade_id(m.last_trade);
            x.send_ts_ns(m.send);
            x.get_limit()
        }
        best_bid_offer_codec::SBE_TEMPLATE_ID => {
            let mut x = best_bid_offer_codec::BestBidOfferEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.book_epoch(m.epoch);
            x.update_id(m.update);
            x.transact_ts_ns(m.transact);
            x.send_ts_ns(m.send);
            x.bid_price_ticks(m.bid_price);
            x.bid_qty_scaled(m.bid_qty);
            x.ask_price_ticks(m.ask_price);
            x.ask_qty_scaled(m.ask_qty);
            x.get_limit()
        }
        depth_snapshot_codec::SBE_TEMPLATE_ID => {
            let mut x = depth_snapshot_codec::DepthSnapshotEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.stream(m.stream.into());
            x.book_epoch(m.epoch);
            x.update_id(m.update);
            x.transact_ts_ns(m.transact);
            x.send_ts_ns(m.send);
            x.levels_per_side(m.levels_per_side);
            let mut g = x.bids_encoder(m.bids.len() as u16, Default::default());
            for l in &m.bids {
                g.advance().unwrap();
                g.price_ticks(l.price);
                g.qty_scaled(l.qty);
            }
            let x = g.parent().unwrap();
            let mut g = x.asks_encoder(m.asks.len() as u16, Default::default());
            for l in &m.asks {
                g.advance().unwrap();
                g.price_ticks(l.price);
                g.qty_scaled(l.qty);
            }
            g.parent().unwrap().get_limit()
        }
        depth_update_codec::SBE_TEMPLATE_ID => {
            let mut x = depth_update_codec::DepthUpdateEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.book_epoch(m.epoch);
            x.first_update_id(m.first);
            x.last_update_id(m.last);
            x.transact_ts_ns(m.transact);
            x.send_ts_ns(m.send);
            let mut g = x.bids_encoder(m.bids.len() as u16, Default::default());
            for l in &m.bids {
                g.advance().unwrap();
                g.price_ticks(l.price);
                g.qty_scaled(l.qty);
            }
            let x = g.parent().unwrap();
            let mut g = x.asks_encoder(m.asks.len() as u16, Default::default());
            for l in &m.asks {
                g.advance().unwrap();
                g.price_ticks(l.price);
                g.qty_scaled(l.qty);
            }
            g.parent().unwrap().get_limit()
        }
        market_status_codec::SBE_TEMPLATE_ID => {
            let mut x = market_status_codec::MarketStatusEncoder::default()
                .wrap(WriteBuf::new(&mut b), HEADER)
                .header(0)
                .parent()
                .unwrap();
            x.symbol_id(m.symbol);
            x.feed(m.feed.into());
            x.send_ts_ns(m.send);
            x.get_limit()
        }
        other => panic!("template {other}"),
    };
    b.truncate(len);
    b
}

/// Maps one capture's values to synthetic ones; symbols are shared.
struct Rewrite {
    requests: HashMap<u64, u64>,
    min_update: u64,
    min_price: i64,
    min_time: u64,
}

fn qty(q: i64) -> i64 {
    if q == 0 { 0 } else { 1000 * (1 + q % 97) }
}

impl Rewrite {
    fn new(frames: &[Frame]) -> Self {
        let mut r = Rewrite {
            requests: HashMap::new(),
            min_update: u64::MAX,
            min_price: i64::MAX,
            min_time: u64::MAX,
        };
        for f in frames {
            let m = &f.m;
            if m.template != trades_skipped_codec::SBE_TEMPLATE_ID {
                for u in [m.update, m.first, m.last] {
                    if u != 0 {
                        r.min_update = r.min_update.min(u);
                    }
                }
            }
            let prices = m
                .bids
                .iter()
                .chain(&m.asks)
                .map(|l| l.price)
                .chain(m.fills.iter().map(|f| f.price))
                .chain([m.bid_price, m.ask_price]);
            for p in prices {
                if p != NULL_PRICE && p > 0 {
                    r.min_price = r.min_price.min(p);
                }
            }
            for t in [m.transact, m.send] {
                if t != 0 {
                    r.min_time = r.min_time.min(t);
                }
            }
        }
        if r.min_update == u64::MAX {
            r.min_update = 0;
        }
        if r.min_price == i64::MAX {
            r.min_price = PRICE_BASE;
        }
        if r.min_time == u64::MAX {
            r.min_time = TIME_BASE;
        }
        r
    }

    fn request(&mut self, id: u64) -> u64 {
        let next = self.requests.len() as u64 + 1;
        *self.requests.entry(id).or_insert(next)
    }

    fn update(&self, u: u64) -> u64 {
        u - self.min_update + UPDATE_BASE
    }

    fn trade(id: u64) -> u64 {
        if id == 0 { 0 } else { id + TRADE_OFFSET }
    }

    fn price(&self, p: i64) -> i64 {
        if p == NULL_PRICE {
            p
        } else {
            p - self.min_price + PRICE_BASE
        }
    }

    fn time(&self, t: u64) -> u64 {
        if t == 0 {
            0
        } else {
            t - self.min_time + TIME_BASE
        }
    }

    fn levels(&self, ls: &[Level]) -> Vec<Level> {
        ls.iter()
            .map(|l| Level {
                price: self.price(l.price),
                qty: qty(l.qty),
            })
            .collect()
    }

    fn apply(&mut self, symbols: &mut HashMap<u32, u32>, mut m: M) -> M {
        let mut symbol = |s: u32| {
            let next = SYMBOL_BASE + symbols.len() as u32;
            *symbols.entry(s).or_insert(next)
        };
        m.request_id = self.request(m.request_id);
        let client_or_reply = [
            subscribe_codec::SBE_TEMPLATE_ID,
            ping_codec::SBE_TEMPLATE_ID,
            subscribe_response_codec::SBE_TEMPLATE_ID,
            pong_codec::SBE_TEMPLATE_ID,
        ];
        if client_or_reply.contains(&m.template) {
            m.items = m
                .items
                .iter()
                .map(|it| Item {
                    symbol: symbol(it.symbol),
                    ..*it
                })
                .collect();
        } else {
            m.request_id = 0;
        }
        if m.symbol != 0 {
            m.symbol = symbol(m.symbol);
        }
        if m.update != 0 {
            m.update = self.update(m.update);
        }
        if m.template == depth_update_codec::SBE_TEMPLATE_ID {
            (m.first, m.last) = (self.update(m.first), self.update(m.last));
        }
        if m.template == trades_skipped_codec::SBE_TEMPLATE_ID {
            (m.first, m.last) = (Self::trade(m.first), Self::trade(m.last));
        }
        if m.template == trades_through_codec::SBE_TEMPLATE_ID {
            m.last_trade = Self::trade(m.last_trade);
        }
        (m.transact, m.send) = (self.time(m.transact), self.time(m.send));
        (m.bids, m.asks) = (self.levels(&m.bids), self.levels(&m.asks));
        m.fills = m
            .fills
            .iter()
            .map(|f| Fill {
                id: Self::trade(f.id),
                price: self.price(f.price),
                qty: qty(f.qty),
                side: f.side,
            })
            .collect();
        if m.template == best_bid_offer_codec::SBE_TEMPLATE_ID {
            (m.bid_price, m.ask_price) = (self.price(m.bid_price), self.price(m.ask_price));
            (m.bid_qty, m.ask_qty) = (qty(m.bid_qty), qty(m.ask_qty));
        }
        m
    }
}

struct Fixture {
    name: String,
    group: String,
    note: String,
    frame: Option<(usize, usize)>,
}

struct Deriver {
    captures: Vec<Vec<Frame>>,
    out: Vec<Fixture>,
}

impl Deriver {
    fn first(&self, f: impl Fn(&Frame) -> bool) -> Option<(usize, usize)> {
        for (ci, c) in self.captures.iter().enumerate() {
            if let Some(i) = c.iter().position(&f) {
                return Some((ci, i));
            }
        }
        None
    }

    fn add(
        &mut self,
        name: impl Into<String>,
        group: impl Into<String>,
        note: impl Into<String>,
        frame: Option<(usize, usize)>,
    ) {
        self.out.push(Fixture {
            name: name.into(),
            group: group.into(),
            note: note.into(),
            frame,
        });
    }

    fn one(&mut self, name: &str, note: &str, f: impl Fn(&Frame) -> bool) {
        match self.first(f) {
            Some(at) => self.add(name, name, note, Some(at)),
            None => self.add(
                name,
                name,
                format!("TODO: no captured frame yet; {note}"),
                None,
            ),
        }
    }

    fn select(&mut self) {
        let is = |dir: &'static str, msg: &'static str| {
            move |f: &Frame| f.direction == dir && f.message == msg
        };
        self.one("client-subscribe", "a Subscribe", is("client", "Subscribe"));
        self.one("client-ping", "a Ping", is("client", "Ping"));
        self.one(
            "subscribe-response",
            "the answer to client-subscribe",
            is("server", "SubscribeResponse"),
        );
        self.one("pong", "the answer to client-ping", is("server", "Pong"));
        self.one(
            "best-bid-offer",
            "a BestBidOffer",
            is("server", "BestBidOffer"),
        );
        self.one("market-status-stale", "MarketStatus STALE", |f| {
            f.message == "MarketStatus" && f.m.feed == FEED_STALE
        });
        self.one("market-status-live", "MarketStatus LIVE", |f| {
            f.message == "MarketStatus" && f.m.feed == FEED_LIVE
        });
        self.trades();
        self.one(
            "trades-skipped",
            "a TradesSkipped",
            is("server", "TradesSkipped"),
        );
        self.depth20();
        self.one("depth-snapshot", "a depth-stream DepthSnapshot", |f| {
            f.message == "DepthSnapshot" && f.m.stream == STREAM_DEPTH
        });
        self.one(
            "depth-snapshot-levels",
            "a depth-stream DepthSnapshot holding levels",
            |f| {
                f.message == "DepthSnapshot"
                    && f.m.stream == STREAM_DEPTH
                    && f.m.bids.len() + f.m.asks.len() > 0
            },
        );
        self.chains();
    }

    fn trades(&mut self) {
        for ci in 0..self.captures.len() {
            let mut through: HashMap<u32, usize> = HashMap::new();
            for i in 0..self.captures[ci].len() {
                let f = &self.captures[ci][i];
                match f.message.as_str() {
                    "TradesThrough" => {
                        through.insert(f.m.symbol, i);
                    }
                    "Trades" => {
                        if let Some(&j) = through.get(&f.m.symbol) {
                            self.add(
                                "trades-through",
                                "trades",
                                "the pair's baseline",
                                Some((ci, j)),
                            );
                            self.add(
                                "trades",
                                "trades",
                                "trades after the baseline",
                                Some((ci, i)),
                            );
                            return;
                        }
                    }
                    _ => {}
                }
            }
        }
        self.add(
            "trades-through",
            "trades",
            "TODO: no captured baseline before trades yet",
            None,
        );
        self.add(
            "trades",
            "trades",
            "TODO: no captured trades after a baseline yet",
            None,
        );
    }

    fn depth20(&mut self) {
        let mut best: Option<(usize, usize, usize)> = None;
        for (ci, c) in self.captures.iter().enumerate() {
            for (i, f) in c.iter().enumerate() {
                let n = f.m.bids.len() + f.m.asks.len();
                if f.message == "DepthSnapshot"
                    && f.m.stream == STREAM_DEPTH20
                    && best.is_none_or(|(_, _, b)| n > b)
                {
                    best = Some((ci, i, n));
                }
            }
        }
        match best {
            Some((ci, i, _)) => self.add(
                "depth20-snapshot",
                "depth20-snapshot",
                "the depth20 snapshot with the most levels",
                Some((ci, i)),
            ),
            None => self.add(
                "depth20-snapshot",
                "depth20-snapshot",
                "TODO: no captured depth20 snapshot yet",
                None,
            ),
        }
    }

    fn chains_of(c: &[Frame], ci: usize) -> Vec<Vec<(usize, usize)>> {
        let mut out = Vec::new();
        for (i, f) in c.iter().enumerate() {
            if f.message != "DepthSnapshot" || f.m.stream != STREAM_DEPTH {
                continue;
            }
            let mut ch = vec![(ci, i)];
            let mut pos = f.m.update;
            for (j, g) in c.iter().enumerate().skip(i + 1) {
                if g.m.symbol != f.m.symbol {
                    continue;
                }
                if g.message == "DepthSnapshot" && g.m.stream == STREAM_DEPTH {
                    break;
                }
                if g.message != "DepthUpdate" {
                    continue;
                }
                if g.m.epoch != f.m.epoch || g.m.first != pos + 1 {
                    break;
                }
                ch.push((ci, j));
                pos = g.m.last;
            }
            if ch.len() > 1 {
                out.push(ch);
            }
        }
        out
    }

    fn chains(&mut self) {
        let mut longest: Option<Vec<(usize, usize)>> = None;
        let mut bridge: Option<Vec<(usize, usize)>> = None;
        for (ci, c) in self.captures.iter().enumerate() {
            for ch in Self::chains_of(c, ci) {
                let m = |at: (usize, usize)| &self.captures[at.0][at.1].m;
                let with_levels = ch[1..]
                    .iter()
                    .filter(|at| m(**at).bids.len() + m(**at).asks.len() > 0)
                    .count();
                if with_levels > 0 && longest.as_ref().is_none_or(|l| ch.len() > l.len()) {
                    longest = Some(ch.clone());
                }
                let u = m(ch[1]);
                if bridge.is_none() && u.last > u.first && u.bids.len() + u.asks.len() == 0 {
                    bridge = Some(ch[..2].to_vec());
                }
            }
        }
        match longest {
            None => self.add(
                "depth-chain",
                "depth-chain",
                "TODO: no captured depth chain with levels yet",
                None,
            ),
            Some(ch) => {
                for (i, at) in ch.into_iter().take(1 + MAX_CHAIN_UPDATES).enumerate() {
                    let note = if i == 0 {
                        "the chain's DepthSnapshot"
                    } else {
                        "contiguous DepthUpdate"
                    };
                    self.add(format!("depth-chain-{i:02}"), "depth-chain", note, Some(at));
                }
            }
        }
        match bridge {
            None => self.add(
                "depth-bridge",
                "depth-bridge",
                "TODO: no captured bridging DepthUpdate yet",
                None,
            ),
            Some(ch) => {
                self.add(
                    "depth-bridge-00",
                    "depth-bridge",
                    "the DepthSnapshot the bridging range starts after",
                    Some(ch[0]),
                );
                self.add(
                    "depth-bridge-01",
                    "depth-bridge",
                    "a bridging DepthUpdate: a range without levels",
                    Some(ch[1]),
                );
            }
        }
    }
}

fn load(dir: &Path) -> Vec<Frame> {
    let index = std::fs::read_to_string(dir.join("frames.tsv")).expect("frames.tsv");
    let capture = dir.file_name().unwrap().to_string_lossy().into_owned();
    index
        .lines()
        .skip(1)
        .filter_map(|line| {
            let cols: Vec<&str> = line.split('\t').collect();
            (cols.len() >= 3).then(|| {
                let data = std::fs::read(dir.join(cols[0])).expect("frame");
                Frame {
                    capture: capture.clone(),
                    direction: cols[1].to_owned(),
                    message: cols[2].to_owned(),
                    m: decode(&data),
                }
            })
        })
        .collect()
}

/// The fixtures' directory in this repository.
pub(super) fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/marketstream/fixtures")
}

#[test]
#[ignore = "derives the fixtures from captures; set POLYESTER_MARKETSTREAM_CAPTURES"]
fn derive_marketstream_fixtures() {
    let root = PathBuf::from(
        std::env::var_os("POLYESTER_MARKETSTREAM_CAPTURES")
            .expect("POLYESTER_MARKETSTREAM_CAPTURES"),
    );
    let out = std::env::var_os("POLYESTER_MARKETSTREAM_FIXTURES_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(fixtures_dir);
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.join("frames.tsv").is_file())
        .collect();
    dirs.sort();
    assert!(!dirs.is_empty(), "no frames.tsv under {}", root.display());
    let captures: Vec<Vec<Frame>> = dirs.iter().map(|d| load(d)).collect();
    let mut rewrites: Vec<Rewrite> = captures.iter().map(|c| Rewrite::new(c)).collect();
    let mut d = Deriver {
        captures,
        out: Vec::new(),
    };
    d.select();

    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let mut symbols = HashMap::new();
    let mut index = String::new();
    index.push_str("# Synthetic market-data stream frames derived from captures by the derive_marketstream_fixtures test.\n");
    index.push_str("# Every identifier, price, quantity and time is synthetic. Rows with no file are TODO slots.\n");
    index.push_str("file\tdirection\tmessage\tgroup\tnote\n");
    for (i, fx) in d.out.iter().enumerate() {
        let Some((ci, fi)) = fx.frame else {
            writeln!(index, "-\t-\t-\t{}\t{}", fx.group, fx.note).unwrap();
            continue;
        };
        let f = &d.captures[ci][fi];
        let m = rewrites[ci].apply(&mut symbols, f.m.clone());
        let data = encode(&m);
        let _ = decode(&data);
        let name = format!("{:02}-{}.bin", i + 1, fx.name);
        std::fs::write(out.join(&name), &data).unwrap();
        writeln!(
            index,
            "{name}\t{}\t{}\t{}\t{}",
            f.direction, f.message, fx.group, fx.note
        )
        .unwrap();
        let _ = &f.capture;
    }
    std::fs::write(out.join("index.tsv"), index).unwrap();
}
