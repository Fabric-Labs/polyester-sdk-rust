//! Each topic's requests go through its owner: an answer applies only to the
//! request the owner sent last, and with none outstanding the owner moves the
//! server toward the caller's intent. These tests drive a client against a
//! simulated server that answers requests in the order received, with the
//! caller's requests, the server's answers, depth updates, gaps and stale
//! periods interleaved in every order.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use super::client::{ClientMsg, FakeServer, TestPairs, decode_client, dial};
use super::frames::*;
use crate::errors::Error;
use crate::marketstream::client::{
    depth_book, handle, issue, live_of, offline_stream, reconnect, settled, wire,
};
use crate::marketstream::codec::{self, Msg};
use crate::marketstream::sbe::subscribe_result::SubscribeResult as Wire;
use crate::marketstream::{
    Config, Event, FeedState, MarketStream, Stream, StreamError, SubscribeResult, Topic,
};

const fn topic(symbol_id: u32, stream: Stream) -> Topic {
    Topic { symbol_id, stream }
}
const DEPTH_7: Topic = topic(7, Stream::Depth);
const TOPICS: [Topic; 5] = [
    topic(7, Stream::Depth),
    topic(9, Stream::Depth),
    topic(7, Stream::Bbo),
    topic(9, Stream::Trades),
    topic(9, Stream::Depth20),
];

/// SplitMix64: a small seeded generator, so every run of a seed is the same.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % n
    }
}

/// One connection's server: it holds subscriptions, answers requests in the
/// order received and sends frames in order. A new subscription opens with
/// its initial state and nothing more: a depth or depth20 snapshot at the
/// pair's position, a quote, or the trades baseline. Streams are otherwise
/// quiet unless a test sends updates.
struct Sim {
    held: HashSet<Topic>,
    wire: mpsc::UnboundedReceiver<Vec<u8>>,
    inbox: VecDeque<ClientMsg>,
    out: VecDeque<Vec<u8>>,
    pos: HashMap<u32, u64>,
    quote: u64,
    log: Vec<String>,
    /// Topics whose next Unsubscribe is answered LIMIT_EXCEEDED, leaving the
    /// subscription in place.
    refuse: HashSet<Topic>,
    /// Topics whose next Subscribe is answered LIMIT_EXCEEDED, leaving the
    /// topic not held.
    refuse_sub: HashSet<Topic>,
    /// Topics whose last answered request was a Subscribe the server
    /// refused, which ends the caller's intent for them.
    refused: HashSet<Topic>,
    /// Each pair's last trade ID.
    trade: HashMap<u32, u64>,
    /// Connections after the first.
    reconnects: usize,
    /// Which answered requests were unsubscribes.
    unsubscribes: HashMap<u64, bool>,
    /// Topics the client received state of since the last answer it received
    /// that ended or replaced the topic's subscription. Only a ready topic
    /// may be live.
    ready: HashSet<Topic>,
}

impl Sim {
    /// A server, and an offline client whose requests go to it.
    fn new() -> (Self, MarketStream) {
        let mut config = Config::new("ws://127.0.0.1/ws/market.sbe", Arc::new(TestPairs));
        config.queue_size = 1 << 16;
        let stream = offline_stream(config);
        let sim = Self {
            held: HashSet::new(),
            wire: wire(stream.shared()),
            inbox: VecDeque::new(),
            out: VecDeque::new(),
            pos: HashMap::from([(7, 10), (9, 10)]),
            quote: 1,
            log: Vec::new(),
            refuse: HashSet::new(),
            refuse_sub: HashSet::new(),
            refused: HashSet::new(),
            trade: HashMap::from([(7, 100), (9, 100)]),
            reconnects: 0,
            unsubscribes: HashMap::new(),
            ready: HashSet::new(),
        };
        (sim, stream)
    }

    fn trace(&self) -> String {
        self.log.join("\n")
    }

    /// Takes in what the client sent.
    fn receive(&mut self) {
        while let Ok(frame) = self.wire.try_recv() {
            let m = decode_client(&frame);
            let verb = if m.unsubscribe {
                "Unsubscribe"
            } else {
                "Subscribe"
            };
            self.log.push(format!(
                "client sends {verb} {} {:?}",
                m.request_id, m.topics
            ));
            self.inbox.push_back(m);
        }
    }

    fn pending(&mut self) -> usize {
        self.receive();
        self.inbox.len()
    }

    fn snapshot(&self, symbol: u32) -> Vec<u8> {
        let p = self.pos[&symbol];
        depth_snapshot(symbol, Stream::Depth, 1, p, &[lv(100, p as i64)], &[])
    }

    fn bbo(&mut self, symbol: u32) -> Vec<u8> {
        self.quote += 1;
        bbo(symbol, 1, self.quote, (100, 1), (101, 1))
    }

    /// Answers the oldest request received.
    fn process(&mut self) {
        self.receive();
        let m = self.inbox.pop_front().expect("a request");
        let mut results = Vec::new();
        let mut opened = Vec::new();
        for t in &m.topics {
            self.refused.remove(t);
            let refused = if m.unsubscribe {
                self.refuse.remove(t)
            } else {
                !self.held.contains(t) && self.refuse_sub.remove(t)
            };
            if refused && !m.unsubscribe {
                self.refused.insert(*t);
            }
            let r = match (m.unsubscribe, self.held.contains(t)) {
                _ if refused => Wire::LIMIT_EXCEEDED,
                (true, false) => Wire::NOT_SUBSCRIBED,
                (true, true) => {
                    self.held.remove(t);
                    Wire::OK
                }
                (false, true) => Wire::ALREADY_SUBSCRIBED,
                (false, false) => {
                    self.held.insert(*t);
                    opened.push(*t);
                    Wire::OK
                }
            };
            results.push((t.symbol_id, t.stream, r));
        }
        self.unsubscribes.insert(m.request_id, m.unsubscribe);
        self.log
            .push(format!("server answers {} {results:?}", m.request_id));
        self.out
            .push_back(subscribe_response(m.request_id, &results));
        for t in opened {
            match t.stream {
                Stream::Depth => {
                    let f = self.snapshot(t.symbol_id);
                    self.out.push_back(f);
                }
                Stream::Depth20 => {
                    let p = self.pos[&t.symbol_id];
                    self.out.push_back(depth_snapshot(
                        t.symbol_id,
                        Stream::Depth20,
                        1,
                        p,
                        &[lv(100, p as i64)],
                        &[],
                    ));
                }
                Stream::Bbo => {
                    let f = self.bbo(t.symbol_id);
                    self.out.push_back(f);
                }
                Stream::Trades => {
                    let last = self.trade[&t.symbol_id];
                    self.out.push_back(trades_through(t.symbol_id, 1, last));
                }
            }
        }
    }

    /// Sends a depth update of a held stream; `skip` leaves out the update
    /// before it, a gap.
    fn update(&mut self, symbol: u32, skip: bool) {
        if !self.held.contains(&Topic::new(symbol, Stream::Depth)) {
            return;
        }
        let first = self.pos[&symbol] + if skip { 2 } else { 1 };
        self.pos.insert(symbol, first);
        self.log.push(format!("server updates {symbol} to {first}"));
        self.out.push_back(depth_update(
            symbol,
            1,
            first,
            first,
            &[lv(100, first as i64)],
            &[],
        ));
    }

    /// Reports a pair stale, then its replacement snapshot, then live.
    fn stale(&mut self, symbol: u32) {
        self.log.push(format!("server reports {symbol} stale"));
        self.out.push_back(market_status(symbol, FeedState::Stale));
        if self.held.contains(&Topic::new(symbol, Stream::Depth)) {
            let f = self.snapshot(symbol);
            self.out.push_back(f);
        }
        self.out.push_back(market_status(symbol, FeedState::Live));
    }

    /// Sends the next trade of a held trades stream.
    fn trades(&mut self, symbol: u32) {
        if !self.held.contains(&Topic::new(symbol, Stream::Trades)) {
            return;
        }
        let id = self.trade[&symbol] + 1;
        self.trade.insert(symbol, id);
        self.log.push(format!("server trades {symbol}: {id}"));
        self.out.push_back(trades(symbol, 1, id, &[fill(id)]));
    }

    /// Sends what a held subscription repeats on its own: the trades frontier,
    /// a quote, a depth20 snapshot.
    fn refresh(&mut self, symbol: u32) {
        self.log.push(format!("server refreshes {symbol}"));
        if self.held.contains(&Topic::new(symbol, Stream::Trades)) {
            let last = self.trade[&symbol];
            self.out.push_back(trades_through(symbol, 1, last));
        }
        if self.held.contains(&Topic::new(symbol, Stream::Bbo)) {
            let f = self.bbo(symbol);
            self.out.push_back(f);
        }
        if self.held.contains(&Topic::new(symbol, Stream::Depth20)) {
            let p = self.pos[&symbol];
            self.out.push_back(depth_snapshot(
                symbol,
                Stream::Depth20,
                1,
                p,
                &[lv(100, p as i64)],
                &[],
            ));
        }
    }

    /// Replaces the connection: the server holds nothing on the new one and
    /// what was in flight is gone; the client marks everything stale and
    /// subscribes what it wants, as its session does.
    fn reconnect(&mut self, stream: &MarketStream) {
        self.reconnects += 1;
        self.log.push("connection replaced".into());
        self.held.clear();
        self.ready.clear();
        self.inbox.clear();
        self.out.clear();
        self.wire = reconnect(stream.shared());
    }

    /// Hands the client the oldest frame sent. A refused unsubscribe of a
    /// held stream ends the connection, which the client replaces.
    fn deliver(&mut self, stream: &mut MarketStream) {
        let frame = self.out.pop_front().expect("a frame");
        match codec::decode(&frame) {
            Ok(Some(Msg::SubscribeResponse {
                request_id,
                results,
            })) => {
                let unsubscribe = self.unsubscribes.get(&request_id).copied().unwrap_or(false);
                for (symbol_id, s, result) in results {
                    let ends =
                        result == Wire::OK || (unsubscribe && result == Wire::NOT_SUBSCRIBED);
                    if let (true, Some(s)) = (ends, s) {
                        self.ready.remove(&Topic::new(symbol_id, s));
                    }
                }
            }
            Ok(Some(Msg::TradesThrough { symbol_id, .. })) => {
                self.ready.insert(Topic::new(symbol_id, Stream::Trades));
            }
            Ok(Some(Msg::Bbo { symbol_id, .. })) => {
                self.ready.insert(Topic::new(symbol_id, Stream::Bbo));
            }
            Ok(Some(Msg::DepthSnapshot {
                symbol_id,
                stream: Some(s),
                ..
            })) => {
                self.ready.insert(Topic::new(symbol_id, s));
            }
            _ => {}
        }
        self.apply(stream, &frame);
        for t in TOPICS {
            assert!(
                !stream.live(t) || self.ready.contains(&t),
                "{t} live without state of the subscription it holds\n{}",
                self.trace()
            );
        }
    }

    fn apply(&mut self, stream: &mut MarketStream, frame: &[u8]) {
        match handle(stream.shared(), frame) {
            Ok(()) => {}
            Err(e @ Error::MarketStream(StreamError::Unsubscribe { .. })) => {
                self.log.push(format!("client ends the connection: {e}"));
                self.reconnect(stream);
            }
            Err(e) => panic!("client refused a frame: {e}\n{}", self.trace()),
        }
        stream.queued();
    }

    /// Runs the server and client until neither has anything left to send.
    /// The streams are quiet: nothing re-sends a topic's initial state.
    fn quiesce(&mut self, stream: &mut MarketStream) {
        let mut steps = 0;
        while self.pending() > 0 || !self.out.is_empty() {
            steps += 1;
            assert!(steps < 10_000, "no quiescence\n{}", self.trace());
            if !self.inbox.is_empty() {
                self.process();
            } else {
                self.deliver(stream);
            }
        }
    }

    /// Sends the caller's Subscribe or Unsubscribe, not waiting for it.
    fn caller(&mut self, stream: &MarketStream, topic: Topic, unsubscribe: bool) {
        let verb = if unsubscribe {
            "unsubscribes"
        } else {
            "subscribes"
        };
        self.log.push(format!("caller {verb} {topic}"));
        issue(stream.shared(), &[topic], unsubscribe).expect("sent");
    }

    /// Checks the client and server at quiescence against the caller's
    /// intent: the server holds exactly what the caller wants, every request
    /// is answered, a wanted depth stream is live with a book at the server's
    /// position, a wanted bbo stream is live, and nothing else keeps state.
    fn settled(&self, stream: &MarketStream, intent: &HashSet<Topic>) {
        let trace = self.trace();
        let s = settled(stream.shared());
        assert!(stream.err().is_none(), "{:?}\n{trace}", stream.err());
        for t in TOPICS {
            let want = intent.contains(&t);
            assert_eq!(
                self.held.contains(&t),
                want,
                "server holds {t} against the caller's intent\n{trace}"
            );
            assert_eq!(
                s.wanted.contains(&t),
                want,
                "client intent for {t}\n{trace}"
            );
            if !want {
                assert!(
                    !s.with_state.contains(&t),
                    "state kept for {t}, which the caller ended\n{trace}"
                );
                continue;
            }
            assert!(stream.live(t), "{t} wanted but not live\n{trace}");
            if t.stream == Stream::Depth {
                let book = stream.order_book(t.symbol_id, Stream::Depth, 0);
                assert_eq!(
                    book.map(|b| b.update_id),
                    Some(self.pos[&t.symbol_id]),
                    "{t} book against the server's position\n{trace}"
                );
            }
        }
        assert_eq!(s.unanswered, 0, "unanswered requests\n{trace}");
        assert!(s.busy.is_empty(), "owners {:?} busy\n{trace}", s.busy);
    }

    /// Subscribes topics and brings them live.
    fn live(&mut self, stream: &mut MarketStream, intent: &mut HashSet<Topic>, topics: &[Topic]) {
        for t in topics {
            self.caller(stream, *t, false);
            intent.insert(*t);
        }
        self.quiesce(stream);
        self.settled(stream, intent);
    }

    /// The single request waiting, which must be of kind `unsubscribe`.
    fn one_request(&mut self, unsubscribe: bool, what: &str) {
        let n = self.pending();
        assert!(
            n == 1 && self.inbox[0].unsubscribe == unsubscribe,
            "{what}: {n} requests\n{}",
            self.trace()
        );
    }
}

/// A recovering pair 7 depth stream: subscribed, live, then a gap, whose
/// recovery Unsubscribe the client has sent.
fn recovering() -> (Sim, MarketStream, HashSet<Topic>) {
    let (mut sim, mut stream) = Sim::new();
    let mut intent = HashSet::new();
    sim.live(&mut stream, &mut intent, &[DEPTH_7]);
    sim.update(7, true);
    sim.deliver(&mut stream);
    sim.one_request(true, "no recovery Unsubscribe after the gap");
    (sim, stream, intent)
}

/// The caller ends a depth stream while its recovery is under way: its
/// Unsubscribe goes out while the recovery's Unsubscribe is answered but the
/// answer not read, so the answer is superseded and starts no Subscribe.
#[tokio::test]
async fn the_caller_ending_a_recovering_stream_before_its_subscribe_ends_it() {
    let (mut sim, mut stream, mut intent) = recovering();
    sim.process();
    sim.caller(&stream, DEPTH_7, true);
    sim.deliver(&mut stream);
    sim.one_request(true, "a superseded answer started a request");
    intent.remove(&DEPTH_7);
    sim.quiesce(&mut stream);
    sim.settled(&stream, &intent);
}

/// The recovery's Subscribe is on the wire when the caller ends the stream:
/// its late OK is superseded, not applied, and the server ends up not
/// holding the stream.
#[tokio::test]
async fn the_caller_ending_a_recovering_stream_during_its_subscribe_ends_it() {
    let (mut sim, mut stream, mut intent) = recovering();
    sim.process();
    sim.deliver(&mut stream);
    sim.one_request(false, "no recovery Subscribe");
    sim.caller(&stream, DEPTH_7, true);
    sim.process();
    sim.deliver(&mut stream);
    assert!(
        !settled(stream.shared()).with_state.contains(&DEPTH_7),
        "a superseded OK made state\n{}",
        sim.trace()
    );
    intent.remove(&DEPTH_7);
    sim.quiesce(&mut stream);
    sim.settled(&stream, &intent);
}

/// The caller ends a recovering depth stream and subscribes it again while
/// the recovery's requests are on the wire, after or before the recovery's
/// Subscribe is answered: the stream ends up held, live, with the new
/// subscription's book; no cleanup Unsubscribe of the recovery's ends it.
#[tokio::test]
async fn the_caller_resubscribing_a_recovering_stream_holds_it() {
    for answered_first in [true, false] {
        let (mut sim, mut stream, intent) = recovering();
        sim.process();
        sim.deliver(&mut stream);
        sim.one_request(false, "no recovery Subscribe");
        sim.caller(&stream, DEPTH_7, true);
        if answered_first {
            sim.process();
            sim.deliver(&mut stream);
        }
        sim.caller(&stream, DEPTH_7, false);
        sim.update(7, false);
        sim.quiesce(&mut stream);
        sim.settled(&stream, &intent);
    }
}

/// Two Subscribes of a quiet stream: the first answered OK, superseded by
/// the second, answered ALREADY_SUBSCRIBED. The initial state that followed
/// the OK (the trades baseline, a quote, a depth or depth20 snapshot) arrived
/// before the stream was held and is never sent again, so the stream is
/// ended and subscribed again, and goes live only with the new
/// subscription's initial state.
#[tokio::test]
async fn a_stream_held_without_its_initial_state_is_subscribed_again() {
    for topic in [
        topic(7, Stream::Depth),
        topic(9, Stream::Depth20),
        topic(7, Stream::Bbo),
        topic(9, Stream::Trades),
    ] {
        let (mut sim, mut stream) = Sim::new();
        sim.caller(&stream, topic, false);
        sim.caller(&stream, topic, false);
        sim.process();
        sim.process();
        sim.deliver(&mut stream); // OK, superseded
        sim.deliver(&mut stream); // the initial state, before the stream is held
        sim.deliver(&mut stream); // ALREADY_SUBSCRIBED
        sim.one_request(
            true,
            "a stream held without its initial state was not subscribed again",
        );
        assert!(
            !stream.live(topic),
            "{topic} live without its initial state"
        );
        if topic.stream == Stream::Trades {
            // A trade of the held subscription, whose baseline was missed,
            // is no protocol error.
            sim.trades(topic.symbol_id);
            let trade = sim.out.pop_back().expect("a trade");
            sim.out.push_front(trade);
            sim.deliver(&mut stream);
        }
        sim.process(); // the Unsubscribe
        sim.deliver(&mut stream);
        sim.process(); // the Subscribe: OK, then the initial state
        sim.deliver(&mut stream);
        assert!(
            !stream.live(topic),
            "{topic} live before the new subscription's initial state"
        );
        sim.deliver(&mut stream);
        assert!(
            stream.live(topic),
            "{topic} not live after the new subscription's initial state\n{}",
            sim.trace()
        );
        sim.quiesce(&mut stream);
        sim.settled(&stream, &HashSet::from([topic]));
    }
}

/// The old subscription's state arrives while the stream, held without its
/// initial state, is being subscribed again: a quote, the periodic trades
/// frontier, a depth20 snapshot. The replacement subscription is not live on
/// it: only its own initial state makes it live, and the trades accounting
/// carries on across it.
#[tokio::test]
async fn a_replacement_subscription_is_not_live_on_its_predecessors_state() {
    for topic in [
        topic(7, Stream::Bbo),
        topic(9, Stream::Trades),
        topic(9, Stream::Depth20),
    ] {
        let (mut sim, mut stream) = Sim::new();
        sim.caller(&stream, topic, false);
        sim.caller(&stream, topic, false);
        sim.process();
        sim.process();
        sim.deliver(&mut stream); // OK, superseded
        sim.deliver(&mut stream); // the initial state, before the stream is held
        sim.deliver(&mut stream); // ALREADY_SUBSCRIBED: the stream is subscribed again
        // The old subscription carries on until the unsubscribe.
        sim.refresh(topic.symbol_id);
        sim.deliver(&mut stream);
        assert!(
            stream.live(topic),
            "{topic} not live on the held subscription's state\n{}",
            sim.trace()
        );
        sim.process(); // the unsubscribe
        sim.deliver(&mut stream);
        assert!(
            !stream.live(topic),
            "{topic} live once its subscription ended"
        );
        sim.process(); // the subscribe: OK, then the initial state
        sim.deliver(&mut stream);
        assert!(
            !stream.live(topic),
            "{topic} live on the previous subscription's state\n{}",
            sim.trace()
        );
        if topic.stream == Stream::Bbo {
            assert!(
                stream
                    .best_bid_offer(topic.symbol_id)
                    .is_some_and(|q| q.stale),
                "the previous subscription's quote not stale"
            );
        }
        sim.deliver(&mut stream);
        assert!(
            stream.live(topic),
            "{topic} not live on its own initial state\n{}",
            sim.trace()
        );
        if topic.stream == Stream::Trades {
            // A trade after the new baseline is delivered, with no gap.
            sim.trades(topic.symbol_id);
            let frame = sim.out.pop_back().expect("a trade");
            handle(stream.shared(), &frame).expect("the trade");
            let mut delivered = false;
            for e in stream.queued() {
                match e {
                    Event::TradesGap(g) => panic!("gap {g:?} across the resubscription"),
                    Event::Trades(t) => {
                        delivered = t.trades[0].trade_id == sim.trade[&topic.symbol_id]
                    }
                    _ => {}
                }
            }
            assert!(delivered, "the trade after the new baseline not delivered");
        }
        sim.quiesce(&mut stream);
        sim.settled(&stream, &HashSet::from([topic]));
    }
}

/// A Subscribe answered OK but superseded by an Unsubscribe, which the
/// server refuses: the server may still hold the stream, so the connection
/// ends, as for a refused unsubscribe of a held stream.
#[tokio::test]
async fn a_refused_unsubscribe_after_a_superseded_subscribe_reconnects() {
    let trades = topic(9, Stream::Trades);
    let (mut sim, mut stream) = Sim::new();
    sim.caller(&stream, trades, false);
    sim.refuse.insert(trades);
    sim.caller(&stream, trades, true);
    sim.process();
    sim.process();
    sim.deliver(&mut stream); // OK, superseded
    sim.deliver(&mut stream); // the baseline of a stream no longer wanted
    let frame = sim.out.pop_front().expect("the refusal");
    let err = handle(stream.shared(), &frame).expect_err("a refused unsubscribe applied");
    assert!(
        matches!(
            err,
            Error::MarketStream(StreamError::Unsubscribe {
                topic,
                result: SubscribeResult::LimitExceeded,
            }) if topic == trades
        ),
        "{err}"
    );
}

/// An unsubscribe of a topic never subscribed that the server answers
/// otherwise than NOT_SUBSCRIBED leaves nothing unknown: the server never
/// held it.
#[tokio::test]
async fn a_refused_unsubscribe_of_a_never_subscribed_topic_changes_nothing() {
    let trades = topic(9, Stream::Trades);
    let (mut sim, mut stream) = Sim::new();
    sim.refuse.insert(trades);
    sim.caller(&stream, trades, true);
    sim.quiesce(&mut stream);
    sim.settled(&stream, &HashSet::new());
}

/// Caller requests, answers, depth updates, gaps and stale periods in random
/// orders, seeded and reproducible: at quiescence the server holds exactly
/// what the caller wants, and every wanted stream is live, depth with the
/// server's book. POLYESTER_INTERLEAVE_SEED runs one seed,
/// POLYESTER_INTERLEAVE_SEEDS the first n.
#[tokio::test]
async fn random_interleavings_settle_on_the_callers_intent() {
    let seeds: Vec<u64> = match std::env::var("POLYESTER_INTERLEAVE_SEED") {
        Ok(seed) => vec![seed.parse().expect("a seed")],
        Err(_) => {
            let n = std::env::var("POLYESTER_INTERLEAVE_SEEDS")
                .map_or(300, |n| n.parse().expect("a seed count"));
            (1..=n).collect()
        }
    };
    for seed in seeds {
        let mut rng = Rng(seed);
        let (mut sim, mut stream) = Sim::new();
        sim.log.push(format!("seed {seed}"));
        let mut intent = HashSet::new();
        for _ in 0..200 {
            let topic = TOPICS[rng.below(TOPICS.len() as u64) as usize];
            let symbol = [7, 9][rng.below(2) as usize];
            match rng.below(100) {
                0..12 => {
                    sim.caller(&stream, topic, false);
                    intent.insert(topic);
                }
                12..20 => {
                    sim.caller(&stream, topic, true);
                    intent.remove(&topic);
                }
                20..45 => {
                    if sim.pending() > 0 {
                        sim.process();
                    }
                }
                45..75 => {
                    if !sim.out.is_empty() {
                        sim.deliver(&mut stream);
                    }
                }
                75..82 => sim.update(symbol, false),
                82..88 => sim.update(symbol, true),
                88..91 => sim.trades(symbol),
                91..93 => sim.refresh(symbol),
                93..95 => {
                    sim.log.push(format!(
                        "server will refuse the next unsubscribe of {topic}"
                    ));
                    sim.refuse.insert(topic);
                }
                95..97 => {
                    sim.log
                        .push(format!("server will refuse the next subscribe of {topic}"));
                    sim.refuse_sub.insert(topic);
                }
                _ => sim.stale(symbol),
            }
        }
        // Refusals not yet used stay out of the settling.
        sim.refuse.clear();
        sim.refuse_sub.clear();
        sim.quiesce(&mut stream);
        // A subscribe the server refused last ends the caller's intent.
        for t in &sim.refused {
            intent.remove(t);
        }
        sim.settled(&stream, &intent);
        assert!(
            sim.log.iter().any(|l| l.starts_with("server answers")),
            "seed {seed} exchanged no requests"
        );
    }
}

/// Concurrent callers, one per topic, send requests back to back while the
/// client's task applies answers, depth updates, gaps and stale periods from
/// a server answering in order. After each round, once the callers stop, the
/// server holds exactly what each caller last asked for and every wanted
/// stream is live; no connection is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_and_gaps_settle_on_the_callers_intent() {
    let mut server = FakeServer::start().await;
    let mut client = dial(&server, |c| c.queue_size = 1 << 16).await;
    let mut conn = server.accept().await;
    let shared = client.shared().clone();
    let sim = Arc::new(Mutex::new(Sim::new().0));
    let gaps = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let disconnected = Arc::new(AtomicBool::new(false));

    let server_task = {
        let (sim, gaps, stop) = (sim.clone(), gaps.clone(), stop.clone());
        tokio::spawn(async move {
            let mut rng = Rng(2);
            let mut tick = tokio::time::interval(Duration::from_micros(200));
            while !stop.load(Ordering::SeqCst) {
                tokio::select! {
                    m = conn.rx.recv() => {
                        let Some(m) = m else { return };
                        if m.ping {
                            continue;
                        }
                        let mut s = sim.lock().unwrap();
                        s.inbox.push_back(m);
                        s.process();
                    }
                    _ = tick.tick() => {
                        let mut s = sim.lock().unwrap();
                        if gaps.load(Ordering::SeqCst) {
                            for symbol in [7, 9] {
                                match rng.below(10) {
                                    0..6 => s.update(symbol, false),
                                    6..9 => s.update(symbol, true),
                                    _ => s.stale(symbol),
                                }
                            }
                        }
                    }
                }
                let out: Vec<Vec<u8>> = sim.lock().unwrap().out.drain(..).collect();
                conn.send(&out);
            }
        })
    };
    let (done, mut finished) = tokio::sync::oneshot::channel::<()>();
    let events_task = {
        let disconnected = disconnected.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    e = client.recv() => match e {
                        Some(Event::Disconnected { .. }) => disconnected.store(true, Ordering::SeqCst),
                        Some(_) => {}
                        None => break,
                    },
                    _ = &mut finished => break,
                }
            }
            client.close();
        })
    };

    let mut intent: HashSet<Topic> = HashSet::new();
    for round in 0..25u64 {
        gaps.store(true, Ordering::SeqCst);
        let callers: Vec<_> = TOPICS
            .iter()
            .enumerate()
            .map(|(i, topic)| {
                let (shared, topic) = (shared.clone(), *topic);
                tokio::spawn(async move {
                    let mut rng = Rng(round * 31 + i as u64);
                    let mut want = false;
                    for _ in 0..12 {
                        want = rng.below(3) > 0;
                        issue(&shared, &[topic], !want).expect("sent");
                        tokio::time::sleep(Duration::from_micros(rng.below(400))).await;
                    }
                    (topic, want)
                })
            })
            .collect();
        for c in callers {
            let (topic, want) = c.await.unwrap();
            if want {
                intent.insert(topic);
            } else {
                intent.remove(&topic);
            }
        }
        gaps.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let why = {
                let s = sim.lock().unwrap();
                let c = settled(&shared);
                let mut why = None;
                for t in TOPICS {
                    let want = intent.contains(&t);
                    let book = depth_book(&shared, t);
                    if s.held.contains(&t) != want {
                        why = Some(format!("server holds {t}: {}, wanted: {want}", !want));
                    } else if want
                        && t.stream == Stream::Depth
                        && book.map(|b| b.1) != Some(s.pos[&t.symbol_id])
                    {
                        why = Some(format!(
                            "{t} book {book:?}, server at {}",
                            s.pos[&t.symbol_id]
                        ));
                    } else if want && !live_of(&shared, t) {
                        why = Some(format!("{t} not live"));
                    } else if !want && t.stream == Stream::Depth && book.is_some() {
                        why = Some(format!("book kept for {t}"));
                    }
                }
                if why.is_none() && c.unanswered != 0 {
                    why = Some(format!("{} requests unanswered", c.unanswered));
                }
                why
            };
            let Some(why) = why else { break };
            assert!(
                tokio::time::Instant::now() < deadline,
                "round {round} not settled: {why}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    stop.store(true, Ordering::SeqCst);
    server_task.await.unwrap();
    let _ = done.send(());
    events_task.await.unwrap();
    assert!(
        !disconnected.load(Ordering::SeqCst),
        "a connection was lost"
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
