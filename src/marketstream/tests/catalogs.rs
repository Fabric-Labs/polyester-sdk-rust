//! The pairs are re-read on every connection before anything is subscribed
//! on it: a reset that renumbers pairs between two connections never maps a
//! new pair ID to an old symbol or scale.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Semaphore;

use super::client::{FakeServer, expect, respond};
use super::frames::bbo;
use crate::errors::{Error, Result};
use crate::marketstream::client::{generation, install_pairs, offline};
use crate::marketstream::sbe::subscribe_result::SubscribeResult as Wire;
use crate::marketstream::{
    Config, Event, LoadedPairs, MarketStream, Pairs, PairsSource, Renumbered, Stream, StreamError,
    Topic, connect,
};

/// A spot config: each pair's ID, symbol and base quantity scale.
struct Table(HashMap<u32, (&'static str, u32)>);

impl Table {
    fn of(pairs: &[(u32, &'static str, u32)]) -> Arc<dyn Pairs> {
        Arc::new(Table(
            pairs
                .iter()
                .map(|(id, s, scale)| (*id, (*s, *scale)))
                .collect(),
        ))
    }
}

impl Pairs for Table {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32> {
        self.0.get(&symbol_id).map(|(_, scale)| *scale)
    }
    fn symbol(&self, symbol_id: u32) -> Option<String> {
        self.0.get(&symbol_id).map(|(s, _)| (*s).to_owned())
    }
    fn base_quantity_scale_and_symbol(&self, symbol_id: u32) -> Option<(u32, Option<String>)> {
        self.0
            .get(&symbol_id)
            .map(|(s, scale)| (*scale, Some((*s).to_owned())))
    }
    fn symbol_id(&self, symbol: &str) -> Option<u32> {
        self.0
            .iter()
            .find(|(_, (s, _))| *s == symbol)
            .map(|(id, _)| *id)
    }
}

/// The spot config before the reset, and after it: BTC-USDC moves from 7 to
/// 12, and 7 becomes ETH-USDC at another scale.
fn before() -> Arc<dyn Pairs> {
    Table::of(&[(7, "BTC-USDC", 3), (9, "ETH-USDC", 4)])
}

fn after() -> Arc<dyn Pairs> {
    Table::of(&[(12, "BTC-USDC", 3), (7, "ETH-USDC", 4)])
}

/// Answers each load in turn, the last one again once the script runs out;
/// each load waits for a permit while `gated`.
struct Scripted {
    answers: Mutex<VecDeque<Result<Arc<dyn Pairs>>>>,
    last: Mutex<Option<Arc<dyn Pairs>>>,
    loads: Mutex<usize>,
    gate: Semaphore,
    gated: bool,
}

impl Scripted {
    fn new(answers: Vec<Result<Arc<dyn Pairs>>>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into()),
            last: Mutex::new(None),
            loads: Mutex::new(0),
            gate: Semaphore::new(0),
            gated: false,
        })
    }

    fn gated(answers: Vec<Result<Arc<dyn Pairs>>>) -> Arc<Self> {
        let mut s = Arc::try_unwrap(Self::new(answers)).ok().unwrap();
        s.gated = true;
        Arc::new(s)
    }

    fn loads(&self) -> usize {
        *self.loads.lock().unwrap()
    }
}

impl PairsSource for Scripted {
    fn load(&self) -> BoxFuture<'_, Result<LoadedPairs>> {
        Box::pin(async move {
            *self.loads.lock().unwrap() += 1;
            if self.gated {
                self.gate.acquire().await.unwrap().forget();
            }
            let next = self.answers.lock().unwrap().pop_front();
            match next {
                Some(Ok(pairs)) => {
                    *self.last.lock().unwrap() = Some(pairs.clone());
                    Ok(LoadedPairs::new(pairs))
                }
                Some(Err(e)) => Err(e),
                None => Ok(LoadedPairs::new(
                    self.last.lock().unwrap().clone().expect("a pairs answer"),
                )),
            }
        })
    }
}

fn failure() -> Error {
    Error::transport("spot config unavailable")
}

fn t(symbol_id: u32, stream: Stream) -> Topic {
    Topic::new(symbol_id, stream)
}

async fn dial(server: &FakeServer, source: Arc<Scripted>) -> MarketStream {
    let mut cfg = Config::new(server.url.clone(), before());
    cfg.reload = Some(source);
    cfg.ping_interval = Duration::from_secs(3600);
    cfg.pong_timeout = Duration::from_secs(3600);
    connect(cfg).await.expect("connects")
}

/// Subscribes `topics` on the first connection and closes it.
async fn subscribe_then_lose(server: &mut FakeServer, stream: &mut MarketStream, topics: &[Topic]) {
    let mut conn = server.accept().await;
    let (results, _) = tokio::join!(stream.subscribe(topics), async {
        let m = conn.request().await;
        conn.send(&[respond(&m, Wire::OK)]);
    });
    assert!(results.unwrap().iter().all(|r| r.result.subscribed()));
    conn.send(&[bbo(
        7,
        1,
        1,
        (95_000_000_000_000, 1_500),
        (95_100_000_000_000, 2_000),
    )]);
    expect(stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    conn.close();
    expect(stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
}

#[tokio::test]
async fn connect_reads_the_pairs_before_it_returns() {
    let mut server = FakeServer::start().await;
    let source = Scripted::new(vec![Ok(after())]);
    let mut stream = dial(&server, source.clone()).await;
    assert_eq!(source.loads(), 1);
    let mut conn = server.accept().await;
    // Pair 12 exists only in the config read at connect.
    let topics = [t(12, Stream::Bbo)];
    let (results, _) = tokio::join!(stream.subscribe(&topics), async {
        let m = conn.request().await;
        assert_eq!(m.topics, [t(12, Stream::Bbo)]);
        conn.send(&[respond(&m, Wire::OK)]);
    });
    assert!(results.unwrap()[0].result.subscribed());
    conn.send(&[bbo(
        12,
        1,
        1,
        (95_000_000_000_000, 1_500),
        (95_100_000_000_000, 2_000),
    )]);
    let quote = expect(&mut stream, |e| match e {
        Event::BestBidOffer(q) => Some(q),
        _ => None,
    })
    .await;
    assert_eq!(quote.symbol.as_deref(), Some("BTC-USDC"));
}

#[tokio::test]
async fn a_failed_first_read_fails_connect() {
    let server = FakeServer::start().await;
    let mut cfg = Config::new(server.url.clone(), before());
    cfg.reload = Some(Scripted::new(vec![Err(failure())]));
    assert!(connect(cfg).await.is_err());
}

#[tokio::test]
async fn a_reset_that_renumbers_pairs_resubscribes_with_the_new_ids() {
    let mut server = FakeServer::start().await;
    let source = Scripted::new(vec![Ok(before()), Ok(after())]);
    let mut stream = dial(&server, source.clone()).await;
    subscribe_then_lose(&mut server, &mut stream, &[t(7, Stream::Bbo)]).await;

    let mut conn = server.accept().await;
    let renumbered = expect(&mut stream, |e| match e {
        Event::PairsReloaded { renumbered } => Some(renumbered),
        _ => None,
    })
    .await;
    assert_eq!(
        renumbered,
        [Renumbered {
            symbol: "BTC-USDC".into(),
            from: 7,
            to: 12
        }]
    );
    let m = conn.request().await;
    assert_eq!(
        m.topics,
        [t(12, Stream::Bbo)],
        "BTC-USDC is subscribed by its new ID, never by its old one"
    );
    conn.send(&[
        respond(&m, Wire::OK),
        bbo(
            12,
            2,
            1,
            (96_000_000_000_000, 2_500),
            (96_100_000_000_000, 3_000),
        ),
    ]);
    let quote = expect(&mut stream, |e| match e {
        Event::BestBidOffer(q) => Some(q),
        _ => None,
    })
    .await;
    assert_eq!(
        (quote.symbol_id, quote.symbol.as_deref()),
        (12, Some("BTC-USDC"))
    );
    assert_eq!(quote.bid.unwrap().qty.scale(), Some(3));
    assert!(stream.live(t(12, Stream::Bbo)));
    assert!(!stream.live(t(7, Stream::Bbo)));
    assert_eq!(source.loads(), 2);
}

#[tokio::test]
async fn a_renumbered_pair_keeps_nothing_of_its_old_id() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, Scripted::new(vec![Ok(before()), Ok(after())])).await;
    subscribe_then_lose(&mut server, &mut stream, &[t(7, Stream::Bbo)]).await;
    let _conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::PairsReloaded { .. }).then_some(())
    })
    .await;
    // Pair 7 is ETH-USDC now: BTC-USDC's last quote is not served under it,
    // at either pair's scale.
    assert_eq!(stream.best_bid_offer(7), None);
}

#[tokio::test]
async fn nothing_is_subscribed_before_the_pairs_are_read() {
    let mut server = FakeServer::start().await;
    let source = Scripted::gated(vec![Ok(before()), Ok(after())]);
    source.gate.add_permits(1); // the read at connect
    let mut stream = dial(&server, source.clone()).await;
    subscribe_then_lose(&mut server, &mut stream, &[t(7, Stream::Bbo)]).await;

    let mut conn = server.accept().await;
    assert_eq!(
        conn.quiet(Duration::from_millis(300)).await,
        None,
        "nothing is sent while the pairs are read"
    );
    assert!(!stream.live(t(7, Stream::Bbo)) && !stream.live(t(12, Stream::Bbo)));
    // A caller's subscribe meanwhile is held too: it applies with the new
    // pairs.
    assert!(stream.subscribe(&[t(9, Stream::Bbo)]).await.is_err());
    assert_eq!(conn.quiet(Duration::from_millis(100)).await, None);
    source.gate.add_permits(1);
    let m = conn.request().await;
    let mut topics = m.topics.clone();
    topics.sort();
    assert_eq!(
        topics,
        [t(7, Stream::Bbo), t(12, Stream::Bbo)],
        "ETH-USDC, asked as 9 before the read, follows to 7, and BTC-USDC to 12"
    );
}

#[tokio::test]
async fn a_failed_read_stays_paused_and_retries() {
    let mut server = FakeServer::start().await;
    let source = Scripted::new(vec![
        Ok(before()),
        Err(failure()),
        Err(failure()),
        Ok(after()),
    ]);
    let mut stream = dial(&server, source.clone()).await;
    subscribe_then_lose(&mut server, &mut stream, &[t(7, Stream::Bbo)]).await;

    let mut conn = server.accept().await;
    for _ in 0..2 {
        expect(&mut stream, |e| {
            matches!(e, Event::PairsReloadFailed { .. }).then_some(())
        })
        .await;
        assert!(!stream.live(t(7, Stream::Bbo)));
    }
    let m = conn.request().await;
    assert_eq!(m.topics, [t(12, Stream::Bbo)]);
    assert_eq!(source.loads(), 4);
}

#[tokio::test]
async fn a_pair_gone_after_the_read_ends_its_stream() {
    let mut server = FakeServer::start().await;
    let gone = Table::of(&[(9, "ETH-USDC", 4)]);
    let mut stream = dial(&server, Scripted::new(vec![Ok(before()), Ok(gone)])).await;
    subscribe_then_lose(
        &mut server,
        &mut stream,
        &[t(7, Stream::Bbo), t(9, Stream::Trades)],
    )
    .await;

    let mut conn = server.accept().await;
    let (topic, error) = expect(&mut stream, |e| match e {
        Event::Ended { topic, error } => Some((topic, error)),
        _ => None,
    })
    .await;
    assert_eq!(topic, t(7, Stream::Bbo));
    assert!(matches!(
        error,
        Error::MarketStream(StreamError::PairNotListed { topic: ended, ref symbol })
            if ended == t(7, Stream::Bbo) && symbol.as_deref() == Some("BTC-USDC")
    ));
    let m = conn.request().await;
    assert_eq!(m.topics, [t(9, Stream::Trades)]);
}

#[tokio::test]
async fn a_superseded_read_installs_nothing() {
    let mut cfg = Config::new("ws://127.0.0.1/ws/market.sbe", before());
    cfg.reload = Some(Scripted::new(vec![]));
    let (shared, _events) = offline(cfg);
    let first = generation(&shared);
    // A new connection supersedes the first before its read lands.
    let _wire = crate::marketstream::client::reconnect(&shared);
    assert!(generation(&shared) > first);
    assert!(!install_pairs(&shared, first, after()));
    assert!(install_pairs(&shared, generation(&shared), after()));
}

#[tokio::test]
async fn every_reconnect_voids_prices_before_it_sends_anything() {
    let mut server = FakeServer::start().await;
    let source = Scripted::gated(vec![Ok(before()), Ok(before())]);
    source.gate.add_permits(1); // the read at connect
    let mut stream = dial(&server, source.clone()).await;
    subscribe_then_lose(&mut server, &mut stream, &[t(7, Stream::Bbo)]).await;

    let mut conn = server.accept().await;
    // The pair is unchanged, but until its pairs are read again the new
    // connection sends nothing and the old quote is void.
    assert_eq!(conn.quiet(Duration::from_millis(200)).await, None);
    assert!(stream.best_bid_offer(7).is_none_or(|q| q.stale));
    assert!(!stream.live(t(7, Stream::Bbo)));
    source.gate.add_permits(1);
    let renumbered = expect(&mut stream, |e| match e {
        Event::PairsReloaded { renumbered } => Some(renumbered),
        _ => None,
    })
    .await;
    assert!(renumbered.is_empty());
    let m = conn.request().await;
    assert_eq!(m.topics, [t(7, Stream::Bbo)]);
}
