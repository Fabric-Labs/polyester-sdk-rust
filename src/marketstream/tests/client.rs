//! The client against a local WebSocket server that plays the stream's side.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use super::frames::*;
use super::units::{FakeHistory, span};
use crate::errors::Error;
use crate::marketstream::backfill::Pacing;
use crate::marketstream::codec;
use crate::marketstream::sbe::{
    ReadBuf, message_header_codec, ping_codec, subscribe_codec,
    subscribe_result::SubscribeResult as Wire, unsubscribe_codec,
};
use crate::marketstream::{
    Config, Event, FeedState, GapCause, MarketStream, PATH, Pairs, Side, Stream, StreamError,
    SubscribeResult, Topic, TopicResult, connect,
};

/// Pair 7 is BTC-USDC and pair 9 is known too, both at base quantity scale 3.
pub struct TestPairs;

impl Pairs for TestPairs {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32> {
        matches!(symbol_id, 7 | 9).then_some(3)
    }
    fn symbol(&self, symbol_id: u32) -> Option<String> {
        (symbol_id == 7).then(|| "BTC-USDC".to_owned())
    }
    fn base_quantity_scale_and_symbol(&self, symbol_id: u32) -> Option<(u32, Option<String>)> {
        Some((self.base_quantity_scale(symbol_id)?, self.symbol(symbol_id)))
    }
    fn symbol_id(&self, symbol: &str) -> Option<u32> {
        (symbol == "BTC-USDC").then_some(7)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClientMsg {
    pub ping: bool,
    pub unsubscribe: bool,
    pub request_id: u64,
    pub topics: Vec<Topic>,
}

pub(super) fn decode_client(frame: &[u8]) -> ClientMsg {
    let h = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0);
    let template = h.template_id();
    let mut out = ClientMsg {
        ping: false,
        unsubscribe: false,
        request_id: 0,
        topics: Vec::new(),
    };
    if template == ping_codec::SBE_TEMPLATE_ID {
        out.ping = true;
        out.request_id = ping_codec::PingDecoder::default().header(h, 0).request_id();
    } else if template == subscribe_codec::SBE_TEMPLATE_ID {
        let d = subscribe_codec::SubscribeDecoder::default().header(h, 0);
        out.request_id = d.request_id();
        let mut items = d.items_decoder();
        while items.advance().unwrap().is_some() {
            out.topics.push(Topic::new(
                items.symbol_id(),
                Stream::from_wire(items.stream()).unwrap(),
            ));
        }
    } else if template == unsubscribe_codec::SBE_TEMPLATE_ID {
        out.unsubscribe = true;
        let d = unsubscribe_codec::UnsubscribeDecoder::default().header(h, 0);
        out.request_id = d.request_id();
        let mut items = d.items_decoder();
        while items.advance().unwrap().is_some() {
            out.topics.push(Topic::new(
                items.symbol_id(),
                Stream::from_wire(items.stream()).unwrap(),
            ));
        }
    } else {
        panic!("unexpected client template {template}");
    }
    out
}

pub(super) struct ServerConn {
    tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    pub(super) rx: mpsc::UnboundedReceiver<ClientMsg>,
}

impl ServerConn {
    pub(super) fn send(&self, frames: &[Vec<u8>]) {
        for f in frames {
            let _ = self.tx.send(Some(f.clone()));
        }
    }
    pub(super) fn close(&self) {
        let _ = self.tx.send(None);
    }
    pub(super) async fn next(&mut self) -> ClientMsg {
        tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
            .await
            .expect("a client message")
            .expect("connection open")
    }
    /// The next Subscribe or Unsubscribe, skipping pings.
    pub(super) async fn request(&mut self) -> ClientMsg {
        loop {
            let m = self.next().await;
            if !m.ping {
                return m;
            }
        }
    }
    pub(super) async fn quiet(&mut self, wait: Duration) -> Option<ClientMsg> {
        tokio::time::timeout(wait, self.rx.recv())
            .await
            .ok()
            .flatten()
    }
}

pub(super) struct FakeServer {
    pub(super) url: String,
    conns: mpsc::UnboundedReceiver<ServerConn>,
    pub(super) count: Arc<AtomicUsize>,
    /// Retry-After values: each upgrade takes the next, if any, and is
    /// refused with 503 and it.
    pub(super) refuse: Arc<std::sync::Mutex<std::collections::VecDeque<&'static str>>>,
    /// When each upgrade request arrived, refused ones included.
    pub(super) upgrades: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
    /// When set, the next connection is held open and never answered.
    pub(super) hang: Arc<std::sync::atomic::AtomicBool>,
    /// Connections held unanswered.
    pub(super) hung: Arc<AtomicUsize>,
}

impl FakeServer {
    pub(super) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}{PATH}", listener.local_addr().unwrap());
        let (conn_tx, conns) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let refuse: Arc<std::sync::Mutex<std::collections::VecDeque<&'static str>>> =
            Arc::default();
        let upgrades: Arc<std::sync::Mutex<Vec<std::time::Instant>>> = Arc::default();
        let (refusals, seen) = (refuse.clone(), upgrades.clone());
        let hang = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hung = Arc::new(AtomicUsize::new(0));
        let (hanging, holding) = (hang.clone(), hung.clone());
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                if hanging.swap(false, Ordering::SeqCst) {
                    holding.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let _held = tcp;
                        std::future::pending::<()>().await;
                    });
                    continue;
                }
                let (refusals, seen) = (refusals.clone(), seen.clone());
                #[allow(clippy::result_large_err)]
                let callback = move |_: &Request, response: Response| {
                    seen.lock().unwrap().push(std::time::Instant::now());
                    match refusals.lock().unwrap().pop_front() {
                        Some(retry_after) => {
                            let mut refusal = ErrorResponse::new(Some("UNAVAILABLE".into()));
                            *refusal.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                            refusal
                                .headers_mut()
                                .insert("retry-after", HeaderValue::from_static(retry_after));
                            Err(refusal)
                        }
                        None => Ok(response),
                    }
                };
                // Anything but a WebSocket upgrade (a port probe) is dropped.
                let Ok(ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
                    continue;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let (mut write, mut read) = ws.split();
                let (tx, mut out_rx) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
                let (in_tx, rx) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    while let Some(frame) = out_rx.recv().await {
                        match frame {
                            Some(f) => {
                                if write.send(Message::binary(f)).await.is_err() {
                                    return;
                                }
                            }
                            None => {
                                let _ = write.close().await;
                                return;
                            }
                        }
                    }
                });
                tokio::spawn(async move {
                    while let Some(Ok(msg)) = read.next().await {
                        if let Message::Binary(f) = msg {
                            let _ = in_tx.send(decode_client(&f));
                        }
                    }
                });
                if conn_tx.send(ServerConn { tx, rx }).is_err() {
                    return;
                }
            }
        });
        Self {
            url,
            conns,
            count,
            refuse,
            upgrades,
            hang,
            hung,
        }
    }

    pub(super) async fn accept(&mut self) -> ServerConn {
        tokio::time::timeout(Duration::from_secs(5), self.conns.recv())
            .await
            .expect("a connection")
            .unwrap()
    }
}

fn config(server: &FakeServer) -> Config {
    let mut cfg = Config::new(server.url.clone(), Arc::new(TestPairs));
    cfg.ping_interval = Duration::from_secs(3600);
    cfg.pong_timeout = Duration::from_secs(3600);
    cfg
}

pub(super) async fn dial(server: &FakeServer, f: impl FnOnce(&mut Config)) -> MarketStream {
    let mut cfg = config(server);
    f(&mut cfg);
    connect(cfg).await.expect("connects")
}

/// Reads events until `pick` returns a value.
pub(super) async fn expect<T>(
    stream: &mut MarketStream,
    mut pick: impl FnMut(Event) -> Option<T>,
) -> T {
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.recv().await {
                Some(e) => {
                    if let Some(v) = pick(e) {
                        return v;
                    }
                }
                None => panic!("events ended: {:?}", stream.err()),
            }
        }
    })
    .await;
    found.expect("the expected event")
}

pub(super) fn respond(m: &ClientMsg, result: Wire) -> Vec<u8> {
    subscribe_response(
        m.request_id,
        &m.topics
            .iter()
            .map(|t| (t.symbol_id, t.stream, result))
            .collect::<Vec<_>>(),
    )
}

/// Runs a request on the stream while the server answers it.
async fn answered(
    conn: &mut ServerConn,
    request: impl std::future::Future<Output = crate::Result<Vec<TopicResult>>>,
    answer: impl FnOnce(&ClientMsg) -> Vec<Vec<u8>>,
) -> Vec<TopicResult> {
    let server = async {
        let m = conn.request().await;
        conn.send(&answer(&m));
    };
    let (results, _) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), request),
        server
    );
    results
        .expect("answered in time")
        .expect("request succeeds")
}

fn t(symbol_id: u32, stream: Stream) -> Topic {
    Topic::new(symbol_id, stream)
}

#[tokio::test]
async fn delivers_every_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::Connected { reconnect: false }).then_some(())
    })
    .await;

    let topics = [
        t(7, Stream::Trades),
        t(7, Stream::Bbo),
        t(7, Stream::Depth20),
        t(7, Stream::Depth),
        t(9, Stream::Trades),
    ];
    let results = answered(&mut conn, stream.subscribe(&topics), |m| {
        assert_eq!(m.topics, topics);
        let mut unknown = pong(1);
        unknown.extend([1, 2, 3]);
        vec![
            subscribe_response(
                m.request_id,
                &[
                    (7, Stream::Trades, Wire::OK),
                    (7, Stream::Bbo, Wire::OK),
                    (7, Stream::Depth20, Wire::OK),
                    (7, Stream::Depth, Wire::OK),
                    (9, Stream::Trades, Wire::UNKNOWN_SYMBOL),
                ],
            ),
            trades_through(7, 1, 100),
            with_header(&unknown, 19, 99, codec::SCHEMA_ID, codec::SCHEMA_VERSION),
            trades(7, 1, 500, &[fill(101), fill(102)]),
            bbo(7, 1, 500, (64_999_500_000_000, 2_000), (NO_PRICE, 0)),
            depth_snapshot(
                7,
                Stream::Depth20,
                1,
                500,
                &[lv(64_999_500_000_000, 2_000)],
                &[],
            ),
            depth_snapshot(
                7,
                Stream::Depth,
                1,
                500,
                &[lv(64_999_500_000_000, 2_000)],
                &[lv(65_000_500_000_000, 1)],
            ),
            depth_update(
                7,
                1,
                501,
                503,
                &[lv(64_999_000_000_000, 5)],
                &[lv(65_000_500_000_000, 0)],
            ),
            market_status(7, FeedState::Stale),
        ]
    })
    .await;
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].result, SubscribeResult::Ok);
    assert_eq!(
        (results[4].topic, results[4].result),
        (Some(t(9, Stream::Trades)), SubscribeResult::UnknownSymbol)
    );

    expect(&mut stream, |e| {
        matches!(e, Event::SubscribeResponse(_)).then_some(())
    })
    .await;
    let through = expect(&mut stream, |e| {
        if let Event::TradesThrough(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!((through.book_epoch, through.last_trade_id), (1, 100));
    let tr = expect(&mut stream, |e| {
        if let Event::Trades(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(tr.symbol.as_deref(), Some("BTC-USDC"));
    assert_eq!(
        tr.trades.iter().map(|x| x.trade_id).collect::<Vec<_>>(),
        vec![101, 102]
    );
    assert_eq!(tr.trades[0].taker_side, Side::Buy);
    assert_eq!(
        tr.trades
            .iter()
            .map(|x| x.transact_ts_ns)
            .collect::<Vec<_>>(),
        vec![tr.transact_ts_ns; 2],
        "each trade carries its book update's time"
    );
    assert_eq!(tr.transact_ts_ns, 1_700_000_000_000_000_000);
    assert_eq!(tr.trades[0].price.format(), "65000");
    assert_eq!(tr.trades[0].qty.format(None).unwrap(), "1.5");
    let quote = expect(&mut stream, |e| {
        if let Event::BestBidOffer(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert!(quote.ask.is_none());
    assert_eq!(quote.bid.as_ref().unwrap().price.format(), "64999.5");
    let s = expect(&mut stream, |e| {
        if let Event::DepthSnapshot(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!((s.stream, s.levels_per_side), (Stream::Depth20, 20));
    let s = expect(&mut stream, |e| {
        if let Event::DepthSnapshot(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!((s.stream, s.levels_per_side), (Stream::Depth, 1000));
    let u = expect(&mut stream, |e| {
        if let Event::DepthUpdate(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!((u.first_update_id, u.last_update_id), (501, 503));
    let st = expect(&mut stream, |e| {
        if let Event::MarketStatus(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(st.feed, FeedState::Stale);

    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert_eq!(
        (book.update_id, book.bids.len(), book.asks.len(), book.stale),
        (503, 2, 0, true)
    );
    assert_eq!(book.bids[1].price.format(), "64999");
    assert_eq!(stream.best_bid_offer(7).unwrap().update_id, 500);
    assert_eq!(stream.feed(7), FeedState::Stale);
}

#[tokio::test]
async fn refused_topics_are_not_resubscribed() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Bbo), t(9, Stream::Bbo)]),
        |m| {
            vec![subscribe_response(
                m.request_id,
                &[
                    (7, Stream::Bbo, Wire::OK),
                    (9, Stream::Bbo, Wire::SYMBOL_DISABLED),
                ],
            )]
        },
    )
    .await;
    conn.close();
    let mut conn = server.accept().await;
    assert_eq!(conn.request().await.topics, vec![t(7, Stream::Bbo)]);
}

#[tokio::test]
async fn heartbeat_pongs_and_times_out() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |c| {
        c.ping_interval = Duration::from_millis(20);
        c.pong_timeout = Duration::from_millis(100);
    })
    .await;
    let mut conn = server.accept().await;
    let ping = conn.next().await;
    assert!(ping.ping);
    conn.send(&[pong(ping.request_id)]);
    let p = expect(&mut stream, |e| {
        if let Event::Pong(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(p.request_id, ping.request_id);
    let err = expect(&mut stream, |e| {
        if let Event::Disconnected { error, .. } = e {
            Some(error)
        } else {
            None
        }
    })
    .await;
    assert!(err.to_string().contains("pong"), "{err}");
    server.accept().await;
}

#[tokio::test]
async fn a_consumer_that_falls_behind_fails_the_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |c| c.queue_size = 4).await;
    let mut conn = server.accept().await;
    let frames: Vec<Vec<u8>> = (1..=10).map(|i| bbo(7, 1, i, (100, 1), (101, 1))).collect();
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Bbo)]), |m| {
        [vec![respond(m, Wire::OK)], frames].concat()
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while stream.is_alive() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stream stops");
    assert!(
        matches!(stream.err(), Some(Error::QueueOverflow(_))),
        "{:?}",
        stream.err()
    );
    let mut n = 0;
    while stream.recv().await.is_some() {
        n += 1;
    }
    assert!(n <= 4, "{n} events past the bound");
}

/// Waits, without polling anything else, until the stream's task has ended.
async fn stops(stream: &MarketStream) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while stream.is_alive() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stream stops");
}

/// The answer and the stop are both ready by the time the caller looks: the
/// answer the server gave is what the subscribe returns.
#[tokio::test]
async fn an_answer_that_arrived_before_the_stream_stopped_wins() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |c| c.queue_size = 4).await;
    let mut conn = server.accept().await;
    let topics = [t(7, Stream::Bbo)];
    let topic = topics[0];
    let mut subscribe = std::pin::pin!(stream.subscribe(&topics));
    assert!(
        subscribe.as_mut().now_or_never().is_none(),
        "sent, unanswered"
    );
    let m = conn.request().await;
    let frames: Vec<Vec<u8>> = (1..=10).map(|i| bbo(7, 1, i, (100, 1), (101, 1))).collect();
    conn.send(&[vec![respond(&m, Wire::OK)], frames].concat());
    stops(&stream).await;
    let results = subscribe.await.expect("the answer wins over the stop");
    assert_eq!(
        results,
        vec![TopicResult {
            topic: Some(topic),
            symbol_id: 7,
            result: SubscribeResult::Ok,
        }]
    );
    assert!(
        matches!(stream.err(), Some(Error::QueueOverflow(_))),
        "{:?}",
        stream.err()
    );
}

/// Requests the stream stopped before answering, and requests made after it
/// stopped, report the error that stopped it.
#[tokio::test]
async fn requests_a_failed_stream_never_answered_return_its_error() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |c| c.queue_size = 4).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Bbo)]), |m| {
        vec![respond(m, Wire::OK)]
    })
    .await;
    let (wanted, unwanted) = ([t(9, Stream::Bbo)], [t(9, Stream::Depth20)]);
    let mut subscribe = std::pin::pin!(stream.subscribe(&wanted));
    let mut unsubscribe = std::pin::pin!(stream.unsubscribe(&unwanted));
    assert!(
        subscribe.as_mut().now_or_never().is_none(),
        "subscribe sent"
    );
    assert!(
        unsubscribe.as_mut().now_or_never().is_none(),
        "unsubscribe sent"
    );
    conn.request().await;
    conn.request().await;
    let frames: Vec<Vec<u8>> = (1..=10).map(|i| bbo(7, 1, i, (100, 1), (101, 1))).collect();
    conn.send(&frames);
    stops(&stream).await;
    let overflowed = |r: crate::Result<Vec<TopicResult>>| matches!(r, Err(Error::QueueOverflow(_)));
    let r = subscribe.await;
    assert!(overflowed(r.clone()), "pending subscribe: {r:?}");
    let r = unsubscribe.await;
    assert!(overflowed(r.clone()), "pending unsubscribe: {r:?}");
    let r = stream.subscribe(&[t(9, Stream::Bbo)]).await;
    assert!(overflowed(r.clone()), "subscribe after the stop: {r:?}");
    let r = stream.unsubscribe(&[t(7, Stream::Bbo)]).await;
    assert!(overflowed(r.clone()), "unsubscribe after the stop: {r:?}");
    // A close after the failure keeps the error that stopped the stream.
    stream.close();
    let r = stream.unsubscribe_symbol(7).await;
    assert!(
        overflowed(r.clone()),
        "unsubscribe_symbol after close: {r:?}"
    );
}

/// A server this client cannot read stops the stream: the requests its
/// connection carried, and requests made after, return the schema error.
#[tokio::test]
async fn requests_pending_when_another_schema_stops_the_stream_return_its_error() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    let (wanted, unwanted) = ([t(7, Stream::Bbo)], [t(9, Stream::Depth20)]);
    let mut subscribe = std::pin::pin!(stream.subscribe(&wanted));
    let mut unsubscribe = std::pin::pin!(stream.unsubscribe(&unwanted));
    assert!(
        subscribe.as_mut().now_or_never().is_none(),
        "subscribe sent"
    );
    assert!(
        unsubscribe.as_mut().now_or_never().is_none(),
        "unsubscribe sent"
    );
    conn.request().await;
    conn.request().await;
    conn.send(&[with_header(
        &pong(1),
        16,
        crate::marketstream::sbe::pong_codec::SBE_TEMPLATE_ID,
        21,
        0,
    )]);
    stops(&stream).await;
    let schema = |r: crate::Result<Vec<TopicResult>>| {
        matches!(
            r,
            Err(Error::MarketStream(StreamError::Schema {
                schema_id: 21,
                version: 0
            }))
        )
    };
    let r = subscribe.await;
    assert!(schema(r.clone()), "pending subscribe: {r:?}");
    let r = unsubscribe.await;
    assert!(schema(r.clone()), "pending unsubscribe: {r:?}");
    let r = stream.subscribe(&wanted).await;
    assert!(schema(r.clone()), "subscribe after the stop: {r:?}");
    let r = stream.unsubscribe(&unwanted).await;
    assert!(schema(r.clone()), "unsubscribe after the stop: {r:?}");
    assert!(
        matches!(
            stream.err(),
            Some(Error::MarketStream(StreamError::Schema {
                schema_id: 21,
                ..
            }))
        ),
        "{:?}",
        stream.err()
    );
}

/// A lost connection is no terminal error: the requests it carried are told
/// they apply once the stream reconnects, and the stream does.
#[tokio::test]
async fn requests_pending_when_the_connection_is_lost_apply_on_reconnect() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    let (wanted, unwanted) = ([t(7, Stream::Bbo)], [t(9, Stream::Depth20)]);
    let mut subscribe = std::pin::pin!(stream.subscribe(&wanted));
    let mut unsubscribe = std::pin::pin!(stream.unsubscribe(&unwanted));
    assert!(
        subscribe.as_mut().now_or_never().is_none(),
        "subscribe sent"
    );
    assert!(
        unsubscribe.as_mut().now_or_never().is_none(),
        "unsubscribe sent"
    );
    conn.request().await;
    conn.request().await;
    conn.close();
    let lost = |r: crate::Result<Vec<TopicResult>>| match r {
        Err(Error::Realtime { message, .. }) => {
            message.starts_with("market stream connection lost before the response")
        }
        _ => false,
    };
    let r = tokio::time::timeout(Duration::from_secs(5), subscribe)
        .await
        .expect("answered");
    assert!(lost(r.clone()), "pending subscribe: {r:?}");
    let r = tokio::time::timeout(Duration::from_secs(5), unsubscribe)
        .await
        .expect("answered");
    assert!(lost(r.clone()), "pending unsubscribe: {r:?}");
    let mut conn = server.accept().await;
    let m = conn.request().await;
    assert_eq!(m.topics, wanted, "the wanted topic is subscribed again");
    assert!(stream.is_alive());
    assert!(stream.err().is_none(), "{:?}", stream.err());
}

/// Without a terminal error, a closed stream says it is closed.
#[tokio::test]
async fn requests_a_closed_stream_never_answered_say_it_is_closed() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    let topics = [t(7, Stream::Bbo)];
    let mut subscribe = std::pin::pin!(stream.subscribe(&topics));
    assert!(
        subscribe.as_mut().now_or_never().is_none(),
        "sent, unanswered"
    );
    conn.request().await;
    stream.close();
    let closed = |r: crate::Result<Vec<TopicResult>>| match r {
        Err(Error::Realtime { message, .. }) => message == "market stream client is closed",
        _ => false,
    };
    let r = subscribe.await;
    assert!(closed(r.clone()), "pending subscribe: {r:?}");
    let r = stream.subscribe(&[t(7, Stream::Bbo)]).await;
    assert!(closed(r.clone()), "subscribe after close: {r:?}");
    assert!(stream.err().is_none(), "{:?}", stream.err());
}

#[tokio::test]
async fn another_schema_stops_the_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let conn = server.accept().await;
    conn.send(&[with_header(
        &pong(1),
        16,
        crate::marketstream::sbe::pong_codec::SBE_TEMPLATE_ID,
        21,
        0,
    )]);
    while stream.recv().await.is_some() {}
    assert!(
        matches!(
            stream.err(),
            Some(Error::MarketStream(StreamError::Schema {
                schema_id: 21,
                ..
            }))
        ),
        "{:?}",
        stream.err()
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.count.load(Ordering::SeqCst),
        1,
        "reconnected to a server it cannot read"
    );
}

#[tokio::test]
async fn a_malformed_message_reconnects() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let conn = server.accept().await;
    conn.send(&[trades(7, 1, 1, &[fill(1)])[..20].to_vec()]);
    let err = expect(&mut stream, |e| {
        if let Event::Disconnected { error, .. } = e {
            Some(error)
        } else {
            None
        }
    })
    .await;
    assert!(
        matches!(err, Error::MarketStream(StreamError::Frame { .. })),
        "{err}"
    );
    server.accept().await;
}

/// By default a lost connection is reported and the stream reconnects.
#[tokio::test]
async fn with_auto_reconnect_a_lost_connection_reconnects() {
    let mut server = FakeServer::start().await;
    assert!(config(&server).auto_reconnect, "on by default");
    let mut stream = dial(&server, |_| {}).await;
    server.accept().await.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    server.accept().await;
    assert!(stream.is_alive());
    assert!(stream.err().is_none(), "{:?}", stream.err());
}

/// With `auto_reconnect` off, a lost connection ends the stream: the loss is
/// its terminal error, which a request in flight returns too; the queued
/// events stay readable, then `recv` returns `None`; nothing reconnects.
#[tokio::test]
async fn without_auto_reconnect_a_lost_connection_ends_the_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |c| c.auto_reconnect = false).await;
    let mut conn = server.accept().await;
    let topics = [t(7, Stream::Bbo)];
    let pending = {
        let mut subscribe = std::pin::pin!(stream.subscribe(&topics));
        assert!(
            subscribe.as_mut().now_or_never().is_none(),
            "subscribe sent"
        );
        conn.request().await;
        conn.close();
        tokio::time::timeout(Duration::from_secs(5), subscribe)
            .await
            .expect("answered")
    };
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(e) = stream.recv().await {
            events.push(e);
        }
    })
    .await
    .expect("events end");
    assert!(
        matches!(events.first(), Some(Event::Connected { reconnect: false })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::Disconnected { .. })),
        "no reconnect is announced: {events:?}"
    );
    stops(&stream).await;
    let err = stream.err().expect("the loss is the terminal error");
    assert!(
        !err.to_string().contains("reconnect"),
        "the terminal error promises no reconnect: {err}"
    );
    let pending = pending.expect_err("the pending request fails");
    assert_eq!(pending.to_string(), err.to_string());
    let after = stream
        .subscribe(&[t(7, Stream::Bbo)])
        .await
        .expect_err("a stopped stream refuses requests");
    assert_eq!(after.to_string(), err.to_string());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.count.load(Ordering::SeqCst), 1, "reconnected");
}

#[tokio::test]
async fn trade_gaps_are_backfilled() {
    let mut server = FakeServer::start().await;
    let history = Arc::new(FakeHistory::new(span(1, 110), &[], |_| 110));
    let mut stream = dial(&server, |c| {
        let history: Arc<dyn crate::marketstream::TradeHistory> = history.clone();
        c.history = Some(history);
        c.pacing = Pacing {
            first: Duration::from_millis(1),
            max: Duration::from_millis(4),
        };
    })
    .await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Trades)]), |m| {
        vec![
            respond(m, Wire::OK),
            trades_through(7, 1, 100),
            trades_skipped(7, 1, 101, 104),
            trades(7, 1, 9, &[fill(105)]),
        ]
    })
    .await;
    let g = expect(&mut stream, |e| {
        if let Event::TradesGap(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(
        (g.after_trade_id, g.through_trade_id, g.cause),
        (100, 104, GapCause::Skipped)
    );
    let b = expect(&mut stream, |e| {
        if let Event::TradesBackfill(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(b.gap, g);
    assert_eq!(
        b.trades
            .unwrap()
            .iter()
            .map(|x| x.trade_id)
            .collect::<Vec<_>>(),
        span(101, 104)
    );
}

#[tokio::test]
async fn unsubscribe_ends_a_stream_and_its_state() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Trades), t(7, Stream::Bbo)]),
        |m| {
            vec![
                respond(m, Wire::OK),
                trades_through(7, 1, 100),
                bbo(7, 1, 5, (100, 1), (101, 1)),
            ]
        },
    )
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    let results = answered(&mut conn, stream.unsubscribe(&[t(7, Stream::Bbo)]), |m| {
        assert!(m.unsubscribe);
        assert_eq!(m.topics, vec![t(7, Stream::Bbo)]);
        vec![respond(m, Wire::OK)]
    })
    .await;
    assert_eq!(results[0].result, SubscribeResult::Ok);
    assert!(
        stream.best_bid_offer(7).is_none(),
        "quote kept after unsubscribe"
    );
    conn.send(&[
        bbo(7, 1, 6, (100, 1), (101, 1)),
        trades(7, 1, 9, &[fill(101)]),
    ]);
    let tr = expect(&mut stream, |e| {
        if let Event::Trades(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(tr.trades[0].trade_id, 101);
    assert!(
        stream.best_bid_offer(7).is_none(),
        "quote of an ended stream applied"
    );
    conn.close();
    let mut conn = server.accept().await;
    assert_eq!(conn.request().await.topics, vec![t(7, Stream::Trades)]);
}

#[tokio::test]
async fn unsubscribe_symbol_ends_every_stream_of_the_pair() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[
            t(7, Stream::Depth20),
            t(7, Stream::Trades),
            t(9, Stream::Bbo),
        ]),
        |m| {
            vec![
                respond(m, Wire::OK),
                depth_snapshot(7, Stream::Depth20, 1, 5, &[lv(100, 1)], &[]),
                market_status(7, FeedState::Stale),
            ]
        },
    )
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::MarketStatus(_)).then_some(())
    })
    .await;
    let results = answered(&mut conn, stream.unsubscribe_symbol(7), |m| {
        assert!(m.unsubscribe);
        assert_eq!(m.topics, vec![t(7, Stream::Depth20), t(7, Stream::Trades)]);
        vec![subscribe_response(
            m.request_id,
            &[
                (7, Stream::Depth20, Wire::OK),
                (7, Stream::Trades, Wire::NOT_SUBSCRIBED),
            ],
        )]
    })
    .await;
    assert_eq!(results[1].result, SubscribeResult::NotSubscribed);
    assert_eq!(results[1].result.to_string(), "not_subscribed");
    assert!(stream.order_book(7, Stream::Depth20, 0).is_none());
    assert_eq!(stream.feed(7), FeedState::Live);
    assert!(stream.unsubscribe_symbol(7).await.unwrap().is_empty());
}

#[tokio::test]
async fn resubscribed_trades_report_no_gap() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Trades)]), |m| {
        vec![respond(m, Wire::OK), trades_through(7, 1, 100)]
    })
    .await;
    answered(
        &mut conn,
        stream.unsubscribe(&[t(7, Stream::Trades)]),
        |m| vec![respond(m, Wire::OK)],
    )
    .await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Trades)]), |m| {
        vec![
            respond(m, Wire::OK),
            trades_through(7, 1, 200),
            trades(7, 1, 9, &[fill(201)]),
        ]
    })
    .await;
    expect(&mut stream, |e| match e {
        Event::TradesGap(g) => panic!("gap {g:?} after a deliberate resubscribe"),
        Event::Trades(x) if x.trades[0].trade_id == 201 => Some(()),
        _ => None,
    })
    .await;
}

#[tokio::test]
async fn depth_recovers_with_a_snapshot_before_live() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Depth)]), |m| {
        vec![
            respond(m, Wire::OK),
            depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
            market_status(7, FeedState::Stale),
        ]
    })
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::MarketStatus(_)).then_some(())
    })
    .await;
    assert!(stream.order_book(7, Stream::Depth, 0).unwrap().stale);
    conn.send(&[
        depth_snapshot(7, Stream::Depth, 1, 40, &[lv(101, 2)], &[]),
        market_status(7, FeedState::Live),
        depth_update(7, 1, 41, 41, &[], &[lv(102, 1)]),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    let st = expect(&mut stream, |e| {
        if let Event::MarketStatus(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(st.feed, FeedState::Live);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthUpdate(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(!book.stale && book.update_id == 41 && book.bids[0].qty.as_scaled() == 2);
}

#[tokio::test]
async fn a_subscribe_right_after_connect_is_sent_once() {
    for _ in 0..10 {
        let mut server = FakeServer::start().await;
        let stream = dial(&server, |_| {}).await;
        let mut conn = server.accept().await;
        let results = answered(&mut conn, stream.subscribe(&[t(7, Stream::Bbo)]), |m| {
            vec![respond(m, Wire::OK)]
        })
        .await;
        assert_eq!(results[0].result, SubscribeResult::Ok);
        assert_eq!(conn.quiet(Duration::from_millis(20)).await, None);
    }
}

#[tokio::test]
async fn subscribe_validates_topics() {
    let mut server = FakeServer::start().await;
    let stream = dial(&server, |_| {}).await;
    let _conn = server.accept().await;
    assert!(stream.subscribe(&[]).await.is_err());
    assert!(stream.subscribe(&[t(8, Stream::Trades)]).await.is_err());
    assert!(stream.unsubscribe(&[]).await.is_err());
}

#[tokio::test]
async fn connect_requires_a_url_and_a_reachable_server() {
    assert!(connect(Config::new("", Arc::new(TestPairs))).await.is_err());
    assert!(
        connect(Config::new(
            "ws://203.0.113.1/ws/market.sbe",
            Arc::new(TestPairs)
        ))
        .await
        .is_err(),
        "plaintext to a remote host"
    );
    assert!(
        connect(Config::new(
            "ws://127.0.0.1:1/ws/market.sbe",
            Arc::new(TestPairs)
        ))
        .await
        .is_err()
    );
}

#[tokio::test]
async fn close_ends_events_and_calls() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let _conn = server.accept().await;
    stream.close();
    while stream.recv().await.is_some() {}
    assert!(stream.subscribe(&[t(7, Stream::Bbo)]).await.is_err());
    assert!(stream.err().is_none());
}

/// `Client::market_stream` connects to the client's WebSocket host at the
/// stream's path, without credentials.
#[tokio::test]
async fn the_client_connects_to_its_environment_without_credentials() {
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let seen_tx = seen_tx.clone();
            let callback = move |req: &Request, resp: Response| {
                let _ = seen_tx.send((
                    req.uri().path().to_owned(),
                    req.headers().contains_key("authorization"),
                ));
                Ok(resp)
            };
            if let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await {
                tokio::spawn(async move { while ws.next().await.is_some() {} });
            }
        }
    });
    let client = crate::Client::new(crate::Config {
        api_url: format!("http://{addr}"),
        ws_url: format!("ws://{addr}"),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let stream = client.market_stream().await.expect("connects");
    let (path, authorization) = tokio::time::timeout(Duration::from_secs(5), seen.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(path, PATH);
    assert!(!authorization, "credentials sent to a public stream");
    stream.close();
}

/// The moment the transport is lost, every book, quote and trades stream it
/// fed is stale: no consumer sees a live-looking book from a dead connection.
#[tokio::test]
async fn transport_loss_makes_every_stream_stale_before_disconnected() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    let topics = [
        t(7, Stream::Depth),
        t(7, Stream::Depth20),
        t(7, Stream::Bbo),
        t(7, Stream::Trades),
    ];
    answered(&mut conn, stream.subscribe(&topics), |m| {
        vec![
            respond(m, Wire::OK),
            depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
            depth_snapshot(7, Stream::Depth20, 1, 10, &[lv(100, 1)], &[]),
            bbo(7, 1, 10, (100, 1), (101, 1)),
            trades_through(7, 1, 100),
        ]
    })
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::TradesThrough(_)).then_some(())
    })
    .await;
    for topic in topics {
        assert!(
            stream.live(topic),
            "{topic} not live on a healthy connection"
        );
    }

    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    // Checked as the Disconnected event is observed, before any reconnect.
    for s in [Stream::Depth, Stream::Depth20] {
        assert!(
            stream.order_book(7, s, 0).unwrap().stale,
            "{s} book not stale after the transport closed"
        );
    }
    assert!(
        stream.best_bid_offer(7).unwrap().stale,
        "quote not stale after the transport closed"
    );
    for topic in topics {
        assert!(
            !stream.live(topic),
            "{topic} live after the transport closed"
        );
    }

    // A new session: stale until its snapshot and quote arrive, and, for a
    // pair reported stale, until LIVE after them.
    let mut conn = server.accept().await;
    let resub = conn.request().await;
    conn.send(&[
        respond(&resub, Wire::OK),
        market_status(7, FeedState::Stale),
        depth_snapshot(7, Stream::Depth, 1, 20, &[lv(100, 2)], &[]),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    assert!(
        stream.order_book(7, Stream::Depth, 0).unwrap().stale,
        "book of a pair reported stale is live before LIVE"
    );
    conn.send(&[
        market_status(7, FeedState::Live),
        bbo(7, 1, 10, (100, 1), (101, 1)),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    assert!(
        !stream.order_book(7, Stream::Depth, 0).unwrap().stale && stream.live(t(7, Stream::Depth))
    );
    assert!(
        !stream.best_bid_offer(7).unwrap().stale,
        "quote stale after a fresh quote"
    );
}

/// An Unsubscribe that was sent is committed: cancelling the wait does not
/// stop a late OK from ending the stream.
#[tokio::test]
async fn unsubscribe_cancelled_after_sending_still_ends_the_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Bbo)]), |m| {
        vec![respond(m, Wire::OK), bbo(7, 1, 5, (100, 1), (101, 1))]
    })
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;

    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            stream.unsubscribe(&[t(7, Stream::Bbo)])
        )
        .await
        .is_err(),
        "answered early"
    );
    let m = conn.request().await;
    assert!(m.unsubscribe);
    conn.send(&[respond(&m, Wire::OK)]);
    let resp = expect(&mut stream, |e| {
        if let Event::SubscribeResponse(r) = e {
            Some(r)
        } else {
            None
        }
    })
    .await;
    assert_eq!(resp.request_id, m.request_id);
    assert!(
        !stream.live(t(7, Stream::Bbo)),
        "stream still held after its Unsubscribe was answered"
    );
    assert!(
        stream.best_bid_offer(7).is_none(),
        "quote kept after its Unsubscribe was answered"
    );
}

/// An Unsubscribe whose connection drops before the answer counts as done.
#[tokio::test]
async fn unsubscribe_unanswered_before_a_drop_counts_as_done() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Bbo), t(7, Stream::Trades)]),
        |m| vec![respond(m, Wire::OK), bbo(7, 1, 5, (100, 1), (101, 1))],
    )
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        stream.unsubscribe(&[t(7, Stream::Bbo)]),
    )
    .await;
    conn.request().await;
    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    assert!(
        stream.best_bid_offer(7).is_none(),
        "quote of an unsubscribed stream kept after the connection dropped"
    );
    let mut conn = server.accept().await;
    assert_eq!(conn.request().await.topics, vec![t(7, Stream::Trades)]);
}

/// A Subscribe cancelled after sending is committed too: its late OK
/// activates the stream, which the next reconnect resubscribes.
#[tokio::test]
async fn subscribe_cancelled_after_sending_still_takes_effect() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        stream.subscribe(&[t(7, Stream::Bbo)]),
    )
    .await;
    let m = conn.request().await;
    conn.send(&[respond(&m, Wire::OK), bbo(7, 1, 5, (100, 1), (101, 1))]);
    let q = expect(&mut stream, |e| {
        if let Event::BestBidOffer(q) = e {
            Some(q)
        } else {
            None
        }
    })
    .await;
    assert_eq!(q.update_id, 5);
    assert!(stream.live(t(7, Stream::Bbo)));
    conn.close();
    let mut conn = server.accept().await;
    assert_eq!(conn.request().await.topics, vec![t(7, Stream::Bbo)]);
}

/// A response to no request of this connection changes nothing.
#[tokio::test]
async fn a_response_to_no_request_changes_nothing() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let conn = server.accept().await;
    conn.send(&[
        subscribe_response(4242, &[(7, Stream::Bbo, Wire::OK)]),
        bbo(7, 1, 5, (100, 1), (101, 1)),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::SubscribeResponse(_)).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !stream.live(t(7, Stream::Bbo)),
        "an unrequested OK activated a stream"
    );
    assert!(
        stream.best_bid_offer(7).is_none(),
        "a quote of an unrequested stream applied"
    );
}

/// A depth20 book and a quote keep their newest (epoch, update) across
/// reconnects: an older state is ignored and stays stale, an equal one
/// confirms it, a higher epoch replaces it.
#[tokio::test]
async fn depth20_and_quotes_never_go_backwards_across_reconnects() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Depth20), t(7, Stream::Bbo)]),
        |m| {
            vec![
                respond(m, Wire::OK),
                depth_snapshot(7, Stream::Depth20, 1, 10, &[lv(100, 1)], &[]),
                bbo(7, 1, 10, (100, 1), (101, 1)),
            ]
        },
    )
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;

    let mut conn = server.accept().await;
    let resub = conn.request().await;
    conn.send(&[
        respond(&resub, Wire::OK),
        depth_snapshot(7, Stream::Depth20, 1, 9, &[lv(99, 1)], &[]),
        bbo(7, 1, 9, (99, 1), (101, 1)),
    ]);
    barrier(&mut stream, &conn).await;
    let book = stream.order_book(7, Stream::Depth20, 0).unwrap();
    assert!(
        book.update_id == 10 && book.stale && book.bids[0].price.as_ticks() == 100,
        "{book:?} after an older snapshot"
    );
    let q = stream.best_bid_offer(7).unwrap();
    assert!(q.update_id == 10 && q.stale, "{q:?} after an older quote");
    conn.send(&[
        depth_snapshot(7, Stream::Depth20, 1, 10, &[lv(100, 1)], &[]),
        bbo(7, 1, 10, (100, 1), (101, 1)),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth20, 0).unwrap();
    assert!(
        !book.stale && book.update_id == 10,
        "{book:?} not confirmed by an equal snapshot"
    );
    let q = stream.best_bid_offer(7).unwrap();
    assert!(
        !q.stale && q.update_id == 10,
        "{q:?} not confirmed by an equal quote"
    );
    conn.send(&[depth_snapshot(7, Stream::Depth20, 2, 3, &[lv(50, 1)], &[])]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth20, 0).unwrap();
    assert!(
        book.book_epoch == 2 && book.update_id == 3 && !book.stale,
        "{book:?} after a new epoch"
    );
}

/// Waits until every frame sent before it is applied: a status of another
/// pair, which always produces an event.
async fn barrier(stream: &mut MarketStream, conn: &ServerConn) {
    conn.send(&[market_status(9, FeedState::Live)]);
    expect(stream, |e| {
        matches!(e, Event::MarketStatus(m) if m.symbol_id == 9).then_some(())
    })
    .await;
}

/// Every depth snapshot replaces the book, older ones included, within a
/// session and across reconnects; its levels per side are the window from
/// then on.
#[tokio::test]
async fn every_depth_snapshot_replaces_the_book() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Depth)]), |m| {
        vec![
            respond(m, Wire::OK),
            depth_snapshot_window(7, Stream::Depth, 2, 10, 2, &[lv(100, 1), lv(99, 1)], &[]),
        ]
    })
    .await;
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;

    // Mid-session with no status around it: newer, older, an older epoch and
    // a newer one each replace.
    for (epoch, update) in [(2u64, 50u64), (2, 45), (1, 900), (3, 1)] {
        conn.send(&[depth_snapshot_window(
            7,
            Stream::Depth,
            epoch,
            update,
            2,
            &[lv(update as i64, 1)],
            &[],
        )]);
        let snap = expect(&mut stream, |e| {
            if let Event::DepthSnapshot(x) = e {
                Some(x)
            } else {
                None
            }
        })
        .await;
        assert_eq!((snap.book_epoch, snap.update_id), (epoch, update));
        let book = stream.order_book(7, Stream::Depth, 0).unwrap();
        assert!(
            book.book_epoch == epoch && book.update_id == update && !book.stale,
            "{book:?} after a mid-session snapshot at {epoch}/{update}"
        );
    }
    // A bridging update over a range with no levels advances the position.
    conn.send(&[
        depth_update(7, 3, 2, 20, &[], &[]),
        depth_update(7, 3, 21, 21, &[lv(2, 1)], &[]),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthUpdate(_)).then_some(())
    })
    .await;
    let u = expect(&mut stream, |e| {
        if let Event::DepthUpdate(u) = e {
            Some(u)
        } else {
            None
        }
    })
    .await;
    assert_eq!(u.first_update_id, 21);
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        book.update_id == 21 && !book.stale,
        "{book:?} after a bridging range"
    );

    // Across a reconnect: an older snapshot with a wider window replaces the
    // book and its window.
    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    let mut conn = server.accept().await;
    let resub = conn.request().await;
    conn.send(&[
        respond(&resub, Wire::OK),
        depth_snapshot_window(
            7,
            Stream::Depth,
            3,
            9,
            3,
            &[lv(10, 1), lv(9, 1), lv(8, 1)],
            &[],
        ),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        book.update_id == 9 && !book.stale && book.levels_per_side == 3 && book.bids.len() == 3,
        "{book:?} after an older, wider snapshot"
    );
    conn.send(&[depth_update(7, 3, 10, 11, &[lv(11, 1)], &[])]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthUpdate(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        book.bids.len() == 3 && book.bids[0].price.as_ticks() == 11 && book.update_id == 11,
        "{book:?} in the new window"
    );
}

/// A depth subscription made while the pair is stale gets no snapshot until
/// the pair recovers: no gap and no error; the book stays stale until the
/// snapshot, then LIVE.
#[tokio::test]
async fn depth_subscribed_while_stale_waits_for_recovery() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Depth)]), |m| {
        vec![respond(m, Wire::OK), market_status(7, FeedState::Stale)]
    })
    .await;
    barrier(&mut stream, &conn).await;
    assert!(
        stream.order_book(7, Stream::Depth, 0).is_none(),
        "a book before any snapshot"
    );
    assert!(!stream.live(t(7, Stream::Depth)));
    conn.send(&[depth_snapshot(7, Stream::Depth, 1, 40, &[lv(100, 1)], &[])]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    assert!(
        stream.order_book(7, Stream::Depth, 0).unwrap().stale,
        "book current before LIVE"
    );
    conn.send(&[
        market_status(7, FeedState::Live),
        depth_update(7, 1, 41, 41, &[lv(100, 2)], &[]),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthUpdate(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        !book.stale && book.update_id == 41 && stream.live(t(7, Stream::Depth)),
        "{book:?} after recovery"
    );
}

// A depth stream recovers from a gap on its own connection: it alone goes
// stale, is unsubscribed and subscribed again, and the new subscription's
// snapshot replaces its book.

/// Subscribes depth, trades and bbo of pair 7, delivers a healthy state for
/// each, then a depth update past a missing one.
async fn recovery_start() -> (MarketStream, FakeServer, ServerConn) {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Depth), t(7, Stream::Trades), t(7, Stream::Bbo)]),
        |m| {
            vec![
                respond(m, Wire::OK),
                depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
                trades_through(7, 1, 100),
                bbo(7, 1, 10, (100, 1), (101, 1)),
                depth_update(7, 1, 11, 11, &[lv(100, 2)], &[]),
                trades(7, 1, 11, &[fill(101)]),
            ]
        },
    )
    .await;
    expect(&mut stream, |e| matches!(e, Event::Trades(_)).then_some(())).await;
    conn.send(&[depth_update(7, 1, 13, 13, &[lv(100, 3)], &[])]); // 12 is missing
    (stream, server, conn)
}

/// The client's next Subscribe or Unsubscribe; it must be for pair 7's depth
/// stream alone.
async fn next_request(conn: &mut ServerConn, unsubscribe: bool) -> ClientMsg {
    let m = conn.request().await;
    assert!(
        m.unsubscribe == unsubscribe && m.topics == vec![t(7, Stream::Depth)],
        "client sent {m:?}, want an {} of pair 7's depth alone",
        if unsubscribe {
            "Unsubscribe"
        } else {
            "Subscribe"
        }
    );
    m
}

async fn no_request(conn: &mut ServerConn) {
    while let Some(m) = conn.quiet(Duration::from_millis(30)).await {
        assert!(m.ping, "client sent {m:?}");
    }
}

fn no_disconnect(stream: &mut MarketStream) {
    for e in stream.queued() {
        if let Event::Disconnected { error, .. } = e {
            panic!("disconnected by {error}");
        }
    }
}

/// A gap on the depth stream recovers that stream alone: trades and bbo keep
/// their state and stay live, updates and old snapshots arriving while it
/// recovers are dropped, a second gap starts nothing more, and the new
/// snapshot makes the book current again.
#[tokio::test]
async fn a_depth_gap_recovers_only_that_stream() {
    let (mut stream, server, mut conn) = recovery_start().await;
    let unsub = next_request(&mut conn, true).await;
    barrier(&mut stream, &conn).await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        book.stale && book.update_id == 11,
        "{book:?} during recovery"
    );
    assert!(!stream.live(t(7, Stream::Depth)));
    assert!(
        stream.live(t(7, Stream::Trades)) && stream.live(t(7, Stream::Bbo)),
        "other streams not live while depth recovers"
    );
    let q = stream.best_bid_offer(7).unwrap();
    assert!(!q.stale && q.update_id == 10);

    conn.send(&[
        depth_update(7, 1, 14, 14, &[lv(100, 9)], &[]),
        depth_snapshot(7, Stream::Depth, 1, 12, &[lv(99, 9)], &[]),
        depth_update(7, 1, 40, 40, &[], &[]),
        trades(7, 1, 12, &[fill(102)]),
    ]);
    let tr = expect(&mut stream, |e| {
        if let Event::Trades(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(tr.trades[0].trade_id, 102);
    no_request(&mut conn).await;

    conn.send(&[
        respond(&unsub, Wire::OK),
        depth_update(7, 1, 15, 15, &[lv(100, 8)], &[]),
    ]);
    let sub = next_request(&mut conn, false).await;
    barrier(&mut stream, &conn).await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        book.stale && book.update_id == 11 && book.bids[0].qty.as_scaled() == 2,
        "{book:?} changed by updates during recovery"
    );

    conn.send(&[
        respond(&sub, Wire::OK),
        depth_snapshot(7, Stream::Depth, 1, 30, &[lv(100, 5)], &[]),
        depth_update(7, 1, 31, 31, &[lv(100, 6)], &[]),
    ]);
    let u = expect(&mut stream, |e| {
        if let Event::DepthUpdate(u) = e {
            Some(u)
        } else {
            None
        }
    })
    .await;
    assert_eq!(u.first_update_id, 31);
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        !book.stale
            && book.update_id == 31
            && book.bids[0].qty.as_scaled() == 6
            && stream.live(t(7, Stream::Depth)),
        "{book:?} after recovery"
    );
    no_disconnect(&mut stream);
    assert_eq!(
        server.count.load(Ordering::SeqCst),
        1,
        "the recovery reconnected"
    );
}

/// NOT_SUBSCRIBED answers the recovery's Unsubscribe when the server no
/// longer held the stream: the recovery goes on with the Subscribe.
#[tokio::test]
async fn a_recovery_unsubscribe_answered_not_subscribed_goes_on() {
    let (mut stream, _server, mut conn) = recovery_start().await;
    let unsub = next_request(&mut conn, true).await;
    conn.send(&[respond(&unsub, Wire::NOT_SUBSCRIBED)]);
    let sub = next_request(&mut conn, false).await;
    conn.send(&[
        respond(&sub, Wire::OK),
        depth_snapshot(7, Stream::Depth, 1, 30, &[lv(100, 5)], &[]),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::DepthSnapshot(_)).then_some(())
    })
    .await;
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(
        !book.stale && book.update_id == 30,
        "{book:?} after recovery"
    );
    no_disconnect(&mut stream);
}

/// An Unsubscribe answered otherwise leaves the stream in a state the client
/// cannot know: the connection ends and every stream is resubscribed.
#[tokio::test]
async fn a_refused_recovery_unsubscribe_reconnects() {
    let (mut stream, mut server, mut conn) = recovery_start().await;
    let unsub = next_request(&mut conn, true).await;
    conn.send(&[respond(&unsub, Wire::LIMIT_EXCEEDED)]);
    let err = expect(&mut stream, |e| {
        if let Event::Disconnected { error, .. } = e {
            Some(error)
        } else {
            None
        }
    })
    .await;
    assert!(
        matches!(
            err,
            Error::MarketStream(StreamError::Unsubscribe { topic, .. })
                if topic == Topic::new(7, Stream::Depth)
        ),
        "{err}"
    );
    let mut conn = server.accept().await;
    assert_eq!(conn.request().await.topics.len(), 3);
}

/// A lost transport still reconnects everything, a recovery in progress
/// included: every wanted stream is resubscribed and rebuilt.
#[tokio::test]
async fn reconnect_during_a_recovery_restores_every_stream() {
    let (mut stream, mut server, mut conn) = recovery_start().await;
    next_request(&mut conn, true).await;
    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    let mut conn = server.accept().await;
    let resub = conn.request().await;
    assert_eq!(resub.topics.len(), 3);
    conn.send(&[
        respond(&resub, Wire::OK),
        depth_snapshot(7, Stream::Depth, 1, 50, &[lv(100, 7)], &[]),
        trades_through(7, 1, 101),
        bbo(7, 1, 50, (100, 1), (101, 1)),
    ]);
    expect(&mut stream, |e| {
        matches!(e, Event::BestBidOffer(_)).then_some(())
    })
    .await;
    for topic in [t(7, Stream::Depth), t(7, Stream::Trades), t(7, Stream::Bbo)] {
        assert!(stream.live(topic), "{topic} not live after the reconnect");
    }
    assert_eq!(
        stream.order_book(7, Stream::Depth, 0).unwrap().update_id,
        50
    );
    conn.send(&[depth_update(7, 1, 51, 51, &[], &[])]);
    let u = expect(&mut stream, |e| {
        if let Event::DepthUpdate(u) = e {
            Some(u)
        } else {
            None
        }
    })
    .await;
    assert_eq!(u.last_update_id, 51);
}

/// A stale pair reported live before its depth book's replacement snapshot,
/// and an update while stale, are gaps of the depth stream alone.
#[tokio::test]
async fn depth_recovery_out_of_order_recovers_the_stream() {
    for frames in [
        vec![
            market_status(7, FeedState::Stale),
            market_status(7, FeedState::Live),
        ],
        vec![
            market_status(7, FeedState::Stale),
            depth_update(7, 1, 11, 11, &[], &[]),
        ],
    ] {
        let mut server = FakeServer::start().await;
        let mut stream = dial(&server, |_| {}).await;
        let mut conn = server.accept().await;
        answered(
            &mut conn,
            stream.subscribe(&[t(7, Stream::Depth), t(7, Stream::Bbo)]),
            |m| {
                vec![
                    respond(m, Wire::OK),
                    depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
                ]
            },
        )
        .await;
        conn.send(&frames);
        let unsub = next_request(&mut conn, true).await;
        conn.send(&[respond(&unsub, Wire::OK)]);
        let sub = next_request(&mut conn, false).await;
        conn.send(&[
            respond(&sub, Wire::OK),
            depth_snapshot(7, Stream::Depth, 1, 20, &[lv(100, 2)], &[]),
            market_status(7, FeedState::Live),
        ]);
        expect(&mut stream, |e| {
            matches!(e, Event::DepthSnapshot(x) if x.update_id == 20).then_some(())
        })
        .await;
        barrier(&mut stream, &conn).await;
        let book = stream.order_book(7, Stream::Depth, 0).unwrap();
        assert!(
            !book.stale && book.update_id == 20,
            "{book:?} after recovery"
        );
        no_disconnect(&mut stream);
    }
}

/// An update before any snapshot of a connection's depth subscription is a
/// gap: the stream recovers on that connection and is not live meanwhile.
#[tokio::test]
async fn a_depth_update_before_a_snapshot_recovers_the_stream() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(&mut conn, stream.subscribe(&[t(7, Stream::Depth)]), |m| {
        vec![respond(m, Wire::OK), depth_update(7, 1, 10, 10, &[], &[])]
    })
    .await;
    next_request(&mut conn, true).await;
    assert!(!stream.live(t(7, Stream::Depth)));
    no_disconnect(&mut stream);
}

/// The caller's Unsubscribe while a stream recovers ends it: the recovery
/// does not subscribe it again.
#[tokio::test]
async fn unsubscribe_during_a_recovery_ends_the_stream() {
    let (stream, _server, mut conn) = recovery_start().await;
    let recovery_unsub = next_request(&mut conn, true).await;
    let topics = [t(7, Stream::Depth)];
    let user = async {
        let m = next_request(&mut conn, true).await;
        conn.send(&[
            respond(&recovery_unsub, Wire::OK),
            respond(&m, Wire::NOT_SUBSCRIBED),
        ]);
    };
    let (results, _) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), stream.unsubscribe(&topics)),
        user
    );
    let results = results.expect("answered").expect("unsubscribed");
    assert_eq!(results[0].result, SubscribeResult::NotSubscribed);
    no_request(&mut conn).await;
    assert!(
        stream.order_book(7, Stream::Depth, 0).is_none(),
        "book kept after the caller's Unsubscribe"
    );
}

/// The connection is lost; the next upgrade is refused with 503 and
/// `retry_after`. Returns the refusal's error and wait.
async fn ms_refused_after_loss(
    retry_after: &'static str,
) -> (FakeServer, MarketStream, Error, Duration) {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::Connected { .. }).then_some(())
    })
    .await;
    server.refuse.lock().unwrap().push_back(retry_after);
    conn.close();
    let lost = expect(&mut stream, |e| match e {
        Event::Disconnected { error, .. } => Some(error),
        _ => None,
    })
    .await;
    assert!(lost.answer().is_none(), "a lost connection has no answer");
    let (error, delay) = expect(&mut stream, |e| match e {
        Event::Disconnected { error, delay } => Some((error, delay)),
        _ => None,
    })
    .await;
    (server, stream, error, delay)
}

#[tokio::test]
async fn a_503_upgrade_waits_its_retry_after_of_30_seconds() {
    let (server, stream, error, delay) = ms_refused_after_loss("30").await;
    let answer = error.answer().expect("the refusal's answer");
    assert_eq!((answer.status, answer.retry_after), (503, Some(30.0)));
    assert_eq!(delay, Duration::from_secs(30));
    let before = server.upgrades.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(server.upgrades.lock().unwrap().len(), before);
    stream.close();
}

#[tokio::test]
async fn a_503_upgrade_waits_its_retry_after_of_400_milliseconds() {
    let (mut server, mut stream, error, delay) = ms_refused_after_loss("0.4").await;
    assert_eq!(error.retry_after(), Some(0.4));
    assert!(delay >= Duration::from_millis(400), "{delay:?}");
    let _conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::Connected { reconnect: true }).then_some(())
    })
    .await;
    let upgrades = server.upgrades.lock().unwrap().clone();
    let gap = upgrades[2] - upgrades[1];
    assert!(
        gap >= Duration::from_millis(400),
        "reconnected {gap:?} after a 0.4 s Retry-After"
    );
}

/// Loses the connection and has the next upgrade refused with 503 and a
/// short Retry-After; returns the refusal's wait.
async fn lost_then_refused(server: &mut FakeServer, stream: &mut MarketStream) -> Duration {
    let conn = server.accept().await;
    expect(stream, |e| {
        matches!(e, Event::Connected { .. }).then_some(())
    })
    .await;
    server.refuse.lock().unwrap().push_back("0.2");
    conn.close();
    expect(stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    expect(stream, |e| match e {
        Event::Disconnected { delay, .. } => Some(delay),
        _ => None,
    })
    .await
}

/// Closing while the reconnect timer is already due starts no upgrade: the
/// stream is held between polls until the timer has elapsed, then closed and
/// resumed with both the timer and the close ready, over and over.
#[tokio::test(flavor = "current_thread")]
async fn a_close_with_the_retry_timer_already_due_starts_no_upgrade() {
    for round in 0..16 {
        let mut server = FakeServer::start().await;
        let gate = Arc::new(crate::marketstream::client::SchedGate::default());
        let held = gate.clone();
        let mut stream = dial(&server, |c| c.gate = Some(held)).await;
        let delay = lost_then_refused(&mut server, &mut stream).await;
        gate.hold();
        tokio::time::sleep(delay + Duration::from_millis(100)).await;
        stream.close();
        let upgrades = server.upgrades.lock().unwrap().len();
        gate.release();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            server.upgrades.lock().unwrap().len(),
            upgrades,
            "round {round}: an upgrade after close"
        );
        assert_eq!(server.count.load(Ordering::SeqCst), 1, "round {round}");
    }
}

/// Closing while an upgrade is in flight ends it at once, not at the connect
/// timeout.
#[tokio::test]
async fn a_close_during_an_upgrade_ends_it() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::Connected { .. }).then_some(())
    })
    .await;
    server.hang.store(true, Ordering::SeqCst);
    conn.close();
    expect(&mut stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.hung.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the reconnect's upgrade is in flight");
    stream.close();
    tokio::time::timeout(Duration::from_secs(2), async {
        while stream.is_alive() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the close ends the upgrade in flight");
}
