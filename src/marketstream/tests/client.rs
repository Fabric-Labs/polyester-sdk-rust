//! The client against a local WebSocket server that plays the stream's side.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientMsg {
    ping: bool,
    unsubscribe: bool,
    request_id: u64,
    topics: Vec<Topic>,
}

fn decode_client(frame: &[u8]) -> ClientMsg {
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

struct ServerConn {
    tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    rx: mpsc::UnboundedReceiver<ClientMsg>,
}

impl ServerConn {
    fn send(&self, frames: &[Vec<u8>]) {
        for f in frames {
            let _ = self.tx.send(Some(f.clone()));
        }
    }
    fn close(&self) {
        let _ = self.tx.send(None);
    }
    async fn next(&mut self) -> ClientMsg {
        tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
            .await
            .expect("a client message")
            .expect("connection open")
    }
    /// The next Subscribe or Unsubscribe, skipping pings.
    async fn request(&mut self) -> ClientMsg {
        loop {
            let m = self.next().await;
            if !m.ping {
                return m;
            }
        }
    }
    async fn quiet(&mut self, wait: Duration) -> Option<ClientMsg> {
        tokio::time::timeout(wait, self.rx.recv())
            .await
            .ok()
            .flatten()
    }
}

struct FakeServer {
    url: String,
    conns: mpsc::UnboundedReceiver<ServerConn>,
    count: Arc<AtomicUsize>,
}

impl FakeServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}{PATH}", listener.local_addr().unwrap());
        let (conn_tx, conns) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                // Anything but a WebSocket upgrade (a port probe) is dropped.
                let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
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
        Self { url, conns, count }
    }

    async fn accept(&mut self) -> ServerConn {
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

async fn dial(server: &FakeServer, f: impl FnOnce(&mut Config)) -> MarketStream {
    let mut cfg = config(server);
    f(&mut cfg);
    connect(cfg).await.expect("connects")
}

/// Reads events until `pick` returns a value.
async fn expect<T>(stream: &mut MarketStream, mut pick: impl FnMut(Event) -> Option<T>) -> T {
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

fn respond(m: &ClientMsg, result: Wire) -> Vec<u8> {
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
async fn rebuilds_the_book_after_a_gap_and_reconciles_trades() {
    let mut server = FakeServer::start().await;
    let mut stream = dial(&server, |_| {}).await;
    let mut conn = server.accept().await;
    answered(
        &mut conn,
        stream.subscribe(&[t(7, Stream::Depth), t(7, Stream::Trades)]),
        |m| {
            vec![
                respond(m, Wire::OK),
                depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
                trades_through(7, 1, 100),
                depth_update(7, 1, 11, 11, &[lv(100, 2)], &[]),
                trades(7, 1, 11, &[fill(101)]),
                depth_update(7, 1, 13, 13, &[lv(100, 3)], &[]),
            ]
        },
    )
    .await;
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
            Error::MarketStream(StreamError::BookGap { symbol_id: 7, .. })
        ),
        "{err}"
    );
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(book.stale && book.update_id == 11);

    let mut conn = server.accept().await;
    expect(&mut stream, |e| {
        matches!(e, Event::Connected { reconnect: true }).then_some(())
    })
    .await;
    let resub = conn.request().await;
    assert_eq!(
        resub.topics,
        vec![t(7, Stream::Depth), t(7, Stream::Trades)]
    );
    conn.send(&[
        respond(&resub, Wire::OK),
        depth_snapshot(7, Stream::Depth, 1, 20, &[lv(100, 9)], &[]),
        trades_through(7, 1, 105),
    ]);
    let s = expect(&mut stream, |e| {
        if let Event::DepthSnapshot(x) = e {
            Some(x)
        } else {
            None
        }
    })
    .await;
    assert_eq!(s.update_id, 20);
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
        (101, 105, GapCause::Reconnect)
    );
    let book = stream.order_book(7, Stream::Depth, 0).unwrap();
    assert!(!book.stale && book.update_id == 20 && book.bids[0].qty.as_scaled() == 9);
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
async fn depth_recovery_out_of_order_is_recovered() {
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
        answered(&mut conn, stream.subscribe(&[t(7, Stream::Depth)]), |m| {
            vec![
                respond(m, Wire::OK),
                depth_snapshot(7, Stream::Depth, 1, 10, &[lv(100, 1)], &[]),
            ]
        })
        .await;
        conn.send(&frames);
        let err = expect(&mut stream, |e| {
            if let Event::Disconnected { error, .. } = e {
                Some(error)
            } else {
                None
            }
        })
        .await;
        assert!(
            matches!(err, Error::MarketStream(StreamError::BookGap { .. })),
            "{err}"
        );
        let mut conn = server.accept().await;
        assert_eq!(conn.request().await.topics, vec![t(7, Stream::Depth)]);
    }
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
