//! `Client::market_stream` against a local Connect API and stream: the
//! client's one catalog is re-read on every reconnect, and live decoding and
//! trade recovery through the real `MarketDataService` both use the catalog of
//! the connection they belong to.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use buffa::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::client::{FakeServer, ServerConn, expect, respond};
use super::frames::{fill, trades, trades_skipped, trades_through};
use crate::marketstream::backfill::Pacing;
use crate::marketstream::sbe::subscribe_result::SubscribeResult as Wire;
use crate::marketstream::{Event, MarketStream, Stream, Topic, TradesBackfill};
use crate::proto::chain::zipper::v1::{
    AssetConfig as ZipperAsset, GetDepositWithdrawConfigResponse,
};
use crate::proto::marketdata::v1::{
    AssetConfig, GetSpotConfigResponse, GetTradesRequest, GetTradesResponse, MarketTrade,
    PairConfig,
};
use crate::{Client, Config};

const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const ZIPPER_CONFIG: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";
const GET_TRADES: &str = "/marketdata.v1.MarketDataService/GetTrades";

/// Before the reset BTC-USDC is pair 7 at base scale 3 and ETH-USDC pair 9 at
/// scale 4; after it, ETH-USDC is pair 7 at scale 4 and BTC-USDC pair 12 at
/// scale 3.
fn spot(reset: bool) -> GetSpotConfigResponse {
    let asset = |asset: &str, scale| AssetConfig {
        asset: asset.into(),
        quantity_scale: scale,
        market_data_volume_scale: scale,
        ..Default::default()
    };
    let pair = |symbol_id, base: &str, scale| PairConfig {
        symbol_id,
        symbol: format!("{base}-USDC"),
        base_asset: base.into(),
        quote_asset: "USDC".into(),
        base_quantity_scale: scale,
        quote_quantity_scale: 6,
        reference_price_scale: 9,
        ..Default::default()
    };
    GetSpotConfigResponse {
        assets: vec![asset("BTC", 3), asset("ETH", 4), asset("USDC", 6)],
        pairs: if reset {
            vec![pair(7, "ETH", 4), pair(12, "BTC", 3)]
        } else {
            vec![pair(7, "BTC", 3), pair(9, "ETH", 4)]
        },
        ..Default::default()
    }
}

fn zipper() -> GetDepositWithdrawConfigResponse {
    GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        assets: vec![ZipperAsset {
            asset: "USDC".into(),
            ledger_id: 99,
            quantity_scale: 6,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Every trade of a pair after `after` through 110, each of quantity 1500
/// (scaled).
fn trades_after(symbol_id: u32, after: u64, page: u64) -> GetTradesResponse {
    let last = (after + page).min(110);
    GetTradesResponse {
        trades: (after + 1..=last)
            .map(|id| MarketTrade {
                symbol_id,
                match_id: id,
                price_ticks: 65_000_000_000_000,
                qty_scaled: 1_500,
                ts_ns: 1_700_000_000_000_000_000,
                is_buy: true,
                ..Default::default()
            })
            .collect(),
        complete_through_trade_id: 110,
        next_after_trade_id: last,
        ..Default::default()
    }
}

/// A Connect API: the spot config before or after the reset, and trade
/// history that answers once `trades` is released.
struct Api {
    base_url: String,
    reset: watch::Sender<bool>,
    trades: watch::Sender<bool>,
    asked: Arc<Mutex<Vec<(u32, u64)>>>,
    /// The most trades one history call returns.
    page: Arc<AtomicU64>,
}

impl Api {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (reset, reset_rx) = watch::channel(false);
        let (trades, trades_rx) = watch::channel(true);
        let asked = Arc::new(Mutex::new(Vec::new()));
        let log = asked.clone();
        let page = Arc::new(AtomicU64::new(1_000));
        let pages = page.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(
                    stream,
                    reset_rx.clone(),
                    trades_rx.clone(),
                    log.clone(),
                    pages.clone(),
                ));
            }
        });
        Self {
            base_url,
            reset,
            trades,
            asked,
            page,
        }
    }

    fn client(&self, server: &FakeServer) -> Client {
        Client::new(Config {
            api_url: self.base_url.clone(),
            ws_url: server.url.clone(),
            timeout: Duration::from_secs(5),
            hydrate_catalogs: true,
            allow_insecure_http: true,
            ..Default::default()
        })
        .unwrap()
    }

    async fn trades_asked(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.asked.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("trade history read");
    }
}

async fn serve(
    mut stream: TcpStream,
    reset: watch::Receiver<bool>,
    mut trades: watch::Receiver<bool>,
    asked: Arc<Mutex<Vec<(u32, u64)>>>,
    page: Arc<AtomicU64>,
) {
    let Some((path, body)) = read_request(&mut stream).await else {
        return;
    };
    let payload = match path.as_str() {
        SPOT_CONFIG => spot(*reset.borrow()).encode_to_vec(),
        ZIPPER_CONFIG => zipper().encode_to_vec(),
        GET_TRADES => {
            let req = GetTradesRequest::decode_from_slice(&body).unwrap();
            let after = req.after_trade_id.unwrap_or(0);
            asked.lock().unwrap().push((req.symbol_id, after));
            if trades.wait_for(|open| *open).await.is_err() {
                return;
            }
            trades_after(req.symbol_id, after, page.load(Ordering::SeqCst)).encode_to_vec()
        }
        _ => return,
    };
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let end = loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..end]).to_string();
    let path = head
        .split_whitespace()
        .nth(1)?
        .split('?')
        .next()?
        .to_owned();
    let length = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buf.len() < end + length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some((path, buf[end..].to_vec()))
}

async fn open(client: &Client) -> MarketStream {
    client
        .market_stream_with(|c| {
            c.ping_interval = Duration::from_secs(3600);
            c.pong_timeout = Duration::from_secs(3600);
            c.pacing = Pacing {
                first: Duration::from_millis(1),
                max: Duration::from_millis(4),
            };
        })
        .await
        .expect("connects")
}

/// The environment resets: the spot config renumbers, and the connection is
/// lost. Returns the new connection once its pairs are read.
async fn reset(
    api: &Api,
    server: &mut FakeServer,
    stream: &mut MarketStream,
    conn: ServerConn,
) -> ServerConn {
    api.reset.send_replace(true);
    conn.close();
    expect(stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    let conn = server.accept().await;
    expect(stream, |e| {
        matches!(e, Event::PairsReloaded { .. }).then_some(())
    })
    .await;
    conn
}

/// Subscribes trades of `symbol_id` with trades 101 to 104 skipped.
async fn trades_with_gap(stream: &mut MarketStream, conn: &mut ServerConn, symbol_id: u32) {
    let topics = [Topic::new(symbol_id, Stream::Trades)];
    let (results, _) = tokio::join!(stream.subscribe(&topics), async {
        let m = conn.request().await;
        conn.send(&[
            respond(&m, Wire::OK),
            trades_through(symbol_id, 1, 100),
            trades_skipped(symbol_id, 1, 101, 104),
            trades(symbol_id, 1, 9, &[fill(105)]),
        ]);
    });
    assert!(results.unwrap()[0].result.subscribed());
}

async fn backfill(stream: &mut MarketStream) -> TradesBackfill {
    expect(stream, |e| match e {
        Event::TradesBackfill(b) => Some(b),
        _ => None,
    })
    .await
}

#[tokio::test]
async fn recovered_trades_use_the_scale_of_the_renumbered_pair() {
    let api = Api::spawn().await;
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    let mut stream = open(&client).await;
    let conn = server.accept().await;
    let mut conn = reset(&api, &mut server, &mut stream, conn).await;

    // Pair 7 is ETH-USDC at scale 4 now, live and recovered alike.
    trades_with_gap(&mut stream, &mut conn, 7).await;
    let live = expect(&mut stream, |e| match e {
        Event::Trades(t) => Some(t),
        _ => None,
    })
    .await;
    assert_eq!(live.symbol.as_deref(), Some("ETH-USDC"));
    assert_eq!(live.trades[0].qty.scale(), Some(4));
    let recovered = backfill(&mut stream).await.trades.expect("recovered");
    assert_eq!(
        recovered.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
        [101, 102, 103, 104]
    );
    assert!(
        recovered.iter().all(|t| t.qty.scale() == Some(4)),
        "{recovered:?}"
    );
    assert_eq!(
        client.catalogs.symbol_for_symbol_id(7).as_deref(),
        Some("ETH-USDC")
    );
}

#[tokio::test]
async fn a_pair_numbered_only_after_the_reset_is_recovered_too() {
    let api = Api::spawn().await;
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    let mut stream = open(&client).await;
    let conn = server.accept().await;
    let mut conn = reset(&api, &mut server, &mut stream, conn).await;

    // Pair 12 is not in the catalog the client started with.
    trades_with_gap(&mut stream, &mut conn, 12).await;
    let recovered = backfill(&mut stream).await.trades.expect("recovered");
    assert_eq!(recovered.len(), 4);
    assert!(recovered.iter().all(|t| t.qty.scale() == Some(3)));
}

#[tokio::test]
async fn a_recovery_that_straddles_a_reset_is_dropped() {
    let api = Api::spawn().await;
    api.trades.send_replace(false);
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    let mut stream = open(&client).await;
    let mut conn = server.accept().await;

    // Recovery of BTC-USDC's gap on pair 7 starts before the reset...
    trades_with_gap(&mut stream, &mut conn, 7).await;
    api.trades_asked().await;
    let conn = reset(&api, &mut server, &mut stream, conn).await;
    // ...and answers after it, when pair 7 is ETH-USDC.
    api.trades.send_replace(true);
    let b = backfill(&mut stream).await;
    assert!(
        b.trades.is_err(),
        "a recovery from the previous catalog is never delivered: {:?}",
        b.trades
    );
    drop(conn);
}

/// Loses `conn` and waits for the stream's next connection to read its pairs.
async fn lose(server: &mut FakeServer, stream: &mut MarketStream, conn: ServerConn) -> ServerConn {
    conn.close();
    expect(stream, |e| {
        matches!(e, Event::Disconnected { .. }).then_some(())
    })
    .await;
    let conn = server.accept().await;
    expect(stream, |e| {
        matches!(e, Event::PairsReloaded { .. }).then_some(())
    })
    .await;
    conn
}

async fn subscribed(stream: &mut MarketStream, conn: &mut ServerConn, topic: Topic) {
    let topics = [topic];
    let (results, _) = tokio::join!(stream.subscribe(&topics), async {
        let m = conn.request().await;
        conn.send(&[respond(&m, Wire::OK)]);
    });
    assert!(results.unwrap()[0].result.subscribed());
}

#[tokio::test]
async fn two_streams_of_one_client_each_follow_their_own_pair_across_a_reset() {
    let api = Api::spawn().await;
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    // A follows BTC-USDC (7), B follows ETH-USDC (9).
    let mut a = open(&client).await;
    let mut a_conn = server.accept().await;
    subscribed(&mut a, &mut a_conn, Topic::new(7, Stream::Bbo)).await;
    let mut b = open(&client).await;
    let mut b_conn = server.accept().await;
    subscribed(&mut b, &mut b_conn, Topic::new(9, Stream::Bbo)).await;

    api.reset.send_replace(true);
    // B reconnects first and refreshes the client's catalog...
    let mut b_conn = lose(&mut server, &mut b, b_conn).await;
    let m = b_conn.request().await;
    assert_eq!(m.topics, [Topic::new(7, Stream::Bbo)], "ETH-USDC is 7 now");
    // ...then A, whose subscription is BTC-USDC whatever B did.
    let mut a_conn = lose(&mut server, &mut a, a_conn).await;
    let m = a_conn.request().await;
    assert_eq!(
        m.topics,
        [Topic::new(12, Stream::Bbo)],
        "BTC-USDC is 12 now"
    );
}

#[tokio::test]
async fn a_recovery_is_not_changed_by_another_streams_reload() {
    let api = Api::spawn().await;
    api.trades.send_replace(false);
    api.page.store(2, Ordering::SeqCst);
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    let mut a = open(&client).await;
    let mut a_conn = server.accept().await;
    let mut b = open(&client).await;
    let b_conn = server.accept().await;

    // A recovers BTC-USDC's gap on 7, two trades a call; the first call
    // answers only after B has read the reset's pairs, where 7 is ETH-USDC.
    trades_with_gap(&mut a, &mut a_conn, 7).await;
    api.trades_asked().await;
    api.reset.send_replace(true);
    let _b_conn = lose(&mut server, &mut b, b_conn).await;
    api.trades.send_replace(true);
    let recovered = backfill(&mut a)
        .await
        .trades
        .expect("A's connection is current");
    assert_eq!(
        recovered.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
        [101, 102, 103, 104]
    );
    assert!(
        recovered.iter().all(|t| t.qty.scale() == Some(3)),
        "every page at A's connection's scale: {recovered:?}"
    );
}

#[tokio::test]
async fn a_stream_opened_after_a_reset_cannot_retarget_an_older_one() {
    let api = Api::spawn().await;
    let mut server = FakeServer::start().await;
    let client = api.client(&server);
    let mut a = open(&client).await;
    let mut a_conn = server.accept().await;
    subscribed(&mut a, &mut a_conn, Topic::new(7, Stream::Bbo)).await;

    // B opens after the reset: its first read refreshes the client's
    // catalog while A still holds BTC-USDC as 7.
    api.reset.send_replace(true);
    let _b = open(&client).await;
    let _b_conn = server.accept().await;
    assert_eq!(
        client.catalogs.symbol_for_symbol_id(7).as_deref(),
        Some("ETH-USDC")
    );
    let mut a_conn = lose(&mut server, &mut a, a_conn).await;
    let m = a_conn.request().await;
    assert_eq!(
        m.topics,
        [Topic::new(12, Stream::Bbo)],
        "A still follows BTC-USDC"
    );
}
