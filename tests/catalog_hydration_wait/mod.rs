//! POLY-9127: a method that names a market by symbol, or needs a pair's
//! catalog scales, waits for construction-time catalog hydration.
//!
//! The stub API holds `GetSpotConfig` until the test releases it, so every
//! call is made while hydration is still running. No timing is involved: the
//! test runtime is single-threaded, and a call that reads the empty catalogs
//! refuses the symbol on its first poll, while one that waits cannot finish
//! before hydration does, however often it is polled.

use super::hardening_support::RefusedPort;
use buffa::Message as _;
use futures_util::future::BoxFuture;
use polyester::models::{GetCandlesOpts, GetTradesOpts, ListOrderHistoryOpts, ListTriggersOpts};
use polyester::proto::chain::zipper::v1::{
    AssetConfig as ZipperAsset, GetDepositWithdrawConfigResponse,
};
use polyester::proto::marketdata::v1::{
    AssetConfig, GetCandlesColumnsRequest, GetCandlesRequest, GetOrderbookHeatmapRequest,
    GetSpotConfigResponse, GetTradesRequest, PairConfig,
};
use polyester::proto::marketoverview::v1::ListMarketOverviewRequest;
use polyester::proto::orderbook::v1::GetOrderBookRequest;
use polyester::proto::orders::v1::GetOrderHistoryRequest;
use polyester::proto::triggers::v1::ListTriggersRequest;
use polyester::services::{
    CreateSubscriptionOptions, GetHeatmapOptions, ListMarketOverviewOptions,
};
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const SYMBOL: &str = "DOGE-USDT";
const SYMBOL_ID: u32 = 6;
const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const ZIPPER_CONFIG: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";

fn spot_config() -> GetSpotConfigResponse {
    let asset = |asset: &str, scale| AssetConfig {
        asset: asset.into(),
        quantity_scale: scale,
        market_data_volume_scale: scale,
        ..Default::default()
    };
    GetSpotConfigResponse {
        assets: vec![asset("DOGE", 8), asset("USDT", 6)],
        pairs: vec![PairConfig {
            symbol_id: SYMBOL_ID,
            symbol: SYMBOL.into(),
            base_asset: "DOGE".into(),
            quote_asset: "USDT".into(),
            base_quantity_scale: 8,
            quote_quantity_scale: 6,
            reference_price_scale: 9,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn zipper_config() -> GetDepositWithdrawConfigResponse {
    GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        assets: vec![ZipperAsset {
            asset: "USDT".into(),
            ledger_id: 99,
            quantity_scale: 6,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Requests other than the catalogs: each path and the symbol IDs it carries.
type SentLog = Arc<Mutex<Vec<(String, Vec<u32>)>>>;

/// A Connect API whose `GetSpotConfig` answers once released: `Some(false)`
/// with the catalog, `Some(true)` with a server error.
struct Stub {
    base_url: String,
    release: watch::Sender<Option<bool>>,
    spot_requested: watch::Receiver<bool>,
    /// Every other request: its path and the symbol IDs it carries.
    sent: SentLog,
    _refused_ws: RefusedPort,
    ws_url: String,
}

impl Stub {
    async fn spawn() -> Self {
        Self::spawn_holding_funding(false).await
    }

    /// With `hold_funding`, `GetDepositWithdrawConfig` waits for the same
    /// release and fails with it.
    async fn spawn_holding_funding(hold_funding: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base_url = format!("http://{}", listener.local_addr().expect("addr"));
        let (release, released) = watch::channel(None::<bool>);
        let (requested_tx, spot_requested) = watch::channel(false);
        let requested_tx = Arc::new(requested_tx);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_task = sent.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let released = released.clone();
                let requested = requested_tx.clone();
                let sent = sent_task.clone();
                tokio::spawn(serve(stream, released, requested, sent, hold_funding));
            }
        });
        let refused = RefusedPort::bind();
        let ws_url = format!("ws://{}/connection/websocket", refused.addr());
        Self {
            base_url,
            release,
            spot_requested,
            sent,
            _refused_ws: refused,
            ws_url,
        }
    }

    fn client(&self, hydrate_catalogs: bool) -> Arc<Client> {
        Arc::new(
            Client::new(Config {
                api_url: self.base_url.clone(),
                ws_url: self.ws_url.clone(),
                api_key_id: Some("ak_test".into()),
                api_private_key: Some(TEST_KEY.into()),
                default_account_id: Some("1".into()),
                timeout: Duration::from_secs(5),
                hydrate_catalogs,
                allow_insecure_http: true,
                ..Default::default()
            })
            .expect("client"),
        )
    }

    async fn spot_requested(&self) {
        let mut requested = self.spot_requested.clone();
        tokio::time::timeout(Duration::from_secs(10), requested.wait_for(|seen| *seen))
            .await
            .expect("hydration never requested the spot catalog")
            .expect("stub stopped");
    }

    fn release(&self, fail: bool) {
        self.release.send_replace(Some(fail));
    }

    fn sent(&self) -> Vec<(String, Vec<u32>)> {
        self.sent.lock().expect("sent").clone()
    }
}

async fn serve(
    mut stream: TcpStream,
    mut released: watch::Receiver<Option<bool>>,
    requested: Arc<watch::Sender<bool>>,
    sent: SentLog,
    hold_funding: bool,
) {
    let Some((path, body)) = read_request(&mut stream).await else {
        return;
    };
    let (status, content_type, payload) = if path == SPOT_CONFIG {
        requested.send_replace(true);
        let Ok(outcome) = released.wait_for(Option::is_some).await else {
            return;
        };
        if outcome.unwrap_or(false) {
            (
                500,
                "application/json",
                br#"{"code":"internal","message":"spot down"}"#.to_vec(),
            )
        } else {
            (200, "application/proto", spot_config().encode_to_vec())
        }
    } else if path == ZIPPER_CONFIG {
        let failed = if hold_funding {
            let Ok(outcome) = released.wait_for(Option::is_some).await else {
                return;
            };
            outcome.unwrap_or(false)
        } else {
            false
        };
        if failed {
            (
                500,
                "application/json",
                br#"{"code":"internal","message":"funding down"}"#.to_vec(),
            )
        } else {
            (200, "application/proto", zipper_config().encode_to_vec())
        }
    } else {
        sent.lock()
            .expect("sent")
            .push((path.clone(), request_symbol_ids(&path, &body)));
        // Unimplemented: neither retried nor read as a validation refusal.
        (
            404,
            "application/json",
            br#"{"code":"unimplemented","message":"stub"}"#.to_vec(),
        )
    };
    let head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(index) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let path = head
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_owned();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buf.len() < header_end + length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some((path, buf[header_end..].to_vec()))
}

/// The symbol IDs a request carries, decoded by its procedure.
fn request_symbol_ids(path: &str, body: &[u8]) -> Vec<u32> {
    let method = path.rsplit('/').next().unwrap_or("");
    match method {
        "GetOrderBook" => GetOrderBookRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        "GetTrades" => GetTradesRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        "GetCandles" => GetCandlesRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        "GetCandlesColumns" => GetCandlesColumnsRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        "GetOrderbookHeatmap" => GetOrderbookHeatmapRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        "ListMarketOverview" => ListMarketOverviewRequest::decode_from_slice(body)
            .map(|r| r.symbol_id)
            .unwrap_or_default(),
        "GetOrderHistory" => GetOrderHistoryRequest::decode_from_slice(body)
            .map(|r| r.symbol_id)
            .unwrap_or_default(),
        "ListTriggers" => ListTriggersRequest::decode_from_slice(body)
            .map(|r| vec![r.symbol_id])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

type Probe = fn(Arc<Client>, String) -> BoxFuture<'static, polyester::Result<()>>;

/// Every public method that resolves a display symbol or needs a pair's
/// scales, with whether it ends at the realtime connection.
fn probes() -> Vec<(&'static str, bool, Probe)> {
    vec![
        ("market_data.get_trades", false, |c, s| {
            Box::pin(async move { c.market_data.get_trades(&s, Some(10)).await.map(drop) })
        }),
        ("market_data.get_trades_with", false, |c, s| {
            Box::pin(async move {
                c.market_data
                    .get_trades_with(GetTradesOpts {
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("market_data.get_candles", false, |c, s| {
            Box::pin(async move {
                c.market_data
                    .get_candles(&s, "1m", Some(10))
                    .await
                    .map(drop)
            })
        }),
        ("market_data.get_candles_with", false, |c, s| {
            Box::pin(async move {
                c.market_data
                    .get_candles_with(GetCandlesOpts {
                        symbol: Some(s),
                        timeframe: "1m".into(),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("market_data.get_current_candle", false, |c, s| {
            Box::pin(async move { c.market_data.get_current_candle(&s, "1m").await.map(drop) })
        }),
        ("market_data.get_candles_columns", false, |c, s| {
            Box::pin(async move {
                c.market_data
                    .get_candles_columns(GetCandlesOpts {
                        symbol: Some(s),
                        timeframe: "1m".into(),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("market_data.subscribe_trades", true, |c, s| {
            Box::pin(async move { c.market_data.subscribe_trades(&s).await.map(drop) })
        }),
        ("market_data.subscribe_candles", true, |c, s| {
            Box::pin(async move { c.market_data.subscribe_candles(&s, "1m").await.map(drop) })
        }),
        ("orderbook.get", false, |c, s| {
            Box::pin(async move { c.orderbook.get(&s, Some(10)).await.map(drop) })
        }),
        ("orderbook.create_subscription", true, |c, s| {
            Box::pin(async move {
                c.orderbook
                    .create_subscription(CreateSubscriptionOptions {
                        symbol: s,
                        symbol_id: None,
                        depth: Some(10),
                        bucket: None,
                    })
                    .await
                    .map(drop)
            })
        }),
        ("heatmap.get", false, |c, s| {
            Box::pin(async move { c.heatmap.get(&s, "1s", 50, 10, "close").await.map(drop) })
        }),
        ("heatmap.get_with_options", false, |c, s| {
            Box::pin(async move {
                c.heatmap
                    .get_with_options(&s, "1s", 50, 10, "close", GetHeatmapOptions::default())
                    .await
                    .map(drop)
            })
        }),
        ("heatmap.subscribe_live", true, |c, s| {
            Box::pin(async move { c.heatmap.subscribe_live(&s, "1s").await.map(drop) })
        }),
        ("market_overview.list", false, |c, s| {
            Box::pin(async move {
                c.market_overview
                    .list(ListMarketOverviewOptions {
                        symbols: Some(vec![s]),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("orders.list_history_with", false, |c, s| {
            Box::pin(async move {
                c.orders
                    .list_history_with(ListOrderHistoryOpts {
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("triggers.list_with", false, |c, s| {
            Box::pin(async move {
                c.triggers
                    .list_with(ListTriggersOpts {
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
    ]
}

/// Starts `probe` once hydration is held and lets it run as far as it can.
async fn start_held(
    stub: &Stub,
    client: &Arc<Client>,
    probe: Probe,
    symbol: &str,
) -> tokio::task::JoinHandle<polyester::Result<()>> {
    stub.spot_requested().await;
    let handle = tokio::spawn(probe(client.clone(), symbol.to_owned()));
    // Single-threaded: each yield lets the call run until it next waits.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    handle
}

async fn outcome(
    name: &str,
    handle: tokio::task::JoinHandle<polyester::Result<()>>,
) -> polyester::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), handle)
        .await
        .unwrap_or_else(|_| panic!("{name} did not return"))
        .expect("probe panicked")
}

fn is_symbol_refusal(err: &Error) -> bool {
    matches!(err, Error::Validation(_) | Error::CatalogHydration(_))
}

/// Runs `check` for every probe and fails with every probe that failed it.
async fn for_each_probe<F>(probes: Vec<(&'static str, bool, Probe)>, check: F)
where
    F: AsyncFn(&'static str, bool, Probe) -> Result<(), String>,
{
    let mut failures = Vec::new();
    for (name, stream, probe) in probes {
        if let Err(failure) = check(name, stream, probe).await {
            failures.push(format!("{name}: {failure}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn symbol_calls_wait_for_in_flight_catalog_hydration() {
    for_each_probe(probes(), async |name, stream, probe| {
        let stub = Stub::spawn().await;
        let client = stub.client(true);
        let handle = start_held(&stub, &client, probe, SYMBOL).await;
        if handle.is_finished() {
            return Err(format!(
                "ended while hydration was still running: {:?}",
                outcome(name, handle).await
            ));
        }
        stub.release(false);
        let Err(err) = outcome(name, handle).await else {
            return Err("the stub and the stream refuse every call, yet it succeeded".into());
        };
        if is_symbol_refusal(&err) {
            return Err(format!("refused the symbol: {err}"));
        }
        let sent = stub.sent();
        if !stream && !sent.iter().any(|(_, ids)| ids.contains(&SYMBOL_ID)) {
            return Err(format!("sent {sent:?}"));
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn symbol_calls_report_failed_hydration_as_catalog_hydration_error() {
    for_each_probe(probes(), async |name, _, probe| {
        let stub = Stub::spawn().await;
        let client = stub.client(true);
        let handle = start_held(&stub, &client, probe, SYMBOL).await;
        if handle.is_finished() {
            return Err(format!(
                "ended while hydration was still running: {:?}",
                outcome(name, handle).await
            ));
        }
        stub.release(true);
        let Err(err) = outcome(name, handle).await else {
            return Err("succeeded after a failed hydration".into());
        };
        let message = err.to_string();
        if !matches!(err, Error::CatalogHydration(_)) {
            return Err(format!("want Error::CatalogHydration, got {err:?}"));
        }
        if !message.contains("catalog hydration failed") || !message.contains("spot down") {
            return Err(format!("does not name the hydration failure: {message}"));
        }
        if !stub.sent().is_empty() {
            return Err(format!("sent {:?}", stub.sent()));
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn unknown_symbol_after_hydration_is_refused_as_unknown() {
    for_each_probe(probes(), async |name, _, probe| {
        let stub = Stub::spawn().await;
        let client = stub.client(true);
        stub.release(false);
        client.wait_for_catalogs().await.expect("hydrated");
        let handle = start_held(&stub, &client, probe, "NOPE-USDT").await;
        let Err(err) = outcome(name, handle).await else {
            return Err("accepted an unknown symbol".into());
        };
        if !matches!(&err, Error::Validation(message) if message.contains("unknown symbol NOPE-USDT"))
        {
            return Err(format!("want the unknown-symbol validation error, got {err:?}"));
        }
        if !stub.sent().is_empty() {
            return Err(format!("sent {:?}", stub.sent()));
        }
        Ok(())
    })
    .await;
}

/// Reads that only label or scale their results from the catalogs: they wait
/// for hydration, and a failed one costs the labels, not the read.
fn labelled_reads() -> Vec<(&'static str, bool, Probe)> {
    vec![
        ("market_overview.get_spot_volume_history", false, |c, _| {
            Box::pin(async move {
                c.market_overview
                    .get_spot_volume_history(None, Some(&[SYMBOL_ID]))
                    .await
                    .map(drop)
            })
        }),
        ("market_overview.subscribe", true, |c, _| {
            Box::pin(async move { c.market_overview.subscribe().await.map(drop) })
        }),
        ("fees.get_spot_fee_rates", false, |c, _| {
            Box::pin(async move { c.fees.get_spot_fee_rates(None, Vec::new()).await.map(drop) })
        }),
        ("triggers.list", false, |c, _| {
            Box::pin(async move {
                c.triggers
                    .list(polyester::proto::triggers::v1::ListTriggersRequest::default())
                    .await
                    .map(drop)
            })
        }),
        ("triggers.get_by_id", false, |c, _| {
            Box::pin(async move { c.triggers.get_by_id("1", None).await.map(drop) })
        }),
    ]
}

#[tokio::test]
async fn labelled_reads_wait_for_hydration_but_tolerate_failure() {
    for fail in [false, true] {
        for_each_probe(labelled_reads(), async |name, _, probe| {
            let stub = Stub::spawn().await;
            let client = stub.client(true);
            let handle = start_held(&stub, &client, probe, SYMBOL).await;
            if handle.is_finished() || !stub.sent().is_empty() {
                return Err(format!("read before the catalogs loaded (failed={fail})"));
            }
            stub.release(fail);
            let Err(err) = outcome(name, handle).await else {
                return Err("the stub and the stream refuse every call, yet it succeeded".into());
            };
            if is_symbol_refusal(&err) {
                return Err(format!("failed={fail}: refused for the catalogs: {err}"));
            }
            Ok(())
        })
        .await;
    }
}

#[tokio::test]
async fn zipped_asset_supply_waits_for_the_scales_it_decodes_with() {
    for fail in [false, true] {
        // POLY-9228: the stream waits for the funding config.
        let stub = Stub::spawn_holding_funding(true).await;
        let client = stub.client(true);
        let probe: Probe = |c, _| {
            Box::pin(async move { c.zipper.subscribe_zipped_asset_supply(true).await.map(drop) })
        };
        let handle = start_held(&stub, &client, probe, SYMBOL).await;
        assert!(
            !handle.is_finished(),
            "the supply stream started before the catalogs loaded"
        );
        stub.release(fail);
        let err = outcome("zipper.subscribe_zipped_asset_supply", handle)
            .await
            .expect_err("the stream is refused");
        assert_eq!(
            matches!(err, Error::CatalogHydration(_)),
            fail,
            "failed={fail}: {err:?}"
        );
        if fail {
            assert!(err.to_string().contains("funding config"), "{err}");
        }
    }
}

/// POLY-9228: a failed spot config does not stop the supply stream, which
/// decodes with the funding config only.
#[tokio::test]
async fn zipped_asset_supply_does_not_need_the_spot_config() {
    let stub = Stub::spawn().await;
    let client = stub.client(true);
    stub.release(true);
    client
        .wait_for_funding_catalog()
        .await
        .expect("funding config");
    let result = client.zipper.subscribe_zipped_asset_supply(true).await;
    assert!(
        !matches!(result, Err(Error::CatalogHydration(_))),
        "the supply stream refused for the spot config"
    );
}

#[tokio::test]
async fn hydration_disabled_keeps_todays_refusals() {
    let stub = Stub::spawn().await;
    let client = stub.client(false);
    let err = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: SYMBOL.into(),
            symbol_id: None,
            depth: Some(10),
            bucket: None,
        })
        .await
        .map(drop)
        .expect_err("no catalog");
    assert!(
        err.to_string()
            .contains("symbol_id is required for orderbook subscriptions (\"DOGE-USDT\")"),
        "{err}"
    );
    let err = client
        .market_data
        .get_trades(SYMBOL, Some(10))
        .await
        .expect_err("no catalog");
    assert!(
        err.to_string().contains("unknown symbol DOGE-USDT"),
        "{err}"
    );
    assert!(
        !*stub.spot_requested.borrow(),
        "hydration ran while disabled"
    );
}
