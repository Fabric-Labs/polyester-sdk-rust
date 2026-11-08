//! Heatmap and market overview reads and streams, through public paths against
//! real local sockets.

#[allow(dead_code)]
#[path = "../hardening_support/centrifugo.rs"]
mod centrifugo;

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::{Enumeration as _, Message as _};
use centrifugo::{Centrifugo, centrifugo_publication};
use polyester::proto::chain::zipper::v1::{
    AssetConfig as ZipperAsset, GetDepositWithdrawConfigResponse,
};
use polyester::proto::marketdata::v1::{
    AssetConfig, GetOrderbookHeatmapRequest, GetOrderbookHeatmapResponse, GetSpotConfigResponse,
    HeatmapInterval, HeatmapLiveBucket, PairConfig, PairMarketdataConfig,
};
use polyester::proto::marketoverview::v1::{
    ListMarketOverviewRequest, ListMarketOverviewResponse, MarketOverview, MarketOverviewBatch,
    Sparkline, SparklineInterval,
};
use polyester::services::MarketOverviewCreateSubscriptionOptions;
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);
const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const ZIPPER_CONFIG: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";
const HEATMAP: &str = "/marketdata.v1.HeatmapService/GetOrderbookHeatmap";
const OVERVIEW: &str = "/marketoverview.v1.MarketOverviewService/ListMarketOverview";
const OVERVIEW_CHANNEL: &str = "public:spot:market_overview:updates:proto";

fn spot_config() -> GetSpotConfigResponse {
    let asset = |asset: &str, scale| AssetConfig {
        asset: asset.into(),
        quantity_scale: scale,
        market_data_volume_scale: scale,
        ..Default::default()
    };
    let pair = |symbol_id, symbol: &str, base: &str, scale, buckets: Vec<f64>| PairConfig {
        symbol_id,
        symbol: symbol.into(),
        base_asset: base.into(),
        quote_asset: "USDT".into(),
        base_quantity_scale: scale,
        quote_quantity_scale: 6,
        reference_price_scale: 9,
        marketdata: PairMarketdataConfig {
            orderbook_price_buckets: buckets,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    GetSpotConfigResponse {
        assets: vec![asset("BTC", 8), asset("BONK", 0), asset("USDT", 6)],
        pairs: vec![
            pair(1, "BTC-USDT", "BTC", 8, vec![0.01, 0.1, 1.0, 10.0, 100.0]),
            pair(
                7,
                "BONK-USDT",
                "BONK",
                0,
                vec![1e-9, 1e-8, 1e-7, 1e-6, 1e-5],
            ),
        ],
        ts_sec: 1,
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

/// An API answering catalogs, heatmap reads (echoing the requested depth) and
/// overview lists; it records every heatmap and overview request.
struct Api {
    http: MockHttpServer,
    heatmap: Arc<Mutex<Vec<GetOrderbookHeatmapRequest>>>,
    overview: Arc<Mutex<Vec<ListMarketOverviewRequest>>>,
}

impl Api {
    async fn spawn(snapshot: ListMarketOverviewResponse) -> Self {
        let heatmap: Arc<Mutex<Vec<GetOrderbookHeatmapRequest>>> = Arc::default();
        let overview: Arc<Mutex<Vec<ListMarketOverviewRequest>>> = Arc::default();
        let (heatmap_sink, overview_sink) = (heatmap.clone(), overview.clone());
        let http = MockHttpServer::spawn(move |request| match request.path.as_str() {
            SPOT_CONFIG => connect_proto_ok(&spot_config()),
            ZIPPER_CONFIG => connect_proto_ok(&zipper_config()),
            HEATMAP => {
                let message = GetOrderbookHeatmapRequest::decode_from_slice(&request.body).unwrap();
                let response = GetOrderbookHeatmapResponse {
                    symbol_id: message.symbol_id,
                    depth: message.depth,
                    ..Default::default()
                };
                heatmap_sink.lock().unwrap().push(message);
                connect_proto_ok(&response)
            }
            OVERVIEW => {
                let message = ListMarketOverviewRequest::decode_from_slice(&request.body).unwrap();
                let mut response = snapshot.clone();
                if !message.include_sparklines {
                    for market in &mut response.markets {
                        market.sparklines.clear();
                    }
                }
                overview_sink.lock().unwrap().push(message);
                connect_proto_ok(&response)
            }
            _ => HttpScript::NotFound,
        })
        .await;
        Self {
            http,
            heatmap,
            overview,
        }
    }

    async fn client(&self, ws_url: String) -> Client {
        let client = Client::new(Config {
            api_url: self.http.base_url(),
            ws_url,
            hydrate_catalogs: true,
            timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .unwrap();
        client.wait_for_catalogs().await.unwrap();
        client
    }
}

// POLY-8745: the server acknowledges a subscribe to any channel but publishes
// live buckets only on public:spot:market:heatmap:<1s|1m|5m|1h>:<id>:proto.
#[tokio::test]
async fn heatmap_subscribe_live_receives_published_buckets_for_every_interval() {
    let cases = [
        ("1s", "1s", HeatmapInterval::INTERVAL_1S),
        ("1m", "1m", HeatmapInterval::INTERVAL_1M),
        ("5m", "5m", HeatmapInterval::INTERVAL_5M),
        ("1h", "1h", HeatmapInterval::INTERVAL_1H),
        ("INTERVAL_1S", "1s", HeatmapInterval::INTERVAL_1S),
        ("interval_5m", "5m", HeatmapInterval::INTERVAL_5M),
    ];
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    for (interval, label, value) in cases {
        let ws = Centrifugo::spawn().await;
        let client = api.client(ws.url.clone()).await;
        let mut sub = client
            .heatmap
            .subscribe_live("BTC-USDT", interval)
            .await
            .unwrap();
        ws.await_subscribes(1).await;
        let published = format!("public:spot:market:heatmap:{label}:1:proto");
        let bucket = HeatmapLiveBucket {
            symbol_id: 1,
            interval: value.into(),
            ts_sec: 1_700_000_000,
            updates_in_bucket: 3,
            ..Default::default()
        };
        assert!(
            ws.try_push(&published, centrifugo_publication(&bucket.encode_to_vec())),
            "{interval}: the SDK did not subscribe to {published}"
        );
        let got = tokio::time::timeout(WAIT, sub.recv())
            .await
            .unwrap_or_else(|_| panic!("{interval}: no live bucket"))
            .expect("subscription open");
        assert_eq!(got.raw["symbolId"], 1, "{interval}: {:?}", got.raw);
    }
}

#[tokio::test]
async fn heatmap_subscribe_live_refuses_intervals_the_server_does_not_publish() {
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    let ws = Centrifugo::spawn().await;
    let client = api.client(ws.url.clone()).await;
    for interval in ["", "2m", "PEAK", "close", "INTERVAL_UNSPECIFIED"] {
        let err = client
            .heatmap
            .subscribe_live("BTC-USDT", interval)
            .await
            .err()
            .unwrap_or_else(|| panic!("{interval:?} was accepted"));
        assert!(matches!(err, Error::Validation(_)), "{interval:?}: {err:?}");
    }
    assert_eq!(ws.connects.load(std::sync::atomic::Ordering::SeqCst), 0);
}

// POLY-8782: the snapshot rule of orderbook.get (POLY-8586).
#[tokio::test]
async fn heatmap_get_requests_the_smallest_supported_depth_covering_the_request() {
    let cases = [
        (0, "DEPTH_50"),
        (1, "DEPTH_1"),
        (2, "DEPTH_5"),
        (5, "DEPTH_5"),
        (11, "DEPTH_20"),
        (50, "DEPTH_50"),
        (101, "DEPTH_200"),
        (200, "DEPTH_200"),
        (201, "DEPTH_500"),
        (500, "DEPTH_500"),
        (501, "DEPTH_1000"),
        (1000, "DEPTH_1000"),
    ];
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    let client = api.client("ws://127.0.0.1:9".into()).await;
    for (depth, want) in cases {
        let result = client
            .heatmap
            .get("BTC-USDT", "1m", depth, 1, "close")
            .await
            .unwrap();
        let sent = api.heatmap.lock().unwrap().pop().unwrap();
        assert_eq!(
            sent.depth.as_known().map(|d| d.proto_name()),
            Some(want),
            "depth {depth}"
        );
        assert_eq!(result.raw["depth"], want, "depth {depth}");
    }
    for depth in [1001, 5000, u32::MAX] {
        let err = client
            .heatmap
            .get("BTC-USDT", "1m", depth, 1, "close")
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "depth {depth}: {err:?}"
        );
    }
    assert!(api.heatmap.lock().unwrap().is_empty());
}

// POLY-8746 parity: each heatmap argument resolves against its own enum only.
#[tokio::test]
async fn heatmap_get_refuses_a_value_of_the_other_enum() {
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    let client = api.client("ws://127.0.0.1:9".into()).await;
    for (interval, mode) in [("PEAK", "peak"), ("1m", "INTERVAL_1M"), ("CLOSE", "close")] {
        let err = client
            .heatmap
            .get("BTC-USDT", interval, 50, 1, mode)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "{interval}/{mode}: {err:?}"
        );
    }
    assert!(api.heatmap.lock().unwrap().is_empty());
}

// POLY-8785 parity: buckets hydrated from the API keep every digit.
#[tokio::test]
async fn hydrated_price_buckets_keep_sub_micro_increments() {
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    let client = api.client("ws://127.0.0.1:9".into()).await;
    assert_eq!(
        client
            .catalogs
            .orderbook_price_buckets_for_symbol("BONK-USDT"),
        [
            "0.000000001",
            "0.00000001",
            "0.0000001",
            "0.000001",
            "0.00001"
        ]
    );
    assert_eq!(
        client
            .catalogs
            .orderbook_price_buckets_for_symbol("BTC-USDT"),
        ["0.01", "0.1", "1", "10", "100"]
    );
}

fn overview_snapshot() -> ListMarketOverviewResponse {
    ListMarketOverviewResponse {
        markets: vec![MarketOverview {
            symbol_id: 1,
            last_price_ticks: 101,
            change_24h_bps: -14,
            volume_24h_base_scaled: Some(250),
            sparklines: vec![
                Sparkline {
                    interval: SparklineInterval::SPARKLINE_24H.into(),
                    close_ticks: vec![101, 100, 99],
                    ..Default::default()
                },
                Sparkline {
                    interval: SparklineInterval::SPARKLINE_1H.into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

// POLY-8783: a live update keeps the symbol and the formatted base volume, and
// (POLY-8789) the snapshot's sparklines, which live publications never carry.
#[tokio::test]
async fn managed_overview_live_updates_keep_symbol_volume_and_sparklines() {
    let api = Api::spawn(overview_snapshot()).await;
    let ws = Centrifugo::spawn().await;
    let client = api.client(ws.url.clone()).await;
    let mut sub = client
        .market_overview
        .create_subscription(MarketOverviewCreateSubscriptionOptions {
            include_sparklines: true,
            ..Default::default()
        })
        .await
        .unwrap();
    ws.await_subscribed(OVERVIEW_CHANNEL).await;
    let first = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .expect("snapshot")
        .unwrap();
    assert_eq!(first[0].symbol, "BTC-USDT");
    let live = MarketOverviewBatch {
        markets: vec![MarketOverview {
            symbol_id: 1,
            last_price_ticks: 102,
            change_24h_bps: 7,
            volume_24h_base_scaled: Some(300),
            ..Default::default()
        }],
        ..Default::default()
    };
    ws.push(
        OVERVIEW_CHANNEL,
        centrifugo_publication(&live.encode_to_vec()),
    );
    let rows = tokio::time::timeout(WAIT, sub.updates().recv())
        .await
        .expect("live update")
        .unwrap();
    let row = &rows[0];
    assert_eq!(row.last_price.as_ref().map(|p| p.as_ticks()), Some(102));
    assert_eq!(row.symbol, "BTC-USDT");
    assert_eq!(row.volume_24h_base.as_deref(), Some("0.000003"));
    sub.close();
}

// POLY-8783: the raw overview stream is enriched like list rows.
#[tokio::test]
async fn overview_subscribe_enriches_live_rows() {
    let api = Api::spawn(ListMarketOverviewResponse::default()).await;
    let ws = Centrifugo::spawn().await;
    let client = api.client(ws.url.clone()).await;
    let mut sub = client.market_overview.subscribe().await.unwrap();
    ws.await_subscribed(OVERVIEW_CHANNEL).await;
    let live = MarketOverviewBatch {
        markets: vec![MarketOverview {
            symbol_id: 1,
            volume_24h_base_scaled: Some(250),
            ..Default::default()
        }],
        ..Default::default()
    };
    ws.push(
        OVERVIEW_CHANNEL,
        centrifugo_publication(&live.encode_to_vec()),
    );
    let batch = tokio::time::timeout(WAIT, sub.recv())
        .await
        .expect("live batch")
        .unwrap();
    assert_eq!(batch.markets[0].symbol, "BTC-USDT");
    assert_eq!(
        batch.markets[0].volume_24h_base.as_deref(),
        Some("0.0000025")
    );
}
