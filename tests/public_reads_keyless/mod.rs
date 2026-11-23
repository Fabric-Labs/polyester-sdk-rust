//! A client without an API key sends every public read the SDK wraps,
//! unsigned, and refuses a signed read before sending it.
//!
//! The public RPCs are those `testdata/rpc_auth.tsv` lists as `public` (the
//! contract marks them `polyester.api.public`), less those `sdk-coverage.toml`
//! leaves unwrapped. Each one has a call below; a public RPC the contract adds
//! and the SDK wraps fails here until it has one.

use super::hardening_support::RefusedPort;
use buffa::Message as _;
use polyester::Error;
use polyester::models::SubAccountScope;
use polyester::proto::chain::lifecycle::v1::{
    FlowDetailView, FlowSummaryView, FlowTxMatchView, GetFlowResponse, ListAccountFlowsRequest,
    ListFlowsByTxRequest, ListFlowsByTxResponse, ListFlowsRequest, ListFlowsResponse,
};
use polyester::proto::marketdata::v1::{AssetConfig, GetSpotConfigResponse, PairConfig};
use polyester::services::{
    ListChangelogChangesOptions, ListChangelogDeploymentGroupsOptions, ListChangelogPostsOptions,
    ListStatusEventsOptions,
};
use polyester::transport::WireFormat;
use polyester::{Client, Config};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const LIST_FLOWS: &str = "/chain.lifecycle.v1.LifecycleReadService/ListFlows";
const GET_FLOW: &str = "/chain.lifecycle.v1.LifecycleReadService/GetFlowById";
const LIST_FLOWS_BY_TX: &str = "/chain.lifecycle.v1.LifecycleReadService/ListFlowsByTx";
const LIST_ACCOUNT_FLOWS: &str = "/chain.lifecycle.v1.LifecycleReadService/ListAccountFlows";
const GET_ACCOUNT_FLOW: &str = "/chain.lifecycle.v1.LifecycleReadService/GetAccountFlow";
const FLOW_ID: &str = "flow_keyfree";
const TX_HASH: &str = "0xabc";

/// One request the fake server received: its path and whether it carried any
/// API-key signature header.
#[derive(Debug, Clone)]
struct Seen {
    path: String,
    signed: bool,
}

type Log = Arc<Mutex<Vec<Seen>>>;

fn reply(path: &str) -> Vec<u8> {
    let flow = || FlowSummaryView {
        flow_id: FLOW_ID.into(),
        ..Default::default()
    };
    match path {
        SPOT_CONFIG => GetSpotConfigResponse {
            assets: vec![AssetConfig {
                asset: "BTC".into(),
                quantity_scale: 8,
                market_data_volume_scale: 8,
                ..Default::default()
            }],
            pairs: vec![PairConfig {
                symbol_id: 1,
                symbol: "BTC-USDT".into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                base_quantity_scale: 8,
                quote_quantity_scale: 6,
                reference_price_scale: 9,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        LIST_FLOWS => ListFlowsResponse {
            flows: vec![flow()],
            ..Default::default()
        }
        .encode_to_vec(),
        GET_FLOW => GetFlowResponse {
            flow: FlowDetailView {
                summary: flow().into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .encode_to_vec(),
        LIST_FLOWS_BY_TX => ListFlowsByTxResponse {
            tx_hash: TX_HASH.into(),
            matches: vec![FlowTxMatchView {
                flow_id: FLOW_ID.into(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        // Every other reply is the empty message: this test is about whether
        // the request is sent, and signed, not about what it returns.
        _ => Vec::new(),
    }
}

async fn serve(mut stream: TcpStream, log: Log) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let Ok(n) = stream.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(index) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut request_line = head.split_whitespace();
    // Only Connect calls are recorded, not probes of the listener.
    if request_line.next() != Some("POST") {
        return;
    }
    let path = request_line.next().unwrap_or("/").to_owned();
    let signed = head
        .lines()
        .any(|line| line.to_ascii_lowercase().starts_with("x-api-"));
    log.lock().unwrap().push(Seen {
        path: path.clone(),
        signed,
    });
    let payload = reply(&path);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

/// A client without an API key, against a fake server that logs each call.
async fn keyless_client() -> (Client, Log, RefusedPort) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    let log: Log = Arc::default();
    let server_log = log.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve(stream, server_log.clone()));
        }
    });
    let refused = RefusedPort::bind();
    let client = Client::new(Config {
        api_url,
        ws_url: format!("ws://{}/connection/websocket", refused.addr()),
        api_key_id: None,
        api_private_key: None,
        wire_format: WireFormat::Binary,
        timeout: Duration::from_secs(5),
        hydrate_catalogs: true,
        allow_insecure_http: true,
        ..Default::default()
    })
    .expect("keyless client");
    client.wait_for_catalogs().await.expect("public catalogs");
    (client, log, refused)
}

/// The contract's public RPCs this SDK wraps: `public` in
/// `testdata/rpc_auth.tsv`, less the RPCs and services `sdk-coverage.toml`
/// leaves unwrapped.
fn wrapped_public_rpcs() -> BTreeSet<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let inventory = std::fs::read_to_string(root.join("testdata/rpc_auth.tsv")).unwrap();
    let coverage = std::fs::read_to_string(root.join("sdk-coverage.toml")).unwrap();
    let mut omitted_procedures = BTreeSet::new();
    let mut omitted_services = BTreeSet::new();
    for line in coverage.lines() {
        let quoted = |rest: &str| {
            rest.trim()
                .trim_start_matches('=')
                .trim()
                .trim_matches('"')
                .to_owned()
        };
        if let Some(rest) = line.trim().strip_prefix("procedure") {
            omitted_procedures.insert(quoted(rest));
        } else if let Some(rest) = line.trim().strip_prefix("service") {
            omitted_services.insert(format!("/{}/", quoted(rest)));
        }
    }
    inventory
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("procedure\t"))
        .filter_map(|line| line.split_once('\t'))
        .filter(|(_, access)| *access == "public")
        .map(|(procedure, _)| procedure.to_owned())
        .filter(|procedure| !omitted_procedures.contains(procedure))
        .filter(|procedure| !omitted_services.iter().any(|s| procedure.starts_with(s)))
        .collect()
}

/// Call the SDK wrappers that send `procedure`. Only the lifecycle reads must
/// also decode their reply; the fake server answers the rest with an empty
/// message, which some decoders refuse after the request was sent.
async fn call_public(client: &Client, procedure: &str) -> Vec<(&'static str, Result<(), Error>)> {
    fn done<T>(result: Result<T, Error>) -> Result<(), Error> {
        result.map(|_| ())
    }
    match procedure {
        "/chain.lifecycle.v1.LifecycleReadService/ListFlows" => {
            let page = client
                .lifecycle
                .list_flows(ListFlowsRequest {
                    limit: 5,
                    ..Default::default()
                })
                .await
                .map(|page| assert_eq!(page.flows[0].intent_id, FLOW_ID));
            vec![("lifecycle.list_flows", page)]
        }
        "/chain.lifecycle.v1.LifecycleReadService/GetFlowById" => {
            let flow = client
                .lifecycle
                .get_flow(FLOW_ID)
                .await
                .map(|flow| assert_eq!(flow.intent_id, FLOW_ID));
            vec![("lifecycle.get_flow", flow)]
        }
        "/chain.lifecycle.v1.LifecycleReadService/ListFlowsByTx" => {
            let by_tx = || ListFlowsByTxRequest {
                tx_hash: TX_HASH.into(),
                ..Default::default()
            };
            let list = client
                .lifecycle
                .list_flows_by_tx(by_tx())
                .await
                .map(|page| assert_eq!(page.flows[0].intent_id, FLOW_ID));
            let get = client
                .lifecycle
                .get_flow_by_tx(by_tx())
                .await
                .map(|page| assert_eq!(page.flows[0].intent_id, FLOW_ID));
            vec![
                ("lifecycle.list_flows_by_tx", list),
                ("lifecycle.get_flow_by_tx", get),
            ]
        }
        "/chain.analytics.v1.ChainAnalyticsService/GetZippedAssetSupply" => vec![(
            "chain_analytics.get_zipped_asset_supply",
            done(
                client
                    .chain_analytics
                    .get_zipped_asset_supply(1, "1d", "1h", 0, 0)
                    .await,
            ),
        )],
        "/chain.analytics.v1.ChainAnalyticsService/GetZippedAssetSupplyGroup" => vec![(
            "chain_analytics.get_zipped_asset_supply_group",
            done(
                client
                    .chain_analytics
                    .get_zipped_asset_supply_group("btc", "1d", "1h", 0, 0)
                    .await,
            ),
        )],
        "/chain.analytics.v1.ChainAnalyticsService/GetUnifiedAssetBalances" => vec![(
            "chain_analytics.get_unified_asset_balances",
            done(
                client
                    .chain_analytics
                    .get_unified_asset_balances(1, "1d", "1h", 0, 0)
                    .await,
            ),
        )],
        "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig" => vec![(
            "zipper.get_deposit_withdraw_config",
            done(client.zipper.get_deposit_withdraw_config().await),
        )],
        "/changelog.v1.ChangelogService/ListPosts" => vec![(
            "changelog.list_posts",
            done(
                client
                    .changelog
                    .list_posts(ListChangelogPostsOptions::default())
                    .await,
            ),
        )],
        "/changelog.v1.ChangelogService/GetPost" => vec![(
            "changelog.get_post",
            done(client.changelog.get_post("release").await),
        )],
        "/changelog.v1.ChangelogService/ListChanges" => vec![(
            "changelog.list_changes",
            done(
                client
                    .changelog
                    .list_changes(ListChangelogChangesOptions::default())
                    .await,
            ),
        )],
        "/changelog.v1.ChangelogService/ListDeploymentGroups" => vec![(
            "changelog.list_deployment_groups",
            done(
                client
                    .changelog
                    .list_deployment_groups(ListChangelogDeploymentGroupsOptions::default())
                    .await,
            ),
        )],
        "/changelog.v1.ChangelogService/GetDeploymentGroup" => vec![(
            "changelog.get_deployment_group",
            done(client.changelog.get_deployment_group("release").await),
        )],
        "/marketdata.v1.HeatmapService/GetOrderbookHeatmap" => vec![(
            "heatmap.get",
            done(client.heatmap.get("BTC-USDT", "1m", 50, 10, "close").await),
        )],
        "/marketdata.v1.MarketDataService/GetSpotConfig" => vec![(
            "market_data.get_spot_config",
            done(client.market_data.get_spot_config().await),
        )],
        "/marketdata.v1.MarketDataService/ListAssetInfo" => vec![(
            "market_data.list_asset_info",
            done(client.market_data.list_asset_info(&[1]).await),
        )],
        "/marketdata.v1.MarketDataService/GetTrades" => vec![(
            "market_data.get_trades",
            done(client.market_data.get_trades("BTC-USDT", Some(5)).await),
        )],
        "/marketdata.v1.MarketDataService/GetCandles" => vec![(
            "market_data.get_candles",
            done(
                client
                    .market_data
                    .get_candles("BTC-USDT", "1m", Some(5))
                    .await,
            ),
        )],
        "/marketdata.v1.MarketDataService/GetCandlesColumns" => vec![(
            "market_data.get_candles_columns",
            done(
                client
                    .market_data
                    .get_candles_columns(polyester::models::GetCandlesOpts {
                        symbol: Some("BTC-USDT".into()),
                        timeframe: "1m".into(),
                        limit: Some(5),
                        ..Default::default()
                    })
                    .await,
            ),
        )],
        "/marketdata.v1.MarketDataService/GetIndexPriceCandles" => vec![(
            "market_data.get_index_price_candles",
            done(
                client
                    .market_data
                    .get_index_price_candles("BTC-USDT", "1h", Some(5))
                    .await,
            ),
        )],
        "/marketoverview.v1.MarketOverviewService/ListMarketOverview" => vec![(
            "market_overview.list",
            done(client.market_overview.list(Some(5)).await),
        )],
        "/marketoverview.v1.MarketOverviewService/GetSpotVolumeHistory" => vec![(
            "market_overview.get_spot_volume_history",
            done(
                client
                    .market_overview
                    .get_spot_volume_history(None, None)
                    .await,
            ),
        )],
        "/marketoverview.v1.MarketOverviewService/GetCurrencyConversionConfig" => vec![(
            "market_overview.get_currency_conversion_config",
            done(
                client
                    .market_overview
                    .get_currency_conversion_config()
                    .await,
            ),
        )],
        "/marketoverview.v1.MarketOverviewService/GetCurrencyConversionRates" => vec![(
            "market_overview.get_currency_conversion_rates",
            done(client.market_overview.get_currency_conversion_rates().await),
        )],
        "/orderbook.v1.OrderbookService/GetOrderBook" => vec![(
            "orderbook.get",
            done(client.orderbook.get("BTC-USDT", Some(5)).await),
        )],
        "/ratelimit.v1.RateLimitService/GetRateLimitConfig" => vec![(
            "rate_limits.get_rate_limit_config",
            done(client.rate_limits.get_rate_limit_config().await),
        )],
        "/system.v1.SystemService/GetTime" => {
            vec![("get_time", done(client.get_time().await))]
        }
        "/system.v1.SystemService/GetVersion" => {
            vec![("get_version", done(client.get_version().await))]
        }
        "/system.v1.SystemService/ListStatusEvents" => vec![(
            "list_status_events",
            done(
                client
                    .list_status_events(ListStatusEventsOptions::default())
                    .await,
            ),
        )],
        "/vip.v1.VIPService/ListVIPTiers" => {
            vec![(
                "vip.list_vip_tiers",
                done(client.vip.list_vip_tiers().await),
            )]
        }
        other => {
            panic!("public RPC {other} has no keyless call here: add the SDK method that sends it")
        }
    }
}

/// Every public read the SDK wraps reaches the server from a client without
/// an API key, and carries no signature header.
#[tokio::test]
async fn keyless_client_sends_every_public_read_unsigned() {
    let public = wrapped_public_rpcs();
    // The lifecycle reads a keyless client once could not send.
    for procedure in [LIST_FLOWS, GET_FLOW, LIST_FLOWS_BY_TX] {
        assert!(public.contains(procedure), "{procedure} is not public");
    }
    let (client, log, _refused) = keyless_client().await;
    let mut failures = Vec::new();
    for procedure in &public {
        log.lock().unwrap().clear();
        for (method, result) in call_public(&client, procedure).await {
            match result {
                Err(err @ Error::Auth { .. }) => {
                    failures.push(format!("{method} ({procedure}) needs a key: {err}"));
                }
                Err(err) if procedure.contains("/chain.lifecycle.v1.") => {
                    failures.push(format!("{method} ({procedure}): {err}"));
                }
                _ => {}
            }
        }
        let seen = log.lock().unwrap().clone();
        if !seen.iter().any(|s| s.path == *procedure) {
            failures.push(format!("{procedure} was not sent: {seen:?}"));
        }
        for call in seen.iter().filter(|s| s.signed) {
            failures.push(format!("{} was signed", call.path));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A signed read is refused without an API key before anything is sent.
#[tokio::test]
async fn keyless_client_refuses_signed_lifecycle_reads_before_sending() {
    let (client, log, _refused) = keyless_client().await;
    log.lock().unwrap().clear();
    let list = client
        .lifecycle
        .list_account_flows(ListAccountFlowsRequest::default())
        .await;
    assert!(matches!(list, Err(Error::Auth { .. })), "{list:?}");
    let get = client
        .lifecycle
        .get_account_flow(FLOW_ID, SubAccountScope::Default)
        .await;
    assert!(matches!(get, Err(Error::Auth { .. })), "{get:?}");
    let seen = log.lock().unwrap().clone();
    assert!(
        !seen
            .iter()
            .any(|s| s.path == LIST_ACCOUNT_FLOWS || s.path == GET_ACCOUNT_FLOW),
        "{seen:?}"
    );
}
