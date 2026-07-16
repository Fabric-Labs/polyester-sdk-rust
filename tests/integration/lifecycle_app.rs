//! Signed lifecycle / app surface integration tests.

use crate::support::{call_optional, require_live_client};
use polyester::proto::chain::analytics::v1::GetUnifiedAssetBalancesRequest;
use polyester::proto::chain::guard::v1::GetGuardSignerStatusRequest;
use polyester::proto::chain::lifecycle::v1::ListFlowsRequest;
use polyester::proto::layout::v1::GetLayoutsRequest;
use polyester::proto::marketdata::v1::GetOrderbookHeatmapRequest;
use polyester::proto::polychart::v1::GetMarketLayersRequest;

#[tokio::test]
async fn heatmap_get_shallow() {
    let Some(client) = require_live_client() else {
        return;
    };
    let api = client.heatmap.connect_client();
    match api
        .get_orderbook_heatmap(GetOrderbookHeatmapRequest::default())
        .await
    {
        Ok(resp) => {
            let _ = resp.into_owned();
        }
        Err(err) => eprintln!("skip: heatmap.get: {err}"),
    }
}

#[tokio::test]
async fn lifecycle_list_flows() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("lifecycle.list_flows", || {
        client.lifecycle.list_flows(ListFlowsRequest::default())
    })
    .await;
}

#[tokio::test]
async fn guard_signer_status() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("guard_signer.get_status", || {
        client
            .guard_signer
            .get_status(GetGuardSignerStatusRequest::default())
    })
    .await;
}

#[tokio::test]
async fn layout_get_layouts() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("layout.get_layouts", || {
        client.layout.get_layouts(GetLayoutsRequest::default())
    })
    .await;
}

#[tokio::test]
async fn polychart_market_layers() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("polychart.get_market_layers", || {
        client
            .polychart
            .get_market_layers(GetMarketLayersRequest::default())
    })
    .await;
}

#[tokio::test]
async fn chain_analytics_unified_balances_shallow() {
    let Some(client) = require_live_client() else {
        return;
    };
    let api = client.chain_analytics.connect_client();
    match api
        .get_unified_asset_balances(GetUnifiedAssetBalancesRequest::default())
        .await
    {
        Ok(resp) => {
            let _ = resp.into_owned();
        }
        Err(err) => eprintln!("skip: chain_analytics.get_unified_asset_balances: {err}"),
    }
}
