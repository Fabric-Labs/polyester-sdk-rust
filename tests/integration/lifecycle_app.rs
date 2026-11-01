//! Signed lifecycle / app surface integration tests.

use crate::support::{call_optional, require_live_client};
use polyester::proto::chain::guard::v1::GetGuardSignerStatusRequest;
use polyester::proto::chain::lifecycle::v1::ListFlowsRequest;

#[tokio::test]
async fn heatmap_get_shallow() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = client.wait_for_catalogs().await;
    let _ = call_optional("heatmap.get", || {
        client.heatmap.get("BTC-USDT", "1m", 50, 10, "close")
    })
    .await;
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
async fn chain_analytics_unified_balances_shallow() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("chain_analytics.get_unified_asset_balances", || {
        client
            .chain_analytics
            .get_unified_asset_balances(1, "1d", "", 0, 0)
    })
    .await;
}
