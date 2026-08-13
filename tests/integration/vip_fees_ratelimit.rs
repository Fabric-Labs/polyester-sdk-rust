//! VIP catalog/status, spot fees, and trading rate-limit integration tests.

use crate::support::{call_optional, require_live_client};

#[tokio::test]
async fn list_vip_tiers() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(result) = call_optional("vip.list_vip_tiers", || client.vip.list_vip_tiers()).await
    else {
        return;
    };
    assert!(result.tiers.len() <= 11);
}

#[tokio::test]
async fn get_vip_status() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(result) = call_optional("vip.get_vip_status", || client.vip.get_vip_status()).await
    else {
        return;
    };
    assert!(result.tier <= 10);
}

#[tokio::test]
async fn get_spot_fee_rates() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("fees.get_spot_fee_rates", || {
        client.fees.get_spot_fee_rates(None, vec![])
    })
    .await;
}

#[tokio::test]
async fn get_rate_limit_config() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("rate_limits.get_rate_limit_config", || {
        client.rate_limits.get_rate_limit_config()
    })
    .await;
}

#[tokio::test]
async fn get_trading_rate_limits() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("rate_limits.get_trading_rate_limits", || {
        client.rate_limits.get_trading_rate_limits(None)
    })
    .await;
}
