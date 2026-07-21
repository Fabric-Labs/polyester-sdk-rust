//! Live on-chain Funding UserOps (POLY-3569).
//!
//! Gated by `POLYESTER_TEST_CHAIN_USEROP=1` plus `POLYESTER_OWNER_PRIVATE_KEY`.

use crate::support::{
    call_required, load_dotenv, require_funded, require_live_client, trading_balance_raw,
};
use alloy_primitives::U256;
use polyester::chain::{
    POLYESTER_TESTNET_ENVIRONMENT, PolyesterSmartAccount, SendCallsResult,
    encode_funding_withdraw_to_chain, encode_trading_gateway_deposit, encode_withdraw_destination,
    quote_zipper_fee,
};
use polyester::models::{DepositWithdrawConfig, ZipperAssetConfig};
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use std::str::FromStr;
use std::time::Duration;

fn require_chain_userop() -> Option<String> {
    load_dotenv();
    if !crate::support::env_truthy("POLYESTER_TEST_CHAIN_USEROP") {
        eprintln!("skip: Set POLYESTER_TEST_CHAIN_USEROP=1 to run on-chain Funding UserOp tests");
        return None;
    }
    match std::env::var("POLYESTER_OWNER_PRIVATE_KEY") {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_owned()),
        _ => {
            eprintln!("skip: Set POLYESTER_OWNER_PRIVATE_KEY for on-chain Funding UserOp tests");
            None
        }
    }
}

fn usdt_asset(cfg: &DepositWithdrawConfig) -> Option<ZipperAssetConfig> {
    let override_id = std::env::var("POLYESTER_TEST_U_ASSET_ID")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    cfg.assets
        .iter()
        .find(|&asset| {
            if let Some(ref o) = override_id {
                return asset.u_asset_id.eq_ignore_ascii_case(o);
            }
            asset.ledger_id == 1 || asset.asset.eq_ignore_ascii_case("USDT")
        })
        .cloned()
}

fn deposit_qty_scaled() -> U256 {
    if let Ok(raw) = std::env::var("POLYESTER_TEST_DEPOSIT_QTY_SCALED") {
        let t = raw.trim();
        if !t.is_empty() {
            return U256::from_str(t).expect("POLYESTER_TEST_DEPOSIT_QTY_SCALED");
        }
    }
    U256::from(10u64).pow(U256::from(18u64))
}

fn funding_balance_raw(balances: &[polyester::models::AssetBalance], asset_id: u32) -> u128 {
    for row in balances {
        if row.asset_id == asset_id {
            return row.funding.parse().unwrap_or(0);
        }
    }
    0
}

#[tokio::test]
async fn funding_to_trading_userop() {
    if !require_funded() {
        return;
    }
    let Some(owner) = require_chain_userop() else {
        return;
    };
    let Some(client) = require_live_client() else {
        return;
    };

    let cfg = call_required("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset) = usdt_asset(&cfg) else {
        eprintln!("skip: USDT / ledger_id=1 not found in zipper deposit-withdraw config");
        return;
    };
    let qty = deposit_qty_scaled();
    let qty_u128: u128 = qty.try_into().unwrap_or(u128::MAX);

    let before = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let funding_before = funding_balance_raw(&before.balances, asset.ledger_id);
    let trading_before = trading_balance_raw(&before.balances, asset.ledger_id);
    if funding_before < qty_u128 {
        eprintln!(
            "skip: funding balance {funding_before} below deposit quantity {qty} for asset {}",
            asset.ledger_id
        );
        return;
    }

    let account = PolyesterSmartAccount::new(&owner, None, 0, Duration::from_secs(60))
        .expect("smart account");
    let call = encode_trading_gateway_deposit(
        POLYESTER_TESTNET_ENVIRONMENT
            .contracts
            .trading_gateway_address,
        &asset.u_asset_id,
        qty,
    )
    .expect("encode deposit");
    let result = account
        .send_calls(&[call], true, Duration::from_secs(120))
        .await
        .expect("send_calls");
    match result {
        SendCallsResult::Receipt(r) => {
            assert!(r.success, "userop success");
            assert!(!r.user_operation_hash.is_empty());
        }
        SendCallsResult::Hash(h) => panic!("expected receipt, got hash {h}"),
    }

    let want = trading_before.saturating_add(qty_u128);
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let after = call_required("balances.list", || {
            client.balances.list(GetBalancesRequest::default())
        })
        .await;
        let trading_after = trading_balance_raw(&after.balances, asset.ledger_id);
        if trading_after >= want {
            return;
        }
    }
    panic!("trading balance did not increase by deposit amount");
}

#[tokio::test]
async fn funding_withdraw_to_chain_userop() {
    if !require_funded() {
        return;
    }
    let Some(owner) = require_chain_userop() else {
        return;
    };
    let dest = std::env::var("POLYESTER_TEST_WITHDRAW_DESTINATION")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    let Some(dest) = dest else {
        eprintln!("skip: Set POLYESTER_TEST_WITHDRAW_DESTINATION for Funding→external UserOp");
        return;
    };
    let chain_id: u16 = std::env::var("POLYESTER_TEST_WITHDRAW_CHAIN_ID")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(6);
    let human = std::env::var("POLYESTER_TEST_WITHDRAW_AMOUNT")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "1".to_owned());
    let human_f: f64 = human.parse().unwrap_or(1.0);
    let z_amount = U256::from((human_f * 1e18) as u128);

    let Some(client) = require_live_client() else {
        return;
    };
    let cfg = call_required("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset) = usdt_asset(&cfg) else {
        eprintln!("skip: USDT / ledger_id=1 not found in zipper deposit-withdraw config");
        return;
    };
    let Some(variant) = asset
        .variants
        .iter()
        .find(|v| v.chain_id == u32::from(chain_id) && !v.z_token.address.is_empty())
        .cloned()
    else {
        eprintln!("skip: No USDT z_token variant for withdraw chain_id={chain_id}");
        return;
    };
    let case_sensitive = cfg
        .chains
        .iter()
        .find(|c| c.chain_id == u32::from(chain_id))
        .map(|c| c.is_case_sensitive)
        .unwrap_or(false);

    let before = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let funding_before = funding_balance_raw(&before.balances, asset.ledger_id);
    let z_u128: u128 = z_amount.try_into().unwrap_or(u128::MAX);
    if funding_before < z_u128 {
        eprintln!(
            "skip: funding balance {funding_before} below withdraw amount {z_amount} for asset {}",
            asset.ledger_id
        );
        return;
    }

    let fee = quote_zipper_fee(
        chain_id,
        &variant.z_token.address,
        POLYESTER_TESTNET_ENVIRONMENT
            .contracts
            .zipper_endpoint_address,
        None,
        None,
    )
    .await
    .expect("quote fee");
    let max_fee = fee.fee + fee.fee / U256::from(10u64);
    if z_amount <= max_fee {
        eprintln!(
            "skip: withdraw amount {z_amount} must be greater than max_fee {max_fee}; raise POLYESTER_TEST_WITHDRAW_AMOUNT"
        );
        return;
    }

    let account = PolyesterSmartAccount::new(&owner, None, 0, Duration::from_secs(60))
        .expect("smart account");
    let dest_bytes = encode_withdraw_destination(&dest, case_sensitive);
    let call = encode_funding_withdraw_to_chain(
        POLYESTER_TESTNET_ENVIRONMENT
            .contracts
            .funding_account_address,
        chain_id,
        &variant.z_token.address,
        &dest_bytes,
        z_amount,
        max_fee,
    )
    .expect("encode withdraw");
    let result = account
        .send_calls(&[call], true, Duration::from_secs(120))
        .await
        .expect("send_calls");
    match result {
        SendCallsResult::Receipt(r) => {
            assert!(r.success, "userop success");
            assert!(!r.user_operation_hash.is_empty());
        }
        SendCallsResult::Hash(h) => panic!("expected receipt, got hash {h}"),
    }

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let after = call_required("balances.list", || {
            client.balances.list(GetBalancesRequest::default())
        })
        .await;
        let funding_after = funding_balance_raw(&after.balances, asset.ledger_id);
        if funding_after < funding_before {
            return;
        }
    }
    panic!("funding balance did not decrease after withdraw UserOp");
}
