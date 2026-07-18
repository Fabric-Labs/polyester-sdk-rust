use crate::support::{
    LEDGER_SCALE, call_optional, call_required, devnet_unavailable, hydrate_spot_and_zipper,
    internal_transfer_dest, quote_asset_id, require_funded, require_live_client,
    require_trading_quote_balance, route_unavailable, scaled_quantity_string, smoke_symbol,
    trading_balance_raw, unique_client_order_id,
};
use polyester::models::CreateInternalTransferParams;
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::types::{AssetAmount, QuantityDomain};

#[tokio::test]
async fn transfer_to_user_tiny() {
    if !require_funded() {
        return;
    }

    let bucket = std::env::var("POLYESTER_TEST_TRANSFER_SOURCE_BUCKET")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "funding".to_owned());
    if bucket == "funding" {
        eprintln!(
            "skip: Funding→another user is on-chain in the Polyester app, not an API-key RPC. \
             Set POLYESTER_TEST_TRANSFER_SOURCE_BUCKET=unified to run unified→user via \
             internal_transfers.create"
        );
        return;
    }
    if bucket != "unified" {
        eprintln!(
            "skip: unknown POLYESTER_TEST_TRANSFER_SOURCE_BUCKET={bucket:?}; use funding or unified"
        );
        return;
    }

    let Some(client) = require_live_client() else {
        return;
    };
    let Some(dest) = internal_transfer_dest() else {
        eprintln!("skip: Set POLYESTER_TEST_INTERNAL_TRANSFER_DEST for internal transfer e2e");
        return;
    };

    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    if !require_trading_quote_balance(&client, &symbol).await {
        return;
    }

    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset_id) = quote_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve quote asset for internal transfer on {symbol}");
        return;
    };

    let quantity = std::env::var("POLYESTER_TEST_INTERNAL_TRANSFER_QTY")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "1".to_owned());
    let amount = match AssetAmount::from_decimal_str(
        &quantity,
        LEDGER_SCALE,
        QuantityDomain::LedgerE18,
        Some(asset_id),
    ) {
        Ok(a) => a,
        Err(err) => {
            eprintln!("skip: amount: {err}");
            return;
        }
    };

    let before = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let trading_before = trading_balance_raw(&before.balances, asset_id);
    let qty_raw_ledger = scaled_quantity_string(&quantity, LEDGER_SCALE)
        .ok()
        .and_then(|s| s.parse::<u128>().ok())
        .unwrap_or(0);
    if qty_raw_ledger > 0 && trading_before < qty_raw_ledger {
        eprintln!(
            "skip: trading balance {trading_before} below transfer quantity {quantity} for asset {asset_id}"
        );
        return;
    }

    let _ = client.internal_transfers.connect_client();
    let params = CreateInternalTransferParams {
        asset_id,
        quantity: amount,
        idempotency_key: unique_client_order_id("e2e-xfer"),
        subaccount_id: None,
        destination_account_id: Some(dest),
        destination_subaccount_id: None,
        destination_smart_account_address: None,
        quantity_scale: Some(LEDGER_SCALE),
    };
    let result = match client.internal_transfers.create(params).await {
        Ok(r) => r,
        Err(err) if devnet_unavailable(&err) || route_unavailable(&err) => {
            eprintln!("skip: devnet internal transfer unavailable: {err}");
            return;
        }
        Err(err) => panic!("internal transfer: {err}"),
    };
    assert!(
        !result.request_id.is_empty() || !result.transfer_id.is_empty(),
        "expected request_id or transfer_id: {result:?}"
    );
}
