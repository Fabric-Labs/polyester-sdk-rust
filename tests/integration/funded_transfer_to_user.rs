use crate::support::{
    LEDGER_SCALE, call_optional, call_required, devnet_unavailable, hydrate_spot_and_zipper,
    internal_transfer_dest, quote_asset_id, require_funded, require_live_client,
    require_trading_quote_balance, route_unavailable, scaled_quantity_string, smoke_symbol,
    trading_balance_raw, unique_client_order_id,
};
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::proto::polyester::r#type::v1::U128;
use polyester::proto::transfer::v1::CreateInternalTransferRequest;
use polyester::proto::transfer::v1::create_internal_transfer_request::Destination;

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
    let dest_account_id: u64 = match dest.parse() {
        Ok(id) => id,
        Err(_) => {
            eprintln!("skip: POLYESTER_TEST_INTERNAL_TRANSFER_DEST must be numeric account id");
            return;
        }
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
    let asset_scale = zipper
        .as_ref()
        .and_then(|z| {
            z.assets
                .iter()
                .find(|a| a.ledger_id == asset_id)
                .map(|a| a.quantity_scale)
        })
        .filter(|s| *s > 0)
        .unwrap_or(LEDGER_SCALE);
    let qty_scaled: u128 = match scaled_quantity_string(&quantity, asset_scale) {
        Ok(s) => s.parse().unwrap_or(0),
        Err(err) => {
            eprintln!("skip: scale qty: {err}");
            return;
        }
    };
    if qty_scaled == 0 {
        eprintln!("skip: invalid qty_scaled");
        return;
    }

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
    let mut req = CreateInternalTransferRequest {
        asset_id,
        idempotency_key: unique_client_order_id("e2e-xfer"),
        destination: Some(Destination::DestinationAccountId(dest_account_id)),
        ..Default::default()
    };
    *req.amount_e18.get_or_insert_default() = U128 {
        hi: (qty_scaled >> 64) as u64,
        lo: qty_scaled as u64,
        ..Default::default()
    };
    let result = match client.internal_transfers.create(req).await {
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
