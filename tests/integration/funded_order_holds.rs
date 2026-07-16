//! Funded order-hold visibility (Python `test_order_holds` parity).

use crate::support::{
    call_optional, hydrate_spot_and_zipper, is_internal_order_error, quote_asset_id,
    require_funded, require_live_client, require_trading_quote_balance, smoke_symbol,
    unique_client_order_id, usdt_funded_buy_limit_params, wait_for_open_order,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::ledger::read::v1::ListHoldsRequest;
use polyester::types::{Price, Quantity};
use std::time::Duration;

#[tokio::test]
async fn order_hold_visible_while_open() {
    if !require_funded() {
        return;
    }
    let Some(client) = require_live_client() else {
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
    let Some(quote_asset_id) = quote_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve quote asset for {symbol}");
        return;
    };

    let (price_str, qty_str) = match usdt_funded_buy_limit_params(&client, &symbol).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: buy params: {err}");
            return;
        }
    };
    let scale = client.catalogs.base_quantity_scale_for_symbol(&symbol);
    let client_order_id = unique_client_order_id("hold");

    let params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Limit,
        quantity: match Quantity::from_decimal_str(&qty_str, scale, Some(symbol.clone()), None) {
            Ok(q) => q,
            Err(err) => {
                eprintln!("skip: qty: {err}");
                return;
            }
        },
        price: Some(
            match Price::from_decimal_str(&price_str, Some(symbol.clone())) {
                Ok(p) => p,
                Err(err) => {
                    eprintln!("skip: price: {err}");
                    return;
                }
            },
        ),
        time_in_force: Some(CreateTimeInForce::Gtc),
        client_order_id: Some(client_order_id.clone()),
        subaccount_id: None,
        post_only: Some(true),
        market_client_ref_price: None,
    };

    let created = match client.orders.create(params).await {
        Ok(o) => o,
        Err(err) if is_internal_order_error(&err) => {
            eprintln!("skip: devnet order internal error: {err}");
            return;
        }
        Err(err) => panic!("orders.create: {err}"),
    };
    assert!(!created.client_order_id.is_empty());

    let cleanup = async {
        let _ = client
            .orders
            .cancel_by_client_order_id(&client_order_id, Some(&symbol), None)
            .await;
    };

    match wait_for_open_order(&client, &client_order_id, Duration::from_secs(15)).await {
        Ok(_) => {}
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            cleanup.await;
            if msg.contains("canceled") || msg.contains("cancelled") || msg.contains("rejected") {
                eprintln!("skip: order reached terminal status before hold check: {err}");
                return;
            }
            panic!("wait_for_open_order: {err}");
        }
    }

    let holds = match call_optional("balances.list_holds", || {
        client.balances.list_holds(ListHoldsRequest {
            limit: 20,
            ..Default::default()
        })
    })
    .await
    {
        Some(h) => h,
        None => {
            cleanup.await;
            return;
        }
    };

    let has_hold = holds.holds.iter().any(|hold| {
        hold.asset_id == quote_asset_id
            && !hold.amount_reserved.is_empty()
            && hold.amount_reserved != "0"
    });
    cleanup.await;
    assert!(
        has_hold,
        "expected a positive hold on quote asset {quote_asset_id}"
    );
}
