//! A resting post-only order reserves quote balance and cancellation releases it.

use crate::support::{
    hydrate_spot_and_zipper, min_base_qty_for_pair, pair_for_symbol, quote_asset_id,
    require_funded, require_live_client, require_mutation, require_trading_quote_balance,
    reserved_balance_raw, trade_symbol, unique_client_order_id, wait_for_no_open_order,
    wait_for_open_order, wait_until_reserved_reconciles,
};
use polyester::codecs::scalars::{format_price_ticks, parse_price_ticks_str};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::types::{Price, Quantity};
use std::time::Duration;

#[tokio::test]
async fn contract_reserved_visible_while_open() {
    if !require_mutation() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
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
    let symbol = trade_symbol(&spot);
    if !require_trading_quote_balance(&client, &symbol).await {
        return;
    }

    let zipper = client
        .zipper
        .get_deposit_withdraw_config()
        .await
        .expect("zipper catalog");
    let quote_asset_id =
        quote_asset_id(&spot, &symbol, Some(&zipper)).expect("quote asset must resolve");

    let before = client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .expect("balances before");
    let reserved_before = reserved_balance_raw(&before.balances, quote_asset_id);
    let pair = pair_for_symbol(&spot, &symbol).expect("market config");
    let book = client
        .orderbook
        .get(&symbol, None)
        .await
        .expect("orderbook");
    let bid_ticks = book
        .bids
        .first()
        .and_then(|level| level.price.as_ref())
        .expect("a live bid is required to choose a far-from-market price")
        .as_ticks();
    let tick = parse_price_ticks_str(&pair.tick_size, "tick_size").expect("tick size");
    assert!(tick > 0);
    let price_str = format_price_ticks(bid_ticks / 2 / tick * tick);
    let qty_str = min_base_qty_for_pair(Some(&pair), &price_str);

    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let client_order_id = unique_client_order_id("hold");

    let params = {
        let mut p = CreateOrderParams::new(symbol.clone(), CreateSide::Buy, CreateOrderType::Limit);
        p.quantity = match Quantity::from_decimal_str(&qty_str, scale, Some(symbol.clone()), None) {
            Ok(q) => Some(q),
            Err(err) => {
                eprintln!("skip: qty: {err}");
                return;
            }
        };
        p.price = Some(
            match Price::from_decimal_str(&price_str, Some(symbol.clone())) {
                Ok(p) => p,
                Err(err) => {
                    eprintln!("skip: price: {err}");
                    return;
                }
            },
        );
        p.time_in_force = Some(CreateTimeInForce::Gtc);
        p.client_order_id = Some(client_order_id.clone());
        p.post_only = Some(true);
        p
    };

    let observed = async {
        let created = client.orders.create(params).await?;
        if created.client_order_id != client_order_id {
            return Err(polyester::errors::Error::validation(
                "create acknowledged a different client order ID",
            )
            .into());
        }
        wait_for_open_order(&client, &client_order_id, Duration::from_secs(15)).await?;
        for _ in 0..40 {
            let balances = client.balances.list(GetBalancesRequest::default()).await?;
            let resting = reserved_balance_raw(&balances.balances, quote_asset_id);
            if resting > reserved_before {
                println!("reserved quote balance: before={reserved_before} resting={resting}");
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(polyester::errors::Error::validation(
            "resting post-only order never increased reserved balance",
        )
        .into())
    }
    .await;
    let cancellation = client
        .orders
        .cancel_by_client_order_id(&client_order_id, Some(&symbol), None)
        .await;
    // Cancel even when the create acknowledgement or observation fails.
    if observed.is_ok() {
        cancellation.expect("cancel resting order");
    }
    wait_for_no_open_order(&client, &client_order_id, Duration::from_secs(20))
        .await
        .expect("no test order remains open");
    observed.expect("reservation must be observed");
    wait_until_reserved_reconciles(
        &client,
        &[(quote_asset_id, reserved_before)],
        Duration::from_secs(20),
    )
    .await
    .expect("cancel releases reserved balance");
    println!("reserved quote balance after cancel={reserved_before}");
}
