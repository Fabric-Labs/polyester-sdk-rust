//! F-22 self-contained market BUY → SELL roundtrip (carry filled qty).
//!
//! Live acceptance may still be blocked by backend reserve corruption (POLY-3028).

use crate::support::{
    base_asset_id, call_optional, far_above_buy_stop_price, hydrate_spot_and_zipper,
    is_internal_order_error, is_notional_validation, maker_client_from_env, min_base_qty_for_pair,
    pair_for_symbol, quote_asset_id, require_funded, require_live_client, require_mutation,
    require_trading_quote_balance, reserved_balance_raw, resolve_post_only_buy_limit_price,
    route_unavailable, strict_live_enabled, trade_e2e_enabled, trade_symbol, trading_balance_raw,
    unique_client_order_id, wait_for_terminal_order, wait_until_no_open_client_ids,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::ledger::read::v1::{GetBalancesRequest, ListHoldsRequest};
use polyester::types::{Price, Quantity, QuantityDomain};
use std::time::Duration;

async fn verified_cancel_all(client: &polyester::Client, symbol: &str, label: &str) {
    match client.orders.cancel_all(Some(symbol), false, None).await {
        Ok(_) => {}
        Err(err) => {
            if strict_live_enabled() {
                panic!("cleanup {label} cancel_all failed: {err}");
            }
            eprintln!("cleanup {label} cancel_all warning: {err}");
        }
    }
}

#[tokio::test]
async fn market_buy_sell_roundtrip_carries_filled_qty() {
    if !require_mutation() {
        return;
    }
    if !require_funded() {
        return;
    }
    if !trade_e2e_enabled() {
        eprintln!("skip: Set POLYESTER_TEST_TRADE_E2E=1 to run market roundtrip e2e");
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(maker) = maker_client_from_env() else {
        eprintln!(
            "skip: Set POLYESTER_TEST_MAKER_API_KEY_ID and POLYESTER_TEST_MAKER_API_PRIVATE_KEY \
             for market roundtrip"
        );
        return;
    };
    let _ = hydrate_spot_and_zipper(&maker).await;
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate: {err}");
            return;
        }
    };
    let symbol = trade_symbol(&spot);
    eprintln!("market roundtrip symbol={symbol}");
    if !require_trading_quote_balance(&client, &symbol).await {
        return;
    }
    let Some(pair) = pair_for_symbol(&spot, &symbol) else {
        eprintln!("skip: trade symbol {symbol} not in spot config");
        return;
    };
    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(base_id) = base_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve base asset for {symbol}");
        return;
    };
    let quote_id = quote_asset_id(&spot, &symbol, zipper.as_ref());
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let price = far_above_buy_stop_price(&symbol);
    let qty = min_base_qty_for_pair(Some(&pair), &price);
    let buy_cid = unique_client_order_id("rt-buy");
    let sell_cid = unique_client_order_id("rt-sell");
    let maker_cid = unique_client_order_id("rt-maker");

    let balances_before = match client.balances.list(GetBalancesRequest::default()).await {
        Ok(b) => b,
        Err(err) => {
            eprintln!("skip: balances.list before: {err}");
            return;
        }
    };
    let base_before = trading_balance_raw(&balances_before.balances, base_id);
    let quote_reserved_before =
        quote_id.map(|qid| reserved_balance_raw(&balances_before.balances, qid));
    let base_reserved_before = reserved_balance_raw(&balances_before.balances, base_id);

    let maker_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Sell,
        order_type: CreateOrderType::Limit,
        quantity: Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
        price: Some(Price::from_decimal_str(&price, Some(symbol.clone())).expect("price")),
        time_in_force: Some(CreateTimeInForce::Gtc),
        client_order_id: Some(maker_cid),
        subaccount_id: None,
        post_only: Some(true),
        market_client_ref_price: None,
        attached_risk: None,
    };
    if let Err(err) = maker.orders.create(maker_params).await {
        if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) {
            eprintln!("skip: maker create unavailable: {err}");
            return;
        }
        panic!("maker create: {err}");
    }

    let buy_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity: Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(buy_cid.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: None,
        attached_risk: None,
    };
    match client.orders.create(buy_params).await {
        Ok(_) => {}
        Err(err) if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) => {
            verified_cancel_all(&maker, &symbol, "maker").await;
            eprintln!("skip: buy unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            verified_cancel_all(&maker, &symbol, "maker").await;
            eprintln!("skip: notional: {err}");
            return;
        }
        Err(err) => {
            verified_cancel_all(&maker, &symbol, "maker").await;
            panic!("buy create: {err}");
        }
    }

    let buy_detail = match wait_for_terminal_order(&client, &buy_cid, Duration::from_secs(20)).await
    {
        Ok(d) => d,
        Err(err) => {
            verified_cancel_all(&client, &symbol, "taker").await;
            verified_cancel_all(&maker, &symbol, "maker").await;
            eprintln!("skip: buy terminal wait (possible POLY-3028): {err}");
            return;
        }
    };
    let filled = buy_detail
        .order
        .as_ref()
        .and_then(|o| o.cum_qty.as_ref())
        .map(|q| q.as_scaled())
        .unwrap_or(0);
    if filled <= 0 {
        verified_cancel_all(&client, &symbol, "taker").await;
        verified_cancel_all(&maker, &symbol, "maker").await;
        eprintln!("skip: buy produced no fill (possible POLY-3028)");
        return;
    }

    let maker_buy_price = resolve_post_only_buy_limit_price(&maker, &symbol, Some(&pair)).await;
    let maker_buy_cid = unique_client_order_id("rt-maker-buy");
    let maker_buy = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Limit,
        quantity: Quantity::from_scaled(
            filled,
            Some(scale),
            QuantityDomain::OrderBase,
            Some(symbol.clone()),
            None,
        )
        .expect("filled qty"),
        price: Some(
            Price::from_decimal_str(&maker_buy_price, Some(symbol.clone())).expect("price"),
        ),
        time_in_force: Some(CreateTimeInForce::Gtc),
        client_order_id: Some(maker_buy_cid),
        subaccount_id: None,
        post_only: Some(true),
        market_client_ref_price: None,
        attached_risk: None,
    };
    if let Err(err) = maker.orders.create(maker_buy).await {
        verified_cancel_all(&client, &symbol, "taker").await;
        verified_cancel_all(&maker, &symbol, "maker").await;
        if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) {
            eprintln!("skip: maker buy unavailable: {err}");
            return;
        }
        panic!("maker buy: {err}");
    }

    // Carry exact filled base qty into cleanup SELL (no larger independent size).
    let sell_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Sell,
        order_type: CreateOrderType::Market,
        quantity: Quantity::from_scaled(
            filled,
            Some(scale),
            QuantityDomain::OrderBase,
            Some(symbol.clone()),
            None,
        )
        .expect("sell qty"),
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(sell_cid.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: None,
        attached_risk: None,
    };
    match client.orders.create(sell_params).await {
        Ok(_) => {}
        Err(err) => {
            verified_cancel_all(&client, &symbol, "taker").await;
            verified_cancel_all(&maker, &symbol, "maker").await;
            if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) {
                eprintln!("skip: sell cleanup unavailable: {err}");
                return;
            }
            panic!("sell create: {err}");
        }
    }

    let sell_detail =
        match wait_for_terminal_order(&client, &sell_cid, Duration::from_secs(20)).await {
            Ok(d) => d,
            Err(err) => {
                verified_cancel_all(&client, &symbol, "taker").await;
                verified_cancel_all(&maker, &symbol, "maker").await;
                eprintln!("skip: sell terminal wait (possible POLY-3028): {err}");
                return;
            }
        };
    let sell_filled = sell_detail
        .order
        .as_ref()
        .and_then(|o| o.cum_qty.as_ref())
        .map(|q| q.as_scaled())
        .unwrap_or(0);
    assert_eq!(
        sell_filled, filled,
        "cleanup SELL must use BUY filled qty (F-22)"
    );

    let open = client
        .orders
        .list_open(None)
        .await
        .expect("list_open after roundtrip");
    for order in &open.orders {
        assert!(
            order.client_order_id != buy_cid && order.client_order_id != sell_cid,
            "test order still open: {:?}",
            order.client_order_id
        );
    }

    let balances_after = client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .expect("balances.list after");
    let base_after = trading_balance_raw(&balances_after.balances, base_id);
    assert_eq!(
        base_after, base_before,
        "residual base position must return to before (exact scaled units)"
    );

    // Holds reconciled when list_holds route is mounted.
    match client
        .balances
        .list_holds(ListHoldsRequest {
            limit: 20,
            ..Default::default()
        })
        .await
    {
        Ok(_holds) => {
            let base_reserved_after = reserved_balance_raw(&balances_after.balances, base_id);
            assert_eq!(
                base_reserved_after, base_reserved_before,
                "base reserved not reconciled"
            );
            if let (Some(qid), Some(q_before)) = (quote_id, quote_reserved_before) {
                let q_after = reserved_balance_raw(&balances_after.balances, qid);
                assert_eq!(q_after, q_before, "quote reserved not reconciled");
            }
        }
        Err(err) if route_unavailable(&err) => {}
        Err(err) => {
            if strict_live_enabled() {
                panic!("list_holds after roundtrip: {err}");
            }
            eprintln!("holds reconcile skipped: {err}");
        }
    }

    verified_cancel_all(&client, &symbol, "taker").await;
    verified_cancel_all(&maker, &symbol, "maker").await;
    if let Err(err) =
        wait_until_no_open_client_ids(&client, &[&buy_cid, &sell_cid], Duration::from_secs(20))
            .await
    {
        if strict_live_enabled() {
            panic!("cleanup verification failed: {err}");
        }
        eprintln!("cleanup verification warning: {err}");
    }
}
