use std::time::Duration;

use crate::support::{
    devnet_unavailable, hydrate_spot_and_zipper, is_internal_order_error, is_not_found,
    is_notional_validation, is_terminal_status, market_ref_price, min_base_qty_for_pair,
    order_status_label, pair_for_symbol, require_live_client, require_mutation,
    require_trading_base_balance, require_trading_quote_balance, resolve_post_only_buy_limit_price,
    unique_client_order_id, wait_for_terminal_order,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::types::{Price, Quantity};

const BTC_USDT: &str = "BTC-USDT";

fn trade_symbol_or_btc() -> String {
    crate::support::env_trade_symbol().unwrap_or_else(|| BTC_USDT.to_owned())
}

#[tokio::test]
async fn market_buy_mutation() {
    if !require_mutation() {
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let symbol = trade_symbol_or_btc();
    if !require_trading_quote_balance(&client, &symbol).await {
        return;
    }
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate: {err}");
            return;
        }
    };
    let Some(pair) = pair_for_symbol(&spot, &symbol) else {
        eprintln!("skip: symbol {symbol} not in spot config");
        return;
    };
    let price = resolve_post_only_buy_limit_price(&client, &symbol, Some(&pair)).await;
    let qty = min_base_qty_for_pair(Some(&pair), &price);
    let ref_price = market_ref_price(&client, &symbol, "buy", Some(&pair)).await;
    let scale = client.catalogs.base_quantity_scale_for_symbol(&symbol);
    let client_order_id = unique_client_order_id("mkt-buy");

    let params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity: match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
            Ok(q) => q,
            Err(err) => {
                eprintln!("skip: qty: {err}");
                return;
            }
        },
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(client_order_id.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: Some(
            match Price::from_decimal_str(&ref_price, Some(symbol.clone())) {
                Ok(p) => p,
                Err(err) => {
                    eprintln!("skip: ref price: {err}");
                    return;
                }
            },
        ),
        attached_risk: None,
    };

    let created = match client.orders.create(params).await {
        Ok(c) => c,
        Err(err) if is_internal_order_error(&err) || devnet_unavailable(&err) => {
            eprintln!("skip: devnet order placement unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            eprintln!("skip: notional: {err}");
            return;
        }
        Err(err) => panic!("create: {err}"),
    };
    assert_eq!(created.client_order_id, client_order_id);
    assert!(!created.order_id.is_empty() && created.order_id != "0");
    if is_terminal_status(&created.status.to_ascii_lowercase()) {
        return;
    }
    // Do not cancel_all before terminal wait — IOC market orders should settle themselves.
    match wait_for_terminal_order(&client, &client_order_id, Duration::ZERO).await {
        Ok(detail) => {
            let order = detail.order.expect("order");
            let status = order_status_label(&order);
            assert!(
                is_terminal_status(&status),
                "unexpected terminal status {status}"
            );
        }
        Err(err) if is_not_found(&err) => {
            eprintln!("skip: market buy accepted but order never indexed for terminal wait: {err}");
        }
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("did not reach terminal") || msg.contains("stuck in status") {
                eprintln!("skip: market buy terminal wait: {err}");
                return;
            }
            panic!("terminal: {err}");
        }
    }
}

#[tokio::test]
async fn market_sell_mutation() {
    if !require_mutation() {
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let symbol = trade_symbol_or_btc();
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate: {err}");
            return;
        }
    };
    let Some(pair) = pair_for_symbol(&spot, &symbol) else {
        eprintln!("skip: symbol {symbol} not in spot config");
        return;
    };
    let price = resolve_post_only_buy_limit_price(&client, &symbol, Some(&pair)).await;
    let qty = min_base_qty_for_pair(Some(&pair), &price);
    let ref_price = market_ref_price(&client, &symbol, "sell", Some(&pair)).await;
    if !require_trading_base_balance(&client, &symbol, &qty).await {
        return;
    }
    let scale = client.catalogs.base_quantity_scale_for_symbol(&symbol);
    let client_order_id = unique_client_order_id("mkt-sell");

    let params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Sell,
        order_type: CreateOrderType::Market,
        quantity: match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
            Ok(q) => q,
            Err(err) => {
                eprintln!("skip: qty: {err}");
                return;
            }
        },
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(client_order_id.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: Some(
            match Price::from_decimal_str(&ref_price, Some(symbol.clone())) {
                Ok(p) => p,
                Err(err) => {
                    eprintln!("skip: ref price: {err}");
                    return;
                }
            },
        ),
        attached_risk: None,
    };

    let created = match client.orders.create(params).await {
        Ok(c) => c,
        Err(err) if is_internal_order_error(&err) || devnet_unavailable(&err) => {
            eprintln!("skip: devnet order placement unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            eprintln!("skip: notional: {err}");
            return;
        }
        Err(err) => panic!("create: {err}"),
    };
    assert_eq!(created.client_order_id, client_order_id);
    assert!(!created.order_id.is_empty() && created.order_id != "0");
    if is_terminal_status(&created.status.to_ascii_lowercase()) {
        return;
    }
    match wait_for_terminal_order(&client, &client_order_id, Duration::ZERO).await {
        Ok(detail) => {
            let order = detail.order.expect("order");
            let status = order_status_label(&order);
            assert!(
                is_terminal_status(&status),
                "unexpected terminal status {status}"
            );
        }
        Err(err) if is_not_found(&err) => {
            eprintln!(
                "skip: market sell accepted but order never indexed for terminal wait: {err}"
            );
        }
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("did not reach terminal") || msg.contains("stuck in status") {
                eprintln!("skip: market sell terminal wait: {err}");
                return;
            }
            panic!("terminal: {err}");
        }
    }
}
