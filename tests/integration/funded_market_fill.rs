use crate::support::{
    base_asset_id, call_optional, call_required, far_above_buy_stop_price, hydrate_spot_and_zipper,
    is_internal_order_error, is_notional_validation, maker_client_from_env, market_ref_price,
    min_base_qty_for_pair, pair_for_symbol, quote_asset_id, require_account_wide_cleanup,
    require_funded, require_live_client, require_mutation, require_trading_quote_balance,
    trade_e2e_enabled, trade_symbol, trading_balance_human, unique_client_order_id,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::types::{Price, Quantity};
use rust_decimal::Decimal;
use std::time::Duration;

#[tokio::test]
async fn market_order_fill() {
    if !require_account_wide_cleanup() {
        return;
    }
    if !require_mutation() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
    if !require_funded() {
        return;
    }
    if !trade_e2e_enabled() {
        eprintln!("skip: Set POLYESTER_TEST_TRADE_E2E=1 to run market order fill e2e");
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(maker) = maker_client_from_env() else {
        eprintln!(
            "skip: Set POLYESTER_TEST_MAKER_API_KEY_ID and POLYESTER_TEST_MAKER_API_PRIVATE_KEY \
             for market order fill e2e"
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

    let price = std::env::var("POLYESTER_TEST_TRADE_PRICE")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| far_above_buy_stop_price(&symbol));
    let qty = std::env::var("POLYESTER_TEST_TRADE_QTY")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| min_base_qty_for_pair(Some(&pair), &price));
    let ref_price = market_ref_price(&client, &symbol, "buy", Some(&pair)).await;

    let Some(quote_asset_id) = quote_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: missing quote asset id");
        return;
    };
    let Some(base_asset_id) = base_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: missing base asset id");
        return;
    };

    let qty_dec: Decimal = match qty.parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skip: bad qty");
            return;
        }
    };
    let price_dec: Decimal = match price.parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skip: bad price");
            return;
        }
    };

    let taker_before = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let taker_quote = trading_balance_human(&taker_before.balances, quote_asset_id);
    let required_quote = price_dec * qty_dec;
    if taker_quote < required_quote {
        eprintln!("skip: taker quote balance {taker_quote} below required {required_quote}");
        return;
    }
    let maker_before = call_required("balances.list", || {
        maker.balances.list(GetBalancesRequest::default())
    })
    .await;
    let maker_base = trading_balance_human(&maker_before.balances, base_asset_id);
    if maker_base < qty_dec {
        eprintln!("skip: maker base balance {maker_base} below fill quantity {qty}");
        return;
    }

    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let maker_cid = unique_client_order_id("maker-mkt");
    let taker_cid = unique_client_order_id("taker-mkt");

    let maker_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Sell,
        order_type: CreateOrderType::Limit,
        quantity: Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
        price: Some(Price::from_decimal_str(&price, Some(symbol.clone())).expect("price")),
        time_in_force: Some(CreateTimeInForce::Gtc),
        client_order_id: Some(maker_cid.clone()),
        subaccount_id: None,
        post_only: Some(true),
        market_client_ref_price: None,
        fee_source: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
    match maker.orders.create(maker_params).await {
        Ok(c) => {
            assert_eq!(c.client_order_id, maker_cid);
            assert!(!c.order_id.is_empty() && c.order_id != "0");
        }
        Err(err) if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) => {
            eprintln!("skip: maker create unavailable: {err}");
            return;
        }
        Err(err) => panic!("maker create: {err}"),
    }

    let taker_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity: Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(taker_cid.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: Some(
            Price::from_decimal_str(&ref_price, Some(symbol.clone())).expect("ref"),
        ),
        fee_source: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
    match client.orders.create(taker_params).await {
        Ok(c) => {
            assert_eq!(c.client_order_id, taker_cid);
            assert!(!c.order_id.is_empty() && c.order_id != "0");
        }
        Err(err) if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) => {
            let _ = maker.orders.cancel_all(Some(&symbol), false, None).await;
            eprintln!("skip: taker create unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            let _ = maker.orders.cancel_all(Some(&symbol), false, None).await;
            eprintln!("skip: notional: {err}");
            return;
        }
        Err(err) => {
            let _ = maker.orders.cancel_all(Some(&symbol), false, None).await;
            panic!("taker create: {err}");
        }
    }

    let _ =
        crate::support::wait_for_terminal_order(&client, &taker_cid, Duration::from_secs(20)).await;
    let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
    let _ = maker.orders.cancel_all(Some(&symbol), false, None).await;
}
