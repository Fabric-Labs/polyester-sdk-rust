//! Self-contained market BUY → SELL roundtrip.
//!
//! The cleanup SELL carries the BUY's net received base quantity: cumulative
//! fills minus fees whose source is the received asset.

use crate::support::{
    base_asset_id, call_optional, far_above_buy_stop_price, hydrate_spot_and_zipper,
    is_internal_order_error, is_notional_validation, maker_client_from_env, market_ref_price,
    min_base_qty_for_pair, pair_for_symbol, quote_asset_id, require_funded, require_live_client,
    require_mutation, require_trading_quote_balance, reserved_balance_raw,
    resolve_post_only_buy_limit_price, route_unavailable, strict_live_enabled, trade_e2e_enabled,
    trade_symbol, trading_balance_raw, unique_client_order_id, wait_for_terminal_order,
    wait_until_no_open_client_ids, wait_until_reserved_reconciles,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::ledger::read::v1::{GetBalancesRequest, ListHoldsRequest};
use polyester::types::{Price, Quantity, QuantityDomain};
use std::time::Duration;

async fn cancel_test_order(
    client: &polyester::Client,
    symbol: &str,
    client_order_id: &str,
    label: &str,
) {
    if let Err(err) = client
        .orders
        .cancel_by_client_order_id(client_order_id, Some(symbol), None)
        .await
    {
        eprintln!("cleanup {label} targeted cancel warning: {err}");
    }
}

fn fee_amount_e18_to_asset_scaled(fee_e18: &str, asset_scale: u32) -> Result<i64, String> {
    if fee_e18.is_empty() || fee_e18 == "0" {
        return Ok(0);
    }
    let value = fee_e18
        .parse::<u128>()
        .map_err(|err| format!("invalid fee_amount_e18 {fee_e18:?}: {err}"))?;
    if asset_scale > polyester::codecs::LEDGER_SCALE {
        return Err(format!("invalid asset scale {asset_scale}"));
    }
    let diff = polyester::codecs::LEDGER_SCALE - asset_scale;
    if diff == 0 {
        return i64::try_from(value).map_err(|_| format!("fee_amount_e18 {fee_e18:?} overflows i64"));
    }
    let divisor = 10u128
        .checked_pow(diff)
        .ok_or_else(|| format!("scale diff {diff} overflows"))?;
    if value % divisor != 0 {
        return Err(format!(
            "fee_amount_e18 {fee_e18:?} not exact at scale {asset_scale}"
        ));
    }
    i64::try_from(value / divisor).map_err(|_| "fee at asset scale overflows i64".to_owned())
}

#[tokio::test]
async fn market_buy_sell_roundtrip_carries_filled_qty() {
    if !require_mutation() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
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
    let maker = maker_client_from_env();
    if let Some(maker) = maker.as_ref() {
        let _ = hydrate_spot_and_zipper(maker).await;
        eprintln!("market roundtrip liquidity=dedicated-maker");
    } else {
        eprintln!("market roundtrip liquidity=external-orderbook");
    }
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
    let buy_ref_price = market_ref_price(&client, &symbol, "buy", Some(&pair)).await;
    let price = if maker.is_some() {
        far_above_buy_stop_price(&symbol)
    } else {
        buy_ref_price.clone()
    };
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

    if let Some(maker) = maker.as_ref() {
        let maker_params = CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Sell,
            order_type: CreateOrderType::Limit,
            quantity: Some(
                Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
            ),
            max_quote_debit_scaled: None,
            price: Some(Price::from_decimal_str(&price, Some(symbol.clone())).expect("price")),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(maker_cid.clone()),
            subaccount_id: None,
            post_only: Some(true),
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
        };
        if let Err(err) = maker.orders.create(maker_params).await {
            if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) {
                eprintln!("skip: maker create unavailable: {err}");
                return;
            }
            panic!("maker create: {err}");
        }
    }

    let buy_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity: Some(
            Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None).expect("qty"),
        ),
        max_quote_debit_scaled: None,
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(buy_cid.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: Some(
            Price::from_decimal_str(&buy_ref_price, Some(symbol.clone())).expect("buy ref price"),
        ),
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
    match client.orders.create(buy_params).await {
        Ok(_) => {}
        Err(err) if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) => {
            if let Some(maker) = maker.as_ref() {
                cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
            }
            eprintln!("skip: buy unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            if let Some(maker) = maker.as_ref() {
                cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
            }
            eprintln!("skip: notional: {err}");
            return;
        }
        Err(err) => {
            if let Some(maker) = maker.as_ref() {
                cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
            }
            panic!("buy create: {err}");
        }
    }

    let buy_detail = match wait_for_terminal_order(&client, &buy_cid, Duration::from_secs(20)).await
    {
        Ok(d) => d,
        Err(err) => {
            cancel_test_order(&client, &symbol, &buy_cid, "taker buy").await;
            if let Some(maker) = maker.as_ref() {
                cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
            }
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
        cancel_test_order(&client, &symbol, &buy_cid, "taker buy").await;
        if let Some(maker) = maker.as_ref() {
            cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
        }
        eprintln!("skip: buy produced no fill (possible POLY-3028)");
        return;
    }
    let buy_projection = client
        .orders
        .wait_for_order_trades_complete(
            polyester::models::OrderKey::ClientOrderId(buy_cid.clone()),
            Duration::from_secs(20),
        )
        .await
        .expect("BUY trade projection must reconcile with cum_qty");
    let received_fee = buy_projection
        .trades
        .iter()
        .filter(|trade| trade.fee_asset == "base")
        .map(|trade| {
            let fee = fee_amount_e18_to_asset_scaled(&trade.fee_amount_e18, scale)
                .expect("received-asset fee_amount_e18 must convert to asset scale");
            if trade.fee_is_rebate { -fee } else { fee }
        })
        .try_fold(0_i64, i64::checked_add)
        .expect("received-asset fee sum overflow");
    let net_received = filled
        .checked_sub(received_fee)
        .filter(|qty| *qty > 0)
        .expect("BUY net received quantity must be positive");

    let mut maker_buy_client_order_id = None;
    if let Some(maker) = maker.as_ref() {
        let maker_buy_price = resolve_post_only_buy_limit_price(maker, &symbol, Some(&pair)).await;
        let maker_buy_cid = unique_client_order_id("rt-maker-buy");
        let maker_buy = CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: Some(
                Quantity::from_scaled(
                    net_received,
                    Some(scale),
                    QuantityDomain::OrderBase,
                    Some(symbol.clone()),
                    None,
                )
                .expect("filled qty"),
            ),
            max_quote_debit_scaled: None,
            price: Some(
                Price::from_decimal_str(&maker_buy_price, Some(symbol.clone())).expect("price"),
            ),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(maker_buy_cid.clone()),
            subaccount_id: None,
            post_only: Some(true),
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
        };
        if let Err(err) = maker.orders.create(maker_buy).await {
            cancel_test_order(&client, &symbol, &buy_cid, "taker buy").await;
            cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
            if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) {
                eprintln!("skip: maker buy unavailable: {err}");
                return;
            }
            panic!("maker buy: {err}");
        }
        maker_buy_client_order_id = Some(maker_buy_cid);
    }

    // Carry the exact net base received into cleanup SELL. A BUY configured to
    // pay fees from the received asset cannot safely sell its gross cum_qty.
    let sell_ref_price = market_ref_price(&client, &symbol, "sell", Some(&pair)).await;
    let sell_params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Sell,
        order_type: CreateOrderType::Market,
        quantity: Some(
            Quantity::from_scaled(
                net_received,
                Some(scale),
                QuantityDomain::OrderBase,
                Some(symbol.clone()),
                None,
            )
            .expect("sell qty"),
        ),
        max_quote_debit_scaled: None,
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: Some(sell_cid.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: Some(
            Price::from_decimal_str(&sell_ref_price, Some(symbol.clone())).expect("sell ref price"),
        ),
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
    match client.orders.create(sell_params).await {
        Ok(_) => {}
        Err(err) => {
            cancel_test_order(&client, &symbol, &buy_cid, "taker buy").await;
            cancel_test_order(&client, &symbol, &sell_cid, "taker sell").await;
            if let Some(maker) = maker.as_ref() {
                cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
                if let Some(maker_buy_cid) = maker_buy_client_order_id.as_deref() {
                    cancel_test_order(maker, &symbol, maker_buy_cid, "maker bid").await;
                }
            }
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
                cancel_test_order(&client, &symbol, &buy_cid, "taker buy").await;
                cancel_test_order(&client, &symbol, &sell_cid, "taker sell").await;
                if let Some(maker) = maker.as_ref() {
                    cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
                    if let Some(maker_buy_cid) = maker_buy_client_order_id.as_deref() {
                        cancel_test_order(maker, &symbol, maker_buy_cid, "maker bid").await;
                    }
                }
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
        sell_filled, net_received,
        "cleanup SELL must use BUY net received qty"
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

    let mut reserved_expectations = vec![(base_id, base_reserved_before)];
    if let (Some(qid), Some(q_before)) = (quote_id, quote_reserved_before) {
        reserved_expectations.push((qid, q_before));
    }
    // Reserved can lag terminal order projection; poll before treating as a leak.
    if let Err(err) =
        wait_until_reserved_reconciles(&client, &reserved_expectations, Duration::from_secs(30))
            .await
    {
        panic!("reserved balance must reconcile: {err}");
    }

    // Trading base can also lag settlement briefly after a filled cleanup SELL.
    let mut base_after = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let balances_after = client
            .balances
            .list(GetBalancesRequest::default())
            .await
            .expect("balances.list after");
        let current = trading_balance_raw(&balances_after.balances, base_id);
        if current == base_before {
            base_after = Some(current);
            break;
        }
        base_after = Some(current);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        base_after.expect("base balance poll"),
        base_before,
        "residual base position must return to before (exact scaled units)"
    );
    let balances_after = client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .expect("balances.list after");
    let base_reserved_after = reserved_balance_raw(&balances_after.balances, base_id);
    assert_eq!(
        base_reserved_after, base_reserved_before,
        "base reserved balance must reconcile"
    );
    if let (Some(qid), Some(q_before)) = (quote_id, quote_reserved_before) {
        let q_after = reserved_balance_raw(&balances_after.balances, qid);
        assert_eq!(q_after, q_before, "quote reserved balance must reconcile");
    }

    // Exercise the detailed holds route when mounted. Reserved balance
    // reconciliation above remains mandatory even when this optional route is absent.
    match client
        .balances
        .list_holds(ListHoldsRequest {
            limit: 20,
            ..Default::default()
        })
        .await
    {
        Ok(_holds) => {}
        Err(err) if route_unavailable(&err) => {}
        Err(err) => {
            if strict_live_enabled() {
                panic!("list_holds after roundtrip: {err}");
            }
            eprintln!("holds reconcile skipped: {err}");
        }
    }

    if let Some(maker) = maker.as_ref() {
        cancel_test_order(maker, &symbol, &maker_cid, "maker ask").await;
        if let Some(maker_buy_cid) = maker_buy_client_order_id.as_deref() {
            cancel_test_order(maker, &symbol, maker_buy_cid, "maker bid").await;
        }
    }
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
