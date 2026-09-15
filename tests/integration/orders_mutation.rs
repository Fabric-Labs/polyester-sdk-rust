use std::time::Duration;

use crate::support::{
    DevnetOrderNotIndexedError, call_required, devnet_unavailable, hydrate_spot_and_zipper,
    is_internal_order_error, is_not_found, is_notional_validation, pair_for_symbol,
    require_account_wide_cleanup, require_funded, require_live_client, require_mutation,
    require_trading_quote_balance, trade_symbol, unique_client_order_id,
    usdt_funded_buy_limit_params, wait_for_no_open_order, wait_for_open_order,
};
use polyester::codecs::scalars::{format_price_ticks, parse_price_ticks_str};
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, GetOrderOpts,
    ModifyOrderParams, OrderKey,
};
use polyester::types::{Price, Quantity};

#[tokio::test]
async fn order_round_trip_mutation() {
    if !require_account_wide_cleanup() {
        return;
    }
    if !require_mutation() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
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

    let (price, qty) = match usdt_funded_buy_limit_params(&client, &symbol).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: buy params: {err}");
            return;
        }
    };
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let client_order_id = unique_client_order_id("e2e");

    let mut params = CreateOrderParams {
        symbol: symbol.clone(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Limit,
        quantity: match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
            Ok(q) => Some(q),
            Err(err) => {
                eprintln!("skip: qty: {err}");
                return;
            }
        },
        price: Some(
            match Price::from_decimal_str(&price, Some(symbol.clone())) {
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
        max_quote_debit_scaled: None,
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
    let _ = &mut params;

    let created = match client.orders.create(params).await {
        Ok(c) => c,
        Err(err) if is_internal_order_error(&err) || devnet_unavailable(&err) => {
            eprintln!("skip: devnet order placement unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            eprintln!("skip: order sizing below min notional: {err}");
            return;
        }
        Err(err) => panic!("orders.create: {err}"),
    };
    assert_eq!(created.client_order_id, client_order_id);
    assert!(
        !created.order_id.is_empty() && created.order_id != "0",
        "expected order_id from create"
    );

    let open_order = match wait_for_open_order(&client, &client_order_id, Duration::ZERO).await {
        Ok(o) => o,
        Err(err) if err.downcast_ref::<DevnetOrderNotIndexedError>().is_some() => {
            eprintln!(
                "skip: devnet order create accepted but orders read APIs never indexed the order"
            );
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            return;
        }
        Err(err) => panic!("wait open: {err}"),
    };
    assert_eq!(open_order.client_order_id, client_order_id);

    let detail = call_required("orders.get", || {
        client.orders.get(
            polyester::models::OrderKey::ClientOrderId(client_order_id.clone()),
            None,
        )
    })
    .await;
    let order = detail.order.expect("detail.order");
    assert_eq!(order.client_order_id, client_order_id);
    assert!(
        order
            .orig_qty
            .as_ref()
            .is_some_and(|qty| qty.as_scaled() > 0),
        "expected current accepted orig_qty, got {:?}",
        order.orig_qty
    );

    // Cancel by client id first (Go parity); cancel_all is cleanup only.
    match client
        .orders
        .cancel_by_client_order_id(&client_order_id, Some(&symbol), None)
        .await
    {
        Ok(cancelled) => {
            assert!(
                !cancelled.status.is_empty()
                    || (!cancelled.order_id.is_empty() && cancelled.order_id != "0"),
                "cancel response empty: {cancelled:?}"
            );
        }
        Err(err) if is_not_found(&err) => {
            eprintln!("skip: cancel not_found (order already gone / not indexed): {err}");
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            return;
        }
        Err(err) => {
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            panic!("cancel: {err}");
        }
    }
    if let Err(err) = wait_for_no_open_order(&client, &client_order_id, Duration::ZERO).await {
        eprintln!("skip: wait gone: {err}");
    }
    let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
}

async fn wait_listed_open(
    client: &polyester::Client,
    client_order_id: &str,
) -> std::result::Result<polyester::models::Order, Box<dyn std::error::Error + Send + Sync>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if let Ok(listed) = client.orders.list_open(None).await
            && let Some(order) = listed
                .orders
                .into_iter()
                .find(|order| order.client_order_id == client_order_id)
        {
            return Ok(order);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(Box::new(DevnetOrderNotIndexedError {
        msg: format!("open order {client_order_id} was not visible in list_open"),
    }))
}

fn one_tick_below(current_ticks: i64, tick_size: &str, fallback: &str) -> String {
    let step = parse_price_ticks_str(tick_size, "tick_size").unwrap_or(1);
    let mut current = current_ticks;
    if current <= 0 {
        current = parse_price_ticks_str(fallback, "price").unwrap_or(step) as i64;
    }
    let next = (current - step as i64).max(step as i64);
    format_price_ticks(next)
}

#[tokio::test]
async fn order_modify_advances_lineage() {
    if !require_mutation() || !require_funded() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
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
    let (price, qty) = match usdt_funded_buy_limit_params(&client, &symbol).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: buy params: {err}");
            return;
        }
    };
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let original_cid = unique_client_order_id("lin");
    let replacement_cid = unique_client_order_id("linr");

    let created = match client
        .orders
        .create(CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
                Ok(q) => Some(q),
                Err(err) => {
                    eprintln!("skip: qty: {err}");
                    return;
                }
            },
            price: Some(
                match Price::from_decimal_str(&price, Some(symbol.clone())) {
                    Ok(p) => p,
                    Err(err) => {
                        eprintln!("skip: price: {err}");
                        return;
                    }
                },
            ),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(original_cid.clone()),
            subaccount_id: None,
            post_only: Some(true),
            market_client_ref_price: None,
            max_quote_debit_scaled: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
        })
        .await
    {
        Ok(c) => c,
        Err(err) if is_internal_order_error(&err) || devnet_unavailable(&err) => {
            eprintln!("skip: create unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            eprintln!("skip: min notional: {err}");
            return;
        }
        Err(err) => panic!("orders.create: {err}"),
    };

    let cleanup = || async {
        let _ = client
            .orders
            .cancel_by_client_order_id(&replacement_cid, Some(&symbol), None)
            .await;
        let _ = client
            .orders
            .cancel_by_client_order_id(&original_cid, Some(&symbol), None)
            .await;
    };

    if let Err(err) = wait_listed_open(&client, &original_cid).await {
        cleanup().await;
        if err.downcast_ref::<DevnetOrderNotIndexedError>().is_some() {
            eprintln!("skip: create accepted but not indexed");
            return;
        }
        panic!("wait open: {err}");
    }

    let first = match client
        .orders
        .get_with(GetOrderOpts {
            key: OrderKey::ClientOrderId(original_cid.clone()),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(false),
            limit: None,
            page_token: None,
        })
        .await
    {
        Ok(v) => v,
        Err(err) => {
            cleanup().await;
            panic!("orders.get original: {err}");
        }
    };
    let first_order = first.order.expect("original order");
    let original_lineage_id = first_order
        .lineage
        .as_ref()
        .map(|lineage| lineage.id.clone())
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| created.order_id.clone());
    let original_generation = first_order
        .lineage
        .as_ref()
        .map(|lineage| lineage.generation)
        .unwrap_or(1);
    let tick_size = pair_for_symbol(&spot, &symbol)
        .map(|pair| {
            if pair.tick_size.trim().is_empty() {
                "0.01".to_string()
            } else {
                pair.tick_size
            }
        })
        .unwrap_or_else(|| "0.01".into());
    let current_ticks = first_order.price.as_ref().map(Price::as_ticks).unwrap_or(0);
    let new_price = one_tick_below(current_ticks, &tick_size, &price);

    let modified = match client
        .orders
        .modify(ModifyOrderParams {
            symbol: symbol.clone(),
            key: OrderKey::ClientOrderId(original_cid.clone()),
            subaccount_id: None,
            request_id: None,
            new_price: Some(
                match Price::from_decimal_str(&new_price, Some(symbol.clone())) {
                    Ok(p) => p,
                    Err(err) => {
                        cleanup().await;
                        panic!("new price: {err}");
                    }
                },
            ),
            new_qty: None,
            new_attached_risk: None,
            behavior: Some("replace_only".into()),
            new_client_order_id: Some(replacement_cid.clone()),
        })
        .await
    {
        Ok(v) => v,
        Err(err) if is_internal_order_error(&err) || devnet_unavailable(&err) => {
            cleanup().await;
            eprintln!("skip: modify unavailable: {err}");
            return;
        }
        Err(err) => {
            cleanup().await;
            panic!("orders.modify: {err}");
        }
    };
    assert!(!modified.action_taken.is_empty());
    assert_eq!(modified.old_order_id, created.order_id);
    assert!(!modified.final_order_id.is_empty());

    if let Err(err) = wait_listed_open(&client, &replacement_cid).await {
        cleanup().await;
        if err.downcast_ref::<DevnetOrderNotIndexedError>().is_some() {
            eprintln!("skip: modify accepted but replacement not indexed");
            return;
        }
        panic!("wait replacement: {err}");
    }

    let history = match client
        .orders
        .get_with(GetOrderOpts {
            key: OrderKey::OrderId(modified.final_order_id.clone()),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(false),
            limit: None,
            page_token: None,
        })
        .await
    {
        Ok(v) => v,
        Err(err) => {
            cleanup().await;
            panic!("orders.get replacement: {err}");
        }
    };
    let order = history.order.expect("replacement order");
    let lineage = order.lineage.expect("replacement lineage");
    assert_eq!(lineage.id, original_lineage_id);
    if modified.final_order_id != created.order_id {
        assert_eq!(lineage.generation, original_generation + 1);
    }
    cleanup().await;
}
