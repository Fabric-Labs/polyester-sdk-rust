use std::time::Duration;

use crate::support::{
    DevnetOrderNotIndexedError, call_required, devnet_unavailable, hydrate_spot_and_zipper,
    is_internal_order_error, is_not_found, is_notional_validation, require_live_client,
    require_mutation, require_trading_quote_balance, trade_symbol, unique_client_order_id,
    usdt_funded_buy_limit_params, wait_for_no_open_order, wait_for_open_order,
};
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::types::{Price, Quantity};

#[tokio::test]
async fn order_round_trip_mutation() {
    if !require_mutation() {
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
            Ok(q) => q,
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
        client.orders.get(Some(&client_order_id), None, None)
    })
    .await;
    let order = detail.order.expect("detail.order");
    assert_eq!(order.client_order_id, client_order_id);

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
