use std::time::Duration;

use crate::support::{
    call_optional, devnet_unavailable, hydrate_spot_and_zipper, is_internal_order_error,
    is_notional_validation, is_terminal_status, market_ref_price, min_base_qty_for_pair,
    mutation_test_guard, order_status_label, pair_for_symbol, require_live_client, require_mutation,
    require_trading_quote_balance, smoke_symbol, unique_client_order_id, wait_for_terminal_order,
};
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, MaxSlippage,
    PreviewOrderParams, PreviewOrderResult,
};
use polyester::types::{Price, Quantity};

fn fingerprint(preview: &PreviewOrderResult) -> String {
    let bound = preview
        .protected_price_bound
        .as_ref()
        .map(|p| p.as_ticks().to_string())
        .unwrap_or_else(|| "none".to_owned());
    let code = preview
        .rejection
        .as_ref()
        .map(|r| r.code.as_str())
        .unwrap_or("");
    format!(
        "admissible={:?} bound={bound} code={code}",
        preview.admissible
    )
}

fn assert_host_accepted_slippage(label: &str, preview: &PreviewOrderResult) {
    let Some(rejection) = preview.rejection.as_ref() else {
        return;
    };
    let mut blob = rejection.code.to_ascii_lowercase();
    for v in &rejection.violations {
        blob.push(' ');
        blob.push_str(&v.field_path.to_ascii_lowercase());
        blob.push(' ');
        blob.push_str(&v.message.to_ascii_lowercase());
    }
    for hint in [
        "unknown field",
        "unknown argument",
        "no such field",
        "unrecognized field",
        "unsupported create",
    ] {
        assert!(
            !blob.contains(hint),
            "{label} treated slippage as unknown/unsupported: {blob}"
        );
    }
}

async fn market_preview_params(
    client: &polyester::Client,
    symbol: &str,
    slippage: Option<MaxSlippage>,
) -> Option<PreviewOrderParams> {
    let spot = match hydrate_spot_and_zipper(client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate: {err}");
            return None;
        }
    };
    let Some(pair) = pair_for_symbol(&spot, symbol) else {
        eprintln!("skip: symbol {symbol} not in spot config");
        return None;
    };
    let ref_price = market_ref_price(client, symbol, "buy", Some(&pair)).await;
    let qty = min_base_qty_for_pair(Some(&pair), &ref_price);
    let scale = match client.catalogs.base_quantity_scale_for_symbol(symbol) {
        Some(s) => s,
        None => {
            eprintln!("skip: catalog scale missing for {symbol}");
            return None;
        }
    };
    let quantity = match Quantity::from_decimal_str(&qty, scale, Some(symbol.to_owned()), None) {
        Ok(q) => Some(q),
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return None;
        }
    };
    let market_client_ref_price = match Price::from_decimal_str(&ref_price, Some(symbol.to_owned()))
    {
        Ok(p) => Some(p),
        Err(err) => {
            eprintln!("skip: ref price: {err}");
            return None;
        }
    };
    Some(PreviewOrderParams {
        symbol: symbol.to_owned(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity,
        max_quote_debit_scaled: None,
        price: None,
        time_in_force: Some(CreateTimeInForce::Ioc),
        client_order_id: None,
        subaccount_id: None,
        post_only: None,
        market_client_ref_price,
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: slippage,
        attached_risk: None,
        expire_at: None,
    })
}

#[tokio::test]
async fn preview_market_ioc_accepts_slippage_overrides() {
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
    let Some(base) = market_preview_params(&client, &symbol, None).await else {
        return;
    };
    let mut bps = base.clone();
    bps.market_max_slippage = Some(MaxSlippage::Bps(25));
    let mut ticks = base.clone();
    ticks.market_max_slippage = Some(MaxSlippage::Ticks(1));

    let Some(unset) = call_optional("orders.preview unset", || client.orders.preview(base)).await
    else {
        return;
    };
    let Some(bps) = call_optional("orders.preview bps", || client.orders.preview(bps)).await else {
        return;
    };
    let Some(ticks) = call_optional("orders.preview ticks", || client.orders.preview(ticks)).await
    else {
        return;
    };
    assert_host_accepted_slippage("unset", &unset);
    assert_host_accepted_slippage("bps", &bps);
    assert_host_accepted_slippage("ticks", &ticks);
    let fp_unset = fingerprint(&unset);
    let fp_bps = fingerprint(&bps);
    let fp_ticks = fingerprint(&ticks);
    eprintln!("preview fingerprints unset={fp_unset} bps={fp_bps} ticks={fp_ticks}");
    if unset.protected_price_bound.is_some() && fp_unset == fp_bps && fp_unset == fp_ticks {
        panic!(
            "slippage overrides did not change preview vs pair default; host may still be ignoring the field: {fp_unset}"
        );
    }
}

#[tokio::test]
async fn create_market_ioc_with_slippage_bps() {
    if !require_mutation() {
        return;
    }
    let _guard = mutation_test_guard().await;
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
    let Some(preview) = market_preview_params(&client, &symbol, Some(MaxSlippage::Bps(25))).await
    else {
        return;
    };
    let client_order_id = unique_client_order_id("mkt-slip");
    let params = CreateOrderParams {
        symbol: symbol.clone(),
        side: preview.side,
        order_type: preview.order_type,
        quantity: preview.quantity,
        max_quote_debit_scaled: None,
        price: None,
        time_in_force: preview.time_in_force,
        client_order_id: Some(client_order_id.clone()),
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: preview.market_client_ref_price,
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: Some(MaxSlippage::Bps(25)),
        attached_risk: None,
        expire_at: None,
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
    eprintln!(
        "create status={} order_id={} client_order_id={}",
        created.status, created.order_id, created.client_order_id
    );
    let _ = client
        .orders
        .cancel_by_client_order_id(&client_order_id, Some(&symbol), None)
        .await;
    let status = created.status.to_ascii_lowercase();
    if is_terminal_status(&status) || status == "accepted" {
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
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("did not reach terminal") && !msg.contains("stuck in status") {
                return;
            }
            panic!("terminal wait: {err}");
        }
    }
}
