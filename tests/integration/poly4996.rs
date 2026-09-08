//! Live API-key coverage for POLY-4996 wrappers (read-only / dry-run).

use crate::support::{
    call_optional, call_required, hydrate_spot_and_zipper, pair_for_symbol, require_live_client,
    require_mutation, smoke_symbol, trade_symbol, unique_client_order_id,
    usdt_funded_buy_limit_params, wait_for_trigger,
};
use polyester::codecs::scalars::LEDGER_SCALE;
use polyester::codecs::scalars::parse_price_ticks_str;
use polyester::errors::Error;
use polyester::models::{
    CancelAllOpts, CreateApiKeyTradingWithdrawParams, CreateInternalTransferParams,
    CreateOrderType, CreateSide, CreateTriggerParams, CreateTriggerType, TriggerDetails,
};
use polyester::types::{AssetAmount, Price, Quantity, QuantityDomain};

fn assert_optional_scaled(value: Option<&str>, label: &str) {
    let Some(raw) = value.filter(|item| !item.is_empty()) else {
        return;
    };
    let parsed: f64 = raw
        .parse()
        .unwrap_or_else(|err| panic!("{label} is not a number: {raw:?} ({err})"));
    assert!(parsed >= 0.0, "{label} must be non-negative: {raw}");
}

#[tokio::test]
async fn spot_volume_history_by_symbol() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let symbols = [symbol.clone()];
    let Some(result) = call_optional("market_overview.get_spot_volume_history", || {
        client
            .market_overview
            .get_spot_volume_history(Some(&symbols), None)
    })
    .await
    else {
        return;
    };
    assert!(result.end_ts_sec >= result.start_ts_sec);
    if result.points > 0 {
        assert_eq!(result.total_volume_usd_scaled.len(), result.points as usize);
    }
    let want_id = client.catalogs.symbol_id_for_symbol(&symbol);
    for pair in &result.pairs {
        assert!(pair.symbol_id > 0, "pair missing symbol_id: {pair:?}");
        assert_eq!(pair.volume_usd_scaled.len(), result.points as usize);
        if let Some(want_id) = want_id {
            assert_eq!(pair.symbol_id, want_id);
        }
    }
}

#[tokio::test]
async fn spot_volume_history_by_symbol_id() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let Some(symbol_id) = client.catalogs.symbol_id_for_symbol(&symbol) else {
        eprintln!("skip: catalog missing symbol_id for {symbol}");
        return;
    };
    let symbol_ids = [symbol_id];
    let Some(result) = call_optional(
        "market_overview.get_spot_volume_history(symbol_ids)",
        || {
            client
                .market_overview
                .get_spot_volume_history(None, Some(&symbol_ids))
        },
    )
    .await
    else {
        return;
    };
    for pair in &result.pairs {
        assert_eq!(pair.symbol_id, symbol_id);
    }
}

#[tokio::test]
async fn candles_preserve_quote_volume() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let result = call_required("market_data.get_candles", || {
        client.market_data.get_candles(&symbol, "1m", Some(5))
    })
    .await;
    for candle in &result.candles {
        assert_optional_scaled(Some(candle.quote_volume.as_str()), "candle.quote_volume");
        assert_optional_scaled(Some(candle.volume.as_str()), "candle.volume");
    }
}

#[tokio::test]
async fn market_overview_optional_volume_fields() {
    let Some(client) = require_live_client() else {
        return;
    };
    let result = call_required("market_overview.list", || {
        client.market_overview.list(Some(10))
    })
    .await;
    for market in &result.markets {
        assert!(market.symbol_id > 0, "market missing symbol_id: {market:?}");
        assert_optional_scaled(
            market.volume_24h_base_scaled.as_deref(),
            "volume_24h_base_scaled",
        );
        assert_optional_scaled(
            market.volume_24h_quote_scaled.as_deref(),
            "volume_24h_quote_scaled",
        );
        assert_optional_scaled(
            market.volume_24h_usd_scaled.as_deref(),
            "volume_24h_usd_scaled",
        );
    }
}

#[tokio::test]
async fn cancel_all_dry_run_symbols_and_symbol_ids() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let Some(symbol_id) = client.catalogs.symbol_id_for_symbol(&symbol) else {
        eprintln!("skip: catalog missing symbol_id for {symbol}");
        return;
    };

    let by_symbols = call_required("orders.cancel_all(symbols)", || {
        client.orders.cancel_all_with(CancelAllOpts {
            symbols: vec![symbol.clone()],
            dry_run: true,
            ..Default::default()
        })
    })
    .await;
    assert!(
        !by_symbols.status.is_empty(),
        "expected status: {by_symbols:?}"
    );
    assert_eq!(by_symbols.submitted_cancels, 0);

    let by_ids = call_required("orders.cancel_all(symbol_ids)", || {
        client.orders.cancel_all_with(CancelAllOpts {
            symbol_ids: vec![symbol_id],
            dry_run: true,
            ..Default::default()
        })
    })
    .await;
    assert!(!by_ids.status.is_empty(), "expected status: {by_ids:?}");
    assert_eq!(by_ids.submitted_cancels, 0);
}

#[tokio::test]
async fn trigger_list_decodes_ladder_executed_fields() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(result) = call_optional("triggers.list", || {
        client
            .triggers
            .list(polyester::proto::triggers::v1::ListTriggersRequest::default())
    })
    .await
    else {
        return;
    };
    for trigger in &result.triggers {
        let Some(TriggerDetails::Ladder(ladder)) = &trigger.details else {
            continue;
        };
        assert!(
            ladder.executed_levels >= 0,
            "executed_levels={}",
            ladder.executed_levels
        );
        let _ = &ladder.executed_qty;
    }
}

#[tokio::test]
async fn create_ladder_decodes_executed_fields() {
    if !require_mutation() {
        return;
    }
    let _guard = crate::support::mutation_test_guard().await;
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = trade_symbol(&spot);
    let (price, qty) = match usdt_funded_buy_limit_params(&client, &symbol).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: ladder sizing: {err}");
            return;
        }
    };
    let max_price = match Price::from_decimal_str(&price, Some(symbol.clone())) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("skip: ladder max price: {err}");
            return;
        }
    };
    let tick_size = pair_for_symbol(&spot, &symbol)
        .map(|pair| {
            if pair.tick_size.trim().is_empty() {
                "0.01".to_owned()
            } else {
                pair.tick_size
            }
        })
        .unwrap_or_else(|| "0.01".to_owned());
    let tick_ticks = match parse_price_ticks_str(&tick_size, "tick_size") {
        Ok(ticks) if ticks > 0 => ticks,
        _ => 1,
    };
    let mut min_ticks = max_price.as_ticks() * 8 / 10 / tick_ticks * tick_ticks;
    if min_ticks < tick_ticks {
        min_ticks = tick_ticks;
    }
    if min_ticks <= 0 || min_ticks >= max_price.as_ticks() {
        eprintln!("skip: could not form a valid far-below ladder range");
        return;
    }
    let min_price = match Price::from_ticks(min_ticks, Some(symbol.clone())) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("skip: ladder min price: {err}");
            return;
        }
    };
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let per_level = match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };
    let total_qty = match Quantity::from_scaled(
        per_level.as_scaled() * 2,
        per_level.scale(),
        per_level.domain(),
        Some(symbol.clone()),
        None,
    ) {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: total qty: {err}");
            return;
        }
    };

    let params = CreateTriggerParams {
        symbol: symbol.clone(),
        trigger_type: CreateTriggerType::Ladder,
        side: CreateSide::Buy,
        order_type: CreateOrderType::Limit,
        qty: total_qty,
        trigger_price: None,
        limit_price: Some(max_price.clone()),
        trigger_price_source: None,
        time_in_force: None,
        subaccount_id: None,
        client_trigger_id: unique_client_order_id("trg-ladder-exec"),
        post_only: false,
        activation_price: None,
        trailing_distance_ticks: None,
        trailing_distance_bps: None,
        max_slippage_ticks: None,
        max_slippage_bps: None,
        twap_duration_ms: None,
        twap_slice_interval_ms: None,
        ladder_price_min: Some(min_price),
        ladder_price_max: Some(max_price),
        ladder_levels: Some(2),
        ladder_distribution: Some("linear".into()),
        fee_asset: None,
        self_trade_prevention_mode: None,
    };
    match client.triggers.create(params).await {
        Ok(created) => {
            assert!(!created.trigger_id.is_empty());
            assert_eq!(created.status, "accepted");
            let trigger =
                match wait_for_trigger(&client, &created.trigger_id, std::time::Duration::ZERO)
                    .await
                {
                    Ok(trigger) => trigger,
                    Err(err) => {
                        let _ = client
                            .triggers
                            .cancel_by_id(&created.trigger_id, None)
                            .await;
                        panic!("triggers.get: {err}");
                    }
                };
            let Some(TriggerDetails::Ladder(ladder)) = &trigger.details else {
                let _ = client
                    .triggers
                    .cancel_by_id(&created.trigger_id, None)
                    .await;
                panic!("expected ladder details, got {:?}", trigger.details);
            };
            assert!(
                ladder.executed_levels >= 0,
                "executed_levels={}",
                ladder.executed_levels
            );
            if let Some(executed_qty) = &ladder.executed_qty {
                assert!(executed_qty.as_scaled() >= 0);
            }
            let _ = client
                .triggers
                .cancel_by_id(&created.trigger_id, None)
                .await;
        }
        Err(err) => {
            let message = err.to_string().to_ascii_lowercase();
            if message.contains("notional")
                || message.contains("insufficient")
                || message.contains("not supported")
                || message.contains("internal")
            {
                eprintln!("skip: ladder create: {err}");
                return;
            }
            panic!("ladder create failed: {err}");
        }
    }
}

const DEAD_SMART_ACCOUNT: &str = "0x000000000000000000000000000000000000dEaD";
const IMPOSSIBLE_WITHDRAW_QTY: &str = "999999999";

fn assert_typed_money_error(err: &Error, label: &str) {
    match err {
        Error::Api { code, .. }
            if code.starts_with("ERROR_CODE_") || code.starts_with("UNKNOWN_ERROR_CODE") => {}
        Error::Auth(_) | Error::PermissionDenied { .. } => {}
        Error::RouteNotFound { .. } => {
            panic!("{label} not mounted on API host: {err}");
        }
        other => panic!("{label} did not raise a mapped SDK error: {other}"),
    }
}

#[tokio::test]
async fn twap_market_ioc_accepts_slippage() {
    if !require_mutation() {
        return;
    }
    let _guard = crate::support::mutation_test_guard().await;
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = trade_symbol(&spot);
    let (_price, qty) = match usdt_funded_buy_limit_params(&client, &symbol).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: twap sizing: {err}");
            return;
        }
    };
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let qty = match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };

    for (ticks, bps, prefix) in [
        (None, Some(25_i32), "trg-twap-bps"),
        (Some(1_i32), None, "trg-twap-ticks"),
    ] {
        let params = CreateTriggerParams {
            symbol: symbol.clone(),
            trigger_type: CreateTriggerType::Twap,
            side: CreateSide::Buy,
            order_type: CreateOrderType::Market,
            qty: qty.clone(),
            trigger_price: None,
            limit_price: None,
            trigger_price_source: None,
            time_in_force: None,
            subaccount_id: None,
            client_trigger_id: unique_client_order_id(prefix),
            post_only: false,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: ticks,
            max_slippage_bps: bps,
            twap_duration_ms: Some(600_000),
            twap_slice_interval_ms: Some(300_000),
            ladder_price_min: None,
            ladder_price_max: None,
            ladder_levels: None,
            ladder_distribution: None,
            fee_asset: None,
            self_trade_prevention_mode: None,
        };
        match client.triggers.create(params).await {
            Ok(created) => {
                assert!(!created.trigger_id.is_empty());
                assert_eq!(created.status, "accepted");
                let _ = client
                    .triggers
                    .cancel_by_id(&created.trigger_id, None)
                    .await;
            }
            Err(err) => {
                let message = err.to_string().to_ascii_lowercase();
                if message.contains("notional")
                    || message.contains("insufficient")
                    || message.contains("not supported")
                    || message.contains("internal")
                {
                    eprintln!("skip: twap create {prefix}: {err}");
                    return;
                }
                panic!("twap create {prefix} failed: {err}");
            }
        }
    }
}

#[tokio::test]
async fn social_verification_discord_without_handle() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("social_verification.start(discord)", || {
        client.social_verification.start("discord", "", "")
    })
    .await;
    let _ = call_optional("social_verification.get(discord)", || {
        client.social_verification.get("discord")
    })
    .await;
}

#[tokio::test]
async fn withdraw_error_detail_from_impossible_funding_move() {
    if !require_mutation() {
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = trade_symbol(&spot);
    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset_id) = crate::support::quote_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve quote asset for {symbol}");
        return;
    };
    let amount = AssetAmount::from_decimal_str(
        IMPOSSIBLE_WITHDRAW_QTY,
        LEDGER_SCALE,
        QuantityDomain::LedgerE18,
        Some(asset_id),
    )
    .expect("withdraw amount");
    let err = match client
        .withdraw
        .create_api_key_to_funding(CreateApiKeyTradingWithdrawParams {
            asset_id,
            amount,
            destination_address: String::new(),
            idempotency_key: unique_client_order_id("poly4996-wd"),
            amount_scale: Some(LEDGER_SCALE),
            deadline_ts_sec: None,
            nonce: None,
        })
        .await
    {
        Ok(_) => panic!("expected withdraw ErrorDetail for an impossible funding amount"),
        Err(err) => err,
    };
    assert_typed_money_error(&err, "withdraw.create_api_key_to_funding");
}

#[tokio::test]
async fn transfer_error_detail_from_unknown_destination() {
    if !require_mutation() {
        return;
    }
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = trade_symbol(&spot);
    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset_id) = crate::support::quote_asset_id(&spot, &symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve quote asset for {symbol}");
        return;
    };
    let quantity = AssetAmount::from_decimal_str(
        "0.000001",
        LEDGER_SCALE,
        QuantityDomain::LedgerE18,
        Some(asset_id),
    )
    .expect("transfer amount");
    let err = match client
        .internal_transfers
        .create(CreateInternalTransferParams {
            asset_id,
            quantity,
            idempotency_key: unique_client_order_id("poly4996-xfer"),
            subaccount_id: None,
            destination_account_id: None,
            destination_subaccount_id: None,
            destination_smart_account_address: Some(DEAD_SMART_ACCOUNT.to_string()),
            quantity_scale: Some(LEDGER_SCALE),
        })
        .await
    {
        Ok(_) => panic!("expected transfer ErrorDetail for an unknown destination"),
        Err(err) => err,
    };
    assert_typed_money_error(&err, "internal_transfers.create");
}
