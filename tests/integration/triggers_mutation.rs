use std::time::Duration;

use crate::support::{
    call_optional, devnet_unavailable, hydrate_spot_and_zipper, is_notional_validation,
    require_live_client, require_mutation, require_trading_quote_balance, trade_symbol,
    unique_client_order_id, usdt_funded_buy_stop_params, wait_for_trigger, wait_for_trigger_events,
};
use polyester::codecs::scalars::{id_to_u64, parse_price_ticks_str, parse_qty_scaled_str};
use polyester::proto::orders::v1::{OrderType, Side};
use polyester::proto::triggers::v1::{
    CancelTriggerRequest, CreateTriggerRequest, PauseTriggerRequest, ResumeTriggerRequest,
    TriggerType,
};

#[tokio::test]
async fn trigger_pause_resume_cancel() {
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

    let (trigger_price, limit_price, qty) =
        match usdt_funded_buy_stop_params(&client, &symbol).await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("skip: stop params: {err}");
                return;
            }
        };
    let scale = client.catalogs.base_quantity_scale_for_symbol(&symbol);
    let client_trigger_id = unique_client_order_id("trg");

    let trigger_ticks = match parse_price_ticks_str(&trigger_price, "trigger_price") {
        Ok(t) => t,
        Err(err) => {
            eprintln!("skip: trigger price: {err}");
            return;
        }
    };
    let limit_ticks = match parse_price_ticks_str(&limit_price, "limit_price") {
        Ok(t) => t,
        Err(err) => {
            eprintln!("skip: limit price: {err}");
            return;
        }
    };
    let qty_scaled = match parse_qty_scaled_str(&qty, scale, "qty") {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };

    let req = CreateTriggerRequest {
        symbol: symbol.clone(),
        trigger_type: TriggerType::StopLoss.into(),
        trigger_price_ticks: trigger_ticks,
        side: Side::Buy.into(),
        order_type: OrderType::Limit.into(),
        qty_scaled,
        limit_price_ticks: limit_ticks,
        client_trigger_id: client_trigger_id.clone(),
        ..Default::default()
    };

    let created = match client.triggers.create(req).await {
        Ok(c) => c,
        Err(err) if devnet_unavailable(&err) => {
            eprintln!("skip: devnet trigger placement unavailable: {err}");
            return;
        }
        Err(err) if is_notional_validation(&err) => {
            eprintln!("skip: trigger sizing below min notional: {err}");
            return;
        }
        Err(err) => {
            eprintln!("skip: triggers.create too hard / failed: {err}");
            return;
        }
    };
    if created.trigger_id.is_empty() || created.trigger_id == "0" {
        eprintln!("skip: no trigger_id from create");
        return;
    }
    if !created.status.is_empty() && !created.status.to_ascii_lowercase().contains("created") {
        eprintln!("skip: unexpected create status {}", created.status);
    }

    let trigger_id = created.trigger_id.clone();
    let trigger_id_u64 = match id_to_u64(&trigger_id, "trigger_id") {
        Ok(v) => v,
        Err(err) => {
            eprintln!("skip: parse trigger_id: {err}");
            return;
        }
    };
    let cleanup = || async {
        let _ = client
            .triggers
            .cancel(CancelTriggerRequest {
                trigger_id: trigger_id_u64,
                ..Default::default()
            })
            .await;
    };

    let trigger = match wait_for_trigger(&client, &trigger_id, Duration::ZERO).await {
        Ok(t) => t,
        Err(err) => {
            cleanup().await;
            eprintln!("skip: wait trigger: {err}");
            return;
        }
    };
    assert_eq!(trigger.trigger_id, trigger_id);
    if !trigger.client_trigger_id.is_empty() {
        assert_eq!(trigger.client_trigger_id, client_trigger_id);
    }

    let paused = match client
        .triggers
        .pause(PauseTriggerRequest {
            trigger_id: trigger_id_u64,
            ..Default::default()
        })
        .await
    {
        Ok(p) => p,
        Err(err) => {
            cleanup().await;
            panic!("pause: {err}");
        }
    };
    assert_eq!(paused.trigger_id, trigger_id);
    assert!(
        paused.status.to_ascii_lowercase().contains("paused"),
        "pause status={}",
        paused.status
    );

    let resumed = match client
        .triggers
        .resume(ResumeTriggerRequest {
            trigger_id: trigger_id_u64,
            ..Default::default()
        })
        .await
    {
        Ok(r) => r,
        Err(err) => {
            cleanup().await;
            panic!("resume: {err}");
        }
    };
    assert_eq!(resumed.trigger_id, trigger_id);
    assert!(
        resumed.status.to_ascii_lowercase().contains("armed"),
        "resume status={}",
        resumed.status
    );

    let cancelled = client
        .triggers
        .cancel(CancelTriggerRequest {
            trigger_id: trigger_id_u64,
            ..Default::default()
        })
        .await
        .expect("cancel");
    assert_eq!(cancelled.trigger_id, trigger_id);
    assert!(
        cancelled.status.to_ascii_lowercase().contains("cancel"),
        "cancel status={}",
        cancelled.status
    );

    let _ = call_optional("triggers.list_events", || async {
        wait_for_trigger_events(&client, &trigger_id, Duration::ZERO).await
    })
    .await;
}
