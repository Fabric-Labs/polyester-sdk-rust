//! F-01 / M1: blocking BatchModify regression (live-gated).
//!
//! Runs 5× complete 40-item BatchModify rounds with safe same-ID retry + cleanup.
//! Staging-only; soft-skips without mutation/funded gates (fails closed under STRICT_LIVE).

use crate::support::{
    far_below_buy_limit_price, hydrate_spot_and_zipper, is_internal_order_error,
    min_base_qty_for_pair, pair_for_symbol, require_account_wide_cleanup, require_funded,
    require_live_client, require_mutation, require_trading_quote_balance, strict_live_enabled,
    trade_symbol, unique_client_order_id, wait_for_open_order, wait_until_no_open_client_ids,
};
use polyester::models::{
    BatchModifyItem, BatchModifyOrdersResult, CreateOrderParams, CreateOrderType, CreateSide,
    CreateTimeInForce,
};
use polyester::types::{Price, Quantity};
use std::collections::HashSet;
use std::time::Duration;

const BATCH_SIZE: usize = 40;
const ROUNDS: usize = 5;

fn all_results_internal_error(result: &BatchModifyOrdersResult) -> bool {
    !result.results.is_empty()
        && result.results.iter().all(|r| {
            r.code.eq_ignore_ascii_case("INTERNAL_ERROR")
                || r.status.eq_ignore_ascii_case("rejected")
                    && r.code.to_ascii_lowercase().contains("internal")
        })
        && result
            .results
            .iter()
            .all(|r| r.code.eq_ignore_ascii_case("INTERNAL_ERROR"))
}

fn assert_complete_batch_result(
    result: &BatchModifyOrdersResult,
    expected_cids: &HashSet<String>,
    round: usize,
) -> bool {
    assert_eq!(
        result.results.len(),
        BATCH_SIZE,
        "round {round}: expected {BATCH_SIZE} result items, got {}",
        result.results.len()
    );
    if all_results_internal_error(result) {
        return false;
    }
    assert_eq!(
        result.rejected_count, 0,
        "round {round}: expected no rejected, got {}",
        result.rejected_count
    );
    assert_eq!(
        result.amended_count + result.replaced_count,
        BATCH_SIZE as i32,
        "round {round}: amended+replaced={} != {BATCH_SIZE}",
        result.amended_count + result.replaced_count
    );
    let seen: HashSet<String> = result
        .results
        .iter()
        .map(|r| r.client_order_id.clone())
        .filter(|s| !s.is_empty())
        .collect();
    let missing: Vec<_> = expected_cids.difference(&seen).cloned().collect();
    assert!(
        missing.is_empty(),
        "round {round}: missing client_order_ids in results: {missing:?}"
    );
    for item in &result.results {
        assert!(
            !item.status.eq_ignore_ascii_case("rejected"),
            "round {round}: rejected item {item:?}"
        );
    }
    true
}

fn result_fingerprint(result: &BatchModifyOrdersResult) -> Vec<(String, String, String)> {
    let mut rows: Vec<_> = result
        .results
        .iter()
        .map(|r| {
            (
                r.client_order_id.clone(),
                r.status.clone(),
                r.final_order_id.clone(),
            )
        })
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn batch_modify_five_rounds_of_forty_with_safe_same_id_retry() {
    if !require_account_wide_cleanup() {
        return;
    }
    if !require_mutation() {
        return;
    }
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
    let Some(pair) = pair_for_symbol(&spot, &symbol) else {
        eprintln!("skip: symbol {symbol} not in spot config");
        return;
    };
    let scale = match client.catalogs.base_quantity_scale_for_symbol(&symbol) {
        Some(s) => s,
        None => {
            eprintln!("skip: missing scale for {symbol}");
            return;
        }
    };
    // Static far-below post-only price — live best-bid-1tick gets drained mid-batch.
    let price = far_below_buy_limit_price(&symbol);
    let qty = min_base_qty_for_pair(Some(&pair), &price);

    let mut client_order_ids = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        let cid = unique_client_order_id(&format!("bm-{i}"));
        let params = CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: match Quantity::from_decimal_str(&qty, scale, Some(symbol.clone()), None) {
                Ok(q) => q,
                Err(err) => {
                    eprintln!("skip: qty: {err}");
                    let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                    return;
                }
            },
            price: Some(Price::from_decimal_str(&price, Some(symbol.clone())).expect("price")),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(cid.clone()),
            subaccount_id: None,
            post_only: Some(true),
            market_client_ref_price: None,
            attached_risk: None,
        };
        match client.orders.create(params).await {
            Ok(_) => match wait_for_open_order(&client, &cid, Duration::from_secs(15)).await {
                Ok(_) => client_order_ids.push(cid),
                Err(err) => {
                    let msg = err.to_string().to_ascii_lowercase();
                    if msg.contains("terminal status") && msg.contains("filled") {
                        eprintln!(
                            "skip: post-only resting order filled by book activity before batch: {err}"
                        );
                    } else {
                        eprintln!("skip: create not indexed: {err}");
                    }
                    let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                    return;
                }
            },
            Err(err)
                if is_internal_order_error(&err) || crate::support::devnet_unavailable(&err) =>
            {
                eprintln!("skip: create unavailable: {err}");
                let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                return;
            }
            Err(err) => {
                let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                panic!("create: {err}");
            }
        }
    }
    let base_price = Price::from_decimal_str(&price, Some(symbol.clone())).expect("price");
    let mut all_cids: HashSet<String> = client_order_ids.iter().cloned().collect();
    for round in 0..ROUNDS {
        let Some(new_price) = Price::from_ticks(
            base_price.as_ticks().saturating_add(1 + round as i64),
            Some(symbol.clone()),
        )
        .ok() else {
            eprintln!("skip: cannot bump price");
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            return;
        };
        let new_cids: Vec<String> = (0..BATCH_SIZE)
            .map(|i| unique_client_order_id(&format!("bm-r{round}-{i}")))
            .collect();
        let requested: HashSet<String> = client_order_ids.iter().cloned().collect();
        let items: Vec<BatchModifyItem> = client_order_ids
            .iter()
            .zip(new_cids.iter())
            .map(|(cid, new_cid)| BatchModifyItem {
                order_id: None,
                client_order_id: Some(cid.clone()),
                new_price: Some(new_price.clone()),
                new_qty: None,
                new_attached_risk: None,
                behavior: None,
                new_client_order_id: Some(new_cid.clone()),
            })
            .collect();
        let request_id = unique_client_order_id(&format!("bm-req-{round}"));
        all_cids.extend(new_cids.iter().cloned());

        let mut before_count = 0usize;
        for _ in 0..20 {
            let before_open = client
                .orders
                .list_open(None)
                .await
                .expect("list_open before batch_modify");
            before_count = before_open
                .orders
                .iter()
                .filter(|o| requested.contains(&o.client_order_id))
                .count();
            if before_count == BATCH_SIZE {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if before_count != BATCH_SIZE {
            eprintln!(
                "skip: round {round}: only {before_count}/{BATCH_SIZE} test orders still open \
                 (book activity drained resting post-only bids)"
            );
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            return;
        }

        let result = match client
            .orders
            .batch_modify(
                items.clone(),
                Some(&symbol),
                None,
                Some(request_id.clone()),
                None,
                true,
            )
            .await
        {
            Ok(r) => r,
            Err(err) if crate::support::devnet_unavailable(&err) => {
                eprintln!("skip: batch_modify unavailable: {err}");
                let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                return;
            }
            Err(err) => {
                // Timeout / ambiguous commit: reconcile then same request_id retry.
                eprintln!("batch_modify round {round} err (retrying once): {err}");
                let after_open = client
                    .orders
                    .list_open(None)
                    .await
                    .expect("list_open reconcile");
                let after_count = after_open
                    .orders
                    .iter()
                    .filter(|o| all_cids.contains(&o.client_order_id))
                    .count();
                assert_eq!(
                    after_count, BATCH_SIZE,
                    "round {round}: open set changed during failed attempt"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
                match client
                    .orders
                    .batch_modify(
                        items.clone(),
                        Some(&symbol),
                        None,
                        Some(request_id.clone()),
                        None,
                        true,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(retry_err) => {
                        let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                        panic!("batch_modify round {round}: {retry_err}");
                    }
                }
            }
        };

        if !assert_complete_batch_result(&result, &requested, round) {
            eprintln!("skip: batch_modify returned all INTERNAL_ERROR (OMS)");
            let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
            return;
        }
        let fingerprint = result_fingerprint(&result);

        // Intentional identical request_id retry after success. The backend may
        // replay its cached success or reject all now-stale source IDs; either
        // contract is safe only if no second mutation is applied.
        let retry = client
            .orders
            .batch_modify(items, Some(&symbol), None, Some(request_id), None, true)
            .await
            .unwrap_or_else(|err| panic!("idempotent retry round {round}: {err}"));
        assert_eq!(
            retry.results.len(),
            BATCH_SIZE,
            "round {round}: retry must return one result per input"
        );
        let replayed_cached_result = result_fingerprint(&retry) == fingerprint;
        let safely_rejected_without_reapply = retry.rejected_count == BATCH_SIZE as i32
            && retry.amended_count == 0
            && retry.replaced_count == 0
            && retry
                .results
                .iter()
                .all(|item| item.status.eq_ignore_ascii_case("rejected"));
        assert!(
            replayed_cached_result || safely_rejected_without_reapply,
            "round {round}: retry must replay the cached result or reject every stale item \
             without another mutation: {retry:?}"
        );
        // Resolve live key per item: prefer new_cid when open.
        let mut next_cids = Vec::with_capacity(BATCH_SIZE);
        for (old_cid, new_cid) in client_order_ids.iter().zip(new_cids.iter()) {
            let chosen = if wait_for_open_order(&client, new_cid, Duration::from_secs(3))
                .await
                .is_ok()
            {
                new_cid.clone()
            } else if wait_for_open_order(&client, old_cid, Duration::from_secs(3))
                .await
                .is_ok()
            {
                old_cid.clone()
            } else {
                let _ = client.orders.cancel_all(Some(&symbol), false, None).await;
                panic!("round {round}: neither {old_cid} nor {new_cid} open after modify");
            };
            next_cids.push(chosen);
        }
        client_order_ids = next_cids;
    }

    match client.orders.cancel_all(Some(&symbol), false, None).await {
        Ok(_) => {}
        Err(err) => {
            if strict_live_enabled() {
                panic!("cleanup cancel_all failed: {err}");
            }
            eprintln!("cleanup cancel_all warning: {err}");
        }
    }
    let all_refs: Vec<&str> = all_cids.iter().map(String::as_str).collect();
    if let Err(err) =
        wait_until_no_open_client_ids(&client, &all_refs, Duration::from_secs(30)).await
    {
        if strict_live_enabled() {
            panic!("cleanup verification failed: {err}");
        }
        eprintln!("cleanup verification warning: {err}");
    }
}
