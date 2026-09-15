use std::collections::HashSet;
use std::time::Duration;

use crate::support::{
    hydrate_spot_and_zipper, is_internal_order_error, is_notional_validation,
    maker_client_from_env, min_base_qty_for_pair, pair_for_symbol, require_funded,
    require_live_client, require_mutation, require_trading_quote_balance, trade_e2e_enabled,
    trade_symbol, unique_client_order_id,
};
use polyester::codecs::scalars::{format_price_ticks, parse_price_ticks_str};
use polyester::models::{
    CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, GetOrderOpts,
    GetOrderResult, ListUserTradesOpts, ModifyOrderParams, OrderKey, UserTrade,
};
use polyester::types::{Price, Quantity};
use polyester::{Client, Config};

#[tokio::test]
async fn partial_fill_modify_keeps_predecessor_execution_history() {
    if !require_mutation() || !require_funded() {
        return;
    }
    let _mutation_guard = crate::support::mutation_test_guard().await;
    if !trade_e2e_enabled() {
        eprintln!("skip: Set POLYESTER_TEST_TRADE_E2E=1 to spend against the live book");
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
        eprintln!("skip: {symbol} is not in spot config");
        return;
    };
    let book = match client.orderbook.get(&symbol, Some(5)).await {
        Ok(v) => v,
        Err(err) => panic!("orderbook.get: {err}"),
    };
    let Some(ask) = book.asks.first() else {
        eprintln!("skip: no visible asks on {symbol}");
        return;
    };
    let Some(ask_price) = ask.price.as_ref() else {
        eprintln!("skip: no visible asks on {symbol}");
        return;
    };
    let scale = client
        .catalogs
        .base_quantity_scale_for_symbol(&symbol)
        .expect("catalog scale");
    let Some(maker) = second_client() else {
        eprintln!(
            "skip: no second key for a cheap partial fill; sweeping the ask is too expensive"
        );
        return;
    };
    let Some(bid) = book.bids.first().and_then(|level| level.price.as_ref()) else {
        eprintln!("skip: no visible bids on {symbol}");
        return;
    };
    let tick_size = if pair.tick_size.trim().is_empty() {
        "0.01".to_string()
    } else {
        pair.tick_size.clone()
    };
    let step = parse_price_ticks_str(&tick_size, "tick_size").unwrap_or(1) as i64;
    let maker_ticks = bid.as_ticks() + step;
    if maker_ticks >= ask_price.as_ticks() {
        eprintln!("skip: spread is too tight for an inside-spread maker");
        return;
    }
    let create_price =
        match Price::from_decimal_str(&format_price_ticks(maker_ticks), Some(symbol.clone())) {
            Ok(p) => p,
            Err(err) => {
                eprintln!("skip: maker price: {err}");
                return;
            }
        };
    let min = min_base_qty_for_pair(Some(&pair), &create_price.format());
    let maker_qty = match Quantity::from_decimal_str(&min, scale, Some(symbol.clone()), None) {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };
    let taker_raw = match min.parse::<rust_decimal::Decimal>() {
        Ok(v) => (v * rust_decimal::Decimal::from(3)).normalize().to_string(),
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };
    let taker_qty = match Quantity::from_decimal_str(&taker_raw, scale, Some(symbol.clone()), None)
    {
        Ok(q) => q,
        Err(err) => {
            eprintln!("skip: qty: {err}");
            return;
        }
    };

    let original_cid = unique_client_order_id("pfill");
    let replacement_cid = unique_client_order_id("pfillr");
    let maker_cid = unique_client_order_id("pfillm");
    let _ = hydrate_spot_and_zipper(&maker).await;
    if let Err(err) = maker
        .orders
        .create(CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Sell,
            order_type: CreateOrderType::Limit,
            quantity: Some(maker_qty),
            price: Some(create_price.clone()),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(maker_cid.clone()),
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
        if skippable_order_error(&err) {
            eprintln!("skip: maker unavailable: {err}");
            return;
        }
        panic!("maker create: {err}");
    }
    if wait_listed_open(&maker, &maker_cid).await.is_err() {
        let _ = maker
            .orders
            .cancel_by_client_order_id(&maker_cid, Some(&symbol), None)
            .await;
        eprintln!("skip: maker sell was not visible as open");
        return;
    }
    if wait_listed_open(&maker, &maker_cid).await.is_err() {
        eprintln!("skip: maker sell was taken before the taker could rest a remainder");
        return;
    }

    let created = match client
        .orders
        .create(CreateOrderParams {
            symbol: symbol.clone(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity: Some(taker_qty),
            price: Some(create_price),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some(original_cid.clone()),
            subaccount_id: None,
            post_only: Some(false),
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
        Err(err) if skippable_order_error(&err) => {
            eprintln!("skip: create unavailable: {err}");
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
        if !created.order_id.is_empty() {
            let _ = client
                .orders
                .cancel_by_order_id(&created.order_id, None)
                .await;
        }
        let _ = maker
            .orders
            .cancel_by_client_order_id(&maker_cid, Some(&symbol), None)
            .await;
    };

    let partial = match wait_partial_fill(&client, &original_cid).await {
        Ok(v) => v,
        Err(err) => {
            cleanup().await;
            panic!("{err}");
        }
    };
    let Some(partial) = partial else {
        cleanup().await;
        eprintln!(
            "skip: buy at best ask fully filled; book too deep to rest a remainder for ModifyOrder"
        );
        return;
    };
    let first_order = partial.order.as_ref().expect("partial order");
    let first_fill_qty = first_order
        .cum_qty
        .as_ref()
        .map(Quantity::as_scaled)
        .unwrap_or(0);
    assert!(first_fill_qty > 0);
    let first_lineage_id = first_order
        .lineage
        .as_ref()
        .map(|lineage| lineage.id.clone())
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| created.order_id.clone());
    let first_generation = first_order
        .lineage
        .as_ref()
        .map(|lineage| lineage.generation)
        .unwrap_or(1);
    let first_match_ids = HashSet::<String>::new();

    let tick_size = if pair.tick_size.trim().is_empty() {
        "0.01".to_string()
    } else {
        pair.tick_size.clone()
    };
    let current_ticks = first_order.price.as_ref().map(Price::as_ticks).unwrap_or(0);
    let mut rest_price = one_tick_below(current_ticks, &tick_size, &ask_price.format());
    if let Some(bid) = book.bids.first().and_then(|level| level.price.as_ref()) {
        let bid_rest = one_tick_below(bid.as_ticks(), &tick_size, &ask_price.format());
        if parse_price_ticks_str(&bid_rest, "price").unwrap_or(0)
            < parse_price_ticks_str(&rest_price, "price").unwrap_or(0)
        {
            rest_price = bid_rest;
        }
    }

    let new_price = match Price::from_decimal_str(&rest_price, Some(symbol.clone())) {
        Ok(p) => p,
        Err(err) => {
            cleanup().await;
            panic!("new price: {err}");
        }
    };
    let modify_keys = [
        OrderKey::OrderId(created.order_id.clone()),
        OrderKey::OrderId(first_order.order_id.clone()),
        OrderKey::ClientOrderId(original_cid.clone()),
    ];
    let mut modified = None;
    let mut last_modify_err = None;
    for key in modify_keys {
        match client
            .orders
            .modify(ModifyOrderParams {
                symbol: symbol.clone(),
                key,
                subaccount_id: None,
                request_id: None,
                new_price: Some(new_price.clone()),
                new_qty: None,
                new_attached_risk: None,
                behavior: Some("replace_only".into()),
                new_client_order_id: Some(replacement_cid.clone()),
            })
            .await
        {
            Ok(v) => {
                modified = Some(v);
                break;
            }
            Err(err)
                if is_internal_order_error(&err)
                    || crate::support::devnet_unavailable(&err)
                    || err.to_string().to_ascii_lowercase().contains("not found") =>
            {
                last_modify_err = Some(err);
            }
            Err(err) => {
                cleanup().await;
                panic!("orders.modify: {err}");
            }
        }
    }
    let Some(modified) = modified else {
        cleanup().await;
        eprintln!("skip: remainder was not modifyable: {:?}", last_modify_err);
        return;
    };
    assert_eq!(modified.old_order_id, created.order_id);
    assert!(!modified.final_order_id.is_empty());

    if wait_listed_open(&client, &replacement_cid).await.is_err() {
        cleanup().await;
        eprintln!("skip: replacement was not visible as open after modify");
        return;
    }

    let history = match get_history(
        &client,
        OrderKey::OrderId(modified.final_order_id.clone()),
        1,
        None,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            let state = match client
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
                Err(state_err) => {
                    cleanup().await;
                    panic!("{err}; state-only also failed: {state_err}");
                }
            };
            let scoped = match client
                .trades
                .list_with(ListUserTradesOpts {
                    lineage_id: Some(first_lineage_id.clone()),
                    through_generation: Some(first_generation + 1),
                    include_transfers: true,
                    limit: Some(5),
                    ..Default::default()
                })
                .await
            {
                Ok(v) => v,
                Err(list_err) => {
                    cleanup().await;
                    panic!("{err}; trades.list lineage also failed: {list_err}");
                }
            };
            let order = state.order.as_ref().expect("replacement order");
            let lineage = order.lineage.as_ref().expect("replacement lineage");
            assert_eq!(lineage.id, first_lineage_id);
            assert_eq!(lineage.generation, first_generation + 1);
            let cum = order.cum_qty.as_ref().map(Quantity::as_scaled).unwrap_or(0);
            assert!(cum >= first_fill_qty);
            assert!(
                !scoped.trades.is_empty(),
                "POLY-4708: lineage trades empty after replacing a partially filled order"
            );
            cleanup().await;
            return;
        }
    };
    let order = history.order.as_ref().expect("replacement order");
    let lineage = order.lineage.as_ref().expect("replacement lineage");
    assert_eq!(lineage.id, first_lineage_id);
    assert_eq!(lineage.generation, first_generation + 1);
    let cum = order.cum_qty.as_ref().map(Quantity::as_scaled).unwrap_or(0);
    assert!(cum >= first_fill_qty);
    let after_match_ids = match_ids(&history.trades);
    assert!(
        !history.trades.is_empty(),
        "POLY-4708: GetOrder history is empty after replacing a partially filled order"
    );
    assert!(
        first_match_ids.is_subset(&after_match_ids),
        "predecessor fills missing after replace: before={first_match_ids:?} after={after_match_ids:?}"
    );
    for trade in &history.trades {
        if let Some(trade_lineage) = &trade.lineage {
            assert_eq!(trade_lineage.id, first_lineage_id);
            assert!(trade_lineage.generation <= lineage.generation);
        }
    }

    let page = match get_history(
        &client,
        OrderKey::OrderId(modified.final_order_id.clone()),
        1,
        None,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            cleanup().await;
            panic!("{err}");
        }
    };
    if !page.next_page_token.is_empty() {
        let page_two = match get_history(
            &client,
            OrderKey::OrderId(modified.final_order_id.clone()),
            1,
            Some(page.next_page_token.clone()),
        )
        .await
        {
            Ok(v) => v,
            Err(err) => {
                cleanup().await;
                panic!("{err}");
            }
        };
        let paged = page_two.order.expect("paged order");
        assert_eq!(paged.order_id, modified.final_order_id);
    }

    let mut last_err = None;
    let mut scoped = None;
    for _ in 0..3 {
        match client
            .trades
            .list_with(ListUserTradesOpts {
                lineage_id: Some(first_lineage_id.clone()),
                through_generation: Some(lineage.generation),
                include_transfers: true,
                limit: Some(5),
                ..Default::default()
            })
            .await
        {
            Ok(v) => {
                scoped = Some(v);
                break;
            }
            Err(err) if is_query_timeout(&err.to_string()) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => {
                cleanup().await;
                panic!("trades.list lineage: {err}");
            }
        }
    }
    let Some(scoped) = scoped else {
        cleanup().await;
        panic!(
            "GetUserTrades lineage_id+transfers timed out after a real replacement: {last_err:?}"
        );
    };
    let scoped_matches = match_ids(&scoped.trades);
    assert!(
        first_match_ids.is_subset(&scoped_matches),
        "lineage trades missing predecessor: before={first_match_ids:?} after={scoped_matches:?}"
    );
    let mut seen_tx = HashSet::new();
    for transfer in scoped.transfers {
        if transfer.tx_id.is_empty() {
            continue;
        }
        assert!(
            seen_tx.insert(transfer.tx_id.clone()),
            "duplicate transfer tx_id={}",
            transfer.tx_id
        );
    }
    cleanup().await;
}

fn skippable_order_error(err: &polyester::Error) -> bool {
    if is_internal_order_error(err)
        || crate::support::devnet_unavailable(err)
        || is_notional_validation(err)
    {
        return true;
    }
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("insufficient") || msg.contains("self-trade") || msg.contains("self_trade")
}

fn second_client() -> Option<Client> {
    if let Some(client) = maker_client_from_env() {
        return Some(client);
    }
    let key_id = std::env::var("POLYESTER_SUBACCOUNT_API_KEY_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let private_key = std::env::var("POLYESTER_SUBACCOUNT_API_PRIVATE_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let primary = std::env::var("POLYESTER_API_KEY_ID").unwrap_or_default();
    if key_id == primary {
        return None;
    }
    Client::new(Config {
        api_key_id: Some(key_id),
        api_private_key: Some(private_key),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .ok()
}

fn match_ids(trades: &[UserTrade]) -> HashSet<String> {
    trades
        .iter()
        .filter(|trade| !trade.match_id.is_empty())
        .map(|trade| trade.match_id.clone())
        .collect()
}

fn is_query_timeout(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("timeout") || lower.contains("query aborted")
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

async fn wait_listed_open(
    client: &polyester::Client,
    client_order_id: &str,
) -> std::result::Result<polyester::models::Order, ()> {
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
    Err(())
}

async fn get_history(
    client: &polyester::Client,
    key: OrderKey,
    limit: u32,
    page_token: Option<String>,
) -> std::result::Result<GetOrderResult, String> {
    let mut last = String::new();
    for _ in 0..5 {
        match client
            .orders
            .get_with(GetOrderOpts {
                key: key.clone(),
                subaccount_id: None,
                include_attached_risk: false,
                include_attached_risk_state: false,
                include_execution_history: Some(true),
                limit: Some(limit),
                page_token: page_token.clone(),
            })
            .await
        {
            Ok(v) => return Ok(v),
            Err(err) if is_query_timeout(&err.to_string()) => {
                last = err.to_string();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Err(format!(
        "GetOrder execution history timed out; lineage predecessor fills cannot be confirmed: {last}"
    ))
}

async fn wait_partial_fill(
    client: &polyester::Client,
    client_order_id: &str,
) -> std::result::Result<Option<GetOrderResult>, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if let Ok(detail) = client
            .orders
            .get_with(GetOrderOpts {
                key: OrderKey::ClientOrderId(client_order_id.to_string()),
                subaccount_id: None,
                include_attached_risk: false,
                include_attached_risk_state: false,
                include_execution_history: Some(false),
                limit: None,
                page_token: None,
            })
            .await
            && let Some(order) = &detail.order
        {
            let cum = order.cum_qty.as_ref().map(Quantity::as_scaled).unwrap_or(0);
            let leaves = order.leaves_qty.as_ref().map(Quantity::as_scaled);
            if cum > 0 && leaves.unwrap_or(0) > 0 {
                return Ok(Some(detail));
            }
            if order.status == "filled" || matches!(leaves, Some(0)) {
                return Ok(None);
            }
            if cum > 0 && matches!(order.status.as_str(), "working" | "pending") {
                return Ok(Some(detail));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("order {client_order_id} never partially filled"))
}
