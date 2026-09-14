use crate::support::{
    call_optional, call_required, is_internal_order_error, require_live_client, smoke_symbol,
};
use polyester::Result;

async fn soft_list_rpc<T, F, Fut>(label: &str, f: F) -> Option<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    match f().await {
        Ok(v) => Some(v),
        Err(err) if is_internal_order_error(&err) => {
            // Known backend NULL-scan / internal failure on some environments.
            eprintln!("skip: {label} backend unavailable: {err}");
            None
        }
        Err(err) => call_optional(label, || async { Err(err) }).await,
    }
}

#[tokio::test]
async fn orders_get_accepts_execution_history_flags() {
    let Some(client) = require_live_client() else {
        return;
    };
    let state_only = client
        .orders
        .get_with(polyester::models::GetOrderOpts {
            key: polyester::models::OrderKey::OrderId(
                polyester::codecs::scalars::format_uint64_id(1),
            ),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(false),
            limit: None,
            page_token: None,
        })
        .await;
    match state_only {
        Ok(result) => {
            assert!(result.trades.is_empty());
            assert!(result.transfers.is_empty());
        }
        Err(err) => {
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("not found") || msg.contains("not_found"),
                "state-only get rejected new fields: {err}"
            );
        }
    }
    let with_history = client
        .orders
        .get_with(polyester::models::GetOrderOpts {
            key: polyester::models::OrderKey::OrderId(
                polyester::codecs::scalars::format_uint64_id(1),
            ),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(true),
            limit: Some(5),
            page_token: None,
        })
        .await;
    match with_history {
        Ok(_) => {}
        Err(err) => {
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("not found") || msg.contains("not_found"),
                "history get rejected new fields: {err}"
            );
        }
    }
}

#[tokio::test]
async fn trades_list_accepts_transfers_and_scope_fields() {
    let Some(client) = require_live_client() else {
        return;
    };
    let listed = client
        .trades
        .list_with(polyester::models::ListUserTradesOpts {
            order_id: Some(polyester::codecs::scalars::format_uint64_id(1)),
            include_transfers: true,
            limit: Some(5),
            ..Default::default()
        })
        .await;
    match listed {
        Ok(result) => {
            let _ = result.transfers;
        }
        Err(err) => {
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("not found") || msg.contains("not_found"),
                "trades.list rejected new fields: {err}"
            );
        }
    }
}

#[tokio::test]
async fn orders_list_open_and_history() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = soft_list_rpc("orders.list_open", || client.orders.list_open(None)).await;
    let _ = soft_list_rpc("orders.list_history", || {
        client.orders.list_history(None, Some(10))
    })
    .await;
    let _ = call_optional("trades.list", || client.trades.list(None, Some(10))).await;
}

#[tokio::test]
async fn orders_get_round_trip_when_open_exists() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(listed) = call_optional("orders.list_open", || client.orders.list_open(None)).await
    else {
        return;
    };
    if listed.orders.is_empty() {
        eprintln!("skip: no open orders; cannot round-trip orders.get");
        return;
    }
    let sample = &listed.orders[0];
    let by_id = call_required("orders.get", || {
        client.orders.get(
            polyester::models::OrderKey::OrderId(sample.order_id.clone()),
            None,
        )
    })
    .await;
    let order = by_id.order.expect("expected order from get by order_id");
    assert_eq!(order.order_id, sample.order_id);
    if !sample.client_order_id.is_empty() {
        let by_cid = call_required("orders.get", || {
            client.orders.get(
                polyester::models::OrderKey::ClientOrderId(sample.client_order_id.clone()),
                None,
            )
        })
        .await;
        let order = by_cid
            .order
            .expect("expected order from get by client_order_id");
        assert_eq!(order.client_order_id, sample.client_order_id);
    }
}

#[tokio::test]
async fn orders_cancel_all_dry_run_optional() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match client.market_data.get_spot_config().await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: spot config: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let _ = call_optional("orders.cancel_all dry_run", || {
        client.orders.cancel_all(Some(&symbol), true, None)
    })
    .await;
}

#[tokio::test]
async fn orders_get_state_only_and_execution_history() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(listed) = call_optional("orders.list_history", || {
        client.orders.list_history(None, Some(10))
    })
    .await
    else {
        return;
    };
    if listed.orders.is_empty() {
        eprintln!("skip: no order history; cannot exercise lineage get");
        return;
    }
    let sample = &listed.orders[0];
    let state_only = call_required("orders.get_state_only", || {
        client.orders.get_with(polyester::models::GetOrderOpts {
            key: polyester::models::OrderKey::OrderId(sample.order_id.clone()),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(false),
            limit: None,
            page_token: None,
        })
    })
    .await;
    let order = state_only.order.expect("state-only get");
    assert_eq!(order.order_id, sample.order_id);
    assert!(state_only.trades.is_empty());
    assert!(state_only.transfers.is_empty());
    assert!(state_only.next_page_token.is_empty());

    let with_history = call_required("orders.get_history", || {
        client.orders.get_with(polyester::models::GetOrderOpts {
            key: polyester::models::OrderKey::OrderId(sample.order_id.clone()),
            subaccount_id: None,
            include_attached_risk: false,
            include_attached_risk_state: false,
            include_execution_history: Some(true),
            limit: Some(5),
            page_token: None,
        })
    })
    .await;
    let order = with_history.order.expect("history get");
    assert_eq!(order.order_id, sample.order_id);
    if let Some(lineage) = order.lineage {
        assert!(!lineage.id.is_empty());
        assert!(lineage.generation >= 1);
    }
}

#[tokio::test]
async fn trades_list_order_and_lineage_filters() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(listed) = call_optional("trades.list", || {
        client
            .trades
            .list_with(polyester::models::ListUserTradesOpts {
                include_transfers: true,
                limit: Some(5),
                ..Default::default()
            })
    })
    .await
    else {
        return;
    };
    if listed.trades.is_empty() {
        eprintln!("skip: no user trades; cannot exercise lineage filters");
        return;
    }
    let sample = &listed.trades[0];
    let by_order = call_required("trades.list_order", || {
        client
            .trades
            .list_with(polyester::models::ListUserTradesOpts {
                order_id: Some(sample.order_id.clone()),
                include_transfers: true,
                limit: Some(5),
                ..Default::default()
            })
    })
    .await;
    assert!(
        by_order
            .trades
            .iter()
            .all(|trade| trade.order_id == sample.order_id)
    );
    let Some(lineage) = sample.lineage.clone() else {
        return;
    };
    let by_lineage = call_required("trades.list_lineage", || {
        client
            .trades
            .list_with(polyester::models::ListUserTradesOpts {
                lineage_id: Some(lineage.id.clone()),
                through_generation: Some(lineage.generation),
                include_transfers: true,
                limit: Some(5),
                ..Default::default()
            })
    })
    .await;
    for trade in by_lineage.trades {
        if let Some(trade_lineage) = trade.lineage {
            assert_eq!(trade_lineage.id, lineage.id);
            assert!(trade_lineage.generation <= lineage.generation);
        }
    }
}
