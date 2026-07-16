use crate::support::{call_optional, call_required, require_live_client, smoke_symbol};

#[tokio::test]
async fn orders_list_open_and_history() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("orders.list_open", || client.orders.list_open(None)).await;
    let _ = call_optional("orders.list_history", || {
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
        client.orders.get(None, Some(&sample.order_id), None)
    })
    .await;
    let order = by_id.order.expect("expected order from get by order_id");
    assert_eq!(order.order_id, sample.order_id);
    if !sample.client_order_id.is_empty() {
        let by_cid = call_required("orders.get", || {
            client.orders.get(Some(&sample.client_order_id), None, None)
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
