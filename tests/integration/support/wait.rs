//! Order / trigger / balance polling helpers.

use super::call::is_not_found;
use super::trade::reserved_balance_raw;
use polyester::models::{Order, Trigger, TriggerEventsList};
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::proto::triggers::v1::{GetTriggerRequest, ListTriggerEventsRequest};
use polyester::{Client, Error, Result};
use std::time::{Duration, Instant};

pub fn order_status_label(order: &Order) -> String {
    order.status.clone()
}

pub fn is_open_status(status: &str) -> bool {
    matches!(
        status,
        "" | "pending"
            | "working"
            | "pending_cancel"
            | "order_status_unspecified"
            | "order_status_pending"
            | "order_status_working"
            | "order_status_pending_cancel"
    ) || status.contains("pending")
        || status.contains("working")
}

pub fn is_terminal_status(status: &str) -> bool {
    let s = status.to_ascii_lowercase();
    s.contains("canceled")
        || s.contains("cancelled")
        || s.contains("rejected")
        || s.contains("filled")
}

#[derive(Debug)]
pub struct DevnetOrderNotIndexedError {
    pub msg: String,
}

impl std::fmt::Display for DevnetOrderNotIndexedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for DevnetOrderNotIndexedError {}

pub async fn wait_for_open_order(
    client: &Client,
    client_order_id: &str,
    timeout: Duration,
) -> std::result::Result<Order, Box<dyn std::error::Error + Send + Sync>> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(15)
    } else {
        timeout
    };
    let deadline = Instant::now() + timeout;
    let mut last_status = String::new();
    while Instant::now() < deadline {
        match client.orders.get(Some(client_order_id), None, None).await {
            Ok(detail) => {
                if let Some(order) = detail.order
                    && order.client_order_id == client_order_id
                {
                    let status = order_status_label(&order);
                    if status.is_empty() || is_open_status(&status) {
                        return Ok(order);
                    }
                    last_status = status.clone();
                    if is_terminal_status(&status) {
                        return Err(format!(
                            "order {client_order_id} reached terminal status {status:?}"
                        )
                        .into());
                    }
                }
            }
            Err(err) if is_not_found(&err) => {}
            Err(_) => {}
        }
        if let Ok(listed) = client.orders.list_open(None).await {
            for order in &listed.orders {
                if order.client_order_id == client_order_id {
                    return Ok(order.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let mut msg = format!("open order {client_order_id} was not visible within {timeout:?}");
    if !last_status.is_empty() {
        msg.push_str(&format!(" (last get status: {last_status})"));
    }
    Err(Box::new(DevnetOrderNotIndexedError { msg }))
}

pub async fn wait_for_no_open_order(
    client: &Client,
    client_order_id: &str,
    timeout: Duration,
) -> Result<()> {
    wait_until_no_open_client_ids(client, &[client_order_id], timeout).await
}

/// Poll `list_open` until none of the given client order IDs remain open.
pub async fn wait_until_no_open_client_ids(
    client: &Client,
    client_order_ids: &[&str],
    timeout: Duration,
) -> Result<()> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(10)
    } else {
        timeout
    };
    if client_order_ids.is_empty() {
        return Ok(());
    }
    let wanted: std::collections::HashSet<&str> = client_order_ids.iter().copied().collect();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let listed = client.orders.list_open(None).await?;
        let remaining: Vec<&str> = listed
            .orders
            .iter()
            .map(|o| o.client_order_id.as_str())
            .filter(|cid| wanted.contains(cid))
            .collect();
        if remaining.is_empty() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(Error::validation(format!(
        "client orders {client_order_ids:?} still open after {timeout:?}"
    )))
}

pub async fn wait_for_terminal_order(
    client: &Client,
    client_order_id: &str,
    timeout: Duration,
) -> Result<polyester::models::GetOrderResult> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(20)
    } else {
        timeout
    };
    let deadline = Instant::now() + timeout;
    let mut last: Option<polyester::models::GetOrderResult> = None;
    while Instant::now() < deadline {
        match client.orders.get(Some(client_order_id), None, None).await {
            Ok(detail) => {
                if let Some(order) = detail.order.as_ref()
                    && order.client_order_id == client_order_id
                {
                    let status = order_status_label(order);
                    last = Some(detail.clone());
                    if is_terminal_status(&status) {
                        return Ok(detail);
                    }
                }
            }
            Err(err) if is_not_found(&err) => {}
            Err(err) => return Err(err),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if let Some(detail) = last.as_ref()
        && let Some(order) = detail.order.as_ref()
    {
        let status = order_status_label(order);
        return Err(Error::validation(format!(
            "order {client_order_id} stuck in status {status:?} after {timeout:?}"
        )));
    }
    Err(Error::validation(format!(
        "order {client_order_id} did not reach terminal status within {timeout:?}"
    )))
}

/// Poll `balances.list` until each `(asset_id, expected_reserved)` pair matches.
///
/// Ledger reserved balances can lag terminal order status; roundtrip tests should wait
/// briefly rather than asserting on the first post-fill snapshot.
pub async fn wait_until_reserved_reconciles(
    client: &Client,
    expectations: &[(u32, u128)],
    timeout: Duration,
) -> Result<()> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(20)
    } else {
        timeout
    };
    if expectations.is_empty() {
        return Ok(());
    }
    let deadline = Instant::now() + timeout;
    let mut last: Vec<(u32, u128, u128)> = Vec::new();
    while Instant::now() < deadline {
        let listed = client.balances.list(GetBalancesRequest::default()).await?;
        last = expectations
            .iter()
            .map(|&(asset_id, expected)| {
                (
                    asset_id,
                    reserved_balance_raw(&listed.balances, asset_id),
                    expected,
                )
            })
            .collect();
        if last.iter().all(|&(_, actual, expected)| actual == expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(Error::validation(format!(
        "reserved balances not reconciled after {timeout:?}: {:?}",
        last.iter()
            .map(|&(asset_id, actual, expected)| {
                format!("asset={asset_id} actual={actual} expected={expected}")
            })
            .collect::<Vec<_>>()
    )))
}

pub async fn wait_for_trigger(
    client: &Client,
    trigger_id: &str,
    timeout: Duration,
) -> Result<Trigger> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(10)
    } else {
        timeout
    };
    let deadline = Instant::now() + timeout;
    let mut last_err: Option<Error> = None;
    let trigger_id_u64 = match polyester::codecs::scalars::id_to_u64(trigger_id, "trigger_id") {
        Ok(v) => v,
        Err(err) => return Err(err),
    };
    while Instant::now() < deadline {
        match client
            .triggers
            .get(GetTriggerRequest {
                trigger_id: trigger_id_u64,
                ..Default::default()
            })
            .await
        {
            Ok(Some(trigger)) => {
                return Ok(trigger);
            }
            Ok(None) => {}
            Err(err) if is_not_found(&err) => {
                last_err = Some(err);
            }
            Err(err) => return Err(err),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(Error::validation(format!(
        "trigger {trigger_id} was not readable within {timeout:?}: {:?}",
        last_err.map(|e| e.to_string())
    )))
}

pub async fn wait_for_trigger_events(
    client: &Client,
    trigger_id: &str,
    timeout: Duration,
) -> Result<TriggerEventsList> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(10)
    } else {
        timeout
    };
    let deadline = Instant::now() + timeout;
    let trigger_id_u64 = polyester::codecs::scalars::id_to_u64(trigger_id, "trigger_id")?;
    while Instant::now() < deadline {
        let events = client
            .triggers
            .list_events(ListTriggerEventsRequest {
                trigger_id: trigger_id_u64,
                limit: 10,
                ..Default::default()
            })
            .await?;
        if events.events.iter().any(|e| e.trigger_id == trigger_id) {
            return Ok(events);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(Error::validation(format!(
        "trigger events for {trigger_id} were not visible within {timeout:?}"
    )))
}
