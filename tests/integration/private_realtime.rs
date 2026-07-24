//! Live private-channel realtime coverage (auth + trading/ledger streams).

use crate::support::{call_optional, require_live_client};
use std::time::Duration;

async fn wait_private_subscribe_optional<T>(
    label: &str,
    mut sub: polyester::realtime::TypedSubscription<T>,
) {
    match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            eprintln!("skip: {label} closed without publications");
        }
        Err(_) => {
            // Subscribe + private-channel auth succeeded; idle channel is OK.
        }
    }
    if !sub.is_alive()
        && let Some(err) = sub.err()
    {
        panic!("{label} realtime connection terminated: {err}");
    }
    sub.close();
}

fn require_account_id(client: &polyester::Client) -> Option<()> {
    if client
        .default_account_id
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        eprintln!("skip: POLYESTER_ACCOUNT_ID required for private realtime");
        None
    } else {
        Some(())
    }
}

#[tokio::test]
async fn private_subscribe_connects_api_keys() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("api_keys.subscribe", || client.api_keys.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("api_keys.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_api_policies() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("policies.subscribe_api_policies", || {
        client.policies.subscribe_api_policies(None)
    })
    .await
    else {
        return;
    };
    wait_private_subscribe_optional("policies.subscribe_api_policies", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_subaccount_policies() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("policies.subscribe_subaccount_policies", || {
        client.policies.subscribe_subaccount_policies(None)
    })
    .await
    else {
        return;
    };
    wait_private_subscribe_optional("policies.subscribe_subaccount_policies", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_subaccounts() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("sub_accounts.subscribe", || {
        client.sub_accounts.subscribe(None)
    })
    .await
    else {
        return;
    };
    wait_private_subscribe_optional("sub_accounts.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_address_books() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("address_book.subscribe", || {
        client.address_book.subscribe(None)
    })
    .await
    else {
        return;
    };
    wait_private_subscribe_optional("address_book.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_balances() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("balances.subscribe", || client.balances.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("balances.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_transfers() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("transfers.subscribe", || client.transfers.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("transfers.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_trades() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("trades.subscribe", || client.trades.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("trades.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_triggers() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("triggers.subscribe", || client.triggers.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("triggers.subscribe", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_trigger_events() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("triggers.subscribe_events", || {
        client.triggers.subscribe_events(None)
    })
    .await
    else {
        return;
    };
    wait_private_subscribe_optional("triggers.subscribe_events", sub).await;
}

#[tokio::test]
async fn private_subscribe_connects_orders() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(()) = require_account_id(&client) else {
        return;
    };
    let Some(sub) = call_optional("orders.subscribe", || client.orders.subscribe(None)).await
    else {
        return;
    };
    wait_private_subscribe_optional("orders.subscribe", sub).await;
}
