//! Centrifugo realtime integration tests (Go/Python parity).

use crate::support::{
    call_optional, hydrate_spot_and_zipper, realtime_heartbeat_enabled, require_live_client,
    smoke_symbol,
};
use std::time::Duration;

const REALTIME_HEARTBEAT_HOLD: Duration = Duration::from_secs(35);

#[tokio::test]
async fn public_trades_subscription_survives_centrifugo_ping() {
    if !realtime_heartbeat_enabled() {
        eprintln!(
            "skip: Set POLYESTER_TEST_REALTIME_HEARTBEAT=1 to run the long Centrifugo ping test"
        );
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
    let symbol = smoke_symbol(&spot);
    let mut sub = match client.market_data.subscribe_trades(&symbol).await {
        Ok(s) => s,
        Err(err) => panic!("subscribe_trades failed: {err}"),
    };

    let deadline = tokio::time::Instant::now() + REALTIME_HEARTBEAT_HOLD;
    let mut publications = 0_usize;
    while tokio::time::Instant::now() < deadline {
        if !sub.is_alive() {
            panic!(
                "public trades subscription closed before Centrifugo heartbeat window elapsed ({REALTIME_HEARTBEAT_HOLD:?})"
            );
        }
        match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
            Ok(Some(_)) => publications += 1,
            Ok(None) => panic!("public trades subscription ended before the heartbeat window"),
            Err(_) => {}
        }
    }
    sub.close();
    assert!(
        publications > 0,
        "protobuf subscription stayed open but delivered no publications"
    );
}

#[tokio::test]
async fn orders_subscribe_receives_connection_optional() {
    let Some(client) = require_live_client() else {
        return;
    };
    if client
        .default_account_id
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        eprintln!("skip: POLYESTER_ACCOUNT_ID required for private orders realtime");
        return;
    }

    let mut sub = match call_optional("orders.subscribe", || client.orders.subscribe(None)).await {
        Some(s) => s,
        None => return,
    };

    match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            eprintln!("skip: orders.subscribe closed without publications (no order activity)");
        }
        Err(_) => {}
    }
    sub.close();
}
