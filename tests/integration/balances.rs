use crate::support::{call_optional, require_live_client};
use polyester::proto::ledger::read::v1::{GetBalanceHistoryRequest, GetBalancesRequest};

#[tokio::test]
async fn balances_list_returns_or_skips() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(resp) = call_optional("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await
    else {
        return;
    };
    for bal in &resp.balances {
        let _ = bal;
    }
}

#[tokio::test]
async fn balances_get_balance_history_optional() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("balances.get_balance_history", || {
        client
            .balances
            .get_balance_history(GetBalanceHistoryRequest::default())
    })
    .await;
}

#[tokio::test]
async fn concurrent_identical_authenticated_reads_do_not_replay_collide() {
    let Some(client) = require_live_client() else {
        return;
    };
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let balances = client.balances.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            balances.list(GetBalancesRequest::default()).await
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("concurrent balance task panicked") {
            Ok(_) => {}
            Err(err) => panic!("concurrent authenticated read failed: {err:?}"),
        }
    }
}
