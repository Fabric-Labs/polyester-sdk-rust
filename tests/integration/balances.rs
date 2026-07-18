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
