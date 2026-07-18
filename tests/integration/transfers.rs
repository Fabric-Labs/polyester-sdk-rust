use crate::support::{call_optional, require_live_client};
use polyester::proto::ledger::read::v1::{ListHoldsRequest, ListTransfersRequest};

#[tokio::test]
async fn balances_list_transfers_and_holds() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("balances.list_transfers", || {
        client
            .balances
            .list_transfers(ListTransfersRequest::default())
    })
    .await;
    let _ = call_optional("balances.list_holds", || {
        client.balances.list_holds(ListHoldsRequest::default())
    })
    .await;
}
