use crate::support::{call_optional, require_live_client};
use polyester::proto::ledger::read::v1::{ListHoldsRequest, ListTransfersRequest};

#[tokio::test]
async fn balances_list_transfers_and_holds() {
    let Some(client) = require_live_client() else {
        return;
    };
    if let Some(result) = call_optional("balances.list_transfers", || {
        client
            .balances
            .list_transfers(ListTransfersRequest::default())
    })
    .await
    {
        for transfer in result.transfers {
            for side in [transfer.source, transfer.destination]
                .into_iter()
                .flatten()
            {
                if side.kind == "external_address" && side.chain_id == Some(0) {
                    panic!("external zipper chain_id must not be the zero sentinel: {side:?}");
                }
            }
        }
    }
    let _ = call_optional("balances.list_holds", || {
        client.balances.list_holds(ListHoldsRequest::default())
    })
    .await;
}
