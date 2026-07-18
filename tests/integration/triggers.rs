use crate::support::{call_optional, require_live_client};
use polyester::proto::triggers::v1::ListTriggersRequest;

#[tokio::test]
async fn triggers_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("triggers.list", || {
        client.triggers.list(ListTriggersRequest::default())
    })
    .await;
}
