//! Signed account-admin integration tests.

use crate::support::{call_optional, require_live_client};
use polyester::proto::auth::v1::{ListAddressBooksRequest, ListSubaccountsRequest};

#[tokio::test]
async fn api_keys_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("api_keys.list", || client.api_keys.list(None)).await;
}

#[tokio::test]
async fn policies_subscribe_only() {
    // Policy unary RPCs are JWT/session-only; API-key SDK keeps subscribe helpers.
    let Some(client) = require_live_client() else {
        return;
    };
    if client
        .default_account_id
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        eprintln!("skip: POLYESTER_ACCOUNT_ID required for policies.subscribe");
        return;
    }
    let _ = call_optional("policies.subscribe_api_policies", || {
        client.policies.subscribe_api_policies(None)
    })
    .await;
    let _ = call_optional("policies.subscribe_subaccount_policies", || {
        client.policies.subscribe_subaccount_policies(None)
    })
    .await;
}

#[tokio::test]
async fn sub_accounts_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("sub_accounts.list", || {
        client.sub_accounts.list(ListSubaccountsRequest::default())
    })
    .await;
}

#[tokio::test]
async fn address_book_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("address_book.list_books", || {
        client
            .address_book
            .list_books(ListAddressBooksRequest::default())
    })
    .await;
}
