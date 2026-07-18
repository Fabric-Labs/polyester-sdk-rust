//! Signed account-admin integration tests.

use crate::support::{call_optional, require_live_client};
use polyester::proto::auth::v1::{
    ListAddressBooksRequest, ListApiPoliciesRequest, ListSubaccountPoliciesRequest,
    ListSubaccountsRequest, ResolveAccountRequest,
};

#[tokio::test]
async fn api_keys_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("api_keys.list", || client.api_keys.list(None)).await;
}

#[tokio::test]
async fn policies_list() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("policies.list_subaccount_policies", || {
        client
            .policies
            .list_subaccount_policies(ListSubaccountPoliciesRequest::default())
    })
    .await;
    let _ = call_optional("policies.list_api_policies", || {
        client
            .policies
            .list_api_policies(ListApiPoliciesRequest::default())
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
async fn resolve_account() {
    let Some(client) = require_live_client() else {
        return;
    };
    let account_id = match std::env::var("POLYESTER_ACCOUNT_ID") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_owned(),
        _ => {
            eprintln!("skip: POLYESTER_ACCOUNT_ID not set");
            return;
        }
    };
    let req = ResolveAccountRequest {
        query: account_id,
        ..Default::default()
    };
    let _ = call_optional("resolve.resolve_account", || {
        client.resolve.resolve_account(req)
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
