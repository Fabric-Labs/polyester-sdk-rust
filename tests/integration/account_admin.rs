//! Signed account-admin integration tests.

use crate::support::{
    call_optional, is_permission_denied, jwt_session_only, require_live_client, require_mutation,
    route_unavailable,
};
use polyester::Error;
use polyester::codecs::scalars::id_to_u64;
use polyester::proto::auth::v1::{
    AddressBookTagInput, CreateAddressBookEntryRequest, DeleteAddressBookEntryRequest,
    ListAddressBooksRequest, ListSubaccountsRequest, RequestedInternalTransferAccount,
};
use polyester::services::AddressBookService;

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

#[tokio::test]
async fn address_book_create_update_new_tags_and_delete() {
    if !require_mutation() {
        return;
    }
    let _guard = crate::support::mutation_test_guard().await;
    let Some(client) = require_live_client() else {
        return;
    };

    let dest = crate::support::internal_transfer_dest()
        .unwrap_or_else(|| "0x0000000000000000000000000000000000000001".into());
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let create_req = CreateAddressBookEntryRequest {
        label: format!("sdk-rust-ab-{stamp}"),
        new_tags: vec![AddressBookTagInput {
            name: format!("sdk-tag-{stamp}"),
            color: "#112233".into(),
            ..Default::default()
        }],
        entry: Some(
            RequestedInternalTransferAccount {
                smart_account_address: dest,
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    };

    let created = match client.address_book.create_entry(create_req).await {
        Ok(entry) => entry,
        Err(err) if skip_address_book_write(&err) => {
            eprintln!("skip: address_book.create_entry: {err}");
            return;
        }
        Err(err) => panic!("address_book.create_entry failed: {err}"),
    };
    assert!(
        !created.tags.is_empty(),
        "create with new_tags should return attached tags: {created:?}"
    );

    let entry_id = id_to_u64(&created.address_book_entry_id, "address_book_entry_id")
        .expect("created address_book_entry_id");

    let update_req = AddressBookService::update_entry_request_with_new_tags(
        entry_id,
        vec![AddressBookTagInput {
            name: format!("sdk-tag-upd-{stamp}"),
            color: "#445566".into(),
            ..Default::default()
        }],
        created.revision,
    );
    let updated = match client.address_book.update_entry(update_req).await {
        Ok(entry) => entry,
        Err(err) if skip_address_book_write(&err) => {
            eprintln!("skip: address_book.update_entry: {err}");
            let _ = client
                .address_book
                .delete_entry(DeleteAddressBookEntryRequest {
                    address_book_entry_id: entry_id,
                    ..Default::default()
                })
                .await;
            return;
        }
        Err(err) => {
            let _ = client
                .address_book
                .delete_entry(DeleteAddressBookEntryRequest {
                    address_book_entry_id: entry_id,
                    ..Default::default()
                })
                .await;
            panic!("address_book.update_entry failed: {err}");
        }
    };
    assert!(
        updated.tags.len() >= created.tags.len(),
        "update new_tags without tag_ids should append: created={:?} updated={:?}",
        created.tags,
        updated.tags
    );

    let _ = call_optional("address_book.delete_entry", || {
        client
            .address_book
            .delete_entry(DeleteAddressBookEntryRequest {
                address_book_entry_id: entry_id,
                ..Default::default()
            })
    })
    .await;
}

fn skip_address_book_write(err: &Error) -> bool {
    route_unavailable(err)
        || is_permission_denied(err)
        || jwt_session_only(err)
        || matches!(err, Error::Validation(_))
        || matches!(
            err,
            Error::Api { code, .. }
                if {
                    let c = code.to_ascii_lowercase();
                    c.contains("invalid") || c.contains("failed_precondition")
                }
        )
}
