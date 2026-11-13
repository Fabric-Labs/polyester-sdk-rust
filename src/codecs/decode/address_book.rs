//! Address book decoders.

use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::{format_id, format_uint64_id};
use crate::errors::{Error, Result};
use crate::models::{
    AccountScope, AddressBookEntriesList, AddressBookEntriesView, AddressBookEntry,
    AddressBookRecentDestinationsView, AddressBookSummary, AddressBookTag, AddressBookTagSummary,
    AddressBookTransferDestination, AddressBookTransferDestinationsList, AddressBookView,
    AddressBookViewInvalidation, AddressBooksList, ExternalAddressBookEntryView,
    ExternalRecentDestination, ExternalWithdrawAddress, InternalAddressBookEntryView,
    InternalRecentDestination, InternalTransferAccount, InternalTransferWhitelistEntriesList,
    InternalTransferWhitelistEntry, MirroredWithdrawWhitelistEntry, TransferCounterpartiesList,
    TransferCounterparty, WithdrawWhitelistView,
};
use crate::proto::auth::v1::{
    AccountScopeRef, AddressBook as ProtoAddressBook, AddressBookEntry as ProtoAddressBookEntry,
    AddressBookTag as ProtoAddressBookTag, AddressBookViewInvalidated,
    CopyAddressBookEntryResponse, CreateAddressBookEntryResponse, CreateAddressBookTagResponse,
    ExternalAddressBookEntry as ProtoExternalEntryView,
    ExternalWithdrawAddress as ProtoExternalWithdrawAddress, GetAddressBookViewResponse,
    GetWithdrawWhitelistViewResponse, InternalAddressBookEntry as ProtoInternalEntryView,
    InternalTransferAccount as ProtoInternalTransferAccount,
    InternalTransferWhitelistEntry as ProtoInternalWhitelistEntry, ListAddressBookEntriesResponse,
    ListAddressBooksResponse, ListInternalTransferWhitelistEntriesResponse,
    ListTransferCounterpartiesResponse, ListTransferDestinationsResponse,
    TransferCounterparty as ProtoTransferCounterparty,
    TransferDestination as ProtoTransferDestination, UpdateAddressBookEntryResponse,
    UpdateAddressBookTagResponse, WithdrawWhitelistView as ProtoWithdrawWhitelistView,
};
use crate::proto::auth::v1::{
    address_book_entry::Entry, transfer_counterparty::Counterparty,
    transfer_destination::Destination,
};
use buffa_types::google::protobuf::Timestamp;

fn time(ts: &buffa::MessageField<Timestamp>) -> Option<Timestamp> {
    ts.as_option().cloned()
}

fn scope_from_proto(scope: &buffa::MessageField<AccountScopeRef>) -> Option<AccountScope> {
    scope.as_option().map(|scope| AccountScope {
        scope_type: enum_proto_name(&scope.scope_type),
        root_account_id: nonzero_id(scope.root_account_id),
        // A root scope sends sub-account 0, which is not an ID: name none.
        sub_account_id: nonzero_id(scope.subaccount_id),
    })
}

fn external_from_proto(msg: &ProtoExternalWithdrawAddress) -> ExternalWithdrawAddress {
    ExternalWithdrawAddress {
        polychain_chain_id: msg.polychain_chain_id,
        address: msg.address.clone(),
    }
}

fn internal_from_proto(msg: &ProtoInternalTransferAccount) -> InternalTransferAccount {
    InternalTransferAccount {
        root_account_id: nonzero_id(msg.root_account_id),
        target_account_id: nonzero_id(msg.target_account_id),
        target_scope_type: enum_proto_name(&msg.target_scope_type),
        smart_account_address: msg.smart_account_address.clone(),
        root_username: msg.root_username.clone(),
        sub_account_label: msg.subaccount_label.clone(),
    }
}

fn summary_from_proto(msg: &ProtoAddressBook) -> AddressBookSummary {
    AddressBookSummary {
        scope: scope_from_proto(&msg.scope),
        caller_role: enum_proto_name(&msg.caller_role),
        label: msg.label.clone(),
        owner_username: msg.owner_username.clone(),
        smart_account_address: msg.smart_account_address.clone(),
    }
}

pub fn list_books_from_proto(msg: &ListAddressBooksResponse) -> AddressBooksList {
    AddressBooksList {
        books: msg.books.iter().map(summary_from_proto).collect(),
    }
}

fn entry_from_proto(msg: &ProtoAddressBookEntry) -> AddressBookEntry {
    let (external, internal) = match &msg.entry {
        Some(Entry::External(external)) => (Some(external_from_proto(external)), None),
        Some(Entry::Internal(internal)) => (None, Some(internal_from_proto(internal))),
        None => (None, None),
    };
    AddressBookEntry {
        address_book_entry_id: format_uint64_id(msg.address_book_entry_id),
        scope: scope_from_proto(&msg.scope),
        kind: enum_proto_name(&msg.kind),
        label: msg.label.clone(),
        note: msg.note.clone(),
        created_at: time(&msg.created_at),
        updated_at: time(&msg.updated_at),
        external,
        internal,
        tags: msg.tags.iter().map(tag_from_proto).collect(),
        revision: msg.revision,
    }
}

pub fn list_entries_from_proto(msg: &ListAddressBookEntriesResponse) -> AddressBookEntriesList {
    AddressBookEntriesList {
        entries: msg.entries.iter().map(entry_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn entry_from_create_proto(msg: &CreateAddressBookEntryResponse) -> Result<AddressBookEntry> {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .ok_or_else(|| Error::transport("invalid CreateAddressBookEntry response: missing entry"))
}

pub fn entry_from_update_proto(msg: &UpdateAddressBookEntryResponse) -> Result<AddressBookEntry> {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .ok_or_else(|| Error::transport("invalid UpdateAddressBookEntry response: missing entry"))
}

pub fn entry_from_copy_proto(msg: &CopyAddressBookEntryResponse) -> Result<AddressBookEntry> {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .ok_or_else(|| Error::transport("invalid CopyAddressBookEntry response: missing entry"))
}

fn tag_from_proto(msg: &ProtoAddressBookTag) -> AddressBookTag {
    AddressBookTag {
        tag_id: format_uint64_id(msg.tag_id),
        scope: scope_from_proto(&msg.scope),
        name: msg.name.clone(),
        color: msg.color.clone(),
        created_at: time(&msg.created_at),
        updated_at: time(&msg.updated_at),
    }
}

pub fn tag_from_create_proto(msg: &CreateAddressBookTagResponse) -> Result<AddressBookTag> {
    msg.tag
        .as_option()
        .map(tag_from_proto)
        .ok_or_else(|| Error::transport("invalid CreateAddressBookTag response: missing tag"))
}

pub fn tag_from_update_proto(msg: &UpdateAddressBookTagResponse) -> Result<AddressBookTag> {
    msg.tag
        .as_option()
        .map(tag_from_proto)
        .ok_or_else(|| Error::transport("invalid UpdateAddressBookTag response: missing tag"))
}

fn counterparty_from_proto(msg: &ProtoTransferCounterparty) -> TransferCounterparty {
    let (external, internal) = match &msg.counterparty {
        Some(Counterparty::External(external)) => (Some(external_from_proto(external)), None),
        Some(Counterparty::Internal(internal)) => (None, Some(internal_from_proto(internal))),
        None => (None, None),
    };
    TransferCounterparty {
        counterparty_id: format_uint64_id(msg.counterparty_id),
        scope: scope_from_proto(&msg.scope),
        direction: enum_proto_name(&msg.direction),
        kind: enum_proto_name(&msg.kind),
        saved: msg.saved,
        address_book_entry_id: nonzero_id(msg.address_book_entry_id),
        use_count: msg.use_count,
        first_seen_at: time(&msg.first_seen_at),
        last_seen_at: time(&msg.last_seen_at),
        external,
        internal,
    }
}

pub fn list_counterparties_from_proto(
    msg: &ListTransferCounterpartiesResponse,
) -> TransferCounterpartiesList {
    TransferCounterpartiesList {
        counterparties: msg
            .counterparties
            .iter()
            .map(counterparty_from_proto)
            .collect(),
        truncated: msg.truncated,
    }
}

fn destination_from_proto(msg: &ProtoTransferDestination) -> AddressBookTransferDestination {
    let (external, internal) = match &msg.destination {
        Some(Destination::External(external)) => (Some(external_from_proto(external)), None),
        Some(Destination::Internal(internal)) => (None, Some(internal_from_proto(internal))),
        None => (None, None),
    };
    AddressBookTransferDestination {
        scope: scope_from_proto(&msg.scope),
        kind: enum_proto_name(&msg.kind),
        saved: msg.saved,
        whitelisted: msg.whitelisted,
        whitelist_status: enum_proto_name(&msg.whitelist_status),
        address_book_entry: msg.address_book_entry.as_option().map(entry_from_proto),
        external,
        internal,
        whitelist_updated_at: time(&msg.whitelist_updated_at),
    }
}

pub fn list_destinations_from_proto(
    msg: &ListTransferDestinationsResponse,
) -> AddressBookTransferDestinationsList {
    AddressBookTransferDestinationsList {
        destinations: msg
            .destinations
            .iter()
            .map(destination_from_proto)
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

fn internal_whitelist_entry_from_proto(
    msg: &ProtoInternalWhitelistEntry,
) -> InternalTransferWhitelistEntry {
    InternalTransferWhitelistEntry {
        entry_id: format_uint64_id(msg.entry_id),
        scope: scope_from_proto(&msg.scope),
        root_account_id: nonzero_id(msg.root_account_id),
        target_account_id: nonzero_id(msg.target_account_id),
        target_scope_type: enum_proto_name(&msg.target_scope_type),
        smart_account_address: msg.smart_account_address.clone(),
        root_username: msg.root_username.clone(),
        sub_account_label: msg.subaccount_label.clone(),
        created_at: time(&msg.created_at),
        updated_at: time(&msg.updated_at),
        resolution_status: enum_proto_name(&msg.resolution_status),
    }
}

pub fn list_internal_whitelist_from_proto(
    msg: &ListInternalTransferWhitelistEntriesResponse,
) -> InternalTransferWhitelistEntriesList {
    InternalTransferWhitelistEntriesList {
        entries: msg
            .entries
            .iter()
            .map(internal_whitelist_entry_from_proto)
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

fn whitelist_view_from_proto(msg: &ProtoWithdrawWhitelistView) -> WithdrawWhitelistView {
    WithdrawWhitelistView {
        scope: scope_from_proto(&msg.scope),
        external_whitelist_required: msg.external_whitelist_required,
        internal_whitelist_required: msg.internal_whitelist_required,
        active_entries: msg
            .active_entries
            .iter()
            .map(|entry| MirroredWithdrawWhitelistEntry {
                canonical_address: entry.canonical_address.clone(),
                raw_address_hex: entry.raw_address_hex.clone(),
                updated_at: time(&entry.updated_at),
                polychain_chain_id: entry.polychain_chain_id,
            })
            .collect(),
    }
}

/// The withdraw whitelist view; `None` when the response carries none.
pub fn withdraw_whitelist_view_from_proto(
    msg: &GetWithdrawWhitelistViewResponse,
) -> Option<WithdrawWhitelistView> {
    msg.view.as_option().map(whitelist_view_from_proto)
}

fn external_entry_view_from_proto(msg: &ProtoExternalEntryView) -> ExternalAddressBookEntryView {
    ExternalAddressBookEntryView {
        address_book_entry_id: format_uint64_id(msg.address_book_entry_id),
        scope: scope_from_proto(&msg.scope),
        label: msg.label.clone(),
        note: msg.note.clone(),
        tag_ids: msg.tag_ids.iter().copied().map(format_uint64_id).collect(),
        whitelist_status: enum_proto_name(&msg.whitelist_status),
        polychain_chain_id: msg.polychain_chain_id,
        address: msg.address.clone(),
        created_at: time(&msg.created_at),
        updated_at: time(&msg.updated_at),
        revision: msg.revision,
    }
}

fn internal_entry_view_from_proto(msg: &ProtoInternalEntryView) -> InternalAddressBookEntryView {
    InternalAddressBookEntryView {
        address_book_entry_id: format_uint64_id(msg.address_book_entry_id),
        scope: scope_from_proto(&msg.scope),
        label: msg.label.clone(),
        note: msg.note.clone(),
        tag_ids: msg.tag_ids.iter().copied().map(format_uint64_id).collect(),
        whitelist_status: enum_proto_name(&msg.whitelist_status),
        root_account_id: nonzero_id(msg.root_account_id),
        target_account_id: nonzero_id(msg.target_account_id),
        target_scope_type: enum_proto_name(&msg.target_scope_type),
        smart_account_address: msg.smart_account_address.clone(),
        root_username: msg.root_username.clone(),
        sub_account_label: msg.subaccount_label.clone(),
        created_at: time(&msg.created_at),
        updated_at: time(&msg.updated_at),
        revision: msg.revision,
    }
}

/// The composed address-book view, with the public IDs the other reads use.
pub fn address_book_view_from_proto(msg: &GetAddressBookViewResponse) -> AddressBookView {
    AddressBookView {
        books: msg.books.iter().map(summary_from_proto).collect(),
        entries: msg
            .entries
            .as_option()
            .map(|entries| AddressBookEntriesView {
                external: entries
                    .external
                    .iter()
                    .map(external_entry_view_from_proto)
                    .collect(),
                internal: entries
                    .internal
                    .iter()
                    .map(internal_entry_view_from_proto)
                    .collect(),
            }),
        recent_destinations: msg.recent_destinations.as_option().map(|recent| {
            AddressBookRecentDestinationsView {
                external: recent
                    .external
                    .iter()
                    .map(|item| ExternalRecentDestination {
                        scope: scope_from_proto(&item.scope),
                        last_direction: enum_proto_name(&item.last_direction),
                        saved: item.saved,
                        address_book_entry_id: nonzero_id(item.address_book_entry_id),
                        use_count: item.use_count,
                        last_seen_at: time(&item.last_seen_at),
                        polychain_chain_id: item.polychain_chain_id,
                        address: item.address.clone(),
                    })
                    .collect(),
                internal: recent
                    .internal
                    .iter()
                    .map(|item| InternalRecentDestination {
                        scope: scope_from_proto(&item.scope),
                        last_direction: enum_proto_name(&item.last_direction),
                        saved: item.saved,
                        address_book_entry_id: nonzero_id(item.address_book_entry_id),
                        use_count: item.use_count,
                        last_seen_at: time(&item.last_seen_at),
                        root_account_id: nonzero_id(item.root_account_id),
                        target_account_id: nonzero_id(item.target_account_id),
                        target_scope_type: enum_proto_name(&item.target_scope_type),
                        smart_account_address: item.smart_account_address.clone(),
                        root_username: item.root_username.clone(),
                        sub_account_label: item.subaccount_label.clone(),
                    })
                    .collect(),
            }
        }),
        tags: msg
            .tags
            .iter()
            .map(|tag| AddressBookTagSummary {
                tag_id: format_uint64_id(tag.tag_id),
                name: tag.name.clone(),
                color: tag.color.clone(),
            })
            .collect(),
        withdraw_whitelist: msg
            .withdraw_whitelist
            .as_option()
            .map(whitelist_view_from_proto),
        recent_destinations_truncated: msg.recent_destinations_truncated,
        view_revision: msg.view_revision,
    }
}

pub fn address_book_invalidation_from_proto(
    msg: &AddressBookViewInvalidated,
) -> AddressBookViewInvalidation {
    let scope = scope_from_proto(&msg.scope);
    let invalidated_at = msg
        .invalidated_at
        .as_option()
        .map(|ts| format_rfc3339_nano(ts.seconds, ts.nanos))
        .unwrap_or_default();
    AddressBookViewInvalidation {
        scope,
        invalidated_at,
        view_revision: msg.view_revision,
    }
}

fn nonzero_id(id: u64) -> String {
    if id == 0 {
        String::new()
    } else {
        format_id(id)
    }
}

fn format_rfc3339_nano(seconds: i64, nanos: i32) -> String {
    let secs = seconds.max(0) as u64;
    let days = secs / 86_400;
    let tod = secs % 86_400;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    let (y, m, d) = civil_from_days(days as i64);
    let nanos = nanos.max(0) as u32;
    if nanos == 0 {
        format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{nanos:09}Z")
    }
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Howard Hinnant civil_from_days (unix epoch day offset).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::{
        AccountScopeRef, AccountScopeType, AddressBook, AddressBookEntriesView,
        AddressBookEntryKind, AddressBookRecentDestinationsView, AddressBookTagSummary,
        DestinationWhitelistStatus, ExternalAddressBookEntry, ExternalWithdrawAddress,
        GetAddressBookViewResponse, GetWithdrawWhitelistViewResponse, InternalAddressBookEntry,
        InternalRecentDestination, InternalTransferAccount, InternalTransferWhitelistEntry,
        ListInternalTransferWhitelistEntriesResponse, ListTransferCounterpartiesResponse,
        ListTransferDestinationsResponse, MirroredWithdrawWhitelistEntry, SubaccountRole,
        TransferCounterparty, TransferCounterpartyDirection, TransferDestination,
        WithdrawWhitelistView,
    };
    use buffa_types::google::protobuf::Timestamp;

    // An entry, its root and target accounts, and a counterparty: the decimal
    // IDs the server sends read as the public IDs every other read returns.
    const ENTRY_ID: u64 = 4_611_686_018_427_387_913;
    const ROOT_ACCOUNT_ID: u64 = 12_297_829_382_473_034_410;
    const TARGET_ACCOUNT_ID: u64 = 98_765_432_109_876_543;
    const COUNTERPARTY_ID: u64 = 13_835_058_055_282_163_729;
    const SMART_ACCOUNT: &str = "0x1111111111111111111111111111111111111111";

    fn public(id: u64) -> String {
        let id = format_id(id);
        assert!(
            !id.chars().all(|c| c.is_ascii_digit()),
            "{id} reads as decimal"
        );
        id
    }

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp {
            seconds,
            nanos,
            ..Default::default()
        }
    }

    fn root_scope() -> AccountScopeRef {
        AccountScopeRef {
            scope_type: AccountScopeType::SCOPE_ROOT.into(),
            root_account_id: ROOT_ACCOUNT_ID,
            ..Default::default()
        }
    }

    fn internal_account() -> InternalTransferAccount {
        InternalTransferAccount {
            root_account_id: ROOT_ACCOUNT_ID,
            target_account_id: TARGET_ACCOUNT_ID,
            target_scope_type: AccountScopeType::SCOPE_SUBACCOUNT.into(),
            smart_account_address: SMART_ACCOUNT.into(),
            root_username: "alice".into(),
            subaccount_label: "desk".into(),
            ..Default::default()
        }
    }

    fn internal_entry() -> ProtoAddressBookEntry {
        ProtoAddressBookEntry {
            address_book_entry_id: ENTRY_ID,
            scope: root_scope().into(),
            kind: AddressBookEntryKind::INTERNAL_ACCOUNT.into(),
            label: "treasury".into(),
            note: "cold wallet".into(),
            created_at: ts(1_794_505_093, 825_341_000).into(),
            updated_at: ts(1_794_505_094, 999_999_999).into(),
            revision: 1,
            entry: Some(internal_account().into()),
            ..Default::default()
        }
    }

    fn public_root_scope() -> AccountScope {
        AccountScope {
            scope_type: "SCOPE_ROOT".into(),
            root_account_id: public(ROOT_ACCOUNT_ID),
            sub_account_id: String::new(),
        }
    }

    fn public_internal_account() -> crate::models::InternalTransferAccount {
        crate::models::InternalTransferAccount {
            root_account_id: public(ROOT_ACCOUNT_ID),
            target_account_id: format_id(TARGET_ACCOUNT_ID),
            target_scope_type: "SCOPE_SUBACCOUNT".into(),
            smart_account_address: SMART_ACCOUNT.into(),
            root_username: "alice".into(),
            sub_account_label: "desk".into(),
        }
    }

    // Every contract field of an entry is read, on every call that returns
    // one: the destination, note, scope and times, with public IDs.
    #[test]
    fn entries_read_destination_note_scope_and_times() {
        let want = AddressBookEntry {
            address_book_entry_id: public(ENTRY_ID),
            scope: Some(public_root_scope()),
            kind: "INTERNAL_ACCOUNT".into(),
            label: "treasury".into(),
            note: "cold wallet".into(),
            created_at: Some(ts(1_794_505_093, 825_341_000)),
            updated_at: Some(ts(1_794_505_094, 999_999_999)),
            external: None,
            internal: Some(public_internal_account()),
            tags: Vec::new(),
            revision: 1,
        };
        let listed = list_entries_from_proto(&ListAddressBookEntriesResponse {
            entries: vec![internal_entry()],
            ..Default::default()
        });
        assert_eq!(listed.entries, vec![want.clone()]);
        let created = entry_from_create_proto(&CreateAddressBookEntryResponse {
            entry: internal_entry().into(),
            ..Default::default()
        })
        .unwrap();
        let updated = entry_from_update_proto(&UpdateAddressBookEntryResponse {
            entry: internal_entry().into(),
            ..Default::default()
        })
        .unwrap();
        let copied = entry_from_copy_proto(&CopyAddressBookEntryResponse {
            entry: internal_entry().into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            [created, updated, copied],
            [want.clone(), want.clone(), want]
        );

        let external = list_entries_from_proto(&ListAddressBookEntriesResponse {
            entries: vec![ProtoAddressBookEntry {
                address_book_entry_id: 9,
                kind: AddressBookEntryKind::EXTERNAL_CHAIN.into(),
                entry: Some(
                    ExternalWithdrawAddress {
                        polychain_chain_id: 3,
                        address: SMART_ACCOUNT.into(),
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        });
        let entry = &external.entries[0];
        assert_eq!(
            entry.external,
            Some(crate::models::ExternalWithdrawAddress {
                polychain_chain_id: 3,
                address: SMART_ACCOUNT.into(),
            })
        );
        assert_eq!(
            (entry.internal.as_ref(), entry.scope.as_ref()),
            (None, None)
        );
        assert_eq!(
            (entry.created_at.as_ref(), entry.updated_at.as_ref()),
            (None, None)
        );
    }

    // The view, the counterparty and destination lists, the internal-transfer
    // whitelist and the withdraw whitelist view are typed, and name the same
    // entry and accounts with the public IDs list_entries returns.
    #[test]
    fn views_and_lists_read_typed_public_ids() {
        let listed = list_entries_from_proto(&ListAddressBookEntriesResponse {
            entries: vec![internal_entry()],
            ..Default::default()
        });
        let view = address_book_view_from_proto(&GetAddressBookViewResponse {
            books: vec![AddressBook {
                scope: root_scope().into(),
                caller_role: SubaccountRole::OWNER.into(),
                label: "main".into(),
                ..Default::default()
            }],
            entries: AddressBookEntriesView {
                internal: vec![InternalAddressBookEntry {
                    address_book_entry_id: ENTRY_ID,
                    scope: root_scope().into(),
                    label: "treasury".into(),
                    note: "cold wallet".into(),
                    tag_ids: vec![11],
                    whitelist_status: DestinationWhitelistStatus::DESTINATION_WHITELIST_ACTIVE
                        .into(),
                    root_account_id: ROOT_ACCOUNT_ID,
                    target_account_id: TARGET_ACCOUNT_ID,
                    target_scope_type: AccountScopeType::SCOPE_SUBACCOUNT.into(),
                    smart_account_address: SMART_ACCOUNT.into(),
                    created_at: ts(1_794_505_093, 825_341_000).into(),
                    revision: 1,
                    ..Default::default()
                }],
                external: vec![ExternalAddressBookEntry {
                    address_book_entry_id: 9,
                    polychain_chain_id: 3,
                    address: SMART_ACCOUNT.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            recent_destinations: AddressBookRecentDestinationsView {
                internal: vec![InternalRecentDestination {
                    last_direction: TransferCounterpartyDirection::WITHDRAW_TO.into(),
                    saved: true,
                    address_book_entry_id: ENTRY_ID,
                    use_count: 4,
                    target_account_id: TARGET_ACCOUNT_ID,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            tags: vec![AddressBookTagSummary {
                tag_id: 11,
                name: "hot".into(),
                ..Default::default()
            }],
            withdraw_whitelist: WithdrawWhitelistView {
                scope: root_scope().into(),
                external_whitelist_required: true,
                active_entries: vec![MirroredWithdrawWhitelistEntry {
                    canonical_address: SMART_ACCOUNT.into(),
                    polychain_chain_id: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            recent_destinations_truncated: true,
            view_revision: 1_004_932_554_200,
            ..Default::default()
        });
        let entries = view.entries.as_ref().expect("entries");
        let internal = &entries.internal[0];
        assert_eq!(internal.address_book_entry_id, public(ENTRY_ID));
        assert_eq!(
            internal.address_book_entry_id,
            listed.entries[0].address_book_entry_id
        );
        assert_eq!(internal.scope, Some(public_root_scope()));
        assert_eq!(internal.root_account_id, public(ROOT_ACCOUNT_ID));
        assert_eq!(internal.target_account_id, format_id(TARGET_ACCOUNT_ID));
        assert_eq!(internal.tag_ids, vec![format_id(11)]);
        assert_eq!(internal.whitelist_status, "DESTINATION_WHITELIST_ACTIVE");
        assert_eq!(internal.created_at, Some(ts(1_794_505_093, 825_341_000)));
        assert_eq!(internal.revision, 1);
        assert_eq!(entries.external[0].address_book_entry_id, format_id(9));
        assert_eq!(entries.external[0].address, SMART_ACCOUNT);
        let recent = &view.recent_destinations.as_ref().expect("recent").internal[0];
        assert_eq!(recent.address_book_entry_id, public(ENTRY_ID));
        assert_eq!(recent.last_direction, "WITHDRAW_TO");
        assert_eq!((recent.saved, recent.use_count), (true, 4));
        assert_eq!(view.books[0].scope, Some(public_root_scope()));
        assert_eq!(view.books[0].caller_role, "OWNER");
        assert_eq!(view.tags[0].tag_id, format_id(11));
        let whitelist = view.withdraw_whitelist.as_ref().expect("whitelist");
        assert!(whitelist.external_whitelist_required);
        assert_eq!(whitelist.active_entries[0].canonical_address, SMART_ACCOUNT);
        assert!(view.recent_destinations_truncated);
        assert_eq!(view.view_revision, 1_004_932_554_200);

        let books = list_books_from_proto(&ListAddressBooksResponse {
            books: vec![AddressBook {
                scope: root_scope().into(),
                label: "main".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(books.books[0].label, "main");
        assert_eq!(books.books[0].scope, Some(public_root_scope()));

        let counterparties = list_counterparties_from_proto(&ListTransferCounterpartiesResponse {
            counterparties: vec![TransferCounterparty {
                counterparty_id: COUNTERPARTY_ID,
                scope: root_scope().into(),
                direction: TransferCounterpartyDirection::WITHDRAW_TO.into(),
                kind: AddressBookEntryKind::INTERNAL_ACCOUNT.into(),
                saved: true,
                address_book_entry_id: ENTRY_ID,
                use_count: 2,
                last_seen_at: ts(1_794_505_093, 0).into(),
                counterparty: Some(internal_account().into()),
                ..Default::default()
            }],
            truncated: true,
            ..Default::default()
        });
        let row = &counterparties.counterparties[0];
        assert_eq!(row.counterparty_id, public(COUNTERPARTY_ID));
        assert_eq!(row.address_book_entry_id, public(ENTRY_ID));
        assert_eq!(row.direction, "WITHDRAW_TO");
        assert_eq!(row.internal, Some(public_internal_account()));
        assert_eq!(row.last_seen_at, Some(ts(1_794_505_093, 0)));
        assert!(counterparties.truncated);

        let destinations = list_destinations_from_proto(&ListTransferDestinationsResponse {
            destinations: vec![TransferDestination {
                scope: root_scope().into(),
                kind: AddressBookEntryKind::INTERNAL_ACCOUNT.into(),
                saved: true,
                whitelisted: true,
                address_book_entry: internal_entry().into(),
                destination: Some(internal_account().into()),
                ..Default::default()
            }],
            next_page_token: "n".into(),
            ..Default::default()
        });
        let row = &destinations.destinations[0];
        assert_eq!(row.address_book_entry, Some(listed.entries[0].clone()));
        assert_eq!(row.internal, Some(public_internal_account()));
        assert!(row.whitelisted);
        assert_eq!(destinations.next_page_token, "n");

        let whitelist =
            list_internal_whitelist_from_proto(&ListInternalTransferWhitelistEntriesResponse {
                entries: vec![InternalTransferWhitelistEntry {
                    entry_id: ENTRY_ID,
                    scope: root_scope().into(),
                    root_account_id: ROOT_ACCOUNT_ID,
                    target_account_id: TARGET_ACCOUNT_ID,
                    ..Default::default()
                }],
                ..Default::default()
            });
        assert_eq!(whitelist.entries[0].entry_id, public(ENTRY_ID));
        assert_eq!(
            whitelist.entries[0].root_account_id,
            public(ROOT_ACCOUNT_ID)
        );
        assert_eq!(
            whitelist.entries[0].target_account_id,
            format_id(TARGET_ACCOUNT_ID)
        );

        let withdraw = withdraw_whitelist_view_from_proto(&GetWithdrawWhitelistViewResponse {
            view: WithdrawWhitelistView {
                scope: root_scope().into(),
                internal_whitelist_required: true,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .expect("view");
        assert_eq!(withdraw.scope, Some(public_root_scope()));
        assert!(withdraw.internal_whitelist_required);
        assert_eq!(
            withdraw_whitelist_view_from_proto(&GetWithdrawWhitelistViewResponse::default()),
            None
        );
    }

    #[test]
    fn list_entries_maps_id_label_kind() {
        let msg = ListAddressBookEntriesResponse {
            entries: vec![ProtoAddressBookEntry {
                address_book_entry_id: 7,
                label: "vault".into(),
                kind: AddressBookEntryKind::INTERNAL_ACCOUNT.into(),
                revision: 5,
                ..Default::default()
            }],
            next_page_token: "t".into(),
            ..Default::default()
        };
        let result = list_entries_from_proto(&msg);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].address_book_entry_id, format_uint64_id(7));
        assert_eq!(result.entries[0].label, "vault");
        assert!(!result.entries[0].kind.is_empty());
        assert_eq!(result.entries[0].revision, 5);
        assert!(result.entries[0].tags.is_empty());
        assert_eq!(result.next_page_token, "t");
    }

    fn proto_entry_with_tag() -> ProtoAddressBookEntry {
        ProtoAddressBookEntry {
            address_book_entry_id: 9,
            label: "tagged".into(),
            kind: AddressBookEntryKind::EXTERNAL_CHAIN.into(),
            revision: 3,
            tags: vec![ProtoAddressBookTag {
                tag_id: 11,
                name: "hot".into(),
                color: "#ff00aa".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn create_and_update_responses_include_tags() {
        let create = CreateAddressBookEntryResponse {
            entry: proto_entry_with_tag().into(),
            ..Default::default()
        };
        let created = entry_from_create_proto(&create).expect("create entry");
        assert_eq!(created.address_book_entry_id, format_uint64_id(9));
        assert_eq!(created.tags.len(), 1);
        assert_eq!(created.tags[0].tag_id, format_uint64_id(11));
        assert_eq!(created.tags[0].name, "hot");
        assert_eq!(created.tags[0].color, "#ff00aa");

        let update = UpdateAddressBookEntryResponse {
            entry: proto_entry_with_tag().into(),
            ..Default::default()
        };
        let updated = entry_from_update_proto(&update).expect("update entry");
        assert_eq!(updated.tags.len(), 1);
        assert_eq!(updated.tags[0].name, "hot");
    }

    #[test]
    fn mutation_responses_reject_missing_required_entities() {
        assert!(entry_from_create_proto(&CreateAddressBookEntryResponse::default()).is_err());
        assert!(entry_from_update_proto(&UpdateAddressBookEntryResponse::default()).is_err());
        assert!(entry_from_copy_proto(&CopyAddressBookEntryResponse::default()).is_err());
        assert!(tag_from_create_proto(&CreateAddressBookTagResponse::default()).is_err());
        assert!(tag_from_update_proto(&UpdateAddressBookTagResponse::default()).is_err());
    }

    // POLY-9020: the Devnet account from the ticket, root Xiw8QFupu6d with
    // sub-account 2nphEX67UQT.
    const ROOT_ID: u64 = 13_235_591_962_976_289_070;
    const SUB_ID: u64 = 771_156_649_032_427_868;

    fn invalidation(scope: Option<AccountScopeRef>) -> AddressBookViewInvalidation {
        let mut msg = AddressBookViewInvalidated {
            view_revision: 1_004_932_554_200,
            ..Default::default()
        };
        if let Some(scope) = scope {
            msg.scope = scope.into();
        }
        address_book_invalidation_from_proto(&msg)
    }

    // POLY-9020: a sub-account's change names the sub-account and its scope type.
    #[test]
    fn invalidation_keeps_the_subaccount_scope() {
        let out = invalidation(Some(AccountScopeRef {
            scope_type: crate::proto::auth::v1::AccountScopeType::SCOPE_SUBACCOUNT.into(),
            root_account_id: ROOT_ID,
            subaccount_id: SUB_ID,
            ..Default::default()
        }));
        assert_eq!(
            out.scope,
            Some(crate::models::AccountScope {
                scope_type: "SCOPE_SUBACCOUNT".into(),
                root_account_id: "Xiw8QFupu6d".into(),
                sub_account_id: "2nphEX67UQT".into(),
            })
        );
        assert_eq!(out.view_revision, 1_004_932_554_200);
    }

    // POLY-9020: a root change names only the root; no scope stays unset.
    #[test]
    fn invalidation_root_scope_has_no_subaccount() {
        let out = invalidation(Some(AccountScopeRef {
            scope_type: crate::proto::auth::v1::AccountScopeType::SCOPE_ROOT.into(),
            root_account_id: ROOT_ID,
            ..Default::default()
        }));
        assert_eq!(
            out.scope,
            Some(crate::models::AccountScope {
                scope_type: "SCOPE_ROOT".into(),
                root_account_id: "Xiw8QFupu6d".into(),
                sub_account_id: String::new(),
            })
        );
        assert_eq!(invalidation(None).scope, None);
    }

    #[test]
    fn invalidation_formats_scope_and_time() {
        let msg = AddressBookViewInvalidated {
            scope: AccountScopeRef {
                root_account_id: 42,
                ..Default::default()
            }
            .into(),
            invalidated_at: Timestamp {
                seconds: 0,
                nanos: 0,
                ..Default::default()
            }
            .into(),
            view_revision: 7,
            ..Default::default()
        };
        let out = address_book_invalidation_from_proto(&msg);
        assert_eq!(out.scope.unwrap().root_account_id, format_id(42));
        assert_eq!(out.invalidated_at, "1970-01-01T00:00:00Z");
        assert_eq!(out.view_revision, 7);
    }
}
