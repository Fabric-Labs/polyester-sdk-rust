//! Address book decoders.

use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::{format_id, format_uint64_id};
use crate::models::{
    AddressBookEntriesList, AddressBookEntry, AddressBookTag, AddressBookViewInvalidation,
    AddressBooksList,
};
use crate::proto::auth::v1::{
    AddressBookEntry as ProtoAddressBookEntry, AddressBookTag as ProtoAddressBookTag,
    AddressBookViewInvalidated, CopyAddressBookEntryResponse, CreateAddressBookEntryResponse,
    CreateAddressBookTagResponse, ListAddressBookEntriesResponse, ListAddressBooksResponse,
    UpdateAddressBookEntryResponse, UpdateAddressBookTagResponse,
};

pub fn list_books_from_proto(msg: &ListAddressBooksResponse) -> AddressBooksList {
    AddressBooksList {
        books: msg
            .books
            .iter()
            .filter_map(|book| serde_json::to_value(book).ok())
            .collect(),
    }
}

fn entry_from_proto(msg: &ProtoAddressBookEntry) -> AddressBookEntry {
    AddressBookEntry {
        address_book_entry_id: format_uint64_id(msg.address_book_entry_id),
        label: msg.label.clone(),
        kind: enum_proto_name(&msg.kind),
        revision: msg.revision,
    }
}

pub fn list_entries_from_proto(msg: &ListAddressBookEntriesResponse) -> AddressBookEntriesList {
    AddressBookEntriesList {
        entries: msg.entries.iter().map(entry_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn entry_from_create_proto(msg: &CreateAddressBookEntryResponse) -> AddressBookEntry {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .unwrap_or_default()
}

pub fn entry_from_update_proto(msg: &UpdateAddressBookEntryResponse) -> AddressBookEntry {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .unwrap_or_default()
}

pub fn entry_from_copy_proto(msg: &CopyAddressBookEntryResponse) -> AddressBookEntry {
    msg.entry
        .as_option()
        .map(entry_from_proto)
        .unwrap_or_default()
}

fn tag_from_proto(msg: &ProtoAddressBookTag) -> AddressBookTag {
    AddressBookTag {
        tag_id: format_uint64_id(msg.tag_id),
        name: msg.name.clone(),
        color: msg.color.clone(),
    }
}

pub fn tag_from_create_proto(msg: &CreateAddressBookTagResponse) -> AddressBookTag {
    msg.tag.as_option().map(tag_from_proto).unwrap_or_default()
}

pub fn tag_from_update_proto(msg: &UpdateAddressBookTagResponse) -> AddressBookTag {
    msg.tag.as_option().map(tag_from_proto).unwrap_or_default()
}

pub fn address_book_invalidation_from_proto(
    msg: &AddressBookViewInvalidated,
) -> AddressBookViewInvalidation {
    let scope = msg
        .scope
        .as_option()
        .filter(|s| s.root_account_id != 0)
        .map(|s| format_id(s.root_account_id))
        .unwrap_or_default();
    let invalidated_at = msg
        .invalidated_at
        .as_option()
        .map(|ts| format_rfc3339_nano(ts.seconds, ts.nanos))
        .unwrap_or_default();
    AddressBookViewInvalidation {
        scope,
        invalidated_at,
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
    use crate::proto::auth::v1::{AccountScopeRef, AddressBook, AddressBookEntryKind};
    use buffa_types::google::protobuf::Timestamp;

    #[test]
    fn list_books_serializes_rows() {
        let msg = ListAddressBooksResponse {
            books: vec![AddressBook {
                label: "main".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = list_books_from_proto(&msg);
        assert_eq!(result.books.len(), 1);
        assert_eq!(result.books[0]["label"], "main");
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
        assert_eq!(result.next_page_token, "t");
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
            ..Default::default()
        };
        let out = address_book_invalidation_from_proto(&msg);
        assert!(!out.scope.is_empty());
        assert_eq!(out.invalidated_at, "1970-01-01T00:00:00Z");
    }
}
