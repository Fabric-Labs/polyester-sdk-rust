//! Address book models (Go `models/address_book.go` thin parity).

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookEntry {
    pub address_book_entry_id: String,
    pub label: String,
    pub kind: String,
    /// Tags currently attached to this entry.
    pub tags: Vec<AddressBookTag>,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookEntriesList {
    pub entries: Vec<AddressBookEntry>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookTag {
    pub tag_id: String,
    pub name: String,
    pub color: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddressBooksList {
    pub books: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookViewInvalidation {
    pub scope: String,
    pub invalidated_at: String,
    /// Revision that `GetAddressBookView` must reach before this invalidation
    /// is considered satisfied.
    pub view_revision: u64,
}
