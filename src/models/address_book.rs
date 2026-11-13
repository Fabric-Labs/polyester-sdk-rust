//! Address book models.
//!
//! Every ID is the public ID other reads return (base58, `"0"` for a record
//! whose own ID is unset); an optional reference the server leaves at 0, such
//! as a root scope's sub-account or the entry of an unsaved counterparty, is
//! empty. Enum fields read as their contract names (`"INTERNAL_ACCOUNT"`,
//! `"SCOPE_ROOT"`), and times are the server's `Timestamp`, `None` when unset.

use buffa_types::google::protobuf::Timestamp;

/// An external destination: an address on a chain.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExternalWithdrawAddress {
    pub polychain_chain_id: u32,
    pub address: String,
}

/// An internal destination: another Polyester account or sub-account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InternalTransferAccount {
    /// The destination's root account public ID.
    pub root_account_id: String,
    /// The destination account's public ID: the root account, or one of its
    /// sub-accounts.
    pub target_account_id: String,
    /// `SCOPE_ROOT` or `SCOPE_SUBACCOUNT`.
    pub target_scope_type: String,
    pub smart_account_address: String,
    pub root_username: String,
    pub sub_account_label: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookEntry {
    pub address_book_entry_id: String,
    /// The address book the entry belongs to.
    pub scope: Option<AccountScope>,
    /// `EXTERNAL_CHAIN` or `INTERNAL_ACCOUNT`.
    pub kind: String,
    pub label: String,
    pub note: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// The destination of an external entry; `None` for an internal one.
    pub external: Option<ExternalWithdrawAddress>,
    /// The destination of an internal entry; `None` for an external one.
    pub internal: Option<InternalTransferAccount>,
    /// Tags currently attached to this entry.
    pub tags: Vec<AddressBookTag>,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookEntriesList {
    pub entries: Vec<AddressBookEntry>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookTag {
    pub tag_id: String,
    /// The address book the tag belongs to; `None` when the server names none.
    pub scope: Option<AccountScope>,
    pub name: String,
    pub color: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
}

/// A tag as the address-book view lists it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookTagSummary {
    pub tag_id: String,
    pub name: String,
    pub color: String,
}

/// An address book the caller can use.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookSummary {
    pub scope: Option<AccountScope>,
    /// The caller's role on the book's account as its contract name
    /// (`"OWNER"`, `"TRADER"`, ...), as sub-account roles read.
    pub caller_role: String,
    pub label: String,
    pub owner_username: String,
    pub smart_account_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBooksList {
    pub books: Vec<AddressBookSummary>,
}

/// An address book: the root account's own, or one of its sub-accounts'.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AccountScope {
    /// `SCOPE_ROOT` or `SCOPE_SUBACCOUNT`.
    pub scope_type: String,
    /// The root account's public ID.
    pub root_account_id: String,
    /// The sub-account's public ID; empty for a root scope.
    pub sub_account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddressBookViewInvalidation {
    /// The address book that changed; `None` when the event names none.
    pub scope: Option<AccountScope>,
    pub invalidated_at: String,
    /// Revision that `GetAddressBookView` must reach before this invalidation
    /// is considered satisfied.
    pub view_revision: u64,
}

/// A destination the account has sent to or received from.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransferCounterparty {
    pub counterparty_id: String,
    pub scope: Option<AccountScope>,
    /// `DEPOSIT_FROM`, `WITHDRAW_TO`, `INTERNAL_TRANSFER_FROM` or
    /// `INTERNAL_TRANSFER_TO`.
    pub direction: String,
    /// `EXTERNAL_CHAIN` or `INTERNAL_ACCOUNT`.
    pub kind: String,
    /// The counterparty is saved in the address book, as
    /// `address_book_entry_id`.
    pub saved: bool,
    /// The saved entry's public ID; empty when not saved.
    pub address_book_entry_id: String,
    pub use_count: u64,
    pub first_seen_at: Option<Timestamp>,
    pub last_seen_at: Option<Timestamp>,
    pub external: Option<ExternalWithdrawAddress>,
    pub internal: Option<InternalTransferAccount>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransferCounterpartiesList {
    pub counterparties: Vec<TransferCounterparty>,
    pub truncated: bool,
}

/// A destination a transfer can be sent to, with its whitelist state.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookTransferDestination {
    pub scope: Option<AccountScope>,
    pub kind: String,
    pub saved: bool,
    pub whitelisted: bool,
    pub whitelist_status: String,
    /// The saved entry; `None` when the destination is not saved.
    pub address_book_entry: Option<AddressBookEntry>,
    pub external: Option<ExternalWithdrawAddress>,
    pub internal: Option<InternalTransferAccount>,
    pub whitelist_updated_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookTransferDestinationsList {
    pub destinations: Vec<AddressBookTransferDestination>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InternalTransferWhitelistEntry {
    pub entry_id: String,
    pub scope: Option<AccountScope>,
    pub root_account_id: String,
    pub target_account_id: String,
    pub target_scope_type: String,
    pub smart_account_address: String,
    pub root_username: String,
    pub sub_account_label: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// `INTERNAL_WHITELIST_RESOLVED` or `INTERNAL_WHITELIST_UNRESOLVED`.
    pub resolution_status: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InternalTransferWhitelistEntriesList {
    pub entries: Vec<InternalTransferWhitelistEntry>,
    pub next_page_token: String,
}

/// An active withdraw whitelist entry as the chain holds it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MirroredWithdrawWhitelistEntry {
    pub canonical_address: String,
    pub raw_address_hex: String,
    pub updated_at: Option<Timestamp>,
    pub polychain_chain_id: u32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct WithdrawWhitelistView {
    pub scope: Option<AccountScope>,
    pub external_whitelist_required: bool,
    pub internal_whitelist_required: bool,
    pub active_entries: Vec<MirroredWithdrawWhitelistEntry>,
}

/// An external entry as the address-book view lists it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExternalAddressBookEntryView {
    /// The same public ID `list_entries` returns for the entry.
    pub address_book_entry_id: String,
    pub scope: Option<AccountScope>,
    pub label: String,
    pub note: String,
    pub tag_ids: Vec<String>,
    pub whitelist_status: String,
    pub polychain_chain_id: u32,
    pub address: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// This entry's own revision, for `expected_revision` on an update; 0
    /// when the server sends none. Not the view's `view_revision`.
    pub revision: u64,
}

/// An internal entry as the address-book view lists it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InternalAddressBookEntryView {
    /// The same public ID `list_entries` returns for the entry.
    pub address_book_entry_id: String,
    pub scope: Option<AccountScope>,
    pub label: String,
    pub note: String,
    pub tag_ids: Vec<String>,
    pub whitelist_status: String,
    pub root_account_id: String,
    pub target_account_id: String,
    pub target_scope_type: String,
    pub smart_account_address: String,
    pub root_username: String,
    pub sub_account_label: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// This entry's own revision, for `expected_revision` on an update; 0
    /// when the server sends none. Not the view's `view_revision`.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookEntriesView {
    pub external: Vec<ExternalAddressBookEntryView>,
    pub internal: Vec<InternalAddressBookEntryView>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExternalRecentDestination {
    pub scope: Option<AccountScope>,
    pub last_direction: String,
    pub saved: bool,
    /// The saved entry's public ID; empty when not saved.
    pub address_book_entry_id: String,
    pub use_count: u64,
    pub last_seen_at: Option<Timestamp>,
    pub polychain_chain_id: u32,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InternalRecentDestination {
    pub scope: Option<AccountScope>,
    pub last_direction: String,
    pub saved: bool,
    /// The saved entry's public ID; empty when not saved.
    pub address_book_entry_id: String,
    pub use_count: u64,
    pub last_seen_at: Option<Timestamp>,
    pub root_account_id: String,
    pub target_account_id: String,
    pub target_scope_type: String,
    pub smart_account_address: String,
    pub root_username: String,
    pub sub_account_label: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookRecentDestinationsView {
    pub external: Vec<ExternalRecentDestination>,
    pub internal: Vec<InternalRecentDestination>,
}

/// The composed address-book view (`address_book.get_view`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AddressBookView {
    pub books: Vec<AddressBookSummary>,
    pub entries: Option<AddressBookEntriesView>,
    pub recent_destinations: Option<AddressBookRecentDestinationsView>,
    pub tags: Vec<AddressBookTagSummary>,
    pub withdraw_whitelist: Option<WithdrawWhitelistView>,
    pub recent_destinations_truncated: bool,
    /// The view's revision; pass it as `minimum_view_revision` to read a view
    /// at least this recent.
    pub view_revision: u64,
}
