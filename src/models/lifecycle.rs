//! Lifecycle flow models (Go `models/trading.go` lifecycle parity).

/// Specific Zipper reason details when a notable outcome has a catalog entry.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZipperReasonDetails {
    /// Numeric [`crate::proto::chain::zipper::v1::ZipperReasonCode`] wire value.
    pub code: i32,
    pub reason_id: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleFlowSummary {
    pub intent_id: String,
    pub flow_kind: String,
    pub latest_step: String,
    pub is_open: bool,
    pub is_terminal: bool,
    /// True when an open flow has made no progress for well beyond the
    /// expected time of its current step. Show it as delayed instead of an
    /// ETA; it still completes normally once progress resumes. Always false
    /// for terminal flows and for transaction lookups, which do not report it.
    pub is_delayed: bool,
    pub owner_account_id: String,
    pub smart_account_address: String,
    /// Product-facing reason label (`unspecified`, snake catalog labels, or
    /// `unknown_reason_<n>` for open-enum forward compatibility).
    pub lifecycle_reason: String,
    pub zipper_reason: Option<ZipperReasonDetails>,
    /// How the selected account takes part in the flow, on a `list_flows` row
    /// of a list that selects an account: `owner` (its deposit, withdrawal or
    /// a transfer it sent, including a move between its own balances) or
    /// `recipient` (a transfer another account sent to it). `unspecified` on
    /// an unscoped list, a flow detail and a realtime update; empty on a
    /// transaction lookup. A value newer than this SDK reads
    /// `unknown_participation_<n>`.
    pub participation: String,
    /// For a transfer, the counterparty is `destination_address` when the
    /// account sent it and `source_address` (with `owner_account_id`) when it
    /// received it.
    pub source_address: String,
    pub destination_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowsList {
    pub flows: Vec<LifecycleFlowSummary>,
    pub next_page_token: String,
}
