//! Lifecycle flow models (Go `models/trading.go` lifecycle parity).

/// Specific Zipper reason details when a notable outcome has a catalog entry.
///
/// Show `message`, or localize with `reason_id`, rather than text keyed by
/// `code`: they can describe the outcome when the code does not. A
/// withdrawal whose execution failed after its destination transaction was
/// committed (lifecycle reason `zipper_withdraw_execution_failed`) reads
/// `withdrawal.execution_failed.under_review` and a message that it is under
/// review by the bridge operator and its funds were not returned, whatever
/// the code, which is 0 when the bridge gave none. It is not a refund, and
/// the user may have received the funds on the destination chain.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZipperReasonDetails {
    /// Numeric [`crate::proto::chain::zipper::v1::ZipperReasonCode`] wire value.
    pub code: i32,
    /// Stable reason identifier for localization.
    pub reason_id: String,
    /// User-displayable English explanation of the outcome.
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
    /// Smart account address of the account the flow belongs to: the
    /// account's public identity on Polyester Chain, and the value of the
    /// `list_flows` account selector.
    pub smart_account_address: String,
    /// Product-facing reason label (`unspecified`, snake catalog labels, or
    /// `unknown_reason_<n>` for open-enum forward compatibility). Every
    /// reason marks a flow that did not complete normally except
    /// `zipper_deposit_delivered_to_wallet`, the one completed flow that
    /// carries a reason: the deposit's zTokens were delivered to the
    /// account's own Polyester Chain wallet (`smart_account_address`) because
    /// the destination did not accept the credit, for example while deposits
    /// into it were paused. Show it as completed to the wallet ("Delivered to
    /// wallet; Funding was not credited"), not as a failure and not as
    /// credited to Funding or Trading.
    pub lifecycle_reason: String,
    pub zipper_reason: Option<ZipperReasonDetails>,
    /// How the selected account takes part in the flow, on a `list_flows` row
    /// of a list that selects an account and on every `list_account_flows`
    /// row and `get_account_flow` reply: `owner` (its deposit, withdrawal or
    /// a transfer it sent, including a move between its own balances) or
    /// `recipient` (a transfer another account sent to it). `unspecified` on
    /// an unscoped list, a public flow detail and a realtime update; empty on
    /// a transaction lookup. A value newer than this SDK reads
    /// `unknown_participation_<n>`.
    pub participation: String,
    /// For a transfer, the counterparty is `destination_address` when the
    /// account sent it and `source_address` when it received it.
    pub source_address: String,
    pub destination_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowsList {
    pub flows: Vec<LifecycleFlowSummary>,
    pub next_page_token: String,
}
