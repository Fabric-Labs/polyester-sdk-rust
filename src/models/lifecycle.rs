//! Lifecycle flow models (Go `models/trading.go` lifecycle parity).

use crate::types::AssetAmount;
use std::time::SystemTime;

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

/// A lifecycle flow: a deposit, withdrawal or transfer, with what moved,
/// where, and when.
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
    /// The flow's asset: the chain-specific asset mapping (zToken) row, and
    /// the unified asset that Funding, Trading and the other balances count
    /// it in. 0 when the server sends none.
    pub zipped_asset_id: u32,
    pub unified_asset_id: u32,
    /// The Polyester Chain the flow belongs to; 0 when it is not known.
    pub polyester_chain_id: u32,
    /// The flow's gross principal, exact, at the 18-decimal ledger scale in
    /// units of `unified_asset_id` (the amount's `asset_id`, `None` when the
    /// server names no unified asset). It does not include the request fee
    /// (`request_fee`). A deposit keeps its deposited amount after Zipper
    /// returned it or transferred it out, which is not the amount Zipper
    /// paid. `None` when the server sends none.
    pub amount: Option<AssetAmount>,
    /// The request's fee when one applies and is visible for the current
    /// step; `None` otherwise. Transaction lookups do not carry it.
    pub request_fee: Option<LifecycleRequestFee>,
    /// The first observed source transaction of the flow; for a transfer
    /// between Trading accounts, the Polyester Chain transaction that
    /// records the settled transfer. Empty until it is observed.
    pub source_tx_hash: String,
    /// Tells apart the flows of one transaction: the leg index of a batched
    /// source-chain deposit, or the log (or equivalent) index of an on-chain
    /// fact. 0 when none is available, which is also a first leg's index.
    pub tx_occurrence_index: u64,
    /// References the latest transaction behind the current visible state.
    /// It is opaque: a chain transaction hash or another reference, such as
    /// a decimal ID, depending on the step. Empty when there is none.
    pub latest_tx_ref: String,
    /// The product domains where the whole flow starts and where it ends or
    /// currently settles, for route labels such as Funding to Trading:
    /// `external_chain`, `zipper`, `funding`, `trading` or `lending`;
    /// `unspecified` when the server sends none, and `unknown_domain_<n>`
    /// for a domain newer than this SDK.
    pub source_domain: String,
    pub destination_domain: String,
    /// The flow's first observed business time (`started_at`) and the
    /// summary's last update (`updated_at`); `None` when the server
    /// sends none. Transaction lookups carry neither.
    pub started_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
    /// When the flow reached its final state; `None` while it is open.
    /// Transaction lookups do not carry it.
    pub terminal_at: Option<SystemTime>,
    /// The time lists order flows by; `None` when the server sends
    /// none.
    pub last_activity_at: Option<SystemTime>,
    /// When the flow is expected to complete; `None` when the server
    /// sends none, as transaction lookups do.
    ///
    /// The contract sends no estimate (`None` here) while `is_delayed` is
    /// true, because no reliable one exists then.
    ///
    /// The contract does not say what a terminal flow carries here. Devnet
    /// sends the flow's terminal time, the same as `terminal_at`, but do not
    /// rely on that: read `terminal_at` for when a flow ended.
    pub estimated_completion_at: Option<SystemTime>,
    /// The one-based position of `latest_step` in `progress_timeline`; 0
    /// when the server sends none.
    pub current_step_sequence: u32,
    /// The current step's progress while the flow is open; `None` when the
    /// server sends none. On a flow that is no longer open it may be present
    /// with every field zero. Transaction lookups do not carry it.
    pub current_progress: Option<LifecycleFlowProgress>,
    /// The flow's expected steps in order, for a progress bar. Its current
    /// step may be derived ahead of the latest step actually observed. Empty
    /// when the server sends none, as transaction lookups do.
    pub progress_timeline: Vec<LifecycleFlowTimelineItem>,
}

/// The fee a lifecycle flow's request pays, apart from the flow's gross
/// [`LifecycleFlowSummary::amount`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleRequestFee {
    /// The fee asset, as on [`LifecycleFlowSummary`]; 0 when the server
    /// sends none.
    pub zipped_asset_id: u32,
    pub unified_asset_id: u32,
    /// The fee, exact, at the 18-decimal ledger scale in units of the fee's
    /// unified asset; `None` when the server sends none.
    pub amount: Option<AssetAmount>,
    /// Receives the fee once it settles on chain; empty before.
    pub recipient_address: String,
    /// `locked` (the request locked the fee when it was created), `settled`
    /// (the request completed far enough for the fee to settle) or `kept`
    /// (the request did not complete and the fee was charged anyway: Zipper
    /// does not refund a withdrawal's fee when it rejects the withdrawal or
    /// its execution fails); `unspecified` when the server sends none, and
    /// `unknown_fee_status_<n>` for a status newer than this SDK.
    pub status: String,
}

/// The progress of an open lifecycle flow's current step. The counts are 0
/// when they do not apply to the step: confirmations to a step waiting on
/// source-chain confirmations, approvals and rejections to a validation
/// step.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleFlowProgress {
    /// When the current step started; `None` when the server sends
    /// none.
    pub current_step_started_at: Option<SystemTime>,
    /// How long the current step is expected to take, in milliseconds.
    pub current_step_expected_duration_ms: u64,
    pub current_confirmations: u32,
    pub required_confirmations: u32,
    pub approve_count: u32,
    pub reject_count: u32,
    /// The validator set's size.
    pub validator_count: u32,
    /// The approvals the step needs (`required_approvals`), and the
    /// rejections that would fail it (`required_rejections`).
    pub required_approvals: u32,
    pub required_rejections: u32,
}

/// One step of a lifecycle flow's progress timeline.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleFlowTimelineItem {
    /// The step's one-based position in the timeline.
    pub sequence: u32,
    /// Names the step as [`LifecycleFlowSummary::latest_step`] does.
    pub step: String,
    /// `completed` (observed, or behind the current step), `current` or
    /// `planned` (expected, not observed yet); `unspecified` when the server
    /// sends none, and `unknown_timeline_status_<n>` for a status newer than
    /// this SDK.
    pub status: String,
    /// How long the step is expected to take, in milliseconds, from recent
    /// timing of the same kind of flow; 0 when there is no estimate.
    pub expected_duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowsList {
    pub flows: Vec<LifecycleFlowSummary>,
    pub next_page_token: String,
}
