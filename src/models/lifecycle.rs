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
    pub owner_account_id: String,
    pub smart_account_address: String,
    /// Product-facing reason label (`unspecified`, snake catalog labels, or
    /// `unknown_reason_<n>` for open-enum forward compatibility).
    pub lifecycle_reason: String,
    pub zipper_reason: Option<ZipperReasonDetails>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowsList {
    pub flows: Vec<LifecycleFlowSummary>,
    pub next_page_token: String,
}
