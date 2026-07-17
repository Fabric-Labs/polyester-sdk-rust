//! Lifecycle flow models (Go `models/trading.go` lifecycle parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowSummary {
    pub intent_id: String,
    pub flow_kind: String,
    pub latest_step: String,
    pub is_open: bool,
    pub is_terminal: bool,
    pub owner_account_id: String,
    pub smart_account_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleFlowsList {
    pub flows: Vec<LifecycleFlowSummary>,
    pub next_page_token: String,
}
