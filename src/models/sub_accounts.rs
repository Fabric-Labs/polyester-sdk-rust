//! Sub-account models (Go `models/sub_accounts.go` thin parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAccount {
    pub subaccount_id: String,
    pub label: String,
    pub smart_account_address: String,
    pub status: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAccountsList {
    pub subaccounts: Vec<SubAccount>,
}
