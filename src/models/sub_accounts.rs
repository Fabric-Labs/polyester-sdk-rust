//! Sub-account models (Go `models/sub_accounts.go` thin parity).

use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccount {
    pub subaccount_id: String,
    pub label: String,
    pub smart_account_address: String,
    pub status: String,
    pub updated_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccountsList {
    pub subaccounts: Vec<SubAccount>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GetSubaccountResult {
    pub subaccount: Option<SubAccount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateSubaccountResult {
    pub subaccount_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountMember {
    pub grantee_account_id: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountMembersList {
    pub members: Vec<SubAccountMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountInvite {
    pub invite_id: String,
    pub grantee_account_id: String,
    pub role: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountInvitesList {
    pub invites: Vec<SubAccountInvite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountActivityEvent {
    pub event_type: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountActivityList {
    pub events: Vec<SubAccountActivityEvent>,
    pub next_page_token: String,
}
