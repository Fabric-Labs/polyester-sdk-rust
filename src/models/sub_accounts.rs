//! Sub-account models (Go `models/sub_accounts.go` thin parity).

use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccount {
    pub subaccount_id: String,
    pub label: String,
    pub smart_account_address: String,
    pub status: String,
    pub updated_at: Option<Timestamp>,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
    /// The caller's role on this sub-account as its proto name (`"VIEWER"`,
    /// `"TRADER"`, `"LEVERAGED_TRADER"`, `"TREASURY"`, `"ADMIN"`, `"OWNER"`),
    /// as member and invite roles read; `"UNKNOWN(n)"` for a role this
    /// version does not know. `None` when the server sends none, as on a
    /// removal: no role is assumed.
    pub role: Option<String>,
    /// The caller no longer has access to this sub-account, for example after
    /// its owner removed the caller as a member: drop it from the caller's
    /// list. Only real-time updates (`sub_accounts.subscribe`) set it;
    /// reads list only accessible sub-accounts. A role or access change does
    /// not change `revision` or `updated_at`, so apply each update as it
    /// arrives.
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccountsList {
    pub subaccounts: Vec<SubAccount>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GetSubaccountResult {
    pub subaccount: Option<SubAccount>,
    /// The API keys scoped to the sub-account, read as
    /// [`crate::services::ApiKeysService::list`] reads them. Empty unless the
    /// read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_api_keys`] and the
    /// server sent some: it sends them to an owner or admin of the
    /// sub-account only.
    pub api_keys: Vec<crate::models::ApiKeySummary>,
    /// The sub-account's current members, read as
    /// [`crate::services::SubAccountsService::list_members`] reads them.
    /// Empty unless the read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_members`] and the
    /// server sent some: it sends them to a current member, and leaves them
    /// out for an API key without `READ_SUBACCOUNT_MEMBERS`.
    pub members: Vec<SubAccountMember>,
    /// The invitations visible to the caller, read as
    /// [`crate::services::SubAccountsService::list_invites`] reads them and
    /// filtered by [`crate::services::GetSubaccountOpts::invites_direction`].
    /// Empty unless the read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_invites`] and the
    /// server sent some: it leaves them out for an API key without
    /// `READ_SUBACCOUNT_INVITES`.
    pub invites: Vec<SubAccountInvite>,
    /// The sub-account policy attached to the sub-account, read as
    /// [`crate::services::PoliciesService::subscribe_subaccount_policies`]
    /// reads it. `Some` only when the read asked for it with
    /// [`crate::services::GetSubaccountOpts::include_policy`], the caller is
    /// a current member and a policy is attached.
    pub policy: Option<crate::models::SubaccountPolicy>,
    /// The sub-account's current balances, one row per asset as the
    /// balances list returns them. `Some` only when the read asked for them
    /// with [`crate::services::GetSubaccountOpts::include_balances`].
    pub balances: Option<crate::models::BalancesList>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateSubaccountResult {
    pub subaccount_id: String,
    /// Initial monotonic revision for the next revision-gated mutation.
    pub revision: u64,
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
