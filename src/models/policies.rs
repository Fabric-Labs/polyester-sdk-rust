//! Policy models (Go `models/policies.go` parity).

/// Allowed spot market in a policy view. `symbol` is catalog display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotMarketRule {
    pub symbol_id: u32,
    pub symbol: String,
}

/// The template a policy was copied from, recorded when the copy was made.
/// It never changes, even after the template is edited or deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyTemplateSource {
    /// Public ID of the template, base58.
    pub template_id: String,
    /// The template's revision when it was copied: compare it with the
    /// template's current `revision` to see whether it changed since.
    pub template_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubaccountPolicy {
    pub policy_id: String,
    pub name: String,
    pub description: String,
    pub spot_markets: Vec<SpotMarketRule>,
    /// `"ALL"` or `"ALLOWLIST"`, empty when unspecified. With `"ALL"`,
    /// `spot_markets` is for display only and is not enforced.
    pub spot_market_scope: String,
    /// Largest notional for one order, in quote microunits (0.000001 USDT).
    /// 0 means no explicit cap.
    pub max_order_notional: u64,
    /// Most resting orders at any time. 0 means no explicit cap.
    pub max_open_orders: u32,
    /// When true, new orders and exposure-increasing changes are refused.
    pub trading_halted: bool,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
    /// Granted `auth.v1.PolicyAction` names, such as `"TRADE_SPOT"` or
    /// `"READ_BALANCES"`, in the order the server sent them; `"UNKNOWN(n)"`
    /// for an action this SDK version does not know.
    pub actions: Vec<String>,
    /// True for a template: a reusable policy of the main account that is
    /// never attached itself. Applying it attaches a new copy, so editing it
    /// later changes nothing it was applied to.
    pub is_template: bool,
    /// The template this policy is a copy of; `None` when it is not a copy.
    pub source_template: Option<PolicyTemplateSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubaccountPoliciesList {
    pub policies: Vec<SubaccountPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPolicy {
    pub policy_id: String,
    pub name: String,
    pub description: String,
    pub spot_markets: Vec<SpotMarketRule>,
    /// `"ALL"` or `"ALLOWLIST"`, empty when unspecified. With `"ALL"`,
    /// `spot_markets` is for display only and is not enforced.
    pub spot_market_scope: String,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
    /// Granted `auth.v1.PolicyAction` names, in the order the server sent
    /// them; `"UNKNOWN(n)"` for an action this SDK version does not know.
    /// Account permissions, valid on API-key policies only, open account
    /// reads to API keys: `READ_API_KEYS`, `READ_API_POLICIES`,
    /// `READ_SUBACCOUNT_POLICIES`, `READ_SUBACCOUNT_PERMISSIONS`,
    /// `READ_VIP_STATUS`, `READ_SOCIAL_VERIFICATION`, `READ_PROFILE`,
    /// `READ_ACCOUNT_DIRECTORY`, `READ_WITHDRAW_VALIDATION`,
    /// `READ_DAILY_CLAIMS`, `CLAIM_DAILY_REWARD`, `READ_CHECKPOINTS`,
    /// `READ_REWARDS`, `CREATE_DEPOSIT_ADDRESS`, `READ_DEPOSIT_ADDRESSES`,
    /// `READ_SUBACCOUNTS`, `READ_SUBACCOUNT_ACTIVITY`,
    /// `READ_SUBACCOUNT_MEMBERS`, `READ_SUBACCOUNT_INVITES`,
    /// `READ_GUARD_SIGNER_STATUS`, `READ_STATEMENTS`,
    /// `READ_NOTIFICATIONS` (list, read and count the main account's
    /// notifications; main-account keys only) and `READ_ACCOUNT_ACTIVITY`
    /// (the security and activity events of the main account and its
    /// sub-accounts within the key's scope). `CONVERT_DUST` (quote and
    /// convert dust, read conversions) and `READ_FUNDING_FLOWS` (the
    /// account's own lifecycle flows) are product actions, valid on both
    /// policy kinds. Tell them apart by name, not by number:
    /// `CREATE_DEPOSIT_ADDRESS` is 10, among the product actions.
    pub actions: Vec<String>,
    /// True for a template: a reusable policy of the main account that is
    /// never attached itself. Applying it attaches a new copy, so editing it
    /// later changes nothing it was applied to.
    pub is_template: bool,
    /// The template this policy is a copy of; `None` when it is not a copy.
    pub source_template: Option<PolicyTemplateSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPoliciesList {
    pub policies: Vec<ApiPolicy>,
}
