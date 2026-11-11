//! Policy models (Go `models/policies.go` parity).

/// Allowed spot market in a policy view. `symbol` is catalog display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotMarketRule {
    pub symbol_id: u32,
    pub symbol: String,
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
    /// `READ_SUBACCOUNT_MEMBERS`, `READ_SUBACCOUNT_INVITES` and
    /// `READ_GUARD_SIGNER_STATUS`. Tell them apart by name, not by number:
    /// `CREATE_DEPOSIT_ADDRESS` is 10, among the product actions.
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPoliciesList {
    pub policies: Vec<ApiPolicy>,
}
