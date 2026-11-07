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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPoliciesList {
    pub policies: Vec<ApiPolicy>,
}
