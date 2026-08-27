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
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPoliciesList {
    pub policies: Vec<ApiPolicy>,
}
