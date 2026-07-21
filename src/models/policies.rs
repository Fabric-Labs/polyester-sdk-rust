//! Policy models (Go `models/policies.go` parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubaccountPolicy {
    pub policy_id: String,
    pub name: String,
    pub description: String,
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
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPoliciesList {
    pub policies: Vec<ApiPolicy>,
}
