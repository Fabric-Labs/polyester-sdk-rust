//! Public trading rate-limit catalog and account-scoped limit models.
//!
//! Distinct from [`super::RateLimitDetail`], which is the
//! `polyester.ratelimit.v1` quota-rejection payload.

use buffa_types::google::protobuf::Timestamp;

/// Weighted placement or cancellation quota for one VIP tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingRateLimitRule {
    /// Full protobuf enum name, for example `TRADING_RATE_LIMIT_CLASS_PLACE`.
    /// Unknown open-enum values become `UNKNOWN_TRADING_RATE_LIMIT_CLASS(n)`.
    pub policy_class: String,
    pub vip_tier: u32,
    pub quota_weight: u64,
    pub period_ms: u64,
    pub burst_weight: u64,
}

/// Complete public trading rate-limit catalog for one policy version.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitConfig {
    pub policy_version: u64,
    pub effective_from: Option<Timestamp>,
    pub rules: Vec<TradingRateLimitRule>,
}

/// Effective trading limits for one account target and caller. Trading
/// limits apply per account: the main account and each sub-account have their
/// own, and every API key and session that trades for an account shares them.
#[derive(Debug, Clone, PartialEq)]
pub struct TradingRateLimits {
    pub policy_version: u64,
    pub effective_from: Option<Timestamp>,
    /// The placement and cancellation rules enforced for the selected
    /// account, placement first and cancellation second. All of the
    /// account's API keys and sessions draw on these together: budget
    /// against them.
    pub rules: Vec<TradingRateLimitRule>,
    /// The rules resolved for the calling API key, placement first; empty
    /// for a session. Informational only: trading requests are not limited
    /// per API key, and only `rules` are enforced.
    pub api_key_rules: Vec<TradingRateLimitRule>,
}
