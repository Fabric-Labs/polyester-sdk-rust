//! Public trading rate-limit catalog and account-scoped limit models.
//!
//! Distinct from [`super::RateLimitDetail`], which is the
//! `polyester.ratelimit.v1` quota-rejection payload.

use buffa_types::google::protobuf::Timestamp;

/// Weighted placement, cancellation or read quota for one VIP tier.
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
    /// The rules enforced for the selected account: placement, cancellation
    /// and read (`TRADING_RATE_LIMIT_CLASS_READ`). All of the account's API
    /// keys and sessions draw on these together: budget against them. The
    /// read rule is the selected account's own: the root account and each
    /// sub-account read on separate budgets, so reading one sub-account never
    /// uses another's. Order, fill, trigger, trading-limit and fee reads, and
    /// order previews, draw on the read limit of the account they read. A
    /// sub-account reads at its own read-limit override, else at its root
    /// account's VIP tier read limit, never on the root account's override.
    /// Select a rule by `policy_class`, not by its position or the number of
    /// rules.
    pub rules: Vec<TradingRateLimitRule>,
    /// The rules resolved for the calling API key, in the same order; empty
    /// for a session. The key's read rule is enforced on top of the
    /// account's: a read is refused when either is exhausted, so pace reads
    /// per API key too when the key has its own read rule. Its placement and
    /// cancellation rules are informational: trading requests are not
    /// limited per API key.
    pub api_key_rules: Vec<TradingRateLimitRule>,
}
