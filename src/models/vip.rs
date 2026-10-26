//! VIP catalog and caller-root status models.

use buffa_types::google::protobuf::Timestamp;

/// One VIP0–VIP10 catalog row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VIPTier {
    pub tier: u32,
    pub volume_threshold_usd: String,
    /// Omitted for VIP0, which has no average-portfolio-value entry threshold.
    pub aop_threshold_usd: Option<String>,
    pub maker_fee_rate_percent: String,
    pub taker_fee_rate_percent: String,
}

/// Complete active VIP policy catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct VIPTiersList {
    pub policy_version: u64,
    pub effective_from: Option<Timestamp>,
    pub retention_threshold_bp: u32,
    pub tiers: Vec<VIPTier>,
}

/// Entry thresholds for the tier immediately above the effective tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextVIPTierThresholds {
    pub tier: u32,
    pub volume_threshold_usd: String,
    pub aop_threshold_usd: String,
}

/// Completeness of one rolling 30-day VIP qualification metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VIPMetricQuality {
    /// `complete`, `price_unavailable` or `incomplete_history`; a value
    /// newer than this SDK reads `unknown(<n>)`.
    pub status: String,
    /// Hours of the 30-day window the metric could not use.
    pub incomplete_hours: u32,
}

/// Authenticated caller-root VIP assignment and qualification facts.
#[derive(Debug, Clone, PartialEq)]
pub struct VIPStatus {
    pub tier: u32,
    pub volume_tier: u32,
    pub aop_tier: u32,
    pub settled_volume_30d_usd: Option<String>,
    pub average_aop_30d_usd: Option<String>,
    pub policy_version: u64,
    pub policy_effective_from: Option<Timestamp>,
    pub effective_from: Option<Timestamp>,
    pub evaluated_at: Option<Timestamp>,
    pub metrics_as_of: Option<Timestamp>,
    pub next_tier_thresholds: Option<NextVIPTierThresholds>,
    /// Completeness of `settled_volume_30d_usd`. When its status is
    /// `price_unavailable` the volume is a lower bound. `None` when the
    /// assignment carries no automatic qualification evidence.
    pub settled_volume_quality: Option<VIPMetricQuality>,
    /// Completeness of `average_aop_30d_usd`; `None` as above.
    pub average_aop_quality: Option<VIPMetricQuality>,
}
