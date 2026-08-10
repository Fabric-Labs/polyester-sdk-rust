//! Public rate-limit rejection detail (polyester.ratelimit.v1).

/// Client-safe quota rejection payload from `polyester.ratelimit.v1.RateLimitDetail`.
///
/// Enum labels use the full protobuf enum name (for example `QUOTA_EXCEEDED`,
/// `TRADING_PLACE`, `API_KEY`). Unknown open-enum values become
/// `UNKNOWN_<FIELD>(<n>)`. Optional numeric fields preserve proto presence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitDetail {
    pub reason: String,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub retry_after_ms: Option<u64>,
    pub policy_version: Option<u64>,
    pub operation_id: String,
    pub policy_class: String,
    pub scope: String,
    pub refill_model: String,
}

impl RateLimitDetail {
    /// Convert `retry_after_ms` to seconds when present.
    pub fn retry_after_seconds(&self) -> Option<f64> {
        self.retry_after_ms.map(|ms| ms as f64 / 1000.0)
    }
}
