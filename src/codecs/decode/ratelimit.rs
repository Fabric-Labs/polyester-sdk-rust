//! Decode polyester.ratelimit.v1.RateLimitDetail into the public model.

use buffa::Enumeration;

use crate::models::RateLimitDetail;
use crate::proto::polyester::ratelimit::v1::RateLimitDetail as ProtoRateLimitDetail;

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>, unknown_prefix: &str) -> String {
    match value.as_known() {
        Some(known) => known.proto_name().to_owned(),
        None => format!("{unknown_prefix}({})", value.to_i32()),
    }
}

/// Decode a proto rate-limit detail into the public SDK model.
pub fn rate_limit_detail_from_proto(msg: &ProtoRateLimitDetail) -> RateLimitDetail {
    RateLimitDetail {
        reason: enum_label(&msg.reason, "UNKNOWN_FAILURE_REASON"),
        limit: msg.limit,
        remaining: msg.remaining,
        retry_after_ms: msg.retry_after_ms,
        policy_version: msg.policy_version,
        operation_id: msg.operation_id.clone(),
        policy_class: enum_label(&msg.policy_class, "UNKNOWN_POLICY_CLASS"),
        scope: enum_label(&msg.scope, "UNKNOWN_LIMITER_SCOPE"),
        refill_model: enum_label(&msg.refill_model, "UNKNOWN_REFILL_MODEL"),
    }
}

/// Prefer structured `retry_after_ms`, then a header-derived fallback.
pub fn prefer_rate_limit_retry_after(
    detail: Option<&RateLimitDetail>,
    fallback: Option<f64>,
) -> Option<f64> {
    detail
        .and_then(RateLimitDetail::retry_after_seconds)
        .or(fallback)
}
