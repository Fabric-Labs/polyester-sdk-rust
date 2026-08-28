//! Decode ratelimit.v1 catalog/account responses into public SDK models.

use buffa::Enumeration;
use buffa_types::google::protobuf::Timestamp;

use crate::models::{RateLimitConfig, TradingRateLimitRule, TradingRateLimits};
use crate::proto::ratelimit::v1::{
    GetRateLimitConfigResponse, GetTradingRateLimitsResponse,
    TradingRateLimitRule as ProtoTradingRateLimitRule,
};

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>, unknown_prefix: &str) -> String {
    match value.as_known() {
        Some(known) => known.proto_name().to_owned(),
        None => format!("{unknown_prefix}({})", value.to_i32()),
    }
}

fn clone_timestamp(ts: Option<&Timestamp>) -> Option<Timestamp> {
    ts.map(|t| Timestamp {
        seconds: t.seconds,
        nanos: t.nanos,
        ..Default::default()
    })
}

pub fn trading_rate_limit_rule_from_proto(msg: &ProtoTradingRateLimitRule) -> TradingRateLimitRule {
    TradingRateLimitRule {
        policy_class: enum_label(&msg.policy_class, "UNKNOWN_TRADING_RATE_LIMIT_CLASS"),
        vip_tier: msg.vip_tier,
        quota_weight: msg.quota_weight,
        period_ms: msg.period_ms,
        burst_weight: msg.burst_weight,
    }
}

pub fn rate_limit_config_from_proto(msg: &GetRateLimitConfigResponse) -> RateLimitConfig {
    RateLimitConfig {
        policy_version: msg.policy_version,
        effective_from: clone_timestamp(msg.effective_from.as_option()),
        rules: msg
            .rules
            .iter()
            .map(trading_rate_limit_rule_from_proto)
            .collect(),
    }
}

pub fn trading_rate_limits_from_proto(msg: &GetTradingRateLimitsResponse) -> TradingRateLimits {
    TradingRateLimits {
        policy_version: msg.policy_version,
        effective_from: clone_timestamp(msg.effective_from.as_option()),
        rules: msg
            .rules
            .iter()
            .map(trading_rate_limit_rule_from_proto)
            .collect(),
        api_key_rules: msg
            .api_key_rules
            .iter()
            .map(trading_rate_limit_rule_from_proto)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ratelimit::v1::TradingRateLimitClass;

    fn ts(seconds: i64) -> Timestamp {
        Timestamp {
            seconds,
            nanos: 0,
            ..Default::default()
        }
    }

    #[test]
    fn rate_limit_config_uses_full_policy_class_names() {
        let msg = GetRateLimitConfigResponse {
            policy_version: 9,
            effective_from: ts(50).into(),
            rules: vec![
                ProtoTradingRateLimitRule {
                    policy_class: TradingRateLimitClass::TRADING_RATE_LIMIT_CLASS_PLACE.into(),
                    vip_tier: 0,
                    quota_weight: 100,
                    period_ms: 1000,
                    burst_weight: 20,
                    ..Default::default()
                },
                ProtoTradingRateLimitRule {
                    policy_class: buffa::EnumValue::from(99),
                    vip_tier: 1,
                    quota_weight: 50,
                    period_ms: 1000,
                    burst_weight: 10,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let result = rate_limit_config_from_proto(&msg);
        assert_eq!(result.policy_version, 9);
        assert_eq!(
            result.rules[0].policy_class,
            "TRADING_RATE_LIMIT_CLASS_PLACE"
        );
        assert_eq!(
            result.rules[1].policy_class,
            "UNKNOWN_TRADING_RATE_LIMIT_CLASS(99)"
        );
    }

    #[test]
    fn trading_rate_limits_decode_account_and_api_key_rules() {
        let rule = ProtoTradingRateLimitRule {
            policy_class: TradingRateLimitClass::TRADING_RATE_LIMIT_CLASS_CANCEL.into(),
            vip_tier: 3,
            quota_weight: 200,
            period_ms: 500,
            burst_weight: 40,
            ..Default::default()
        };
        let msg = GetTradingRateLimitsResponse {
            policy_version: 4,
            rules: vec![rule.clone()],
            api_key_rules: vec![rule],
            ..Default::default()
        };
        let result = trading_rate_limits_from_proto(&msg);
        assert!(result.effective_from.is_none());
        assert_eq!(
            result.rules[0].policy_class,
            "TRADING_RATE_LIMIT_CLASS_CANCEL"
        );
        assert_eq!(result.api_key_rules[0].vip_tier, 3);
    }
}
