//! Decode vip.v1 responses into public SDK models.

use crate::models::{NextVIPTierThresholds, VIPStatus, VIPTier, VIPTiersList};
use crate::proto::vip::v1::{
    GetVIPStatusResponse, ListVIPTiersResponse,
    NextVIPTierThresholds as ProtoNextVIPTierThresholds, VIPTier as ProtoVIPTier,
};
use buffa_types::google::protobuf::Timestamp;

fn clone_timestamp(ts: Option<&Timestamp>) -> Option<Timestamp> {
    ts.map(|t| Timestamp {
        seconds: t.seconds,
        nanos: t.nanos,
        ..Default::default()
    })
}

pub fn vip_tier_from_proto(msg: &ProtoVIPTier) -> VIPTier {
    VIPTier {
        tier: msg.tier,
        volume_threshold_usd: msg.volume_threshold_usd.clone(),
        aop_threshold_usd: msg.aop_threshold_usd.clone(),
        maker_fee_rate_percent: msg.maker_fee_rate_percent.clone(),
        taker_fee_rate_percent: msg.taker_fee_rate_percent.clone(),
    }
}

pub fn vip_tiers_list_from_proto(msg: &ListVIPTiersResponse) -> VIPTiersList {
    VIPTiersList {
        policy_version: msg.policy_version,
        effective_from: clone_timestamp(msg.effective_from.as_option()),
        retention_threshold_bp: msg.retention_threshold_bp,
        tiers: msg.tiers.iter().map(vip_tier_from_proto).collect(),
    }
}

pub fn next_vip_tier_thresholds_from_proto(
    msg: &ProtoNextVIPTierThresholds,
) -> NextVIPTierThresholds {
    NextVIPTierThresholds {
        tier: msg.tier,
        volume_threshold_usd: msg.volume_threshold_usd.clone(),
        aop_threshold_usd: msg.aop_threshold_usd.clone(),
    }
}

pub fn vip_status_from_proto(msg: &GetVIPStatusResponse) -> VIPStatus {
    VIPStatus {
        tier: msg.tier,
        volume_tier: msg.volume_tier,
        aop_tier: msg.aop_tier,
        settled_volume_30d_usd: msg.settled_volume_30d_usd.clone(),
        average_aop_30d_usd: msg.average_aop_30d_usd.clone(),
        policy_version: msg.policy_version,
        policy_effective_from: clone_timestamp(msg.policy_effective_from.as_option()),
        effective_from: clone_timestamp(msg.effective_from.as_option()),
        evaluated_at: clone_timestamp(msg.evaluated_at.as_option()),
        metrics_as_of: clone_timestamp(msg.metrics_as_of.as_option()),
        next_tier_thresholds: msg
            .next_tier_thresholds
            .as_option()
            .map(next_vip_tier_thresholds_from_proto),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::vip::v1::VIPTier as ProtoVIPTier;

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp {
            seconds,
            nanos,
            ..Default::default()
        }
    }

    #[test]
    fn vip_tiers_preserve_optional_aop_and_timestamps() {
        let msg = ListVIPTiersResponse {
            policy_version: 7,
            effective_from: ts(1_700_000_000, 250_000_000).into(),
            retention_threshold_bp: 9500,
            tiers: vec![
                ProtoVIPTier {
                    tier: 0,
                    volume_threshold_usd: "0".into(),
                    maker_fee_rate_percent: "0.02".into(),
                    taker_fee_rate_percent: "0.05".into(),
                    ..Default::default()
                },
                ProtoVIPTier {
                    tier: 1,
                    volume_threshold_usd: "100000".into(),
                    aop_threshold_usd: Some("50000.5".into()),
                    maker_fee_rate_percent: "0.01".into(),
                    taker_fee_rate_percent: "0.04".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let result = vip_tiers_list_from_proto(&msg);
        assert_eq!(result.policy_version, 7);
        assert_eq!(result.retention_threshold_bp, 9500);
        assert_eq!(
            result.effective_from.as_ref().map(|t| (t.seconds, t.nanos)),
            Some((1_700_000_000, 250_000_000))
        );
        assert!(result.tiers[0].aop_threshold_usd.is_none());
        assert_eq!(
            result.tiers[1].aop_threshold_usd.as_deref(),
            Some("50000.5")
        );
        assert_eq!(result.tiers[1].volume_threshold_usd, "100000");
    }

    #[test]
    fn vip_status_omits_unset_qualification_fields() {
        let msg = GetVIPStatusResponse {
            policy_version: 1,
            policy_effective_from: ts(1_700_000_100, 0).into(),
            ..Default::default()
        };
        let status = vip_status_from_proto(&msg);
        assert_eq!(status.tier, 0);
        assert!(status.settled_volume_30d_usd.is_none());
        assert!(status.average_aop_30d_usd.is_none());
        assert!(status.effective_from.is_none());
        assert!(status.evaluated_at.is_none());
        assert!(status.metrics_as_of.is_none());
        assert!(status.next_tier_thresholds.is_none());
        assert_eq!(
            status
                .policy_effective_from
                .as_ref()
                .map(|t| (t.seconds, t.nanos)),
            Some((1_700_000_100, 0))
        );
    }

    #[test]
    fn vip_status_surfaces_next_tier_and_metrics() {
        let msg = GetVIPStatusResponse {
            tier: 2,
            volume_tier: 2,
            aop_tier: 1,
            settled_volume_30d_usd: Some("250000.12".into()),
            average_aop_30d_usd: Some("80000".into()),
            policy_version: 3,
            policy_effective_from: ts(10, 0).into(),
            effective_from: ts(20, 0).into(),
            evaluated_at: ts(30, 0).into(),
            metrics_as_of: ts(40, 0).into(),
            next_tier_thresholds: ProtoNextVIPTierThresholds {
                tier: 3,
                volume_threshold_usd: "500000".into(),
                aop_threshold_usd: "150000".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let status = vip_status_from_proto(&msg);
        assert_eq!(status.settled_volume_30d_usd.as_deref(), Some("250000.12"));
        let next = status.next_tier_thresholds.expect("next tier");
        assert_eq!(next.tier, 3);
        assert_eq!(status.metrics_as_of.as_ref().map(|t| t.seconds), Some(40));
    }
}
