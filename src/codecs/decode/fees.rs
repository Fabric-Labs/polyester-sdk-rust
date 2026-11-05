//! Decode fees.v1 responses into public SDK models.

use crate::models::{
    SpotFeeDiscount, SpotFeeMultiplier, SpotFeeRate, SpotFeeRateBreakdown, SpotFeeRatesList,
};
use crate::proto::fees::v1::{GetSpotFeeRatesResponse, SpotFeeRate as ProtoSpotFeeRate};

pub fn spot_fee_rate_from_proto(msg: &ProtoSpotFeeRate) -> SpotFeeRate {
    SpotFeeRate {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
        maker_fee_rate_percent: msg.maker_fee_rate_percent.clone(),
        taker_fee_rate_percent: msg.taker_fee_rate_percent.clone(),
        vip_tier: msg.vip_tier,
        reserve_fee_rate_percent: msg.reserve_fee_rate_percent.clone(),
        post_only_reserve_fee_rate_percent: msg.post_only_reserve_fee_rate_percent.clone(),
        next_change_at: msg.next_change_at.as_option().cloned(),
        breakdown: msg.breakdown.as_option().map(spot_fee_breakdown),
    }
}

pub fn spot_fee_rates_list_from_proto(msg: &GetSpotFeeRatesResponse) -> SpotFeeRatesList {
    SpotFeeRatesList {
        fee_rates: msg.fee_rates.iter().map(spot_fee_rate_from_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_fee_rates_decode_rows() {
        let msg = GetSpotFeeRatesResponse {
            fee_rates: vec![ProtoSpotFeeRate {
                symbol_id: 7,
                maker_fee_rate_percent: "0.01".into(),
                taker_fee_rate_percent: "0.04".into(),
                vip_tier: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = spot_fee_rates_list_from_proto(&msg);
        assert_eq!(result.fee_rates.len(), 1);
        let row = &result.fee_rates[0];
        assert_eq!(row.symbol_id, 7);
        assert_eq!(row.symbol, "");
        assert_eq!(row.vip_tier, 2);
    }
}

#[cfg(test)]
mod reserve_contract_tests {
    use super::*;
    #[test]
    fn preserves_reserves_breakdown_and_proto_defaults() {
        let msg:ProtoSpotFeeRate=serde_json::from_str(r#"{"reserveFeeRatePercent":"0.09","postOnlyReserveFeeRatePercent":"0.08","nextChangeAt":"2026-11-06T00:00:00Z","breakdown":{"tierMakerFeeRatePercent":"-0.08","tierTakerFeeRatePercent":"0.135","marketMultiplier":{"makerMultiplierPercent":"90","takerMultiplierPercent":"20","effectiveFrom":"2026-11-01T00:00:00Z"},"referralDiscount":{"makerDiscountPercent":"5","takerDiscountPercent":"10","effectiveFrom":"2026-11-01T00:00:00Z"}}}"#).unwrap();
        let row = spot_fee_rate_from_proto(&msg);
        assert_eq!(row.reserve_fee_rate_percent, "0.09");
        assert_eq!(row.post_only_reserve_fee_rate_percent, "0.08");
        assert_eq!(row.next_change_at.as_ref().unwrap().seconds, 1793923200);
        let breakdown = row.breakdown.unwrap();
        assert_eq!(breakdown.tier_maker_fee_rate_percent, "-0.08");
        assert!(breakdown.account_discounts.is_empty());
        let multiplier = breakdown.market_multiplier.unwrap();
        assert_eq!(multiplier.maker_multiplier_percent, "90");
        assert!(multiplier.effective_to.is_none());
        let referral = breakdown.referral_discount.unwrap();
        assert_eq!(referral.taker_discount_percent, "10");
        assert!(!referral.all_markets);
        assert!(referral.effective_to.is_none());
        let empty = spot_fee_rate_from_proto(&ProtoSpotFeeRate::default());
        assert!(empty.breakdown.is_none());
        assert!(empty.next_change_at.is_none());
    }
}

fn spot_fee_breakdown(msg: &crate::proto::fees::v1::SpotFeeRateBreakdown) -> SpotFeeRateBreakdown {
    SpotFeeRateBreakdown {
        tier_maker_fee_rate_percent: msg.tier_maker_fee_rate_percent.clone(),
        tier_taker_fee_rate_percent: msg.tier_taker_fee_rate_percent.clone(),
        market_multiplier: msg
            .market_multiplier
            .as_option()
            .map(|item| SpotFeeMultiplier {
                maker_multiplier_percent: item.maker_multiplier_percent.clone(),
                taker_multiplier_percent: item.taker_multiplier_percent.clone(),
                effective_from: item.effective_from.as_option().cloned(),
                effective_to: item.effective_to.as_option().cloned(),
            }),
        account_discounts: msg
            .account_discounts
            .iter()
            .map(spot_fee_discount)
            .collect(),
        referral_discount: msg.referral_discount.as_option().map(spot_fee_discount),
    }
}
fn spot_fee_discount(msg: &crate::proto::fees::v1::SpotFeeDiscount) -> SpotFeeDiscount {
    SpotFeeDiscount {
        maker_discount_percent: msg.maker_discount_percent.clone(),
        taker_discount_percent: msg.taker_discount_percent.clone(),
        all_markets: msg.all_markets,
        effective_from: msg.effective_from.as_option().cloned(),
        effective_to: msg.effective_to.as_option().cloned(),
    }
}
