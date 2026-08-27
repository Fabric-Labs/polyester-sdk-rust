//! Decode fees.v1 responses into public SDK models.

use crate::models::{SpotFeeRate, SpotFeeRatesList};
use crate::proto::fees::v1::{GetSpotFeeRatesResponse, SpotFeeRate as ProtoSpotFeeRate};

pub fn spot_fee_rate_from_proto(msg: &ProtoSpotFeeRate) -> SpotFeeRate {
    SpotFeeRate {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
        maker_fee_rate_percent: msg.maker_fee_rate_percent.clone(),
        taker_fee_rate_percent: msg.taker_fee_rate_percent.clone(),
        vip_tier: msg.vip_tier,
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
