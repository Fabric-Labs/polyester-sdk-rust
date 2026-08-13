//! Effective spot fee-rate models.

/// Effective maker/taker rates for one spot market and account target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotFeeRate {
    pub symbol_id: u32,
    pub symbol: String,
    pub maker_fee_rate_percent: String,
    pub taker_fee_rate_percent: String,
    pub vip_tier: u32,
}

/// Effective spot fee rows ordered by numeric market identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotFeeRatesList {
    pub fee_rates: Vec<SpotFeeRate>,
}
