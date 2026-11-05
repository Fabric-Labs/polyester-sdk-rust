//! Effective and reserve spot fee-rate models.

use buffa_types::google::protobuf::Timestamp;

/// Effective fill rates and the rates used to reserve quote fees.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotFeeRate {
    pub symbol_id: u32,
    pub symbol: String,
    pub maker_fee_rate_percent: String,
    pub taker_fee_rate_percent: String,
    pub vip_tier: u32,
    pub reserve_fee_rate_percent: String,
    pub post_only_reserve_fee_rate_percent: String,
    pub next_change_at: Option<Timestamp>,
    pub breakdown: Option<SpotFeeRateBreakdown>,
}

/// Components of the effective fee rates.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotFeeRateBreakdown {
    pub tier_maker_fee_rate_percent: String,
    pub tier_taker_fee_rate_percent: String,
    pub market_multiplier: Option<SpotFeeMultiplier>,
    pub account_discounts: Vec<SpotFeeDiscount>,
    pub referral_discount: Option<SpotFeeDiscount>,
}

/// Market fee schedule in force over a UTC interval.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotFeeMultiplier {
    pub maker_multiplier_percent: String,
    pub taker_multiplier_percent: String,
    pub effective_from: Option<Timestamp>,
    pub effective_to: Option<Timestamp>,
}

/// Account or referral discount in force over a UTC interval.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotFeeDiscount {
    pub maker_discount_percent: String,
    pub taker_discount_percent: String,
    pub all_markets: bool,
    pub effective_from: Option<Timestamp>,
    pub effective_to: Option<Timestamp>,
}

/// Effective spot fee rows ordered by numeric market identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotFeeRatesList {
    pub fee_rates: Vec<SpotFeeRate>,
}
