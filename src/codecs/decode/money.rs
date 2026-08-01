//! Read-side money decoders (Go `codecs.DecodePriceTicks` / `DecodeQtyScaled` parity).

use crate::types::{AssetAmount, Price, Quantity, QuantityDomain};

pub fn decode_price_ticks(ticks: i64, symbol: Option<String>) -> Option<Price> {
    if ticks == 0 {
        return None;
    }
    Price::from_ticks(ticks, symbol).ok()
}

/// Decode price ticks where a present zero is meaningful.
pub fn decode_price_ticks_allow_zero(ticks: i64, symbol: Option<String>) -> Option<Price> {
    Price::from_ticks(ticks, symbol).ok()
}

pub fn decode_qty_scaled(
    scaled: i64,
    scale: Option<u32>,
    symbol: Option<String>,
    symbol_id: Option<u32>,
) -> Option<Quantity> {
    if scaled == 0 {
        return None;
    }
    Quantity::from_scaled(scaled, scale, QuantityDomain::OrderBase, symbol, symbol_id).ok()
}

/// Decode qty where zero is meaningful (e.g. fully filled `leaves_qty` / `cum_qty`).
///
/// Negative wire values are invalid and map to [`None`] (corruption), distinct from
/// a present zero quantity.
pub fn decode_qty_scaled_allow_zero(
    scaled: i64,
    scale: Option<u32>,
    symbol: Option<String>,
    symbol_id: Option<u32>,
) -> Option<Quantity> {
    if scaled < 0 {
        return None;
    }
    Quantity::from_scaled(scaled, scale, QuantityDomain::OrderBase, symbol, symbol_id).ok()
}

/// Decode protobuf U128 hi/lo into an [`AssetAmount`] (ledger/asset domains).
pub fn decode_asset_amount_u128(
    hi: u64,
    lo: u64,
    scale: Option<u32>,
    domain: QuantityDomain,
    asset_id: Option<u32>,
) -> Option<AssetAmount> {
    let value = (u128::from(hi) << 64) | u128::from(lo);
    let scaled = i128::try_from(value).ok()?;
    AssetAmount::from_scaled(scaled, scale, domain, asset_id).ok()
}
