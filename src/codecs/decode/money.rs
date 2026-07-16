//! Read-side money decoders (Go `codecs.DecodePriceTicks` / `DecodeQtyScaled` parity).

use crate::types::{Price, Quantity, QuantityDomain};

pub fn decode_price_ticks(ticks: i64, symbol: Option<String>) -> Option<Price> {
    if ticks == 0 {
        return None;
    }
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
    Quantity::from_scaled(
        scaled,
        scale,
        QuantityDomain::OrderBase,
        symbol,
        symbol_id,
    )
    .ok()
}
