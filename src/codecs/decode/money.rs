//! Read-side money decoders (Go `codecs.DecodePriceTicks` / `DecodeQtyScaled` parity).

use crate::errors::{Error, Result};
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

/// Decode a quantity whose wire 0 is the proto default of an unset value:
/// 0 is `Ok(None)`, a positive value `Ok(Some(_))`.
///
/// `field` names the wire field, such as `UserTrade.qty_scaled`, in the
/// error.
///
/// # Errors
///
/// [`Error::ResponseContract`] when `scaled` is negative, which no
/// quantity can be, or when `scale` is above the protocol maximum.
pub fn decode_qty_scaled(
    scaled: i64,
    scale: Option<u32>,
    symbol: Option<String>,
    symbol_id: Option<u32>,
    field: &str,
) -> Result<Option<Quantity>> {
    if scaled == 0 {
        return Ok(None);
    }
    decode_qty_scaled_allow_zero(scaled, scale, symbol, symbol_id, field).map(Some)
}

/// Gives a decoded quantity the pair's catalog base scale when the wire did
/// not carry one. A quantity that already has a scale, or an unknown scale,
/// leaves it as it is.
pub(crate) fn attach_qty_scale(qty: &mut Option<Quantity>, scale: Option<u32>) {
    let (Some(scale), Some(current)) = (scale, qty.as_ref()) else {
        return;
    };
    if current.scale().is_some() {
        return;
    }
    if let Ok(scaled) = Quantity::from_scaled(
        current.as_scaled(),
        Some(scale),
        current.domain(),
        current.symbol().map(str::to_owned),
        current.symbol_id(),
    ) {
        *qty = Some(scaled);
    }
}

/// Decode a quantity whose wire 0 is a zero quantity, such as the
/// `cum_qty` of an order with no fill or the `leaves_qty` of a filled one.
///
/// `field` names the wire field, such as `Order.cum_qty_scaled`, in the
/// error.
///
/// # Errors
///
/// [`Error::ResponseContract`] when `scaled` is negative, which no
/// quantity can be, or when `scale` is above the protocol maximum.
pub fn decode_qty_scaled_allow_zero(
    scaled: i64,
    scale: Option<u32>,
    symbol: Option<String>,
    symbol_id: Option<u32>,
    field: &str,
) -> Result<Quantity> {
    if scaled < 0 {
        return Err(Error::response_contract(
            field,
            format!("negative quantity {scaled}"),
        ));
    }
    Quantity::from_scaled(scaled, scale, QuantityDomain::OrderBase, symbol, symbol_id)
        .map_err(|err| Error::response_contract(field, err.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn contract_error(err: Error) -> (String, String) {
        match err {
            Error::ResponseContract { context, message } => (context, message),
            other => panic!("want Error::ResponseContract, got {other:?}"),
        }
    }

    #[test]
    fn decode_qty_scaled_reads_zero_as_unset_and_refuses_negative() {
        assert_eq!(
            decode_qty_scaled(0, None, None, Some(3), "T.qty_scaled").unwrap(),
            None
        );
        let qty = decode_qty_scaled(5, Some(2), None, Some(3), "T.qty_scaled")
            .unwrap()
            .expect("positive quantity");
        assert_eq!(
            (qty.as_scaled(), qty.scale(), qty.symbol_id()),
            (5, Some(2), Some(3))
        );

        let err = decode_qty_scaled(-5, None, None, Some(3), "T.qty_scaled").unwrap_err();
        assert!(!err.is_retryable());
        assert_eq!(
            contract_error(err),
            ("T.qty_scaled".to_owned(), "negative quantity -5".to_owned())
        );
        let err = decode_qty_scaled(i64::MIN, None, None, None, "T.qty_scaled").unwrap_err();
        assert_eq!(
            contract_error(err).1,
            format!("negative quantity {}", i64::MIN)
        );
    }

    #[test]
    fn decode_qty_scaled_allow_zero_reads_zero_and_refuses_negative() {
        let zero =
            decode_qty_scaled_allow_zero(0, Some(8), None, Some(3), "T.cum_qty_scaled").unwrap();
        assert_eq!(
            (zero.as_scaled(), zero.scale(), zero.symbol_id()),
            (0, Some(8), Some(3))
        );

        let err =
            decode_qty_scaled_allow_zero(-1, None, None, Some(3), "T.cum_qty_scaled").unwrap_err();
        assert!(!err.is_retryable());
        assert_eq!(
            contract_error(err),
            (
                "T.cum_qty_scaled".to_owned(),
                "negative quantity -1".to_owned()
            )
        );
    }

    #[test]
    fn qty_decoders_refuse_an_out_of_range_scale() {
        let scale = Some(crate::codecs::scalars::MAX_PROTOCOL_SCALE + 1);
        let err = decode_qty_scaled(1, scale, None, None, "T.qty_scaled").unwrap_err();
        assert_eq!(contract_error(err).0, "T.qty_scaled");
        let err = decode_qty_scaled_allow_zero(0, scale, None, None, "T.qty_scaled").unwrap_err();
        assert_eq!(contract_error(err).0, "T.qty_scaled");
    }
}
