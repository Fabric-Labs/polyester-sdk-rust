//! Money scalar types for write/read surfaces.

use crate::codecs::scalars::{
    format_price_ticks, format_qty_scaled, parse_price_ticks, parse_price_ticks_str,
    parse_qty_scaled, parse_qty_scaled_str,
};
use crate::errors::{Error, Result};
use rust_decimal::Decimal;

/// Quantity domain — mixing domains is a validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum QuantityDomain {
    #[default]
    OrderBase,
    OrderQuote,
    Asset,
    LedgerE18,
}

/// Distinct newtype for protocol price ticks (compile-time mix-up prevention).
///
/// Construction is crate-private so invalid negative ticks cannot bypass
/// [`Price::from_ticks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PriceTicks(i64);

impl PriceTicks {
    pub(crate) const fn new(ticks: i64) -> Self {
        Self(ticks)
    }
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Distinct newtype for order/trigger qty_scaled.
///
/// Construction is crate-private so invalid negative values cannot bypass
/// [`Quantity::from_scaled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QtyScaled(i64);

impl QtyScaled {
    pub(crate) const fn new(scaled: i64) -> Self {
        Self(scaled)
    }
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Resolved protocol price units (protobuf `price_ticks`, fixed 1e6).
///
/// Fields are private so metadata cannot be changed independently of the
/// validated ticks. Use [`Price::symbol`] to inspect the immutable metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Price {
    ticks: PriceTicks,
    symbol: Option<String>,
}

impl Price {
    pub fn from_ticks(ticks: i64, symbol: Option<String>) -> Result<Self> {
        if ticks < 0 {
            return Err(Error::validation("ticks must be non-negative"));
        }
        Ok(Self {
            ticks: PriceTicks::new(ticks),
            symbol,
        })
    }

    pub fn from_decimal_str(raw: &str, symbol: Option<String>) -> Result<Self> {
        let ticks = parse_price_ticks_str(raw, "price")?;
        Self::from_ticks(ticks, symbol)
    }

    pub fn from_decimal(raw: Decimal, symbol: Option<String>) -> Result<Self> {
        let ticks = parse_price_ticks(raw, "price")?;
        Self::from_ticks(ticks, symbol)
    }

    pub fn as_ticks(&self) -> i64 {
        self.ticks.get()
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    pub fn as_decimal(&self) -> Decimal {
        // Price ticks always use the protocol's fixed 1e6 scale, so this
        // conversion is exact and cannot silently substitute Decimal::ZERO.
        Decimal::new(self.ticks.get(), 6)
    }

    pub fn format(&self) -> String {
        format_price_ticks(self.ticks.get())
    }

    pub fn compatible_with(&self, symbol: Option<&str>) -> Result<()> {
        if let (Some(a), Some(b)) = (self.symbol(), symbol)
            && a != b
        {
            return Err(Error::validation(format!(
                "price symbol mismatch: value is for {a}, destination is {b}"
            )));
        }
        Ok(())
    }
}

/// Resolved order/trigger base quantity (protobuf `qty_scaled`).
///
/// Fields are private so metadata cannot be changed independently of the
/// validated scaled value. Use the immutable metadata getters to inspect it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantity {
    scaled: QtyScaled,
    scale: Option<u32>,
    domain: QuantityDomain,
    symbol: Option<String>,
    symbol_id: Option<u32>,
}

impl Quantity {
    pub fn from_scaled(
        scaled: i64,
        scale: Option<u32>,
        domain: QuantityDomain,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        if scaled < 0 {
            return Err(Error::validation("scaled must be non-negative"));
        }
        if !matches!(
            domain,
            QuantityDomain::OrderBase | QuantityDomain::OrderQuote
        ) {
            return Err(Error::validation(
                "Quantity domain must be order_base or order_quote",
            ));
        }
        if let Some(scale) = scale {
            crate::codecs::scalars::validate_protocol_scale(scale)?;
        }
        Ok(Self {
            scaled: QtyScaled::new(scaled),
            scale,
            domain,
            symbol,
            symbol_id,
        })
    }

    pub fn from_decimal_str(
        raw: &str,
        scale: u32,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        let scaled = parse_qty_scaled_str(raw, scale, "qty")?;
        Self::from_scaled(
            scaled,
            Some(scale),
            QuantityDomain::OrderBase,
            symbol,
            symbol_id,
        )
    }

    pub fn from_decimal(
        raw: Decimal,
        scale: u32,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        let scaled = parse_qty_scaled(raw, scale, "qty")?;
        Self::from_scaled(
            scaled,
            Some(scale),
            QuantityDomain::OrderBase,
            symbol,
            symbol_id,
        )
    }

    /// Construct a quote-debit budget from scaled integer units.
    ///
    /// `scale` is required so a bare integer can never silently inherit the
    /// catalog scale. Use [`crate::catalogs::Manager::quote_quantity_scale_for_symbol`].
    pub fn from_quote_scaled(
        scaled: i64,
        scale: u32,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        Self::from_scaled(
            scaled,
            Some(scale),
            QuantityDomain::OrderQuote,
            symbol,
            symbol_id,
        )
    }

    pub fn from_quote_decimal_str(
        raw: &str,
        scale: u32,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        let scaled = parse_qty_scaled_str(raw, scale, "quote amount")?;
        Self::from_quote_scaled(scaled, scale, symbol, symbol_id)
    }

    pub fn from_quote_decimal(
        raw: Decimal,
        scale: u32,
        symbol: Option<String>,
        symbol_id: Option<u32>,
    ) -> Result<Self> {
        let scaled = parse_qty_scaled(raw, scale, "quote amount")?;
        Self::from_quote_scaled(scaled, scale, symbol, symbol_id)
    }

    pub fn as_scaled(&self) -> i64 {
        self.scaled.get()
    }

    pub fn scale(&self) -> Option<u32> {
        self.scale
    }

    pub fn domain(&self) -> QuantityDomain {
        self.domain
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    pub fn symbol_id(&self) -> Option<u32> {
        self.symbol_id
    }

    pub fn format(&self, scale: Option<u32>) -> Result<String> {
        let resolved = scale.or(self.scale()).ok_or_else(|| {
            Error::validation("format requires a known scale; pass scale= or construct with scale=")
        })?;
        format_qty_scaled(self.scaled.get(), resolved)
    }

    pub fn compatible_with(
        &self,
        domain: QuantityDomain,
        scale: Option<u32>,
        symbol: Option<&str>,
        symbol_id: Option<u32>,
    ) -> Result<()> {
        if self.domain() != domain {
            return Err(Error::validation(format!(
                "quantity domain mismatch: value is {:?}, destination is {domain:?}",
                self.domain()
            )));
        }
        if let (Some(a), Some(b)) = (self.scale(), scale)
            && a != b
        {
            return Err(Error::validation(format!(
                "quantity scale mismatch: value scale is {a}, destination is {b}"
            )));
        }
        if let (Some(a), Some(b)) = (self.symbol(), symbol)
            && a != b
        {
            return Err(Error::validation(format!(
                "quantity symbol mismatch: value is for {a}, destination is {b}"
            )));
        }
        if let (Some(a), Some(b)) = (self.symbol_id(), symbol_id)
            && a != b
        {
            return Err(Error::validation(format!(
                "quantity symbol_id mismatch: value is for {a}, destination is {b}"
            )));
        }
        Ok(())
    }
}

/// Resolved asset/ledger amount.
///
/// Fields are private so invariants from [`AssetAmount::from_scaled`] cannot be
/// bypassed via struct literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetAmount {
    scaled: i128,
    scale: Option<u32>,
    domain: QuantityDomain,
    asset_id: Option<u32>,
}

impl AssetAmount {
    pub fn from_scaled(
        scaled: i128,
        scale: Option<u32>,
        domain: QuantityDomain,
        asset_id: Option<u32>,
    ) -> Result<Self> {
        if scaled < 0 {
            return Err(Error::validation("scaled must be non-negative"));
        }
        if !matches!(domain, QuantityDomain::Asset | QuantityDomain::LedgerE18) {
            return Err(Error::validation(
                "AssetAmount domain must be asset or ledger_e18",
            ));
        }
        if let Some(scale) = scale {
            crate::codecs::scalars::validate_protocol_scale(scale)?;
        }
        if domain != QuantityDomain::LedgerE18 && scaled > crate::codecs::scalars::INT64_MAX {
            return Err(Error::validation("scaled exceeds int64 range"));
        }
        Ok(Self {
            scaled,
            scale,
            domain,
            asset_id,
        })
    }

    pub fn from_decimal_str(
        raw: &str,
        scale: u32,
        domain: QuantityDomain,
        asset_id: Option<u32>,
    ) -> Result<Self> {
        use crate::codecs::scalars::decimal_to_scaled_str;
        let scaled = decimal_to_scaled_str(raw, scale, "amount")?;
        Self::from_scaled(scaled, Some(scale), domain, asset_id)
    }

    pub fn from_decimal(
        raw: Decimal,
        scale: u32,
        domain: QuantityDomain,
        asset_id: Option<u32>,
    ) -> Result<Self> {
        use crate::codecs::scalars::decimal_to_scaled;
        let scaled = decimal_to_scaled(raw, scale, "amount")?;
        Self::from_scaled(scaled, Some(scale), domain, asset_id)
    }

    pub fn as_i64(&self) -> Result<i64> {
        i64::try_from(self.scaled).map_err(|_| Error::validation("amount exceeds int64 range"))
    }

    pub fn as_scaled(&self) -> i128 {
        self.scaled
    }

    pub fn scale(&self) -> Option<u32> {
        self.scale
    }

    pub fn domain(&self) -> QuantityDomain {
        self.domain
    }

    pub fn asset_id(&self) -> Option<u32> {
        self.asset_id
    }

    pub fn compatible_with(
        &self,
        domain: QuantityDomain,
        scale: Option<u32>,
        asset_id: Option<u32>,
    ) -> Result<()> {
        if self.domain != domain {
            return Err(Error::validation(format!(
                "amount domain mismatch: value is {:?}, destination is {domain:?}",
                self.domain
            )));
        }
        if let (Some(a), Some(b)) = (self.scale, scale)
            && a != b
        {
            return Err(Error::validation(format!(
                "amount scale mismatch: value scale is {a}, destination is {b}"
            )));
        }
        if let (Some(a), Some(b)) = (self.asset_id, asset_id)
            && a != b
        {
            return Err(Error::validation(format!(
                "amount asset_id mismatch: value is for {a}, destination is {b}"
            )));
        }
        Ok(())
    }
}

/// Resolve price for write paths.
pub fn resolve_price_ticks(value: &Price, symbol: Option<&str>) -> Result<i64> {
    value.compatible_with(symbol)?;
    let ticks = value.as_ticks();
    if ticks < 0 {
        return Err(Error::validation("ticks must be non-negative"));
    }
    Ok(ticks)
}

/// Resolve qty for write paths. Requires a positive scaled value.
pub fn resolve_qty_scaled(
    value: &Quantity,
    scale: u32,
    symbol: Option<&str>,
    symbol_id: Option<u32>,
) -> Result<i64> {
    value.compatible_with(QuantityDomain::OrderBase, Some(scale), symbol, symbol_id)?;
    let scaled = value.as_scaled();
    if scaled <= 0 {
        return Err(Error::validation("qty must be positive"));
    }
    Ok(scaled)
}

/// Resolve a quote-debit budget. The value must carry the catalog quote scale.
pub fn resolve_quote_qty_scaled(
    value: &Quantity,
    scale: u32,
    symbol: Option<&str>,
    symbol_id: Option<u32>,
) -> Result<i64> {
    if value.scale().is_none() {
        return Err(Error::validation(
            "quote amount scale is required; use Quantity::from_quote_scaled/from_quote_decimal",
        ));
    }
    value.compatible_with(QuantityDomain::OrderQuote, Some(scale), symbol, symbol_id)?;
    let scaled = value.as_scaled();
    if scaled <= 0 {
        return Err(Error::validation("quote amount must be positive"));
    }
    Ok(scaled)
}

/// Resolve asset/ledger amount for transfer/withdraw write paths.
pub fn resolve_asset_amount_scaled(
    value: &AssetAmount,
    scale: u32,
    domain: QuantityDomain,
    asset_id: Option<u32>,
) -> Result<i128> {
    resolve_asset_amount_scaled_with_input_scale(value, None, scale, domain, asset_id)
}

/// Resolve an asset amount to `target_scale`, using `input_scale` only when the
/// value does not already carry a scale.
pub(crate) fn resolve_asset_amount_scaled_with_input_scale(
    value: &AssetAmount,
    input_scale: Option<u32>,
    target_scale: u32,
    domain: QuantityDomain,
    asset_id: Option<u32>,
) -> Result<i128> {
    crate::codecs::scalars::validate_protocol_scale(target_scale)?;
    if let Some(scale) = input_scale {
        crate::codecs::scalars::validate_protocol_scale(scale)?;
    }
    value.compatible_with(domain, None, asset_id)?;
    if let (Some(value_scale), Some(input_scale)) = (value.scale, input_scale)
        && value_scale != input_scale
    {
        return Err(Error::validation(format!(
            "amount scale mismatch: value scale is {value_scale}, input scale is {input_scale}"
        )));
    }
    if value.scaled <= 0 {
        return Err(Error::validation("amount must be positive"));
    }
    let source_scale = value.scale.or(input_scale).ok_or_else(|| {
        Error::validation(
            "amount scale is required; construct AssetAmount with an explicit scale or pass amount_scale/quantity_scale",
        )
    })?;
    let scaled = if source_scale < target_scale {
        let factor = 10_i128
            .checked_pow(target_scale - source_scale)
            .ok_or_else(|| Error::validation("amount scale conversion overflow"))?;
        value
            .scaled
            .checked_mul(factor)
            .ok_or_else(|| Error::validation("amount scale conversion overflow"))?
    } else if source_scale > target_scale {
        let divisor = 10_i128
            .checked_pow(source_scale - target_scale)
            .ok_or_else(|| Error::validation("amount scale conversion overflow"))?;
        if value.scaled % divisor != 0 {
            return Err(Error::validation(format!(
                "amount cannot be represented exactly at scale {target_scale}"
            )));
        }
        value.scaled / divisor
    } else {
        value.scaled
    };
    if domain != QuantityDomain::LedgerE18 && scaled > crate::codecs::scalars::INT64_MAX {
        return Err(Error::validation("amount exceeds int64 range"));
    }
    Ok(scaled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_from_ticks_rejects_negative() {
        assert!(Price::from_ticks(-1, None).is_err());
    }

    #[test]
    fn price_as_decimal_is_exact_at_the_maximum_tick_value() {
        let price = Price::from_ticks(i64::MAX, None).unwrap();
        assert_eq!(price.as_decimal(), Decimal::new(i64::MAX, 6));
    }

    #[test]
    fn quantity_from_scaled_rejects_negative() {
        assert!(Quantity::from_scaled(-1, Some(8), QuantityDomain::OrderBase, None, None).is_err());
    }

    #[test]
    fn resolve_paths_round_trip() {
        let price = Price::from_decimal_str("42.5", Some("BTC-USDT".into())).unwrap();
        assert_eq!(
            resolve_price_ticks(&price, Some("BTC-USDT")).unwrap(),
            42_500_000
        );
        let qty = Quantity::from_decimal_str("1.25", 8, Some("BTC-USDT".into()), Some(1)).unwrap();
        assert_eq!(
            resolve_qty_scaled(&qty, 8, Some("BTC-USDT"), Some(1)).unwrap(),
            125_000_000
        );
    }

    #[test]
    fn price_and_quantity_metadata_getters_preserve_compatibility() {
        let price = Price::from_ticks(42_500_000, Some("BTC-USDT".into())).unwrap();
        assert_eq!(price.symbol(), Some("BTC-USDT"));
        assert_eq!(price.as_ticks(), 42_500_000);
        assert_eq!(price.format(), "42.5");
        assert_eq!(price.clone(), price);
        assert!(format!("{price:?}").contains("BTC-USDT"));

        let qty = Quantity::from_scaled(
            125_000_000,
            Some(8),
            QuantityDomain::OrderBase,
            Some("BTC-USDT".into()),
            Some(7),
        )
        .unwrap();
        assert_eq!(qty.scale(), Some(8));
        assert_eq!(qty.domain(), QuantityDomain::OrderBase);
        assert_eq!(qty.symbol(), Some("BTC-USDT"));
        assert_eq!(qty.symbol_id(), Some(7));
        assert_eq!(qty.as_scaled(), 125_000_000);
        assert_eq!(qty.format(None).unwrap(), "1.25");
        assert_eq!(qty.clone(), qty);
        assert!(format!("{qty:?}").contains("BTC-USDT"));
        assert!(
            qty.compatible_with(
                QuantityDomain::OrderBase,
                Some(8),
                Some("BTC-USDT"),
                Some(7)
            )
            .is_ok()
        );
    }

    #[test]
    fn resolve_qty_rejects_zero() {
        let qty = Quantity::from_scaled(0, Some(8), QuantityDomain::OrderBase, None, None).unwrap();
        assert!(resolve_qty_scaled(&qty, 8, None, None).is_err());
    }

    #[test]
    fn quote_amount_requires_explicit_matching_scale() {
        let quote =
            Quantity::from_quote_decimal_str("12.5", 6, Some("BTC-USDT".into()), Some(1)).unwrap();
        assert_eq!(
            resolve_quote_qty_scaled(&quote, 6, Some("BTC-USDT"), Some(1)).unwrap(),
            12_500_000
        );
        assert!(resolve_quote_qty_scaled(&quote, 8, Some("BTC-USDT"), Some(1)).is_err());

        let missing_scale =
            Quantity::from_scaled(12_500_000, None, QuantityDomain::OrderQuote, None, None)
                .unwrap();
        assert!(resolve_quote_qty_scaled(&missing_scale, 6, None, None).is_err());
    }

    #[test]
    fn asset_amount_dual_path() {
        let from_dec =
            AssetAmount::from_decimal_str("0.5", 18, QuantityDomain::LedgerE18, Some(7)).unwrap();
        let from_scaled = AssetAmount::from_scaled(
            500_000_000_000_000_000,
            Some(18),
            QuantityDomain::LedgerE18,
            Some(7),
        )
        .unwrap();
        assert_eq!(
            resolve_asset_amount_scaled(&from_dec, 18, QuantityDomain::LedgerE18, Some(7)).unwrap(),
            resolve_asset_amount_scaled(&from_scaled, 18, QuantityDomain::LedgerE18, Some(7))
                .unwrap()
        );
    }

    #[test]
    fn asset_amount_rejects_domain_scale_and_asset_mismatch() {
        let amount =
            AssetAmount::from_scaled(100, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
        assert!(resolve_asset_amount_scaled(&amount, 18, QuantityDomain::Asset, Some(7)).is_err());
        assert!(
            resolve_asset_amount_scaled(&amount, 6, QuantityDomain::LedgerE18, Some(7)).is_err()
        );
        assert!(
            resolve_asset_amount_scaled(&amount, 18, QuantityDomain::LedgerE18, Some(8)).is_err()
        );
    }

    #[test]
    fn asset_amount_rescales_exactly_without_rounding() {
        let asset_precision =
            AssetAmount::from_scaled(125, Some(2), QuantityDomain::LedgerE18, Some(7)).unwrap();
        assert_eq!(
            resolve_asset_amount_scaled(&asset_precision, 18, QuantityDomain::LedgerE18, Some(7))
                .unwrap(),
            1_250_000_000_000_000_000
        );

        let exact_downscale = AssetAmount::from_scaled(
            1_250_000_000_000_000_000,
            Some(18),
            QuantityDomain::LedgerE18,
            Some(7),
        )
        .unwrap();
        assert_eq!(
            resolve_asset_amount_scaled(&exact_downscale, 2, QuantityDomain::LedgerE18, Some(7))
                .unwrap(),
            125
        );

        let inexact_downscale =
            AssetAmount::from_scaled(126, Some(3), QuantityDomain::LedgerE18, Some(7)).unwrap();
        assert!(
            resolve_asset_amount_scaled(&inexact_downscale, 2, QuantityDomain::LedgerE18, Some(7))
                .is_err()
        );
    }

    #[test]
    fn asset_amount_rescale_rejects_overflow() {
        let amount =
            AssetAmount::from_scaled(i128::MAX, Some(17), QuantityDomain::LedgerE18, None).unwrap();
        assert!(resolve_asset_amount_scaled(&amount, 18, QuantityDomain::LedgerE18, None).is_err());
    }

    #[test]
    fn quantity_reuse_rejects_scale_symbol_and_symbol_id_mismatch() {
        let qty = Quantity::from_scaled(
            100,
            Some(8),
            QuantityDomain::OrderBase,
            Some("BTC-USDT".into()),
            Some(7),
        )
        .unwrap();
        assert!(resolve_qty_scaled(&qty, 6, Some("BTC-USDT"), Some(7)).is_err());
        assert!(resolve_qty_scaled(&qty, 8, Some("ETH-USDT"), Some(7)).is_err());
        assert!(resolve_qty_scaled(&qty, 8, Some("BTC-USDT"), Some(8)).is_err());
    }

    #[test]
    fn asset_amount_requires_positive_value_at_resolve() {
        let amount =
            AssetAmount::from_scaled(0, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
        assert!(
            resolve_asset_amount_scaled(&amount, 18, QuantityDomain::LedgerE18, Some(7)).is_err()
        );
    }

    #[test]
    fn asset_amount_without_value_or_parameter_scale_fails_closed() {
        let amount = AssetAmount::from_scaled(1, None, QuantityDomain::LedgerE18, Some(7)).unwrap();
        let err = resolve_asset_amount_scaled(&amount, 18, QuantityDomain::LedgerE18, Some(7))
            .expect_err("missing source scale must not be treated as e18");
        assert!(err.to_string().contains("amount scale is required"));

        assert_eq!(
            resolve_asset_amount_scaled_with_input_scale(
                &amount,
                Some(6),
                18,
                QuantityDomain::LedgerE18,
                Some(7),
            )
            .unwrap(),
            1_000_000_000_000
        );
    }

    #[test]
    fn asset_amount_as_i64_rejects_overflow_not_truncate() {
        let amount = AssetAmount::from_scaled(
            i128::from(u64::MAX) + 1,
            Some(18),
            QuantityDomain::LedgerE18,
            None,
        )
        .unwrap();
        assert!(amount.as_i64().is_err());
    }
}
