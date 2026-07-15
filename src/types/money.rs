//! Money scalar types for write/read surfaces (POLY-3262).

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
    Asset,
    LedgerE18,
}

/// Distinct newtype for protocol price ticks (compile-time mix-up prevention).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PriceTicks(i64);

impl PriceTicks {
    pub const fn new(ticks: i64) -> Self {
        Self(ticks)
    }
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Distinct newtype for order/trigger qty_scaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QtyScaled(i64);

impl QtyScaled {
    pub const fn new(scaled: i64) -> Self {
        Self(scaled)
    }
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Resolved protocol price units (protobuf `price_ticks`, fixed 1e6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Price {
    pub ticks: PriceTicks,
    pub symbol: Option<String>,
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

    pub fn as_decimal(&self) -> Decimal {
        format_price_ticks(self.ticks.get())
            .parse()
            .unwrap_or_default()
    }

    pub fn format(&self) -> String {
        format_price_ticks(self.ticks.get())
    }

    pub fn compatible_with(&self, symbol: Option<&str>) -> Result<()> {
        if let (Some(a), Some(b)) = (self.symbol.as_deref(), symbol) {
            if a != b {
                return Err(Error::validation(format!(
                    "price symbol mismatch: value is for {a}, destination is {b}"
                )));
            }
        }
        Ok(())
    }
}

/// Resolved order/trigger base quantity (protobuf `qty_scaled`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantity {
    pub scaled: QtyScaled,
    pub scale: Option<u32>,
    pub domain: QuantityDomain,
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
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
        if let Some(s) = scale {
            if (s as i32) < 0 {
                return Err(Error::validation("scale must be non-negative"));
            }
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

    pub fn as_scaled(&self) -> i64 {
        self.scaled.get()
    }

    pub fn format(&self, scale: Option<u32>) -> Result<String> {
        let resolved = scale.or(self.scale).ok_or_else(|| {
            Error::validation("format requires a known scale; pass scale= or construct with scale=")
        })?;
        Ok(format_qty_scaled(self.scaled.get(), resolved))
    }

    pub fn compatible_with(
        &self,
        domain: QuantityDomain,
        scale: Option<u32>,
        symbol: Option<&str>,
        symbol_id: Option<u32>,
    ) -> Result<()> {
        if self.domain != domain {
            return Err(Error::validation(format!(
                "quantity domain mismatch: value is {:?}, destination is {domain:?}",
                self.domain
            )));
        }
        if let (Some(a), Some(b)) = (self.scale, scale) {
            if a != b {
                return Err(Error::validation(format!(
                    "quantity scale mismatch: value scale is {a}, destination is {b}"
                )));
            }
        }
        if let (Some(a), Some(b)) = (self.symbol.as_deref(), symbol) {
            if a != b {
                return Err(Error::validation(format!(
                    "quantity symbol mismatch: value is for {a}, destination is {b}"
                )));
            }
        }
        if let (Some(a), Some(b)) = (self.symbol_id, symbol_id) {
            if a != b {
                return Err(Error::validation(format!(
                    "quantity symbol_id mismatch: value is for {a}, destination is {b}"
                )));
            }
        }
        Ok(())
    }
}

/// Resolved asset/ledger amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetAmount {
    pub scaled: i128,
    pub scale: Option<u32>,
    pub domain: QuantityDomain,
    pub asset_id: Option<u32>,
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

    pub fn as_i64(&self) -> Result<i64> {
        if self.scaled > i64::MAX as i128 {
            return Err(Error::validation("amount exceeds int64 range"));
        }
        Ok(self.scaled as i64)
    }
}

/// Resolve price for write paths: `Price` or decimal string.
pub fn resolve_price_ticks(value: &Price, symbol: Option<&str>) -> Result<i64> {
    value.compatible_with(symbol)?;
    Ok(value.as_ticks())
}

/// Resolve qty for write paths.
pub fn resolve_qty_scaled(
    value: &Quantity,
    scale: u32,
    symbol: Option<&str>,
    symbol_id: Option<u32>,
) -> Result<i64> {
    value.compatible_with(QuantityDomain::OrderBase, Some(scale), symbol, symbol_id)?;
    Ok(value.as_scaled())
}
