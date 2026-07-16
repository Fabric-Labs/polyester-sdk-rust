//! Strict decimal ↔ scaled-integer codecs (POLY-3262).

use crate::errors::{Error, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

pub const PRICE_TICK_SCALE: u32 = 6;
pub const INT64_MAX: i128 = i64::MAX as i128;
pub const INT64_MIN: i128 = i64::MIN as i128;
pub const UINT64_MAX: u128 = u64::MAX as u128;

/// Strict non-negative decimal: digits with optional fractional part.
fn decimal_string_from_input(raw: &str, field_name: &str) -> Result<String> {
    let text = raw.trim();
    if text.is_empty() || !is_strict_decimal(text) {
        return Err(Error::validation(format!(
            "{field_name} must be a valid decimal string"
        )));
    }
    Ok(text.to_owned())
}

fn decimal_string_from_decimal(raw: Decimal, field_name: &str) -> Result<String> {
    if raw.is_sign_negative() {
        return Err(Error::validation(format!(
            "{field_name} must be non-negative"
        )));
    }
    let text = format!("{raw}");
    // Normalize trailing zeros from Decimal display.
    let text = if let Some((h, t)) = text.split_once('.') {
        let t = t.trim_end_matches('0');
        if t.is_empty() {
            h.to_owned()
        } else {
            format!("{h}.{t}")
        }
    } else {
        text
    };
    decimal_string_from_input(&text, field_name)
}

fn is_strict_decimal(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_digit() {
        return false;
    }
    let mut saw_dot = false;
    for c in chars {
        if c == '.' {
            if saw_dot {
                return false;
            }
            saw_dot = true;
            continue;
        }
        if !c.is_ascii_digit() {
            return false;
        }
    }
    true
}

/// Strict decimal→scaled. Never rounds; excess fractional digits fail.
pub fn try_decimal_to_scaled(decimal: &str, scale: u32) -> std::result::Result<i128, &'static str> {
    let raw = decimal.trim();
    if !is_strict_decimal(raw) {
        return Err("invalid");
    }
    let (int_part, frac_part) = match raw.split_once('.') {
        Some((i, f)) => (i, f),
        None => (raw, ""),
    };
    if frac_part.len() as u32 > scale {
        return Err("precision");
    }
    let mut digits = String::with_capacity(int_part.len() + scale as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let pad = scale as usize - frac_part.len();
    digits.extend(std::iter::repeat_n('0', pad));
    if digits.is_empty() {
        digits.push('0');
    }
    digits.parse::<i128>().map_err(|_| "invalid")
}

pub fn decimal_to_scaled_str(raw: &str, scale: u32, field_name: &str) -> Result<i128> {
    let text = decimal_string_from_input(raw, field_name)?;
    match try_decimal_to_scaled(&text, scale) {
        Ok(v) => Ok(v),
        Err("precision") => Err(Error::validation(format!(
            "{field_name} supports at most {scale} decimal places: {text}"
        ))),
        Err(_) => Err(Error::validation(format!(
            "{field_name} must be a valid decimal string"
        ))),
    }
}

pub fn decimal_to_scaled(raw: Decimal, scale: u32, field_name: &str) -> Result<i128> {
    let text = decimal_string_from_decimal(raw, field_name)?;
    decimal_to_scaled_str(&text, scale, field_name)
}

pub fn parse_price_ticks_str(raw: &str, field_name: &str) -> Result<i64> {
    let scaled = decimal_to_scaled_str(raw, PRICE_TICK_SCALE, field_name)?;
    if scaled < 0 {
        return Err(Error::validation(format!(
            "{field_name} must be non-negative"
        )));
    }
    if scaled > INT64_MAX {
        return Err(Error::validation(format!(
            "{field_name} exceeds int64 range"
        )));
    }
    Ok(scaled as i64)
}

pub fn parse_price_ticks(raw: Decimal, field_name: &str) -> Result<i64> {
    let scaled = decimal_to_scaled(raw, PRICE_TICK_SCALE, field_name)?;
    if scaled > INT64_MAX {
        return Err(Error::validation(format!(
            "{field_name} exceeds int64 range"
        )));
    }
    Ok(scaled as i64)
}

pub fn format_price_ticks(ticks: i64) -> String {
    format_scaled(ticks as i128, PRICE_TICK_SCALE)
}

pub fn parse_qty_scaled_str(raw: &str, scale: u32, field_name: &str) -> Result<i64> {
    let scaled = decimal_to_scaled_str(raw, scale, field_name)?;
    if scaled <= 0 {
        return Err(Error::validation(format!("{field_name} must be positive")));
    }
    if scale != 18 && scaled > INT64_MAX {
        return Err(Error::validation(format!(
            "{field_name} exceeds int64 range"
        )));
    }
    if scaled > INT64_MAX {
        return Err(Error::validation(format!(
            "{field_name} exceeds int64 range"
        )));
    }
    Ok(scaled as i64)
}

pub fn parse_qty_scaled(raw: Decimal, scale: u32, field_name: &str) -> Result<i64> {
    let text = decimal_string_from_decimal(raw, field_name)?;
    parse_qty_scaled_str(&text, scale, field_name)
}

pub fn format_qty_scaled(qty_scaled: i64, scale: u32) -> String {
    format_scaled(qty_scaled as i128, scale)
}

fn format_scaled(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let digits = value.abs().to_string();
    let width = scale as usize + 1;
    let padded = format!("{digits:0>width$}");
    let (head, tail) = padded.split_at(padded.len() - scale as usize);
    let head = head.trim_start_matches('0');
    let head = if head.is_empty() { "0" } else { head };
    let tail = tail.trim_end_matches('0');
    let raw = if tail.is_empty() {
        head.to_owned()
    } else {
        format!("{head}.{tail}")
    };
    if neg { format!("-{raw}") } else { raw }
}

pub fn id_to_u64(value: &str, label: &str) -> Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Error::validation(format!(
            "{label} must be base58 or decimal uint64"
        )));
    }
    if value.chars().all(|c| c.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .map_err(|_| Error::validation(format!("{label} exceeds uint64 range")));
    }
    let bytes = bs58::decode(value)
        .into_vec()
        .map_err(|_| Error::validation(format!("{label} must be base58 or decimal uint64")))?;
    if bytes.len() > 8 {
        return Err(Error::validation(format!("{label} exceeds uint64 range")));
    }
    let mut buf = [0u8; 8];
    buf[8 - bytes.len()..].copy_from_slice(&bytes);
    Ok(u64::from_be_bytes(buf))
}

pub fn format_id(id: u64) -> String {
    if id == 0 {
        return bs58::encode([1u8]).into_string(); // match Go: 0→1
    }
    let bytes = id.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bs58::encode(&bytes[start..]).into_string()
}

/// Format a uint64 id as base58, or `"0"` when zero (order/trade ids).
pub fn format_uint64_id(id: u64) -> String {
    if id == 0 {
        "0".to_owned()
    } else {
        format_id(id)
    }
}

pub fn parse_decimal_input(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| Error::validation("invalid decimal".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_ticks_round_trip() {
        let ticks = parse_price_ticks_str("1.5", "price").unwrap();
        assert_eq!(ticks, 1_500_000);
        assert_eq!(format_price_ticks(ticks), "1.5");
    }

    #[test]
    fn reject_excess_precision() {
        let err = parse_price_ticks_str("1.1234567", "price").unwrap_err();
        assert!(err.to_string().contains("at most 6"));
    }

    #[test]
    fn qty_positive() {
        assert!(parse_qty_scaled_str("0", 8, "qty").is_err());
        assert_eq!(parse_qty_scaled_str("0.00000001", 8, "qty").unwrap(), 1);
    }

    #[test]
    fn qty_rejects_excess_precision() {
        let err = parse_qty_scaled_str("1.123456789", 8, "qty").unwrap_err();
        assert!(err.to_string().contains("at most") || err.to_string().contains("precision"));
    }

    #[test]
    fn price_rejects_negative_string() {
        assert!(parse_price_ticks_str("-1", "price").is_err());
    }

    #[test]
    fn format_qty_scaled_round_trip() {
        assert_eq!(format_qty_scaled(1_000_000, 8), "0.01");
    }
}
