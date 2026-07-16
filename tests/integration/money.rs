//! Offline integration checks for the public money surface (no live API).

use polyester::Error;
use polyester::types::{Price, Quantity, QuantityDomain};

#[test]
fn price_quantity_dual_constructors() {
    let p = Price::from_decimal_str("1.5", Some("BTC-USDT".into())).unwrap();
    assert_eq!(p.as_ticks(), 1_500_000);
    assert_eq!(
        Price::from_ticks(1_500_000, None).unwrap().as_ticks(),
        1_500_000
    );

    let q = Quantity::from_decimal_str("0.01", 8, Some("BTC-USDT".into()), None).unwrap();
    assert_eq!(q.as_scaled(), 1_000_000);
    let q2 =
        Quantity::from_scaled(1_000_000, Some(8), QuantityDomain::OrderBase, None, None).unwrap();
    assert_eq!(q2.as_scaled(), 1_000_000);
}

#[test]
fn money_rejects_excess_precision() {
    let err = Price::from_decimal_str("1.1234567", None).unwrap_err();
    assert!(matches!(err, Error::Validation(_)));
    assert!(err.to_string().contains("at most 6"));

    let err = Quantity::from_decimal_str("1.123456789", 8, None, None).unwrap_err();
    assert!(matches!(err, Error::Validation(_)));
}

#[test]
fn money_symbol_mismatch() {
    let p = Price::from_ticks(1, Some("BTC-USDT".into())).unwrap();
    assert!(p.compatible_with(Some("ETH-USDT")).is_err());
    assert!(p.compatible_with(Some("BTC-USDT")).is_ok());
}
