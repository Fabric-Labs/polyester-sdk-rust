//! Offline integration checks for the public money surface (no live API).

use polyester::Error;
use polyester::types::{AssetAmount, Price, Quantity, QuantityDomain, resolve_asset_amount_scaled};

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

#[test]
fn asset_amount_dual_constructors_and_domain_safety() {
    let decimal =
        AssetAmount::from_decimal_str("0.5", 18, QuantityDomain::LedgerE18, Some(7)).unwrap();
    let scaled = AssetAmount::from_scaled(
        500_000_000_000_000_000,
        Some(18),
        QuantityDomain::LedgerE18,
        Some(7),
    )
    .unwrap();
    assert_eq!(decimal.as_scaled(), scaled.as_scaled());
    assert!(resolve_asset_amount_scaled(&decimal, 18, QuantityDomain::Asset, Some(7)).is_err());
}

#[test]
fn asset_amount_rejects_protocol_scale_above_max() {
    let err = AssetAmount::from_scaled(1, Some(65535), QuantityDomain::LedgerE18, None)
        .expect_err("scale above MAX_PROTOCOL_SCALE");
    assert!(
        err.to_string().to_ascii_lowercase().contains("scale"),
        "{err}"
    );
}
