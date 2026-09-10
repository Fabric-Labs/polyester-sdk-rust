//! Live coverage for currency-conversion config and rates.

use crate::support::{call_optional, require_live_client};

const USD_E8: i64 = 100_000_000;

#[tokio::test]
async fn currency_conversion_config() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(result) = call_optional("market_overview.get_currency_conversion_config", || {
        client.market_overview.get_currency_conversion_config()
    })
    .await
    else {
        return;
    };
    for item in &result.fiat {
        assert!(!item.code.is_empty(), "fiat missing code: {item:?}");
    }
    for item in &result.stablecoins {
        assert!(!item.code.is_empty(), "stablecoin missing code: {item:?}");
    }
}

#[tokio::test]
async fn currency_conversion_rates() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(result) = call_optional("market_overview.get_currency_conversion_rates", || {
        client.market_overview.get_currency_conversion_rates()
    })
    .await
    else {
        return;
    };
    if let Some(fiat) = &result.fiat {
        for rate in &fiat.rates {
            assert!(!rate.code.is_empty(), "fiat rate missing code: {rate:?}");
            assert_ne!(rate.units_per_usd_e8, 0, "fiat rate is zero: {rate:?}");
            if rate.code == "USD" {
                assert_eq!(rate.units_per_usd_e8, USD_E8);
            }
        }
    }
    for rate in &result.stablecoins {
        assert!(!rate.code.is_empty(), "stablecoin missing code: {rate:?}");
        assert_ne!(rate.usd_per_unit_e8, 0, "stablecoin rate is zero: {rate:?}");
    }
}
