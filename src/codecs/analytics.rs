//! Chain analytics helpers (Go `codecs/analytics.go` parity).

use crate::errors::{Error, Result};
use crate::proto::chain::analytics::v1::ChainAnalyticsRange;

pub fn resolve_analytics_range(range_key: &str) -> Result<ChainAnalyticsRange> {
    match range_key.trim().to_ascii_lowercase().as_str() {
        "1d" => Ok(ChainAnalyticsRange::Day1),
        "7d" => Ok(ChainAnalyticsRange::Day7),
        "30d" => Ok(ChainAnalyticsRange::Day30),
        "90d" => Ok(ChainAnalyticsRange::Day90),
        "180d" => Ok(ChainAnalyticsRange::Day180),
        "365d" => Ok(ChainAnalyticsRange::Day365),
        _ => Err(Error::validation(
            "range must be one of '1d', '7d', '30d', '90d', '180d', or '365d'",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_known_ranges() {
        assert_eq!(
            resolve_analytics_range("7d").unwrap(),
            ChainAnalyticsRange::Day7
        );
        assert_eq!(
            resolve_analytics_range("365D").unwrap(),
            ChainAnalyticsRange::Day365
        );
    }

    #[test]
    fn rejects_unknown_range() {
        assert!(resolve_analytics_range("2d").is_err());
    }
}
