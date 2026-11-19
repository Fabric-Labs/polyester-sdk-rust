//! Heatmap request helpers (Go `codecs/heatmap.go` parity).

use buffa::Enumeration;

use crate::errors::{Error, Result};
use crate::proto::marketdata::v1::{HeatmapInterval, HeatmapQuantityMode};
use crate::proto::polyester::r#type::v1::DepthTier;

/// The heatmap interval for `value`: an alias (`1s`, `1m`, `5m`, `1h`) or an
/// interval name such as `INTERVAL_1S`, in any case. Any other value, including
/// `INTERVAL_UNSPECIFIED` and a quantity mode, is a validation error.
pub fn resolve_heatmap_interval(value: &str) -> Result<HeatmapInterval> {
    let interval = match value {
        "1s" => Some(HeatmapInterval::INTERVAL_1S),
        "1m" => Some(HeatmapInterval::INTERVAL_1M),
        "5m" => Some(HeatmapInterval::INTERVAL_5M),
        "1h" => Some(HeatmapInterval::INTERVAL_1H),
        other => HeatmapInterval::from_proto_name(&other.to_ascii_uppercase()),
    };
    match interval {
        Some(interval) if interval != HeatmapInterval::INTERVAL_UNSPECIFIED => Ok(interval),
        _ => Err(Error::validation(format!(
            "unknown heatmap interval {value:?}; use 1s, 1m, 5m or 1h"
        ))),
    }
}

/// Interval segment of the live heatmap channel the server publishes `value`
/// on: `1s`, `1m`, `5m` or `1h`. Refuses what [`resolve_heatmap_interval`] refuses.
pub fn heatmap_interval_channel_name(value: &str) -> Result<&'static str> {
    Ok(match resolve_heatmap_interval(value)? {
        HeatmapInterval::INTERVAL_1S => "1s",
        HeatmapInterval::INTERVAL_1M => "1m",
        HeatmapInterval::INTERVAL_5M => "5m",
        HeatmapInterval::INTERVAL_1H => "1h",
        HeatmapInterval::INTERVAL_UNSPECIFIED => {
            unreachable!("refused by resolve_heatmap_interval")
        }
    })
}

/// Depths a heatmap can request, ascending: every depth tier but 5000, which
/// the heatmap refuses.
pub const HEATMAP_DEPTHS: [u32; 9] = [1, 5, 10, 20, 50, 100, 200, 500, 1000];

/// The heatmap depth requested for `depth`: `depth` itself when it is a
/// supported depth (1, 5, 10, 20, 50, 100, 200, 500, 1000), else the smallest
/// one covering it. 0 is the default 50. A depth above 1000 is a validation error.
pub fn heatmap_depth_for_levels(depth: u32) -> Result<DepthTier> {
    let depth = if depth == 0 { 50 } else { depth };
    let covering = HEATMAP_DEPTHS
        .into_iter()
        .find(|supported| *supported >= depth)
        .ok_or_else(|| {
            Error::validation(format!(
                "heatmap depth {depth} is above the deepest heatmap depth ({})",
                HEATMAP_DEPTHS[HEATMAP_DEPTHS.len() - 1]
            ))
        })?;
    Ok(match covering {
        1 => DepthTier::DEPTH_1,
        5 => DepthTier::DEPTH_5,
        10 => DepthTier::DEPTH_10,
        20 => DepthTier::DEPTH_20,
        50 => DepthTier::DEPTH_50,
        100 => DepthTier::DEPTH_100,
        200 => DepthTier::DEPTH_200,
        500 => DepthTier::DEPTH_500,
        _ => DepthTier::DEPTH_1000,
    })
}

pub fn resolve_heatmap_quantity_mode(value: &str) -> Result<HeatmapQuantityMode> {
    let lower = value.to_ascii_lowercase();
    let key = match lower.as_str() {
        "close" => "CLOSE",
        "peak" => "PEAK",
        _ => value,
    };
    HeatmapQuantityMode::from_proto_name(key)
        .or_else(|| HeatmapQuantityMode::from_proto_name(&key.to_ascii_uppercase()))
        .ok_or_else(|| Error::validation("quantity_mode must be 'close' or 'peak'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_aliases() {
        assert_eq!(
            resolve_heatmap_interval("1m").unwrap(),
            HeatmapInterval::Interval1m
        );
        assert_eq!(
            resolve_heatmap_interval("INTERVAL_1H").unwrap(),
            HeatmapInterval::Interval1h
        );
        assert_eq!(heatmap_interval_channel_name("INTERVAL_1S").unwrap(), "1s");
        assert!(resolve_heatmap_interval("INTERVAL_UNSPECIFIED").is_err());
        assert!(resolve_heatmap_interval("PEAK").is_err());
    }

    #[test]
    fn depth_buckets() {
        assert_eq!(heatmap_depth_for_levels(1).unwrap(), DepthTier::Depth1);
        assert_eq!(heatmap_depth_for_levels(5).unwrap(), DepthTier::Depth5);
        assert_eq!(heatmap_depth_for_levels(50).unwrap(), DepthTier::Depth50);
        assert_eq!(heatmap_depth_for_levels(200).unwrap(), DepthTier::Depth200);
        assert_eq!(
            heatmap_depth_for_levels(1000).unwrap(),
            DepthTier::Depth1000
        );
        assert!(heatmap_depth_for_levels(1001).is_err());
    }
}
