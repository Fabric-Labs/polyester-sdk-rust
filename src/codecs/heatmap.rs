//! Heatmap request helpers (Go `codecs/heatmap.go` parity).

use buffa::Enumeration;

use crate::codecs::decode::covering_snapshot_depth;
use crate::errors::{Error, Result};
use crate::proto::marketdata::v1::{HeatmapDepth, HeatmapInterval, HeatmapQuantityMode};

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

/// The heatmap depth requested for `depth`, by the rule of
/// [`crate::codecs::decode::orderbook_snapshot_depth`]: `depth` itself when it is
/// a supported depth (1, 5, 10, 20, 50, 100, 200, 500, 1000), else the smallest
/// one covering it. 0 is the default 50. A depth above 1000 is a validation error.
pub fn heatmap_depth_for_levels(depth: u32) -> Result<HeatmapDepth> {
    Ok(match covering_snapshot_depth(depth, "heatmap depth")? {
        1 => HeatmapDepth::DEPTH_1,
        5 => HeatmapDepth::DEPTH_5,
        10 => HeatmapDepth::DEPTH_10,
        20 => HeatmapDepth::DEPTH_20,
        50 => HeatmapDepth::DEPTH_50,
        100 => HeatmapDepth::DEPTH_100,
        200 => HeatmapDepth::DEPTH_200,
        500 => HeatmapDepth::DEPTH_500,
        _ => HeatmapDepth::DEPTH_1000,
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
        assert_eq!(heatmap_depth_for_levels(1).unwrap(), HeatmapDepth::Depth1);
        assert_eq!(heatmap_depth_for_levels(5).unwrap(), HeatmapDepth::Depth5);
        assert_eq!(heatmap_depth_for_levels(50).unwrap(), HeatmapDepth::Depth50);
        assert_eq!(
            heatmap_depth_for_levels(200).unwrap(),
            HeatmapDepth::Depth200
        );
        assert_eq!(
            heatmap_depth_for_levels(1000).unwrap(),
            HeatmapDepth::Depth1000
        );
        assert!(heatmap_depth_for_levels(1001).is_err());
    }
}
