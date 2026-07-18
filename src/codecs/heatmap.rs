//! Heatmap request helpers (Go `codecs/heatmap.go` parity).

use buffa::Enumeration;

use crate::errors::{Error, Result};
use crate::proto::marketdata::v1::{HeatmapDepth, HeatmapInterval, HeatmapQuantityMode};

pub fn resolve_heatmap_interval(value: &str) -> Result<HeatmapInterval> {
    let key = heatmap_interval_channel_name(value);
    HeatmapInterval::from_proto_name(&key)
        .or_else(|| HeatmapInterval::from_proto_name(&key.to_ascii_uppercase()))
        .ok_or_else(|| Error::validation(format!("unknown heatmap interval: {value}")))
}

/// Channel segment for live heatmap subscriptions (Go `IntervalAliases`).
pub fn heatmap_interval_channel_name(value: &str) -> String {
    match value {
        "1s" => "INTERVAL_1S".to_owned(),
        "1m" => "INTERVAL_1M".to_owned(),
        "5m" => "INTERVAL_5M".to_owned(),
        "1h" => "INTERVAL_1H".to_owned(),
        other => other.to_owned(),
    }
}

pub fn heatmap_depth_for_levels(depth: u32) -> HeatmapDepth {
    match depth {
        0..=5 => HeatmapDepth::Depth5,
        6..=10 => HeatmapDepth::Depth10,
        11..=20 => HeatmapDepth::Depth20,
        21..=50 => HeatmapDepth::Depth50,
        51..=100 => HeatmapDepth::Depth100,
        _ => HeatmapDepth::Depth200,
    }
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
    }

    #[test]
    fn depth_buckets() {
        assert_eq!(heatmap_depth_for_levels(5), HeatmapDepth::Depth5);
        assert_eq!(heatmap_depth_for_levels(50), HeatmapDepth::Depth50);
        assert_eq!(heatmap_depth_for_levels(200), HeatmapDepth::Depth200);
    }
}
