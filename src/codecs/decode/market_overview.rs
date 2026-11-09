//! Market overview decoders.

use super::money::{
    decode_price_ticks, decode_price_ticks_allow_zero, decode_qty_scaled_allow_zero,
};
use crate::models::{
    CurrencyConversionConfig, CurrencyConversionRates, CurrencyMetadata, FiatConversionRate,
    FiatConversionSnapshot, MarketOverviewEntry, MarketOverviewList, MarketOverviewSparkline,
    SpotPairVolumeSeries, SpotVolumeHistory, StablecoinConversionRate,
};
use crate::proto::marketoverview::v1::{
    CurrencyMetadata as ProtoCurrencyMetadata,
    FiatConversionSnapshot as ProtoFiatConversionSnapshot, GetCurrencyConversionConfigResponse,
    GetCurrencyConversionRatesResponse, GetSpotVolumeHistoryResponse, ListMarketOverviewResponse,
    MarketOverview as ProtoMarketOverview, MarketOverviewBatch, Sparkline as ProtoSparkline,
};
use crate::types::money::{Price, Quantity};

pub fn market_overview_entry_from_proto(msg: &ProtoMarketOverview) -> MarketOverviewEntry {
    let (best_bid, best_bid_qty) =
        book_side(msg.best_bid_ticks, msg.best_bid_qty_scaled, msg.symbol_id);
    let (best_ask, best_ask_qty) =
        book_side(msg.best_ask_ticks, msg.best_ask_qty_scaled, msg.symbol_id);
    MarketOverviewEntry {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
        last_price: decode_price_ticks(msg.last_price_ticks, None),
        index_price: decode_price_ticks(msg.index_price_ticks, None),
        volume_24h_base_scaled: msg.volume_24h_base_scaled.map(|value| value.to_string()),
        volume_24h_base: None,
        volume_24h_quote_scaled: msg.volume_24h_quote_scaled.map(|value| value.to_string()),
        volume_24h_usd_scaled: msg.volume_24h_usd_scaled.map(|value| value.to_string()),
        change_24h_bps: msg.change_24h_bps,
        sparklines: msg
            .sparklines
            .iter()
            .map(market_overview_sparkline_from_proto)
            .collect(),
        high_24h: decode_price_ticks(msg.high_24h_ticks, None),
        low_24h: decode_price_ticks(msg.low_24h_ticks, None),
        best_bid,
        best_bid_qty,
        best_ask,
        best_ask_qty,
        last_trade_ts_ns: ts_ns_or_empty(msg.last_trade_ts_ns),
        listed_ts_ns: ts_ns_or_empty(msg.listed_ts_ns),
    }
}

/// The best price of one side of the book and the base quantity at it; both
/// `None` when that side is empty.
fn book_side(ticks: i64, qty_scaled: i64, symbol_id: u32) -> (Option<Price>, Option<Quantity>) {
    match decode_price_ticks(ticks, None) {
        Some(price) => (
            Some(price),
            decode_qty_scaled_allow_zero(qty_scaled, None, None, Some(symbol_id)),
        ),
        None => (None, None),
    }
}

fn ts_ns_or_empty(ts_ns: u64) -> String {
    if ts_ns == 0 {
        String::new()
    } else {
        ts_ns.to_string()
    }
}

/// A sparkline's interval name and close prices, newest first. A negative close
/// price, which is not a price, is skipped.
pub fn market_overview_sparkline_from_proto(msg: &ProtoSparkline) -> MarketOverviewSparkline {
    use buffa::Enumeration as _;
    MarketOverviewSparkline {
        interval: msg.interval.as_known().map_or_else(
            || msg.interval.to_i32().to_string(),
            |known| known.proto_name().to_owned(),
        ),
        close_prices: msg
            .close_ticks
            .iter()
            .filter_map(|ticks| decode_price_ticks_allow_zero(*ticks, None))
            .collect(),
    }
}

pub fn spot_volume_history_from_proto(msg: &GetSpotVolumeHistoryResponse) -> SpotVolumeHistory {
    SpotVolumeHistory {
        bucket: msg.bucket.clone(),
        start_ts_sec: msg.start_ts_sec,
        end_ts_sec: msg.end_ts_sec,
        points: msg.points,
        pairs: msg
            .pairs
            .iter()
            .map(|item| SpotPairVolumeSeries {
                symbol_id: item.symbol_id,
                symbol: String::new(),
                volume_usd_scaled: item.volume_usd_scaled.clone(),
            })
            .collect(),
        total_volume_usd_scaled: msg.total_volume_usd_scaled.clone(),
    }
}

pub fn market_overview_list_from_proto(msg: &ListMarketOverviewResponse) -> MarketOverviewList {
    MarketOverviewList {
        markets: msg
            .markets
            .iter()
            .map(market_overview_entry_from_proto)
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn currency_metadata_from_proto(msg: &ProtoCurrencyMetadata) -> CurrencyMetadata {
    CurrencyMetadata {
        code: msg.code.clone(),
        default_english_name: msg.default_english_name.clone(),
        symbol: msg.symbol.clone(),
        fraction_digits: msg.fraction_digits,
    }
}

pub fn currency_conversion_config_from_proto(
    msg: &GetCurrencyConversionConfigResponse,
) -> CurrencyConversionConfig {
    CurrencyConversionConfig {
        fiat: msg.fiat.iter().map(currency_metadata_from_proto).collect(),
        stablecoins: msg
            .stablecoins
            .iter()
            .map(currency_metadata_from_proto)
            .collect(),
    }
}

fn fiat_conversion_snapshot_from_proto(
    msg: &ProtoFiatConversionSnapshot,
) -> FiatConversionSnapshot {
    FiatConversionSnapshot {
        rates: msg
            .rates
            .iter()
            .map(|item| FiatConversionRate {
                code: item.code.clone(),
                units_per_usd_e8: item.units_per_usd_e8,
            })
            .collect(),
        source_ts_sec: msg.source_ts_sec,
        stale: msg.stale,
    }
}

pub fn currency_conversion_rates_from_proto(
    msg: &GetCurrencyConversionRatesResponse,
) -> CurrencyConversionRates {
    CurrencyConversionRates {
        fiat: msg
            .fiat
            .as_option()
            .map(fiat_conversion_snapshot_from_proto),
        stablecoins: msg
            .stablecoins
            .iter()
            .map(|item| StablecoinConversionRate {
                code: item.code.clone(),
                usd_per_unit_e8: item.usd_per_unit_e8,
                source_ts_sec: item.source_ts_sec,
                stale: item.stale,
            })
            .collect(),
        snapshot_ts_sec: msg.snapshot_ts_sec,
    }
}

pub fn market_overview_batch_from_proto(msg: &MarketOverviewBatch) -> MarketOverviewList {
    MarketOverviewList {
        markets: msg
            .markets
            .iter()
            .map(market_overview_entry_from_proto)
            .collect(),
        next_page_token: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Devnet BTC-USDT row from POLY-9019.
    fn devnet_row() -> ProtoMarketOverview {
        ProtoMarketOverview {
            symbol_id: 1,
            last_price_ticks: 74_900_000_000_000,
            high_24h_ticks: 76_167_500_000_000,
            low_24h_ticks: 73_621_960_000_000,
            best_bid_ticks: 74_883_900_000_000,
            best_bid_qty_scaled: 6_166_686,
            best_ask_ticks: 74_921_360_000_000,
            best_ask_qty_scaled: 5_532_396,
            last_trade_ts_ns: 1_794_236_100_652_374_117,
            listed_ts_ns: 1_792_404_511_505_163_000,
            ..Default::default()
        }
    }

    fn ticks(price: &Option<crate::types::money::Price>) -> Option<i64> {
        price.as_ref().map(|price| price.as_ticks())
    }

    fn scaled(qty: &Option<crate::types::money::Quantity>) -> Option<i64> {
        qty.as_ref().map(|qty| qty.as_scaled())
    }

    // POLY-9019: rows keep the 24h range, the top of book with sizes, and the
    // last-trade and listing times, exactly as sent.
    #[test]
    fn overview_keeps_range_top_of_book_and_times() {
        let list = market_overview_list_from_proto(&ListMarketOverviewResponse {
            markets: vec![devnet_row()],
            ..Default::default()
        });
        let batch = market_overview_batch_from_proto(&MarketOverviewBatch {
            markets: vec![devnet_row()],
            ..Default::default()
        });
        for row in [&list.markets[0], &batch.markets[0]] {
            assert_eq!(ticks(&row.high_24h), Some(76_167_500_000_000));
            assert_eq!(ticks(&row.low_24h), Some(73_621_960_000_000));
            assert_eq!(ticks(&row.best_bid), Some(74_883_900_000_000));
            assert_eq!(scaled(&row.best_bid_qty), Some(6_166_686));
            assert_eq!(ticks(&row.best_ask), Some(74_921_360_000_000));
            assert_eq!(scaled(&row.best_ask_qty), Some(5_532_396));
            assert_eq!(row.last_trade_ts_ns, "1794236100652374117");
            assert_eq!(row.listed_ts_ns, "1792404511505163000");
            assert_eq!(ticks(&row.last_price), Some(74_900_000_000_000));
        }
    }

    // POLY-9019: an empty book side and missing times decode as unset, not 0.
    #[test]
    fn overview_empty_side_and_missing_times_are_unset() {
        for bid in [true, false] {
            let mut msg = devnet_row();
            msg.high_24h_ticks = 0;
            msg.low_24h_ticks = 0;
            msg.last_trade_ts_ns = 0;
            msg.listed_ts_ns = 0;
            if bid {
                msg.best_bid_ticks = 0;
                msg.best_bid_qty_scaled = 0;
            } else {
                msg.best_ask_ticks = 0;
                msg.best_ask_qty_scaled = 0;
            }
            let row = market_overview_entry_from_proto(&msg);
            assert!(row.high_24h.is_none() && row.low_24h.is_none());
            assert!(row.last_trade_ts_ns.is_empty() && row.listed_ts_ns.is_empty());
            let (empty, other) = if bid {
                (
                    (&row.best_bid, &row.best_bid_qty),
                    (&row.best_ask, &row.best_ask_qty),
                )
            } else {
                (
                    (&row.best_ask, &row.best_ask_qty),
                    (&row.best_bid, &row.best_bid_qty),
                )
            };
            assert!(empty.0.is_none() && empty.1.is_none(), "{row:?}");
            assert!(other.0.is_some() && other.1.is_some(), "{row:?}");
        }
    }

    #[test]
    fn overview_maps_last_price() {
        let msg = ListMarketOverviewResponse {
            markets: vec![ProtoMarketOverview {
                symbol_id: 2,
                last_price_ticks: 2_000_000,
                index_price_ticks: 1_999_500,
                ..Default::default()
            }],
            next_page_token: "tok".into(),
            ..Default::default()
        };
        let list = market_overview_list_from_proto(&msg);
        assert_eq!(list.markets.len(), 1);
        assert_eq!(list.markets[0].symbol_id, 2);
        assert_eq!(list.markets[0].symbol, "");
        assert_eq!(
            list.markets[0].last_price.as_ref().unwrap().as_ticks(),
            2_000_000
        );
        assert_eq!(
            list.markets[0].index_price.as_ref().unwrap().as_ticks(),
            1_999_500
        );
        assert_eq!(list.next_page_token, "tok");
    }

    #[test]
    fn overview_optional_volume_fields_stay_absent() {
        let present = market_overview_entry_from_proto(&ProtoMarketOverview {
            symbol_id: 1,
            volume_24h_base_scaled: Some(10),
            volume_24h_quote_scaled: Some(20),
            volume_24h_usd_scaled: Some(30),
            ..Default::default()
        });
        assert_eq!(present.volume_24h_base_scaled.as_deref(), Some("10"));
        assert_eq!(present.volume_24h_quote_scaled.as_deref(), Some("20"));
        assert_eq!(present.volume_24h_usd_scaled.as_deref(), Some("30"));

        let absent = market_overview_entry_from_proto(&ProtoMarketOverview {
            symbol_id: 2,
            ..Default::default()
        });
        assert!(absent.volume_24h_base_scaled.is_none());
        assert!(absent.volume_24h_quote_scaled.is_none());
        assert!(absent.volume_24h_usd_scaled.is_none());
    }

    #[test]
    fn spot_volume_history_from_proto_maps_columnar_series() {
        let result = spot_volume_history_from_proto(&GetSpotVolumeHistoryResponse {
            bucket: "15m".into(),
            start_ts_sec: 100,
            end_ts_sec: 200,
            points: 2,
            pairs: vec![crate::proto::marketoverview::v1::SpotPairVolumeSeries {
                symbol_id: 7,
                volume_usd_scaled: vec![1, 2],
                ..Default::default()
            }],
            total_volume_usd_scaled: vec![3, 4],
            ..Default::default()
        });
        assert_eq!(result.bucket, "15m");
        assert_eq!(result.points, 2);
        assert_eq!(result.pairs[0].symbol_id, 7);
        assert_eq!(result.pairs[0].volume_usd_scaled, vec![1, 2]);
        assert_eq!(result.total_volume_usd_scaled, vec![3, 4]);
    }

    #[test]
    fn currency_conversion_config_from_proto_maps_metadata() {
        let result = currency_conversion_config_from_proto(&GetCurrencyConversionConfigResponse {
            fiat: vec![ProtoCurrencyMetadata {
                code: "EUR".into(),
                default_english_name: "Euro".into(),
                symbol: "€".into(),
                fraction_digits: 2,
                ..Default::default()
            }],
            stablecoins: vec![ProtoCurrencyMetadata {
                code: "USDT".into(),
                default_english_name: "Tether".into(),
                symbol: "USDT".into(),
                fraction_digits: 2,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(result.fiat[0].code, "EUR");
        assert_eq!(result.fiat[0].symbol, "€");
        assert_eq!(result.stablecoins[0].code, "USDT");
    }

    #[test]
    fn currency_conversion_rates_preserve_e8_and_absent_fiat() {
        let present = currency_conversion_rates_from_proto(&GetCurrencyConversionRatesResponse {
            fiat: crate::proto::marketoverview::v1::FiatConversionSnapshot {
                rates: vec![crate::proto::marketoverview::v1::FiatConversionRate {
                    code: "USD".into(),
                    units_per_usd_e8: 100_000_000,
                    ..Default::default()
                }],
                source_ts_sec: 1_700_000_000,
                stale: true,
                ..Default::default()
            }
            .into(),
            stablecoins: vec![crate::proto::marketoverview::v1::StablecoinConversionRate {
                code: "USDT".into(),
                usd_per_unit_e8: 99_990_000,
                source_ts_sec: 1_700_000_005,
                stale: false,
                ..Default::default()
            }],
            snapshot_ts_sec: 1_700_000_010,
            ..Default::default()
        });
        let fiat = present.fiat.expect("fiat snapshot");
        assert!(fiat.stale);
        assert_eq!(fiat.rates[0].units_per_usd_e8, 100_000_000);
        assert_eq!(present.stablecoins[0].usd_per_unit_e8, 99_990_000);

        let absent = currency_conversion_rates_from_proto(&GetCurrencyConversionRatesResponse {
            snapshot_ts_sec: 7,
            ..Default::default()
        });
        assert!(absent.fiat.is_none());
        assert!(absent.stablecoins.is_empty());
        assert_eq!(absent.snapshot_ts_sec, 7);
    }
}
