//! Market overview decoders.

use super::money::decode_price_ticks;
use crate::models::{
    CurrencyConversionConfig, CurrencyConversionRates, CurrencyMetadata, FiatConversionRate,
    FiatConversionSnapshot, MarketOverviewEntry, MarketOverviewList, SpotPairVolumeSeries,
    SpotVolumeHistory, StablecoinConversionRate,
};
use crate::proto::marketoverview::v1::{
    CurrencyMetadata as ProtoCurrencyMetadata, FiatConversionSnapshot as ProtoFiatConversionSnapshot,
    GetCurrencyConversionConfigResponse, GetCurrencyConversionRatesResponse,
    GetSpotVolumeHistoryResponse, ListMarketOverviewResponse,
    MarketOverview as ProtoMarketOverview, MarketOverviewBatch,
};

pub fn market_overview_entry_from_proto(msg: &ProtoMarketOverview) -> MarketOverviewEntry {
    MarketOverviewEntry {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
        last_price: decode_price_ticks(msg.last_price_ticks, None),
        index_price: decode_price_ticks(msg.index_price_ticks, None),
        volume_24h_base_scaled: msg.volume_24h_base_scaled.map(|value| value.to_string()),
        volume_24h_quote_scaled: msg.volume_24h_quote_scaled.map(|value| value.to_string()),
        volume_24h_usd_scaled: msg.volume_24h_usd_scaled.map(|value| value.to_string()),
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
