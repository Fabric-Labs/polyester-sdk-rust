//! Market overview decoders.

use super::money::{
    decode_price_ticks, decode_price_ticks_allow_zero, decode_qty_scaled_allow_zero,
};
use crate::errors::Result;
use crate::models::{
    CurrencyConversionConfig, CurrencyConversionRates, CurrencyMetadata, FiatConversionRate,
    FiatConversionSnapshot, LivePriceBand, MarketOverviewEntry, MarketOverviewList,
    MarketOverviewSparkline, PriceBand, SpotPairVolumeSeries, SpotVolumeHistory,
    StablecoinConversionRate,
};
use crate::proto::marketoverview::v1::{
    CurrencyMetadata as ProtoCurrencyMetadata,
    FiatConversionSnapshot as ProtoFiatConversionSnapshot, GetCurrencyConversionConfigResponse,
    GetCurrencyConversionRatesResponse, GetSpotVolumeHistoryResponse, ListMarketOverviewResponse,
    MarketOverview as ProtoMarketOverview, MarketOverviewBatch, Sparkline as ProtoSparkline,
};
use crate::types::money::{Price, Quantity};

/// # Errors
///
/// [`crate::Error::ResponseContract`] when a best bid or ask size is
/// negative.
pub fn market_overview_entry_from_proto(msg: &ProtoMarketOverview) -> Result<MarketOverviewEntry> {
    let (best_bid, best_bid_qty) = book_side(
        msg.best_bid_ticks,
        msg.best_bid_qty_scaled,
        msg.symbol_id,
        "MarketOverview.best_bid_qty_scaled",
    )?;
    let (best_ask, best_ask_qty) = book_side(
        msg.best_ask_ticks,
        msg.best_ask_qty_scaled,
        msg.symbol_id,
        "MarketOverview.best_ask_qty_scaled",
    )?;
    Ok(MarketOverviewEntry {
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
        price_band: msg.price_band.as_option().map(live_price_band_from_proto),
    })
}

/// Decode a live price band, as the market overview row carries it.
pub fn live_price_band_from_proto(
    band: &crate::proto::marketdata::v1::LivePriceBand,
) -> LivePriceBand {
    use crate::proto::marketdata::v1::{
        PriceBandReferenceSource as Source, PriceBandState as State,
    };
    let price = |ticks: Option<i64>| ticks.and_then(|t| decode_price_ticks_allow_zero(t, None));
    let state = match band.state.as_known() {
        Some(State::ACTIVE) => "active".to_owned(),
        Some(State::OFF) => "off".to_owned(),
        Some(State::EMPTY) => "empty".to_owned(),
        Some(State::UNKNOWN) => "unknown".to_owned(),
        Some(State::BAND_STATE_UNSPECIFIED) => String::new(),
        None => format!("unknown({})", band.state.to_i32()),
    };
    let reference_source = match band.reference_source.as_known() {
        Some(Source::LAST_TRADE) => "last_trade".to_owned(),
        Some(Source::LISTING_REFERENCE) => "listing_reference".to_owned(),
        Some(Source::INDEX_PRICE) => "index_price".to_owned(),
        Some(Source::REFERENCE_SOURCE_UNSPECIFIED) => String::new(),
        None => format!("unknown({})", band.reference_source.to_i32()),
    };
    LivePriceBand {
        symbol_id: band.symbol_id,
        state,
        reference_price: price(band.reference_price_ticks),
        reference_source,
        max_buy_price: price(band.max_buy_price_ticks),
        min_sell_price: price(band.min_sell_price_ticks),
        min_buy_price: price(band.min_buy_price_ticks),
        max_sell_price: price(band.max_sell_price_ticks),
        updated_ts_ns: band.updated_ts_ns,
        reanchor_eligible_ts_ns: band.reanchor_eligible_ts_ns,
        reanchor_pending: band.reanchor_pending,
        parameters: band.parameters.as_option().map(|p| PriceBand {
            lower_bps: p.lower_bps,
            upper_bps: p.upper_bps,
            reference_window_ms: p.reference_window_ms,
        }),
        epoch: band.epoch,
        update_id: band.update_id,
        as_of_ts_ns: band.as_of_ts_ns,
        valid_until_ts_ns: band.valid_until_ts_ns,
    }
}

/// The best price of one side of the book and the base quantity at it; both
/// `None` when that side is empty.
fn book_side(
    ticks: i64,
    qty_scaled: i64,
    symbol_id: u32,
    field: &str,
) -> Result<(Option<Price>, Option<Quantity>)> {
    // A negative size is refused whether or not the side has a price.
    let qty = decode_qty_scaled_allow_zero(qty_scaled, None, None, Some(symbol_id), field)?;
    Ok(match decode_price_ticks(ticks, None) {
        Some(price) => (Some(price), Some(qty)),
        None => (None, None),
    })
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

pub fn market_overview_list_from_proto(
    msg: &ListMarketOverviewResponse,
) -> Result<MarketOverviewList> {
    Ok(MarketOverviewList {
        markets: msg
            .markets
            .iter()
            .map(market_overview_entry_from_proto)
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
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

pub fn market_overview_batch_from_proto(msg: &MarketOverviewBatch) -> Result<MarketOverviewList> {
    Ok(MarketOverviewList {
        markets: msg
            .markets
            .iter()
            .map(market_overview_entry_from_proto)
            .collect::<Result<_>>()?,
        next_page_token: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A negative best size is refused whether or not that side has a
    /// price; an empty side with no size still reads as no side.
    #[test]
    fn market_overview_entry_refuses_a_negative_book_size_without_a_price() {
        let row = ProtoMarketOverview {
            symbol_id: 1,
            best_bid_ticks: 74_800_000_000_000,
            best_bid_qty_scaled: 12,
            ..Default::default()
        };
        let entry = market_overview_entry_from_proto(&row).unwrap();
        assert_eq!(
            (entry.best_ask.as_ref(), entry.best_ask_qty.as_ref()),
            (None, None)
        );
        for (field, msg) in [
            (
                "MarketOverview.best_ask_qty_scaled",
                ProtoMarketOverview {
                    best_ask_qty_scaled: -2,
                    ..row.clone()
                },
            ),
            (
                "MarketOverview.best_bid_qty_scaled",
                ProtoMarketOverview {
                    best_bid_ticks: 0,
                    best_bid_qty_scaled: -1,
                    ..row.clone()
                },
            ),
        ] {
            match market_overview_entry_from_proto(&msg) {
                Err(crate::Error::ResponseContract { context, .. }) => assert_eq!(context, field),
                other => panic!("{field}: want Error::ResponseContract, got {other:?}"),
            }
        }
    }

    #[test]
    fn market_overview_entry_refuses_a_negative_book_size() {
        let msg = ProtoMarketOverview {
            symbol_id: 1,
            best_ask_ticks: 75_000_000_000_000,
            best_ask_qty_scaled: -3,
            ..Default::default()
        };
        match market_overview_entry_from_proto(&msg) {
            Err(crate::Error::ResponseContract { context, message }) => assert_eq!(
                (context.as_str(), message.as_str()),
                ("MarketOverview.best_ask_qty_scaled", "negative quantity -3")
            ),
            other => panic!("want Error::ResponseContract, got {other:?}"),
        }
        let list = ListMarketOverviewResponse {
            markets: vec![msg],
            ..Default::default()
        };
        assert!(market_overview_list_from_proto(&list).is_err());
    }

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
        })
        .unwrap();
        let batch = market_overview_batch_from_proto(&MarketOverviewBatch {
            markets: vec![devnet_row()],
            ..Default::default()
        })
        .unwrap();
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
            let row = market_overview_entry_from_proto(&msg).unwrap();
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
        let list = market_overview_list_from_proto(&msg).unwrap();
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
        })
        .unwrap();
        assert_eq!(present.volume_24h_base_scaled.as_deref(), Some("10"));
        assert_eq!(present.volume_24h_quote_scaled.as_deref(), Some("20"));
        assert_eq!(present.volume_24h_usd_scaled.as_deref(), Some("30"));

        let absent = market_overview_entry_from_proto(&ProtoMarketOverview {
            symbol_id: 2,
            ..Default::default()
        })
        .unwrap();
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

    /// A row's live price band keeps its limits, settings and freshness; an
    /// absent band is `None`, and a band past `valid_until` is not fresh.
    #[test]
    fn market_overview_row_carries_its_live_price_band() {
        use crate::proto::marketdata::v1::{
            LivePriceBand as ProtoBand, PriceBand as ProtoParams, PriceBandReferenceSource,
            PriceBandState,
        };
        use std::time::{Duration, UNIX_EPOCH};
        let msg = ProtoMarketOverview {
            symbol_id: 1,
            price_band: ProtoBand {
                symbol_id: 1,
                state: PriceBandState::ACTIVE.into(),
                reference_price_ticks: Some(61_000_250_000_000),
                reference_source: PriceBandReferenceSource::LAST_TRADE.into(),
                max_buy_price_ticks: Some(64_050_250_000_000),
                min_sell_price_ticks: Some(57_950_250_000_000),
                reanchor_eligible_ts_ns: Some(7),
                parameters: ProtoParams {
                    lower_bps: 500,
                    upper_bps: 500,
                    reference_window_ms: 1_000,
                    ..Default::default()
                }
                .into(),
                epoch: 3,
                update_id: 41,
                as_of_ts_ns: 1_000_000_000,
                valid_until_ts_ns: 6_000_000_000,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let band = market_overview_entry_from_proto(&msg)
            .unwrap()
            .price_band
            .expect("band");
        assert_eq!(band.state, "active");
        assert_eq!(band.reference_source, "last_trade");
        assert_eq!(
            band.reference_price.as_ref().unwrap().as_ticks(),
            61_000_250_000_000
        );
        assert_eq!(
            band.max_buy_price.as_ref().unwrap().as_ticks(),
            64_050_250_000_000
        );
        assert_eq!(
            band.min_sell_price.as_ref().unwrap().as_ticks(),
            57_950_250_000_000
        );
        assert!(band.min_buy_price.is_none() && band.max_sell_price.is_none());
        assert_eq!(band.reanchor_eligible_ts_ns, Some(7));
        assert_eq!(band.parameters.unwrap().lower_bps, 500);
        assert_eq!((band.epoch, band.update_id), (3, 41));
        assert!(band.is_fresh_at(UNIX_EPOCH + Duration::from_secs(6)));
        assert!(!band.is_fresh_at(UNIX_EPOCH + Duration::from_nanos(6_000_000_001)));
        let mut lapsed = band.clone();
        lapsed.state = "unknown".into();
        assert!(!lapsed.is_fresh_at(UNIX_EPOCH));

        let none = market_overview_entry_from_proto(&ProtoMarketOverview::default()).unwrap();
        assert_eq!(none.price_band, None);
    }
}
