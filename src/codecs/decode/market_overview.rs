//! Market overview decoders.

use super::money::decode_price_ticks;
use crate::models::{
    MarketOverviewEntry, MarketOverviewList, SpotPairVolumeSeries, SpotVolumeHistory,
};
use crate::proto::marketoverview::v1::{
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
}
