//! Market overview decoders.

use super::money::decode_price_ticks;
use crate::models::{MarketOverviewEntry, MarketOverviewList};
use crate::proto::marketoverview::v1::{
    ListMarketOverviewResponse, MarketOverview as ProtoMarketOverview, MarketOverviewBatch,
};

pub fn market_overview_entry_from_proto(msg: &ProtoMarketOverview) -> MarketOverviewEntry {
    MarketOverviewEntry {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
        last_price: decode_price_ticks(msg.last_price_ticks, None),
        index_price: decode_price_ticks(msg.index_price_ticks, None),
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
}
