//! Decode `marketdata.v1.ListAssetInfoResponse`.

use super::changelog_enum_label;
use crate::models::{
    AssetEditorialInfo, AssetInfo, AssetIssuePrice, AssetMarketFlag, AssetMarketStats,
    AssetMarketValue,
};
use crate::proto::marketdata::v1::{
    AssetEditorialInfo as ProtoAssetEditorialInfo, AssetInfo as ProtoAssetInfo,
    AssetMarketFlag as ProtoAssetMarketFlag, AssetMarketStats as ProtoAssetMarketStats,
    AssetMarketValue as ProtoAssetMarketValue, ListAssetInfoResponse,
};

fn market_value(value: &buffa::MessageField<ProtoAssetMarketValue>) -> Option<AssetMarketValue> {
    value.as_option().map(|v| AssetMarketValue {
        value: v.value.clone(),
        source: changelog_enum_label(&v.source),
        as_of: v.as_of.as_option().cloned(),
    })
}

fn market_flag(flag: &buffa::MessageField<ProtoAssetMarketFlag>) -> Option<AssetMarketFlag> {
    flag.as_option().map(|f| AssetMarketFlag {
        source: changelog_enum_label(&f.source),
        as_of: f.as_of.as_option().cloned(),
    })
}

fn market_stats_from_proto(msg: &ProtoAssetMarketStats) -> AssetMarketStats {
    AssetMarketStats {
        circulating_supply: market_value(&msg.circulating_supply),
        total_supply: market_value(&msg.total_supply),
        max_supply: market_value(&msg.max_supply),
        max_supply_unlimited: market_flag(&msg.max_supply_unlimited),
        market_cap_usd: market_value(&msg.market_cap_usd),
        fully_diluted_market_cap_usd: market_value(&msg.fully_diluted_market_cap_usd),
        rank: market_value(&msg.rank),
    }
}

fn asset_editorial_from_proto(msg: &ProtoAssetEditorialInfo) -> AssetEditorialInfo {
    AssetEditorialInfo {
        description: msg.description.clone(),
        website_url: msg.website_url.clone(),
        whitepaper_url: msg.whitepaper_url.clone(),
        explorer_urls: msg.explorer_urls.clone(),
        x_url: msg.x_url.clone(),
        telegram_url: msg.telegram_url.clone(),
        discord_url: msg.discord_url.clone(),
        github_url: msg.github_url.clone(),
        issue_date: msg.issue_date.clone(),
        issue_price: msg.issue_price.as_option().map(|price| AssetIssuePrice {
            amount: price.amount.clone(),
            currency: price.currency.clone(),
        }),
    }
}

pub fn asset_info_from_proto(msg: &ProtoAssetInfo) -> AssetInfo {
    AssetInfo {
        asset_id: msg.asset_id,
        editorial: msg
            .editorial
            .as_option()
            .map(asset_editorial_from_proto)
            .unwrap_or_default(),
        editorial_updated_at: msg.editorial_updated_at.as_option().cloned(),
        market_stats: msg.market_stats.as_option().map(market_stats_from_proto),
    }
}

/// Every entry, in the server's ascending `asset_id` order.
pub fn asset_info_list_from_proto(msg: &ListAssetInfoResponse) -> Vec<AssetInfo> {
    msg.asset_info.iter().map(asset_info_from_proto).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::marketdata::v1::AssetIssuePrice as ProtoAssetIssuePrice;
    use buffa_types::google::protobuf::Timestamp;

    #[test]
    fn asset_info_keeps_links_dates_and_prices_as_sent() {
        let msg = ListAssetInfoResponse {
            asset_info: vec![
                ProtoAssetInfo {
                    asset_id: 1,
                    editorial: ProtoAssetEditorialInfo {
                        description: "Bitcoin <b>is</b> text".into(),
                        website_url: "https://bitcoin.org".into(),
                        explorer_urls: vec![
                            "https://mempool.space".into(),
                            "https://blockstream.info".into(),
                        ],
                        github_url: "https://github.com/bitcoin".into(),
                        issue_date: "2009-01-03".into(),
                        issue_price: ProtoAssetIssuePrice {
                            amount: "0".into(),
                            currency: "USD".into(),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    }
                    .into(),
                    editorial_updated_at: Timestamp {
                        seconds: 1_794_500_000,
                        nanos: 250_000_000,
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
                // A cleared entry: no editorial fields, no price, no time.
                ProtoAssetInfo {
                    asset_id: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let infos = asset_info_list_from_proto(&msg);
        assert_eq!(infos.len(), 2);
        let btc = &infos[0];
        assert_eq!(btc.asset_id, 1);
        assert_eq!(btc.editorial.description, "Bitcoin <b>is</b> text");
        assert_eq!(btc.editorial.website_url, "https://bitcoin.org");
        assert_eq!(
            btc.editorial.explorer_urls,
            ["https://mempool.space", "https://blockstream.info"]
        );
        assert_eq!(btc.editorial.issue_date, "2009-01-03");
        // A known zero launch price is kept, distinct from an unknown one.
        assert_eq!(
            btc.editorial.issue_price,
            Some(AssetIssuePrice {
                amount: "0".into(),
                currency: "USD".into()
            })
        );
        let updated = btc.editorial_updated_at.as_ref().unwrap();
        assert_eq!(
            (updated.seconds, updated.nanos),
            (1_794_500_000, 250_000_000)
        );

        let cleared = &infos[1];
        assert_eq!(cleared.asset_id, 2);
        assert_eq!(cleared.editorial, AssetEditorialInfo::default());
        assert_eq!(cleared.editorial.issue_price, None);
        assert_eq!(cleared.editorial_updated_at, None);
        assert_eq!(cleared.market_stats, None);
    }

    /// Market datapoints keep their exact values, sources and times; an
    /// absent value stays unknown, and an unlimited maximum supply is a flag
    /// with no value. An asset with datapoints and no editorial is listed
    /// with an empty editorial.
    #[test]
    fn asset_market_stats_keep_values_sources_and_times() {
        use crate::proto::marketdata::v1::{
            AssetDataSource, AssetMarketFlag as Flag, AssetMarketStats as Stats,
            AssetMarketValue as Value,
        };
        let at = |seconds| Timestamp {
            seconds,
            ..Default::default()
        };
        let msg = ProtoAssetInfo {
            asset_id: 3,
            market_stats: Stats {
                circulating_supply: Value {
                    value: "19876543.12345678".into(),
                    source: AssetDataSource::COINMARKETCAP.into(),
                    as_of: at(1_794_700_000).into(),
                    ..Default::default()
                }
                .into(),
                max_supply_unlimited: Flag {
                    source: AssetDataSource::POLYESTER.into(),
                    as_of: at(1_794_600_000).into(),
                    ..Default::default()
                }
                .into(),
                rank: Value {
                    value: "1".into(),
                    source: buffa::EnumValue::from(7),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let info = asset_info_from_proto(&msg);
        assert_eq!(info.editorial, AssetEditorialInfo::default());
        assert_eq!(info.editorial_updated_at, None);
        let stats = info.market_stats.expect("market stats");
        let circulating = stats.circulating_supply.expect("circulating supply");
        assert_eq!(circulating.value, "19876543.12345678");
        assert_eq!(circulating.source, "coinmarketcap");
        assert_eq!(circulating.as_of.map(|t| t.seconds), Some(1_794_700_000));
        assert_eq!(stats.max_supply, None);
        let unlimited = stats.max_supply_unlimited.expect("unlimited flag");
        assert_eq!(unlimited.source, "polyester");
        let rank = stats.rank.expect("rank");
        assert_eq!(
            (rank.value.as_str(), rank.source.as_str()),
            ("1", "unknown(7)")
        );
        assert_eq!(rank.as_of, None);
        assert_eq!(stats.total_supply, None);
        assert_eq!(stats.market_cap_usd, None);
        assert_eq!(stats.fully_diluted_market_cap_usd, None);
    }
}
