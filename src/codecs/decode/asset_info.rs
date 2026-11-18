//! Decode `marketdata.v1.ListAssetInfoResponse`.

use super::changelog_enum_label;
use crate::errors::{Error, Result};
use crate::models::{
    AssetEditorialInfo, AssetInfo, AssetIssuePrice, AssetMarketFlag, AssetMarketStats,
    AssetMarketValue,
};
use crate::proto::marketdata::v1::{
    AssetEditorialInfo as ProtoAssetEditorialInfo, AssetInfo as ProtoAssetInfo,
    AssetMarketFlag as ProtoAssetMarketFlag, AssetMarketStats as ProtoAssetMarketStats,
    AssetMarketValue as ProtoAssetMarketValue, ListAssetInfoResponse,
};

const OP: &str = "ListAssetInfo";

/// The source of a present datapoint: set, as the contract promises. A
/// source this SDK does not name reads `unknown(<n>)`.
fn market_source(
    field: &str,
    source: &buffa::EnumValue<crate::proto::marketdata::v1::AssetDataSource>,
) -> Result<String> {
    let label = changelog_enum_label(source);
    if label.is_empty() {
        return Err(Error::response_contract(
            OP,
            format!("{field} has no source"),
        ));
    }
    Ok(label)
}

fn market_as_of(
    field: &str,
    as_of: &buffa::MessageField<buffa_types::google::protobuf::Timestamp>,
) -> Result<buffa_types::google::protobuf::Timestamp> {
    as_of
        .as_option()
        .cloned()
        .ok_or_else(|| Error::response_contract(OP, format!("{field} has no as_of")))
}

/// A nonnegative decimal without sign or exponent, at most 64 characters, as
/// the contract states; a rank is a positive integer.
fn valid_market_value(value: &str, integer: bool) -> bool {
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (value, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(whole) || fraction.is_some_and(|f| !digits(f)) {
        return false;
    }
    !integer || (fraction.is_none() && !whole.starts_with('0'))
}

/// A present datapoint must carry its value, source and observation time;
/// an absent one is `None` (unknown).
fn market_value(
    field: &str,
    value: &buffa::MessageField<ProtoAssetMarketValue>,
    integer: bool,
) -> Result<Option<AssetMarketValue>> {
    let Some(v) = value.as_option() else {
        return Ok(None);
    };
    if !valid_market_value(&v.value, integer) {
        return Err(Error::response_contract(
            OP,
            format!("{field} value {:?} is not a valid amount", v.value),
        ));
    }
    Ok(Some(AssetMarketValue {
        value: v.value.clone(),
        source: market_source(field, &v.source)?,
        as_of: market_as_of(field, &v.as_of)?,
    }))
}

fn market_flag(
    field: &str,
    flag: &buffa::MessageField<ProtoAssetMarketFlag>,
) -> Result<Option<AssetMarketFlag>> {
    let Some(f) = flag.as_option() else {
        return Ok(None);
    };
    Ok(Some(AssetMarketFlag {
        source: market_source(field, &f.source)?,
        as_of: market_as_of(field, &f.as_of)?,
    }))
}

fn market_stats_from_proto(msg: &ProtoAssetMarketStats) -> Result<AssetMarketStats> {
    Ok(AssetMarketStats {
        circulating_supply: market_value("circulating_supply", &msg.circulating_supply, false)?,
        total_supply: market_value("total_supply", &msg.total_supply, false)?,
        max_supply: market_value("max_supply", &msg.max_supply, false)?,
        max_supply_unlimited: market_flag("max_supply_unlimited", &msg.max_supply_unlimited)?,
        market_cap_usd: market_value("market_cap_usd", &msg.market_cap_usd, false)?,
        fully_diluted_market_cap_usd: market_value(
            "fully_diluted_market_cap_usd",
            &msg.fully_diluted_market_cap_usd,
            false,
        )?,
        rank: market_value("rank", &msg.rank, true)?,
    })
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

/// Decode one asset's information. A present market datapoint without its
/// value, source or observation time is an [`Error::ResponseContract`].
pub fn asset_info_from_proto(msg: &ProtoAssetInfo) -> Result<AssetInfo> {
    Ok(AssetInfo {
        asset_id: msg.asset_id,
        editorial: msg
            .editorial
            .as_option()
            .map(asset_editorial_from_proto)
            .unwrap_or_default(),
        editorial_updated_at: msg.editorial_updated_at.as_option().cloned(),
        market_stats: msg
            .market_stats
            .as_option()
            .map(market_stats_from_proto)
            .transpose()?,
    })
}

/// Every entry, in the server's ascending `asset_id` order.
pub fn asset_info_list_from_proto(msg: &ListAssetInfoResponse) -> Result<Vec<AssetInfo>> {
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
        let infos = asset_info_list_from_proto(&msg).unwrap();
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
                    as_of: at(1_794_800_000).into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let info = asset_info_from_proto(&msg).unwrap();
        assert_eq!(info.editorial, AssetEditorialInfo::default());
        assert_eq!(info.editorial_updated_at, None);
        let stats = info.market_stats.expect("market stats");
        let circulating = stats.circulating_supply.expect("circulating supply");
        assert_eq!(circulating.value, "19876543.12345678");
        assert_eq!(circulating.source, "coinmarketcap");
        assert_eq!(circulating.as_of.seconds, 1_794_700_000);
        assert_eq!(stats.max_supply, None);
        let unlimited = stats.max_supply_unlimited.expect("unlimited flag");
        assert_eq!(unlimited.source, "polyester");
        let rank = stats.rank.expect("rank");
        assert_eq!(
            (rank.value.as_str(), rank.source.as_str()),
            ("1", "unknown(7)")
        );
        assert_eq!(rank.as_of.seconds, 1_794_800_000);
        assert_eq!(stats.total_supply, None);
        assert_eq!(stats.market_cap_usd, None);
        assert_eq!(stats.fully_diluted_market_cap_usd, None);
    }

    /// A market value is a nonnegative decimal without sign or exponent, at
    /// most 64 characters; a rank is a positive integer.
    #[test]
    fn market_value_grammar() {
        for ok in ["0", "0.5", "19876543.12345678", &"9".repeat(64)] {
            assert!(valid_market_value(ok, false), "{ok}");
        }
        for bad in [
            "",
            "-1",
            "+1",
            "1e9",
            ".5",
            "5.",
            "1.2.3",
            " 1",
            &"9".repeat(65),
        ] {
            assert!(!valid_market_value(bad, false), "{bad:?}");
        }
        assert!(valid_market_value("1", true) && valid_market_value("250", true));
        for bad in ["0", "01", "1.0"] {
            assert!(!valid_market_value(bad, true), "{bad:?}");
        }
    }
}
