//! Decode `marketdata.v1.ListAssetInfoResponse`.

use crate::models::{AssetEditorialInfo, AssetInfo, AssetIssuePrice};
use crate::proto::marketdata::v1::{
    AssetEditorialInfo as ProtoAssetEditorialInfo, AssetInfo as ProtoAssetInfo,
    ListAssetInfoResponse,
};

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
    }
}
