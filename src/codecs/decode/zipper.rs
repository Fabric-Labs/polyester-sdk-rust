//! Zipper deposit/withdraw config decoder.

use crate::codecs::decode::changelog_enum_label;
use crate::codecs::scalars::format_ledger_u64;
use crate::errors::{Error, Result};
use crate::models::{
    DepositWithdrawConfig, ZippedAssetSupplyBatch, ZippedAssetSupplyUpdate,
    ZipperAssetChainVariant, ZipperAssetConfig, ZipperChainConfig, ZipperChainContractConfig,
    ZipperTokenConfig,
};
use crate::proto::chain::zipper::v1::{
    GetDepositWithdrawConfigResponse, MemoRequirement,
    ZippedAssetSupplyBatch as ProtoZippedAssetSupplyBatch,
    ZippedAssetSupplyUpdate as ProtoZippedAssetSupplyUpdate,
};

/// Memo rule label: `required`, `optional` or `not_supported`, empty when
/// unspecified, and `UNKNOWN(n)` for a rule newer than this SDK.
fn memo_requirement_label(requirement: &buffa::EnumValue<MemoRequirement>) -> String {
    match requirement.as_known() {
        Some(MemoRequirement::REQUIRED) => "required".to_owned(),
        Some(MemoRequirement::OPTIONAL) => "optional".to_owned(),
        Some(MemoRequirement::NOT_SUPPORTED) => "not_supported".to_owned(),
        Some(MemoRequirement::MEMO_UNSPECIFIED) => String::new(),
        None => format!("UNKNOWN({})", requirement.to_i32()),
    }
}

pub fn deposit_withdraw_config_from_proto(
    msg: &GetDepositWithdrawConfigResponse,
) -> DepositWithdrawConfig {
    DepositWithdrawConfig {
        ts_ms: (msg.ts_sec as i64).saturating_mul(1000),
        chains: msg
            .chains
            .iter()
            .map(|c| ZipperChainConfig {
                chain_id: c.chain_id,
                code: c.code.clone(),
                name: c.name.clone(),
                native_chain_id: c.native_chain_id.clone(),
                native_currency_symbol: c.native_currency_symbol.clone(),
                explorer_url: c.explorer_url.clone(),
                icon: c.icon.clone(),
                required_confirmations: c.required_confirmations,
                confirmation_time_seconds: c.confirmation_time_seconds,
                is_case_sensitive: c.is_case_sensitive,
                min_address_length: c.min_address_length,
                max_address_length: c.max_address_length,
                memo_requirement: memo_requirement_label(&c.memo_requirement),
                memo_label: c.memo_label.clone(),
                memo_format_hint: c.memo_format_hint.clone(),
            })
            .collect(),
        assets: msg
            .assets
            .iter()
            .map(|a| ZipperAssetConfig {
                asset: a.asset.clone(),
                ledger_id: a.ledger_id,
                name: a.name.clone(),
                icon: a.icon.clone(),
                quantity_scale: a.quantity_scale,
                quantity_display_decimals: a.quantity_display_decimals,
                u_asset_id: a.u_asset_id.clone(),
                variants: a
                    .variants
                    .iter()
                    .map(|v| ZipperAssetChainVariant {
                        zipped_asset_id: v.zipped_asset_id,
                        chain_id: v.chain_id,
                        is_native_asset: v.is_native_asset,
                        network_fee: v.network_fee.clone(),
                        deposit_min_amount: v.deposit_min_amount.clone(),
                        withdraw_min_amount: v.withdraw_min_amount.clone(),
                        // supply_q is scaled by the parent asset's quantity_scale.
                        supply: format_ledger_u64(v.supply_q, a.quantity_scale).unwrap_or_default(),
                        source_token: ZipperTokenConfig {
                            address: v.source_address.clone(),
                            decimals: v.source_decimals,
                        },
                        z_token: ZipperTokenConfig {
                            address: v.ztoken_address.clone(),
                            decimals: v.ztoken_decimals,
                        },
                        deposit_status: changelog_enum_label(&v.deposit_status),
                        withdrawal_status: changelog_enum_label(&v.withdrawal_status),
                    })
                    .collect(),
            })
            .collect(),
        contracts: msg
            .contracts
            .iter()
            .map(|c| ZipperChainContractConfig {
                name: c.name.clone(),
                address: c.address.clone(),
                contract_type: c.r#type.clone(),
                description: c.description.clone(),
                version: c.version,
            })
            .collect(),
    }
}

pub fn zipped_asset_supply_update_from_proto(
    msg: &ProtoZippedAssetSupplyUpdate,
    scale_fn: impl Fn(u32) -> Option<u32>,
) -> Result<ZippedAssetSupplyUpdate> {
    let scale = scale_fn(msg.zipped_asset_id).ok_or_else(|| {
        Error::validation(format!(
            "unknown quantity scale for zipped_asset_id {}; hydrate catalogs first",
            msg.zipped_asset_id
        ))
    })?;
    Ok(ZippedAssetSupplyUpdate {
        zipped_asset_id: msg.zipped_asset_id,
        supply: format_ledger_u64(msg.supply_q, scale).map_err(|e| {
            Error::validation(format!(
                "zipped asset {} supply scale invalid: {e}",
                msg.zipped_asset_id
            ))
        })?,
    })
}

pub fn zipped_asset_supply_batch_from_proto(
    msg: &ProtoZippedAssetSupplyBatch,
    scale_fn: impl Fn(u32) -> Option<u32>,
) -> Result<ZippedAssetSupplyBatch> {
    let mut updates = Vec::with_capacity(msg.updates.len());
    for u in &msg.updates {
        updates.push(zipped_asset_supply_update_from_proto(u, &scale_fn)?);
    }
    Ok(ZippedAssetSupplyBatch { updates })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::chain::zipper::v1::{AssetConfig, ChainConfig};

    #[test]
    fn zipper_config_maps_assets_and_ts() {
        let msg = GetDepositWithdrawConfigResponse {
            ts_sec: 1_700_000_000,
            chains: vec![ChainConfig {
                chain_id: 1,
                code: "ethereum".into(),
                name: "Ethereum".into(),
                ..Default::default()
            }],
            assets: vec![AssetConfig {
                asset: "USDT".into(),
                ledger_id: 7,
                quantity_scale: 6,
                ..Default::default()
            }],
            ..Default::default()
        };
        let cfg = deposit_withdraw_config_from_proto(&msg);
        assert_eq!(cfg.ts_ms, 1_700_000_000_000);
        assert_eq!(cfg.chains[0].code, "ethereum");
        assert_eq!(cfg.assets[0].asset, "USDT");
        assert_eq!(cfg.assets[0].ledger_id, 7);
    }

    #[test]
    fn zipped_supply_decode_rejects_invalid_catalog_scale() {
        let msg = ProtoZippedAssetSupplyUpdate {
            zipped_asset_id: 1,
            supply_q: 1_000,
            ..Default::default()
        };
        let err = zipped_asset_supply_update_from_proto(&msg, |_| Some(65535))
            .expect_err("invalid scale");
        assert!(err.to_string().to_ascii_lowercase().contains("scale"));
    }

    #[test]
    fn zipped_supply_decode_rejects_unknown_catalog_scale() {
        let msg = ProtoZippedAssetSupplyUpdate {
            zipped_asset_id: 99,
            supply_q: 1_000,
            ..Default::default()
        };
        let err = zipped_asset_supply_update_from_proto(&msg, |_| None).unwrap_err();
        assert!(err.to_string().contains("zipped_asset_id 99"));
    }

    /// POLY-9102: a network's memo rule, label and format hint reach the
    /// chain config as sent. An unspecified rule reads as empty, not as
    /// "no memo needed", and a rule newer than this SDK keeps its number.
    #[test]
    fn poly_9102_chain_config_keeps_memo_rule_label_and_hint() {
        use crate::proto::chain::zipper::v1::MemoRequirement;

        let chain = |requirement: buffa::EnumValue<MemoRequirement>| {
            let msg = GetDepositWithdrawConfigResponse {
                chains: vec![ChainConfig {
                    chain_id: 9,
                    code: "xrp".into(),
                    memo_requirement: requirement,
                    memo_label: "Destination tag".into(),
                    memo_format_hint: "Up to 10 digits, e.g. 12345 (no leading zeros)".into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            deposit_withdraw_config_from_proto(&msg).chains.remove(0)
        };
        let xrp = chain(MemoRequirement::OPTIONAL.into());
        assert_eq!(xrp.memo_requirement, "optional");
        assert_eq!(xrp.memo_label, "Destination tag");
        assert_eq!(
            xrp.memo_format_hint,
            "Up to 10 digits, e.g. 12345 (no leading zeros)"
        );
        assert_eq!(
            chain(MemoRequirement::REQUIRED.into()).memo_requirement,
            "required"
        );
        assert_eq!(
            chain(MemoRequirement::NOT_SUPPORTED.into()).memo_requirement,
            "not_supported"
        );
        assert_eq!(
            chain(MemoRequirement::MEMO_UNSPECIFIED.into()).memo_requirement,
            ""
        );
        assert_eq!(
            chain(buffa::EnumValue::Unknown(9)).memo_requirement,
            "UNKNOWN(9)"
        );

        let bare = deposit_withdraw_config_from_proto(&GetDepositWithdrawConfigResponse {
            chains: vec![ChainConfig::default()],
            ..Default::default()
        });
        assert_eq!(bare.chains[0].memo_requirement, "");
        assert_eq!(bare.chains[0].memo_label, "");
        assert_eq!(bare.chains[0].memo_format_hint, "");
    }

    /// Each variant says whether its route accepts new deposits and new
    /// withdrawals. Only `NORMAL` accepts: an unspecified (or, over JSON,
    /// absent) status and one newer than this SDK are not available.
    #[test]
    fn variant_route_status_reads_each_direction() {
        use crate::proto::chain::zipper::v1::{AssetChainVariant, RouteStatus};

        let variant = |deposit: buffa::EnumValue<RouteStatus>,
                       withdrawal: buffa::EnumValue<RouteStatus>| {
            let msg = GetDepositWithdrawConfigResponse {
                assets: vec![AssetConfig {
                    asset: "USDT".into(),
                    variants: vec![AssetChainVariant {
                        zipped_asset_id: 3,
                        chain_id: 2,
                        deposit_status: deposit,
                        withdrawal_status: withdrawal,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            deposit_withdraw_config_from_proto(&msg).assets[0].variants[0].clone()
        };

        let split = variant(RouteStatus::NORMAL.into(), RouteStatus::SUSPENDED.into());
        assert_eq!(split.deposit_status, "normal");
        assert_eq!(split.withdrawal_status, "suspended");
        assert!(split.accepts_deposits());
        assert!(!split.accepts_withdrawals());

        let unknown = variant(
            RouteStatus::STATUS_UNSPECIFIED.into(),
            buffa::EnumValue::Unknown(7),
        );
        assert_eq!(unknown.deposit_status, "");
        assert_eq!(unknown.withdrawal_status, "unknown(7)");
        assert!(!unknown.accepts_deposits());
        assert!(!unknown.accepts_withdrawals());

        let json: GetDepositWithdrawConfigResponse = serde_json::from_str(
            r#"{"assets":[{"asset":"USDT","variants":[{"zippedAssetId":3,"withdrawalStatus":"NORMAL"}]}]}"#,
        )
        .unwrap();
        let absent = &deposit_withdraw_config_from_proto(&json).assets[0].variants[0];
        assert_eq!(absent.deposit_status, "");
        assert!(!absent.accepts_deposits());
        assert!(absent.accepts_withdrawals());
    }
}
