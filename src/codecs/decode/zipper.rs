//! Zipper deposit/withdraw config decoder.

use crate::models::{
    DepositWithdrawConfig, ZipperAssetChainVariant, ZipperAssetConfig, ZipperChainConfig,
    ZipperChainContractConfig, ZipperTokenConfig,
};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigResponse;

pub fn deposit_withdraw_config_from_proto(
    msg: &GetDepositWithdrawConfigResponse,
) -> DepositWithdrawConfig {
    DepositWithdrawConfig {
        polyester_chain_id: msg.polyester_chain_id,
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
                        source_token: ZipperTokenConfig {
                            address: v.source_address.clone(),
                            decimals: v.source_decimals,
                        },
                        z_token: ZipperTokenConfig {
                            address: v.ztoken_address.clone(),
                            decimals: v.ztoken_decimals,
                        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::chain::zipper::v1::{AssetConfig, ChainConfig};

    #[test]
    fn zipper_config_maps_assets_and_ts() {
        let msg = GetDepositWithdrawConfigResponse {
            polyester_chain_id: 9,
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
        assert_eq!(cfg.polyester_chain_id, 9);
        assert_eq!(cfg.ts_ms, 1_700_000_000_000);
        assert_eq!(cfg.chains[0].code, "ethereum");
        assert_eq!(cfg.assets[0].asset, "USDT");
        assert_eq!(cfg.assets[0].ledger_id, 7);
    }
}
