//! Zipper deposit/withdraw config models (Go `models/zipper.go` parity).

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperTokenConfig {
    pub address: String,
    pub decimals: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperAssetChainVariant {
    pub zipped_asset_id: u32,
    pub chain_id: u32,
    pub is_native_asset: bool,
    pub network_fee: String,
    pub deposit_min_amount: String,
    pub withdraw_min_amount: String,
    pub source_token: ZipperTokenConfig,
    pub z_token: ZipperTokenConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperChainConfig {
    pub chain_id: u32,
    pub code: String,
    pub name: String,
    pub native_chain_id: String,
    pub native_currency_symbol: String,
    pub explorer_url: String,
    pub icon: String,
    pub required_confirmations: u32,
    pub confirmation_time_seconds: u32,
    pub is_case_sensitive: bool,
    pub min_address_length: u32,
    pub max_address_length: u32,
    /// Memo or destination-tag rule for withdrawal destinations on this
    /// network: `"required"`, `"optional"` or `"not_supported"`. Empty when
    /// the network names none, which does not mean a memo can be omitted;
    /// `"UNKNOWN(n)"` for a rule this SDK version does not know. It does not
    /// describe deposit instructions.
    pub memo_requirement: String,
    /// Display label for the memo, such as `"Destination tag"` or `"Memo"`.
    /// Empty when memos are not supported.
    pub memo_label: String,
    /// Human-readable memo format guidance for display, exactly as sent.
    /// Use `memo_requirement` as the rule; do not parse this text. Empty when
    /// no guidance applies.
    pub memo_format_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperAssetConfig {
    pub asset: String,
    pub ledger_id: u32,
    pub name: String,
    pub icon: String,
    pub quantity_scale: u32,
    pub quantity_display_decimals: u32,
    pub u_asset_id: String,
    pub variants: Vec<ZipperAssetChainVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperChainContractConfig {
    pub name: String,
    pub address: String,
    #[serde(rename = "type")]
    pub contract_type: String,
    pub description: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DepositWithdrawConfig {
    pub chains: Vec<ZipperChainConfig>,
    pub assets: Vec<ZipperAssetConfig>,
    pub contracts: Vec<ZipperChainContractConfig>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct ZippedAssetSupplyUpdate {
    pub zipped_asset_id: u32,
    pub supply: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct ZippedAssetSupplyBatch {
    pub updates: Vec<ZippedAssetSupplyUpdate>,
}
