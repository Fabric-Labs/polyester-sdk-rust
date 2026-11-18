//! Asset information (`marketdata.v1.MarketDataService/ListAssetInfo`).
//!
//! Team-maintained descriptions, official links and known launch dates and
//! prices, and market datapoints (supply, market capitalization, rank) with
//! their source and observation time, loaded independently of the trading
//! and funding configs. Empty strings and lists, and an absent `issue_price`
//! or market value, mean unknown: hide them.

use buffa_types::google::protobuf::Timestamp;

/// A known launch price in US dollars, zero included.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssetIssuePrice {
    /// Exact nonnegative decimal string, such as `"0.0123"` or `"0"`; never
    /// parsed into a float.
    pub amount: String,
    /// Always `"USD"`.
    pub currency: String,
}

/// Team-maintained plain text and official links of one asset.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssetEditorialInfo {
    /// Plain-text description; render it as text, not markup.
    pub description: String,
    /// Official HTTPS website.
    pub website_url: String,
    /// HTTPS whitepaper URL.
    pub whitepaper_url: String,
    /// Up to 10 HTTPS block explorer URLs, in editorial order.
    pub explorer_urls: Vec<String>,
    /// Official HTTPS X profile URL.
    pub x_url: String,
    /// Official HTTPS Telegram URL.
    pub telegram_url: String,
    /// Official HTTPS Discord URL.
    pub discord_url: String,
    /// Official HTTPS GitHub URL.
    pub github_url: String,
    /// Launch calendar date as `YYYY-MM-DD`, kept as sent; empty when
    /// unknown. It is a date, not a time.
    pub issue_date: String,
    /// Launch price; `None` when unknown, distinct from a known zero price.
    pub issue_price: Option<AssetIssuePrice>,
}

/// One asset's information from [`crate::services::MarketDataService::list_asset_info`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetInfo {
    /// Numeric asset ID, matching the spot config's `ledger_id`.
    pub asset_id: u32,
    /// Team-maintained information. All fields are empty when it was
    /// cleared.
    pub editorial: AssetEditorialInfo,
    /// Time of the last effective editorial change, in UTC; `None` when it
    /// was never edited.
    pub editorial_updated_at: Option<Timestamp>,
    /// Market datapoints; `None` when the asset has none.
    pub market_stats: Option<AssetMarketStats>,
}

/// Market datapoints of one asset. An absent value is unknown: hide it.
/// Show each value with its source and `as_of` time, such as "Source:
/// CoinMarketCap": a value stays until a newer observation replaces it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetMarketStats {
    /// Coins circulating in the market and in public hands, in asset units.
    pub circulating_supply: Option<AssetMarketValue>,
    /// Coins in existence now, minus any verifiably burned, in asset units.
    pub total_supply: Option<AssetMarketValue>,
    /// Maximum coins that will ever exist, in asset units; `None` when
    /// unknown or when `max_supply_unlimited` is set.
    pub max_supply: Option<AssetMarketValue>,
    /// Set when the source reports no maximum supply (`max_supply` is then
    /// `None`); `None` when a maximum is known or when it is unknown.
    pub max_supply_unlimited: Option<AssetMarketFlag>,
    /// Price multiplied by circulating supply, in US dollars.
    pub market_cap_usd: Option<AssetMarketValue>,
    /// Price multiplied by maximum supply, or by total supply when the
    /// maximum is unknown or unlimited, in US dollars.
    pub fully_diluted_market_cap_usd: Option<AssetMarketValue>,
    /// Market capitalization rank among all listed coins as a positive
    /// integer string, 1 the largest.
    pub rank: Option<AssetMarketValue>,
}

/// One market datapoint with its source and observation time.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetMarketValue {
    /// Exact nonnegative decimal string without sign or exponent, as sent;
    /// never parsed into a float.
    pub value: String,
    /// Who supplied it: `coinmarketcap` (show "Source: CoinMarketCap") or
    /// `polyester` (set by the Polyester team); `unknown(<n>)` for a source
    /// this SDK does not name.
    pub source: String,
    /// When the source last observed or set the value, in UTC. Always set:
    /// a datapoint without it is an [`crate::Error::ResponseContract`].
    pub as_of: Timestamp,
}

/// A market fact that holds when present, with its source and observation
/// time.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetMarketFlag {
    /// Who reported it, as [`AssetMarketValue::source`].
    pub source: String,
    /// When the source last observed it, in UTC.
    pub as_of: Timestamp,
}
