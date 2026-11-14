//! Asset information (`marketdata.v1.MarketDataService/ListAssetInfo`).
//!
//! Team-maintained descriptions, official links and known launch dates and
//! prices, loaded independently of the trading and funding configs. Empty
//! strings and lists, and an absent `issue_price`, mean unknown: hide them.

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
    /// Time of the last effective editorial change, in UTC.
    pub editorial_updated_at: Option<Timestamp>,
}
