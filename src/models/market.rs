//! Market-data SDK models (Go `models/market.go` / `models/common.go` parity).

use crate::types::{Price, Quantity};
use serde_json::Value;

/// Options for [`crate::services::MarketDataService::get_candles_with`].
#[derive(Debug, Clone, Default)]
pub struct GetCandlesOpts {
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    /// Candle interval alias (`"1m"`, `"MIN_1"`, …). Default `"1m"` when empty.
    pub timeframe: String,
    pub limit: Option<u32>,
    /// Inclusive lower bound (unix seconds UTC).
    pub start: Option<i64>,
    /// Inclusive upper bound (unix seconds UTC).
    pub end: Option<i64>,
    pub include_incomplete: bool,
    /// When true, request composite reference candles. Their OHLC uses
    /// `reference_price_scale`; primary OHLC uses the fixed price scale 9.
    pub include_reference: bool,
    pub page_token: Option<String>,
}

/// Options for [`crate::services::MarketDataService::get_trades_with`].
#[derive(Debug, Clone, Default)]
pub struct GetTradesOpts {
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub limit: Option<u32>,
    /// Inclusive lower bound (unix seconds UTC).
    pub start: Option<i64>,
    /// Inclusive upper bound (unix seconds UTC).
    pub end: Option<i64>,
    pub page_token: Option<String>,
    /// Gap-free backfill cursor: return the symbol's trades with `match_id`
    /// greater than this, ascending by `match_id`, up to `limit`. Pass 0 to
    /// start from the first trade, then the response's
    /// [`MarketTradesResult::next_after_trade_id`]. Cannot be combined with
    /// `start`, `end` or `page_token`.
    pub after_trade_id: Option<u64>,
}

/// Spot pair catalog payload (Go `SpotConfig` raw-map escape hatch).
#[derive(Debug, Clone, PartialEq)]
pub struct SpotConfig {
    pub raw: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    pub ts_sec: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: String,
    pub symbol_id: u32,
    pub timeframe: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandlesResult {
    pub symbol_id: u32,
    pub timeframe: String,
    pub candles: Vec<Candle>,
    pub reference_candles: Vec<Candle>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketTrade {
    pub symbol_id: u32,
    pub match_id: String,
    pub price: Option<Price>,
    pub qty: Option<Quantity>,
    pub ts_ns: String,
    pub side: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketTradesResult {
    pub trades: Vec<MarketTrade>,
    /// Opaque cursor for the next page; always empty with `after_trade_id`.
    pub next_page_token: String,
    /// With `after_trade_id` only (0 otherwise): every trade with `match_id`
    /// at or below this exists and is returned by this method. It only grows.
    pub complete_through_trade_id: u64,
    /// With `after_trade_id` only (0 otherwise): the `after_trade_id` of the
    /// next call. It never moves backwards; when it equals the request's
    /// `after_trade_id`, nothing newer is available yet, so retry later with
    /// backoff.
    pub next_after_trade_id: u64,
    /// With `after_trade_id` only: every trade-ID epoch that ended between
    /// `after_trade_id` and `next_after_trade_id`, ascending by epoch.
    pub epoch_ends: Vec<TradeEpochEnd>,
}

/// Where one epoch of a symbol's trade IDs ended. Trade IDs of epoch `e` are
/// greater than `(e-1) * 2^40` and less than `e * 2^40`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeEpochEnd {
    pub epoch: u64,
    /// `match_id` of the epoch's last trade; `(epoch-1) * 2^40` when the epoch
    /// ended without trades.
    pub last_trade_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketOverviewEntry {
    pub symbol_id: u32,
    pub symbol: String,
    pub last_price: Option<Price>,
    pub index_price: Option<Price>,
    pub volume_24h_base_scaled: Option<String>,
    /// Base volume decoded with the asset `market_data_volume_scale`.
    pub volume_24h_base: Option<String>,
    pub volume_24h_quote_scaled: Option<String>,
    pub volume_24h_usd_scaled: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotPairVolumeSeries {
    pub symbol_id: u32,
    pub symbol: String,
    pub volume_usd_scaled: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotVolumeHistory {
    pub bucket: String,
    pub start_ts_sec: u32,
    pub end_ts_sec: u32,
    pub points: u32,
    pub pairs: Vec<SpotPairVolumeSeries>,
    pub total_volume_usd_scaled: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketOverviewList {
    pub markets: Vec<MarketOverviewEntry>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrencyMetadata {
    pub code: String,
    pub default_english_name: String,
    pub symbol: String,
    pub fraction_digits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrencyConversionConfig {
    pub fiat: Vec<CurrencyMetadata>,
    pub stablecoins: Vec<CurrencyMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiatConversionRate {
    pub code: String,
    pub units_per_usd_e8: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiatConversionSnapshot {
    pub rates: Vec<FiatConversionRate>,
    pub source_ts_sec: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StablecoinConversionRate {
    pub code: String,
    pub usd_per_unit_e8: i64,
    pub source_ts_sec: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrencyConversionRates {
    pub fiat: Option<FiatConversionSnapshot>,
    pub stablecoins: Vec<StablecoinConversionRate>,
    pub snapshot_ts_sec: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderbookLevel {
    pub price: Option<Price>,
    pub qty: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderbookData {
    pub symbol: String,
    pub depth: u32,
    pub book_seq: String,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}
