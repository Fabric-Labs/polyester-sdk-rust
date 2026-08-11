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
    pub symbol_id: u32,
    pub timeframe: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandlesResult {
    pub symbol_id: u32,
    pub timeframe: String,
    pub candles: Vec<Candle>,
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
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketOverviewEntry {
    pub symbol_id: u32,
    pub symbol: String,
    pub last_price: Option<Price>,
    pub index_price: Option<Price>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketOverviewList {
    pub markets: Vec<MarketOverviewEntry>,
    pub next_page_token: String,
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
