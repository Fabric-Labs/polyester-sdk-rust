//! Realtime publication models (Go `models/realtime.go` parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceQtyPair {
    pub price_ticks: i64,
    pub qty_scaled: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookDeltaUpdate {
    pub symbol_id: u32,
    pub book_seq_start: String,
    pub book_seq_end: String,
    pub reset: bool,
    pub bids: Vec<PriceQtyPair>,
    pub asks: Vec<PriceQtyPair>,
}
