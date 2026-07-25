//! Local orderbook helpers (Go `orderbook` package parity).

mod subscription;

pub use subscription::Subscription;

use std::collections::BTreeMap;

use crate::codecs::decode::{decode_price_ticks, decode_qty_scaled};
use crate::models::{OrderBookDeltaUpdate, OrderbookData, OrderbookLevel};

pub type BookSide = BTreeMap<i64, i64>; // price_ticks -> qty_scaled

pub fn apply_side_delta(book: &mut BookSide, pairs: &[(i64, i64)]) {
    for &(price, qty) in pairs {
        if qty == 0 {
            book.remove(&price);
        } else {
            book.insert(price, qty);
        }
    }
}

/// Parse a bucket string into tick size. Empty/invalid → `0` (no bucketing).
pub fn parse_bucket_ticks(bucket: &str) -> i64 {
    if bucket.is_empty() {
        return 0;
    }
    crate::codecs::scalars::parse_price_ticks_str(bucket, "bucket").unwrap_or(0)
}

/// Aggregate levels into price buckets. `bucket_ticks <= 0` returns `book` unchanged.
pub fn bucket_side(book: &BookSide, bucket_ticks: i64) -> BookSide {
    if bucket_ticks <= 0 {
        return book.clone();
    }
    let mut out = BookSide::new();
    for (&price, &qty) in book {
        if qty <= 0 {
            continue;
        }
        let bucket = (price / bucket_ticks) * bucket_ticks;
        *out.entry(bucket).or_insert(0) += qty;
    }
    out
}

/// Build a book side from `(price_ticks, qty_scaled)` pairs (zero qty skipped).
pub fn levels_from_pairs(levels: impl IntoIterator<Item = (i64, i64)>) -> BookSide {
    let mut book = BookSide::new();
    for (price, qty) in levels {
        if qty == 0 {
            continue;
        }
        book.insert(price, qty);
    }
    book
}

/// Build a book side from decoded [`OrderbookLevel`] rows.
pub fn levels_from_orderbook_side(levels: &[OrderbookLevel]) -> BookSide {
    levels_from_pairs(levels.iter().filter_map(|l| {
        let price = l.price.as_ref()?.as_ticks();
        let qty = l.qty.as_ref()?.as_scaled();
        Some((price, qty))
    }))
}

/// Apply a delta; returns `(new_seq, needs_refresh)`.
pub fn apply_delta(
    bids: &mut BookSide,
    asks: &mut BookSide,
    mut current_seq: u64,
    delta: &OrderBookDeltaUpdate,
) -> (u64, bool) {
    if delta.reset {
        bids.clear();
        asks.clear();
        current_seq = 0;
    }
    // Keep seq as u64 end-to-end. Never coerce parse failures to 0 (that disables
    // gap detection). Invalid/overflowing sequences fail toward refresh.
    let seq_start = delta.book_seq_start;
    let seq_end = delta.book_seq_end;
    if seq_end < seq_start {
        return (current_seq, true);
    }
    if current_seq != 0 && seq_start > current_seq.saturating_add(1) {
        return (current_seq, true);
    }
    if seq_end <= current_seq {
        return (current_seq, false);
    }
    let bid_pairs: Vec<(i64, i64)> = delta
        .bids
        .iter()
        .map(|p| (p.price_ticks, p.qty_scaled))
        .collect();
    let ask_pairs: Vec<(i64, i64)> = delta
        .asks
        .iter()
        .map(|p| (p.price_ticks, p.qty_scaled))
        .collect();
    apply_side_delta(bids, &bid_pairs);
    apply_side_delta(asks, &ask_pairs);
    if seq_end > current_seq {
        current_seq = seq_end;
    }
    (current_seq, false)
}

fn side_to_levels(
    book: &BookSide,
    side: &str,
    limit: usize,
    bucket_ticks: i64,
    symbol: &str,
    quantity_scale: u32,
) -> Vec<OrderbookLevel> {
    let view = bucket_side(book, bucket_ticks);
    let mut entries: Vec<(i64, i64)> = view.into_iter().collect();
    if side == "bids" {
        entries.sort_by_key(|b| std::cmp::Reverse(b.0));
    } else {
        entries.sort_by_key(|a| a.0);
    }
    let limit = limit.min(entries.len());
    let symbol = Some(symbol.to_owned());
    entries
        .into_iter()
        .take(limit)
        .map(|(price, qty)| OrderbookLevel {
            price: decode_price_ticks(price, symbol.clone()),
            qty: decode_qty_scaled(qty, Some(quantity_scale), symbol.clone(), None),
        })
        .collect()
}

/// Render the current in-memory book as [`OrderbookData`].
pub fn build_orderbook_data(
    symbol: &str,
    depth: u32,
    book_seq: u64,
    bids: &BookSide,
    asks: &BookSide,
    bucket_ticks: i64,
    quantity_scale: u32,
) -> OrderbookData {
    let limit = depth as usize;
    OrderbookData {
        symbol: symbol.to_owned(),
        depth,
        book_seq: book_seq.to_string(),
        bids: side_to_levels(bids, "bids", limit, bucket_ticks, symbol, quantity_scale),
        asks: side_to_levels(asks, "asks", limit, bucket_ticks, symbol, quantity_scale),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::PriceQtyPair;

    fn delta(
        start: u64,
        end: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
        reset: bool,
    ) -> OrderBookDeltaUpdate {
        OrderBookDeltaUpdate {
            symbol_id: 1,
            book_seq_start: start,
            book_seq_end: end,
            reset,
            bids: bids
                .iter()
                .map(|&(price_ticks, qty_scaled)| PriceQtyPair {
                    price_ticks,
                    qty_scaled,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|&(price_ticks, qty_scaled)| PriceQtyPair {
                    price_ticks,
                    qty_scaled,
                })
                .collect(),
        }
    }

    #[test]
    fn apply_side_delta_updates_and_deletes() {
        let mut book = BookSide::from([(100, 5)]);
        apply_side_delta(&mut book, &[(100, 7), (101, 2)]);
        assert_eq!(book.get(&100), Some(&7));
        assert_eq!(book.get(&101), Some(&2));
        apply_side_delta(&mut book, &[(101, 0)]);
        assert!(!book.contains_key(&101));
    }

    #[test]
    fn apply_delta_detects_gap() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::from([(200, 3)]);
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            3,
            &delta(5, 6, &[(100, 7)], &[], false),
        );
        assert!(needs_refresh);
        assert_eq!(seq, 3);
        assert_eq!(bids.get(&100), Some(&5));
    }

    #[test]
    fn apply_delta_updates_book() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::from([(200, 3)]);
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            3,
            &delta(3, 4, &[(100, 7)], &[], false),
        );
        assert!(!needs_refresh);
        assert_eq!(seq, 4);
        assert_eq!(bids.get(&100), Some(&7));
    }

    #[test]
    fn apply_delta_skips_stale() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::new();
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            5,
            &delta(3, 4, &[(100, 9)], &[], false),
        );
        assert!(!needs_refresh);
        assert_eq!(seq, 5);
        assert_eq!(bids.get(&100), Some(&5));
    }

    #[test]
    fn apply_delta_inverted_seq_fails_toward_refresh() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::new();
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            3,
            &delta(9, 2, &[(100, 9)], &[], false),
        );
        assert!(needs_refresh);
        assert_eq!(seq, 3);
        assert_eq!(bids.get(&100), Some(&5));
    }

    #[test]
    fn apply_delta_reset_clears_book() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::from([(200, 3)]);
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            9,
            &delta(1, 2, &[(101, 4)], &[], true),
        );
        assert!(!needs_refresh);
        assert_eq!(seq, 2);
        assert_eq!(bids.get(&101), Some(&4));
        assert!(!bids.contains_key(&100));
        assert!(asks.is_empty());
    }

    #[test]
    fn bucket_side_aggregates() {
        let book = BookSide::from([(101, 2), (105, 3)]);
        let bucketed = bucket_side(&book, 10);
        assert_eq!(bucketed.get(&100), Some(&5));
    }

    #[test]
    fn parse_bucket_ticks_empty_is_zero() {
        assert_eq!(parse_bucket_ticks(""), 0);
    }

    #[test]
    fn build_orderbook_data_sorts_and_limits() {
        let bids = BookSide::from([(100, 1), (110, 2), (120, 3)]);
        let asks = BookSide::from([(200, 4), (210, 5)]);
        let data = build_orderbook_data("BTC-USDT", 2, 7, &bids, &asks, 0, 8);
        assert_eq!(data.book_seq, "7");
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.bids[0].price.as_ref().unwrap().as_ticks(), 120);
        assert_eq!(data.asks[0].price.as_ref().unwrap().as_ticks(), 200);
    }
}
