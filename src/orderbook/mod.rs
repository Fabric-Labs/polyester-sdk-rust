//! Local orderbook helpers (Go `orderbook` package parity).

mod subscription;

pub use subscription::Subscription;

use std::collections::BTreeMap;

use crate::codecs::decode::{decode_price_ticks, decode_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{OrderBookDeltaUpdate, OrderbookData, OrderbookLevel};

pub type BookSide = BTreeMap<i64, i64>; // price_ticks -> qty_scaled

pub fn apply_side_delta(book: &mut BookSide, pairs: &[(i64, i64)]) {
    for &(price, qty) in pairs {
        // Negative price/qty is wire corruption; never materialize it into the book.
        if price < 0 || qty < 0 {
            continue;
        }
        if qty == 0 {
            book.remove(&price);
        } else {
            book.insert(price, qty);
        }
    }
}

/// Parse a bucket string into a positive tick size. Empty means no bucketing.
pub fn parse_bucket_ticks(bucket: &str) -> Result<i64> {
    if bucket.trim().is_empty() {
        return Ok(0);
    }
    let ticks = crate::codecs::scalars::parse_price_ticks_str(bucket, "bucket")?;
    if ticks <= 0 {
        return Err(Error::validation(
            "bucket must be a positive price increment",
        ));
    }
    Ok(ticks)
}

/// Aggregate levels into executable-side-safe price buckets.
///
/// Bids round down; asks round up so displayed asks never appear below their
/// executable price.
pub fn bucket_side(book: &BookSide, bucket_ticks: i64, asks: bool) -> Result<BookSide> {
    if bucket_ticks <= 0 {
        for (&price, &qty) in book {
            if price < 0 {
                return Err(Error::validation(
                    "orderbook price ticks must be non-negative",
                ));
            }
            if qty < 0 {
                return Err(Error::validation("orderbook quantity must be non-negative"));
            }
        }
        return Ok(book.clone());
    }
    let mut out = BookSide::new();
    for (&price, &qty) in book {
        if price < 0 {
            return Err(Error::validation(
                "orderbook price ticks must be non-negative",
            ));
        }
        if qty <= 0 {
            continue;
        }
        let quotient = price.div_euclid(bucket_ticks);
        let floor = quotient
            .checked_mul(bucket_ticks)
            .ok_or_else(|| Error::validation("orderbook bucket price overflow"))?;
        let bucket = if asks && price.rem_euclid(bucket_ticks) != 0 {
            floor
                .checked_add(bucket_ticks)
                .ok_or_else(|| Error::validation("ask bucket price overflow"))?
        } else {
            floor
        };
        let entry = out.entry(bucket).or_insert(0);
        *entry = entry
            .checked_add(qty)
            .ok_or_else(|| Error::validation("orderbook bucket quantity overflow"))?;
    }
    Ok(out)
}

/// Build a book side from `(price_ticks, qty_scaled)` pairs (zero qty skipped).
pub fn levels_from_pairs(levels: impl IntoIterator<Item = (i64, i64)>) -> BookSide {
    let mut book = BookSide::new();
    for (price, qty) in levels {
        if price < 0 || qty <= 0 {
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
    // Reject the whole update atomically. Skipping only corrupt rows while
    // advancing the sequence leaves a stale book that can no longer self-heal.
    if delta
        .bids
        .iter()
        .chain(&delta.asks)
        .any(|pair| pair.price_ticks < 0 || pair.qty_scaled < 0)
    {
        return (current_seq, true);
    }
    // Keep seq as u64 end-to-end. Never coerce parse failures to 0 (that disables
    // gap detection). Invalid/overflowing sequences fail toward refresh.
    let seq_start = delta.book_seq_start;
    let seq_end = delta.book_seq_end;
    if seq_end < seq_start {
        return (current_seq, true);
    }
    let comparison_seq = if delta.reset { 0 } else { current_seq };
    if comparison_seq != 0 && seq_start > comparison_seq.saturating_add(1) {
        return (current_seq, true);
    }
    if !delta.reset && seq_end <= current_seq {
        return (current_seq, false);
    }
    if delta.reset {
        bids.clear();
        asks.clear();
        current_seq = 0;
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
) -> Result<Vec<OrderbookLevel>> {
    let view = bucket_side(book, bucket_ticks, side == "asks")?;
    let mut entries: Vec<(i64, i64)> = view.into_iter().collect();
    if side == "bids" {
        entries.sort_by_key(|b| std::cmp::Reverse(b.0));
    } else {
        entries.sort_by_key(|a| a.0);
    }
    let limit = limit.min(entries.len());
    let symbol = Some(symbol.to_owned());
    let mut levels = Vec::with_capacity(limit);
    for (price, qty) in entries.into_iter().take(limit) {
        let price = decode_price_ticks(price, symbol.clone())
            .ok_or_else(|| Error::validation("orderbook level has invalid or missing price"))?;
        let qty = decode_qty_scaled(qty, Some(quantity_scale), symbol.clone(), None)
            .ok_or_else(|| Error::validation("orderbook level has invalid or missing quantity"))?;
        levels.push(OrderbookLevel {
            price: Some(price),
            qty: Some(qty),
        });
    }
    Ok(levels)
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
) -> Result<OrderbookData> {
    let limit = depth as usize;
    Ok(OrderbookData {
        symbol: symbol.to_owned(),
        depth,
        book_seq: book_seq.to_string(),
        bids: side_to_levels(bids, "bids", limit, bucket_ticks, symbol, quantity_scale)?,
        asks: side_to_levels(asks, "asks", limit, bucket_ticks, symbol, quantity_scale)?,
    })
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
    fn apply_delta_rejects_malformed_levels_without_advancing_or_mutating() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::from([(200, 3)]);
        let (seq, needs_refresh) = apply_delta(
            &mut bids,
            &mut asks,
            1,
            &delta(2, 2, &[(100, -1), (101, 4)], &[], false),
        );
        assert!(needs_refresh);
        assert_eq!(seq, 1);
        assert_eq!(bids, BookSide::from([(100, 5)]));
        assert_eq!(asks, BookSide::from([(200, 3)]));
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
        let bucketed = bucket_side(&book, 10, false).unwrap();
        assert_eq!(bucketed.get(&100), Some(&5));
        let asks = bucket_side(&book, 10, true).unwrap();
        assert_eq!(asks.get(&110), Some(&5));
        assert!(bucket_side(&BookSide::from([(i64::MAX, 1)]), 10, true).is_err());
    }

    #[test]
    fn bucket_side_rejects_negative_price_and_qty_overflow() {
        assert!(bucket_side(&BookSide::from([(-1, 1)]), 10, false).is_err());
        assert!(bucket_side(&BookSide::from([(100, i64::MAX), (101, 1)]), 10, false).is_err());
        // Extreme floor multiply must fail closed instead of wrapping/panicking.
        assert!(bucket_side(&BookSide::from([(i64::MIN + 1, 1)]), 10, false).is_err());
    }

    #[test]
    fn apply_side_delta_ignores_negative_levels() {
        let mut book = BookSide::from([(100, 5)]);
        apply_side_delta(&mut book, &[(-1, 3), (100, -2), (101, 4)]);
        assert_eq!(book.get(&100), Some(&5));
        assert_eq!(book.get(&101), Some(&4));
        assert!(!book.contains_key(&-1));
    }

    #[test]
    fn build_orderbook_data_rejects_negative_levels() {
        let bids = BookSide::from([(-5, 1)]);
        let asks = BookSide::from([(200, 1)]);
        assert!(build_orderbook_data("BTC-USDT", 2, 7, &bids, &asks, 0, 8).is_err());
    }

    #[test]
    fn parse_bucket_ticks_empty_is_zero() {
        assert_eq!(parse_bucket_ticks("").unwrap(), 0);
        assert!(parse_bucket_ticks("nope").is_err());
        assert!(parse_bucket_ticks("-1").is_err());
    }

    #[test]
    fn build_orderbook_data_sorts_and_limits() {
        let bids = BookSide::from([(100, 1), (110, 2), (120, 3)]);
        let asks = BookSide::from([(200, 4), (210, 5)]);
        let data = build_orderbook_data("BTC-USDT", 2, 7, &bids, &asks, 0, 8).unwrap();
        assert_eq!(data.book_seq, "7");
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.bids[0].price.as_ref().unwrap().as_ticks(), 120);
        assert_eq!(data.asks[0].price.as_ref().unwrap().as_ticks(), 200);
    }
}
