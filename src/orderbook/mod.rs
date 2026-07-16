//! Local orderbook helpers.

use std::collections::BTreeMap;

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

pub fn parse_bucket_ticks(bucket: &str) -> i64 {
    crate::codecs::scalars::parse_price_ticks_str(bucket, "bucket").unwrap_or(1)
}

pub fn bucket_side(book: &BookSide, bucket_ticks: i64) -> BookSide {
    if bucket_ticks <= 1 {
        return book.clone();
    }
    let mut out = BookSide::new();
    for (&price, &qty) in book {
        let bucket = (price / bucket_ticks) * bucket_ticks;
        *out.entry(bucket).or_insert(0) += qty;
    }
    out
}

/// Apply a delta; returns (new_seq, needs_refresh).
pub fn apply_delta(
    bids: &mut BookSide,
    asks: &mut BookSide,
    current_seq: i64,
    seq_start: i64,
    seq_end: i64,
    bid_pairs: &[(i64, i64)],
    ask_pairs: &[(i64, i64)],
) -> (i64, bool) {
    if seq_start > current_seq + 1 {
        return (current_seq, true);
    }
    apply_side_delta(bids, bid_pairs);
    apply_side_delta(asks, ask_pairs);
    (seq_end, false)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let (seq, needs_refresh) = apply_delta(&mut bids, &mut asks, 3, 5, 6, &[(100, 7)], &[]);
        assert!(needs_refresh);
        assert_eq!(seq, 3);
        assert_eq!(bids.get(&100), Some(&5));
    }

    #[test]
    fn apply_delta_updates_book() {
        let mut bids = BookSide::from([(100, 5)]);
        let mut asks = BookSide::from([(200, 3)]);
        let (seq, needs_refresh) = apply_delta(&mut bids, &mut asks, 3, 3, 4, &[(100, 7)], &[]);
        assert!(!needs_refresh);
        assert_eq!(seq, 4);
        assert_eq!(bids.get(&100), Some(&7));
    }

    #[test]
    fn bucket_side_aggregates() {
        let book = BookSide::from([(101, 2), (105, 3)]);
        let bucketed = bucket_side(&book, 10);
        assert_eq!(bucketed.get(&100), Some(&5));
    }
}
