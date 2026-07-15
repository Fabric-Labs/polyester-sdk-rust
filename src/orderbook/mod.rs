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
