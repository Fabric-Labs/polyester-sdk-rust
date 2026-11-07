//! Orderbook snapshot decoders.

use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{OrderbookData, OrderbookLevel};
use crate::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};

/// Depths the server publishes on the delta stream, fewer than the snapshot enum.
/// It also accepts a subscribe at any other depth, and then never sends anything on it.
pub const ORDERBOOK_STREAM_DEPTHS: [u32; 5] = [1, 20, 50, 200, 500];

/// Depths `orderbook.get` can request, ascending.
pub const ORDERBOOK_SNAPSHOT_DEPTHS: [u32; 9] = [1, 5, 10, 20, 50, 100, 200, 500, 1000];

const ORDERBOOK_SNAPSHOT_MAX_DEPTH: u32 = 1000;
const ORDERBOOK_SNAPSHOT_DEFAULT_DEPTH: u32 = 50;

/// Snapshot depth `orderbook.get` requests for `depth`: `depth` itself when it is
/// a snapshot depth, else the smallest one covering it, so a snapshot never holds
/// fewer levels than asked. 0 is the default 50. A depth above 1000 is a
/// validation error.
pub fn orderbook_snapshot_depth(depth: u32) -> Result<u32> {
    if depth == 0 {
        return Ok(ORDERBOOK_SNAPSHOT_DEFAULT_DEPTH);
    }
    ORDERBOOK_SNAPSHOT_DEPTHS
        .into_iter()
        .find(|supported| *supported >= depth)
        .ok_or_else(|| {
            Error::validation(format!(
                "orderbook snapshot depth {depth} is above the deepest snapshot depth \
                 ({ORDERBOOK_SNAPSHOT_MAX_DEPTH})"
            ))
        })
}

fn deepest_stream_depth() -> u32 {
    ORDERBOOK_STREAM_DEPTHS[ORDERBOOK_STREAM_DEPTHS.len() - 1]
}

/// Smallest published stream depth covering `depth`, for a view trimmed to `depth`.
/// A depth no tier covers is a validation error.
pub fn orderbook_stream_tier(depth: u32) -> Result<u32> {
    if depth == 0 {
        return Err(Error::validation(
            "orderbook subscription depth must be positive, got 0",
        ));
    }
    ORDERBOOK_STREAM_DEPTHS
        .into_iter()
        .find(|tier| *tier >= depth)
        .ok_or_else(|| {
            Error::validation(format!(
                "orderbook subscription depth {depth} is above the deepest published stream tier \
                 ({}); use orderbook.get for a snapshot of up to {ORDERBOOK_SNAPSHOT_MAX_DEPTH} levels",
                deepest_stream_depth()
            ))
        })
}

/// `depth` itself when the server publishes it. Raw deltas of a tier can't be
/// trimmed, so any other depth is a validation error.
pub fn require_published_stream_depth(depth: u32) -> Result<u32> {
    if ORDERBOOK_STREAM_DEPTHS.contains(&depth) {
        return Ok(depth);
    }
    let published = ORDERBOOK_STREAM_DEPTHS
        .map(|tier| tier.to_string())
        .join(", ");
    Err(Error::validation(format!(
        "orderbook delta streams are published only at depths {published}, got {depth}; use \
         orderbook.create_subscription for a book trimmed to any depth up to {}",
        deepest_stream_depth()
    )))
}

pub fn depth_enum_for_levels(depth: u32) -> crate::proto::orderbook::v1::Depth {
    use crate::proto::orderbook::v1::Depth;
    match depth {
        0 => Depth::Depth5,
        1 => Depth::Depth1,
        2..=5 => Depth::Depth5,
        6..=10 => Depth::Depth10,
        11..=20 => Depth::Depth20,
        21..=50 => Depth::Depth50,
        51..=100 => Depth::Depth100,
        101..=200 => Depth::Depth200,
        201..=500 => Depth::Depth500,
        _ => Depth::Depth1000,
    }
}

pub fn levels_from_proto(
    levels: &[PriceLevel],
    symbol: &str,
    quantity_scale: u32,
) -> Result<Vec<OrderbookLevel>> {
    let symbol = Some(symbol.to_owned());
    levels
        .iter()
        .map(|l| {
            let price = decode_price_ticks(l.price_ticks, symbol.clone())
                .ok_or_else(|| Error::validation("orderbook level has invalid or missing price"))?;
            let qty = decode_qty_scaled(l.qty_scaled, Some(quantity_scale), symbol.clone(), None)
                .ok_or_else(|| {
                Error::validation("orderbook level has invalid or missing quantity")
            })?;
            Ok(OrderbookLevel {
                price: Some(price),
                qty: Some(qty),
            })
        })
        .collect()
}

pub fn orderbook_from_proto(
    msg: &GetOrderBookResponse,
    symbol: &str,
    depth: u32,
    quantity_scale: u32,
) -> Result<OrderbookData> {
    Ok(OrderbookData {
        symbol: symbol.to_owned(),
        depth,
        book_seq: msg.book_seq.to_string(),
        bids: levels_from_proto(&msg.bids, symbol, quantity_scale)?,
        asks: levels_from_proto(&msg.asks, symbol, quantity_scale)?,
        available: msg.available,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orderbook_maps_levels() {
        let msg = GetOrderBookResponse {
            book_seq: 7,
            bids: vec![PriceLevel {
                price_ticks: 1_000_000,
                qty_scaled: 50,
                ..Default::default()
            }],
            asks: vec![PriceLevel {
                price_ticks: 1_100_000,
                qty_scaled: 25,
                ..Default::default()
            }],
            ..Default::default()
        };
        let book = orderbook_from_proto(&msg, "ETH-USDT", 50, 8).unwrap();
        assert_eq!(book.symbol, "ETH-USDT");
        assert_eq!(book.depth, 50);
        assert_eq!(book.book_seq, "7");
        assert_eq!(book.bids[0].price.as_ref().unwrap().as_ticks(), 1_000_000);
        assert_eq!(book.asks[0].qty.as_ref().unwrap().as_scaled(), 25);
        assert!(
            !book.available,
            "a response without `available` reads false"
        );
    }

    #[test]
    fn orderbook_maps_availability() {
        let current = orderbook_from_proto(
            &GetOrderBookResponse {
                book_seq: 7,
                available: true,
                ..Default::default()
            },
            "ETH-USDT",
            50,
            8,
        )
        .unwrap();
        assert!(current.available);

        let building = orderbook_from_proto(
            &GetOrderBookResponse {
                book_seq: 7,
                available: false,
                ..Default::default()
            },
            "ETH-USDT",
            50,
            8,
        )
        .unwrap();
        assert!(!building.available);
        assert!(building.bids.is_empty() && building.asks.is_empty());
    }

    #[test]
    fn depth_mapping_preserves_protocol_boundaries() {
        use crate::proto::orderbook::v1::Depth;

        assert_eq!(depth_enum_for_levels(1), Depth::Depth1);
        assert_eq!(depth_enum_for_levels(5), Depth::Depth5);
        assert_eq!(depth_enum_for_levels(500), Depth::Depth500);
        assert_eq!(depth_enum_for_levels(1000), Depth::Depth1000);
    }

    #[test]
    fn orderbook_rejects_levels_with_missing_price_or_quantity() {
        let missing_price = GetOrderBookResponse {
            bids: vec![PriceLevel {
                price_ticks: 0,
                qty_scaled: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(orderbook_from_proto(&missing_price, "ETH-USDT", 1, 8).is_err());

        let missing_qty = GetOrderBookResponse {
            asks: vec![PriceLevel {
                price_ticks: 1,
                qty_scaled: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(orderbook_from_proto(&missing_qty, "ETH-USDT", 1, 8).is_err());
    }

    #[test]
    fn empty_orderbook_with_zero_sequence_is_success() {
        let book = orderbook_from_proto(
            &GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 0,
                ..Default::default()
            },
            "BTC-USDT",
            50,
            8,
        )
        .expect("empty book");
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());
    }
}
