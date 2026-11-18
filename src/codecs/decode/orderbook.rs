//! Orderbook snapshot decoders.

use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{OrderbookData, OrderbookLevel};
use crate::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};
use crate::proto::polyester::r#type::v1::DepthTier;

/// Depths the server publishes on the delta stream. It also accepts a
/// subscribe at any other depth, and then never sends anything on it.
pub const ORDERBOOK_STREAM_DEPTHS: [u32; 5] = [1, 20, 50, 200, 500];

/// Depth per side a snapshot request with no depth tier receives.
pub const ORDERBOOK_SNAPSHOT_DEFAULT_DEPTH: u32 = 50;

/// The contract's depth tiers (`polyester.type.v1.DepthTier`), in levels per
/// side, ascending. A snapshot serves every tier.
pub const DEPTH_TIERS: [u32; 10] = [1, 5, 10, 20, 50, 100, 200, 500, 1000, 5000];

/// The depth tier of exactly `levels` levels per side; `None` for a count
/// that is no tier.
pub fn depth_tier_for_levels(levels: u32) -> Option<DepthTier> {
    Some(match levels {
        1 => DepthTier::DEPTH_1,
        5 => DepthTier::DEPTH_5,
        10 => DepthTier::DEPTH_10,
        20 => DepthTier::DEPTH_20,
        50 => DepthTier::DEPTH_50,
        100 => DepthTier::DEPTH_100,
        200 => DepthTier::DEPTH_200,
        500 => DepthTier::DEPTH_500,
        1000 => DepthTier::DEPTH_1000,
        5000 => DepthTier::DEPTH_5000,
        _ => return None,
    })
}

/// The levels per side of `tier`; `None` for `DEPTH_UNSPECIFIED` and a tier
/// this SDK does not name.
pub fn depth_tier_levels(tier: &buffa::EnumValue<DepthTier>) -> Option<u32> {
    Some(match tier.as_known()? {
        DepthTier::DEPTH_UNSPECIFIED => return None,
        DepthTier::DEPTH_1 => 1,
        DepthTier::DEPTH_5 => 5,
        DepthTier::DEPTH_10 => 10,
        DepthTier::DEPTH_20 => 20,
        DepthTier::DEPTH_50 => 50,
        DepthTier::DEPTH_100 => 100,
        DepthTier::DEPTH_200 => 200,
        DepthTier::DEPTH_500 => 500,
        DepthTier::DEPTH_1000 => 1000,
        DepthTier::DEPTH_5000 => 5000,
    })
}

/// The depth a snapshot of `depth` levels per side asks for: `depth` itself,
/// or [`ORDERBOOK_SNAPSHOT_DEFAULT_DEPTH`] for 0, which sends no tier.
pub fn orderbook_snapshot_depth(depth: u32) -> u32 {
    if depth == 0 {
        ORDERBOOK_SNAPSHOT_DEFAULT_DEPTH
    } else {
        depth
    }
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
                 ({}); use orderbook.get for a deeper snapshot",
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

/// Decode a snapshot asked at `depth` levels per side. Its `depth` is the
/// tier the server says it served, or `depth` when it names none.
pub fn orderbook_from_proto(
    msg: &GetOrderBookResponse,
    symbol: &str,
    depth: u32,
    quantity_scale: u32,
) -> Result<OrderbookData> {
    Ok(OrderbookData {
        symbol: symbol.to_owned(),
        depth: depth_tier_levels(&msg.depth).unwrap_or(depth),
        book_seq: msg.book_seq.to_string(),
        book_epoch: msg.book_epoch.to_string(),
        bids: levels_from_proto(&msg.bids, symbol, quantity_scale)?,
        asks: levels_from_proto(&msg.asks, symbol, quantity_scale)?,
        available: msg.available,
    })
}

/// Orders two [`OrderbookData`] snapshots from `orderbook.get` by
/// `(book_epoch, book_seq)`: a higher `book_epoch` is newer whatever its
/// `book_seq`, since a new epoch may restart the sequence, and sequences are
/// compared only within one epoch. Keep the newest snapshot seen for a symbol
/// and discard one that orders before it. An unavailable snapshot has epoch 0,
/// so it orders before every available one of a later epoch.
///
/// A view of a managed [`crate::orderbook::Subscription`] is not a snapshot:
/// its `book_epoch` is empty, and comparing it, or a `book_epoch` or
/// `book_seq` that is not a decimal u64, is a validation error.
pub fn compare_snapshots(a: &OrderbookData, b: &OrderbookData) -> Result<std::cmp::Ordering> {
    Ok(snapshot_position(a)?.cmp(&snapshot_position(b)?))
}

fn snapshot_position(book: &OrderbookData) -> Result<(u64, u64)> {
    let parse = |value: &str, field: &str| {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::validation(format!(
                "orderbook snapshot {field} must be a decimal u64 from orderbook.get, got {value:?}"
            )));
        }
        value.parse::<u64>().map_err(|_| {
            Error::validation(format!(
                "orderbook snapshot {field} must be a decimal u64 from orderbook.get, got {value:?}"
            ))
        })
    };
    Ok((
        parse(&book.book_epoch, "book_epoch")?,
        parse(&book.book_seq, "book_seq")?,
    ))
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
    fn snapshot_depth_is_sent_as_is_and_zero_reports_the_default() {
        assert_eq!(orderbook_snapshot_depth(0), 50);
        for depth in [1, 2, 7, 50, 1000, 2500, 5000, 6000, u32::MAX] {
            assert_eq!(orderbook_snapshot_depth(depth), depth);
        }
    }

    /// A snapshot reports the tier the server served, or the depth asked for
    /// when it names none.
    #[test]
    fn orderbook_maps_epoch_and_served_tier() {
        let served = |tier: buffa::EnumValue<DepthTier>| {
            orderbook_from_proto(
                &GetOrderBookResponse {
                    book_seq: 984_321,
                    book_epoch: u64::MAX,
                    depth: tier,
                    available: true,
                    ..Default::default()
                },
                "BTC-USDT",
                50,
                8,
            )
            .unwrap()
        };
        let book = served(DepthTier::DEPTH_5000.into());
        assert_eq!(book.book_epoch, u64::MAX.to_string());
        assert_eq!(book.depth, 5000);
        assert_eq!(book.book_seq, "984321");
        assert_eq!(served(DepthTier::DEPTH_UNSPECIFIED.into()).depth, 50);
        assert_eq!(served(buffa::EnumValue::from(42)).depth, 50);
    }

    /// Every tier maps to its level count and back; a count that is no tier
    /// has none.
    #[test]
    fn depth_tiers_round_trip() {
        for levels in DEPTH_TIERS {
            let tier = depth_tier_for_levels(levels).expect("a tier");
            assert_eq!(depth_tier_levels(&tier.into()), Some(levels));
        }
        for levels in [0, 2, 15, 21, 501, 2500, 5001, u32::MAX] {
            assert_eq!(depth_tier_for_levels(levels), None, "{levels}");
        }
    }

    fn snapshot(epoch: &str, seq: &str) -> OrderbookData {
        OrderbookData {
            symbol: "BTC-USDT".into(),
            depth: 50,
            book_seq: seq.into(),
            book_epoch: epoch.into(),
            bids: vec![],
            asks: vec![],
            available: true,
        }
    }

    #[test]
    fn snapshots_order_by_epoch_then_sequence() {
        use std::cmp::Ordering;
        // A newer epoch wins even when it restarted the sequence.
        assert_eq!(
            compare_snapshots(&snapshot("4", "1"), &snapshot("3", "984321")).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_snapshots(&snapshot("3", "984321"), &snapshot("4", "1")).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_snapshots(&snapshot("3", "10"), &snapshot("3", "9")).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_snapshots(&snapshot("3", "9"), &snapshot("3", "9")).unwrap(),
            Ordering::Equal
        );
        // Compared as integers, not text.
        assert_eq!(
            compare_snapshots(&snapshot("10", "1"), &snapshot("9", "1")).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_snapshots(&snapshot("18446744073709551615", "0"), &snapshot("1", "1")).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn comparing_a_managed_view_or_malformed_position_is_refused() {
        for (epoch, seq) in [
            ("", "1"),
            ("1", ""),
            ("-1", "1"),
            ("+1", "1"),
            (" 1", "1"),
            ("1", "x"),
            ("18446744073709551616", "1"),
        ] {
            let err = compare_snapshots(&snapshot(epoch, seq), &snapshot("1", "1")).unwrap_err();
            assert!(
                matches!(err, Error::Validation(_)),
                "{epoch:?}/{seq:?}: {err:?}"
            );
            let err = compare_snapshots(&snapshot("1", "1"), &snapshot(epoch, seq)).unwrap_err();
            assert!(
                matches!(err, Error::Validation(_)),
                "{epoch:?}/{seq:?}: {err:?}"
            );
        }
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
