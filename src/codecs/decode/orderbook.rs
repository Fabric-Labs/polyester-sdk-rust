//! Orderbook snapshot decoders.

use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{OrderbookData, OrderbookLevel};
use crate::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};

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
        book_seq: if msg.book_seq == 0 {
            String::new()
        } else {
            msg.book_seq.to_string()
        },
        bids: levels_from_proto(&msg.bids, symbol, quantity_scale)?,
        asks: levels_from_proto(&msg.asks, symbol, quantity_scale)?,
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
}
