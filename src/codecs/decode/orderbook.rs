//! Orderbook snapshot decoders.

use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::models::{OrderbookData, OrderbookLevel};
use crate::proto::orderbook::v1::{GetOrderBookResponse, PriceLevel};

pub fn depth_enum_for_levels(depth: u32) -> crate::proto::orderbook::v1::Depth {
    use crate::proto::orderbook::v1::Depth;
    match depth {
        0..=5 => Depth::Depth5,
        6..=10 => Depth::Depth10,
        11..=20 => Depth::Depth20,
        21..=50 => Depth::Depth50,
        51..=100 => Depth::Depth100,
        101..=200 => Depth::Depth200,
        _ => Depth::Depth500,
    }
}

pub fn levels_from_proto(
    levels: &[PriceLevel],
    symbol: &str,
    quantity_scale: u32,
) -> Vec<OrderbookLevel> {
    let symbol = Some(symbol.to_owned());
    levels
        .iter()
        .map(|l| OrderbookLevel {
            price: decode_price_ticks(l.price_ticks, symbol.clone()),
            qty: decode_qty_scaled(l.qty_scaled, Some(quantity_scale), symbol.clone(), None),
        })
        .collect()
}

pub fn orderbook_from_proto(
    msg: &GetOrderBookResponse,
    symbol: &str,
    depth: u32,
    quantity_scale: u32,
) -> OrderbookData {
    OrderbookData {
        symbol: symbol.to_owned(),
        depth,
        book_seq: if msg.book_seq == 0 {
            String::new()
        } else {
            msg.book_seq.to_string()
        },
        bids: levels_from_proto(&msg.bids, symbol, quantity_scale),
        asks: levels_from_proto(&msg.asks, symbol, quantity_scale),
    }
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
        let book = orderbook_from_proto(&msg, "ETH-USDT", 50, 8);
        assert_eq!(book.symbol, "ETH-USDT");
        assert_eq!(book.depth, 50);
        assert_eq!(book.book_seq, "7");
        assert_eq!(book.bids[0].price.as_ref().unwrap().as_ticks(), 1_000_000);
        assert_eq!(book.asks[0].qty.as_ref().unwrap().as_scaled(), 25);
    }
}
