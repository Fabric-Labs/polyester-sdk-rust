//! Synthesized server messages, encoded with the generated encoders.

use crate::marketstream::codec::{Fill, WireLevel};
use crate::marketstream::sbe::{
    self, Encoder, WriteBuf, best_bid_offer_codec, depth_snapshot_codec, depth_update_codec,
    market_status_codec, message_header_codec, pong_codec, subscribe_response_codec, trades_codec,
    trades_skipped_codec, trades_through_codec,
};
use crate::marketstream::{FeedState, Stream};

const HEADER: usize = message_header_codec::ENCODED_LENGTH;

pub fn buf() -> Vec<u8> {
    vec![0u8; 1 << 16]
}

pub fn fill(id: u64) -> Fill {
    Fill {
        trade_id: id,
        price: 65_000_000_000_000,
        qty: 1_500,
        side: sbe::side::Side::BUY,
    }
}

pub fn lv(price: i64, qty: i64) -> WireLevel {
    WireLevel { price, qty }
}

pub fn trades(symbol: u32, epoch: u64, update_id: u64, fills: &[Fill]) -> Vec<u8> {
    let mut b = buf();
    let mut m = trades_codec::TradesEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.book_epoch(epoch);
    m.update_id(update_id);
    m.transact_ts_ns(1_700_000_000_000_000_000);
    m.send_ts_ns(1_700_000_000_000_001_000);
    let mut g = m.entries_encoder(fills.len() as u16, Default::default());
    for f in fills {
        g.advance().unwrap();
        g.trade_id(f.trade_id);
        g.price_ticks(f.price);
        g.qty_scaled(f.qty);
        g.taker_side(f.side);
    }
    let len = g.parent().unwrap().get_limit();
    b.truncate(len);
    b
}

pub fn trades_through(symbol: u32, epoch: u64, last: u64) -> Vec<u8> {
    let mut b = buf();
    let mut m = trades_through_codec::TradesThroughEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.book_epoch(epoch);
    m.last_trade_id(last);
    m.send_ts_ns(1);
    let len = m.get_limit();
    b.truncate(len);
    b
}

pub fn trades_skipped(symbol: u32, epoch: u64, first: u64, last: u64) -> Vec<u8> {
    let mut b = buf();
    let mut m = trades_skipped_codec::TradesSkippedEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.book_epoch(epoch);
    m.first_trade_id(first);
    m.last_trade_id(last);
    m.send_ts_ns(1);
    let len = m.get_limit();
    b.truncate(len);
    b
}

pub const NO_PRICE: i64 = i64::MIN;

pub fn bbo(symbol: u32, epoch: u64, update_id: u64, bid: (i64, i64), ask: (i64, i64)) -> Vec<u8> {
    let mut b = buf();
    let mut m = best_bid_offer_codec::BestBidOfferEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.book_epoch(epoch);
    m.update_id(update_id);
    m.transact_ts_ns(1);
    m.send_ts_ns(2);
    m.bid_price_ticks(bid.0);
    m.bid_qty_scaled(bid.1);
    m.ask_price_ticks(ask.0);
    m.ask_qty_scaled(ask.1);
    let len = m.get_limit();
    b.truncate(len);
    b
}

pub fn window(stream: Stream) -> u16 {
    if stream == Stream::Depth20 { 20 } else { 1000 }
}

pub fn depth_snapshot(
    symbol: u32,
    stream: Stream,
    epoch: u64,
    update_id: u64,
    bids: &[WireLevel],
    asks: &[WireLevel],
) -> Vec<u8> {
    depth_snapshot_window(symbol, stream, epoch, update_id, window(stream), bids, asks)
}

pub fn depth_snapshot_window(
    symbol: u32,
    stream: Stream,
    epoch: u64,
    update_id: u64,
    levels_per_side: u16,
    bids: &[WireLevel],
    asks: &[WireLevel],
) -> Vec<u8> {
    let mut b = buf();
    let mut m = depth_snapshot_codec::DepthSnapshotEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.stream(stream.wire());
    m.book_epoch(epoch);
    m.update_id(update_id);
    m.transact_ts_ns(1);
    m.send_ts_ns(2);
    m.levels_per_side(levels_per_side);
    let mut g = m.bids_encoder(bids.len() as u16, Default::default());
    for l in bids {
        g.advance().unwrap();
        g.price_ticks(l.price);
        g.qty_scaled(l.qty);
    }
    let m = g.parent().unwrap();
    let mut g = m.asks_encoder(asks.len() as u16, Default::default());
    for l in asks {
        g.advance().unwrap();
        g.price_ticks(l.price);
        g.qty_scaled(l.qty);
    }
    let len = g.parent().unwrap().get_limit();
    b.truncate(len);
    b
}

pub fn depth_update(
    symbol: u32,
    epoch: u64,
    first: u64,
    last: u64,
    bids: &[WireLevel],
    asks: &[WireLevel],
) -> Vec<u8> {
    let mut b = buf();
    let mut m = depth_update_codec::DepthUpdateEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.book_epoch(epoch);
    m.first_update_id(first);
    m.last_update_id(last);
    m.transact_ts_ns(1);
    m.send_ts_ns(2);
    let mut g = m.bids_encoder(bids.len() as u16, Default::default());
    for l in bids {
        g.advance().unwrap();
        g.price_ticks(l.price);
        g.qty_scaled(l.qty);
    }
    let m = g.parent().unwrap();
    let mut g = m.asks_encoder(asks.len() as u16, Default::default());
    for l in asks {
        g.advance().unwrap();
        g.price_ticks(l.price);
        g.qty_scaled(l.qty);
    }
    let len = g.parent().unwrap().get_limit();
    b.truncate(len);
    b
}

pub fn market_status(symbol: u32, feed: FeedState) -> Vec<u8> {
    let mut b = buf();
    let mut m = market_status_codec::MarketStatusEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.symbol_id(symbol);
    m.feed(match feed {
        FeedState::Live => sbe::feed_state::FeedState::LIVE,
        FeedState::Stale => sbe::feed_state::FeedState::STALE,
        FeedState::Unknown => sbe::feed_state::FeedState::NullVal,
    });
    m.send_ts_ns(3);
    let len = m.get_limit();
    b.truncate(len);
    b
}

pub fn subscribe_response(
    request_id: u64,
    results: &[(u32, Stream, sbe::subscribe_result::SubscribeResult)],
) -> Vec<u8> {
    let mut b = buf();
    let mut m = subscribe_response_codec::SubscribeResponseEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.request_id(request_id);
    let mut g = m.results_encoder(results.len() as u16, Default::default());
    for (symbol, stream, result) in results {
        g.advance().unwrap();
        g.symbol_id(*symbol);
        g.stream(stream.wire());
        g.result(*result);
    }
    let len = g.parent().unwrap().get_limit();
    b.truncate(len);
    b
}

pub fn pong(request_id: u64) -> Vec<u8> {
    let mut b = buf();
    let mut m = pong_codec::PongEncoder::default()
        .wrap(WriteBuf::new(&mut b), HEADER)
        .header(0)
        .parent()
        .unwrap();
    m.request_id(request_id);
    m.send_ts_ns(4);
    let len = m.get_limit();
    b.truncate(len);
    b
}

fn put_u16(frame: &mut [u8], at: usize, v: u16) {
    frame[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn get_u16(frame: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([frame[at], frame[at + 1]])
}

/// Rewrites a frame's header fields.
pub fn with_header(frame: &[u8], block: u16, template: u16, schema: u16, version: u16) -> Vec<u8> {
    let mut out = frame.to_vec();
    put_u16(&mut out, 0, block);
    put_u16(&mut out, 2, template);
    put_u16(&mut out, 4, schema);
    put_u16(&mut out, 6, version);
    out
}

/// Re-encodes a frame as a later schema version would: bytes appended to the
/// fixed block and to each group entry, and a trailing group this version
/// does not know.
pub fn newer_version(
    frame: &[u8],
    extra_block: usize,
    extra_entry: usize,
    groups: usize,
) -> Vec<u8> {
    let block = get_u16(frame, 0) as usize;
    let mut out = frame[..HEADER + block].to_vec();
    out.extend(std::iter::repeat_n(0u8, extra_block));
    let mut pos = HEADER + block;
    for _ in 0..groups {
        let entry = get_u16(frame, pos) as usize;
        let count = get_u16(frame, pos + 2) as usize;
        out.extend(((entry + extra_entry) as u16).to_le_bytes());
        out.extend((count as u16).to_le_bytes());
        pos += 4;
        for _ in 0..count {
            out.extend(&frame[pos..pos + entry]);
            out.extend(std::iter::repeat_n(0u8, extra_entry));
            pos += entry;
        }
    }
    out.extend(3u16.to_le_bytes());
    out.extend(1u16.to_le_bytes());
    out.extend([7, 7, 7]);
    let (template, schema, version) = (get_u16(frame, 2), get_u16(frame, 4), get_u16(frame, 6));
    with_header(
        &out,
        (block + extra_block) as u16,
        template,
        schema,
        version + 1,
    )
}
