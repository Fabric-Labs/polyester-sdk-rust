//! Decoding server messages and encoding client messages with the generated
//! codecs.

use super::sbe::{
    self, ReadBuf, WriteBuf, best_bid_offer_codec, depth_snapshot_codec, depth_update_codec,
    group_size_encoding_codec, market_status_codec, message_header_codec, ping_codec, pong_codec,
    subscribe_codec, subscribe_response_codec, trades_codec, trades_skipped_codec,
    trades_through_codec, unsubscribe_codec,
};
use super::{Stream, StreamError, Topic};
use crate::errors::{Error, Result};

/// The schema the decoders are generated from. A message of another schema is
/// refused; one of a newer version of this schema is read by its declared
/// block lengths.
pub const SCHEMA_ID: u16 = sbe::SBE_SCHEMA_ID;
/// See [`SCHEMA_ID`].
pub const SCHEMA_VERSION: u16 = sbe::SBE_SCHEMA_VERSION;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const GROUP_HEADER: usize = group_size_encoding_codec::ENCODED_LENGTH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fill {
    pub trade_id: u64,
    pub price: i64,
    pub qty: i64,
    pub side: sbe::side::Side,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WireLevel {
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Msg {
    Trades {
        symbol_id: u32,
        epoch: u64,
        update_id: u64,
        transact_ns: u64,
        send_ns: u64,
        fills: Vec<Fill>,
    },
    TradesSkipped {
        symbol_id: u32,
        epoch: u64,
        first: u64,
        /// `None`: the rest of the epoch.
        last: Option<u64>,
        send_ns: u64,
    },
    TradesThrough {
        symbol_id: u32,
        epoch: u64,
        last: u64,
        send_ns: u64,
    },
    Bbo {
        symbol_id: u32,
        epoch: u64,
        update_id: u64,
        transact_ns: u64,
        send_ns: u64,
        bid_price: Option<i64>,
        bid_qty: i64,
        ask_price: Option<i64>,
        ask_qty: i64,
    },
    DepthSnapshot {
        symbol_id: u32,
        stream: Option<Stream>,
        epoch: u64,
        update_id: u64,
        transact_ns: u64,
        send_ns: u64,
        levels_per_side: u16,
        bids: Vec<WireLevel>,
        asks: Vec<WireLevel>,
    },
    DepthUpdate {
        symbol_id: u32,
        epoch: u64,
        first: u64,
        last: u64,
        transact_ns: u64,
        send_ns: u64,
        bids: Vec<WireLevel>,
        asks: Vec<WireLevel>,
    },
    MarketStatus {
        symbol_id: u32,
        feed: sbe::feed_state::FeedState,
        send_ns: u64,
    },
    SubscribeResponse {
        request_id: u64,
        results: Vec<(u32, Option<Stream>, sbe::subscribe_result::SubscribeResult)>,
    },
    Pong {
        request_id: u64,
        send_ns: u64,
    },
}

fn u16_at(frame: &[u8], at: usize) -> u16 {
    ReadBuf::new(frame).get_u16_at(at)
}

/// Checks that the frame holds the fixed block its header declares and every
/// repeating group in order, each entry at least as long as this version's. A
/// message of this client's version must end with its last group; a newer
/// version may append groups or data after them, which are skipped.
// At schema version 0 the version comparisons are trivially true; they
// keep their meaning once the version rises.
#[allow(clippy::absurd_extreme_comparisons)]
fn check_layout(
    frame: &[u8],
    template: u16,
    block: u16,
    version: u16,
    min_block: u16,
    groups: &[u16],
) -> Result<()> {
    let size = frame.len();
    if version >= SCHEMA_VERSION && block < min_block {
        return Err(StreamError::frame(
            template,
            format!("block length {block}, shorter than this version's {min_block}"),
        ));
    }
    let mut pos = HEADER + block as usize;
    if pos > size {
        return Err(StreamError::frame(
            template,
            format!("block length {block} past the end of a {size}-byte message"),
        ));
    }
    for (i, min_entry) in groups.iter().enumerate() {
        if pos + GROUP_HEADER > size {
            return Err(StreamError::frame(
                template,
                format!("group {} header past the end of the message", i + 1),
            ));
        }
        let entry = u16_at(frame, pos) as usize;
        let count = u16_at(frame, pos + 2) as usize;
        if entry < *min_entry as usize {
            return Err(StreamError::frame(
                template,
                format!(
                    "group {} entry length {entry}, shorter than this version's {min_entry}",
                    i + 1
                ),
            ));
        }
        pos += GROUP_HEADER + entry * count;
        if pos > size {
            return Err(StreamError::frame(
                template,
                format!(
                    "group {} of {count} entries past the end of the message",
                    i + 1
                ),
            ));
        }
    }
    if pos != size && version <= SCHEMA_VERSION {
        return Err(StreamError::frame(
            template,
            format!("{} bytes after the last group", size - pos),
        ));
    }
    Ok(())
}

/// Decodes one WebSocket binary message. `Ok(None)` is a template this client
/// does not know, which the protocol lets a newer server add.
pub(crate) fn decode(frame: &[u8]) -> Result<Option<Msg>> {
    if frame.len() < HEADER {
        return Err(StreamError::frame(
            0,
            format!("{} bytes, shorter than the message header", frame.len()),
        ));
    }
    let header = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0);
    let (schema, template, block, version) = (
        header.schema_id(),
        header.template_id(),
        header.block_length(),
        header.version(),
    );
    if schema != SCHEMA_ID {
        return Err(Error::MarketStream(StreamError::Schema {
            schema_id: schema,
            version,
        }));
    }
    // The layout is checked before any generated accessor reads, so none
    // indexes past the frame; catching a panic only guards against a
    // generator defect.
    std::panic::catch_unwind(|| decode_checked(frame, template, block, version))
        .unwrap_or_else(|_| Err(StreamError::frame(template, "undecodable message")))
}

fn decode_checked(frame: &[u8], template: u16, block: u16, version: u16) -> Result<Option<Msg>> {
    let check = |min_block: u16, groups: &[u16]| {
        check_layout(frame, template, block, version, min_block, groups)
    };
    let header =
        || message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0);
    let sbe_err = |e: sbe::SbeErr| StreamError::frame(template, e.to_string());
    let msg = match template {
        trades_codec::SBE_TEMPLATE_ID => {
            check(
                trades_codec::SBE_BLOCK_LENGTH,
                &[trades_codec::encoder::EntriesEncoder::<
                    trades_codec::TradesEncoder<'_>,
                >::block_length()],
            )?;
            let d = trades_codec::TradesDecoder::default().header(header(), 0);
            let (symbol_id, epoch, update_id, transact_ns, send_ns) = (
                d.symbol_id(),
                d.book_epoch(),
                d.update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
            );
            let mut g = d.entries_decoder();
            let mut fills = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                fills.push(Fill {
                    trade_id: g.trade_id(),
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                    side: g.taker_side(),
                });
            }
            Msg::Trades {
                symbol_id,
                epoch,
                update_id,
                transact_ns,
                send_ns,
                fills,
            }
        }
        trades_skipped_codec::SBE_TEMPLATE_ID => {
            check(trades_skipped_codec::SBE_BLOCK_LENGTH, &[])?;
            let d = trades_skipped_codec::TradesSkippedDecoder::default().header(header(), 0);
            Msg::TradesSkipped {
                symbol_id: d.symbol_id(),
                epoch: d.book_epoch(),
                first: d.first_trade_id(),
                last: d.last_trade_id(),
                send_ns: d.send_ts_ns(),
            }
        }
        trades_through_codec::SBE_TEMPLATE_ID => {
            check(trades_through_codec::SBE_BLOCK_LENGTH, &[])?;
            let d = trades_through_codec::TradesThroughDecoder::default().header(header(), 0);
            Msg::TradesThrough {
                symbol_id: d.symbol_id(),
                epoch: d.book_epoch(),
                last: d.last_trade_id(),
                send_ns: d.send_ts_ns(),
            }
        }
        best_bid_offer_codec::SBE_TEMPLATE_ID => {
            check(best_bid_offer_codec::SBE_BLOCK_LENGTH, &[])?;
            let d = best_bid_offer_codec::BestBidOfferDecoder::default().header(header(), 0);
            Msg::Bbo {
                symbol_id: d.symbol_id(),
                epoch: d.book_epoch(),
                update_id: d.update_id(),
                transact_ns: d.transact_ts_ns(),
                send_ns: d.send_ts_ns(),
                bid_price: d.bid_price_ticks(),
                bid_qty: d.bid_qty_scaled(),
                ask_price: d.ask_price_ticks(),
                ask_qty: d.ask_qty_scaled(),
            }
        }
        depth_snapshot_codec::SBE_TEMPLATE_ID => {
            use depth_snapshot_codec::encoder::{AsksEncoder, BidsEncoder, DepthSnapshotEncoder};
            check(
                depth_snapshot_codec::SBE_BLOCK_LENGTH,
                &[
                    BidsEncoder::<DepthSnapshotEncoder<'_>>::block_length(),
                    AsksEncoder::<DepthSnapshotEncoder<'_>>::block_length(),
                ],
            )?;
            let d = depth_snapshot_codec::DepthSnapshotDecoder::default().header(header(), 0);
            let (symbol_id, stream, epoch, update_id, transact_ns, send_ns, levels_per_side) = (
                d.symbol_id(),
                Stream::from_wire(d.stream()),
                d.book_epoch(),
                d.update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
                d.levels_per_side(),
            );
            let mut g = d.bids_decoder();
            let mut bids = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                bids.push(WireLevel {
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                });
            }
            let d = g.parent().map_err(sbe_err)?;
            let mut g = d.asks_decoder();
            let mut asks = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                asks.push(WireLevel {
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                });
            }
            Msg::DepthSnapshot {
                symbol_id,
                stream,
                epoch,
                update_id,
                transact_ns,
                send_ns,
                levels_per_side,
                bids,
                asks,
            }
        }
        depth_update_codec::SBE_TEMPLATE_ID => {
            use depth_update_codec::encoder::{AsksEncoder, BidsEncoder, DepthUpdateEncoder};
            check(
                depth_update_codec::SBE_BLOCK_LENGTH,
                &[
                    BidsEncoder::<DepthUpdateEncoder<'_>>::block_length(),
                    AsksEncoder::<DepthUpdateEncoder<'_>>::block_length(),
                ],
            )?;
            let d = depth_update_codec::DepthUpdateDecoder::default().header(header(), 0);
            let (symbol_id, epoch, first, last, transact_ns, send_ns) = (
                d.symbol_id(),
                d.book_epoch(),
                d.first_update_id(),
                d.last_update_id(),
                d.transact_ts_ns(),
                d.send_ts_ns(),
            );
            let mut g = d.bids_decoder();
            let mut bids = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                bids.push(WireLevel {
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                });
            }
            let d = g.parent().map_err(sbe_err)?;
            let mut g = d.asks_decoder();
            let mut asks = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                asks.push(WireLevel {
                    price: g.price_ticks(),
                    qty: g.qty_scaled(),
                });
            }
            Msg::DepthUpdate {
                symbol_id,
                epoch,
                first,
                last,
                transact_ns,
                send_ns,
                bids,
                asks,
            }
        }
        market_status_codec::SBE_TEMPLATE_ID => {
            check(market_status_codec::SBE_BLOCK_LENGTH, &[])?;
            let d = market_status_codec::MarketStatusDecoder::default().header(header(), 0);
            Msg::MarketStatus {
                symbol_id: d.symbol_id(),
                feed: d.feed(),
                send_ns: d.send_ts_ns(),
            }
        }
        subscribe_response_codec::SBE_TEMPLATE_ID => {
            use subscribe_response_codec::encoder::{ResultsEncoder, SubscribeResponseEncoder};
            check(
                subscribe_response_codec::SBE_BLOCK_LENGTH,
                &[ResultsEncoder::<SubscribeResponseEncoder<'_>>::block_length()],
            )?;
            let d =
                subscribe_response_codec::SubscribeResponseDecoder::default().header(header(), 0);
            let request_id = d.request_id();
            let mut g = d.results_decoder();
            let mut results = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(sbe_err)?.is_some() {
                results.push((g.symbol_id(), Stream::from_wire(g.stream()), g.result()));
            }
            Msg::SubscribeResponse {
                request_id,
                results,
            }
        }
        pong_codec::SBE_TEMPLATE_ID => {
            check(pong_codec::SBE_BLOCK_LENGTH, &[])?;
            let d = pong_codec::PongDecoder::default().header(header(), 0);
            Msg::Pong {
                request_id: d.request_id(),
                send_ns: d.send_ts_ns(),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(msg))
}

/// Most topics in one Subscribe or Unsubscribe, which keeps it well within the
/// server's inbound message limit.
pub(crate) const MAX_ITEMS: usize = 128;

fn encode_items(request_id: u64, topics: &[Topic], unsubscribe: bool) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + 8 + GROUP_HEADER + 5 * topics.len()];
    let len = if unsubscribe {
        let mut m = unsubscribe_codec::UnsubscribeEncoder::default()
            .wrap(WriteBuf::new(&mut buf), HEADER)
            .header(0)
            .parent()
            .expect("header parent");
        m.request_id(request_id);
        let mut items = m.items_encoder(topics.len() as u16, Default::default());
        for t in topics {
            items.advance().expect("item");
            items.symbol_id(t.symbol_id);
            items.stream(t.stream.wire());
        }
        let m = items.parent().expect("items parent");
        sbe::Encoder::get_limit(&m)
    } else {
        let mut m = subscribe_codec::SubscribeEncoder::default()
            .wrap(WriteBuf::new(&mut buf), HEADER)
            .header(0)
            .parent()
            .expect("header parent");
        m.request_id(request_id);
        let mut items = m.items_encoder(topics.len() as u16, Default::default());
        for t in topics {
            items.advance().expect("item");
            items.symbol_id(t.symbol_id);
            items.stream(t.stream.wire());
        }
        let m = items.parent().expect("items parent");
        sbe::Encoder::get_limit(&m)
    };
    buf.truncate(len);
    buf
}

pub(crate) fn encode_subscribe(request_id: u64, topics: &[Topic]) -> Vec<u8> {
    encode_items(request_id, topics, false)
}

pub(crate) fn encode_unsubscribe(request_id: u64, topics: &[Topic]) -> Vec<u8> {
    encode_items(request_id, topics, true)
}

pub(crate) fn encode_ping(request_id: u64) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + ping_codec::SBE_BLOCK_LENGTH as usize];
    let mut m = ping_codec::PingEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .expect("header parent");
    m.request_id(request_id);
    buf
}
