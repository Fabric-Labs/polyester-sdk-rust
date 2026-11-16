//! Encoding commands and decoding server messages with the generated codecs.
//!
//! The generated decoders index the buffer without bounds checks, so every
//! frame is checked against its header, fixed block, groups and data before a
//! generated accessor reads a byte. The server speaks exactly one version of
//! the schema; a frame of any other version is refused rather than guessed at.

use std::time::Duration;

use super::sbe::{
    self, ReadBuf, WriteBuf, batch_cancel_ack_codec, batch_cancel_orders_codec,
    batch_create_ack_codec, batch_create_orders_codec, batch_replace_ack_codec,
    batch_replace_orders_codec, cancel_all_after_ack_codec, cancel_all_after_codec,
    cancel_all_after_status, cancel_all_orders_codec, cancel_order_codec, command_ack_codec,
    command_reject_codec, create_order_codec, event_channels, execution_event_codec, fee_asset,
    group_size_encoding_codec, liquidity_role, market_resync_codec, message_header_codec,
    modify_behavior, modify_order_codec, order_event_codec, order_flags, order_sizing_kind,
    order_type, ping_codec, pong_codec, settlement_status, side, stp_mode, subscribe_codec,
    subscription_ack_codec, time_in_force,
};
use super::{
    BatchCancelAck, BatchCancelOrders, BatchCreateAck, BatchCreateOrders, BatchItem,
    BatchReplaceAck, BatchReplaceOrders, BatchStatus, CancelAllAck, CancelAllListing,
    CancelAllOrders, CancelReason, CancelledOrder, Deadline, Execution, FeeAsset, MarketResync,
    ModifyBehavior, ModifyOrder, OrderAck, OrderRef, OrderStatus, OrderUpdate, PostOnlyOrder,
    RejectCode, ReplaceAction, ReplaceItemResult, ResyncReason, Side, TimerAck, TradeError,
    from_unix_ns,
};
use crate::errors::{Error, Result};
use crate::types::{Price, Quantity, QuantityDomain};

/// The schema the codecs are generated from; the server sends and accepts
/// exactly this schema and version.
pub const SCHEMA_ID: u16 = sbe::SBE_SCHEMA_ID;
/// See [`SCHEMA_ID`].
pub const SCHEMA_VERSION: u16 = sbe::SBE_SCHEMA_VERSION;

pub(crate) const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const GROUP_HEADER: usize = group_size_encoding_codec::ENCODED_LENGTH;
const VAR_HEADER: usize = 4;
const NULL_I64: i64 = i64::MIN;
const NULL_I32: i32 = i32::MIN;
const MAX_ID: usize = super::MAX_CLIENT_ORDER_ID_LEN;
/// More than any batch item's fixed block: an allocation bound only, the
/// frame is cut to its encoded length.
const ITEM_BOUND: usize = 128;

/// The channels the session subscribes to: executions, orders and
/// marketResync.
pub(crate) const CHANNELS: u32 = 0b111;

// Offsets of fields read or written as raw values, from the start of their
// block or group entry: an enum whose value this client may not name keeps its
// number, and the generated Rust enums spell a `ZeroNullUint8` enum's null as
// 0xff where the schema says 0, so an absent side is written as 0 here.
// `tests::codec` checks each against the generated encoders.
pub(crate) const CANCEL_ALL_SIDE: usize = 16;
pub(crate) const CANCEL_ALL_AFTER_SIDE: usize = 24;
pub(crate) const REJECT_CODE: usize = 16;
pub(crate) const ACK_LISTING: usize = 66;
pub(crate) const ORDER_STATUS: usize = 16;
pub(crate) const ORDER_REJECT_REASON: usize = 58;
pub(crate) const ORDER_CANCEL_REASON: usize = 60;
pub(crate) const EXECUTION_FEE_ASSET: usize = 30;
pub(crate) const RESYNC_REASON: usize = 4;
pub(crate) const REPLACE_ACK_STATUS: usize = 32;
pub(crate) const ITEM_CODE: usize = 1;
pub(crate) const REPLACE_ITEM_ACTION: usize = 3;

/// A frame's `seq`: the first field of every client message's block.
const SEQ: usize = HEADER;

/// Sets the `seq` of an encoded client frame.
pub(crate) fn set_seq(frame: &mut [u8], seq: u64) {
    frame[SEQ..SEQ + 8].copy_from_slice(&seq.to_le_bytes());
}

fn wire_side(s: Side) -> side::Side {
    match s {
        Side::Buy => side::Side::BUY,
        Side::Sell => side::Side::SELL,
    }
}

fn post_only() -> order_flags::OrderFlags {
    let mut flags = order_flags::OrderFlags::default();
    flags.set_post_only(true);
    flags
}

fn count(n: usize) -> u16 {
    u16::try_from(n).expect("batch sizes are checked before encoding")
}

/// The wire form of an order reference: an order ID or a client order ID,
/// the other zero or empty.
fn split(order: &OrderRef) -> (u64, &[u8]) {
    match order {
        OrderRef::Id(id) => (*id, b""),
        OrderRef::Client(id) => (0, id.as_bytes()),
    }
}

fn behavior(b: ModifyBehavior) -> modify_behavior::ModifyBehavior {
    match b {
        ModifyBehavior::AmendOrReplace => modify_behavior::ModifyBehavior::AMEND_OR_REPLACE,
        ModifyBehavior::AmendOnly => modify_behavior::ModifyBehavior::AMEND_ONLY,
        ModifyBehavior::ReplaceOnly => modify_behavior::ModifyBehavior::REPLACE_ONLY,
    }
}

fn frame(capacity: usize) -> Vec<u8> {
    vec![0u8; HEADER + capacity]
}

macro_rules! encoder {
    ($codec:ident :: $encoder:ident, $buf:expr) => {
        $codec::$encoder::default()
            .wrap(WriteBuf::new($buf), HEADER)
            .header(0)
            .parent()
            .expect("the header encoder holds its parent")
    };
}

pub(crate) fn encode_ping(seq: u64) -> Vec<u8> {
    let mut buf = frame(ping_codec::SBE_BLOCK_LENGTH as usize + VAR_HEADER);
    let mut e = encoder!(ping_codec::PingEncoder, &mut buf);
    e.seq(seq);
    e.correlation_id(0);
    e.payload(b"");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    buf
}

pub(crate) fn encode_subscribe(seq: u64) -> Vec<u8> {
    let mut buf = frame(subscribe_codec::SBE_BLOCK_LENGTH as usize);
    let mut e = encoder!(subscribe_codec::SubscribeEncoder, &mut buf);
    e.seq(seq);
    e.correlation_id(0);
    e.channels(event_channels::EventChannels(CHANNELS));
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    buf
}

/// A post-only GTC limit order's fields, shared by `CreateOrder` and a
/// `BatchCreateOrders` item; both encoders have the same setters.
macro_rules! order_fields {
    ($e:expr, $order:expr, $price:expr, $qty:expr) => {
        $e.symbol_id($order.symbol_id);
        $e.side(wire_side($order.side));
        $e.order_type(order_type::OrderType::LIMIT);
        $e.time_in_force(time_in_force::TimeInForce::GTC);
        $e.base_qty_scaled($qty);
        $e.price_ticks($price);
        $e.flags(post_only());
        $e.fee_asset(fee_asset::FeeAsset::QUOTE);
        $e.stp_mode(stp_mode::STPMode::EXPIRE_MAKER);
        $e.market_client_ref_price_ticks(NULL_I64);
        $e.market_max_slippage_ticks(NULL_I64);
        $e.market_max_slippage_bps(NULL_I32);
        $e.sizing_kind(order_sizing_kind::OrderSizingKind::BASE_QUANTITY);
        $e.quote_qty_scaled(NULL_I64);
        $e.expire_at_ts_ns(NULL_I64);
        $e.display_qty_scaled(0);
        $e.peg_reference(0);
        $e.peg_offset_ticks(0);
    };
}

/// Encodes a checked order with `seq` 0; [`set_seq`] sets it when sent.
pub(crate) fn encode_create(
    order: &PostOnlyOrder,
    correlation_id: u64,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    let (price, qty) = order.check()?;
    let mut buf = frame(create_order_codec::SBE_BLOCK_LENGTH as usize + VAR_HEADER + MAX_ID);
    let mut e = encoder!(create_order_codec::CreateOrderEncoder, &mut buf);
    e.seq(0);
    e.correlation_id(correlation_id);
    order_fields!(e, order, price, qty);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    e.client_order_id(order.client_order_id.as_bytes());
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

pub(crate) fn encode_batch_create(
    batch: &BatchCreateOrders,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    batch.check()?;
    let mut buf = frame(
        batch_create_orders_codec::SBE_BLOCK_LENGTH as usize
            + GROUP_HEADER
            + batch.orders.len() * (ITEM_BOUND + VAR_HEADER + MAX_ID),
    );
    let mut e = encoder!(
        batch_create_orders_codec::BatchCreateOrdersEncoder,
        &mut buf
    );
    e.seq(0);
    e.correlation_id(batch.correlation_id);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    let mut g = e.items_encoder(count(batch.orders.len()), Default::default());
    for order in &batch.orders {
        let (price, qty) = order.check()?;
        g.advance().expect("the group encoder holds its parent");
        order_fields!(g, order, price, qty);
        g.client_order_id(order.client_order_id.as_bytes());
    }
    let e = g.parent().expect("the group encoder holds its parent");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

fn opt_qty(qty: Option<&Quantity>) -> i64 {
    qty.map_or(NULL_I64, Quantity::as_scaled)
}

fn opt_price(price: Option<&Price>) -> i64 {
    price.map_or(NULL_I64, Price::as_ticks)
}

pub(crate) fn encode_modify(modify: &ModifyOrder, deadline: &Deadline) -> Result<Vec<u8>> {
    modify.check()?;
    let mut buf = frame(modify_order_codec::SBE_BLOCK_LENGTH as usize + 2 * (VAR_HEADER + MAX_ID));
    let mut e = encoder!(modify_order_codec::ModifyOrderEncoder, &mut buf);
    e.seq(0);
    e.correlation_id(modify.correlation_id);
    let (order_id, client) = split(&modify.order);
    e.order_id(order_id);
    e.symbol_id(modify.symbol_id);
    e.new_qty_scaled(opt_qty(modify.qty.as_ref()));
    e.price_ticks(opt_price(modify.price.as_ref()));
    e.behavior(behavior(modify.behavior));
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    e.client_order_id(client);
    e.new_client_order_id(b"");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

pub(crate) fn encode_batch_replace(
    batch: &BatchReplaceOrders,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    batch.check()?;
    let mut buf = frame(
        batch_replace_orders_codec::SBE_BLOCK_LENGTH as usize
            + GROUP_HEADER
            + batch.items.len() * (ITEM_BOUND + 2 * (VAR_HEADER + MAX_ID)),
    );
    let mut e = encoder!(
        batch_replace_orders_codec::BatchReplaceOrdersEncoder,
        &mut buf
    );
    e.seq(0);
    e.correlation_id(batch.correlation_id);
    e.symbol_id(batch.symbol_id);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    let mut g = e.items_encoder(count(batch.items.len()), Default::default());
    for item in &batch.items {
        g.advance().expect("the group encoder holds its parent");
        let (order_id, client) = split(&item.order);
        g.order_id(order_id);
        g.new_qty_scaled(opt_qty(item.qty.as_ref()));
        g.price_ticks(opt_price(item.price.as_ref()));
        g.client_order_id(client);
        g.new_client_order_id(b"");
    }
    let e = g.parent().expect("the group encoder holds its parent");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

pub(crate) fn encode_cancel(
    symbol_id: u32,
    order: &OrderRef,
    correlation_id: u64,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    super::check_symbol(symbol_id)?;
    super::check_ref(order)?;
    let mut buf = frame(cancel_order_codec::SBE_BLOCK_LENGTH as usize + VAR_HEADER + MAX_ID);
    let mut e = encoder!(cancel_order_codec::CancelOrderEncoder, &mut buf);
    e.seq(0);
    e.correlation_id(correlation_id);
    let (order_id, client) = split(order);
    e.order_id(order_id);
    e.symbol_id(symbol_id);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    e.client_order_id(client);
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

pub(crate) fn encode_batch_cancel(
    batch: &BatchCancelOrders,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    batch.check()?;
    let mut buf = frame(
        batch_cancel_orders_codec::SBE_BLOCK_LENGTH as usize
            + GROUP_HEADER
            + batch.items.len() * (ITEM_BOUND + VAR_HEADER + MAX_ID),
    );
    let mut e = encoder!(
        batch_cancel_orders_codec::BatchCancelOrdersEncoder,
        &mut buf
    );
    e.seq(0);
    e.correlation_id(batch.correlation_id);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    let mut g = e.items_encoder(count(batch.items.len()), Default::default());
    for item in &batch.items {
        g.advance().expect("the group encoder holds its parent");
        let (order_id, client) = split(&item.order);
        g.order_id(order_id);
        g.symbol_id(item.symbol_id);
        g.client_order_id(client);
    }
    let e = g.parent().expect("the group encoder holds its parent");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    Ok(buf)
}

pub(crate) fn encode_cancel_all(cancel: &CancelAllOrders, deadline: &Deadline) -> Result<Vec<u8>> {
    cancel.check()?;
    let mut buf = frame(
        cancel_all_orders_codec::SBE_BLOCK_LENGTH as usize
            + GROUP_HEADER
            + 4 * cancel.symbol_ids.len(),
    );
    let mut e = encoder!(cancel_all_orders_codec::CancelAllOrdersEncoder, &mut buf);
    e.seq(0);
    e.correlation_id(cancel.correlation_id);
    if let Some(s) = cancel.side {
        e.side(wire_side(s));
    }
    e.dry_run(0);
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    let mut g = e.symbols_encoder(count(cancel.symbol_ids.len()), Default::default());
    for symbol_id in &cancel.symbol_ids {
        g.advance().expect("the group encoder holds its parent");
        g.symbol_id(*symbol_id);
    }
    let e = g.parent().expect("the group encoder holds its parent");
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    // Both sides: the schema's null, 0 (the buffer starts zeroed).
    debug_assert!(cancel.side.is_some() || buf[HEADER + CANCEL_ALL_SIDE] == 0);
    Ok(buf)
}

pub(crate) fn encode_cancel_all_after(
    timeout: Duration,
    correlation_id: u64,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    let timeout_sec = super::timer_seconds(timeout)?;
    let mut buf = frame(cancel_all_after_codec::SBE_BLOCK_LENGTH as usize);
    let mut e = encoder!(cancel_all_after_codec::CancelAllAfterEncoder, &mut buf);
    e.seq(0);
    e.correlation_id(correlation_id);
    e.timeout_sec(timeout_sec);
    e.symbol_id(0);
    // Every pair and both sides: symbolId 0 and side left at the schema's
    // null, 0 (the buffer starts zeroed).
    e.timestamp_ms(deadline.timestamp_ms);
    e.recv_window_ms(deadline.window_ms);
    let len = HEADER + e.encoded_length();
    buf.truncate(len);
    debug_assert_eq!(buf[HEADER + CANCEL_ALL_AFTER_SIDE], 0);
    Ok(buf)
}

/// A server message this client reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Msg {
    /// The answer to the command sent with `seq`.
    Answer {
        seq: u64,
        answer: Answer,
    },
    Pong {
        seq: u64,
    },
    SubscriptionAck {
        seq: u64,
        channels: u32,
    },
    Order(OrderUpdate),
    Execution(Execution),
    MarketResync(MarketResync),
}

/// The server's answer to a command.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Answer {
    /// A `CommandAck` of a command other than `CANCEL_ALL_ORDERS`, with its
    /// command type's raw value.
    Order {
        command_type: u8,
        ack: OrderAck,
    },
    CancelAll(CancelAllAck),
    Timer(TimerAck),
    BatchCreate(BatchCreateAck),
    BatchReplace(BatchReplaceAck),
    BatchCancel(BatchCancelAck),
    Reject(RejectFrame),
}

/// A `CommandReject`, before it is matched to its command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectFrame {
    pub correlation_id: u64,
    pub code: RejectCode,
    pub detail: String,
    pub retry_after: Option<Duration>,
    pub existing_order_id: Option<u64>,
    pub ts_ns: u64,
}

struct Header {
    block_length: u16,
    template_id: u16,
}

fn short(template_id: u16, need: usize, have: usize) -> Error {
    TradeError::frame(
        template_id,
        format!("a {have}-byte message, shorter than the {need} bytes it declares"),
    )
}

fn header(frame: &[u8]) -> Result<Header> {
    if frame.len() < HEADER {
        return Err(short(0, HEADER, frame.len()));
    }
    let h = header_decoder(frame);
    if h.schema_id() != SCHEMA_ID || h.version() != SCHEMA_VERSION {
        return Err(Error::TradeSession(TradeError::Schema {
            schema_id: h.schema_id(),
            version: h.version(),
        }));
    }
    Ok(Header {
        block_length: h.block_length(),
        template_id: h.template_id(),
    })
}

fn header_decoder(frame: &[u8]) -> message_header_codec::MessageHeaderDecoder<ReadBuf<'_>> {
    message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(frame), 0)
}

fn u16_at(frame: &[u8], at: usize) -> u16 {
    ReadBuf::new(frame).get_u16_at(at)
}

fn u32_at(frame: &[u8], at: usize) -> usize {
    ReadBuf::new(frame).get_u32_at(at) as usize
}

/// A repeating group's shape: its least entry length and the variable-length
/// fields each entry carries after its fixed part.
struct Group {
    min_entry: u16,
    var_fields: usize,
}

/// A checked group: where its header starts, its entry length and count.
#[derive(Clone, Copy)]
struct Shape {
    at: usize,
    entry: usize,
    count: usize,
}

impl Shape {
    /// The start of entry `i` of a group without variable-length data.
    fn entry_at(&self, i: usize) -> usize {
        self.at + GROUP_HEADER + i * self.entry
    }
}

/// Checks that the frame holds the fixed block, each group with its entries'
/// data, then `trailing` variable-length fields, and ends there, before any
/// generated accessor reads a byte.
fn check_layout(
    frame: &[u8],
    h: &Header,
    min_block: u16,
    groups: &[Group],
    trailing: usize,
) -> Result<Vec<Shape>> {
    let t = h.template_id;
    if h.block_length < min_block {
        return Err(TradeError::frame(
            t,
            format!(
                "block length {}, shorter than this version's {min_block}",
                h.block_length
            ),
        ));
    }
    let need = |at: usize| {
        if frame.len() < at {
            Err(short(t, at, frame.len()))
        } else {
            Ok(())
        }
    };
    let mut at = HEADER + h.block_length as usize;
    need(at)?;
    let mut shapes = Vec::with_capacity(groups.len());
    for group in groups {
        need(at + GROUP_HEADER)?;
        let entry = u16_at(frame, at);
        let n = u16_at(frame, at + 2) as usize;
        if entry < group.min_entry {
            return Err(TradeError::frame(
                t,
                format!(
                    "group entry length {entry}, shorter than this version's {}",
                    group.min_entry
                ),
            ));
        }
        shapes.push(Shape {
            at,
            entry: entry as usize,
            count: n,
        });
        at += GROUP_HEADER;
        for _ in 0..n {
            at += entry as usize;
            need(at)?;
            for _ in 0..group.var_fields {
                need(at + VAR_HEADER)?;
                at += VAR_HEADER + u32_at(frame, at);
                need(at)?;
            }
        }
    }
    for _ in 0..trailing {
        need(at + VAR_HEADER)?;
        at += VAR_HEADER + u32_at(frame, at);
        need(at)?;
    }
    if at != frame.len() {
        return Err(TradeError::frame(
            t,
            format!("{} bytes after the message's end at {at}", frame.len() - at),
        ));
    }
    Ok(shapes)
}

fn code(raw: u16) -> Option<RejectCode> {
    (raw != 0).then_some(RejectCode::from_raw(raw))
}

fn qty(template_id: u16, scaled: i64, symbol_id: u32) -> Result<Quantity> {
    Quantity::from_scaled(
        scaled,
        None,
        QuantityDomain::OrderBase,
        None,
        Some(symbol_id),
    )
    .map_err(|_| TradeError::frame(template_id, format!("negative quantity {scaled}")))
}

fn price(template_id: u16, ticks: i64) -> Result<Price> {
    Price::from_ticks(ticks, None)
        .map_err(|_| TradeError::frame(template_id, format!("negative price {ticks}")))
}

fn retry_after(ns: Option<u64>) -> Option<Duration> {
    ns.map(Duration::from_nanos)
}

fn utf8(template_id: u16, bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| TradeError::frame(template_id, "text that is not UTF-8"))
}

/// Decodes one server frame. A frame shorter or longer than its header,
/// block, groups and data declare, of another schema or version, or of a
/// template this client does not know is an error.
pub(crate) fn decode(frame: &[u8]) -> Result<Msg> {
    let h = header(frame)?;
    let t = h.template_id;
    let raw_u8 = |at: usize| frame[HEADER + at];
    match t {
        command_ack_codec::SBE_TEMPLATE_ID => {
            check_layout(
                frame,
                &h,
                command_ack_codec::SBE_BLOCK_LENGTH,
                &[Group {
                    min_entry: 8,
                    var_fields: 1,
                }],
                0,
            )?;
            let listing = raw_u8(ACK_LISTING);
            let d =
                command_ack_codec::CommandAckDecoder::default().header(header_decoder(frame), 0);
            let seq = d.seq();
            let correlation_id = d.correlation_id();
            let command_type = u8::from(d.command_type());
            let order_id = d.order_id();
            let symbol_id = d.symbol_id();
            let acked_at = from_unix_ns(d.ts_ns());
            let matched_orders = d.matched_orders().unwrap_or(0);
            let submitted_cancels = d.submitted_cancels().unwrap_or(0);
            let failed_cancels = d.failed_cancels().unwrap_or(0);
            let truncated = d.cancelled_orders_truncated() != 0;
            let mut cancelled = Vec::new();
            if let Some(mut g) = d.cancelled_orders_decoder() {
                while let Ok(Some(_)) = g.advance() {
                    let order_id = g.order_id();
                    let coordinates = g.client_order_id_decoder();
                    let client = utf8(t, g.client_order_id_slice(coordinates))?;
                    cancelled.push(CancelledOrder {
                        order_id,
                        client_order_id: (!client.is_empty()).then_some(client),
                    });
                }
            }
            let answer =
                if command_type == u8::from(sbe::command_type::CommandType::CANCEL_ALL_ORDERS) {
                    Answer::CancelAll(CancelAllAck {
                        correlation_id,
                        matched_orders,
                        submitted_cancels,
                        failed_cancels,
                        cancelled,
                        truncated,
                        listing: match listing {
                            0 => CancelAllListing::Unspecified,
                            1 => CancelAllListing::Included,
                            2 => CancelAllListing::NotRetained,
                            other => CancelAllListing::Unknown(other),
                        },
                        acked_at,
                    })
                } else {
                    Answer::Order {
                        command_type,
                        ack: OrderAck {
                            correlation_id,
                            order_id,
                            symbol_id,
                            acked_at,
                        },
                    }
                };
            Ok(Msg::Answer { seq, answer })
        }
        command_reject_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, command_reject_codec::SBE_BLOCK_LENGTH, &[], 1)?;
            let raw = u16_at(frame, HEADER + REJECT_CODE);
            let mut d = command_reject_codec::CommandRejectDecoder::default()
                .header(header_decoder(frame), 0);
            let (seq, correlation_id, ts_ns) = (d.seq(), d.correlation_id(), d.ts_ns());
            let retry = retry_after(d.rate_limit_retry_after_ns());
            let existing_order_id = d.existing_order_id();
            let coordinates = d.detail_decoder();
            let detail = String::from_utf8_lossy(d.detail_slice(coordinates)).into_owned();
            Ok(Msg::Answer {
                seq,
                answer: Answer::Reject(RejectFrame {
                    correlation_id,
                    code: RejectCode::from_raw(raw),
                    detail,
                    retry_after: retry,
                    existing_order_id,
                    ts_ns,
                }),
            })
        }
        pong_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, pong_codec::SBE_BLOCK_LENGTH, &[], 1)?;
            let d = pong_codec::PongDecoder::default().header(header_decoder(frame), 0);
            Ok(Msg::Pong { seq: d.seq() })
        }
        subscription_ack_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, subscription_ack_codec::SBE_BLOCK_LENGTH, &[], 0)?;
            let d = subscription_ack_codec::SubscriptionAckDecoder::default()
                .header(header_decoder(frame), 0);
            Ok(Msg::SubscriptionAck {
                seq: d.seq(),
                channels: d.channels().0,
            })
        }
        execution_event_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, execution_event_codec::SBE_BLOCK_LENGTH, &[], 0)?;
            let fee = raw_u8(EXECUTION_FEE_ASSET);
            let d = execution_event_codec::ExecutionEventDecoder::default()
                .header(header_decoder(frame), 0);
            let symbol_id = d.symbol_id();
            Ok(Msg::Execution(Execution {
                symbol_id,
                match_id: d.match_id(),
                match_seq: d.match_seq(),
                order_id: d.order_id(),
                side: match d.side() {
                    side::Side::BUY => Some(Side::Buy),
                    side::Side::SELL => Some(Side::Sell),
                    side::Side::NullVal => None,
                },
                maker: d.liquidity_role() == liquidity_role::LiquidityRole::MAKER,
                fee_asset: match fee {
                    1 => FeeAsset::Quote,
                    2 => FeeAsset::Base,
                    other => FeeAsset::Unknown(other),
                },
                settled: d.settlement_status() == settlement_status::SettlementStatus::SETTLED,
                qty: qty(t, d.fill_qty_scaled(), symbol_id)?,
                price: price(t, d.fill_price_ticks())?,
                remaining_qty: d
                    .remaining_qty_scaled()
                    .map(|v| qty(t, v, symbol_id))
                    .transpose()?,
                match_time: from_unix_ns(d.match_ts_ns()),
                gateway_time: from_unix_ns(d.gateway_ts_ns()),
            }))
        }
        order_event_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, order_event_codec::SBE_BLOCK_LENGTH, &[], 0)?;
            let status = raw_u8(ORDER_STATUS);
            let reject = u16_at(frame, HEADER + ORDER_REJECT_REASON);
            let cancel = raw_u8(ORDER_CANCEL_REASON);
            let d =
                order_event_codec::OrderEventDecoder::default().header(header_decoder(frame), 0);
            let symbol_id = d.symbol_id();
            Ok(Msg::Order(OrderUpdate {
                symbol_id,
                order_id: d.order_id(),
                version: d.version(),
                status: OrderStatus::from_raw(status),
                remaining_qty: qty(t, d.remaining_qty_scaled(), symbol_id)?,
                cumulative_qty: qty(t, d.cumulative_qty_scaled(), symbol_id)?,
                event_time: from_unix_ns(d.event_ts_ns()),
                gateway_time: from_unix_ns(d.gateway_ts_ns()),
                reject_reason: code(reject),
                cancel_reason: CancelReason::from_raw(cancel),
            }))
        }
        batch_create_ack_codec::SBE_TEMPLATE_ID => {
            let shapes = check_layout(
                frame,
                &h,
                batch_create_ack_codec::SBE_BLOCK_LENGTH,
                &[Group {
                    min_entry: 35,
                    var_fields: 0,
                }],
                0,
            )?;
            let codes = item_codes(frame, shapes[0]);
            let d = batch_create_ack_codec::BatchCreateAckDecoder::default()
                .header(header_decoder(frame), 0);
            let (seq, correlation_id, ts_ns) = (d.seq(), d.correlation_id(), d.ts_ns());
            let retry = retry_after(d.rate_limit_retry_after_ns());
            let mut g = d.results_decoder();
            let mut items = Vec::with_capacity(codes.len());
            while let Ok(Some(i)) = g.advance() {
                items.push(BatchItem {
                    accepted: accepted(g.status()),
                    code: code(codes[i]),
                    order_id: g.order_id(),
                    existing_order_id: g.existing_order_id(),
                });
            }
            Ok(Msg::Answer {
                seq,
                answer: Answer::BatchCreate(BatchCreateAck {
                    correlation_id,
                    items,
                    retry_after: retry,
                    acked_at: from_unix_ns(ts_ns),
                }),
            })
        }
        batch_replace_ack_codec::SBE_TEMPLATE_ID => {
            let shapes = check_layout(
                frame,
                &h,
                batch_replace_ack_codec::SBE_BLOCK_LENGTH,
                &[Group {
                    min_entry: 20,
                    var_fields: 0,
                }],
                0,
            )?;
            let codes = item_codes(frame, shapes[0]);
            let actions: Vec<u8> = (0..shapes[0].count)
                .map(|i| frame[shapes[0].entry_at(i) + REPLACE_ITEM_ACTION])
                .collect();
            let status = raw_u8(REPLACE_ACK_STATUS);
            let d = batch_replace_ack_codec::BatchReplaceAckDecoder::default()
                .header(header_decoder(frame), 0);
            let (seq, correlation_id, ts_ns) = (d.seq(), d.correlation_id(), d.ts_ns());
            let batch_request_id = d.batch_request_id();
            let retry = retry_after(d.rate_limit_retry_after_ns());
            let mut g = d.results_decoder();
            let mut items = Vec::with_capacity(codes.len());
            while let Ok(Some(i)) = g.advance() {
                items.push(ReplaceItemResult {
                    accepted: accepted(g.status()),
                    code: code(codes[i]),
                    action: match actions[i] {
                        0 => None,
                        1 => Some(ReplaceAction::Amended),
                        2 => Some(ReplaceAction::Replaced),
                        other => Some(ReplaceAction::Unknown(other)),
                    },
                    order_id: g.old_order_id(),
                    replacement_order_id: g.replacement_order_id(),
                });
            }
            Ok(Msg::Answer {
                seq,
                answer: Answer::BatchReplace(BatchReplaceAck {
                    correlation_id,
                    batch_request_id,
                    status: match status {
                        1 => BatchStatus::Admitted,
                        2 => BatchStatus::PartiallyAdmitted,
                        3 => BatchStatus::Rejected,
                        other => BatchStatus::Unknown(other),
                    },
                    items,
                    retry_after: retry,
                    acked_at: from_unix_ns(ts_ns),
                }),
            })
        }
        batch_cancel_ack_codec::SBE_TEMPLATE_ID => {
            let shapes = check_layout(
                frame,
                &h,
                batch_cancel_ack_codec::SBE_BLOCK_LENGTH,
                &[Group {
                    min_entry: 11,
                    var_fields: 0,
                }],
                0,
            )?;
            let codes = item_codes(frame, shapes[0]);
            let d = batch_cancel_ack_codec::BatchCancelAckDecoder::default()
                .header(header_decoder(frame), 0);
            let (seq, correlation_id, ts_ns) = (d.seq(), d.correlation_id(), d.ts_ns());
            let retry = retry_after(d.rate_limit_retry_after_ns());
            let mut g = d.results_decoder();
            let mut items = Vec::with_capacity(codes.len());
            while let Ok(Some(i)) = g.advance() {
                items.push(BatchItem {
                    accepted: accepted(g.status()),
                    code: code(codes[i]),
                    order_id: g.order_id(),
                    existing_order_id: None,
                });
            }
            Ok(Msg::Answer {
                seq,
                answer: Answer::BatchCancel(BatchCancelAck {
                    correlation_id,
                    items,
                    retry_after: retry,
                    acked_at: from_unix_ns(ts_ns),
                }),
            })
        }
        market_resync_codec::SBE_TEMPLATE_ID => {
            check_layout(frame, &h, market_resync_codec::SBE_BLOCK_LENGTH, &[], 0)?;
            let reason = raw_u8(RESYNC_REASON);
            let d = market_resync_codec::MarketResyncDecoder::default()
                .header(header_decoder(frame), 0);
            Ok(Msg::MarketResync(MarketResync {
                symbol_id: d.symbol_id(),
                reason: match reason {
                    1 => ResyncReason::Interrupted,
                    2 => ResyncReason::Discontinuity,
                    other => ResyncReason::Unknown(other),
                },
                gateway_time: from_unix_ns(d.gateway_ts_ns()),
            }))
        }
        cancel_all_after_ack_codec::SBE_TEMPLATE_ID => {
            check_layout(
                frame,
                &h,
                cancel_all_after_ack_codec::SBE_BLOCK_LENGTH,
                &[],
                0,
            )?;
            let d = cancel_all_after_ack_codec::CancelAllAfterAckDecoder::default()
                .header(header_decoder(frame), 0);
            let armed = d.status() == cancel_all_after_status::CancelAllAfterStatus::ARMED;
            let expires = d.expires_at_ts_ns();
            if armed && expires == 0 {
                return Err(TradeError::frame(t, "an armed timer without an expiry"));
            }
            Ok(Msg::Answer {
                seq: d.seq(),
                answer: Answer::Timer(TimerAck {
                    correlation_id: d.correlation_id(),
                    armed,
                    timeout: Duration::from_secs(u64::from(d.timeout_sec())),
                    expires_at: armed.then(|| from_unix_ns(expires)),
                    acked_at: from_unix_ns(d.ts_ns()),
                }),
            })
        }
        other => Err(TradeError::frame(
            other,
            "a template this client does not know",
        )),
    }
}

/// The raw reject code of each fixed-size batch result entry.
fn item_codes(frame: &[u8], shape: Shape) -> Vec<u16> {
    (0..shape.count)
        .map(|i| u16_at(frame, shape.entry_at(i) + ITEM_CODE))
        .collect()
}

fn accepted(status: sbe::batch_item_status::BatchItemStatus) -> bool {
    status == sbe::batch_item_status::BatchItemStatus::ACCEPTED
}
