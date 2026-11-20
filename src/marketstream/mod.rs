//! Client for the public binary market-data WebSocket: trades, best bid and
//! offer, and order book depth of spot pairs, encoded with Simple Binary
//! Encoding (SBE).
//!
//! The stream is public: connecting needs no API key. [`connect`] connects and
//! returns a [`MarketStream`] whose [`MarketStream::recv`] delivers every
//! stream's messages in order, with prices and quantities as exact decimal
//! values scaled by the pair's catalog scales. The client reconnects on its own
//! and resubscribes, unless [`Config::auto_reconnect`] is off; each stream
//! recovers as the protocol defines:
//!
//! - trades: every trade arrives once or is reported in a [`TradesGap`], which
//!   the client can backfill from the trade history API ([`Config::history`]);
//! - bbo and depth20: each message is the full state, so a newer one replaces
//!   the older and the client keeps the newest;
//! - depth: a [`DepthSnapshot`] opens the book and [`DepthUpdate`] messages
//!   apply only to a book at exactly their base. A book that cannot follow the
//!   stream is rebuilt from a fresh snapshot, never patched across a gap.
//!
//! Decoders are generated from the published schema with Real Logic's
//! sbe-tool. Messages of templates the decoders do not know are skipped. A
//! message of a newer schema version is read by its declared block lengths, as
//! the SBE versioning rules allow; a message whose layout this client cannot
//! read, or of another schema, is refused ([`StreamError`]), never misread.

mod backfill;
mod book;
mod client;
mod codec;
#[allow(dead_code, unused_imports, unused_mut, clippy::all)]
pub(crate) mod sbe;
#[cfg(test)]
mod tests;
mod trades;

use std::fmt;

use crate::types::{Price, Quantity};

pub use backfill::{EpochEnd, HistoryPage, TradeHistory, backfill};
pub use book::OrderBook;
pub use client::{Config, LoadedPairs, MarketStream, Pairs, PairsSource, connect, url};
pub use codec::{SCHEMA_ID, SCHEMA_VERSION};

/// The market-data stream's path on the API's WebSocket host.
pub const PATH: &str = "/ws/market.sbe";

/// A market-data stream of one trading pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Stream {
    /// Every trade of the pair in real time, with a trade frontier and
    /// explicit gaps.
    Trades,
    /// The best bid and offer in real time; a subscriber that falls behind
    /// receives only the newest.
    Bbo,
    /// The best 20 price levels per side as full replacement snapshots.
    Depth20,
    /// The live depth window: a snapshot, then exact-base updates.
    Depth,
}

impl Stream {
    pub(crate) const ALL: [Stream; 4] =
        [Stream::Trades, Stream::Bbo, Stream::Depth20, Stream::Depth];

    pub(crate) fn wire(self) -> sbe::stream_type::StreamType {
        use sbe::stream_type::StreamType;
        match self {
            Stream::Trades => StreamType::TRADES,
            Stream::Bbo => StreamType::BBO,
            Stream::Depth20 => StreamType::DEPTH20,
            Stream::Depth => StreamType::DEPTH,
        }
    }

    pub(crate) fn from_wire(value: sbe::stream_type::StreamType) -> Option<Self> {
        use sbe::stream_type::StreamType;
        match value {
            StreamType::TRADES => Some(Stream::Trades),
            StreamType::BBO => Some(Stream::Bbo),
            StreamType::DEPTH20 => Some(Stream::Depth20),
            StreamType::DEPTH => Some(Stream::Depth),
            StreamType::NullVal => None,
        }
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Stream::Trades => "trades",
            Stream::Bbo => "bbo",
            Stream::Depth20 => "depth20",
            Stream::Depth => "depth",
        })
    }
}

/// One stream of one trading pair, by the pair's numeric ID from the spot
/// configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Topic {
    pub symbol_id: u32,
    pub stream: Stream,
}

impl Topic {
    pub fn new(symbol_id: u32, stream: Stream) -> Self {
        Self { symbol_id, stream }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.symbol_id, self.stream)
    }
}

/// The side of the order that took liquidity in a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
    /// A side the server did not set, or one this client does not know.
    Unknown,
}

/// Whether the server holds a pair's current market data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedState {
    /// The pair's streams are current.
    Live,
    /// The server lost the pair's current state. Best bid and offer and depth
    /// pause until it recovers; a depth book gets its replacement snapshot
    /// before the pair is live again, and trade gaps are reported.
    Stale,
    /// A state this client does not know.
    Unknown,
}

/// The outcome of one subscription item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscribeResult {
    /// Subscribe: the stream is subscribed and its messages follow.
    /// Unsubscribe: the stream ended.
    Ok,
    /// The pair is not served.
    UnknownSymbol,
    /// The connection already holds the most subscriptions it may; the item
    /// is not subscribed.
    LimitExceeded,
    /// The connection already holds the stream.
    AlreadySubscribed,
    /// Unsubscribe: the connection did not hold the stream.
    NotSubscribed,
    /// The pair is listed but disabled; subscribe again once it is enabled.
    SymbolDisabled,
    /// A result this client does not know.
    Unknown,
}

impl SubscribeResult {
    /// Whether a Subscribe left the stream held on the connection.
    pub fn subscribed(self) -> bool {
        matches!(
            self,
            SubscribeResult::Ok | SubscribeResult::AlreadySubscribed
        )
    }
}

impl fmt::Display for SubscribeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SubscribeResult::Ok => "ok",
            SubscribeResult::UnknownSymbol => "unknown_symbol",
            SubscribeResult::LimitExceeded => "limit_exceeded",
            SubscribeResult::AlreadySubscribed => "already_subscribed",
            SubscribeResult::NotSubscribed => "not_subscribed",
            SubscribeResult::SymbolDisabled => "symbol_disabled",
            SubscribeResult::Unknown => "unknown",
        })
    }
}

/// One price level: a price in quote units and the total resting quantity in
/// base units at the pair's scale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub price: Price,
    pub qty: Quantity,
}

/// One public trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    /// The trade's match ID, the ID the trade history API returns. Trade IDs
    /// ascend per pair, across book epochs too.
    pub trade_id: u64,
    pub price: Price,
    pub qty: Quantity,
    pub taker_side: Side,
    /// Time the trade executed, in Unix nanoseconds: the time of the book
    /// update that executed it, the [`Trades::transact_ts_ns`] of a live
    /// trade. 0 when the trade history does not report it.
    pub transact_ts_ns: u64,
}

/// Trades of one book update, in ascending trade ID order. Each trade is
/// delivered at most once per [`MarketStream`], across reconnects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trades {
    pub symbol_id: u32,
    /// The pair's symbol from the catalog, when known.
    pub symbol: Option<String>,
    pub book_epoch: u64,
    /// The book update that executed the trades.
    pub update_id: u64,
    /// Time of the book update, in Unix nanoseconds.
    pub transact_ts_ns: u64,
    /// Server time the message was encoded, in Unix nanoseconds.
    pub send_ts_ns: u64,
    pub trades: Vec<Trade>,
}

/// How a trades gap became known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GapCause {
    /// The server named trades this subscription will not receive live.
    Skipped,
    /// The client found trades neither delivered nor named.
    Detected,
    /// Trades between the last one accounted for before a reconnect and the
    /// new subscription's baseline.
    Reconnect,
}

/// Trades of a pair that were not delivered live. Recover them from the trade
/// history API by trade ID: [`backfill`] walks the gap, and
/// [`Config::history`] makes the client do so on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradesGap {
    pub symbol_id: u32,
    /// The last trade ID accounted for before the gap: the gap starts after
    /// it.
    pub after_trade_id: u64,
    /// The last missing trade ID, inclusive; 0 when the gap is the rest of
    /// `tail_epoch`.
    pub through_trade_id: u64,
    /// When not 0, a book epoch whose every trade after `after_trade_id` is
    /// missing; the trade history API reports the epoch's last trade once the
    /// epoch has ended.
    pub tail_epoch: u64,
    pub cause: GapCause,
}

/// The trade frontier: every trade of the pair up to `last_trade_id` has been
/// delivered or reported in a [`TradesGap`]. It arrives on subscribe and then
/// every second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradesThrough {
    pub symbol_id: u32,
    pub book_epoch: u64,
    pub last_trade_id: u64,
    pub send_ts_ns: u64,
}

/// The outcome of recovering one [`TradesGap`] from the trade history API,
/// when [`Config::history`] is set: the gap's trades in ascending ID order, or
/// why the recovery failed.
#[derive(Debug, Clone)]
pub struct TradesBackfill {
    pub gap: TradesGap,
    pub trades: crate::Result<Vec<Trade>>,
}

/// A pair's best bid and offer; a side without orders is `None`. Only states
/// newer than the last one delivered are delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestBidOffer {
    pub symbol_id: u32,
    pub symbol: Option<String>,
    pub book_epoch: u64,
    pub update_id: u64,
    pub transact_ts_ns: u64,
    pub send_ts_ns: u64,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
    /// Set on the copy [`MarketStream::best_bid_offer`] returns while the
    /// quote may not be current: the connection that fed it was lost, or the
    /// pair's feed is stale. Events always carry `false`.
    pub stale: bool,
}

/// Replaces a pair's book of the depth20 or depth stream. Bids are best
/// (highest) first, asks best (lowest) first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshot {
    pub symbol_id: u32,
    pub symbol: Option<String>,
    pub stream: Stream,
    pub book_epoch: u64,
    pub update_id: u64,
    /// The most levels per side the stream holds: 20 for depth20; for depth,
    /// the live window each side is kept to.
    pub levels_per_side: u16,
    pub transact_ts_ns: u64,
    pub send_ts_ns: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// A change to a pair's depth book, already applied to the client's book
/// ([`MarketStream::order_book`]): each level is set, and a level with
/// quantity 0 is deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthUpdate {
    pub symbol_id: u32,
    pub symbol: Option<String>,
    pub book_epoch: u64,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub transact_ts_ns: u64,
    pub send_ts_ns: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// A pair's feed state; it arrives when it changes, and when a subscription
/// starts while the pair is stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketStatus {
    pub symbol_id: u32,
    pub feed: FeedState,
    pub send_ts_ns: u64,
}

/// One topic's subscription outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicResult {
    /// The topic; `None` for a stream this client does not know.
    pub topic: Option<Topic>,
    pub symbol_id: u32,
    pub result: SubscribeResult,
}

/// Answers one Subscribe or Unsubscribe: one result per topic, in request
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeResponse {
    pub request_id: u64,
    pub results: Vec<TopicResult>,
}

/// Answers the client's heartbeat ping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pong {
    pub request_id: u64,
    /// Time from sending the ping to reading the pong.
    pub round_trip: std::time::Duration,
    pub send_ts_ns: u64,
}

/// One message of the stream.
#[derive(Debug, Clone)]
pub enum Event {
    Trades(Trades),
    TradesGap(TradesGap),
    TradesThrough(TradesThrough),
    TradesBackfill(TradesBackfill),
    BestBidOffer(BestBidOffer),
    DepthSnapshot(DepthSnapshot),
    DepthUpdate(DepthUpdate),
    MarketStatus(MarketStatus),
    SubscribeResponse(SubscribeResponse),
    Pong(Pong),
    /// A connection whose subscriptions are being re-established; `reconnect`
    /// is false for the first.
    Connected {
        reconnect: bool,
    },
    /// A lost connection, or a failed reconnect; the client connects again
    /// after `delay`. When `error` comes from an answer, such as a refused
    /// upgrade, [`crate::Error::answer`] holds its status and the delay it
    /// asked for with `Retry-After`, and `delay` is at least that delay: a
    /// server's `Retry-After` is never shortened, and the backoff's 30-second
    /// ceiling applies to the backoff alone. With a
    /// [`Config::reconnect_limiter`], the upgrade then waits for a slot too,
    /// which adds to `delay` and never shortens it. A caller that will not
    /// wait that long can close the stream.
    Disconnected {
        error: crate::Error,
        delay: std::time::Duration,
    },
    /// The pairs were re-read for a new connection ([`Config::reload`]);
    /// every pair whose ID changed is listed.
    PairsReloaded {
        renumbered: Vec<Renumbered>,
    },
    /// Re-reading the pairs failed; the connection subscribes nothing until a
    /// retry after `delay` succeeds.
    PairsReloadFailed {
        error: crate::Error,
        delay: std::time::Duration,
    },
    /// A stream the caller subscribed to ended: `error` says why.
    Ended {
        topic: Topic,
        error: crate::Error,
    },
}

/// A pair the server renumbered: the client follows it by symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Renumbered {
    pub symbol: String,
    pub from: u32,
    pub to: u32,
}

/// Why the market-data stream refused a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    /// A message of another schema. The server speaks a protocol this client
    /// does not, so the client stops instead of reconnecting.
    #[error(
        "market stream message of schema {schema_id} version {version}; this client reads schema {} up to the rules of version {}",
        SCHEMA_ID,
        SCHEMA_VERSION
    )]
    Schema { schema_id: u16, version: u16 },
    /// A message whose bytes do not match its declared layout, or whose values
    /// break the protocol. The client reconnects, so every stream restarts
    /// from a fresh baseline; without [`Config::auto_reconnect`] it stops
    /// with this error.
    #[error("invalid market stream message (template {template_id}): {message}")]
    Frame { template_id: u16, message: String },
    /// An unsubscribe that the server answered with neither
    /// [`SubscribeResult::Ok`] nor [`SubscribeResult::NotSubscribed`] for a
    /// stream it holds, as its answers show, those to superseded requests
    /// included: the stream stays held whatever the caller wants, so the
    /// client reconnects, and every stream it wants is resubscribed; without
    /// [`Config::auto_reconnect`] it stops with this error.
    #[error("market stream unsubscribe of {topic} answered {result}")]
    Unsubscribe {
        topic: Topic,
        result: SubscribeResult,
    },
    /// The pair of a subscribed stream is no longer in the spot config the
    /// client re-read; the stream ended.
    #[error("market stream {topic}: pair {} is no longer listed", .symbol.as_deref().unwrap_or("?"))]
    PairNotListed {
        topic: Topic,
        symbol: Option<String>,
    },
    /// A trades recovery of pair `symbol_id` answered after its connection
    /// was replaced. It was read with that connection's catalog, which the
    /// pairs read since may have replaced, so its trades were dropped:
    /// recover the gap from the trade history API again if it is needed.
    #[error("market stream pair {symbol_id}: trades recovery from a replaced connection dropped")]
    Superseded { symbol_id: u32 },
}

impl StreamError {
    pub(crate) fn frame(template_id: u16, message: impl Into<String>) -> crate::Error {
        crate::Error::MarketStream(StreamError::Frame {
            template_id,
            message: message.into(),
        })
    }
}
