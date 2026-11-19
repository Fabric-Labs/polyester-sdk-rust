//! Client for the trading WebSocket with Simple Binary Encoding (SBE): post-only
//! limit orders, cancels, modifications, batches and the cancel-all-after
//! dead-man switch, with the account's private order and execution events.
//!
//! [`connect`] opens a [`TradeSession`]. The upgrade request is signed with the
//! API key's Ed25519 key and, unless [`Config::cancel_on_disconnect`] is
//! `None`, opts into cancel-on-disconnect for the whole account
//! (`X-Cancel-On-Disconnect: ACCOUNT`), on every reconnect too until
//! [`TradeSender::disable_cancel_on_disconnect`] turns it off for every later
//! upgrade. The session subscribes to the
//! `executions`, `orders` and `marketResync` channels and sends no command
//! before the server confirms that subscription ([`TradeSession::ready`]). It
//! pings the server, and a ping or command left unanswered ends the
//! connection. It reconnects on its own; a new connection starts again at
//! `seq` 1, unarmed, with nothing in flight.
//!
//! [`TradeSession::split`] divides the session into a [`TradeSender`], which
//! any number of tasks clone to keep commands in flight, and the
//! [`TradeEvents`] one task drains; dropping the events half closes the
//! session.
//!
//! Every write is answered by exactly one of:
//!
//! - its acknowledgement, the method's `Ok` value;
//! - [`TradeError::Rejected`]: the server refused it. A refusal whose code ends
//!   in `_OUTCOME_UNKNOWN`, or a batch refused with `UNAVAILABLE`, may have
//!   applied in part ([`Reject::outcome_unknown`]);
//! - [`TradeError::NotSent`]: refused locally, nothing reached the server;
//! - [`TradeError::OutcomeUnknown`]: it was sent, and its answer was lost
//!   because the connection ended or no answer came within
//!   [`Config::answer_timeout`]. It may have applied.
//!
//! [`crate::Error::mutation_outcome_unknown`] is true for the unknown outcomes:
//! read the orders back before acting on them, and resend a batch, modify or
//! cancel-all with the same `correlation_id`, which the server keeps as the
//! request's idempotency identity.
//!
//! Prices are exact integer ticks ([`Price`], quote units times 1e9) and
//! quantities exact integers scaled by the pair's `base_quantity_scale` from
//! the spot config ([`Quantity`]). Every order and modification is checked
//! against the pair in [`Config::pairs`], usually the client's catalogs, as
//! the Orders API checks it: a quantity whose scale, symbol or pair differs
//! from the pair's, a price whose symbol differs, or a pair the catalog does
//! not have is refused before sending ([`crate::Error::Validation`]). A
//! quantity without a scale is taken as already at the pair's scale, and a
//! price without a symbol as the pair's.
//!
//! The codecs are generated from the published schema with Real Logic's
//! sbe-tool; a message of another schema or version is refused
//! ([`TradeError::Schema`]), never misread. The schema adds messages
//! without changing its version, so a message of a template this client does
//! not know is skipped: it is logged at debug level, never queued as an event,
//! and the connection and its commands in flight carry on.

mod codec;
#[allow(dead_code, unused_imports, unused_mut, clippy::all)]
pub(crate) mod sbe;
mod session;
#[cfg(test)]
mod tests;
mod upgrade;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::{Error, Result};
use crate::marketstream::Pairs;
use crate::types::{Price, Quantity, QuantityDomain};

pub use codec::{SCHEMA_ID, SCHEMA_VERSION};
pub use session::{Config, TimerGate, TradeEvents, TradeSender, TradeSession, connect, url};

/// The trading WebSocket's SBE path on the API's WebSocket host.
pub const PATH: &str = "/ws/trade.sbe";

/// The most orders a [`BatchCreateOrders`] carries.
pub const MAX_BATCH_CREATE: usize = 20;
/// The most items a [`BatchReplaceOrders`] carries.
pub const MAX_BATCH_REPLACE: usize = 50;
/// The most items a [`BatchCancelOrders`] carries.
pub const MAX_BATCH_CANCEL: usize = 50;
/// The most pairs a [`CancelAllOrders`] names.
pub const MAX_CANCEL_ALL_SYMBOLS: usize = 100;
/// The longest client order ID, in ASCII characters.
pub const MAX_CLIENT_ORDER_ID_LEN: usize = 36;
/// The longest request receive window.
pub const MAX_RECV_WINDOW: Duration = Duration::from_secs(60);
/// The shortest and longest cancel-all-after timeout; zero disarms.
pub const MIN_CANCEL_ALL_AFTER: Duration = Duration::from_secs(10);
pub const MAX_CANCEL_ALL_AFTER: Duration = Duration::from_secs(120);
/// The shortest and longest cancel-on-disconnect grace period.
pub const MIN_CANCEL_ON_DISCONNECT_GRACE: Duration = Duration::from_secs(10);
pub const MAX_CANCEL_ON_DISCONNECT_GRACE: Duration = Duration::from_secs(120);

/// How far ahead of the local clock a [`Deadline`] may start. The server
/// refuses a command timestamped more than a second ahead of its own clock
/// without taking its `seq`, which would put every later command of the
/// connection out of sequence.
const MAX_DEADLINE_LEAD_MS: u64 = crate::auth::MAX_SIGNING_FUTURE_SKEW_MS;

/// A fresh nonzero correlation ID: the Unix time in milliseconds shifted left
/// by 20 bits, plus a process counter, so IDs do not repeat across restarts
/// unless more than a million are made in one millisecond.
pub fn new_correlation_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        u64::try_from(d.as_millis()).unwrap_or(u64::MAX >> 20)
    });
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xF_FFFF;
    ((ms << 20) | n).max(1)
}

pub(crate) fn unix_ms(at: SystemTime) -> Option<u64> {
    at.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
}

pub(crate) fn from_unix_ns(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

/// A write's request deadline: the server processes it only until
/// [`Self::expires_at`] on its own clock, and refuses it after that with
/// `REQUEST_DEADLINE_EXCEEDED`, changing nothing. The session refuses to send
/// a command whose deadline has passed ([`NotSent::Expired`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Deadline {
    timestamp_ms: u64,
    window_ms: u32,
}

impl Deadline {
    /// A deadline `window` from now.
    pub fn within(window: Duration) -> Result<Self> {
        Self::new(SystemTime::now(), window)
    }

    /// A deadline `window` after `sent_at`, which counts to the millisecond.
    /// The window is whole milliseconds from 1 ms to [`MAX_RECV_WINDOW`];
    /// `sent_at` may not be ahead of the local clock by more than half a
    /// second.
    pub fn new(sent_at: SystemTime, window: Duration) -> Result<Self> {
        let timestamp_ms = unix_ms(sent_at)
            .filter(|ms| *ms > 0)
            .ok_or_else(|| Error::validation("tradews deadline: sent_at must be after 1970"))?;
        let now_ms = unix_ms(SystemTime::now()).unwrap_or(0);
        if timestamp_ms > now_ms.saturating_add(MAX_DEADLINE_LEAD_MS) {
            return Err(Error::validation(
                "tradews deadline: sent_at is ahead of the local clock",
            ));
        }
        if !window.subsec_nanos().is_multiple_of(1_000_000)
            || window < Duration::from_millis(1)
            || window > MAX_RECV_WINDOW
        {
            return Err(Error::validation(
                "tradews deadline: the window must be whole milliseconds from 1 ms to 60 s",
            ));
        }
        Ok(Self {
            timestamp_ms,
            window_ms: window.as_millis() as u32,
        })
    }

    /// When the command was made, to the millisecond.
    pub fn sent_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.timestamp_ms)
    }

    pub fn window(&self) -> Duration {
        Duration::from_millis(u64::from(self.window_ms))
    }

    /// The last instant the server may process the command.
    pub fn expires_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.expires_at_ms())
    }

    pub(crate) fn expires_at_ms(&self) -> u64 {
        self.timestamp_ms + u64::from(self.window_ms)
    }
}

/// An order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

/// An order named by its order ID or its client order ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OrderRef {
    Id(u64),
    Client(String),
}

/// One post-only, good-till-cancelled limit order. It rests on the book or is
/// refused with `POST_ONLY_CROSS`; it never takes liquidity. Self-trade
/// prevention is `EXPIRE_MAKER` and fees are paid in the quote asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostOnlyOrder {
    pub symbol_id: u32,
    pub side: Side,
    /// The limit price; positive.
    pub price: Price,
    /// The base quantity at the pair's base quantity scale; positive. A
    /// quantity that carries a scale, symbol or pair must carry the pair's;
    /// one without a scale is already in the pair's units.
    pub qty: Quantity,
    /// 1 to 36 of `A-Z a-z 0-9 . _ : / -`, unique in the account while the
    /// server retains it. It is how an order of an unknown outcome is found.
    pub client_order_id: String,
}

/// How a modification changes an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ModifyBehavior {
    /// Amend in place, keeping the order ID and queue priority, when the price
    /// stays and the quantity does not rise; otherwise replace the order with
    /// a successor under a new order ID, at the back of the queue.
    #[default]
    AmendOrReplace,
    /// Amend in place, or refuse with `MODIFICATION_REQUIRES_REPLACE`.
    AmendOnly,
    /// Always replace with a successor under a new order ID.
    ReplaceOnly,
}

/// Modifies one order's price, quantity or both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrder {
    /// The modification's account-wide idempotency identity: resend the same
    /// value with the same fields to retry, and use a fresh one, as
    /// [`Self::new`] does, for each new modification.
    pub correlation_id: u64,
    pub symbol_id: u32,
    pub order: OrderRef,
    /// The new total quantity, or `None` to keep it.
    pub qty: Option<Quantity>,
    /// The new limit price, or `None` to keep it.
    pub price: Option<Price>,
    pub behavior: ModifyBehavior,
}

impl ModifyOrder {
    /// A modification of `order` with a fresh correlation ID, changing
    /// nothing yet, by [`ModifyBehavior::AmendOrReplace`].
    pub fn new(symbol_id: u32, order: OrderRef) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            symbol_id,
            order,
            qty: None,
            price: None,
            behavior: ModifyBehavior::AmendOrReplace,
        }
    }
}

/// Cancels every open order of the account, in the named pairs (every pair
/// when none) and on one side (both when `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllOrders {
    /// The cancellation's idempotency identity: a retry with the same value
    /// and criteria within two minutes requests no new cancellations.
    pub correlation_id: u64,
    pub symbol_ids: Vec<u32>,
    pub side: Option<Side>,
    /// Also stop the account's waiting triggers in that scope (stop-loss,
    /// take-profit, trailing stop, TWAP, ladder and attached legs created
    /// before the request was accepted): each ends canceled with reason
    /// `cancel_all` and places no order after the answer. `false`, the
    /// default, cancels only orders. It is part of the request's criteria:
    /// a retry under the same `correlation_id` must keep it.
    pub include_triggers: bool,
}

impl CancelAllOrders {
    pub fn new(symbol_ids: Vec<u32>) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            symbol_ids,
            side: None,
            include_triggers: false,
        }
    }

    /// Also stops the account's waiting triggers in the request's scope; see
    /// [`Self::include_triggers`].
    pub fn with_triggers(mut self) -> Self {
        self.include_triggers = true;
        self
    }
}

/// Options of [`TradeSender::cancel_all_after_with`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CancelAllAfterOptions {
    /// When the timer expires, also stop the account's waiting triggers
    /// created before its expiry time, as [`CancelAllOrders::include_triggers`]
    /// does; `false`, the default, cancels only orders. Set it on every arm
    /// and refresh: each one replaces the timer's scope. A disarm ignores it.
    pub include_triggers: bool,
}

impl CancelAllAfterOptions {
    /// Options that also stop triggers when the timer expires.
    pub fn with_triggers() -> Self {
        Self {
            include_triggers: true,
        }
    }
}

/// Up to [`MAX_BATCH_CREATE`] post-only orders, each admitted or refused on
/// its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateOrders {
    /// The batch's idempotency identity for 15 minutes: resending the same
    /// value and orders replays the original results.
    pub correlation_id: u64,
    pub orders: Vec<PostOnlyOrder>,
}

impl BatchCreateOrders {
    pub fn new(orders: Vec<PostOnlyOrder>) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            orders,
        }
    }
}

/// One [`BatchReplaceOrders`] item: a new price, a new total quantity or both.
/// It amends the order when the price stays and the quantity does not rise,
/// and replaces it with a successor otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceItem {
    pub order: OrderRef,
    pub qty: Option<Quantity>,
    pub price: Option<Price>,
}

/// Up to [`MAX_BATCH_REPLACE`] modifications of orders of one pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceOrders {
    /// The batch's idempotency identity, as for [`BatchCreateOrders`].
    pub correlation_id: u64,
    pub symbol_id: u32,
    pub items: Vec<ReplaceItem>,
}

impl BatchReplaceOrders {
    pub fn new(symbol_id: u32, items: Vec<ReplaceItem>) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            symbol_id,
            items,
        }
    }
}

/// One [`BatchCancelOrders`] item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelItem {
    pub symbol_id: u32,
    pub order: OrderRef,
}

/// Up to [`MAX_BATCH_CANCEL`] cancellations, each accepted or refused on its
/// own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelOrders {
    /// The batch's idempotency identity: a cancellation never takes effect
    /// twice.
    pub correlation_id: u64,
    pub items: Vec<CancelItem>,
}

impl BatchCancelOrders {
    pub fn new(items: Vec<CancelItem>) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            items,
        }
    }
}

/// A trading command, as errors name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Command {
    CreateOrder,
    CancelOrder,
    ModifyOrder,
    CancelAllOrders,
    BatchCreateOrders,
    BatchReplaceOrders,
    BatchCancelOrders,
    CancelAllAfter,
}

impl Command {
    /// Whether it adds or changes resting orders, which the timer gate
    /// ([`TimerGate`]) holds back unless the cancel-all-after timer is armed.
    pub(crate) fn places(self) -> bool {
        matches!(
            self,
            Command::CreateOrder
                | Command::ModifyOrder
                | Command::BatchCreateOrders
                | Command::BatchReplaceOrders
        )
    }

    pub(crate) fn is_batch(self) -> bool {
        matches!(
            self,
            Command::BatchCreateOrders | Command::BatchReplaceOrders | Command::BatchCancelOrders
        )
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Command::CreateOrder => "CreateOrder",
            Command::CancelOrder => "CancelOrder",
            Command::ModifyOrder => "ModifyOrder",
            Command::CancelAllOrders => "CancelAllOrders",
            Command::BatchCreateOrders => "BatchCreateOrders",
            Command::BatchReplaceOrders => "BatchReplaceOrders",
            Command::BatchCancelOrders => "BatchCancelOrders",
            Command::CancelAllAfter => "CancelAllAfter",
        })
    }
}

/// A trading refusal code (`RejectCode`). Codes this client does not name keep
/// their number.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RejectCode(u16);

impl RejectCode {
    pub const BAD_REQUEST: Self = Self(1);
    pub const CONFLICT_DUPLICATE_CLIENT_ORDER_ID: Self = Self(9);
    pub const UNAVAILABLE: Self = Self(10);
    /// The order is not one the exchange can confirm. It does not prove the
    /// order never existed: an ended order answers it too, by client order ID
    /// once that ID's window has passed and by order ID after exchange
    /// maintenance. For an order you know ended, a cancel answered with it is
    /// done; otherwise read the order. See [`crate::Error::is_order_unknown`].
    pub const ORDER_UNKNOWN: Self = Self(17);
    pub const CONFLICT_IDEMPOTENCY_KEY_REUSE: Self = Self(36);
    pub const POST_ONLY_CROSS: Self = Self(58);
    pub const ORDER_ALREADY_TERMINAL: Self = Self(64);
    pub const OVERLOADED: Self = Self(67);
    pub const RATE_LIMIT_EXCEEDED: Self = Self(69);
    pub const SYSTEM_RATE_LIMITED: Self = Self(225);
    pub const ORDER_CANCEL_OUTCOME_UNKNOWN: Self = Self(297);
    pub const ORDER_MODIFY_OUTCOME_UNKNOWN: Self = Self(298);
    pub const ACCOUNT_REPAIRING: Self = Self(300);
    pub const ORDER_CREATE_OUTCOME_UNKNOWN: Self = Self(303);
    pub const REQUEST_DEADLINE_EXCEEDED: Self = Self(308);
    pub const SEQUENCE_OUT_OF_ORDER: Self = Self(1001);

    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }

    /// The code's name, such as `POST_ONLY_CROSS`; `None` for a code this
    /// client does not know.
    pub fn name(self) -> Option<String> {
        match sbe::reject_code::RejectCode::from(self.0) {
            sbe::reject_code::RejectCode::NullVal => None,
            known => Some(known.to_string()),
        }
    }

    /// Whether the command may have applied: the code ends in
    /// `_OUTCOME_UNKNOWN`.
    pub fn outcome_unknown(self) -> bool {
        self.name()
            .is_some_and(|name| name.ends_with("_OUTCOME_UNKNOWN"))
    }
}

impl fmt::Display for RejectCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => f.write_str(&name),
            None => write!(f, "UNKNOWN({})", self.0),
        }
    }
}

impl fmt::Debug for RejectCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RejectCode({self})")
    }
}

/// Why an order was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CancelReason {
    UserRequest,
    IocRemainder,
    MarketRemainder,
    StpExpireMaker,
    StpExpireTaker,
    StpExpireBoth,
    /// A modification replaced the order with a successor.
    Replaced,
    GtdExpired,
    QtyAtOrBelowFilled,
    PairDrain,
    AttachedRiskRefused,
    /// The account's cancel-all-after timer expired.
    CancelAllAfter,
    AccountRepair,
    TwapRollover,
    StrategyCancel,
    System,
    MarketHalted,
    AttachedExitFired,
    StpDecrement,
    PriceBand,
    /// No session of the account that opted into cancel-on-disconnect stayed
    /// connected through its grace period.
    CancelOnDisconnect,
    /// A reason this client does not know, by number.
    Unknown(u8),
}

impl CancelReason {
    /// The reason of a raw value; `None` for 0, no reason.
    pub fn from_raw(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => return None,
            1 => Self::UserRequest,
            2 => Self::IocRemainder,
            3 => Self::MarketRemainder,
            4 => Self::StpExpireMaker,
            5 => Self::StpExpireTaker,
            6 => Self::StpExpireBoth,
            7 => Self::Replaced,
            8 => Self::GtdExpired,
            9 => Self::QtyAtOrBelowFilled,
            10 => Self::PairDrain,
            11 => Self::AttachedRiskRefused,
            12 => Self::CancelAllAfter,
            13 => Self::AccountRepair,
            14 => Self::TwapRollover,
            15 => Self::StrategyCancel,
            16 => Self::System,
            17 => Self::MarketHalted,
            18 => Self::AttachedExitFired,
            19 => Self::StpDecrement,
            20 => Self::PriceBand,
            21 => Self::CancelOnDisconnect,
            other => Self::Unknown(other),
        })
    }

    /// The schema's name of the reason, such as `CANCEL_ON_DISCONNECT`.
    pub fn name(self) -> String {
        match self {
            Self::Unknown(raw) => format!("UNKNOWN({raw})"),
            known => {
                let raw = known.raw();
                sbe::cancel_reason::CancelReason::from(raw).to_string()
            }
        }
    }

    pub fn raw(self) -> u8 {
        match self {
            Self::UserRequest => 1,
            Self::IocRemainder => 2,
            Self::MarketRemainder => 3,
            Self::StpExpireMaker => 4,
            Self::StpExpireTaker => 5,
            Self::StpExpireBoth => 6,
            Self::Replaced => 7,
            Self::GtdExpired => 8,
            Self::QtyAtOrBelowFilled => 9,
            Self::PairDrain => 10,
            Self::AttachedRiskRefused => 11,
            Self::CancelAllAfter => 12,
            Self::AccountRepair => 13,
            Self::TwapRollover => 14,
            Self::StrategyCancel => 15,
            Self::System => 16,
            Self::MarketHalted => 17,
            Self::AttachedExitFired => 18,
            Self::StpDecrement => 19,
            Self::PriceBand => 20,
            Self::CancelOnDisconnect => 21,
            Self::Unknown(raw) => raw,
        }
    }
}

/// An order's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OrderStatus {
    Pending,
    PendingCancel,
    /// Not active liquidity.
    PendingReplace,
    Working,
    Filled,
    Canceled,
    Rejected,
    Unknown(u8),
}

impl OrderStatus {
    pub(crate) fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Pending,
            2 => Self::PendingCancel,
            3 => Self::PendingReplace,
            4 => Self::Working,
            5 => Self::Filled,
            6 => Self::Canceled,
            7 => Self::Rejected,
            other => Self::Unknown(other),
        }
    }

    /// Whether the order has ended.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }
}

/// One transition of one of the account's orders. Keep the highest `version`
/// per order and ignore older or repeated ones; after a gap, read the order's
/// state from the Orders API.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OrderUpdate {
    pub symbol_id: u32,
    pub order_id: u64,
    pub version: u32,
    pub status: OrderStatus,
    /// Unexecuted quantity after the transition; zero once terminal.
    pub remaining_qty: Quantity,
    pub cumulative_qty: Quantity,
    pub event_time: SystemTime,
    /// When the server sent it.
    pub gateway_time: SystemTime,
    /// Why a `Rejected` order was refused.
    pub reject_reason: Option<RejectCode>,
    /// Why a `Canceled` order was cancelled.
    pub cancel_reason: Option<CancelReason>,
}

/// The asset an execution's fee is charged in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FeeAsset {
    Quote,
    Base,
    Unknown(u8),
}

/// One execution of one of the account's orders. The same execution may
/// arrive first unsettled and later settled: identify it by `symbol_id`,
/// `match_id` and `order_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Execution {
    pub symbol_id: u32,
    pub match_id: u64,
    pub match_seq: u64,
    pub order_id: u64,
    pub side: Option<Side>,
    /// The order provided liquidity.
    pub maker: bool,
    pub fee_asset: FeeAsset,
    pub settled: bool,
    pub qty: Quantity,
    pub price: Price,
    /// The order's quantity left after this execution; `None` on settled
    /// notifications.
    pub remaining_qty: Option<Quantity>,
    pub match_time: SystemTime,
    pub gateway_time: SystemTime,
}

/// Why a [`MarketResync`] was sent; the recovery is the same for each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResyncReason {
    Interrupted,
    Discontinuity,
    Unknown(u8),
}

/// The connection may have missed order and execution events of one pair:
/// read the pair's orders and executions from the Orders API, then apply the
/// later events, which may repeat state the read returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MarketResync {
    pub symbol_id: u32,
    pub reason: ResyncReason,
    pub gateway_time: SystemTime,
}

/// What [`TradeSession::recv`] delivers, in order.
///
/// The server adds messages without changing the schema's version, and this
/// enum grows with them: match it with a `_ =>` arm.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// A connection is open and its subscription requested; commands wait
    /// for [`Event::Ready`].
    Connected {
        connection: u64,
    },
    /// The server confirmed the executions, orders and marketResync
    /// subscription: commands may be sent on this connection. It starts with
    /// the cancel-all-after gate closed ([`TimerGate`]).
    Ready {
        connection: u64,
    },
    /// The connection ended, or a reconnect failed with `error`; the session
    /// tries again after `delay`. When `error` comes from an answer, such as
    /// a refused upgrade ([`TradeError::Upgrade`]), [`crate::Error::answer`]
    /// holds its status and the delay it asked for with `Retry-After`, and
    /// `delay` is at least that delay: a server's `Retry-After` is never
    /// shortened, and the backoff's 30-second ceiling applies to the backoff
    /// alone. With a [`Config::reconnect_limiter`], the upgrade then waits
    /// for a slot too, which adds to `delay` and never shortens it. A caller
    /// that will not wait that long can close the session.
    /// `connection` is the last connection's number. `unanswered` holds the
    /// correlation ID of every command sent on it without an answer: each
    /// outcome is unknown, and each caller got [`TradeError::OutcomeUnknown`].
    Disconnected {
        connection: u64,
        error: Error,
        delay: Duration,
        unanswered: Vec<u64>,
    },
    Order(OrderUpdate),
    Execution(Execution),
    MarketResync(MarketResync),
    /// The server answered the session's ping.
    Pong {
        round_trip: Duration,
    },
    /// An `OcoAck`, the server's answer to a CreateOco or CancelOco. This
    /// client sends neither on the trading WebSocket (create and cancel OCOs
    /// through the Triggers API), so an `OcoAck` answers no command of this
    /// session and is reported as it arrives. One that names a command this
    /// connection has in flight is a wrong answer and ends the connection.
    OcoAck(OcoAck),
}

/// A single-order acknowledgement: the server accepted the command, and order
/// events report what follows.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OrderAck {
    pub correlation_id: u64,
    /// The order the command created, cancelled or modified; for a modify
    /// that replaced the order, its successor.
    pub order_id: Option<u64>,
    pub symbol_id: Option<u32>,
    pub acked_at: SystemTime,
}

/// Whether a [`CancelAllAck`] lists the orders it cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CancelAllListing {
    /// The server did not say.
    Unspecified,
    /// The first answer, with its listing.
    Included,
    /// A retry's answer: the original counts without the listing. Read the
    /// open orders for the current state.
    NotRetained,
    Unknown(u8),
}

/// An order whose cancellation a cancel-all requested.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CancelledOrder {
    pub order_id: u64,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CancelAllAck {
    pub correlation_id: u64,
    /// Open orders that matched.
    pub matched_orders: u32,
    /// With [`CancelAllOrders::include_triggers`], the triggers that matched,
    /// each stopped; zero without it.
    pub matched_triggers: u32,
    pub submitted_cancels: u32,
    pub failed_cancels: u32,
    /// The orders whose cancellation was requested, oldest first, at most
    /// 200; `truncated` when more were.
    pub cancelled: Vec<CancelledOrder>,
    pub truncated: bool,
    pub listing: CancelAllListing,
    pub acked_at: SystemTime,
}

/// The account's cancel-all-after timer after a [`TradeSession::cancel_all_after`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimerAck {
    pub correlation_id: u64,
    pub armed: bool,
    /// The armed timer's timeout; zero when disarmed.
    pub timeout: Duration,
    /// When the armed timer expires unless refreshed; `None` when disarmed.
    pub expires_at: Option<SystemTime>,
    pub acked_at: SystemTime,
}

/// One batch create or batch cancel item's result, in request order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchItem {
    pub accepted: bool,
    /// The refusal of an item not accepted.
    pub code: Option<RejectCode>,
    /// Create: the admitted order. Cancel: the order resolved, else the
    /// order ID sent.
    pub order_id: Option<u64>,
    /// Create: the order holding the client order ID of an item refused as a
    /// duplicate, when known.
    pub existing_order_id: Option<u64>,
}

impl BatchItem {
    /// Whether the item may have applied though not accepted: resend the
    /// batch with the same correlation ID and items to learn its result.
    pub fn outcome_unknown(&self) -> bool {
        self.code.is_some_and(RejectCode::outcome_unknown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchCreateAck {
    pub correlation_id: u64,
    pub items: Vec<BatchItem>,
    /// The wait before retrying rate-limited items.
    pub retry_after: Option<Duration>,
    pub acked_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchCancelAck {
    pub correlation_id: u64,
    pub items: Vec<BatchItem>,
    pub retry_after: Option<Duration>,
    pub acked_at: SystemTime,
}

/// What an admitted replace item did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReplaceAction {
    /// Changed in place, under the same order ID.
    Amended,
    /// Replaced by a successor under `replacement_order_id`.
    Replaced,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplaceItemResult {
    pub accepted: bool,
    pub code: Option<RejectCode>,
    pub action: Option<ReplaceAction>,
    /// The order the item named.
    pub order_id: Option<u64>,
    /// Its successor, when replaced.
    pub replacement_order_id: Option<u64>,
}

impl ReplaceItemResult {
    pub fn outcome_unknown(&self) -> bool {
        self.code.is_some_and(RejectCode::outcome_unknown)
    }
}

/// A batch replace's aggregate outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BatchStatus {
    Admitted,
    PartiallyAdmitted,
    Rejected,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchReplaceAck {
    pub correlation_id: u64,
    pub batch_request_id: u64,
    pub status: BatchStatus,
    pub items: Vec<ReplaceItemResult>,
    pub retry_after: Option<Duration>,
    pub acked_at: SystemTime,
}

/// Which command an [`OcoAck`] answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OcoCommand {
    /// A CreateOco: the OCO's quantity is reserved and both legs created.
    Create,
    /// A CancelOco: both legs ended canceled and the quantity is released.
    Cancel,
    Unknown(u8),
}

/// An `OcoAck`: the server accepted a CreateOco or a CancelOco. The legs'
/// state and orders follow through the trigger reads and events.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OcoAck {
    pub correlation_id: u64,
    pub command: OcoCommand,
    /// The OCO, which both legs carry as their order list ID.
    pub order_list_id: u64,
    /// Create: the take-profit leg's trigger; `None` for a cancel.
    pub take_profit_trigger_id: Option<u64>,
    /// Create: the stop-loss leg's trigger; `None` for a cancel.
    pub stop_loss_trigger_id: Option<u64>,
    /// Create: the client OCO ID sent; `None` for a cancel.
    pub client_list_id: Option<String>,
    pub acked_at: SystemTime,
}

/// Why a command was not sent. Nothing of it reached the server, so sending it
/// again is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NotSent {
    /// No connection's subscription is confirmed yet.
    NotReady,
    /// Its connection ended before it could be written; it is not carried to
    /// the next one.
    Stale,
    /// Its deadline passed while it waited to be written.
    Expired,
    /// It adds or changes an order, and its deadline, plus the gate's clock
    /// margin, could end after the cancel-all-after timer's expiry minus the
    /// guard, or no timer was armed on this connection (`expires_at` `None`).
    NotArmed { expires_at: Option<SystemTime> },
    /// The most commands that may wait for an answer are in flight.
    InFlightFull,
    /// The most commands that may wait to be written are queued.
    QueueFull,
    /// The session is closed.
    Closed,
}

impl fmt::Display for NotSent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NotSent::NotReady => "no connection is ready",
            NotSent::Stale => "its connection ended before it was written",
            NotSent::Expired => "its deadline passed before it was written",
            NotSent::NotArmed { .. } => {
                "its deadline is not inside an armed cancel-all-after timer"
            }
            NotSent::InFlightFull => "too many commands await an answer",
            NotSent::QueueFull => "too many commands await sending",
            NotSent::Closed => "the session is closed",
        })
    }
}

/// The server's refusal of a command.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Reject {
    pub command: Command,
    pub correlation_id: u64,
    pub code: RejectCode,
    /// Display text; branch on `code`.
    pub detail: String,
    /// The wait before retrying a rate-limited command.
    pub retry_after: Option<Duration>,
    /// The order holding the client order ID of a create refused as a
    /// duplicate, when known.
    pub existing_order_id: Option<u64>,
    pub rejected_at: SystemTime,
}

impl Reject {
    /// Whether the command may have applied though refused: a code ending in
    /// `_OUTCOME_UNKNOWN`, or a batch refused with `UNAVAILABLE`, which may
    /// follow the admission of some items.
    pub fn outcome_unknown(&self) -> bool {
        self.code.outcome_unknown()
            || (self.command.is_batch() && self.code == RejectCode::UNAVAILABLE)
    }
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tradews refused {} (correlation {}): {}",
            self.command, self.correlation_id, self.code
        )?;
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
    }
}

/// A trading WebSocket failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TradeError {
    /// Refused locally; nothing reached the server.
    #[error("tradews {command} was not sent: {reason}")]
    NotSent { command: Command, reason: NotSent },
    /// The server refused the command.
    #[error("{0}")]
    Rejected(Box<Reject>),
    /// The command was sent and its answer lost: it may have applied.
    #[error("tradews {command} (correlation {correlation_id}) outcome unknown: {reason}")]
    OutcomeUnknown {
        command: Command,
        correlation_id: u64,
        reason: String,
    },
    /// The server refused the WebSocket upgrade with `status` and the body
    /// code `code`. `504` with `ORDER_CANCEL_OUTCOME_UNKNOWN` means the
    /// cancel-on-disconnect opt-in may have armed the account's timer: the
    /// session reconnects with the same opt-in.
    #[error("tradews upgrade refused (HTTP {status}, code {code})")]
    Upgrade {
        status: u16,
        code: String,
        /// The refused upgrade's answer: its status and the delay it asked
        /// for with `Retry-After`, which the next connection waits at least.
        /// See [`crate::Error::answer`] and [`crate::Error::retry_after`].
        answer: Option<Box<crate::errors::AnswerMeta>>,
    },
    /// The server accepted the upgrade without echoing the cancel-on-disconnect
    /// opt-in, so the session would not be protected; it is closed.
    #[error("tradews accepted the connection without confirming cancel-on-disconnect")]
    NotProtected,
    /// A message whose bytes do not match its layout or whose values break
    /// the protocol; the connection ends.
    #[error("invalid tradews message (template {template_id}): {message}")]
    Frame { template_id: u16, message: String },
    /// A message of another schema or version; the session stops.
    #[error(
        "tradews message of schema {schema_id} version {version}; this client speaks schema {} version {}",
        SCHEMA_ID,
        SCHEMA_VERSION
    )]
    Schema { schema_id: u16, version: u16 },
}

impl TradeError {
    pub(crate) fn frame(template_id: u16, message: impl Into<String>) -> Error {
        Error::TradeSession(TradeError::Frame {
            template_id,
            message: message.into(),
        })
    }

    /// This error with the server's and transport's text redacted as
    /// [`crate::transport::redact_text`] does.
    pub(crate) fn redacted(self) -> Self {
        let text = |value: String| crate::transport::redact_text(&value);
        match self {
            TradeError::Rejected(mut reject) => {
                reject.detail = text(std::mem::take(&mut reject.detail));
                TradeError::Rejected(reject)
            }
            TradeError::OutcomeUnknown {
                command,
                correlation_id,
                reason,
            } => TradeError::OutcomeUnknown {
                command,
                correlation_id,
                reason: text(reason),
            },
            TradeError::Upgrade {
                status,
                code,
                answer,
            } => TradeError::Upgrade {
                status,
                code: text(code),
                answer,
            },
            TradeError::Frame {
                template_id,
                message,
            } => TradeError::Frame {
                template_id,
                message: text(message),
            },
            other => other,
        }
    }

    pub(crate) fn mutation_outcome_unknown(&self) -> bool {
        match self {
            TradeError::OutcomeUnknown { .. } => true,
            TradeError::Rejected(reject) => reject.outcome_unknown(),
            TradeError::Upgrade { status, code, .. } => {
                *status == 504 && code == "ORDER_CANCEL_OUTCOME_UNKNOWN"
            }
            _ => false,
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            TradeError::NotSent { reason, .. } => *reason != NotSent::Closed,
            TradeError::Rejected(reject) => {
                !reject.outcome_unknown()
                    && [
                        RejectCode::UNAVAILABLE,
                        RejectCode::OVERLOADED,
                        RejectCode::RATE_LIMIT_EXCEEDED,
                        RejectCode::SYSTEM_RATE_LIMITED,
                        RejectCode::ACCOUNT_REPAIRING,
                        RejectCode::REQUEST_DEADLINE_EXCEEDED,
                    ]
                    .contains(&reject.code)
            }
            TradeError::Upgrade { status, .. } => *status == 429 || *status == 503,
            _ => false,
        }
    }

    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            TradeError::Rejected(reject) => reject.retry_after,
            _ => None,
        }
    }
}

fn not_sent(command: Command, reason: NotSent) -> Error {
    Error::TradeSession(TradeError::NotSent { command, reason })
}

/// Checks a client order ID: 1 to 36 of `A-Z a-z 0-9 . _ : / -`.
fn check_client_order_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_CLIENT_ORDER_ID_LEN {
        return Err(Error::validation(format!(
            "tradews client order ID {id:?} is not 1 to {MAX_CLIENT_ORDER_ID_LEN} characters"
        )));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b".-_:/".contains(&b))
    {
        return Err(Error::validation(format!(
            "tradews client order ID {id:?} has a character outside A-Z a-z 0-9 . _ : / -"
        )));
    }
    Ok(())
}

fn check_symbol(symbol_id: u32) -> Result<()> {
    if symbol_id == 0 {
        return Err(Error::validation("tradews symbol_id must be positive"));
    }
    Ok(())
}

fn check_correlation(correlation_id: u64) -> Result<()> {
    if correlation_id == 0 {
        return Err(Error::validation("tradews correlation_id must be nonzero"));
    }
    Ok(())
}

fn check_ref(order: &OrderRef) -> Result<()> {
    match order {
        OrderRef::Id(0) => Err(Error::validation("tradews order ID must be nonzero")),
        OrderRef::Id(_) => Ok(()),
        OrderRef::Client(id) => check_client_order_id(id),
    }
}

/// The pair an order or modification names, as the catalog has it: what its
/// quantity and price are checked against.
struct Pair {
    symbol_id: u32,
    symbol: Option<String>,
    base_quantity_scale: u32,
}

impl Pair {
    /// The pair `symbol_id` of `pairs`, refused when its base quantity scale
    /// is unknown, as the Orders API refuses a pair missing from the catalog.
    fn of(pairs: &dyn Pairs, symbol_id: u32) -> Result<Self> {
        let base_quantity_scale = pairs.base_quantity_scale(symbol_id).ok_or_else(|| {
            Error::validation(format!(
                "quantity scale for pair {symbol_id} is unavailable; await client.wait_for_catalogs() before placing orders"
            ))
        })?;
        Ok(Self {
            symbol_id,
            symbol: pairs.symbol(symbol_id),
            base_quantity_scale,
        })
    }

    /// The scaled value of a positive base quantity of this pair. As on the
    /// Orders API, a scale, symbol or pair the quantity carries must be the
    /// pair's, and one without a scale is already in the pair's units.
    fn qty_scaled(&self, qty: &Quantity) -> Result<i64> {
        if qty.domain() != QuantityDomain::OrderBase {
            return Err(Error::validation(
                "tradews quantity must be a base quantity",
            ));
        }
        if qty.symbol_id().is_some_and(|id| id != self.symbol_id) {
            return Err(Error::validation(format!(
                "tradews quantity is for pair {}, the order for pair {}",
                qty.symbol_id().unwrap_or_default(),
                self.symbol_id
            )));
        }
        qty.compatible_with(
            QuantityDomain::OrderBase,
            Some(self.base_quantity_scale),
            self.symbol.as_deref(),
            Some(self.symbol_id),
        )?;
        if qty.as_scaled() <= 0 {
            return Err(Error::validation("tradews quantity must be positive"));
        }
        Ok(qty.as_scaled())
    }

    /// The ticks of a positive price of this pair. Ticks have the fixed
    /// scale of 1e9 for every pair, so a price has no scale to mismatch; as
    /// on the Orders API, a symbol it carries must be the pair's, and the
    /// venue checks the pair's tick size.
    fn price_ticks(&self, price: &Price) -> Result<i64> {
        price.compatible_with(self.symbol.as_deref())?;
        if price.as_ticks() <= 0 {
            return Err(Error::validation("tradews price must be positive"));
        }
        Ok(price.as_ticks())
    }

    fn check_replace(&self, qty: Option<&Quantity>, price: Option<&Price>) -> Result<()> {
        if qty.is_none() && price.is_none() {
            return Err(Error::validation(
                "tradews modification changes the price, the quantity or both",
            ));
        }
        if let Some(qty) = qty {
            self.qty_scaled(qty)?;
        }
        if let Some(price) = price {
            self.price_ticks(price)?;
        }
        Ok(())
    }
}

impl PostOnlyOrder {
    pub(crate) fn check(&self, pairs: &dyn Pairs) -> Result<(i64, i64)> {
        check_symbol(self.symbol_id)?;
        check_client_order_id(&self.client_order_id)?;
        let pair = Pair::of(pairs, self.symbol_id)?;
        Ok((pair.price_ticks(&self.price)?, pair.qty_scaled(&self.qty)?))
    }
}

impl ModifyOrder {
    pub(crate) fn check(&self, pairs: &dyn Pairs) -> Result<()> {
        check_correlation(self.correlation_id)?;
        check_symbol(self.symbol_id)?;
        check_ref(&self.order)?;
        Pair::of(pairs, self.symbol_id)?.check_replace(self.qty.as_ref(), self.price.as_ref())
    }
}

impl CancelAllOrders {
    pub(crate) fn check(&self) -> Result<()> {
        check_correlation(self.correlation_id)?;
        if self.symbol_ids.len() > MAX_CANCEL_ALL_SYMBOLS {
            return Err(Error::validation(format!(
                "tradews cancel-all names at most {MAX_CANCEL_ALL_SYMBOLS} pairs"
            )));
        }
        self.symbol_ids.iter().try_for_each(|id| check_symbol(*id))
    }
}

impl BatchCreateOrders {
    pub(crate) fn check(&self, pairs: &dyn Pairs) -> Result<()> {
        check_correlation(self.correlation_id)?;
        if self.orders.is_empty() || self.orders.len() > MAX_BATCH_CREATE {
            return Err(Error::validation(format!(
                "tradews batch create carries 1 to {MAX_BATCH_CREATE} orders"
            )));
        }
        self.orders
            .iter()
            .try_for_each(|o| o.check(pairs).map(drop))
    }
}

impl BatchReplaceOrders {
    pub(crate) fn check(&self, pairs: &dyn Pairs) -> Result<()> {
        check_correlation(self.correlation_id)?;
        check_symbol(self.symbol_id)?;
        if self.items.is_empty() || self.items.len() > MAX_BATCH_REPLACE {
            return Err(Error::validation(format!(
                "tradews batch replace carries 1 to {MAX_BATCH_REPLACE} items"
            )));
        }
        let pair = Pair::of(pairs, self.symbol_id)?;
        self.items.iter().try_for_each(|item| {
            check_ref(&item.order)?;
            pair.check_replace(item.qty.as_ref(), item.price.as_ref())
        })
    }
}

impl BatchCancelOrders {
    pub(crate) fn check(&self) -> Result<()> {
        check_correlation(self.correlation_id)?;
        if self.items.is_empty() || self.items.len() > MAX_BATCH_CANCEL {
            return Err(Error::validation(format!(
                "tradews batch cancel carries 1 to {MAX_BATCH_CANCEL} items"
            )));
        }
        self.items.iter().try_for_each(|item| {
            check_symbol(item.symbol_id)?;
            check_ref(&item.order)
        })
    }
}

/// The whole seconds of a cancel-all-after timeout: zero, or 10 to 120.
fn timer_seconds(timeout: Duration) -> Result<u32> {
    if timeout.is_zero() {
        return Ok(0);
    }
    if timeout.subsec_nanos() != 0
        || timeout < MIN_CANCEL_ALL_AFTER
        || timeout > MAX_CANCEL_ALL_AFTER
    {
        return Err(Error::validation(
            "tradews cancel-all-after timeout must be zero or whole seconds from 10 to 120",
        ));
    }
    Ok(timeout.as_secs() as u32)
}
