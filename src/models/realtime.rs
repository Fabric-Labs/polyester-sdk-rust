//! Realtime publication models (Go `models/realtime.go` parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceQtyPair {
    pub price_ticks: i64,
    pub qty_scaled: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookDeltaUpdate {
    pub symbol_id: u32,
    pub book_seq_start: u64,
    pub book_seq_end: u64,
    pub reset: bool,
    pub bids: Vec<PriceQtyPair>,
    pub asks: Vec<PriceQtyPair>,
}

/// One delivery of an account's live order stream
/// ([`crate::services::OrdersService::subscribe`]).
#[derive(Debug, Clone)]
pub enum PrivateOrdersEvent {
    /// The server confirmed the subscription. Sent again after every
    /// reconnect or resubscribe that followed a [`Self::Disconnected`].
    Connected,
    /// The account's full live view, ascending by order ID. Replace local
    /// state with it: orders missing from it left the live view.
    Snapshot(Vec<crate::models::Order>),
    /// One change, applied whole.
    Unit(OrdersUnit),
    /// The account's live view is temporarily behind (`true`) or caught up
    /// again (`false`). The stream recovers by itself.
    Stale(bool),
    /// The connection or subscription carrying the stream dropped. It resumes
    /// from the last applied position by itself unless an [`Self::Error`]
    /// ended the stream.
    Disconnected,
    /// A stream error. A terminal error is followed by
    /// [`Self::Disconnected`] and the end of the stream.
    Error(crate::errors::Error),
}

/// One change to an account's live order view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrdersUnit {
    /// Each changed order, complete. Replace the local copy by `order_id`;
    /// a higher `version` is always newer and equal versions are identical.
    pub orders: Vec<crate::models::Order>,
    /// Public IDs of orders that left the live view. A removal is not a
    /// cancellation: ended orders stay readable through the order reads.
    pub removed_order_ids: Vec<String>,
    /// What happened, in order and never coalesced.
    pub events: Vec<OrderUnitEvent>,
}

/// One event inside an [`OrdersUnit`].
///
/// Event kinds this SDK does not know yet are skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderUnitEvent {
    Order(OrderStreamEvent),
    Outcome(OrderCommandOutcome),
}

/// Something that happened to an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderStreamEvent {
    pub order_id: String,
    pub version: u32,
    /// `accepted`, `filled`, `canceled`, `rejected`, `replaced`,
    /// `attached_risk`, or `UNKNOWN(<number>)` for a future kind.
    pub kind: String,
    /// The reason for a cancel, rejection or replace; 0 when none applies.
    pub reason_code: u32,
    /// The successor's public ID for a `replaced` event.
    pub replacement_order_id: Option<String>,
    /// The execution for a `filled` event.
    pub fill: Option<OrderFill>,
}

/// One execution of an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFill {
    /// The user trade's `match_id`.
    pub match_id: String,
    pub ordinal: u32,
    /// `maker`, `taker`, or `UNKNOWN(<number>)`.
    pub role: String,
    /// Filled base quantity. The stream does not carry the scale; resolve it
    /// from the catalog by the order's `symbol_id`.
    pub qty: Option<crate::types::Quantity>,
    pub price: Option<crate::types::Price>,
}

/// The outcome of an order command that changed no order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCommandOutcome {
    /// `amend_rejected`, `cancel_rejected`, `replace_rejected`, `rejected`,
    /// `cancel_all_done`, or `UNKNOWN(<number>)`.
    pub kind: String,
    pub order_id: Option<String>,
    pub new_order_id: Option<String>,
    pub client_order_id: String,
    /// The order error code the Orders API returns for the same refusal
    /// (for example `ERROR_CODE_ORDER_UNKNOWN`), `UNKNOWN(<number>)` for a
    /// code this SDK does not know, or empty when none applies.
    pub reason: String,
}
