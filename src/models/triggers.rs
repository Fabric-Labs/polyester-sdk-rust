//! Trigger SDK models (Go `models` parity).

use super::{CreateOrderType, CreateSide, CreateTimeInForce, FeeAsset};
use crate::types::{Price, Quantity};
use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTriggerType {
    StopLoss,
    TakeProfit,
    TrailingStop,
    Twap,
    Ladder,
}

/// Typed create-trigger params.
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateTriggerParams {
    pub symbol: String,
    pub trigger_type: CreateTriggerType,
    pub side: CreateSide,
    pub order_type: CreateOrderType,
    pub qty: Quantity,
    pub trigger_price: Option<Price>,
    /// Limit price of a limit child. For a [`CreateTriggerType::TrailingStop`]
    /// with side BUY it is required and is the maximum price: the most the
    /// child pays per unit, at least the activation price (or the last trade
    /// price when no activation price is set). Must be `None` for a SELL
    /// trailing stop. Fixed at create.
    pub limit_price: Option<Price>,
    /// Price a stop-loss, take-profit or trailing stop watches, with the
    /// labels trigger reads use: `last`, `index` or `mark`. `None` or empty
    /// watches the last trade price. Fixed at create. Rejected for TWAP and
    /// ladder triggers.
    pub trigger_price_source: Option<String>,
    pub time_in_force: Option<CreateTimeInForce>,
    pub subaccount_id: Option<u64>,
    /// Stable identity for this logical trigger. Persist and reuse it on retries.
    pub client_trigger_id: String,
    pub post_only: bool,
    pub activation_price: Option<Price>,
    /// Trailing distance as a price delta in 1e-9 quote-unit ticks, the scale
    /// of every price tick: on BTC-USDT, 50 USDT is `50_000_000_000` and
    /// 0.5 USDT is `500_000_000` (`Price::from_decimal_str("0.5", None)?.as_ticks()`).
    /// It is not a count of the pair's tick size.
    pub trailing_distance_ticks: Option<i64>,
    /// Trailing distance in basis points of the price, from 1 to 10000.
    pub trailing_distance_bps: Option<i32>,
    /// Slippage cap (at most one of ticks or bps) for a trailing stop, a
    /// TWAP market slice, or a stop-loss / take-profit market child. Without
    /// one, a market child uses the pair's default market slippage. Rejected
    /// for a stop-loss / take-profit limit child.
    pub max_slippage_ticks: Option<i64>,
    pub max_slippage_bps: Option<i32>,
    pub twap_duration_ms: Option<i64>,
    pub twap_slice_interval_ms: Option<i64>,
    pub ladder_price_min: Option<Price>,
    pub ladder_price_max: Option<Price>,
    pub ladder_levels: Option<i32>,
    pub ladder_distribution: Option<String>,
    /// Fee asset. `base` is valid only for BUY child orders.
    pub fee_asset: Option<FeeAsset>,
    /// Self-trade prevention mode of the trigger's orders: `expire_taker`,
    /// `expire_maker`, `expire_both` or `decrement`, in any case; `None`
    /// uses the server default, `expire_maker`. Another label is refused
    /// with [`crate::Error::Validation`] before sending.
    pub self_trade_prevention_mode: Option<String>,
}

/// Typed modify-trigger params.
///
/// Omitted `activation_price` / `max_slippage_*` preserve the current values.
/// Send an explicit zero (`Price::from_ticks(0)` or `Some(0)`) to clear an
/// existing activation price or maximum-slippage cap.
impl CreateTriggerParams {
    /// Params with only the required fields set; assign the type's optional
    /// fields (`trigger_price`, `limit_price`, trailing distance, TWAP or
    /// ladder settings, …) afterwards.
    pub fn new(
        symbol: impl Into<String>,
        trigger_type: CreateTriggerType,
        side: CreateSide,
        order_type: CreateOrderType,
        qty: Quantity,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            trigger_type,
            side,
            order_type,
            qty,
            trigger_price: None,
            limit_price: None,
            trigger_price_source: None,
            time_in_force: None,
            subaccount_id: None,
            client_trigger_id: String::new(),
            post_only: false,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
            twap_duration_ms: None,
            twap_slice_interval_ms: None,
            ladder_price_min: None,
            ladder_price_max: None,
            ladder_levels: None,
            ladder_distribution: None,
            fee_asset: None,
            self_trade_prevention_mode: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModifyTriggerParams {
    pub trigger_id: String,
    pub subaccount_id: Option<u64>,
    /// Display symbol; resolved to Connect `symbol_id`. Mutually exclusive with [`Self::symbol_id`].
    /// Optional: leave both out to target the trigger's own market. An API
    /// key restricted to some markets must name it.
    pub symbol: Option<String>,
    /// Connect `symbol_id`. Mutually exclusive with [`Self::symbol`].
    pub symbol_id: Option<u32>,
    pub trigger_price: Option<Price>,
    pub activation_price: Option<Price>,
    /// New trailing distance as a price delta in 1e-9 quote-unit ticks, as on
    /// [`CreateTriggerParams::trailing_distance_ticks`].
    pub trailing_distance_ticks: Option<i64>,
    pub trailing_distance_bps: Option<i32>,
    /// Slippage cap of a trailing stop, or of a stop-loss or take-profit with
    /// a market child. `max_slippage_bps` is 1 to 10000 here: a buy
    /// stop-loss or take-profit market child takes up to 10000, as at
    /// creation, and the server refuses 10000 for a trailing stop and a sell
    /// one (`MAX_SLIPPAGE_INVALID`). Zero clears the cap; a cleared cap on a
    /// market child falls back to the pair's default market slippage.
    pub max_slippage_ticks: Option<i64>,
    pub max_slippage_bps: Option<i32>,
}

/// Typed resume-trigger params. Connect requires `symbol_id`.
#[derive(Debug, Clone)]
pub struct ResumeTriggerParams {
    pub trigger_id: String,
    pub subaccount_id: Option<u64>,
    /// Display symbol; resolved to Connect `symbol_id`. Mutually exclusive with [`Self::symbol_id`].
    /// Optional: leave both out to target the trigger's own market. An API
    /// key restricted to some markets must name it.
    pub symbol: Option<String>,
    /// Connect `symbol_id`. Mutually exclusive with [`Self::symbol`].
    pub symbol_id: Option<u32>,
}

/// One leg of an OCO: the price that triggers it and the order it then places
/// for the OCO's whole remaining quantity.
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with [`Self::market`] or [`Self::limit`] and sets fields afterwards instead
/// of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OcoLegParams {
    /// Price that triggers the leg.
    pub trigger_price: Price,
    /// The order the leg places: [`CreateOrderType::Market`] places a market
    /// order (IOC), [`CreateOrderType::Limit`] a resting limit order (GTC) at
    /// [`Self::limit_price`].
    pub order_type: CreateOrderType,
    /// Limit price of a limit leg's order. Required for a limit leg; must be
    /// `None` for a market leg.
    pub limit_price: Option<Price>,
    /// Price the leg watches, with the labels trigger reads use: `last`,
    /// `index` or `mark`. `None` or empty watches the last trade price. The
    /// server decides which sources a market supports: on a market without an
    /// index price, an `index` OCO is rejected on both legs, and `mark` is not
    /// supported. Another label is refused before sending.
    pub trigger_price_source: Option<String>,
}

impl OcoLegParams {
    /// A leg that places a market order (IOC) when `trigger_price` is reached.
    pub fn market(trigger_price: Price) -> Self {
        Self {
            trigger_price,
            order_type: CreateOrderType::Market,
            limit_price: None,
            trigger_price_source: None,
        }
    }

    /// A leg that places a resting limit order (GTC) at `limit_price` when
    /// `trigger_price` is reached.
    pub fn limit(trigger_price: Price, limit_price: Price) -> Self {
        Self {
            trigger_price,
            order_type: CreateOrderType::Limit,
            limit_price: Some(limit_price),
            trigger_price_source: None,
        }
    }
}

/// Typed create-OCO params: a one-cancels-the-other pair of a take-profit and
/// a stop-loss over a quantity the account already holds.
///
/// Both legs trade `side` for `qty`, which is reserved once, when the OCO is
/// created, and shared by both legs. A SELL OCO protects a held base
/// position: its take-profit triggers above the last price and its stop-loss
/// below it. A BUY OCO buys with held quote: its take-profit triggers below
/// the last price and its stop-loss above it. The first leg to reach its
/// trigger price places its order for the OCO's remaining quantity; when the
/// whole quantity has traded, the other leg ends canceled. Cancelling either
/// leg, or the OCO with `triggers.cancel_oco_by_id`, cancels both.
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with [`Self::new`] and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateOcoParams {
    pub symbol: String,
    /// The side both legs trade.
    pub side: CreateSide,
    /// Quantity both legs trade; a multiple of the pair's quantity step.
    pub qty: Quantity,
    /// Stable identity for this logical OCO, unique per sub-account and pair
    /// among OCOs. Persist and reuse it on retries: while the OCO is active
    /// and for 60 seconds after both legs ended, a create with the identical
    /// params and the same ID returns the same OCO, and different params with
    /// the same ID are refused with `CONFLICT_DUPLICATE_CLIENT_TRIGGER_ID`.
    /// Any 1 to 36 bytes of UTF-8, sent exactly as given (never trimmed), so
    /// [`ListTriggersOpts::client_list_id`] finds the OCO with the same
    /// string; another length is refused with [`crate::Error::Validation`]
    /// before sending.
    pub client_list_id: String,
    /// The take-profit leg. The server requires its trigger price to be above
    /// the stop-loss's for a SELL OCO and below it for a BUY OCO.
    pub take_profit: OcoLegParams,
    /// The stop-loss leg.
    pub stop_loss: OcoLegParams,
    pub subaccount_id: Option<u64>,
    /// `expire_taker`, `expire_maker`, `expire_both` or `decrement`, in any
    /// case; `None` uses the server default, `expire_maker`. Another label is
    /// refused with [`crate::Error::Validation`] before sending. The server
    /// judges whether an OCO takes `decrement`.
    pub self_trade_prevention_mode: Option<String>,
}

impl CreateOcoParams {
    /// Params with only the required fields set; assign `client_list_id` and
    /// the optional fields afterwards.
    pub fn new(
        symbol: impl Into<String>,
        side: CreateSide,
        qty: Quantity,
        take_profit: OcoLegParams,
        stop_loss: OcoLegParams,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            qty,
            client_list_id: String::new(),
            take_profit,
            stop_loss,
            subaccount_id: None,
            self_trade_prevention_mode: None,
        }
    }
}

/// Valid trigger status filter / response labels, spelled as the contract
/// and trigger events spell them (`canceled`).
pub const TRIGGER_STATUS_VALUES: &[&str] = &[
    "created",
    "armed",
    "running",
    "completed",
    "canceled",
    "failed",
    "paused",
];

/// Typed list-triggers options.
#[derive(Debug, Clone, Default)]
pub struct ListTriggersOpts {
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    /// Status filter labels (`created`, `armed`, …). Unknown values are rejected.
    pub status: Vec<String>,
    /// Omitted or `Some(0)` uses the server default.
    pub limit: Option<u32>,
    pub page_token: Option<String>,
    pub subaccount_id: Option<u64>,
    /// Only triggers of this type (the contract's `trigger_type` filter).
    /// `None` lists every type.
    pub trigger_type: Option<CreateTriggerType>,
    /// Only the triggers attached to this order (the contract's
    /// `parent_order_id` filter), as order reads and
    /// [`Trigger::parent_order_id`] spell it, in base58 or decimal. An ID
    /// that does not parse, or names order 0, is refused before sending.
    /// `None` lists standalone and attached triggers alike.
    pub parent_order_id: Option<String>,
    /// Only the take-profit and stop-loss of this OCO (the contract's
    /// `order_list_id` filter), as `CreateOcoResult::order_list_id` and
    /// [`Trigger::order_list_id`] spell it, in base58 or decimal. An ID that
    /// does not parse, or names 0, is refused before sending. `None` lists
    /// every trigger.
    pub order_list_id: Option<String>,
    /// Only the take-profit and stop-loss of the OCO created with this
    /// `client_list_id` (the contract's `client_list_id` filter), sent exactly
    /// as given, never trimmed: pass the key the create used. Use it to learn
    /// whether a create whose outcome is unknown made the OCO. 1 to 36 bytes
    /// of UTF-8; another length is refused with [`crate::Error::Validation`]
    /// before sending. `None` lists every trigger.
    pub client_list_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerStopDetails {
    pub trigger_price: Option<Price>,
    pub trigger_price_source: String,
    pub trigger_direction: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTrailingDetails {
    /// Trailing distance in quote units, decoded from 1e-9 quote-unit ticks
    /// like every price.
    pub trailing_distance: Option<Price>,
    pub trailing_distance_bps: i32,
    pub activation_price: Option<Price>,
    /// Highest price seen, for a SELL trailing stop.
    pub peak_price: Option<Price>,
    /// Lowest price seen, for a BUY trailing stop.
    pub trough_price: Option<Price>,
    /// Current trigger threshold. It moves with the peak or trough, and is
    /// `None` until the trailing stop is armed.
    pub trigger_price: Option<Price>,
    pub max_slippage: Option<Price>,
    pub max_slippage_bps: i32,
    pub trigger_price_source: String,
    pub trigger_direction: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTwapDetails {
    pub twap_duration_ms: i64,
    pub twap_slice_interval_ms: i64,
    pub slice_idx: i32,
    pub slice_count: i32,
    pub executed_qty: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerLadderDetails {
    pub ladder_price_min: Option<Price>,
    pub ladder_price_max: Option<Price>,
    pub ladder_levels: i32,
    pub ladder_distribution: String,
    pub executed_levels: i32,
    pub executed_qty: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerDetails {
    Stop(TriggerStopDetails),
    Trailing(TriggerTrailingDetails),
    Twap(TriggerTwapDetails),
    Ladder(TriggerLadderDetails),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    pub trigger_id: String,
    pub subaccount_id: String,
    pub symbol_id: u32,
    pub symbol: String,
    pub trigger_type: String,
    pub status: String,
    pub parent_order_id: Option<String>,
    /// Public ID of the OCO this take-profit or stop-loss belongs to: both
    /// triggers of an OCO created with `triggers.create_oco` carry it, and
    /// neither has a parent order. `None` for any other trigger.
    pub order_list_id: Option<String>,
    /// The `client_list_id` of the OCO this take-profit or stop-loss belongs
    /// to, as given to `triggers.create_oco`; both triggers of the OCO carry
    /// it. Empty for any other trigger.
    pub client_list_id: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub qty: Option<Quantity>,
    /// Child limit price; for a BUY trailing stop, its maximum price.
    pub limit_price: Option<Price>,
    pub fee_asset: String,
    /// `expire_taker`, `expire_maker`, `expire_both` or `decrement`, empty
    /// when unset, or `UNKNOWN(<number>)` for a mode this SDK does not know.
    pub self_trade_prevention_mode: String,
    pub post_only: bool,
    /// Convenience: stop-trigger price when details are `Stop`.
    pub trigger_price: Option<Price>,
    /// Slippage cap configured on a stop-loss or take-profit market child,
    /// on a trailing stop, or on each slice of a market TWAP, in the variant
    /// it was set in (ticks or bps); `None` when none is set (a market child
    /// or TWAP slice then uses the pair's default market slippage).
    pub max_slippage: Option<super::MaxSlippage>,
    pub client_trigger_id: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub armed_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    /// Proto `terminal_reason.cancel_reason`. Empty when unset or unspecified.
    pub cancel_reason: String,
    /// Proto `terminal_reason.failure_reason`. Empty when unset or unspecified.
    pub failure_reason: String,
    pub details: Option<TriggerDetails>,
    /// State version: 1 for the trigger's first state, higher for every later
    /// state change. Reads and realtime updates carry the same version for
    /// the same state; keep the copy with the highest version and ignore one
    /// whose version is not higher than the one you hold. `None` from a
    /// server that predates trigger versions.
    pub version: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggersList {
    pub triggers: Vec<Trigger>,
    pub total: usize,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerMutationResult {
    pub trigger_id: String,
    pub client_trigger_id: String,
    pub status: String,
}

/// An OCO's creation: its quantity is reserved and both legs are created.
/// Read the legs' status and orders with the trigger reads and events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOcoResult {
    /// Public ID of the OCO, which both legs carry.
    pub order_list_id: String,
    /// Echoed client OCO ID.
    pub client_list_id: String,
    /// Public trigger ID of the take-profit leg.
    pub take_profit_trigger_id: String,
    /// Public trigger ID of the stop-loss leg.
    pub stop_loss_trigger_id: String,
    /// When admission completed, in nanoseconds since the Unix epoch: the
    /// contract's `accepted_at_ts_ns`, or `accepted_at` when only that is
    /// sent; 0 when neither is.
    pub accepted_ts_ns: u64,
}

/// An OCO's cancellation: both legs end canceled and the OCO's reserved
/// quantity is released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOcoResult {
    /// Public ID of the canceled OCO.
    pub order_list_id: String,
    /// Both legs' status after the cancel, which is always `canceled`.
    pub status: String,
    /// When the cancel was answered, in nanoseconds since the Unix epoch:
    /// `ts_ns`, or `ts` when only that is sent; 0 when neither is.
    pub ts_ns: u64,
}

/// The outcome of one trigger of a batch cancel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelTriggersResultItem {
    /// `accepted`: the trigger's events report when it is canceled.
    /// `rejected`: `code` says why.
    pub status: String,
    /// The requested trigger's public ID.
    pub trigger_id: String,
    /// Rejection code, empty when accepted. `TRIGGER_NOT_FOUND`: no trigger
    /// of the account has this ID. `TRIGGER_STATUS_INVALID`: the trigger has
    /// already ended; treat it as done. `RATE_LIMIT_EXCEEDED`,
    /// `PAIR_BACKLOGGED` and `TRIGGER_CONTROL_IN_PROGRESS`: retry the trigger
    /// in a new request with a new `request_id`. Other codes are those
    /// `triggers.cancel` returns.
    pub code: String,
    /// Rate-limit guidance, when the item was rate limited.
    pub rate_limit: Option<super::RateLimitDetail>,
    /// Field violations a rejection names, in the order the server sent them;
    /// empty for an accepted item or a rejection that names no field.
    pub violations: Vec<super::OrderFieldViolation>,
}

/// Results of a batch trigger cancel, one per requested trigger, in request
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelTriggersResult {
    pub results: Vec<BatchCancelTriggersResultItem>,
    pub accepted_count: u32,
    pub rejected_count: u32,
    /// The server time of the answer, in nanoseconds since the Unix epoch:
    /// `ts_ns`, or `ts` when only that is sent; 0 when neither is.
    pub ts_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEvent {
    pub trigger_id: String,
    pub subaccount_id: String,
    pub symbol_id: u32,
    pub trigger_type: String,
    /// `fired`, `canceled`, `updated`, `failed`, `activated` (a trailing stop
    /// started tracking), `armed` (an attached take-profit, stop-loss or
    /// trailing stop started protecting its parent's filled quantity, once) or
    /// `fire_refused`. A ladder has no event types of its own: read its
    /// status, `completed`, `canceled` or `failed`.
    pub event_type: String,
    pub ts_ns: String,
    /// The fire attempt the event names. A `fire_refused` event uses a
    /// `child_seq` of its own; the next attempt takes the next one. Two
    /// exceptions name an earlier attempt instead: with the
    /// `self_trade_prevented` reason, the child order self-trade prevention
    /// expired; on a ladder, a level the order book rejected when it arrived
    /// (for example `post_only_cross`), which follows that level's `fired`
    /// event with the same `child_seq` while the ladder keeps working its
    /// other levels. A ladder level whose `fired` is followed by a
    /// `fire_refused` of the same `child_seq` was not placed: do not count
    /// it.
    pub child_seq: i32,
    /// Child order the fire placed. Empty on a `fire_refused` event that
    /// placed no order.
    pub child_order_id: String,
    pub fire_price: Option<Price>,
    /// Proto `terminal_reason.cancel_reason`. Empty when unset or unspecified.
    pub cancel_reason: String,
    /// Proto `terminal_reason.failure_reason`. Empty when unset or unspecified.
    pub failure_reason: String,
    /// Why the fire attempt of a `fire_refused` event was refused while the
    /// trigger stayed live, as a failure reason label (`rate_limited`,
    /// `account_repairing`, `market_halted`, `min_notional`, ...), or
    /// `UNKNOWN_TRIGGER_FAILURE_REASON(<n>)` for a reason this SDK does not
    /// know. Empty on every other event type.
    pub fire_refusal_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEventsList {
    pub events: Vec<TriggerEvent>,
    pub next_page_token: String,
}
