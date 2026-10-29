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
    pub trailing_distance_ticks: Option<i64>,
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
    pub self_trade_prevention_mode: Option<String>,
}

/// Typed modify-trigger params.
///
/// Omitted `activation_price` / `max_slippage_*` preserve the current values.
/// Send an explicit zero (`Price::from_ticks(0)` or `Some(0)`) to clear an
/// existing activation price or maximum-slippage cap.
#[derive(Debug, Clone)]
pub struct ModifyTriggerParams {
    pub trigger_id: String,
    pub subaccount_id: Option<u64>,
    /// Display symbol; resolved to Connect `symbol_id`. Mutually exclusive with [`Self::symbol_id`].
    pub symbol: Option<String>,
    /// Connect `symbol_id`. Mutually exclusive with [`Self::symbol`].
    pub symbol_id: Option<u32>,
    pub trigger_price: Option<Price>,
    pub activation_price: Option<Price>,
    pub trailing_distance_ticks: Option<i64>,
    pub trailing_distance_bps: Option<i32>,
    /// Slippage cap of a trailing stop, or of a stop-loss or take-profit with
    /// a market child. Zero clears the cap; a cleared cap on a market child
    /// falls back to the pair's default market slippage.
    pub max_slippage_ticks: Option<i64>,
    pub max_slippage_bps: Option<i32>,
}

/// Typed resume-trigger params. Connect requires `symbol_id`.
#[derive(Debug, Clone)]
pub struct ResumeTriggerParams {
    pub trigger_id: String,
    pub subaccount_id: Option<u64>,
    /// Display symbol; resolved to Connect `symbol_id`. Mutually exclusive with [`Self::symbol_id`].
    pub symbol: Option<String>,
    /// Connect `symbol_id`. Mutually exclusive with [`Self::symbol`].
    pub symbol_id: Option<u32>,
}

/// Valid trigger status filter / response labels (British "cancelled").
pub const TRIGGER_STATUS_VALUES: &[&str] = &[
    "created",
    "armed",
    "running",
    "completed",
    "cancelled",
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerStopDetails {
    pub trigger_price: Option<Price>,
    pub trigger_price_source: String,
    pub trigger_direction: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTrailingDetails {
    pub trailing_distance: Option<Price>,
    pub trailing_distance_bps: i32,
    pub activation_price: Option<Price>,
    /// Highest price seen, for a SELL trailing stop.
    pub peak_price: Option<Price>,
    /// Lowest price seen, for a BUY trailing stop.
    pub trough_price: Option<Price>,
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
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub qty: Option<Quantity>,
    /// Child limit price; for a BUY trailing stop, its maximum price.
    pub limit_price: Option<Price>,
    pub fee_asset: String,
    pub self_trade_prevention_mode: String,
    pub post_only: bool,
    /// Convenience: stop-trigger price when details are `Stop`.
    pub trigger_price: Option<Price>,
    /// Slippage cap configured on a stop-loss or take-profit market child or
    /// on a trailing stop; `None` when none is set (a market child then uses
    /// the pair's default market slippage).
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEvent {
    pub trigger_id: String,
    pub subaccount_id: String,
    pub symbol_id: u32,
    pub trigger_type: String,
    pub event_type: String,
    pub ts_ns: String,
    pub child_seq: i32,
    pub child_order_id: String,
    pub fire_price: Option<Price>,
    /// Proto `terminal_reason.cancel_reason`. Empty when unset or unspecified.
    pub cancel_reason: String,
    /// Proto `terminal_reason.failure_reason`. Empty when unset or unspecified.
    pub failure_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEventsList {
    pub events: Vec<TriggerEvent>,
    pub next_page_token: String,
}
