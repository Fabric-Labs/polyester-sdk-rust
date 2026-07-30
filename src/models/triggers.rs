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
    pub limit_price: Option<Price>,
    pub trigger_price_source: Option<String>,
    pub time_in_force: Option<CreateTimeInForce>,
    pub subaccount_id: Option<u64>,
    /// Stable identity for this logical trigger. Persist and reuse it on retries.
    pub client_trigger_id: String,
    pub post_only: bool,
    pub activation_price: Option<Price>,
    pub trailing_distance_ticks: Option<i64>,
    pub trailing_distance_bps: Option<i32>,
    pub max_slippage_ticks: Option<i32>,
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
#[derive(Debug, Clone)]
pub struct ModifyTriggerParams {
    pub trigger_id: String,
    pub subaccount_id: Option<u64>,
    pub trigger_price: Option<Price>,
    pub limit_price: Option<Price>,
    pub activation_price: Option<Price>,
    pub trailing_distance_ticks: Option<i64>,
    pub trailing_distance_bps: Option<i32>,
    pub max_slippage_ticks: Option<i32>,
    pub max_slippage_bps: Option<i32>,
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
    /// Status filter labels (`created`, `armed`, …). Unknown values are rejected.
    pub status: Vec<String>,
    pub limit: u32,
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
    pub peak_price: Option<Price>,
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
    pub limit_price: Option<Price>,
    pub fee_asset: String,
    pub self_trade_prevention_mode: String,
    pub post_only: bool,
    /// Convenience: stop-trigger price when details are `Stop`.
    pub trigger_price: Option<Price>,
    pub client_trigger_id: String,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub armed_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub child_order_ids: Vec<String>,
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
    pub event_type: String,
    pub ts_ns: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEventsList {
    pub events: Vec<TriggerEvent>,
    pub next_page_token: String,
}
