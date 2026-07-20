//! Trigger SDK models (Go `models` parity).

use super::{CreateOrderType, CreateSide, CreateTimeInForce};
use crate::types::{Price, Quantity};

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
    pub client_trigger_id: Option<String>,
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
    pub fee_source: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub trigger_id: String,
    pub symbol_id: u32,
    pub symbol: String,
    pub trigger_type: String,
    pub status: String,
    pub side: String,
    pub qty: Option<Quantity>,
    pub trigger_price: Option<Price>,
    pub client_trigger_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggersList {
    pub triggers: Vec<Trigger>,
    pub total: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerMutationResult {
    pub trigger_id: String,
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
}
