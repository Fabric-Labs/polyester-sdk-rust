//! Trigger SDK models (Go `models` parity).

use crate::types::{Price, Quantity};

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
