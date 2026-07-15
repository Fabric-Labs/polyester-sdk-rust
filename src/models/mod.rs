//! Owned SDK models. Proto owned messages are also re-exported for escape hatches.

use crate::types::{Price, Quantity};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMe {
    pub account_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSummary {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CreateOrderParams {
    pub symbol: String,
    pub side: CreateSide,
    pub order_type: CreateOrderType,
    pub quantity: Quantity,
    pub price: Option<Price>,
    pub time_in_force: Option<CreateTimeInForce>,
    pub client_order_id: Option<String>,
    pub subaccount_id: Option<u64>,
    pub post_only: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
pub enum CreateSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub enum CreateOrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy)]
pub enum CreateTimeInForce {
    Gtc,
    Ioc,
    Fok,
}
