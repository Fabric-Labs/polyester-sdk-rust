//! Trading read/write models (Go `models/trading.go` parity).

use crate::types::{Price, Quantity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub order_id: String,
    pub symbol_id: u32,
    pub client_order_id: String,
    pub side: String,
    pub status: String,
    pub order_type: String,
    pub tif: String,
    pub orig_qty: Option<Quantity>,
    pub cum_qty: Option<Quantity>,
    pub leaves_qty: Option<Quantity>,
    pub price: Option<Price>,
    pub avg_px: Option<Price>,
    pub created_ts_ns: String,
    pub state_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrdersList {
    pub orders: Vec<Order>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderMutationResult {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOrderResult {
    pub order: Option<Order>,
    pub trades: Vec<UserTrade>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserTrade {
    pub symbol_id: u32,
    pub match_id: String,
    pub order_id: String,
    pub side: String,
    pub is_maker: bool,
    pub price: Option<Price>,
    pub qty: Option<Quantity>,
    pub fee_scaled: String,
    pub ts_ns: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserTradesList {
    pub trades: Vec<UserTrade>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrderResult {
    pub action_taken: String,
    pub old_order_id: String,
    pub final_order_id: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllOrdersResult {
    pub status: String,
    pub matched_orders: i32,
    pub submitted_cancels: i32,
    pub failed_cancels: i32,
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
    /// Client reference price for MARKET order reservation (price ticks domain).
    pub market_client_ref_price: Option<Price>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTimeInForce {
    Gtc,
    Ioc,
    Fok,
}
