//! Trading read/write models (Go `models/trading.go` parity).

use crate::types::{AssetAmount, Price, Quantity};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateResultItem {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateOrdersResult {
    pub results: Vec<BatchCreateResultItem>,
    pub accepted_count: i32,
    pub rejected_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelItem {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub symbol_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelResultItem {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelOrdersResult {
    pub results: Vec<BatchCancelResultItem>,
    pub accepted_count: i32,
    pub rejected_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchModifyItem {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub new_price: Option<Price>,
    pub new_qty: Option<Quantity>,
    pub new_attached_risk: Option<AttachedRisk>,
    pub behavior: Option<String>,
    pub new_client_order_id: Option<String>,
}

/// Typed single-order modify params (POLY-3262 wrappers-only).
#[derive(Debug, Clone)]
pub struct ModifyOrderParams {
    pub symbol: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub subaccount_id: Option<u64>,
    pub request_id: Option<String>,
    pub new_price: Option<Price>,
    pub new_qty: Option<Quantity>,
    pub new_attached_risk: Option<AttachedRisk>,
    pub behavior: Option<String>,
    pub new_client_order_id: Option<String>,
}

/// Price source for attached TP/SL/trailing trigger evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerPriceSourceKind {
    LastPrice,
    IndexPrice,
    MarkPrice,
}

/// Take-profit or stop-loss leg (trigger + optional LIMIT child).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskLeg {
    pub trigger_price: Price,
    pub trigger_price_source: Option<TriggerPriceSourceKind>,
    pub order_type: Option<CreateOrderType>,
    pub limit_price: Option<Price>,
}

/// Trailing-stop distance (exactly one of ticks or bps).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrailingDistance {
    Ticks(i64),
    Bps(i32),
}

/// Optional max slippage for trailing-stop MARKET children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxSlippage {
    /// Quote ticks (proto field is int32).
    Ticks(i32),
    Bps(i32),
}

/// Trailing-stop attached-risk leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingStop {
    pub distance: TrailingDistance,
    pub activation_price: Option<Price>,
    pub trigger_price_source: Option<TriggerPriceSourceKind>,
    pub order_type: Option<CreateOrderType>,
    pub max_slippage: Option<MaxSlippage>,
}

/// Typed attached risk policy for order create/modify (TP/SL/trailing).
///
/// Prefer this over raw proto/`map` escape hatches: trigger/limit prices use [`Price`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachedRisk {
    pub take_profit: Option<RiskLeg>,
    pub stop_loss: Option<RiskLeg>,
    pub trailing_stop: Option<TrailingStop>,
    /// When true, take-profit and the stop leg form an OCO pair.
    pub oco: bool,
}

/// Typed internal-transfer create params (POLY-3262 `AssetAmount`).
#[derive(Debug, Clone)]
pub struct CreateInternalTransferParams {
    pub asset_id: u32,
    pub quantity: AssetAmount,
    pub idempotency_key: String,
    pub subaccount_id: Option<u64>,
    pub destination_account_id: Option<String>,
    pub destination_subaccount_id: Option<String>,
    pub destination_smart_account_address: Option<String>,
    /// Override ledger scale (default 18).
    pub quantity_scale: Option<u32>,
}

/// Typed trading-withdraw create params (POLY-3262 `AssetAmount`).
#[derive(Debug, Clone)]
pub struct CreateTradingWithdrawParams {
    pub asset_id: u32,
    pub amount: AssetAmount,
    pub payload_signature: Vec<u8>,
    pub destination_address: String,
    pub idempotency_key: Option<String>,
    /// Override ledger scale (default 18).
    pub amount_scale: Option<u32>,
    pub deadline_ts_sec: Option<u64>,
    pub nonce: Option<u128>,
}

/// Typed wallet trading-withdraw create params (POLY-3262 `AssetAmount`).
#[derive(Debug, Clone)]
pub struct CreateWalletTradingWithdrawParams {
    pub action: String,
    pub asset_id: u32,
    pub amount: AssetAmount,
    pub idempotency_key: String,
    pub payload_signature: Vec<u8>,
    pub signer_wallet: String,
    pub destination_chain_id: u64,
    pub destination_address: String,
    pub subaccount_id: Option<u64>,
    pub amount_scale: Option<u32>,
    pub deadline_ts_sec: Option<u64>,
    pub nonce: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchModifyResultItem {
    pub status: String,
    pub client_order_id: String,
    pub final_order_id: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchModifyOrdersResult {
    pub results: Vec<BatchModifyResultItem>,
    pub amended_count: i32,
    pub replaced_count: i32,
    pub rejected_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllAfterResult {
    pub status: String,
    pub effective_timeout_sec: i32,
    pub expires_at_ts_ns: String,
}

/// Options for [`crate::services::OrdersService::list_open_with`].
#[derive(Debug, Clone, Default)]
pub struct ListOpenOrdersOpts {
    pub subaccount_id: Option<u64>,
    pub page_token: Option<String>,
    pub limit: Option<u32>,
    pub include_attached_risk: bool,
    pub include_attached_risk_state: bool,
}

/// Options for [`crate::services::OrdersService::list_history_with`].
#[derive(Debug, Clone, Default)]
pub struct ListOrderHistoryOpts {
    pub subaccount_id: Option<u64>,
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub page_token: Option<String>,
    pub limit: Option<u32>,
    pub include_attached_risk: bool,
    pub include_attached_risk_state: bool,
}

/// Options for [`crate::services::OrdersService::get_with`].
#[derive(Debug, Clone, Default)]
pub struct GetOrderOpts {
    pub client_order_id: Option<String>,
    pub order_id: Option<String>,
    pub subaccount_id: Option<u64>,
    pub include_attached_risk: bool,
    pub include_attached_risk_state: bool,
}

/// Params for [`crate::services::OrdersService::cancel_with`].
#[derive(Debug, Clone, Default)]
pub struct CancelOrderParams {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub subaccount_id: Option<u64>,
}

/// Options for [`crate::services::OrdersService::cancel_all_with`].
#[derive(Debug, Clone, Default)]
pub struct CancelAllOpts {
    pub symbol: Option<String>,
    pub dry_run: bool,
    pub subaccount_id: Option<u64>,
    pub side: Option<String>,
    pub request_id: Option<String>,
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
    /// Optional TP/SL/trailing controls that arm after the parent fills.
    pub attached_risk: Option<AttachedRisk>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalTransferResult {
    pub request_id: String,
    pub transfer_id: String,
    pub asset_id: u32,
    pub asset_code: String,
    pub quantity: Option<AssetAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepositAddress {
    pub chain_id: u32,
    pub deposit_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepositAddressesList {
    pub addresses: Vec<DepositAddress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawIntentResult {
    pub intent_id: String,
    pub status: String,
    pub flow_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeySummary {
    pub key_id: String,
    pub label: String,
    pub status: String,
    pub public_key_ed25519: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeysList {
    pub keys: Vec<ApiKeySummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAccount {
    pub account_id: String,
    pub username: String,
    pub smart_account_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAccountsList {
    pub accounts: Vec<ResolvedAccount>,
}
