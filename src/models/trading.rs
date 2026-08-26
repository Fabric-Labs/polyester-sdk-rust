//! Trading read/write models (Go `models/trading.go` parity).

use crate::types::{AssetAmount, Price, Quantity};
use buffa_types::google::protobuf::Timestamp;

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
    pub version: u32,
    pub post_only: bool,
    /// Asset selected to pay fees: `quote`, `base`, or an
    /// `UNKNOWN(<number>)` forward-compatible enum value.
    pub fee_asset: String,
    /// Hard all-in quote debit submitted with quote-budget sizing, when used.
    pub submitted_max_quote_debit_scaled: Option<i64>,
    /// Attached risk policy when requested via `include_attached_risk`.
    pub attached_risk: Option<AttachedRisk>,
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
    /// Gross base quantity resolved by the admission service.
    pub resolved_base_qty: Option<Quantity>,
    /// Hard all-in quote debit submitted with quote-budget sizing, when used.
    pub submitted_max_quote_debit_scaled: Option<i64>,
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
    /// Exact fee magnitude in fixed 18-decimal units of `fee_asset`.
    pub fee_amount_e18: String,
    /// Asset used to pay/credit the fee: `quote`, `base`, or an
    /// `UNKNOWN(<number>)` forward-compatible enum value.
    pub fee_asset: String,
    /// Exact referral share magnitude in fixed 18-decimal units of `fee_asset`.
    pub referral_share_amount_e18: String,
    pub ts_ns: String,
    /// True when `fee_amount_e18` is a rebate credit instead of a fee debit.
    /// Proto3 omits false, so sparse wire encoding only sets this for rebates.
    pub fee_is_rebate: bool,
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
    pub matched_orders: u32,
    pub submitted_cancels: u32,
    pub failed_cancels: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateResultItem {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
    pub code: String,
    pub rate_limit: Option<super::RateLimitDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateOrdersResult {
    pub results: Vec<BatchCreateResultItem>,
    pub accepted_count: u32,
    pub rejected_count: u32,
}

/// Identifies an order by exactly one of exchange order id or client order id.
///
/// Matches TypeScript/Go oneOf semantics for get/cancel/modify and batch items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderKey {
    OrderId(String),
    ClientOrderId(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelItem {
    pub key: OrderKey,
    pub symbol_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelResultItem {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
    pub code: String,
    pub rate_limit: Option<super::RateLimitDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCancelOrdersResult {
    pub results: Vec<BatchCancelResultItem>,
    pub accepted_count: u32,
    pub rejected_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceItem {
    pub key: OrderKey,
    pub new_price: Option<Price>,
    pub new_qty: Option<Quantity>,
    pub new_attached_risk: Option<AttachedRisk>,
    pub new_client_order_id: Option<String>,
}

/// Typed single-order modify params.
#[derive(Debug, Clone)]
pub struct ModifyOrderParams {
    pub symbol: String,
    pub key: OrderKey,
    pub subaccount_id: Option<u64>,
    /// Optional mutation request id (API-required on the wire).
    ///
    /// When omitted or blank, the SDK generates a unique id (TypeScript/Go/Python parity).
    /// Set a stable non-empty value when you may retry the same logical modification after an
    /// ambiguous failure, and reuse that same value on retry. A blind retry that omits
    /// `request_id` mints a *new* id and is not an idempotent replay.
    pub request_id: Option<String>,
    pub new_price: Option<Price>,
    pub new_qty: Option<Quantity>,
    pub new_attached_risk: Option<AttachedRisk>,
    pub behavior: Option<String>,
    pub new_client_order_id: Option<String>,
}

/// Price source requested for trigger evaluation.
///
/// Attached order risk currently evaluates against last trade and cannot
/// encode a caller-selected source. Standalone triggers expose their own
/// supported semantics.
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
    /// Deprecated for attached risk: any supplied value is rejected because
    /// the wire contract always evaluates against last trade.
    #[deprecated(
        note = "attached risk always uses last trade; supplying trigger_price_source is rejected"
    )]
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
///
/// Distance and optional max slippage must be positive. The child is always a
/// market-IOC execution evaluated against last trade; supplying
/// [`trigger_price_source`](Self::trigger_price_source) or
/// [`order_type`](Self::order_type) is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingStop {
    pub distance: TrailingDistance,
    pub activation_price: Option<Price>,
    /// Deprecated for attached trailing: any supplied value is rejected because
    /// the wire contract always evaluates against last trade.
    #[deprecated(
        note = "attached trailing always uses last trade; supplying trigger_price_source is rejected"
    )]
    pub trigger_price_source: Option<TriggerPriceSourceKind>,
    /// Deprecated for attached trailing: any supplied value is rejected because
    /// the child is always an implicit market execution.
    #[deprecated(
        note = "attached trailing child is always market; supplying order_type is rejected"
    )]
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

/// Typed internal-transfer create params.
#[derive(Debug, Clone)]
pub struct CreateInternalTransferParams {
    pub asset_id: u32,
    pub quantity: AssetAmount,
    pub idempotency_key: String,
    pub subaccount_id: Option<u64>,
    pub destination_account_id: Option<String>,
    pub destination_subaccount_id: Option<String>,
    pub destination_smart_account_address: Option<String>,
    /// Input quantity scale when `quantity` does not carry one. Wire
    /// `amount_e18` is always rescaled exactly to 18 decimals.
    pub quantity_scale: Option<u32>,
}

/// Typed trading-withdraw create params.
#[derive(Debug, Clone)]
pub struct CreateTradingWithdrawParams {
    pub asset_id: u32,
    pub amount: AssetAmount,
    pub payload_signature: Vec<u8>,
    pub destination_address: String,
    /// Stable key for this logical withdrawal. Persist it and reuse it for
    /// every retry; generating a new key per attempt defeats deduplication.
    pub idempotency_key: String,
    /// Input amount scale when `amount` does not carry one. Wire `amount_e18`
    /// is always rescaled exactly to 18 decimals.
    pub amount_scale: Option<u32>,
    /// Exact deadline covered by `payload_signature`. Required for this
    /// precomputed-signature path.
    pub deadline_ts_sec: Option<u64>,
    /// Non-zero nonce included in the signed withdrawal payload.
    pub nonce: u128,
}

/// API-key trading-withdraw params for SDK-owned payload construction/signing.
#[derive(Debug, Clone)]
pub struct CreateApiKeyTradingWithdrawParams {
    pub asset_id: u32,
    pub amount: AssetAmount,
    pub destination_address: String,
    /// Stable key for this logical withdrawal.
    pub idempotency_key: String,
    /// Input amount scale when `amount` does not carry one. Wire `amount_e18`
    /// is always rescaled exactly to 18 decimals.
    pub amount_scale: Option<u32>,
    /// Optional explicit deadline. The SDK uses now + five minutes when absent.
    pub deadline_ts_sec: Option<u64>,
    /// Optional explicit nonce. The SDK generates a secure non-zero nonce when absent.
    pub nonce: Option<u128>,
}

/// Typed wallet trading-withdraw create params.
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
    /// Input amount scale when `amount` does not carry one. Wire `amount_e18`
    /// is always rescaled exactly to 18 decimals.
    pub amount_scale: Option<u32>,
    /// Exact deadline covered by `payload_signature`. Required for this
    /// precomputed-signature path.
    pub deadline_ts_sec: Option<u64>,
    /// Non-zero nonce included in the signed withdrawal payload.
    pub nonce: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceAdmissionItem {
    pub item_index: u32,
    pub status: String,
    pub old_order_id: String,
    pub client_order_id: String,
    pub replacement_order_id: String,
    pub code: String,
    pub rate_limit: Option<super::RateLimitDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceOrdersResult {
    pub batch_request_id: String,
    pub status: String,
    pub results: Vec<BatchReplaceAdmissionItem>,
    pub accepted_count: u32,
    pub rejected_count: u32,
    pub accepted_ts_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceStatusItem {
    pub item_index: u32,
    pub phase: String,
    pub old_order_id: String,
    pub replacement_order_id: String,
    pub order_status: String,
    pub code: String,
    pub updated_ts_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReplaceStatusResult {
    pub batch_request_id: String,
    pub admission_status: String,
    pub items: Vec<BatchReplaceStatusItem>,
    pub accepted_count: u32,
    pub rejected_count: u32,
    pub accepted_ts_ns: u64,
    pub updated_ts_ns: u64,
}

impl BatchReplaceStatusResult {
    /// Returns true once every item has left admission processing.
    ///
    /// `working` means the replacement is live, not that it has reached an
    /// execution terminal state. Continue polling/reconciling order state when
    /// execution finality is required.
    pub fn is_settled(&self) -> bool {
        is_batch_replace_settled(self)
    }
}

/// Returns true once every batch-replace item is `working`, `rejected`, or
/// `terminal`. An empty status is not considered settled.
pub fn is_batch_replace_settled(status: &BatchReplaceStatusResult) -> bool {
    !status.items.is_empty()
        && status
            .items
            .iter()
            .all(|item| matches!(item.phase.as_str(), "working" | "rejected" | "terminal"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllAfterResult {
    pub status: String,
    pub effective_timeout_sec: u32,
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
    /// When set, only child orders created by this trigger are returned.
    pub trigger_id: Option<String>,
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
    /// When set, only child orders created by this trigger are returned.
    pub trigger_id: Option<String>,
}

/// Options for [`crate::services::OrdersService::get_with`].
#[derive(Debug, Clone)]
pub struct GetOrderOpts {
    pub key: OrderKey,
    pub subaccount_id: Option<u64>,
    pub include_attached_risk: bool,
    pub include_attached_risk_state: bool,
}

/// Params for [`crate::services::OrdersService::cancel_with`].
#[derive(Debug, Clone)]
pub struct CancelOrderParams {
    pub key: OrderKey,
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub subaccount_id: Option<u64>,
}

/// Options for [`crate::services::OrdersService::cancel_all_with`].
#[derive(Debug, Clone, Default)]
pub struct CancelAllOpts {
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub dry_run: bool,
    pub subaccount_id: Option<u64>,
    pub side: Option<String>,
    /// Optional mutation request id (API-required on the wire).
    ///
    /// When omitted or blank, the SDK generates a unique id (TypeScript/Go/Python parity).
    /// Set a stable non-empty value when you may retry the same logical cancel-all after an
    /// ambiguous failure, and reuse that same value on retry. A blind retry that omits
    /// `request_id` mints a *new* id and is not an idempotent replay.
    pub request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateOrderParams {
    pub symbol: String,
    pub side: CreateSide,
    pub order_type: CreateOrderType,
    /// Base quantity. Set exactly one of this and `max_quote_debit_scaled`.
    pub quantity: Option<Quantity>,
    /// Hard all-in quote debit limit. The [`Quantity`] must use
    /// [`crate::types::QuantityDomain::OrderQuote`] and carry the pair's
    /// catalog quote scale. Set exactly one of this and `quantity`.
    pub max_quote_debit_scaled: Option<Quantity>,
    pub price: Option<Price>,
    pub time_in_force: Option<CreateTimeInForce>,
    /// Optional client order id (API-optional).
    ///
    /// Set a stable non-empty value when you may retry after an ambiguous failure
    /// (`Error::mutation_outcome_unknown`), and reuse that same value on retry.
    /// Omit (`None`) for one-shot creates where you will not reconcile by client id.
    pub client_order_id: Option<String>,
    pub subaccount_id: Option<u64>,
    pub post_only: Option<bool>,
    /// Client reference price for MARKET order reservation (price ticks domain).
    pub market_client_ref_price: Option<Price>,
    /// Fee asset. `Base` is valid only for BUY orders; SELL orders use `Quote`.
    pub fee_asset: Option<FeeAsset>,
    /// Self-trade prevention policy for this order.
    pub self_trade_prevention: Option<OrderSelfTradePrevention>,
    /// Optional market-order slippage guard.
    pub market_max_slippage: Option<MaxSlippage>,
    /// Optional TP/SL/trailing controls that arm after the parent fills.
    pub attached_risk: Option<AttachedRisk>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeAsset {
    Quote,
    Base,
}

/// Legacy name for [`FeeAsset`].
///
/// `Received` was removed by the API contract; use `FeeAsset::Base` for a
/// BUY fee deducted from received base quantity.
#[deprecated(note = "renamed to FeeAsset; use FeeAsset::Base instead of the removed Received")]
pub type OrderFeeSource = FeeAsset;

/// Order inputs accepted by [`crate::services::OrdersService::preview`].
///
/// Preview uses the same [`OrderIntent`](crate::proto::orders::v1::OrderIntent)
/// contract as create. The host performs an admissibility check only: no hold is
/// placed, and `client_order_id` is accepted but not claimed.
#[derive(Debug, Clone)]
pub struct PreviewOrderParams {
    pub symbol: String,
    pub side: CreateSide,
    pub order_type: CreateOrderType,
    /// Base quantity. Set exactly one of this and `max_quote_debit_scaled`.
    pub quantity: Option<Quantity>,
    /// Hard all-in quote debit limit. The [`Quantity`] must use
    /// [`crate::types::QuantityDomain::OrderQuote`] and carry the pair's
    /// catalog quote scale. Set exactly one of this and `quantity`.
    pub max_quote_debit_scaled: Option<Quantity>,
    pub price: Option<Price>,
    pub time_in_force: Option<CreateTimeInForce>,
    /// Optional client order id. Accepted for shape parity with create; preview
    /// does not claim it.
    pub client_order_id: Option<String>,
    pub subaccount_id: Option<u64>,
    pub post_only: Option<bool>,
    pub market_client_ref_price: Option<Price>,
    pub fee_asset: Option<FeeAsset>,
    pub self_trade_prevention: Option<OrderSelfTradePrevention>,
    pub market_max_slippage: Option<MaxSlippage>,
    /// Optional TP/SL/trailing controls. Preview validates the full intent;
    /// nothing is armed until a subsequent create.
    pub attached_risk: Option<AttachedRisk>,
}

/// One actionable field-level validation failure from preview/create rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFieldViolation {
    pub field_path: String,
    pub rule_id: String,
    pub message: String,
}

/// Typed rejection detail when a preview (or related) admission check fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderErrorDetail {
    /// Public error code label (for example `BAD_QTY`), or
    /// `UNKNOWN_ERROR_CODE(<n>)` for open-enum forward compatibility.
    pub code: String,
    pub violations: Vec<OrderFieldViolation>,
    /// Structured quota rejection when `ErrorDetail.rate_limit` is present.
    pub rate_limit: Option<super::RateLimitDetail>,
}

/// Advisory admission result for [`crate::services::OrdersService::preview`].
///
/// Preview no longer returns fee/quote estimates. It reports whether the intent
/// is currently admissible, any typed rejection, and any sizing / price-
/// protection values resolved during evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewOrderResult {
    pub admissible: Option<bool>,
    pub rejection: Option<OrderErrorDetail>,
    pub resolved_base_qty: Option<Quantity>,
    /// Protective execution boundary (renamed from `price_bound`).
    pub protected_price_bound: Option<Price>,
    /// Evaluation completion time as epoch milliseconds.
    pub evaluated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSelfTradePrevention {
    ExpireTaker,
    ExpireMaker,
    ExpireBoth,
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

/// User-safe outcome of [`crate::services::WithdrawService::validate_destination`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawDestinationValidation {
    pub valid: bool,
    pub code: String,
    pub message: String,
    pub canonical_destination_address: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiKeySummary {
    pub key_id: String,
    pub label: String,
    pub status: String,
    pub public_key_ed25519: String,
    pub created_at: Option<Timestamp>,
    pub last_used_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
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
