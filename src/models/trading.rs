//! Trading read/write models (Go `models/trading.go` parity).

use crate::types::{AssetAmount, Price, Quantity};
use buffa_types::google::protobuf::Timestamp;
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub order_id: String,
    pub symbol_id: u32,
    pub client_order_id: String,
    pub side: String,
    pub status: String,
    pub order_type: String,
    pub tif: String,
    /// Current accepted total quantity. A successful modify updates this to
    /// the amended total; retain the first submitted quantity separately if
    /// needed. `cum_qty` and `avg_px` are cumulative execution values across
    /// the lineage through the returned generation, not values limited to one
    /// physical order. `leaves_qty` is remaining working quantity.
    pub orig_qty: Option<Quantity>,
    pub cum_qty: Option<Quantity>,
    pub leaves_qty: Option<Quantity>,
    /// Filled base quantity this replacement inherited from its predecessors
    /// when it was accepted; zero for an original order. `cum_qty` minus
    /// this is what this generation executed itself.
    pub inherited_cum_qty: Option<Quantity>,
    pub price: Option<Price>,
    /// Average execution price in 1e9 price ticks, rounded down. Prefer
    /// [`Order::avg_price`], which is exact.
    pub avg_px: Option<Price>,
    /// Exact average execution price across the lineage through this
    /// generation, in quote units scaled by 1e18 (integer string): the exact
    /// executed quote amount divided by `cum_qty`, rounded half to even at
    /// the 18th decimal. `"0"` while the order has no fills. Read it as a
    /// decimal with [`Order::avg_price`].
    pub avg_price_e18: String,
    /// Exact quote amount of the fills across the lineage through this
    /// generation, before fees (integer string), in quote units scaled by
    /// 10^(9 + the pair's `base_quantity_scale`). `"0"` while the order has
    /// no fills. Read it as a decimal with [`Order::cum_quote`].
    pub cum_notional: String,
    /// Total fee of the fills, net of rebates, in `fee_asset`. Order history
    /// reads and single-order reads with execution history (the default)
    /// carry it; open-order reads, single-order reads without execution
    /// history and private stream updates leave it `None`.
    pub fee_total: Option<OrderFeeTotal>,
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
    pub lineage: Option<OrderLineage>,
    /// RFC3339 UTC expiry for GTD orders. `None` for every other TIF.
    pub expire_at: Option<String>,
    /// Why a `rejected` order was refused: the order error code the Orders
    /// API returns for the same refusal, formatted like
    /// [`crate::Error::Api`]'s `code` (for example
    /// `ERROR_CODE_POST_ONLY_CROSS`, or `UNKNOWN_ERROR_CODE(<number>)` for a
    /// code this SDK does not know). `None` for every other status.
    pub reject_reason: Option<String>,
    /// Why a `canceled` order ended: an `orders.v1.CancelReason` proto name
    /// (for example `CANCEL_REASON_USER_REQUEST`, `CANCEL_REASON_REPLACED` or
    /// `CANCEL_REASON_GTD_EXPIRED`), or `UNKNOWN(<number>)` for a reason this
    /// SDK does not know. `None` for every other status.
    pub cancel_reason: Option<String>,
}

/// Total fee of an order's fills, net of rebates, in the order's fee asset.
///
/// A fill's fee is recorded shortly after the fill executes, so the total
/// can trail the order's fills: it covers them all once `qty` equals the
/// order's `cum_qty`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFeeTotal {
    /// Net fee magnitude in fixed 18-decimal units of the order's fee asset
    /// (integer string): the fees of the covered fills less their rebates.
    pub amount_e18: String,
    /// True when rebates exceed fees, so the amount was credited.
    pub is_rebate: bool,
    /// Executed base quantity of the fills the total covers, scaled as the
    /// order's `cum_qty`.
    pub qty: Option<Quantity>,
}

impl OrderFeeTotal {
    /// The net fee as an exact decimal in units of the order's fee asset.
    /// `None` only when the amount does not fit a [`Decimal`].
    pub fn amount(&self) -> Option<Decimal> {
        scaled_decimal(&self.amount_e18, 18)
    }
}

/// `value / 10^scale` for an unsigned integer string, exactly; `None` when it
/// does not fit a [`Decimal`].
fn scaled_decimal(value: &str, scale: u32) -> Option<Decimal> {
    let value = value.parse::<i128>().ok()?;
    Decimal::try_from_i128_with_scale(value, scale)
        .ok()
        .map(|d| d.normalize())
}

impl Order {
    /// Exact average execution price in quote units per base unit, from
    /// `avg_price_e18`; zero while the order has no fills. `None` only when
    /// the value does not fit a [`Decimal`].
    pub fn avg_price(&self) -> Option<Decimal> {
        scaled_decimal(&self.avg_price_e18, 18)
    }

    /// Exact quote amount of the fills, before fees, in quote units: the
    /// REST `cumQuote`. `base_quantity_scale` is the pair's (from
    /// `GetSpotConfig`); zero while the order has no fills. `None` only when
    /// the value does not fit a [`Decimal`].
    pub fn cum_quote(&self, base_quantity_scale: u32) -> Option<Decimal> {
        scaled_decimal(&self.cum_notional, 9 + base_quantity_scale)
    }

    /// Quote the fills used, in quote units: [`Self::cum_quote`] plus the
    /// fee total when the fee is paid in quote (less it when `is_rebate`), or
    /// `cum_quote` alone when the fee is taken from the base received.
    ///
    /// `None` until the fee total covers every fill (`fee_total.qty` equals
    /// `cum_qty`), when the read carries no fee total, and for a fee asset
    /// this SDK does not know.
    pub fn quote_used(&self, base_quantity_scale: u32) -> Option<Decimal> {
        let fee = self.fee_total.as_ref()?;
        let covered = fee.qty.as_ref().map(Quantity::as_scaled);
        if covered != Some(self.cum_qty.as_ref().map_or(0, Quantity::as_scaled)) {
            return None;
        }
        let quote = self.cum_quote(base_quantity_scale)?;
        match self.fee_asset.as_str() {
            "base" => Some(quote),
            "quote" if fee.is_rebate => quote.checked_sub(fee.amount()?),
            "quote" => quote.checked_add(fee.amount()?),
            _ => None,
        }
    }

    /// What a buy sized by a quote amount did not use, in quote units:
    /// `submitted_max_quote_debit_scaled` (at the pair's
    /// `quote_quantity_scale`) less [`Self::quote_used`]. `None` for an order
    /// not sized by a quote amount and whenever `quote_used` is `None`.
    pub fn quote_unspent(
        &self,
        base_quantity_scale: u32,
        quote_quantity_scale: u32,
    ) -> Option<Decimal> {
        let submitted = self.submitted_max_quote_debit_scaled?;
        let submitted =
            Decimal::try_from_i128_with_scale(i128::from(submitted), quote_quantity_scale).ok()?;
        submitted
            .checked_sub(self.quote_used(base_quantity_scale)?)
            .map(|d| d.normalize())
    }
}

/// Stable replacement-chain identity. `id` is the first generation's public
/// order ID. `generation` is one-based.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderLineage {
    pub id: String,
    pub generation: u32,
}

/// Settlement leg linked to a match. Identify a match by `(symbol_id, match_id)`
/// and deduplicate repeated legs across pages by `tx_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTransfer {
    pub match_id: String,
    pub symbol_id: u32,
    pub asset_id: u32,
    pub amount_e18: String,
    pub is_debit: bool,
    pub transfer_code: i32,
    pub account_code: i32,
    pub ts_ns: String,
    pub tx_id: String,
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
    /// Public ID of the attached take-profit trigger, when the order has one.
    pub take_profit_trigger_id: Option<String>,
    /// Public ID of the attached stop-loss trigger, when the order has one.
    pub stop_loss_trigger_id: Option<String>,
    /// Public ID of the attached trailing-stop trigger, when the order has one.
    pub trailing_stop_trigger_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOrderResult {
    pub order: Option<Order>,
    pub trades: Vec<UserTrade>,
    pub transfers: Vec<OrderTransfer>,
    pub next_page_token: String,
    /// True when the order's trades across every page add up to its filled
    /// quantity. False while a recent execution is still being recorded:
    /// `trades` then lists the executions recorded so far, and a later read
    /// returns the rest. Always false when execution history is not included.
    pub executions_complete: bool,
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
    pub lineage: Option<OrderLineage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserTradesList {
    pub trades: Vec<UserTrade>,
    pub transfers: Vec<OrderTransfer>,
    pub next_page_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrderResult {
    pub action_taken: String,
    pub old_order_id: String,
    pub final_order_id: String,
    pub code: String,
    /// Public ID of the attached take-profit trigger, when the final order has one.
    pub take_profit_trigger_id: Option<String>,
    /// Public ID of the attached stop-loss trigger, when the final order has one.
    pub stop_loss_trigger_id: Option<String>,
    /// Public ID of the attached trailing-stop trigger, when the final order has one.
    pub trailing_stop_trigger_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllOrdersResult {
    pub status: String,
    pub matched_orders: u32,
    pub submitted_cancels: u32,
    pub failed_cancels: u32,
    /// Orders whose cancellation was requested, oldest first, at most 200.
    /// Empty for a dry run. Each order's final state arrives on the order
    /// stream and the order reads.
    pub cancelled_orders: Vec<CancelledOrder>,
    /// Number of orders whose cancellation was requested, including those not
    /// listed in `cancelled_orders`. Zero for a dry run.
    pub total_cancelled: u32,
    /// True when more orders were canceled than `cancelled_orders` lists.
    pub truncated: bool,
    /// Whether `cancelled_orders` is available: `included` on the first
    /// response, `not_retained_read_open_orders` when a retry with the same
    /// `request_id` returns the original counts without the listing, empty
    /// when not provided, or `UNKNOWN(<number>)`. An empty listing on a retry
    /// does not mean nothing was canceled: read open orders for current state.
    pub listing_status: String,
}

/// One order whose cancellation a cancel-all requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledOrder {
    pub order_id: String,
    /// Empty when the order has no client order ID.
    pub client_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCreateResultItem {
    pub status: String,
    pub order_id: String,
    pub client_order_id: String,
    pub code: String,
    pub rate_limit: Option<super::RateLimitDetail>,
    /// Public ID of the order that already holds this client order ID, when
    /// the item is rejected with `ERROR_CODE_CONFLICT_DUPLICATE_CLIENT_ORDER_ID`
    /// and the server knows the holder. Absent for a duplicate within the
    /// same batch.
    pub existing_order_id: Option<String>,
    /// Public ID of the attached take-profit trigger, when an accepted order has one.
    pub take_profit_trigger_id: Option<String>,
    /// Public ID of the attached stop-loss trigger, when an accepted order has one.
    pub stop_loss_trigger_id: Option<String>,
    /// Public ID of the attached trailing-stop trigger, when an accepted order has one.
    pub trailing_stop_trigger_id: Option<String>,
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
    /// Replacement attached risk. A trailing stop's `limit_price` is sent as
    /// given: the request does not carry the order's side, so the server
    /// applies the side rule (a maximum only when the order sells) and
    /// answers `ERROR_CODE_BAD_PRICE` with no violation when it is broken.
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
    /// Replacement attached risk. A trailing stop's `limit_price` is sent as
    /// given: the request does not carry the order's side, so the server
    /// applies the side rule (a maximum only when the order sells) and
    /// answers `ERROR_CODE_BAD_PRICE` with no violation when it is broken.
    pub new_attached_risk: Option<AttachedRisk>,
    pub behavior: Option<String>,
    pub new_client_order_id: Option<String>,
}

/// Price an attached-risk leg watches. `None` on a leg watches the last
/// trade price. `IndexPrice` watches the market's index price; the server
/// rejects it on a market without one. `MarkPrice` is not supported on spot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerPriceSourceKind {
    LastPrice,
    IndexPrice,
    MarkPrice,
}

/// Take-profit or stop-loss leg (trigger + optional LIMIT child).
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskLeg {
    /// Trigger price when policy data was requested and supplied. State-only
    /// order reads leave this absent rather than fabricating a zero price.
    pub trigger_price: Option<Price>,
    /// Price the leg watches; `None` watches the last trade price.
    pub trigger_price_source: Option<TriggerPriceSourceKind>,
    pub order_type: Option<CreateOrderType>,
    pub limit_price: Option<Price>,
    /// Runtime linkage/state returned when attached-risk state is requested.
    /// This field is ignored when the leg is used in an order write.
    pub state: Option<AttachedRiskLegState>,
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
    /// Absolute Q9 price ticks (proto field is int64).
    Ticks(i64),
    Bps(i32),
}

/// Trailing-stop attached-risk leg.
///
/// Distance and optional max slippage must be positive. The child is always a
/// market-IOC execution; supplying [`order_type`](Self::order_type) is
/// rejected.
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingStop {
    /// Trailing distance when policy data was requested and supplied.
    /// State-only order reads leave this absent.
    pub distance: Option<TrailingDistance>,
    pub activation_price: Option<Price>,
    /// Maximum price of a trailing stop that buys back, the most its exit pays
    /// per unit. Required when the parent order sells, so the trailing stop
    /// buys, and must be at least `activation_price` when that is set; must be
    /// `None` when the parent order buys. Order reads do not return it yet.
    pub limit_price: Option<Price>,
    /// Price the trailing stop watches (activation, peak or trough, and
    /// fire); `None` watches the last trade price.
    pub trigger_price_source: Option<TriggerPriceSourceKind>,
    /// Deprecated for attached trailing: any supplied value is rejected because
    /// the child is always an implicit market execution.
    #[deprecated(
        note = "attached trailing child is always market; supplying order_type is rejected"
    )]
    pub order_type: Option<CreateOrderType>,
    pub max_slippage: Option<MaxSlippage>,
    /// Runtime linkage/state returned when attached-risk state is requested.
    /// This field is ignored when the leg is used in an order write.
    pub state: Option<AttachedRiskLegState>,
}

impl RiskLeg {
    /// A leg whose child is a market IOC order once `trigger_price` is crossed.
    pub fn market(trigger_price: Price) -> Self {
        Self {
            trigger_price: Some(trigger_price),
            trigger_price_source: None,
            order_type: Some(CreateOrderType::Market),
            limit_price: None,
            state: None,
        }
    }

    /// A leg whose child is a GTC limit order at `limit_price` once
    /// `trigger_price` is crossed.
    pub fn limit(trigger_price: Price, limit_price: Price) -> Self {
        Self {
            trigger_price: Some(trigger_price),
            trigger_price_source: None,
            order_type: Some(CreateOrderType::Limit),
            limit_price: Some(limit_price),
            state: None,
        }
    }

    /// Watch `source` instead of the last trade price.
    pub fn with_trigger_price_source(mut self, source: TriggerPriceSourceKind) -> Self {
        self.trigger_price_source = Some(source);
        self
    }
}

impl TrailingStop {
    /// A trailing stop `distance` behind the peak (or trough) price.
    #[allow(deprecated)]
    pub fn new(distance: TrailingDistance) -> Self {
        Self {
            distance: Some(distance),
            activation_price: None,
            limit_price: None,
            trigger_price_source: None,
            order_type: None,
            max_slippage: None,
            state: None,
        }
    }

    /// Start trailing only once `price` is reached.
    pub fn with_activation_price(mut self, price: Price) -> Self {
        self.activation_price = Some(price);
        self
    }

    /// The most a buy-back pays per unit; required when the parent order sells.
    pub fn with_limit_price(mut self, price: Price) -> Self {
        self.limit_price = Some(price);
        self
    }

    /// Watch and trail `source` instead of the last trade price.
    pub fn with_trigger_price_source(mut self, source: TriggerPriceSourceKind) -> Self {
        self.trigger_price_source = Some(source);
        self
    }

    /// Cap the market child's slippage.
    pub fn with_max_slippage(mut self, slippage: MaxSlippage) -> Self {
        self.max_slippage = Some(slippage);
        self
    }
}

/// Runtime lifecycle and child linkage for one attached-risk leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedRiskLegState {
    /// Lowercase lifecycle label, or `UNKNOWN(<number>)` for a future enum.
    pub status: String,
    /// Nanoseconds since epoch as a decimal string; empty when not armed.
    pub armed_ts_ns: String,
    /// Nanoseconds since epoch as a decimal string; empty when not terminal.
    pub terminal_ts_ns: String,
    /// Public base58 trigger ID when supplied by the host.
    pub trigger_id: Option<String>,
    /// Public base58 child order ID when supplied by the host.
    pub child_order_id: Option<String>,
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
    /// The successor's planned order ID. It is not proof of an order: the
    /// item can still be refused when applied, and then this ID never
    /// becomes an order. Only [`BatchReplaceStatusItem`] or order reads
    /// confirm a successor.
    pub replacement_order_id: String,
    /// `replaced` when a successor order takes over under
    /// `replacement_order_id`, with its queue priority reset. `amended` when
    /// the order keeps `old_order_id`: amended in place with its queue
    /// priority kept, or ended at its filled quantity when the new total
    /// quantity is at or below it. Keep tracking `old_order_id`. Empty for a
    /// rejected item.
    pub action_taken: String,
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
    /// `admitted`, `working`, `rejected` or `terminal`. An item admitted and
    /// then refused when applied, such as a post-only price that would
    /// cross, reads `rejected` with its `code`: the order keeps its previous
    /// price and quantity under `old_order_id`.
    pub phase: String,
    pub old_order_id: String,
    /// The successor order, or empty when there is none: always empty for a
    /// rejected item.
    pub replacement_order_id: String,
    /// `replaced` or `amended`, as on [`BatchReplaceAdmissionItem`]; empty
    /// until known and for a rejected item.
    pub action_taken: String,
    pub order_status: String,
    /// The refusal's error code without its `ERROR_CODE_` prefix, such as
    /// `POST_ONLY_CROSS`; see [`Self::error_code`].
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

impl BatchReplaceStatusItem {
    /// The refusal's code as an orders error code, the same string an
    /// [`crate::Error::Api`] from a single modify carries: `code` with the
    /// `ERROR_CODE_` prefix, such as `ERROR_CODE_POST_ONLY_CROSS`. A code this
    /// SDK version does not know is returned unchanged. `None` when the item
    /// has no code.
    pub fn error_code(&self) -> Option<String> {
        crate::errors::order_codes::from_status_code(&self.code)
    }
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
    /// The orders the armed switch cancels when it expires. `None` when the
    /// switch is disabled.
    pub effective_scope: Option<DeadManSwitchScope>,
}

/// The recorded scope of an armed dead-man switch. Expiry cancels working
/// orders in this scope, including working child orders; waiting triggers
/// stay active and may create further orders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadManSwitchScope {
    /// Public ID of the account or sub-account whose orders are canceled.
    pub account_id: String,
    /// Zero means all symbols.
    pub symbol_id: u32,
    /// `buy`, `sell`, empty for both sides, or `UNKNOWN(<number>)`.
    pub side: String,
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

/// Options for [`crate::services::TradesService::list_with`].
///
/// When [`Self::after_match_id`] is set, a resolved non-zero `symbol_id` is
/// required (display `symbol` or numeric id). Without `after_match_id`, the
/// symbol filter stays optional.
#[derive(Debug, Clone, Default)]
pub struct ListUserTradesOpts {
    pub subaccount_id: Option<u64>,
    pub symbol: Option<String>,
    pub symbol_id: Option<u32>,
    pub after_match_id: Option<u64>,
    pub order_id: Option<String>,
    pub lineage_id: Option<String>,
    pub through_generation: Option<u32>,
    pub include_transfers: bool,
    pub limit: Option<u32>,
    pub page_token: Option<String>,
}

/// Options for [`crate::services::OrdersService::get_with`].
#[derive(Debug, Clone)]
pub struct GetOrderOpts {
    pub key: OrderKey,
    pub subaccount_id: Option<u64>,
    pub include_attached_risk: bool,
    pub include_attached_risk_state: bool,
    /// `None` omits the field so the server default (enabled) applies.
    pub include_execution_history: Option<bool>,
    pub limit: Option<u32>,
    pub page_token: Option<String>,
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
    pub symbols: Vec<String>,
    pub symbol_id: Option<u32>,
    pub symbol_ids: Vec<u32>,
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

/// Scope of [`crate::Client::stop_all_trading`]: one account (or sub-account)
/// and optionally one market.
#[derive(Debug, Clone, Default)]
pub struct StopAllTradingScope {
    /// Sub-account to stop; `None` uses the client's default scope.
    pub subaccount_id: Option<u64>,
    /// Market to stop, such as `BTC-USDT`; `None` stops every market.
    pub symbol: Option<String>,
    /// Request id of the final cancel-all; generated when omitted. Reuse it
    /// when retrying the same stop after an ambiguous failure.
    pub request_id: Option<String>,
}

/// One live trigger [`crate::Client::stop_all_trading`] could not cancel.
#[derive(Debug, Clone)]
pub struct StopAllTradingFailure {
    pub trigger_id: String,
    pub error: crate::Error,
}

/// What [`crate::Client::stop_all_trading`] canceled.
#[derive(Debug, Clone)]
pub struct StopAllTradingResult {
    /// Live triggers canceled, in the order they were canceled.
    pub canceled_trigger_ids: Vec<String>,
    /// Live triggers whose cancel failed. The open orders were still canceled
    /// after them; cancel these triggers again, then the orders.
    pub trigger_failures: Vec<StopAllTradingFailure>,
    /// The final cancel-all of the open orders, those triggers placed included.
    pub orders: CancelAllOrdersResult,
}

/// Order inputs accepted by [`crate::services::OrdersService::create`].
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
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
    /// Set a stable non-empty value so you can reconcile after an ambiguous
    /// failure (`Error::mutation_outcome_unknown`). Look up that id before
    /// creating again; a second create with the same id is a conflict if the
    /// first attempt admitted. Omit (`None`) for one-shot creates.
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
    /// Required when `time_in_force` is [`CreateTimeInForce::Gtd`].
    pub expire_at: Option<std::time::SystemTime>,
}

impl CreateOrderParams {
    /// Params with only the required fields set; assign the optional fields
    /// (`quantity` or `max_quote_debit_scaled`, `price`, `time_in_force`, …)
    /// afterwards.
    pub fn new(symbol: impl Into<String>, side: CreateSide, order_type: CreateOrderType) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            order_type,
            quantity: None,
            max_quote_debit_scaled: None,
            price: None,
            time_in_force: None,
            client_order_id: None,
            subaccount_id: None,
            post_only: None,
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
            expire_at: None,
        }
    }
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
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
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
    /// Required when `time_in_force` is [`CreateTimeInForce::Gtd`].
    pub expire_at: Option<std::time::SystemTime>,
}

/// One actionable field-level validation failure from preview/create rejection.
impl PreviewOrderParams {
    /// Params with only the required fields set; assign the optional fields
    /// (`quantity` or `max_quote_debit_scaled`, `price`, `time_in_force`, …)
    /// afterwards.
    pub fn new(symbol: impl Into<String>, side: CreateSide, order_type: CreateOrderType) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            order_type,
            quantity: None,
            max_quote_debit_scaled: None,
            price: None,
            time_in_force: None,
            client_order_id: None,
            subaccount_id: None,
            post_only: None,
            market_client_ref_price: None,
            fee_asset: None,
            self_trade_prevention: None,
            market_max_slippage: None,
            attached_risk: None,
            expire_at: None,
        }
    }
}

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
    Gtd,
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
