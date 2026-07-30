//! Orders read/mutation decoders.

use super::enums::{
    enum_value_fee_asset, enum_value_order_status, enum_value_order_type, enum_value_side,
    enum_value_time_in_force,
};
use super::money::{decode_price_ticks, decode_qty_scaled, decode_qty_scaled_allow_zero};
use crate::codecs::scalars::format_uint64_id;
use crate::errors::{Error, Result};
use crate::models::{
    AttachedRisk, BatchCancelOrdersResult, BatchCancelResultItem, BatchCreateOrdersResult,
    BatchCreateResultItem, BatchReplaceAdmissionItem, BatchReplaceOrdersResult,
    BatchReplaceStatusItem, BatchReplaceStatusResult, CancelAllAfterResult, CancelAllOrdersResult,
    CreateOrderType, GetOrderResult, MaxSlippage, ModifyOrderResult, Order, OrderMutationResult,
    OrdersList, PreviewOrderResult, RiskLeg, TrailingDistance, TrailingStop, UserTrade,
    UserTradesList,
};
use crate::proto::orders::v1::{
    AttachedRisk as ProtoAttachedRisk, BatchCancelOrdersResponse, BatchCreateOrdersResponse,
    BatchReplaceAdmissionStatus, BatchReplaceItemAdmissionStatus, BatchReplaceOrdersResponse,
    BatchReplacePhase, CancelAllAfterResponse, CancelAllOrdersResponse, CancelOrderResponse,
    CreateOrderResponse, GetBatchReplaceStatusResponse, GetOpenOrdersResponse,
    GetOrderHistoryResponse, GetOrderResponse, GetUserTradesResponse, ModifyOrderResponse,
    Order as ProtoOrder, PreviewOrderResponse, RiskExecution, StopLossPolicy, TakeProfitPolicy,
    TrailingStopPolicy, UserTrade as ProtoUserTrade, batch_create_result_item, risk_execution,
    trailing_stop_policy,
};
use buffa::Enumeration;

pub fn order_from_proto(msg: &ProtoOrder) -> Order {
    let symbol_id = msg.symbol_id;
    let symbol_id_opt = if symbol_id == 0 {
        None
    } else {
        Some(symbol_id)
    };
    Order {
        order_id: format_uint64_id(msg.order_id),
        symbol_id,
        client_order_id: msg.client_order_id.clone(),
        side: enum_value_side(msg.side).to_owned(),
        status: enum_value_order_status(msg.status).to_owned(),
        order_type: enum_value_order_type(msg.order_type).to_owned(),
        tif: enum_value_time_in_force(msg.time_in_force).to_owned(),
        orig_qty: decode_qty_scaled(msg.orig_qty_scaled, None, None, symbol_id_opt),
        cum_qty: decode_qty_scaled_allow_zero(msg.cum_qty_scaled, None, None, symbol_id_opt),
        leaves_qty: decode_qty_scaled_allow_zero(msg.leaves_qty_scaled, None, None, symbol_id_opt),
        price: decode_price_ticks(msg.price_ticks, None),
        avg_px: decode_price_ticks(msg.avg_price_ticks, None),
        created_ts_ns: if msg.created_ts_ns == 0 {
            String::new()
        } else {
            msg.created_ts_ns.to_string()
        },
        version: msg.version,
        post_only: msg.post_only,
        fee_asset: enum_value_fee_asset(msg.fee_asset),
        submitted_max_quote_debit_scaled: msg.submitted_max_quote_debit_scaled,
        attached_risk: msg
            .attached_risk
            .as_option()
            .and_then(attached_risk_from_proto),
    }
}

/// Project an attached take-profit/stop-loss policy onto the flat public
/// [`RiskLeg`]. The child execution determines `order_type`/`limit_price`.
/// `trigger_price_source` is no longer part of the policy wire and is left empty.
#[allow(deprecated)]
fn decode_risk_leg(trigger_price_ticks: i64, child: Option<&RiskExecution>) -> Option<RiskLeg> {
    if trigger_price_ticks == 0 {
        return None;
    }
    let mut order_type = None;
    let mut limit_price = None;
    if let Some(child) = child {
        match child.execution.as_ref() {
            Some(risk_execution::Execution::MarketIoc(_)) => {
                order_type = Some(CreateOrderType::Market);
            }
            Some(risk_execution::Execution::LimitGtc(limit)) => {
                order_type = Some(CreateOrderType::Limit);
                limit_price = decode_price_ticks(limit.price_ticks, None);
            }
            None => {}
        }
    }
    Some(RiskLeg {
        trigger_price: decode_price_ticks(trigger_price_ticks, None)?,
        trigger_price_source: None,
        order_type,
        limit_price,
    })
}

fn risk_leg_from_take_profit(policy: &TakeProfitPolicy) -> Option<RiskLeg> {
    decode_risk_leg(policy.trigger_price_ticks, policy.child.as_option())
}

fn risk_leg_from_stop_loss(policy: &StopLossPolicy) -> Option<RiskLeg> {
    decode_risk_leg(policy.trigger_price_ticks, policy.child.as_option())
}

fn trailing_stop_from_policy(policy: &TrailingStopPolicy) -> TrailingStop {
    let distance = match policy.trailing_distance.as_ref() {
        Some(trailing_stop_policy::TrailingDistance::TrailingDistanceTicks(v)) => {
            TrailingDistance::Ticks(*v)
        }
        Some(trailing_stop_policy::TrailingDistance::TrailingDistanceBps(v)) => {
            TrailingDistance::Bps(*v)
        }
        None => TrailingDistance::Ticks(0),
    };
    let max_slippage = match policy.max_slippage.as_ref() {
        Some(trailing_stop_policy::MaxSlippage::MaxSlippageTicks(v)) => {
            Some(MaxSlippage::Ticks(*v))
        }
        Some(trailing_stop_policy::MaxSlippage::MaxSlippageBps(v)) => Some(MaxSlippage::Bps(*v)),
        None => None,
    };
    TrailingStop {
        distance,
        activation_price: if policy.activation_price_ticks > 0 {
            decode_price_ticks(policy.activation_price_ticks, None)
        } else {
            None
        },
        // `trigger_price_source`/`order_type` were dropped from the trailing-stop
        // policy wire; the child is an implicit market execution.
        trigger_price_source: None,
        order_type: None,
        max_slippage,
    }
}

fn attached_risk_from_proto(msg: &ProtoAttachedRisk) -> Option<AttachedRisk> {
    let take_profit = msg
        .take_profit
        .as_option()
        .and_then(|leg| leg.policy.as_option().and_then(risk_leg_from_take_profit));
    let trailing_stop = msg
        .trailing_stop
        .as_option()
        .and_then(|leg| leg.policy.as_option().map(trailing_stop_from_policy));
    // Match TS: when trailing is present, stop-loss is suppressed.
    let stop_loss = if trailing_stop.is_some() {
        None
    } else {
        msg.stop_loss
            .as_option()
            .and_then(|leg| leg.policy.as_option().and_then(risk_leg_from_stop_loss))
    };
    if take_profit.is_none() && stop_loss.is_none() && trailing_stop.is_none() {
        return None;
    }
    Some(AttachedRisk {
        take_profit,
        stop_loss,
        trailing_stop,
        oco: msg.oco,
    })
}

pub fn orders_list_from_open(msg: &GetOpenOrdersResponse) -> OrdersList {
    orders_list(&msg.orders, &msg.next_page_token)
}

pub fn orders_list_from_history(msg: &GetOrderHistoryResponse) -> OrdersList {
    orders_list(&msg.orders, &msg.next_page_token)
}

fn orders_list(orders: &[ProtoOrder], next_page_token: &str) -> OrdersList {
    OrdersList {
        orders: orders.iter().map(order_from_proto).collect(),
        next_page_token: next_page_token.to_owned(),
    }
}

pub fn user_trade_from_proto(msg: &ProtoUserTrade) -> UserTrade {
    let symbol_id = msg.symbol_id;
    let symbol_id_opt = if symbol_id == 0 {
        None
    } else {
        Some(symbol_id)
    };
    UserTrade {
        symbol_id,
        match_id: if msg.match_id == 0 {
            String::new()
        } else {
            msg.match_id.to_string()
        },
        order_id: format_uint64_id(msg.order_id),
        side: enum_value_side(msg.side).to_owned(),
        is_maker: msg.is_maker,
        price: decode_price_ticks(msg.price_ticks, None),
        qty: decode_qty_scaled(msg.qty_scaled, None, None, symbol_id_opt),
        fee_scaled: if msg.fee_scaled == 0 {
            String::new()
        } else {
            msg.fee_scaled.to_string()
        },
        fee_asset: enum_value_fee_asset(msg.fee_asset),
        referral_share_scaled: if msg.referral_share_scaled == 0 {
            String::new()
        } else {
            msg.referral_share_scaled.to_string()
        },
        ts_ns: if msg.ts_ns == 0 {
            String::new()
        } else {
            msg.ts_ns.to_string()
        },
    }
}

pub fn user_trades_list_from_proto(msg: &GetUserTradesResponse) -> UserTradesList {
    UserTradesList {
        trades: msg.trades.iter().map(user_trade_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn get_order_from_proto(msg: &GetOrderResponse) -> GetOrderResult {
    let order = msg.order.as_option().map(order_from_proto);
    let trades = msg.trades.iter().map(user_trade_from_proto).collect();
    GetOrderResult { order, trades }
}

/// `CreateOrderResponse` acknowledges admission only and no longer carries a
/// status field; synthesize `"accepted"`.
pub fn order_mutation_from_create(msg: &CreateOrderResponse) -> Result<OrderMutationResult> {
    // client_order_id is API-optional on create; an omitted request id may echo empty.
    if msg.order_id == 0 {
        return Err(Error::response_contract("CreateOrder", "missing order_id"));
    }
    let mut result = order_mutation("accepted", msg.order_id, &msg.client_order_id);
    result.resolved_base_qty = decode_qty_scaled(msg.resolved_base_qty_scaled, None, None, None);
    result.submitted_max_quote_debit_scaled = msg.submitted_max_quote_debit_scaled;
    Ok(result)
}

pub fn order_mutation_from_cancel(msg: &CancelOrderResponse) -> Result<OrderMutationResult> {
    if msg.order_id == 0 || msg.status.trim().is_empty() {
        return Err(Error::response_contract(
            "CancelOrder",
            "missing order_id or status",
        ));
    }
    Ok(order_mutation(&msg.status, msg.order_id, ""))
}

fn order_mutation(status: &str, order_id: u64, client_order_id: &str) -> OrderMutationResult {
    OrderMutationResult {
        status: status.to_owned(),
        order_id: format_uint64_id(order_id),
        client_order_id: client_order_id.to_owned(),
        resolved_base_qty: None,
        submitted_max_quote_debit_scaled: None,
    }
}

pub fn preview_order_from_proto(msg: &PreviewOrderResponse) -> PreviewOrderResult {
    PreviewOrderResult {
        resolved_base_qty: decode_qty_scaled(msg.resolved_base_qty_scaled, None, None, None),
        price_bound: decode_price_ticks(msg.price_bound_ticks, None),
        estimated_quote_debit_scaled: msg.estimated_quote_debit_scaled,
        estimated_fee_scaled: msg.estimated_fee_scaled,
        estimated_net_base_qty: decode_qty_scaled(
            msg.estimated_net_base_qty_scaled,
            None,
            None,
            None,
        ),
        fee_asset: enum_value_fee_asset(msg.fee_asset),
        fresh_at_ts_ns: msg.fresh_at_ts_ns,
    }
}

pub fn modify_order_from_proto(msg: &ModifyOrderResponse) -> Result<ModifyOrderResult> {
    let action_taken = modify_action_name(msg.action_taken);
    if action_taken.is_empty() || msg.old_order_id == 0 || msg.final_order_id == 0 {
        return Err(Error::response_contract(
            "ModifyOrder",
            "missing action_taken, old_order_id, or final_order_id",
        ));
    }
    Ok(ModifyOrderResult {
        action_taken,
        old_order_id: format_uint64_id(msg.old_order_id),
        final_order_id: format_uint64_id(msg.final_order_id),
        code: msg.code.clone(),
    })
}

fn modify_action_name(
    action: buffa::EnumValue<crate::proto::orders::v1::ModifyActionTaken>,
) -> String {
    use crate::proto::orders::v1::ModifyActionTaken;
    match action.as_known() {
        Some(ModifyActionTaken::Amended) => "amended".to_owned(),
        Some(ModifyActionTaken::Replaced) => "replaced".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", action.to_i32()),
    }
}

pub fn cancel_all_from_proto(msg: &CancelAllOrdersResponse) -> Result<CancelAllOrdersResult> {
    let status = msg.status.trim();
    if status.is_empty()
        || !(status.eq_ignore_ascii_case("submitted") || status.eq_ignore_ascii_case("dry_run"))
    {
        return Err(Error::response_contract(
            "CancelAllOrders",
            format!("unknown status {:?}", msg.status),
        ));
    }
    if status.eq_ignore_ascii_case("submitted")
        && msg
            .submitted_cancels
            .checked_add(msg.failed_cancels)
            .filter(|total| *total == msg.matched_orders)
            .is_none()
    {
        return Err(Error::response_contract(
            "CancelAllOrders",
            format!(
                "response counts mismatch: matched {}, submitted {}, failed {}",
                msg.matched_orders, msg.submitted_cancels, msg.failed_cancels
            ),
        ));
    }
    if status.eq_ignore_ascii_case("dry_run")
        && (msg.submitted_cancels != 0 || msg.failed_cancels != 0)
    {
        return Err(Error::response_contract(
            "CancelAllOrders",
            format!(
                "dry_run reported submitted or failed cancels: submitted {}, failed {}",
                msg.submitted_cancels, msg.failed_cancels
            ),
        ));
    }
    Ok(CancelAllOrdersResult {
        status: msg.status.clone(),
        matched_orders: msg.matched_orders,
        submitted_cancels: msg.submitted_cancels,
        failed_cancels: msg.failed_cancels,
    })
}

/// Per-item results now carry an `Accepted`/`Rejected` outcome oneof instead of
/// flat status/order_id/code fields.
pub fn batch_create_from_proto(msg: &BatchCreateOrdersResponse) -> Result<BatchCreateOrdersResult> {
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let results = msg
        .results
        .iter()
        .map(|item| {
            let mut out = BatchCreateResultItem {
                status: String::new(),
                order_id: String::new(),
                client_order_id: item.client_order_id.clone(),
                code: String::new(),
            };
            match item.outcome.as_ref() {
                Some(batch_create_result_item::Outcome::Accepted(value)) => {
                    accepted += 1;
                    out.status = "accepted".to_owned();
                    out.order_id = format_uint64_id(value.order_id);
                }
                Some(batch_create_result_item::Outcome::Rejected(value)) => {
                    rejected += 1;
                    out.status = "rejected".to_owned();
                    out.code = value
                        .error
                        .as_option()
                        .map(|err| match err.code.as_known() {
                            Some(code) => code.proto_name().to_owned(),
                            None => format!("UNKNOWN_ERROR_CODE({})", err.code.to_i32()),
                        })
                        .unwrap_or_else(|| "ERROR_CODE_UNSPECIFIED".to_owned());
                }
                None => {
                    return Err(Error::response_contract(
                        "BatchCreateOrders",
                        format!(
                            "item {:?} has neither accepted nor rejected outcome",
                            item.client_order_id
                        ),
                    ));
                }
            }
            Ok(out)
        })
        .collect::<Result<Vec<_>>>()?;

    if accepted != msg.accepted_count as usize
        || rejected != msg.rejected_count as usize
        || accepted + rejected != results.len()
    {
        return Err(Error::response_contract(
            "BatchCreateOrders",
            format!(
                "response counts mismatch: decoded {accepted} accepted/{rejected} rejected for {} results, server reported {}/{}",
                results.len(),
                msg.accepted_count,
                msg.rejected_count
            ),
        ));
    }

    Ok(BatchCreateOrdersResult {
        results,
        accepted_count: msg.accepted_count,
        rejected_count: msg.rejected_count,
    })
}

pub fn batch_cancel_from_proto(msg: &BatchCancelOrdersResponse) -> Result<BatchCancelOrdersResult> {
    let mut accepted = 0u32;
    let mut rejected = 0u32;
    let mut results = Vec::with_capacity(msg.results.len());
    for item in &msg.results {
        if item.status.eq_ignore_ascii_case("accepted") {
            accepted += 1;
        } else if item.status.eq_ignore_ascii_case("rejected") {
            rejected += 1;
        } else {
            return Err(Error::response_contract(
                "BatchCancelOrders",
                format!("unknown item status {:?}", item.status),
            ));
        }
        results.push(BatchCancelResultItem {
            status: item.status.clone(),
            order_id: format_uint64_id(item.order_id),
            client_order_id: item.client_order_id.clone(),
            code: item.code.clone(),
        });
    }
    if accepted != msg.accepted_count
        || rejected != msg.rejected_count
        || usize::try_from(accepted + rejected).ok() != Some(results.len())
    {
        return Err(Error::response_contract(
            "BatchCancelOrders",
            format!(
                "response counts mismatch: decoded {accepted} accepted/{rejected} rejected for {} results, server reported {}/{}",
                results.len(),
                msg.accepted_count,
                msg.rejected_count
            ),
        ));
    }
    Ok(BatchCancelOrdersResult {
        results,
        accepted_count: msg.accepted_count,
        rejected_count: msg.rejected_count,
    })
}

pub fn batch_replace_from_proto(
    msg: &BatchReplaceOrdersResponse,
) -> Result<BatchReplaceOrdersResult> {
    if msg.batch_request_id == 0 {
        return Err(Error::response_contract(
            "BatchReplaceOrders",
            "missing batch_request_id",
        ));
    }
    let status = batch_replace_admission_status_name(msg.status).ok_or_else(|| {
        Error::response_contract(
            "BatchReplaceOrders",
            format!("unknown admission status {}", msg.status.to_i32()),
        )
    })?;
    let mut accepted = 0u32;
    let mut rejected = 0u32;
    let mut results = Vec::with_capacity(msg.results.len());
    for item in &msg.results {
        let item_status =
            batch_replace_item_admission_status_name(item.status).ok_or_else(|| {
                Error::response_contract(
                    "BatchReplaceOrders",
                    format!("unknown item status {}", item.status.to_i32()),
                )
            })?;
        match item_status {
            "admitted" => accepted += 1,
            "rejected" => rejected += 1,
            _ => unreachable!("known item admission status"),
        }
        results.push(BatchReplaceAdmissionItem {
            item_index: item.item_index,
            status: item_status.to_owned(),
            old_order_id: format_uint64_id(item.old_order_id),
            replacement_order_id: format_uint64_id(item.replacement_order_id),
            client_order_id: item.client_order_id.clone(),
            code: item.code.clone(),
        });
    }
    if accepted != msg.accepted_count
        || rejected != msg.rejected_count
        || accepted
            .checked_add(rejected)
            .and_then(|count| usize::try_from(count).ok())
            != Some(results.len())
    {
        return Err(Error::response_contract(
            "BatchReplaceOrders",
            format!(
                "response counts mismatch: decoded {accepted} accepted/{rejected} rejected for {} results, server reported {}/{}",
                results.len(),
                msg.accepted_count,
                msg.rejected_count
            ),
        ));
    }
    Ok(BatchReplaceOrdersResult {
        batch_request_id: format_uint64_id(msg.batch_request_id),
        status: status.to_owned(),
        results,
        accepted_count: msg.accepted_count,
        rejected_count: msg.rejected_count,
        accepted_ts_ns: msg.accepted_ts_ns,
    })
}

pub fn batch_replace_status_from_proto(
    msg: &GetBatchReplaceStatusResponse,
) -> Result<BatchReplaceStatusResult> {
    if msg.batch_request_id == 0 {
        return Err(Error::response_contract(
            "GetBatchReplaceStatus",
            "missing batch_request_id",
        ));
    }
    let admission_status =
        batch_replace_admission_status_name(msg.admission_status).ok_or_else(|| {
            Error::response_contract(
                "GetBatchReplaceStatus",
                format!("unknown admission status {}", msg.admission_status.to_i32()),
            )
        })?;
    let mut admitted = 0u32;
    let mut rejected = 0u32;
    let mut items = Vec::with_capacity(msg.items.len());
    for item in &msg.items {
        let phase = batch_replace_phase_name(item.phase).ok_or_else(|| {
            Error::response_contract(
                "GetBatchReplaceStatus",
                format!("unknown batch replace phase {}", item.phase.to_i32()),
            )
        })?;
        match phase {
            "admitted" => admitted += 1,
            "rejected" => rejected += 1,
            // Working and terminal entries were admitted successfully before
            // their post-admission state transition.
            "working" | "terminal" => admitted += 1,
            _ => unreachable!("known batch replace phase"),
        }
        items.push(BatchReplaceStatusItem {
            item_index: item.item_index,
            phase: phase.to_owned(),
            old_order_id: format_uint64_id(item.old_order_id),
            replacement_order_id: format_uint64_id(item.replacement_order_id),
            order_status: enum_value_order_status(item.order_status).to_owned(),
            code: item.code.clone(),
            updated_ts_ns: item.updated_ts_ns,
        });
    }
    if admitted != msg.accepted_count
        || rejected != msg.rejected_count
        || admitted
            .checked_add(rejected)
            .and_then(|count| usize::try_from(count).ok())
            != Some(items.len())
    {
        return Err(Error::response_contract(
            "GetBatchReplaceStatus",
            format!(
                "response counts mismatch: decoded {admitted} admitted/{rejected} rejected for {} items, server reported {}/{}",
                items.len(),
                msg.accepted_count,
                msg.rejected_count
            ),
        ));
    }
    Ok(BatchReplaceStatusResult {
        batch_request_id: format_uint64_id(msg.batch_request_id),
        admission_status: admission_status.to_owned(),
        items,
        accepted_count: msg.accepted_count,
        rejected_count: msg.rejected_count,
        accepted_ts_ns: msg.accepted_ts_ns,
        updated_ts_ns: msg.updated_ts_ns,
    })
}

fn batch_replace_admission_status_name(
    status: buffa::EnumValue<BatchReplaceAdmissionStatus>,
) -> Option<&'static str> {
    match status.as_known() {
        Some(BatchReplaceAdmissionStatus::Admitted) => Some("admitted"),
        Some(BatchReplaceAdmissionStatus::PartiallyAdmitted) => Some("partially_admitted"),
        Some(BatchReplaceAdmissionStatus::Rejected) => Some("rejected"),
        _ => None,
    }
}

fn batch_replace_item_admission_status_name(
    status: buffa::EnumValue<BatchReplaceItemAdmissionStatus>,
) -> Option<&'static str> {
    match status.as_known() {
        Some(BatchReplaceItemAdmissionStatus::Admitted) => Some("admitted"),
        Some(BatchReplaceItemAdmissionStatus::Rejected) => Some("rejected"),
        _ => None,
    }
}

fn batch_replace_phase_name(phase: buffa::EnumValue<BatchReplacePhase>) -> Option<&'static str> {
    match phase.as_known() {
        Some(BatchReplacePhase::Admitted) => Some("admitted"),
        Some(BatchReplacePhase::Working) => Some("working"),
        Some(BatchReplacePhase::Rejected) => Some("rejected"),
        Some(BatchReplacePhase::Terminal) => Some("terminal"),
        _ => None,
    }
}

pub fn cancel_all_after_from_proto(msg: &CancelAllAfterResponse) -> Result<CancelAllAfterResult> {
    let status = msg.status.trim();
    if status.is_empty()
        || !(status.eq_ignore_ascii_case("armed") || status.eq_ignore_ascii_case("disabled"))
    {
        return Err(Error::response_contract(
            "CancelAllAfter",
            format!("unknown status {:?}", msg.status),
        ));
    }
    Ok(CancelAllAfterResult {
        status: msg.status.clone(),
        effective_timeout_sec: msg.effective_timeout_sec,
        expires_at_ts_ns: if msg.expires_at_ts_ns == 0 {
            String::new()
        } else {
            msg.expires_at_ts_ns.to_string()
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::scalars::format_uint64_id;
    use crate::proto::orders::v1::{
        GetOpenOrdersResponse, GetOrderResponse, OrderStatus, OrderType, Side, TimeInForce,
    };

    #[test]
    fn order_from_proto_maps_enums_and_ids() {
        let msg = ProtoOrder {
            order_id: 42,
            symbol_id: 3,
            client_order_id: "coid-1".into(),
            side: Side::Buy.into(),
            status: OrderStatus::Working.into(),
            order_type: OrderType::Limit.into(),
            time_in_force: TimeInForce::Gtc.into(),
            orig_qty_scaled: 100,
            cum_qty_scaled: 10,
            leaves_qty_scaled: 90,
            price_ticks: 5000,
            avg_price_ticks: 4990,
            created_ts_ns: 1_700_000_000_000,
            post_only: true,
            ..Default::default()
        };
        let order = order_from_proto(&msg);
        assert_eq!(order.order_id, format_uint64_id(42));
        assert_eq!(order.side, "buy");
        assert_eq!(order.status, "working");
        assert_eq!(order.order_type, "limit");
        assert_eq!(order.tif, "gtc");
        assert!(order.post_only);
        assert!(order.attached_risk.is_none());
        assert_eq!(order.orig_qty.as_ref().unwrap().as_scaled(), 100);
        let mut msg2 = msg;
        msg2.version = 7;
        let order2 = order_from_proto(&msg2);
        assert_eq!(order2.version, 7);
    }

    #[test]
    #[allow(deprecated)]
    fn order_from_proto_maps_attached_risk_policy() {
        use crate::models::{CreateOrderType, TrailingDistance};
        use crate::proto::orders::v1::{
            AttachedRisk as ProtoAttachedRisk, AttachedRiskTakeProfit, AttachedRiskTrailingStop,
            RiskExecution, RiskLimitGtc, TakeProfitPolicy, TrailingStopPolicy, risk_execution,
            trailing_stop_policy,
        };

        let msg = ProtoOrder {
            order_id: 1,
            symbol_id: 1,
            post_only: false,
            attached_risk: ProtoAttachedRisk {
                take_profit: AttachedRiskTakeProfit {
                    policy: TakeProfitPolicy {
                        trigger_price_ticks: 6000,
                        child: RiskExecution {
                            execution: Some(risk_execution::Execution::MarketIoc(Box::default())),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
                .into(),
                trailing_stop: AttachedRiskTrailingStop {
                    policy: TrailingStopPolicy {
                        activation_price_ticks: 5500,
                        trailing_distance: Some(
                            trailing_stop_policy::TrailingDistance::TrailingDistanceBps(25),
                        ),
                        max_slippage: Some(trailing_stop_policy::MaxSlippage::MaxSlippageTicks(10)),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
                .into(),
                oco: true,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let order = order_from_proto(&msg);
        let risk = order.attached_risk.expect("attached_risk");
        assert!(risk.oco);
        let tp = risk.take_profit.expect("take_profit");
        assert_eq!(tp.trigger_price.as_ticks(), 6000);
        // trigger_price_source is no longer carried on the policy wire.
        assert!(tp.trigger_price_source.is_none());
        assert_eq!(tp.order_type, Some(CreateOrderType::Market));
        assert!(tp.limit_price.is_none());
        assert!(risk.stop_loss.is_none());
        let trailing = risk.trailing_stop.expect("trailing_stop");
        assert_eq!(trailing.distance, TrailingDistance::Bps(25));
        assert_eq!(
            trailing.max_slippage,
            Some(crate::models::MaxSlippage::Ticks(10))
        );
        assert_eq!(trailing.activation_price.as_ref().unwrap().as_ticks(), 5500);
        assert!(trailing.trigger_price_source.is_none());
        assert!(trailing.order_type.is_none());

        // A LIMIT stop-loss child projects order_type=limit with its limit price.
        let sl_msg = ProtoOrder {
            order_id: 2,
            symbol_id: 1,
            attached_risk: ProtoAttachedRisk {
                stop_loss: crate::proto::orders::v1::AttachedRiskStopLoss {
                    policy: crate::proto::orders::v1::StopLossPolicy {
                        trigger_price_ticks: 4900,
                        child: RiskExecution {
                            execution: Some(risk_execution::Execution::LimitGtc(Box::new(
                                RiskLimitGtc {
                                    price_ticks: 4890,
                                    ..Default::default()
                                },
                            ))),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let sl = order_from_proto(&sl_msg)
            .attached_risk
            .expect("attached_risk")
            .stop_loss
            .expect("stop_loss");
        assert_eq!(sl.trigger_price.as_ticks(), 4900);
        assert_eq!(sl.order_type, Some(CreateOrderType::Limit));
        assert_eq!(sl.limit_price.as_ref().unwrap().as_ticks(), 4890);
    }

    #[test]
    fn filled_order_preserves_zero_leaves_and_cum_qty() {
        let msg = ProtoOrder {
            order_id: 1,
            symbol_id: 1,
            orig_qty_scaled: 100,
            cum_qty_scaled: 100,
            leaves_qty_scaled: 0,
            ..Default::default()
        };
        let order = order_from_proto(&msg);
        assert_eq!(order.cum_qty.as_ref().map(|q| q.as_scaled()), Some(100));
        assert_eq!(order.leaves_qty.as_ref().map(|q| q.as_scaled()), Some(0));
    }

    #[test]
    fn orders_list_from_open_proto() {
        let msg = GetOpenOrdersResponse {
            orders: vec![ProtoOrder {
                order_id: 1,
                symbol_id: 1,
                side: Side::Sell.into(),
                ..Default::default()
            }],
            next_page_token: "tok".into(),
            ..Default::default()
        };
        let result = orders_list_from_open(&msg);
        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.next_page_token, "tok");
        assert_eq!(result.orders[0].side, "sell");
    }

    #[test]
    fn get_order_includes_trades() {
        let msg = GetOrderResponse {
            order: ProtoOrder {
                order_id: 7,
                symbol_id: 2,
                ..Default::default()
            }
            .into(),
            trades: vec![ProtoUserTrade {
                symbol_id: 2,
                match_id: 99,
                order_id: 7,
                side: Side::Buy.into(),
                fee_scaled: 5,
                fee_asset: crate::proto::orders::v1::FeeAsset::Base.into(),
                referral_share_scaled: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = get_order_from_proto(&msg);
        assert_eq!(result.order.as_ref().unwrap().order_id, format_uint64_id(7));
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].match_id, "99");
        assert_eq!(result.trades[0].fee_scaled, "5");
        assert_eq!(result.trades[0].fee_asset, "base");
        assert_eq!(result.trades[0].referral_share_scaled, "2");
    }

    #[test]
    fn modify_and_mutation_results() {
        use crate::proto::orders::v1::{
            CancelOrderResponse, CreateOrderResponse, ModifyActionTaken, ModifyOrderResponse,
        };
        let modified = modify_order_from_proto(&ModifyOrderResponse {
            action_taken: ModifyActionTaken::Amended.into(),
            old_order_id: 10,
            final_order_id: 11,
            code: "ok".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(modified.action_taken, "amended");
        assert_eq!(modified.old_order_id, format_uint64_id(10));
        assert_eq!(modified.final_order_id, format_uint64_id(11));

        let created = order_mutation_from_create(&CreateOrderResponse {
            order_id: 42,
            client_order_id: "coid-1".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(created.status, "accepted");
        assert_eq!(created.order_id, format_uint64_id(42));
        assert_eq!(created.client_order_id, "coid-1");

        let created_without_client_id = order_mutation_from_create(&CreateOrderResponse {
            order_id: 43,
            client_order_id: String::new(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(created_without_client_id.order_id, format_uint64_id(43));
        assert!(created_without_client_id.client_order_id.is_empty());

        let cancelled = order_mutation_from_cancel(&CancelOrderResponse {
            status: "cancelled".into(),
            order_id: 42,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(cancelled.status, "cancelled");
        assert_eq!(cancelled.order_id, format_uint64_id(42));
        assert!(cancelled.client_order_id.is_empty());
    }

    #[test]
    fn singular_order_mutations_reject_empty_success_responses() {
        use crate::proto::orders::v1::{
            CancelOrderResponse, CreateOrderResponse, ModifyOrderResponse,
        };
        assert!(order_mutation_from_create(&CreateOrderResponse::default()).is_err());
        assert!(order_mutation_from_cancel(&CancelOrderResponse::default()).is_err());
        assert!(modify_order_from_proto(&ModifyOrderResponse::default()).is_err());
    }

    #[test]
    fn preview_order_surfaces_resolved_sizing_and_fee_asset() {
        use crate::proto::orders::v1::{FeeAsset, PreviewOrderResponse};

        let preview = preview_order_from_proto(&PreviewOrderResponse {
            resolved_base_qty_scaled: 100,
            price_bound_ticks: 5_000,
            estimated_quote_debit_scaled: 510,
            estimated_fee_scaled: 1,
            estimated_net_base_qty_scaled: 99,
            fee_asset: FeeAsset::Base.into(),
            fresh_at_ts_ns: 42,
            ..Default::default()
        });
        assert_eq!(
            preview
                .resolved_base_qty
                .as_ref()
                .map(|qty| qty.as_scaled()),
            Some(100)
        );
        assert_eq!(
            preview.price_bound.as_ref().map(|price| price.as_ticks()),
            Some(5_000)
        );
        assert_eq!(preview.estimated_quote_debit_scaled, 510);
        assert_eq!(preview.fee_asset, "base");
        assert_eq!(preview.fresh_at_ts_ns, 42);
    }

    #[test]
    fn batch_create_from_proto_maps_counts() {
        use crate::proto::orders::v1::{
            BatchCreateAccepted, BatchCreateResultItem as ProtoItem, batch_create_result_item,
        };
        let msg = BatchCreateOrdersResponse {
            results: vec![ProtoItem {
                client_order_id: "c1".into(),
                outcome: Some(batch_create_result_item::Outcome::Accepted(Box::new(
                    BatchCreateAccepted {
                        order_id: 9,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            }],
            accepted_count: 1,
            rejected_count: 0,
            ..Default::default()
        };
        let result = batch_create_from_proto(&msg).expect("valid batch response");
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.results[0].status, "accepted");
        assert_eq!(result.results[0].order_id, format_uint64_id(9));
        assert_eq!(result.results[0].client_order_id, "c1");
    }

    #[test]
    fn batch_create_rejects_missing_outcome() {
        use crate::proto::orders::v1::BatchCreateResultItem as ProtoItem;

        let msg = BatchCreateOrdersResponse {
            results: vec![ProtoItem {
                client_order_id: "ambiguous".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = batch_create_from_proto(&msg).expect_err("missing outcome must fail closed");
        assert!(matches!(&err, Error::ResponseContract { .. }));
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("neither accepted nor rejected"));
    }

    #[test]
    fn batch_create_preserves_unknown_rejection_code() {
        use crate::proto::orders::v1::{
            BatchCreateRejected, BatchCreateResultItem as ProtoItem, ErrorDetail,
            batch_create_result_item,
        };

        let msg = BatchCreateOrdersResponse {
            results: vec![ProtoItem {
                client_order_id: "unknown-code".into(),
                outcome: Some(batch_create_result_item::Outcome::Rejected(Box::new(
                    BatchCreateRejected {
                        error: ErrorDetail {
                            code: buffa::EnumValue::from(999),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            }],
            accepted_count: 0,
            rejected_count: 1,
            ..Default::default()
        };

        let result = batch_create_from_proto(&msg).expect("unknown code stays observable");
        assert_eq!(result.results[0].code, "UNKNOWN_ERROR_CODE(999)");
    }

    #[test]
    fn batch_create_rejects_count_mismatch() {
        use crate::proto::orders::v1::{
            BatchCreateAccepted, BatchCreateResultItem as ProtoItem, batch_create_result_item,
        };

        let msg = BatchCreateOrdersResponse {
            results: vec![ProtoItem {
                client_order_id: "accepted".into(),
                outcome: Some(batch_create_result_item::Outcome::Accepted(Box::new(
                    BatchCreateAccepted {
                        order_id: 9,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            }],
            accepted_count: 0,
            rejected_count: 1,
            ..Default::default()
        };

        let err = batch_create_from_proto(&msg).expect_err("count mismatch must fail closed");
        assert!(matches!(&err, Error::ResponseContract { .. }));
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("response counts"));
    }

    #[test]
    fn batch_cancel_rejects_count_mismatch_and_unknown_status() {
        use crate::proto::orders::v1::BatchCancelResultItem as ProtoItem;

        let mismatch = BatchCancelOrdersResponse {
            results: vec![ProtoItem {
                status: "accepted".into(),
                order_id: 9,
                ..Default::default()
            }],
            accepted_count: 0,
            rejected_count: 1,
            ..Default::default()
        };
        let err = batch_cancel_from_proto(&mismatch).expect_err("count mismatch must fail closed");
        assert!(matches!(&err, Error::ResponseContract { .. }));
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("response counts"));

        let unknown = BatchCancelOrdersResponse {
            results: vec![ProtoItem {
                status: "maybe".into(),
                ..Default::default()
            }],
            accepted_count: 1,
            ..Default::default()
        };
        let err = batch_cancel_from_proto(&unknown).expect_err("unknown status must fail closed");
        assert!(err.to_string().contains("unknown item status"));
    }

    #[test]
    fn batch_replace_reconciles_admission_counts_and_decodes_status() {
        use crate::proto::orders::v1::{
            BatchReplaceAdmissionItem as ProtoAdmissionItem, BatchReplaceAdmissionStatus,
            BatchReplaceItemAdmissionStatus, BatchReplaceOrdersResponse, BatchReplacePhase,
            BatchReplaceStatusItem as ProtoStatusItem, GetBatchReplaceStatusResponse, OrderStatus,
        };

        let valid = BatchReplaceOrdersResponse {
            batch_request_id: 9,
            status: BatchReplaceAdmissionStatus::PartiallyAdmitted.into(),
            results: vec![
                ProtoAdmissionItem {
                    item_index: 0,
                    status: BatchReplaceItemAdmissionStatus::Admitted.into(),
                    old_order_id: 1,
                    replacement_order_id: 2,
                    ..Default::default()
                },
                ProtoAdmissionItem {
                    item_index: 1,
                    status: BatchReplaceItemAdmissionStatus::Rejected.into(),
                    old_order_id: 3,
                    code: "REJECTED".into(),
                    ..Default::default()
                },
            ],
            accepted_count: 1,
            rejected_count: 1,
            ..Default::default()
        };
        let decoded = batch_replace_from_proto(&valid).expect("consistent response");
        assert_eq!(decoded.batch_request_id, format_uint64_id(9));
        assert_eq!(decoded.status, "partially_admitted");
        assert_eq!(decoded.accepted_count, 1);
        assert_eq!(decoded.rejected_count, 1);
        assert_eq!(decoded.results[0].status, "admitted");

        let mismatch = BatchReplaceOrdersResponse {
            accepted_count: 2,
            ..valid.clone()
        };
        let err = batch_replace_from_proto(&mismatch).expect_err("count mismatch must fail closed");
        assert!(matches!(&err, Error::ResponseContract { .. }));
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("response counts"));

        let status = batch_replace_status_from_proto(&GetBatchReplaceStatusResponse {
            batch_request_id: 9,
            admission_status: BatchReplaceAdmissionStatus::Admitted.into(),
            items: vec![
                ProtoStatusItem {
                    item_index: 0,
                    phase: BatchReplacePhase::Working.into(),
                    old_order_id: 1,
                    replacement_order_id: 2,
                    order_status: OrderStatus::Working.into(),
                    ..Default::default()
                },
                ProtoStatusItem {
                    item_index: 1,
                    phase: BatchReplacePhase::Terminal.into(),
                    old_order_id: 3,
                    replacement_order_id: 4,
                    order_status: OrderStatus::Filled.into(),
                    ..Default::default()
                },
            ],
            accepted_count: 2,
            rejected_count: 0,
            ..Default::default()
        })
        .expect("known phases decode");
        assert_eq!(status.admission_status, "admitted");
        assert_eq!(status.items[0].phase, "working");
        assert_eq!(status.items[1].phase, "terminal");
        assert!(status.is_settled());

        let mismatch = GetBatchReplaceStatusResponse {
            batch_request_id: 9,
            admission_status: BatchReplaceAdmissionStatus::PartiallyAdmitted.into(),
            items: vec![
                ProtoStatusItem {
                    item_index: 0,
                    phase: BatchReplacePhase::Admitted.into(),
                    ..Default::default()
                },
                ProtoStatusItem {
                    item_index: 1,
                    phase: BatchReplacePhase::Rejected.into(),
                    ..Default::default()
                },
            ],
            accepted_count: 2,
            rejected_count: 0,
            ..Default::default()
        };
        let err =
            batch_replace_status_from_proto(&mismatch).expect_err("status counts must reconcile");
        assert!(matches!(err, Error::ResponseContract { .. }));
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("response counts"));
    }

    #[test]
    fn cancel_all_requires_known_status() {
        let valid = CancelAllOrdersResponse {
            status: "submitted".into(),
            matched_orders: 2,
            submitted_cancels: 2,
            failed_cancels: 0,
            ..Default::default()
        };
        let decoded = cancel_all_from_proto(&valid).expect("submitted is valid");
        assert_eq!(decoded.status, "submitted");
        assert_eq!(decoded.matched_orders, 2);

        let dry_run = CancelAllOrdersResponse {
            status: "dry_run".into(),
            matched_orders: 3,
            ..Default::default()
        };
        assert!(cancel_all_from_proto(&dry_run).is_ok());

        let mismatched = CancelAllOrdersResponse {
            status: "submitted".into(),
            matched_orders: 3,
            submitted_cancels: 1,
            failed_cancels: 1,
            ..Default::default()
        };
        let err = cancel_all_from_proto(&mismatched).expect_err("cancel-all counts must reconcile");
        assert!(matches!(&err, Error::ResponseContract { .. }));
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert!(err.to_string().contains("response counts"));

        for status in ["", "ok", "maybe", "accepted"] {
            let bad = CancelAllOrdersResponse {
                status: status.into(),
                matched_orders: 1,
                ..Default::default()
            };
            let err = cancel_all_from_proto(&bad).expect_err("unknown cancel-all status");
            assert!(matches!(&err, Error::ResponseContract { .. }));
            assert!(!err.is_retryable());
            assert!(err.mutation_outcome_unknown());
            assert!(err.to_string().contains("unknown status"));
        }
    }

    #[test]
    fn cancel_all_after_requires_known_status() {
        let armed = CancelAllAfterResponse {
            status: "armed".into(),
            effective_timeout_sec: 30,
            expires_at_ts_ns: 99,
            ..Default::default()
        };
        let decoded = cancel_all_after_from_proto(&armed).expect("armed is valid");
        assert_eq!(decoded.status, "armed");
        assert_eq!(decoded.effective_timeout_sec, 30);
        assert_eq!(decoded.expires_at_ts_ns, "99");

        let disabled = CancelAllAfterResponse {
            status: "disabled".into(),
            ..Default::default()
        };
        assert!(cancel_all_after_from_proto(&disabled).is_ok());

        for status in ["", "ok", "submitted", "maybe"] {
            let bad = CancelAllAfterResponse {
                status: status.into(),
                effective_timeout_sec: 10,
                ..Default::default()
            };
            let err =
                cancel_all_after_from_proto(&bad).expect_err("unknown cancel-all-after status");
            assert!(err.to_string().contains("unknown status"));
        }
    }
}
