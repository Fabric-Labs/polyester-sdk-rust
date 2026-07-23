//! Orders read/mutation decoders.

use super::enums::{
    enum_value_order_status, enum_value_order_type, enum_value_side, enum_value_time_in_force,
};
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    AttachedRisk, BatchCancelOrdersResult, BatchCancelResultItem, BatchCreateOrdersResult,
    BatchCreateResultItem, BatchModifyOrdersResult, BatchModifyResultItem, CancelAllAfterResult,
    CancelAllOrdersResult, CreateOrderType, GetOrderResult, MaxSlippage, ModifyOrderResult, Order,
    OrderMutationResult, OrdersList, RiskLeg, TrailingDistance, TrailingStop, UserTrade,
    UserTradesList,
};
use crate::proto::orders::v1::{
    AttachedRisk as ProtoAttachedRisk, BatchCancelOrdersResponse, BatchCreateOrdersResponse,
    BatchModifyOrdersResponse, CancelAllAfterResponse, CancelAllOrdersResponse,
    CancelOrderResponse, CreateOrderResponse, GetOpenOrdersResponse, GetOrderHistoryResponse,
    GetOrderResponse, GetUserTradesResponse, ModifyOrderResponse, Order as ProtoOrder,
    RiskExecution, StopLossPolicy, TakeProfitPolicy, TrailingStopPolicy,
    UserTrade as ProtoUserTrade, batch_create_result_item, risk_execution, trailing_stop_policy,
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
        cum_qty: decode_qty_scaled(msg.cum_qty_scaled, None, None, symbol_id_opt),
        leaves_qty: decode_qty_scaled(msg.leaves_qty_scaled, None, None, symbol_id_opt),
        price: decode_price_ticks(msg.price_ticks, None),
        avg_px: decode_price_ticks(msg.avg_price_ticks, None),
        created_ts_ns: if msg.created_ts_ns == 0 {
            String::new()
        } else {
            msg.created_ts_ns.to_string()
        },
        version: msg.version,
        post_only: msg.post_only,
        attached_risk: msg
            .attached_risk
            .as_option()
            .and_then(attached_risk_from_proto),
    }
}

/// Project an attached take-profit/stop-loss policy onto the flat public
/// [`RiskLeg`]. The child execution determines `order_type`/`limit_price`.
/// `trigger_price_source` is no longer part of the policy wire and is left empty.
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
pub fn order_mutation_from_create(msg: &CreateOrderResponse) -> OrderMutationResult {
    order_mutation("accepted", msg.order_id, &msg.client_order_id)
}

pub fn order_mutation_from_cancel(msg: &CancelOrderResponse) -> OrderMutationResult {
    order_mutation(&msg.status, msg.order_id, "")
}

fn order_mutation(status: &str, order_id: u64, client_order_id: &str) -> OrderMutationResult {
    OrderMutationResult {
        status: status.to_owned(),
        order_id: format_uint64_id(order_id),
        client_order_id: client_order_id.to_owned(),
    }
}

pub fn modify_order_from_proto(msg: &ModifyOrderResponse) -> ModifyOrderResult {
    ModifyOrderResult {
        action_taken: modify_action_name(msg.action_taken),
        old_order_id: format_uint64_id(msg.old_order_id),
        final_order_id: format_uint64_id(msg.final_order_id),
        code: msg.code.clone(),
    }
}

fn modify_action_name(
    action: buffa::EnumValue<crate::proto::orders::v1::ModifyActionTaken>,
) -> String {
    use crate::proto::orders::v1::ModifyActionTaken;
    match action.as_known() {
        Some(ModifyActionTaken::Amended) => "amended".to_owned(),
        Some(ModifyActionTaken::Replaced) => "replaced".to_owned(),
        _ => String::new(),
    }
}

pub fn cancel_all_from_proto(msg: &CancelAllOrdersResponse) -> CancelAllOrdersResult {
    CancelAllOrdersResult {
        status: msg.status.clone(),
        matched_orders: msg.matched_orders as i32,
        submitted_cancels: msg.submitted_cancels as i32,
        failed_cancels: msg.failed_cancels as i32,
    }
}

/// Per-item results now carry an `Accepted`/`Rejected` outcome oneof instead of
/// flat status/order_id/code fields.
pub fn batch_create_from_proto(msg: &BatchCreateOrdersResponse) -> BatchCreateOrdersResult {
    BatchCreateOrdersResult {
        results: msg
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
                    Some(batch_create_result_item::Outcome::Accepted(accepted)) => {
                        out.status = "accepted".to_owned();
                        out.order_id = format_uint64_id(accepted.order_id);
                    }
                    Some(batch_create_result_item::Outcome::Rejected(rejected)) => {
                        out.status = "rejected".to_owned();
                        if let Some(err) = rejected.error.as_option() {
                            out.code = err
                                .code
                                .as_known()
                                .map(|c| c.proto_name().to_owned())
                                .unwrap_or_default();
                        }
                    }
                    None => {}
                }
                out
            })
            .collect(),
        accepted_count: msg.accepted_count as i32,
        rejected_count: msg.rejected_count as i32,
    }
}

pub fn batch_cancel_from_proto(msg: &BatchCancelOrdersResponse) -> BatchCancelOrdersResult {
    BatchCancelOrdersResult {
        results: msg
            .results
            .iter()
            .map(|item| BatchCancelResultItem {
                status: item.status.clone(),
                order_id: format_uint64_id(item.order_id),
                client_order_id: item.client_order_id.clone(),
                code: item.code.clone(),
            })
            .collect(),
        accepted_count: msg.accepted_count as i32,
        rejected_count: msg.rejected_count as i32,
    }
}

pub fn batch_modify_from_proto(msg: &BatchModifyOrdersResponse) -> BatchModifyOrdersResult {
    BatchModifyOrdersResult {
        results: msg
            .results
            .iter()
            .map(|item| BatchModifyResultItem {
                status: item.status.clone(),
                client_order_id: item.client_order_id.clone(),
                final_order_id: format_uint64_id(item.final_order_id),
                code: item.code.clone(),
            })
            .collect(),
        amended_count: msg.amended_count as i32,
        replaced_count: msg.replaced_count as i32,
        rejected_count: msg.rejected_count as i32,
    }
}

pub fn cancel_all_after_from_proto(msg: &CancelAllAfterResponse) -> CancelAllAfterResult {
    CancelAllAfterResult {
        status: msg.status.clone(),
        effective_timeout_sec: msg.effective_timeout_sec as i32,
        expires_at_ts_ns: if msg.expires_at_ts_ns == 0 {
            String::new()
        } else {
            msg.expires_at_ts_ns.to_string()
        },
    }
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
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = get_order_from_proto(&msg);
        assert_eq!(result.order.as_ref().unwrap().order_id, format_uint64_id(7));
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].match_id, "99");
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
        });
        assert_eq!(modified.action_taken, "amended");
        assert_eq!(modified.old_order_id, format_uint64_id(10));
        assert_eq!(modified.final_order_id, format_uint64_id(11));

        let created = order_mutation_from_create(&CreateOrderResponse {
            order_id: 42,
            client_order_id: "coid-1".into(),
            ..Default::default()
        });
        assert_eq!(created.status, "accepted");
        assert_eq!(created.order_id, format_uint64_id(42));
        assert_eq!(created.client_order_id, "coid-1");

        let cancelled = order_mutation_from_cancel(&CancelOrderResponse {
            status: "cancelled".into(),
            order_id: 42,
            ..Default::default()
        });
        assert_eq!(cancelled.status, "cancelled");
        assert_eq!(cancelled.order_id, format_uint64_id(42));
        assert!(cancelled.client_order_id.is_empty());
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
        let result = batch_create_from_proto(&msg);
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.results[0].status, "accepted");
        assert_eq!(result.results[0].order_id, format_uint64_id(9));
        assert_eq!(result.results[0].client_order_id, "c1");
    }
}
