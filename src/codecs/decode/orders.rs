//! Orders read/mutation decoders.

use super::enums::{
    enum_value_order_status, enum_value_order_type, enum_value_side, enum_value_time_in_force,
};
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    BatchCancelOrdersResult, BatchCancelResultItem, BatchCreateOrdersResult, BatchCreateResultItem,
    BatchModifyOrdersResult, BatchModifyResultItem, CancelAllAfterResult, CancelAllOrdersResult,
    GetOrderResult, ModifyOrderResult, Order, OrderMutationResult, OrdersList, UserTrade,
    UserTradesList,
};
use crate::proto::orders::v1::{
    BatchCancelOrdersResponse, BatchCreateOrdersResponse, BatchModifyOrdersResponse,
    CancelAllAfterResponse, CancelAllOrdersResponse, CancelOrderResponse, CreateOrderResponse,
    GetOpenOrdersResponse, GetOrderHistoryResponse, GetOrderResponse, GetUserTradesResponse,
    ModifyOrderResponse, Order as ProtoOrder, UserTrade as ProtoUserTrade,
};

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
        state_revision: msg.state_revision,
    }
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

pub fn order_mutation_from_create(msg: &CreateOrderResponse) -> OrderMutationResult {
    order_mutation(&msg.status, msg.order_id, &msg.client_order_id)
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

pub fn batch_create_from_proto(msg: &BatchCreateOrdersResponse) -> BatchCreateOrdersResult {
    BatchCreateOrdersResult {
        results: msg
            .results
            .iter()
            .map(|item| BatchCreateResultItem {
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
            ..Default::default()
        };
        let order = order_from_proto(&msg);
        assert_eq!(order.order_id, format_uint64_id(42));
        assert_eq!(order.side, "buy");
        assert_eq!(order.status, "working");
        assert_eq!(order.order_type, "limit");
        assert_eq!(order.tif, "gtc");
        assert_eq!(order.orig_qty.as_ref().unwrap().as_scaled(), 100);
        let mut msg2 = msg;
        msg2.state_revision = 7;
        let order2 = order_from_proto(&msg2);
        assert_eq!(order2.state_revision, 7);
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
            status: "accepted".into(),
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
        use crate::proto::orders::v1::BatchCreateResultItem as ProtoItem;
        let msg = BatchCreateOrdersResponse {
            results: vec![ProtoItem {
                status: "accepted".into(),
                order_id: 9,
                client_order_id: "c1".into(),
                ..Default::default()
            }],
            accepted_count: 1,
            rejected_count: 0,
            ..Default::default()
        };
        let result = batch_create_from_proto(&msg);
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.results[0].order_id, format_uint64_id(9));
    }
}
