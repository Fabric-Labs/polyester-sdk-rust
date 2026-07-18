//! Proto enum → SDK label helpers (Go `codecs/proto_helpers` parity).

use crate::proto::orders::v1::{OrderStatus, OrderType, Side, TimeInForce};

pub fn order_side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
        Side::SideUnspecified => "",
    }
}

pub fn order_type_name(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "limit",
        OrderType::Market => "market",
        OrderType::OrderTypeUnspecified => "",
    }
}

pub fn time_in_force_name(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::TimeInForceUnspecified => "",
    }
}

pub fn order_status_name(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Pending => "pending",
        OrderStatus::PendingCancel => "pending_cancel",
        OrderStatus::Working => "working",
        OrderStatus::Filled => "filled",
        OrderStatus::Canceled => "canceled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::OrderStatusUnspecified => "",
    }
}

pub fn enum_value_side(value: buffa::EnumValue<Side>) -> &'static str {
    value.as_known().map(order_side_name).unwrap_or("")
}

pub fn enum_value_order_type(value: buffa::EnumValue<OrderType>) -> &'static str {
    value.as_known().map(order_type_name).unwrap_or("")
}

pub fn enum_value_time_in_force(value: buffa::EnumValue<TimeInForce>) -> &'static str {
    value.as_known().map(time_in_force_name).unwrap_or("")
}

pub fn enum_value_order_status(value: buffa::EnumValue<OrderStatus>) -> &'static str {
    value.as_known().map(order_status_name).unwrap_or("")
}
