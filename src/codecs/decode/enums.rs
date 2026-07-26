//! Proto enum → SDK label helpers (Go `codecs/proto_helpers` parity).

use crate::proto::orders::v1::{OrderStatus, OrderType, Side, TimeInForce};
use buffa::Enumeration;

pub fn enum_proto_name<T: Enumeration>(value: &buffa::EnumValue<T>) -> String {
    value
        .as_known()
        .map(|known| known.proto_name().to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

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

pub fn enum_value_side(value: buffa::EnumValue<Side>) -> String {
    value
        .as_known()
        .map(|known| order_side_name(known).to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

pub fn enum_value_order_type(value: buffa::EnumValue<OrderType>) -> String {
    value
        .as_known()
        .map(|known| order_type_name(known).to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

pub fn enum_value_time_in_force(value: buffa::EnumValue<TimeInForce>) -> String {
    value
        .as_known()
        .map(|known| time_in_force_name(known).to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

pub fn enum_value_order_status(value: buffa::EnumValue<OrderStatus>) -> String {
    value
        .as_known()
        .map(|known| order_status_name(known).to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}
