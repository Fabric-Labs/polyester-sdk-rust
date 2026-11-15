//! Proto enum → SDK label helpers (Go `codecs/proto_helpers` parity).

use crate::proto::orders::v1::{
    CancelReason, ErrorCode, FeeAsset, OrderStatus, OrderType, SelfTradePreventionMode, Side,
    TimeInForce,
};
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
        TimeInForce::Gtd => "gtd",
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

pub fn enum_value_fee_asset(value: buffa::EnumValue<FeeAsset>) -> String {
    value
        .as_known()
        .map(|known| match known {
            FeeAsset::Quote => "quote".to_owned(),
            FeeAsset::Base => "base".to_owned(),
            FeeAsset::FeeAssetUnspecified => String::new(),
        })
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

/// Self-trade prevention mode label: `expire_taker`, `expire_maker`,
/// `expire_both` or `decrement`, empty when unset, or `UNKNOWN(<number>)` for a
/// mode this SDK does not know.
pub fn enum_value_self_trade_prevention_mode(
    value: buffa::EnumValue<SelfTradePreventionMode>,
) -> String {
    match value.as_known() {
        Some(SelfTradePreventionMode::ExpireTaker) => "expire_taker".to_owned(),
        Some(SelfTradePreventionMode::ExpireMaker) => "expire_maker".to_owned(),
        Some(SelfTradePreventionMode::ExpireBoth) => "expire_both".to_owned(),
        Some(SelfTradePreventionMode::Decrement) => "decrement".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

/// An order's or order event's `reject_reason`: the `orders.v1.ErrorCode`
/// name as a short lowercase label, without its `ERROR_CODE_` prefix (for
/// example `post_only_cross`), as every Polyester SDK spells it;
/// `UNKNOWN_ERROR_CODE(<number>)` for a code this SDK does not know. `None`
/// when unset.
pub fn order_reject_reason(value: buffa::EnumValue<ErrorCode>) -> Option<String> {
    (value.to_i32() != 0).then(|| match value.as_known() {
        Some(known) => order_error_code_label(known),
        None => crate::errors::known_or_unknown_code(None, value.to_i32()),
    })
}

/// An `orders.v1.ErrorCode` as a reason label: lowercase, without the
/// `ERROR_CODE_` prefix (`ERROR_CODE_POST_ONLY_CROSS` is `post_only_cross`).
pub(crate) fn order_error_code_label(code: ErrorCode) -> String {
    let name = code.proto_name();
    name.strip_prefix("ERROR_CODE_")
        .unwrap_or(name)
        .to_ascii_lowercase()
}

/// An order's or order event's `cancel_reason`: the `orders.v1.CancelReason`
/// name as a short lowercase label, without its `CANCEL_REASON_` prefix (for
/// example `user_request` or `stp_decrement`), as every Polyester SDK spells
/// it; `UNKNOWN(<number>)` for a reason this SDK does not know. `None` when
/// unset.
pub fn order_cancel_reason(value: buffa::EnumValue<CancelReason>) -> Option<String> {
    (value.to_i32() != 0).then(|| match value.as_known() {
        Some(known) => {
            let name = known.proto_name();
            name.strip_prefix("CANCEL_REASON_")
                .unwrap_or(name)
                .to_ascii_lowercase()
        }
        None => enum_proto_name(&value),
    })
}
