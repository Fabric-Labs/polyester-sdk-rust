//! Trigger response decoders.

use super::enums::enum_value_side;
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    Trigger, TriggerDetails, TriggerEvent, TriggerEventsList, TriggerLadderDetails,
    TriggerMutationResult, TriggerStopDetails, TriggerTrailingDetails, TriggerTwapDetails,
    TriggersList,
};
use crate::types::Price;
use crate::proto::orders::v1::{
    FeeSource, SelfTradePreventionMode, TriggerDirection, TriggerPriceSource,
};
use crate::proto::triggers::v1::{
    CancelTriggerResponse, ConditionalTrigger, CreateTriggerResponse, GetTriggerResponse,
    LadderDistribution, ListTriggerEventsResponse, ListTriggersResponse, ModifyTriggerResponse,
    PauseTriggerResponse, ResumeTriggerResponse, Trigger as ProtoTrigger,
    TriggerEvent as ProtoTriggerEvent, TriggerStatus, conditional_child_execution, trigger,
    twap_trigger,
};
use buffa::Enumeration;
use buffa_types::google::protobuf::Timestamp;

pub fn trigger_status_label(status: TriggerStatus) -> &'static str {
    match status {
        TriggerStatus::StatusCreated => "created",
        TriggerStatus::StatusArmed => "armed",
        TriggerStatus::StatusRunning => "running",
        TriggerStatus::StatusCompleted => "completed",
        TriggerStatus::StatusCanceled => "cancelled",
        TriggerStatus::StatusFailed => "failed",
        TriggerStatus::StatusPaused => "paused",
        TriggerStatus::StatusUnspecified => "",
    }
}

/// Parse a trigger status filter label into a proto enum.
pub fn trigger_status_from_label(label: &str) -> Result<TriggerStatus, String> {
    match label.trim().to_ascii_lowercase().as_str() {
        "created" => Ok(TriggerStatus::StatusCreated),
        "armed" => Ok(TriggerStatus::StatusArmed),
        "running" => Ok(TriggerStatus::StatusRunning),
        "completed" => Ok(TriggerStatus::StatusCompleted),
        "cancelled" | "canceled" => Ok(TriggerStatus::StatusCanceled),
        "failed" => Ok(TriggerStatus::StatusFailed),
        "paused" => Ok(TriggerStatus::StatusPaused),
        other => Err(format!(
            "invalid trigger status {other:?}; expected one of: created, armed, running, completed, cancelled, failed, paused"
        )),
    }
}

fn enum_value_trigger_status(value: buffa::EnumValue<TriggerStatus>) -> String {
    value
        .as_known()
        .map(trigger_status_label)
        .unwrap_or("")
        .to_owned()
}

fn trigger_price_source_label(value: buffa::EnumValue<TriggerPriceSource>) -> String {
    match value.as_known() {
        Some(TriggerPriceSource::LastPrice) => "last".to_owned(),
        Some(TriggerPriceSource::IndexPrice) => "index".to_owned(),
        Some(TriggerPriceSource::MarkPrice) => "mark".to_owned(),
        _ => String::new(),
    }
}

fn trigger_direction_label(value: buffa::EnumValue<TriggerDirection>) -> String {
    match value.as_known() {
        Some(TriggerDirection::Above) => "above".to_owned(),
        Some(TriggerDirection::Below) => "below".to_owned(),
        _ => String::new(),
    }
}

fn fee_source_label(value: buffa::EnumValue<FeeSource>) -> String {
    match value.as_known() {
        Some(FeeSource::Quote) => "quote".to_owned(),
        Some(FeeSource::Received) => "received".to_owned(),
        _ => String::new(),
    }
}

fn stp_mode_label(value: buffa::EnumValue<SelfTradePreventionMode>) -> String {
    match value.as_known() {
        Some(SelfTradePreventionMode::ExpireTaker) => "expire_taker".to_owned(),
        Some(SelfTradePreventionMode::ExpireMaker) => "expire_maker".to_owned(),
        Some(SelfTradePreventionMode::ExpireBoth) => "expire_both".to_owned(),
        _ => String::new(),
    }
}

fn ladder_distribution_label(value: buffa::EnumValue<LadderDistribution>) -> String {
    match value.as_known() {
        Some(LadderDistribution::Linear) => "linear".to_owned(),
        Some(LadderDistribution::Geometric) => "geometric".to_owned(),
        Some(LadderDistribution::WeightedFavorable) => "weighted_favorable".to_owned(),
        _ => String::new(),
    }
}

fn clone_timestamp(ts: Option<&Timestamp>) -> Option<Timestamp> {
    ts.map(|t| Timestamp {
        seconds: t.seconds,
        nanos: t.nanos,
        ..Default::default()
    })
}

fn trigger_details_from_proto(
    msg: &ProtoTrigger,
    symbol: Option<String>,
    symbol_id_opt: Option<u32>,
) -> Option<TriggerDetails> {
    match msg.runtime_details.as_ref() {
        Some(trigger::RuntimeDetails::Stop(stop)) => Some(TriggerDetails::Stop(TriggerStopDetails {
            trigger_price: decode_price_ticks(stop.trigger_price_ticks, symbol.clone()),
            trigger_price_source: trigger_price_source_label(stop.trigger_price_source),
            trigger_direction: trigger_direction_label(stop.trigger_direction),
        })),
        Some(trigger::RuntimeDetails::Trailing(trailing)) => {
            Some(TriggerDetails::Trailing(TriggerTrailingDetails {
                trailing_distance: if trailing.trailing_distance_ticks > 0 {
                    decode_price_ticks(trailing.trailing_distance_ticks, symbol.clone())
                } else {
                    None
                },
                trailing_distance_bps: trailing.trailing_distance_bps,
                activation_price: if trailing.activation_price_ticks > 0 {
                    decode_price_ticks(trailing.activation_price_ticks, symbol.clone())
                } else {
                    None
                },
                peak_price: if trailing.peak_price_ticks > 0 {
                    decode_price_ticks(trailing.peak_price_ticks, symbol.clone())
                } else {
                    None
                },
                trough_price: if trailing.trough_price_ticks > 0 {
                    decode_price_ticks(trailing.trough_price_ticks, symbol.clone())
                } else {
                    None
                },
                max_slippage: if trailing.max_slippage_ticks > 0 {
                    decode_price_ticks(i64::from(trailing.max_slippage_ticks), symbol.clone())
                } else {
                    None
                },
                max_slippage_bps: trailing.max_slippage_bps,
                trigger_price_source: trigger_price_source_label(trailing.trigger_price_source),
                trigger_direction: trigger_direction_label(trailing.trigger_direction),
            }))
        }
        Some(trigger::RuntimeDetails::TwapState(twap)) => {
            Some(TriggerDetails::Twap(TriggerTwapDetails {
                twap_duration_ms: twap.twap_duration_ms,
                twap_slice_interval_ms: twap.twap_slice_interval_ms,
                slice_idx: twap.slice_idx,
                slice_count: twap.slice_count,
                executed_qty: decode_qty_scaled(
                    twap.executed_qty_scaled,
                    None,
                    symbol.clone(),
                    symbol_id_opt,
                ),
            }))
        }
        Some(trigger::RuntimeDetails::LadderState(ladder)) => {
            Some(TriggerDetails::Ladder(TriggerLadderDetails {
                ladder_price_min: if ladder.ladder_price_min_ticks > 0 {
                    decode_price_ticks(ladder.ladder_price_min_ticks, symbol.clone())
                } else {
                    None
                },
                ladder_price_max: if ladder.ladder_price_max_ticks > 0 {
                    decode_price_ticks(ladder.ladder_price_max_ticks, symbol.clone())
                } else {
                    None
                },
                ladder_levels: ladder.ladder_levels,
                ladder_distribution: ladder_distribution_label(ladder.ladder_distribution),
            }))
        }
        None => None,
    }
}

fn trigger_price_from_details(details: &Option<TriggerDetails>) -> Option<Price> {
    match details {
        Some(TriggerDetails::Stop(stop)) => stop.trigger_price.clone(),
        _ => None,
    }
}

/// Flat public trigger fields derived from the immutable `Configuration` oneof.
#[derive(Default)]
struct TriggerConfigProjection {
    trigger_type: String,
    side: String,
    order_type: String,
    time_in_force: String,
    post_only: bool,
    limit_price: Option<Price>,
    trigger_price: Option<Price>,
}

/// Derive flat child fields (side/order_type/tif/post_only/limit_price) from a
/// stop-loss / take-profit `ConditionalTrigger` configuration.
fn conditional_child_projection(
    cond: &ConditionalTrigger,
    symbol: Option<String>,
) -> (String, String, String, bool, Option<Price>) {
    let side = enum_value_side(cond.side).to_owned();
    let mut order_type = String::new();
    let mut time_in_force = String::new();
    let mut post_only = false;
    let mut limit_price = None;
    if let Some(child) = cond.child.as_option() {
        match child.execution.as_ref() {
            Some(conditional_child_execution::Execution::MarketIoc(_)) => {
                order_type = "market".to_owned();
                time_in_force = "ioc".to_owned();
            }
            Some(conditional_child_execution::Execution::LimitGtc(limit)) => {
                order_type = "limit".to_owned();
                time_in_force = "gtc".to_owned();
                post_only = limit.post_only;
                limit_price = decode_price_ticks(limit.price_ticks, symbol);
            }
            Some(conditional_child_execution::Execution::LimitIoc(limit)) => {
                order_type = "limit".to_owned();
                time_in_force = "ioc".to_owned();
                limit_price = decode_price_ticks(limit.price_ticks, symbol);
            }
            Some(conditional_child_execution::Execution::LimitFok(limit)) => {
                order_type = "limit".to_owned();
                time_in_force = "fok".to_owned();
                limit_price = decode_price_ticks(limit.price_ticks, symbol);
            }
            None => {}
        }
    }
    (side, order_type, time_in_force, post_only, limit_price)
}

/// Derive the flat public trigger fields (type/side/order_type/tif/post_only/
/// limit_price/trigger_price) from the immutable `Configuration` oneof.
fn trigger_config_projection(
    msg: &ProtoTrigger,
    symbol: Option<String>,
) -> TriggerConfigProjection {
    let mut proj = TriggerConfigProjection::default();
    match msg.configuration.as_ref() {
        Some(trigger::Configuration::StopLoss(cond)) => {
            proj.trigger_type = "stop_loss".to_owned();
            let (side, order_type, tif, post_only, limit_price) =
                conditional_child_projection(cond, symbol.clone());
            proj.side = side;
            proj.order_type = order_type;
            proj.time_in_force = tif;
            proj.post_only = post_only;
            proj.limit_price = limit_price;
            if cond.trigger_price_ticks != 0 {
                proj.trigger_price = decode_price_ticks(cond.trigger_price_ticks, symbol);
            }
        }
        Some(trigger::Configuration::TakeProfit(cond)) => {
            proj.trigger_type = "take_profit".to_owned();
            let (side, order_type, tif, post_only, limit_price) =
                conditional_child_projection(cond, symbol.clone());
            proj.side = side;
            proj.order_type = order_type;
            proj.time_in_force = tif;
            proj.post_only = post_only;
            proj.limit_price = limit_price;
            if cond.trigger_price_ticks != 0 {
                proj.trigger_price = decode_price_ticks(cond.trigger_price_ticks, symbol);
            }
        }
        Some(trigger::Configuration::TrailingStop(_)) => {
            // Trailing stop is an implicit SELL market-IOC strategy.
            proj.trigger_type = "trailing_stop".to_owned();
            proj.side = "sell".to_owned();
            proj.order_type = "market".to_owned();
            proj.time_in_force = "ioc".to_owned();
        }
        Some(trigger::Configuration::Twap(twap)) => {
            proj.trigger_type = "twap".to_owned();
            proj.side = enum_value_side(twap.side).to_owned();
            match twap.execution.as_ref() {
                Some(twap_trigger::Execution::LimitGtc(limit)) => {
                    proj.order_type = "limit".to_owned();
                    proj.time_in_force = "gtc".to_owned();
                    proj.limit_price = decode_price_ticks(limit.price_ticks, symbol);
                }
                Some(twap_trigger::Execution::MarketIoc(_)) => {
                    proj.order_type = "market".to_owned();
                    proj.time_in_force = "ioc".to_owned();
                }
                None => {}
            }
        }
        Some(trigger::Configuration::Ladder(ladder)) => {
            proj.trigger_type = "ladder".to_owned();
            proj.side = enum_value_side(ladder.side).to_owned();
            proj.order_type = "limit".to_owned();
            proj.time_in_force = "gtc".to_owned();
            proj.post_only = ladder.post_only;
        }
        None => {}
    }
    proj
}

pub fn trigger_from_proto(msg: &ProtoTrigger) -> Trigger {
    let symbol_id = msg.symbol_id;
    let symbol_id_opt = if symbol_id == 0 {
        None
    } else {
        Some(symbol_id)
    };
    let symbol = if msg.symbol.is_empty() {
        None
    } else {
        Some(msg.symbol.clone())
    };
    let details = trigger_details_from_proto(msg, symbol.clone(), symbol_id_opt);
    let proj = trigger_config_projection(msg, symbol.clone());
    // Fall back to stop runtime details for the trigger-price convenience field.
    let trigger_price = proj
        .trigger_price
        .clone()
        .or_else(|| trigger_price_from_details(&details));
    Trigger {
        trigger_id: format_uint64_id(msg.trigger_id),
        subaccount_id: format_uint64_id(msg.subaccount_id),
        symbol_id,
        symbol: msg.symbol.clone(),
        trigger_type: proj.trigger_type,
        status: enum_value_trigger_status(msg.status),
        parent_order_id: msg.parent_order_id.map(format_uint64_id),
        side: proj.side,
        order_type: proj.order_type,
        time_in_force: proj.time_in_force,
        qty: decode_qty_scaled(msg.qty_scaled, None, symbol.clone(), symbol_id_opt),
        limit_price: proj.limit_price,
        fee_source: fee_source_label(msg.fee_source),
        self_trade_prevention_mode: stp_mode_label(msg.self_trade_prevention_mode),
        post_only: proj.post_only,
        trigger_price,
        client_trigger_id: msg.client_trigger_id.clone(),
        created_at: clone_timestamp(msg.created_at.as_option()),
        updated_at: clone_timestamp(msg.updated_at.as_option()),
        armed_at: clone_timestamp(msg.armed_at.as_option()),
        completed_at: clone_timestamp(msg.completed_at.as_option()),
        child_order_ids: msg
            .child_order_ids
            .iter()
            .copied()
            .map(format_uint64_id)
            .collect(),
        details,
    }
}

pub fn triggers_list_from_proto(msg: &ListTriggersResponse) -> TriggersList {
    let triggers: Vec<_> = msg.triggers.iter().map(trigger_from_proto).collect();
    let total = triggers.len();
    TriggersList { triggers, total }
}

pub fn get_trigger_from_proto(msg: &GetTriggerResponse) -> Option<Trigger> {
    msg.trigger.as_option().map(trigger_from_proto)
}

fn trigger_mutation(
    trigger_id: u64,
    status: buffa::EnumValue<TriggerStatus>,
) -> TriggerMutationResult {
    TriggerMutationResult {
        trigger_id: format_uint64_id(trigger_id),
        status: enum_value_trigger_status(status),
    }
}

/// `CreateTriggerResponse` acknowledges admission only and no longer carries a
/// status field; synthesize `"accepted"`.
pub fn trigger_mutation_from_create(msg: &CreateTriggerResponse) -> TriggerMutationResult {
    TriggerMutationResult {
        trigger_id: format_uint64_id(msg.trigger_id),
        status: "accepted".to_owned(),
    }
}

pub fn trigger_mutation_from_cancel(msg: &CancelTriggerResponse) -> TriggerMutationResult {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_pause(msg: &PauseTriggerResponse) -> TriggerMutationResult {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_resume(msg: &ResumeTriggerResponse) -> TriggerMutationResult {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_modify(msg: &ModifyTriggerResponse) -> TriggerMutationResult {
    trigger_mutation(msg.trigger_id, msg.status)
}

fn enum_proto_name<E: Enumeration>(value: buffa::EnumValue<E>) -> String {
    value
        .as_known()
        .map(|e| e.proto_name().to_owned())
        .unwrap_or_default()
}

pub fn trigger_event_from_proto(msg: &ProtoTriggerEvent) -> TriggerEvent {
    TriggerEvent {
        trigger_id: format_uint64_id(msg.trigger_id),
        event_type: enum_proto_name(msg.event_type),
        ts_ns: if msg.ts_ns == 0 {
            String::new()
        } else {
            msg.ts_ns.to_string()
        },
    }
}

pub fn trigger_events_list_from_proto(msg: &ListTriggerEventsResponse) -> TriggerEventsList {
    TriggerEventsList {
        events: msg.events.iter().map(trigger_event_from_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::orders::v1::Side;
    use crate::proto::triggers::v1::{
        ConditionalChildExecution, ConditionalTrigger, GetTriggerResponse, ListTriggersResponse,
        StopDetails, TriggerLimitGtc,
    };

    #[test]
    fn trigger_from_proto_maps_status_and_stop_price() {
        let msg = ProtoTrigger {
            trigger_id: 42,
            subaccount_id: 9,
            symbol_id: 3,
            symbol: "ETH-USDT".into(),
            status: TriggerStatus::StatusArmed.into(),
            qty_scaled: 100,
            client_trigger_id: "cid".into(),
            configuration: Some(trigger::Configuration::StopLoss(Box::new(ConditionalTrigger {
                trigger_price_ticks: 5000,
                side: Side::Buy.into(),
                child: ConditionalChildExecution {
                    execution: Some(conditional_child_execution::Execution::LimitGtc(Box::new(
                        TriggerLimitGtc {
                            price_ticks: 4990,
                            post_only: true,
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }))),
            runtime_details: Some(trigger::RuntimeDetails::Stop(Box::new(StopDetails {
                trigger_price_ticks: 5000,
                ..Default::default()
            }))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_id, format_uint64_id(42));
        assert_eq!(t.subaccount_id, format_uint64_id(9));
        assert_eq!(t.trigger_type, "stop_loss");
        assert_eq!(t.status, "armed");
        assert_eq!(t.side, "buy");
        assert_eq!(t.order_type, "limit");
        assert_eq!(t.time_in_force, "gtc");
        assert!(t.post_only);
        assert_eq!(t.limit_price.as_ref().unwrap().as_ticks(), 4990);
        assert_eq!(t.qty.as_ref().unwrap().as_scaled(), 100);
        assert_eq!(t.trigger_price.as_ref().unwrap().as_ticks(), 5000);
        assert_eq!(t.client_trigger_id, "cid");
        assert!(matches!(t.details, Some(TriggerDetails::Stop(_))));
    }

    #[test]
    fn trigger_status_from_label_validates() {
        assert_eq!(
            trigger_status_from_label("armed").unwrap(),
            TriggerStatus::StatusArmed
        );
        assert_eq!(
            trigger_status_from_label("cancelled").unwrap(),
            TriggerStatus::StatusCanceled
        );
        assert!(trigger_status_from_label("nope").is_err());
    }

    #[test]
    fn triggers_list_and_get() {
        let listed = triggers_list_from_proto(&ListTriggersResponse {
            triggers: vec![ProtoTrigger {
                trigger_id: 1,
                symbol_id: 1,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(listed.triggers.len(), 1);
        assert_eq!(listed.total, 1);

        let got = get_trigger_from_proto(&GetTriggerResponse {
            trigger: ProtoTrigger {
                trigger_id: 3,
                symbol_id: 1,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        });
        assert_eq!(got.unwrap().trigger_id, format_uint64_id(3));
    }
}
