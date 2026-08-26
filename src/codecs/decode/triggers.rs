//! Trigger response decoders.

use super::enums::enum_value_side;
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::TsNs;
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    Trigger, TriggerDetails, TriggerEvent, TriggerEventsList, TriggerLadderDetails,
    TriggerMutationResult, TriggerStopDetails, TriggerTrailingDetails, TriggerTwapDetails,
    TriggersList,
};
use crate::proto::orders::v1::{
    FeeAsset, SelfTradePreventionMode, TriggerDirection, TriggerPriceSource,
};
use crate::proto::triggers::v1::{
    CancelTriggerResponse, ConditionalTrigger, CreateTriggerResponse, GetTriggerResponse,
    LadderDistribution, ListTriggerEventsResponse, ListTriggersResponse, ModifyTriggerResponse,
    PauseTriggerResponse, ResumeTriggerResponse, Trigger as ProtoTrigger,
    TriggerEvent as ProtoTriggerEvent, TriggerEventType, TriggerStatus, TriggerType,
    conditional_child_execution, trigger, twap_trigger,
};
use crate::types::Price;
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
        .map(str::to_owned)
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

fn trigger_price_source_label(value: buffa::EnumValue<TriggerPriceSource>) -> String {
    match value.as_known() {
        Some(TriggerPriceSource::LastPrice) => "last".to_owned(),
        Some(TriggerPriceSource::IndexPrice) => "index".to_owned(),
        Some(TriggerPriceSource::MarkPrice) => "mark".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

fn trigger_direction_label(value: buffa::EnumValue<TriggerDirection>) -> String {
    match value.as_known() {
        Some(TriggerDirection::Above) => "above".to_owned(),
        Some(TriggerDirection::Below) => "below".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

fn fee_asset_label(value: buffa::EnumValue<FeeAsset>) -> String {
    match value.as_known() {
        Some(FeeAsset::Quote) => "quote".to_owned(),
        Some(FeeAsset::Base) => "base".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

fn stp_mode_label(value: buffa::EnumValue<SelfTradePreventionMode>) -> String {
    match value.as_known() {
        Some(SelfTradePreventionMode::ExpireTaker) => "expire_taker".to_owned(),
        Some(SelfTradePreventionMode::ExpireMaker) => "expire_maker".to_owned(),
        Some(SelfTradePreventionMode::ExpireBoth) => "expire_both".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

fn ladder_distribution_label(value: buffa::EnumValue<LadderDistribution>) -> String {
    match value.as_known() {
        Some(LadderDistribution::Linear) => "linear".to_owned(),
        Some(LadderDistribution::Geometric) => "geometric".to_owned(),
        Some(LadderDistribution::WeightedFavorable) => "weighted_favorable".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
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
        Some(trigger::RuntimeDetails::Stop(stop)) => {
            Some(TriggerDetails::Stop(TriggerStopDetails {
                trigger_price: decode_price_ticks(stop.trigger_price_ticks, symbol.clone()),
                trigger_price_source: trigger_price_source_label(stop.trigger_price_source),
                trigger_direction: trigger_direction_label(stop.trigger_direction),
            }))
        }
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
        Some(trigger::Configuration::TrailingStop(trailing)) => {
            // Trailing stop is market-IOC; side is carried on the wire (standalone
            // creates are SELL; attached risk may be either side opposite parent).
            proj.trigger_type = "trailing_stop".to_owned();
            proj.side = enum_value_side(trailing.side).to_owned();
            if proj.side.is_empty() {
                proj.side = "sell".to_owned();
            }
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
    let details = trigger_details_from_proto(msg, None, symbol_id_opt);
    let proj = trigger_config_projection(msg, None);
    // Fall back to stop runtime details for the trigger-price convenience field.
    let trigger_price = proj
        .trigger_price
        .clone()
        .or_else(|| trigger_price_from_details(&details));
    Trigger {
        trigger_id: format_uint64_id(msg.trigger_id),
        subaccount_id: format_uint64_id(msg.subaccount_id),
        symbol_id,
        symbol: String::new(),
        trigger_type: proj.trigger_type,
        status: enum_value_trigger_status(msg.status),
        parent_order_id: msg.parent_order_id.map(format_uint64_id),
        side: proj.side,
        order_type: proj.order_type,
        time_in_force: proj.time_in_force,
        qty: decode_qty_scaled(msg.qty_scaled, None, None, symbol_id_opt),
        limit_price: proj.limit_price,
        fee_asset: fee_asset_label(msg.fee_asset),
        self_trade_prevention_mode: stp_mode_label(msg.self_trade_prevention_mode),
        post_only: proj.post_only,
        trigger_price,
        client_trigger_id: msg.client_trigger_id.clone(),
        created_at: clone_timestamp(msg.created_at.as_option()),
        updated_at: clone_timestamp(msg.updated_at.as_option()),
        armed_at: clone_timestamp(msg.armed_at.as_option()),
        completed_at: clone_timestamp(msg.completed_at.as_option()),
        details,
    }
}

pub fn triggers_list_from_proto(msg: &ListTriggersResponse) -> TriggersList {
    let triggers: Vec<_> = msg.triggers.iter().map(trigger_from_proto).collect();
    let total = triggers.len();
    TriggersList {
        triggers,
        total,
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn get_trigger_from_proto(msg: &GetTriggerResponse) -> Option<Trigger> {
    msg.trigger.as_option().map(trigger_from_proto)
}

fn trigger_mutation(
    trigger_id: u64,
    status: buffa::EnumValue<TriggerStatus>,
) -> crate::errors::Result<TriggerMutationResult> {
    let status = enum_value_trigger_status(status);
    if trigger_id == 0 || status.is_empty() {
        return Err(crate::Error::transport(
            "invalid trigger mutation response: missing trigger_id or status",
        ));
    }
    Ok(TriggerMutationResult {
        trigger_id: format_uint64_id(trigger_id),
        client_trigger_id: String::new(),
        status,
    })
}

/// `CreateTriggerResponse` acknowledges admission only and no longer carries a
/// status field; synthesize `"accepted"`.
pub fn trigger_mutation_from_create(
    msg: &CreateTriggerResponse,
) -> crate::errors::Result<TriggerMutationResult> {
    if msg.trigger_id == 0 || msg.client_trigger_id.trim().is_empty() {
        return Err(crate::Error::transport(
            "invalid CreateTrigger response: missing trigger_id or client_trigger_id",
        ));
    }
    Ok(TriggerMutationResult {
        trigger_id: format_uint64_id(msg.trigger_id),
        client_trigger_id: msg.client_trigger_id.clone(),
        status: "accepted".to_owned(),
    })
}

pub fn trigger_mutation_from_cancel(
    msg: &CancelTriggerResponse,
) -> crate::errors::Result<TriggerMutationResult> {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_pause(
    msg: &PauseTriggerResponse,
) -> crate::errors::Result<TriggerMutationResult> {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_resume(
    msg: &ResumeTriggerResponse,
) -> crate::errors::Result<TriggerMutationResult> {
    trigger_mutation(msg.trigger_id, msg.status)
}

pub fn trigger_mutation_from_modify(
    msg: &ModifyTriggerResponse,
) -> crate::errors::Result<TriggerMutationResult> {
    trigger_mutation(msg.trigger_id, msg.status)
}

fn trigger_event_type_label(value: buffa::EnumValue<TriggerEventType>) -> String {
    match value.as_known() {
        Some(TriggerEventType::EventFired) => "fired".to_owned(),
        Some(TriggerEventType::EventCanceled) => "canceled".to_owned(),
        Some(TriggerEventType::EventUpdated) => "updated".to_owned(),
        Some(TriggerEventType::EventFailed) => "failed".to_owned(),
        Some(TriggerEventType::EventUnspecified) => String::new(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

fn trigger_type_label(value: buffa::EnumValue<TriggerType>) -> String {
    match value.as_known() {
        Some(TriggerType::TriggerTypeUnspecified) => String::new(),
        Some(known) => known
            .proto_name()
            .trim_start_matches("TRIGGER_TYPE_")
            .to_ascii_lowercase(),
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

/// Parse a trigger event type filter label into a proto enum.
pub fn trigger_event_type_from_label(label: &str) -> Result<TriggerEventType, String> {
    match label.trim().to_ascii_lowercase().as_str() {
        "fired" => Ok(TriggerEventType::EventFired),
        "canceled" | "cancelled" => Ok(TriggerEventType::EventCanceled),
        "updated" => Ok(TriggerEventType::EventUpdated),
        "failed" => Ok(TriggerEventType::EventFailed),
        other => Err(format!(
            "invalid trigger event type {other:?}; expected one of: fired, canceled, updated, failed"
        )),
    }
}

pub fn trigger_event_from_proto(msg: &ProtoTriggerEvent) -> crate::errors::Result<TriggerEvent> {
    Ok(TriggerEvent {
        trigger_id: format_uint64_id(msg.trigger_id),
        subaccount_id: if msg.subaccount_id == 0 {
            String::new()
        } else {
            format_uint64_id(msg.subaccount_id)
        },
        symbol_id: msg.symbol_id,
        trigger_type: trigger_type_label(msg.trigger_type),
        event_type: trigger_event_type_label(msg.event_type),
        ts_ns: TsNs::from_wire(msg.ts_ns, "TriggerEvent.ts_ns")?.optional_string(),
        child_seq: msg.child_seq,
        child_order_id: if msg.child_order_id == 0 {
            String::new()
        } else {
            format_uint64_id(msg.child_order_id)
        },
        fire_price: msg
            .fire_price_ticks
            .and_then(|ticks| decode_price_ticks(ticks, None)),
        reason: msg.reason.clone(),
    })
}

pub fn trigger_events_list_from_proto(
    msg: &ListTriggerEventsResponse,
) -> crate::errors::Result<TriggerEventsList> {
    Ok(TriggerEventsList {
        events: msg
            .events
            .iter()
            .map(trigger_event_from_proto)
            .collect::<crate::errors::Result<Vec<_>>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::orders::v1::Side;
    use crate::proto::triggers::v1::{
        ConditionalChildExecution, ConditionalTrigger, GetTriggerResponse,
        ListTriggerEventsResponse, ListTriggersResponse, StopDetails, TriggerEventType,
        TriggerLimitGtc, TriggerType,
    };

    #[test]
    fn trigger_from_proto_projects_attached_trailing_stop_side_and_parent() {
        use crate::proto::triggers::v1::TrailingStopTrigger;

        let msg = ProtoTrigger {
            trigger_id: 77,
            subaccount_id: 9,
            symbol_id: 3,
            status: TriggerStatus::StatusArmed.into(),
            parent_order_id: Some(9001),
            qty_scaled: 100,
            client_trigger_id: "trail-attached".into(),
            configuration: Some(trigger::Configuration::TrailingStop(Box::new(
                TrailingStopTrigger {
                    side: Side::Buy.into(),
                    trailing_distance: Some(
                        crate::proto::triggers::v1::trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(
                            50,
                        ),
                    ),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_type, "trailing_stop");
        assert_eq!(t.side, "buy");
        assert_eq!(t.order_type, "market");
        assert_eq!(t.time_in_force, "ioc");
        assert_eq!(t.parent_order_id, Some(format_uint64_id(9001)));
    }

    #[test]
    fn trigger_from_proto_maps_status_and_stop_price() {
        let msg = ProtoTrigger {
            trigger_id: 42,
            subaccount_id: 9,
            symbol_id: 3,
            status: TriggerStatus::StatusArmed.into(),
            qty_scaled: 100,
            client_trigger_id: "cid".into(),
            configuration: Some(trigger::Configuration::StopLoss(Box::new(
                ConditionalTrigger {
                    trigger_price_ticks: 5000,
                    side: Side::Buy.into(),
                    child: ConditionalChildExecution {
                        execution: Some(conditional_child_execution::Execution::LimitGtc(
                            Box::new(TriggerLimitGtc {
                                price_ticks: 4990,
                                post_only: true,
                                ..Default::default()
                            }),
                        )),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
            ))),
            runtime_details: Some(trigger::RuntimeDetails::Stop(Box::new(StopDetails {
                trigger_price_ticks: 5000,
                ..Default::default()
            }))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_id, format_uint64_id(42));
        assert_eq!(t.subaccount_id, format_uint64_id(9));
        assert_eq!(t.symbol, "");
        assert_eq!(t.symbol_id, 3);
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
    fn trigger_from_proto_projects_twap_executed_qty() {
        use crate::proto::triggers::v1::{TwapDetails, TwapTrigger};

        let msg = ProtoTrigger {
            trigger_id: 11,
            symbol_id: 1,
            status: TriggerStatus::StatusRunning.into(),
            qty_scaled: 100_000_000,
            client_trigger_id: "twap-1".into(),
            configuration: Some(trigger::Configuration::Twap(Box::new(TwapTrigger {
                side: Side::Buy.into(),
                duration_ms: 60_000,
                slice_interval_ms: 5_000,
                execution: Some(twap_trigger::Execution::MarketIoc(Box::default())),
                ..Default::default()
            }))),
            runtime_details: Some(trigger::RuntimeDetails::TwapState(Box::new(TwapDetails {
                twap_duration_ms: 60_000,
                twap_slice_interval_ms: 5_000,
                slice_idx: 2,
                slice_count: 12,
                executed_qty_scaled: 25_000_000,
                ..Default::default()
            }))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_type, "twap");
        assert_eq!(t.side, "buy");
        assert_eq!(t.order_type, "market");
        let Some(TriggerDetails::Twap(twap)) = t.details.as_ref() else {
            panic!("expected twap details");
        };
        assert_eq!(twap.slice_idx, 2);
        assert_eq!(twap.slice_count, 12);
        assert_eq!(twap.executed_qty.as_ref().unwrap().as_scaled(), 25_000_000);
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
    fn singular_trigger_mutations_reject_empty_success_responses() {
        assert!(trigger_mutation_from_create(&CreateTriggerResponse::default()).is_err());
        assert!(trigger_mutation_from_cancel(&CancelTriggerResponse::default()).is_err());
        assert!(trigger_mutation_from_pause(&PauseTriggerResponse::default()).is_err());
        assert!(trigger_mutation_from_resume(&ResumeTriggerResponse::default()).is_err());
        assert!(trigger_mutation_from_modify(&ModifyTriggerResponse::default()).is_err());

        let created = trigger_mutation_from_create(&CreateTriggerResponse {
            trigger_id: 7,
            client_trigger_id: "stable-trigger".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(created.client_trigger_id, "stable-trigger");
    }

    #[test]
    fn triggers_list_and_get() {
        let listed = triggers_list_from_proto(&ListTriggersResponse {
            triggers: vec![ProtoTrigger {
                trigger_id: 1,
                symbol_id: 1,
                ..Default::default()
            }],
            next_page_token: "trig-page-2".into(),
            ..Default::default()
        });
        assert_eq!(listed.triggers.len(), 1);
        assert_eq!(listed.total, 1);
        assert_eq!(listed.next_page_token, "trig-page-2");

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

    #[test]
    fn trigger_events_list_keeps_next_page_token() {
        let listed = trigger_events_list_from_proto(&ListTriggerEventsResponse {
            events: vec![ProtoTriggerEvent {
                trigger_id: 1,
                subaccount_id: 9,
                symbol_id: 2,
                trigger_type: TriggerType::TakeProfit.into(),
                event_type: TriggerEventType::EventFired.into(),
                ts_ns: 123,
                child_seq: 3,
                child_order_id: 77,
                fire_price_ticks: Some(100),
                reason: "hit".into(),
                ..Default::default()
            }],
            next_page_token: "evt-page-2".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(listed.events.len(), 1);
        assert_eq!(listed.next_page_token, "evt-page-2");
        let event = &listed.events[0];
        assert_eq!(event.event_type, "fired");
        let failed = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 2,
            event_type: TriggerEventType::EventFailed.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(failed.event_type, "failed");
        assert_eq!(event.trigger_type, "take_profit");
        assert_eq!(event.subaccount_id, format_uint64_id(9));
        assert_eq!(event.child_seq, 3);
        assert_eq!(event.child_order_id, format_uint64_id(77));
        assert_eq!(event.fire_price.as_ref().unwrap().as_ticks(), 100);
        assert_eq!(event.reason, "hit");
    }

    #[test]
    fn trigger_event_absent_fire_price_is_none() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 1,
            trigger_type: TriggerType::Twap.into(),
            event_type: TriggerEventType::EventFired.into(),
            child_seq: 1,
            fire_price_ticks: None,
            ..Default::default()
        })
        .unwrap();
        assert!(event.fire_price.is_none());
    }

    #[test]
    fn trigger_event_preserves_unknown_event_type_number() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 1,
            event_type: buffa::EnumValue::Unknown(321),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.event_type, "UNKNOWN(321)");
    }

    #[test]
    fn trigger_event_type_from_label_validates() {
        assert_eq!(
            trigger_event_type_from_label("fired").unwrap(),
            TriggerEventType::EventFired
        );
        assert_eq!(
            trigger_event_type_from_label("canceled").unwrap(),
            TriggerEventType::EventCanceled
        );
        assert_eq!(
            trigger_event_type_from_label("failed").unwrap(),
            TriggerEventType::EventFailed
        );
        assert!(trigger_event_type_from_label("nope").is_err());
    }

    #[test]
    fn trigger_event_accepts_millisecond_shaped_ts_ns() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 1,
            ts_ns: 1_700_000_000_000,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.ts_ns, "1700000000000");
    }
}
