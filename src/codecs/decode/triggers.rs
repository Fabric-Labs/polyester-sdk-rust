//! Trigger response decoders.

use super::enums::enum_value_side;
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    Trigger, TriggerEvent, TriggerEventsList, TriggerMutationResult, TriggersList,
};
use crate::proto::triggers::v1::{
    CancelTriggerResponse, CreateTriggerResponse, GetTriggerResponse, ListTriggerEventsResponse,
    ListTriggersResponse, PauseTriggerResponse, ResumeTriggerResponse, Trigger as ProtoTrigger,
    TriggerEvent as ProtoTriggerEvent, TriggerStatus,
};
use buffa::Enumeration;

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

fn enum_value_trigger_status(value: buffa::EnumValue<TriggerStatus>) -> String {
    value
        .as_known()
        .map(trigger_status_label)
        .unwrap_or("")
        .to_owned()
}

fn enum_proto_name<E: Enumeration>(value: buffa::EnumValue<E>) -> String {
    value
        .as_known()
        .map(|e| e.proto_name().to_owned())
        .unwrap_or_default()
}

fn trigger_price_ticks(msg: &ProtoTrigger) -> i64 {
    match msg.details.as_ref() {
        Some(crate::proto::triggers::v1::__buffa::oneof::trigger::Details::Stop(stop)) => {
            stop.trigger_price_ticks
        }
        _ => 0,
    }
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
    Trigger {
        trigger_id: format_uint64_id(msg.trigger_id),
        symbol_id,
        symbol: msg.symbol.clone(),
        trigger_type: enum_proto_name(msg.trigger_type),
        status: enum_value_trigger_status(msg.status),
        side: enum_value_side(msg.side).to_owned(),
        qty: decode_qty_scaled(msg.qty_scaled, None, symbol.clone(), symbol_id_opt),
        trigger_price: decode_price_ticks(trigger_price_ticks(msg), symbol),
        client_trigger_id: msg.client_trigger_id.clone(),
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

pub fn trigger_mutation_from_create(msg: &CreateTriggerResponse) -> TriggerMutationResult {
    trigger_mutation(msg.trigger_id, msg.status)
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
        GetTriggerResponse, ListTriggersResponse, StopDetails, TriggerType,
    };

    #[test]
    fn trigger_from_proto_maps_status_and_stop_price() {
        let msg = ProtoTrigger {
            trigger_id: 42,
            symbol_id: 3,
            symbol: "ETH-USDT".into(),
            trigger_type: TriggerType::TriggerTypeUnspecified.into(),
            status: TriggerStatus::StatusArmed.into(),
            side: Side::Buy.into(),
            qty_scaled: 100,
            client_trigger_id: "cid".into(),
            details: Some(
                crate::proto::triggers::v1::__buffa::oneof::trigger::Details::Stop(Box::new(
                    StopDetails {
                        trigger_price_ticks: 5000,
                        ..Default::default()
                    },
                )),
            ),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_id, format_uint64_id(42));
        assert_eq!(t.status, "armed");
        assert_eq!(t.side, "buy");
        assert_eq!(t.qty.as_ref().unwrap().as_scaled(), 100);
        assert_eq!(t.trigger_price.as_ref().unwrap().as_ticks(), 5000);
        assert_eq!(t.client_trigger_id, "cid");
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
