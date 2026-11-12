//! Trigger response decoders.

use super::enums::{enum_value_self_trade_prevention_mode, enum_value_side};
use super::money::{attach_qty_scale, decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::TsNs;
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    BatchCancelTriggersResult, BatchCancelTriggersResultItem, CancelOcoResult, CreateOcoResult,
    MaxSlippage, Trigger, TriggerDetails, TriggerEvent, TriggerEventsList, TriggerLadderDetails,
    TriggerMutationResult, TriggerStopDetails, TriggerTrailingDetails, TriggerTwapDetails,
    TriggersList,
};
use crate::proto::orders::v1::{
    FeeAsset, Side as TrailingSide, TriggerDirection, TriggerPriceSource,
};
use crate::proto::triggers::v1::{
    BatchCancelTriggersResponse, CancelOcoResponse, CancelTriggerResponse, ConditionalTrigger,
    CreateOcoResponse, CreateTriggerResponse, GetTriggerResponse, LadderDistribution,
    ListTriggerEventsResponse, ListTriggersResponse, ModifyTriggerResponse, PauseTriggerResponse,
    ResumeTriggerResponse, Trigger as ProtoTrigger, TriggerEvent as ProtoTriggerEvent,
    TriggerEventType, TriggerStatus, TriggerType, batch_cancel_triggers_result_item,
    conditional_child_execution, trailing_stop_trigger, trigger, trigger_event, trigger_market_ioc,
    twap_market_ioc, twap_trigger,
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
        TriggerStatus::StatusCanceled => "canceled",
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
        "canceled" => Ok(TriggerStatus::StatusCanceled),
        "failed" => Ok(TriggerStatus::StatusFailed),
        "paused" => Ok(TriggerStatus::StatusPaused),
        other => Err(format!(
            "invalid trigger status {other:?}; expected one of: created, armed, running, completed, canceled, failed, paused"
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

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>, unknown_prefix: &str) -> String {
    match value.as_known() {
        Some(known) if known.to_i32() == 0 => String::new(),
        Some(known) => {
            let name = known.proto_name();
            for prefix in ["TRIGGER_CANCEL_REASON_", "TRIGGER_FAILURE_REASON_"] {
                if let Some(rest) = name.strip_prefix(prefix) {
                    return rest.to_ascii_lowercase();
                }
            }
            name.to_ascii_lowercase()
        }
        None => format!("{unknown_prefix}({})", value.to_i32()),
    }
}

fn trigger_terminal_reasons(reason: Option<&trigger::TerminalReason>) -> (String, String) {
    match reason {
        Some(trigger::TerminalReason::CancelReason(value)) => (
            enum_label(value, "UNKNOWN_TRIGGER_CANCEL_REASON"),
            String::new(),
        ),
        Some(trigger::TerminalReason::FailureReason(value)) => (
            String::new(),
            enum_label(value, "UNKNOWN_TRIGGER_FAILURE_REASON"),
        ),
        None => (String::new(), String::new()),
    }
}

fn trigger_event_terminal_reasons(
    reason: Option<&trigger_event::TerminalReason>,
) -> (String, String) {
    match reason {
        Some(trigger_event::TerminalReason::CancelReason(value)) => (
            enum_label(value, "UNKNOWN_TRIGGER_CANCEL_REASON"),
            String::new(),
        ),
        Some(trigger_event::TerminalReason::FailureReason(value)) => (
            String::new(),
            enum_label(value, "UNKNOWN_TRIGGER_FAILURE_REASON"),
        ),
        None => (String::new(), String::new()),
    }
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
                trigger_price: trailing
                    .trigger_price_ticks
                    .filter(|ticks| *ticks > 0)
                    .and_then(|ticks| decode_price_ticks(ticks, symbol.clone())),
                max_slippage: if trailing.max_slippage_ticks > 0 {
                    decode_price_ticks(trailing.max_slippage_ticks, symbol.clone())
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
                executed_levels: ladder.executed_levels,
                executed_qty: decode_qty_scaled(
                    ladder.executed_qty_scaled,
                    None,
                    symbol.clone(),
                    symbol_id_opt,
                ),
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
    max_slippage: Option<MaxSlippage>,
}

/// Derive flat child fields (side/order_type/tif/post_only/limit_price) from a
/// stop-loss / take-profit `ConditionalTrigger` configuration.
fn conditional_child_projection(
    cond: &ConditionalTrigger,
    symbol: Option<String>,
) -> (
    String,
    String,
    String,
    bool,
    Option<Price>,
    Option<MaxSlippage>,
) {
    let side = enum_value_side(cond.side).to_owned();
    let mut max_slippage = None;
    let mut order_type = String::new();
    let mut time_in_force = String::new();
    let mut post_only = false;
    let mut limit_price = None;
    if let Some(child) = cond.child.as_option() {
        match child.execution.as_ref() {
            Some(conditional_child_execution::Execution::MarketIoc(market)) => {
                order_type = "market".to_owned();
                time_in_force = "ioc".to_owned();
                max_slippage = market.max_slippage.as_ref().map(|cap| match cap {
                    trigger_market_ioc::MaxSlippage::MaxSlippageTicks(ticks) => {
                        MaxSlippage::Ticks(*ticks)
                    }
                    trigger_market_ioc::MaxSlippage::MaxSlippageBps(bps) => MaxSlippage::Bps(*bps),
                });
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
    (
        side,
        order_type,
        time_in_force,
        post_only,
        limit_price,
        max_slippage,
    )
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
            let (side, order_type, tif, post_only, limit_price, max_slippage) =
                conditional_child_projection(cond, symbol.clone());
            proj.side = side;
            proj.order_type = order_type;
            proj.time_in_force = tif;
            proj.post_only = post_only;
            proj.limit_price = limit_price;
            proj.max_slippage = max_slippage;
            if cond.trigger_price_ticks != 0 {
                proj.trigger_price = decode_price_ticks(cond.trigger_price_ticks, symbol);
            }
        }
        Some(trigger::Configuration::TakeProfit(cond)) => {
            proj.trigger_type = "take_profit".to_owned();
            let (side, order_type, tif, post_only, limit_price, max_slippage) =
                conditional_child_projection(cond, symbol.clone());
            proj.side = side;
            proj.order_type = order_type;
            proj.time_in_force = tif;
            proj.post_only = post_only;
            proj.limit_price = limit_price;
            proj.max_slippage = max_slippage;
            if cond.trigger_price_ticks != 0 {
                proj.trigger_price = decode_price_ticks(cond.trigger_price_ticks, symbol);
            }
        }
        Some(trigger::Configuration::TrailingStop(trailing)) => {
            // A SELL trailing stop fires a market IOC child. A BUY one,
            // standalone or attached to an order that sells, fires an IOC
            // limit child at its maximum price (or at its slippage cap above
            // the fire price when that is lower).
            proj.trigger_type = "trailing_stop".to_owned();
            proj.side = enum_value_side(trailing.side).to_owned();
            if proj.side.is_empty() {
                proj.side = "sell".to_owned();
            }
            proj.time_in_force = "ioc".to_owned();
            let max_price = decode_price_ticks(trailing.limit_price_ticks, symbol);
            if trailing.side.as_known() == Some(TrailingSide::Buy) && max_price.is_some() {
                proj.order_type = "limit".to_owned();
                proj.limit_price = max_price;
            } else {
                proj.order_type = "market".to_owned();
            }
            proj.max_slippage = trailing.max_slippage.as_ref().map(|cap| match cap {
                trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(ticks) => {
                    MaxSlippage::Ticks(*ticks)
                }
                trailing_stop_trigger::MaxSlippage::MaxSlippageBps(bps) => MaxSlippage::Bps(*bps),
            });
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
                Some(twap_trigger::Execution::MarketIoc(market)) => {
                    proj.order_type = "market".to_owned();
                    proj.time_in_force = "ioc".to_owned();
                    // Each slice's cap; `None` leaves the pair's default.
                    proj.max_slippage = market.max_slippage.as_ref().map(|cap| match cap {
                        twap_market_ioc::MaxSlippage::MaxSlippageTicks(ticks) => {
                            MaxSlippage::Ticks(*ticks)
                        }
                        twap_market_ioc::MaxSlippage::MaxSlippageBps(bps) => MaxSlippage::Bps(*bps),
                    });
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

/// A trailing stop's cap from its runtime details, for a trigger that carries
/// no configuration; the configuration's cap is read first.
fn trailing_details_cap(
    configuration: Option<&trigger::Configuration>,
    details: Option<&TriggerDetails>,
) -> Option<MaxSlippage> {
    let (None, Some(TriggerDetails::Trailing(trailing))) = (configuration, details) else {
        return None;
    };
    match (&trailing.max_slippage, trailing.max_slippage_bps) {
        (Some(ticks), _) => Some(MaxSlippage::Ticks(ticks.as_ticks())),
        (None, bps) if bps > 0 => Some(MaxSlippage::Bps(bps)),
        _ => None,
    }
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
    let (cancel_reason, failure_reason) = trigger_terminal_reasons(msg.terminal_reason.as_ref());
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
        self_trade_prevention_mode: enum_value_self_trade_prevention_mode(
            msg.self_trade_prevention_mode,
        ),
        post_only: proj.post_only,
        trigger_price,
        max_slippage: proj
            .max_slippage
            .or_else(|| trailing_details_cap(msg.configuration.as_ref(), details.as_ref())),
        client_trigger_id: msg.client_trigger_id.clone(),
        created_at: clone_timestamp(msg.created_at.as_option()),
        updated_at: clone_timestamp(msg.updated_at.as_option()),
        armed_at: clone_timestamp(msg.armed_at.as_option()),
        completed_at: clone_timestamp(msg.completed_at.as_option()),
        cancel_reason,
        failure_reason,
        details,
        version: (msg.version != 0).then_some(msg.version),
    }
}

/// Gives a read trigger's quantities (`qty` and a TWAP's or ladder's
/// `executed_qty`) the pair's base quantity scale (`base_scale`, from the
/// catalogs by `symbol_id`); the wire carries none.
pub(crate) fn attach_trigger_scale(trigger: &mut Trigger, base_scale: Option<u32>) {
    attach_qty_scale(&mut trigger.qty, base_scale);
    match trigger.details.as_mut() {
        Some(TriggerDetails::Twap(twap)) => attach_qty_scale(&mut twap.executed_qty, base_scale),
        Some(TriggerDetails::Ladder(ladder)) => {
            attach_qty_scale(&mut ladder.executed_qty, base_scale)
        }
        _ => {}
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

/// `CreateOcoResponse` acknowledges the OCO and both legs; an answer missing
/// any of their IDs or the echoed client OCO ID is refused.
pub fn create_oco_from_proto(msg: &CreateOcoResponse) -> crate::errors::Result<CreateOcoResult> {
    if msg.order_list_id == 0
        || msg.take_profit_trigger_id == 0
        || msg.stop_loss_trigger_id == 0
        || msg.client_list_id.trim().is_empty()
    {
        return Err(crate::Error::transport(
            "invalid CreateOco response: missing order_list_id, client_list_id or a leg trigger_id",
        ));
    }
    Ok(CreateOcoResult {
        order_list_id: format_uint64_id(msg.order_list_id),
        client_list_id: msg.client_list_id.clone(),
        take_profit_trigger_id: format_uint64_id(msg.take_profit_trigger_id),
        stop_loss_trigger_id: format_uint64_id(msg.stop_loss_trigger_id),
    })
}

pub fn cancel_oco_from_proto(msg: &CancelOcoResponse) -> crate::errors::Result<CancelOcoResult> {
    let status = enum_value_trigger_status(msg.status);
    if msg.order_list_id == 0 || status.is_empty() {
        return Err(crate::Error::transport(
            "invalid CancelOco response: missing order_list_id or status",
        ));
    }
    Ok(CancelOcoResult {
        order_list_id: format_uint64_id(msg.order_list_id),
        status,
    })
}

fn batch_cancel_triggers_item_status_name(
    status: &buffa::EnumValue<batch_cancel_triggers_result_item::Status>,
) -> Option<&'static str> {
    match status.as_known()? {
        batch_cancel_triggers_result_item::Status::Accepted => Some("accepted"),
        batch_cancel_triggers_result_item::Status::Rejected => Some("rejected"),
        batch_cancel_triggers_result_item::Status::StatusUnspecified => None,
    }
}

/// Per-trigger results of a batch cancel, as `batch_cancel_from_proto` reads
/// a batch order cancel: an item with an unknown status, or counts that do not
/// add up to the items, are refused.
pub fn batch_cancel_triggers_from_proto(
    msg: &BatchCancelTriggersResponse,
) -> crate::errors::Result<BatchCancelTriggersResult> {
    let mut accepted = 0u32;
    let mut rejected = 0u32;
    let mut results = Vec::with_capacity(msg.results.len());
    for item in &msg.results {
        let status = batch_cancel_triggers_item_status_name(&item.status).ok_or_else(|| {
            crate::Error::response_contract(
                "BatchCancelTriggers",
                format!("unknown item status {}", item.status.to_i32()),
            )
        })?;
        match status {
            "accepted" => accepted += 1,
            "rejected" => rejected += 1,
            _ => unreachable!("known batch cancel status"),
        }
        results.push(BatchCancelTriggersResultItem {
            status: status.to_owned(),
            // Zero is no trigger: it stays empty instead of reading as an ID.
            trigger_id: if item.trigger_id == 0 {
                String::new()
            } else {
                format_uint64_id(item.trigger_id)
            },
            code: item.code.clone(),
            rate_limit: item.error.as_option().and_then(|err| {
                err.rate_limit
                    .as_option()
                    .map(super::rate_limit_detail_from_proto)
            }),
            violations: super::orders::field_violations_from_proto(item.error.as_option()),
        });
    }
    if accepted != msg.accepted_count
        || rejected != msg.rejected_count
        || usize::try_from(accepted + rejected).ok() != Some(results.len())
    {
        return Err(crate::Error::response_contract(
            "BatchCancelTriggers",
            format!(
                "response counts mismatch: decoded {accepted} accepted/{rejected} rejected for {} results, server reported {}/{}",
                results.len(),
                msg.accepted_count,
                msg.rejected_count
            ),
        ));
    }
    Ok(BatchCancelTriggersResult {
        results,
        accepted_count: msg.accepted_count,
        rejected_count: msg.rejected_count,
    })
}

fn trigger_event_type_label(value: buffa::EnumValue<TriggerEventType>) -> String {
    match value.as_known() {
        Some(TriggerEventType::EventFired) => "fired".to_owned(),
        Some(TriggerEventType::EventCanceled) => "canceled".to_owned(),
        Some(TriggerEventType::EventUpdated) => "updated".to_owned(),
        Some(TriggerEventType::EventFailed) => "failed".to_owned(),
        Some(TriggerEventType::EventActivated) => "activated".to_owned(),
        Some(TriggerEventType::EventArmed) => "armed".to_owned(),
        Some(TriggerEventType::EventFireRefused) => "fire_refused".to_owned(),
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
        "canceled" => Ok(TriggerEventType::EventCanceled),
        "updated" => Ok(TriggerEventType::EventUpdated),
        "failed" => Ok(TriggerEventType::EventFailed),
        "activated" => Ok(TriggerEventType::EventActivated),
        "armed" => Ok(TriggerEventType::EventArmed),
        "fire_refused" => Ok(TriggerEventType::EventFireRefused),
        other => Err(format!(
            "invalid trigger event type {other:?}; expected one of: fired, canceled, updated, failed, activated, armed, fire_refused"
        )),
    }
}

pub fn trigger_event_from_proto(msg: &ProtoTriggerEvent) -> crate::errors::Result<TriggerEvent> {
    let (cancel_reason, failure_reason) =
        trigger_event_terminal_reasons(msg.terminal_reason.as_ref());
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
        cancel_reason,
        failure_reason,
        fire_refusal_reason: msg
            .fire_refusal_reason
            .as_ref()
            .map(|reason| enum_label(reason, "UNKNOWN_TRIGGER_FAILURE_REASON"))
            .unwrap_or_default(),
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
        ListTriggerEventsResponse, ListTriggersResponse, StopDetails, TriggerCancelReason,
        TriggerEventType, TriggerFailureReason, TriggerLimitGtc, TriggerType,
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
    fn trigger_from_proto_maps_version() {
        let versioned = |version: u32| {
            trigger_from_proto(&ProtoTrigger {
                trigger_id: 1,
                version,
                ..Default::default()
            })
            .version
        };
        assert_eq!(versioned(1), Some(1));
        assert_eq!(versioned(42), Some(42));
        // Servers before trigger versions omit the field.
        assert_eq!(versioned(0), None);
        let listed = triggers_list_from_proto(&ListTriggersResponse {
            triggers: vec![ProtoTrigger {
                trigger_id: 1,
                version: 3,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(listed.triggers[0].version, Some(3));
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
    fn trigger_from_proto_reads_a_market_child_slippage_cap() {
        use crate::proto::triggers::v1::{ConditionalChildExecution, TriggerMarketIoc};

        let market = |cap: Option<trigger_market_ioc::MaxSlippage>| ProtoTrigger {
            trigger_id: 13,
            symbol_id: 1,
            configuration: Some(trigger::Configuration::StopLoss(Box::new(
                ConditionalTrigger {
                    side: Side::Buy.into(),
                    trigger_price_ticks: 85_000_000_000_000,
                    child: buffa::MessageField::some(ConditionalChildExecution {
                        execution: Some(conditional_child_execution::Execution::MarketIoc(
                            Box::new(TriggerMarketIoc {
                                max_slippage: cap,
                                ..Default::default()
                            }),
                        )),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        let t = trigger_from_proto(&market(Some(
            trigger_market_ioc::MaxSlippage::MaxSlippageTicks(25_000_000_000),
        )));
        assert_eq!(t.side, "buy");
        assert_eq!(t.order_type, "market");
        assert_eq!(t.trigger_price.as_ref().unwrap().format(), "85000");
        assert_eq!(t.max_slippage, Some(MaxSlippage::Ticks(25_000_000_000)));
        assert_eq!(trigger_from_proto(&market(None)).max_slippage, None);

        // Two triggers that differ only in their cap must not decode alike.
        let ten = trigger_from_proto(&market(Some(
            trigger_market_ioc::MaxSlippage::MaxSlippageBps(10),
        )));
        let fifty = trigger_from_proto(&market(Some(
            trigger_market_ioc::MaxSlippage::MaxSlippageBps(50),
        )));
        assert_eq!(ten.max_slippage, Some(MaxSlippage::Bps(10)));
        assert_eq!(fifty.max_slippage, Some(MaxSlippage::Bps(50)));
        assert_ne!(ten, fifty);

        // The stream decoder and the list and get decoders use the same projection.
        let bytes = buffa::Message::encode_to_vec(&market(Some(
            trigger_market_ioc::MaxSlippage::MaxSlippageBps(10),
        )));
        let streamed = crate::codecs::decode::trigger_from_bytes(&bytes).unwrap();
        assert_eq!(streamed.max_slippage, Some(MaxSlippage::Bps(10)));
        let listed = triggers_list_from_proto(&ListTriggersResponse {
            triggers: vec![market(Some(
                trigger_market_ioc::MaxSlippage::MaxSlippageBps(50),
            ))],
            ..Default::default()
        });
        assert_eq!(listed.triggers[0].max_slippage, Some(MaxSlippage::Bps(50)));
        let got = get_trigger_from_proto(&GetTriggerResponse {
            trigger: buffa::MessageField::some(market(Some(
                trigger_market_ioc::MaxSlippage::MaxSlippageBps(10),
            ))),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(got.max_slippage, Some(MaxSlippage::Bps(10)));
    }

    /// POLY-9396: a market TWAP's per-slice cap (`twap.market_ioc.max_slippage`)
    /// reads back as `max_slippage`, in its own variant, through every decoder;
    /// no cap reads `None` (each slice uses the pair's default slippage).
    #[test]
    fn poly_9396_trigger_reads_a_market_twap_slice_cap() {
        use crate::proto::triggers::v1::{TwapMarketIoc, TwapTrigger, twap_market_ioc};

        let twap = |cap: Option<twap_market_ioc::MaxSlippage>| ProtoTrigger {
            trigger_id: 21,
            symbol_id: 1,
            qty_scaled: 100_000,
            configuration: Some(trigger::Configuration::Twap(Box::new(TwapTrigger {
                side: Side::Sell.into(),
                duration_ms: 60_000,
                slice_interval_ms: 5_000,
                execution: Some(twap_trigger::Execution::MarketIoc(Box::new(
                    TwapMarketIoc {
                        max_slippage: cap,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            }))),
            ..Default::default()
        };
        let cases = [
            (
                Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(10)),
                Some(MaxSlippage::Bps(10)),
            ),
            (
                Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(50)),
                Some(MaxSlippage::Bps(50)),
            ),
            (
                Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(
                    25_000_000_000,
                )),
                Some(MaxSlippage::Ticks(25_000_000_000)),
            ),
            (None, None),
        ];
        let mut failures = Vec::new();
        for (cap, want) in cases {
            let msg = twap(cap);
            // The generated wire keeps the cap; the typed read must too.
            let bytes = buffa::Message::encode_to_vec(&msg);
            let json = serde_json::to_vec(&msg).unwrap();
            let from_json: ProtoTrigger = serde_json::from_slice(&json).unwrap();
            assert_eq!(from_json, msg, "proto JSON round trip");
            let reads = [
                ("from_proto", trigger_from_proto(&msg)),
                ("from_json", trigger_from_proto(&from_json)),
                (
                    "from_bytes",
                    crate::codecs::decode::trigger_from_bytes(&bytes).unwrap(),
                ),
                (
                    "list",
                    triggers_list_from_proto(&ListTriggersResponse {
                        triggers: vec![msg.clone()],
                        ..Default::default()
                    })
                    .triggers
                    .remove(0),
                ),
                (
                    "get",
                    get_trigger_from_proto(&GetTriggerResponse {
                        trigger: buffa::MessageField::some(msg.clone()),
                        ..Default::default()
                    })
                    .unwrap(),
                ),
            ];
            for (path, read) in reads {
                assert_eq!(read.trigger_type, "twap", "{path}");
                assert_eq!(read.order_type, "market", "{path}");
                if read.max_slippage != want {
                    failures.push(format!(
                        "{path}: got {:?}, want {want:?}",
                        read.max_slippage
                    ));
                }
            }
        }
        assert!(failures.is_empty(), "TWAP slice cap lost: {failures:#?}");
    }

    /// POLY-9425: a trailing stop's cap reads at the top level, from its
    /// configuration, or from its runtime details when it has none; the
    /// details keep their own copy.
    #[test]
    fn poly_9425_trailing_stop_cap_reads_at_the_top_level() {
        use crate::proto::triggers::v1::{TrailingDetails, TrailingStopTrigger};

        let configured = |cap| ProtoTrigger {
            trigger_id: 31,
            symbol_id: 1,
            configuration: Some(trigger::Configuration::TrailingStop(Box::new(
                TrailingStopTrigger {
                    side: Side::Sell.into(),
                    max_slippage: cap,
                    ..Default::default()
                },
            ))),
            runtime_details: Some(trigger::RuntimeDetails::Trailing(Box::new(
                TrailingDetails {
                    max_slippage_bps: 100,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        let t = trigger_from_proto(&configured(Some(
            trailing_stop_trigger::MaxSlippage::MaxSlippageBps(100),
        )));
        assert_eq!(t.max_slippage, Some(MaxSlippage::Bps(100)));
        let Some(TriggerDetails::Trailing(details)) = t.details.as_ref() else {
            panic!("expected trailing details");
        };
        assert_eq!(details.max_slippage_bps, 100);
        let t = trigger_from_proto(&configured(Some(
            trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(7_000_000_000),
        )));
        assert_eq!(t.max_slippage, Some(MaxSlippage::Ticks(7_000_000_000)));

        for (details, want) in [
            (
                TrailingDetails {
                    max_slippage_bps: 100,
                    ..Default::default()
                },
                Some(MaxSlippage::Bps(100)),
            ),
            (
                TrailingDetails {
                    max_slippage_ticks: 7_000_000_000,
                    ..Default::default()
                },
                Some(MaxSlippage::Ticks(7_000_000_000)),
            ),
            (TrailingDetails::default(), None),
        ] {
            let msg = ProtoTrigger {
                trigger_id: 32,
                symbol_id: 1,
                runtime_details: Some(trigger::RuntimeDetails::Trailing(Box::new(details))),
                ..Default::default()
            };
            assert_eq!(trigger_from_proto(&msg).max_slippage, want);
        }
    }

    #[test]
    fn trigger_from_proto_reads_buy_trailing_stop_maximum_and_trough() {
        use crate::proto::triggers::v1::{
            TrailingDetails, TrailingStopTrigger, trailing_stop_trigger,
        };

        let msg = ProtoTrigger {
            trigger_id: 12,
            symbol_id: 1,
            status: TriggerStatus::StatusArmed.into(),
            qty_scaled: 1_000_000,
            configuration: Some(trigger::Configuration::TrailingStop(Box::new(
                TrailingStopTrigger {
                    side: Side::Buy.into(),
                    activation_price_ticks: 80_000_000_000_000,
                    // Maximum price 85000.25.
                    limit_price_ticks: 85_000_250_000_000,
                    trailing_distance: Some(
                        trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(100),
                    ),
                    max_slippage: Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(50)),
                    ..Default::default()
                },
            ))),
            runtime_details: Some(trigger::RuntimeDetails::Trailing(Box::new(
                TrailingDetails {
                    trailing_distance_bps: 100,
                    activation_price_ticks: 80_000_000_000_000,
                    // Lowest price seen 79500.5; the trigger is 1% above it.
                    trough_price_ticks: 79_500_500_000_000,
                    trigger_price_ticks: Some(80_295_505_000_000),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        assert_eq!(t.trigger_type, "trailing_stop");
        assert_eq!(t.side, "buy");
        // A BUY trailing child is an IOC limit order at the maximum price.
        assert_eq!(t.order_type, "limit");
        assert_eq!(t.time_in_force, "ioc");
        assert_eq!(t.limit_price.as_ref().unwrap().format(), "85000.25");
        assert_eq!(t.max_slippage, Some(MaxSlippage::Bps(50)));
        let Some(TriggerDetails::Trailing(trailing)) = t.details.as_ref() else {
            panic!("expected trailing details, got {:?}", t.details);
        };
        assert_eq!(trailing.trough_price.as_ref().unwrap().format(), "79500.5");
        assert!(trailing.peak_price.is_none());
        assert_eq!(
            trailing.activation_price.as_ref().unwrap().format(),
            "80000"
        );
        assert_eq!(
            trailing.trigger_price.as_ref().unwrap().format(),
            "80295.505"
        );

        // Before it is armed, a trailing stop has no threshold yet.
        let mut unarmed = msg.clone();
        if let Some(trigger::RuntimeDetails::Trailing(details)) = unarmed.runtime_details.as_mut() {
            details.trigger_price_ticks = None;
        }
        let Some(TriggerDetails::Trailing(trailing)) = trigger_from_proto(&unarmed).details else {
            panic!("expected trailing details");
        };
        assert!(trailing.trigger_price.is_none());

        // A SELL trailing stop has no maximum price.
        let mut sell = msg.clone();
        if let Some(trigger::Configuration::TrailingStop(config)) = sell.configuration.as_mut() {
            config.side = Side::Sell.into();
            config.limit_price_ticks = 0;
        }
        let t = trigger_from_proto(&sell);
        assert_eq!(t.side, "sell");
        // A SELL trailing child is a market IOC order; its cap stays in max_slippage.
        assert_eq!(t.order_type, "market");
        assert_eq!(t.time_in_force, "ioc");
        assert!(t.limit_price.is_none());
        assert_eq!(t.max_slippage, Some(MaxSlippage::Bps(50)));
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
    fn trigger_from_proto_projects_ladder_executed_fields() {
        use crate::proto::triggers::v1::{LadderDetails, LadderTrigger};

        let msg = ProtoTrigger {
            trigger_id: 9,
            symbol_id: 1,
            status: TriggerStatus::StatusRunning.into(),
            qty_scaled: 100_000_000,
            configuration: Some(trigger::Configuration::Ladder(Box::new(LadderTrigger {
                side: Side::Buy.into(),
                price_min_ticks: 1,
                price_max_ticks: 2,
                levels: 4,
                ..Default::default()
            }))),
            runtime_details: Some(trigger::RuntimeDetails::LadderState(Box::new(
                LadderDetails {
                    ladder_price_min_ticks: 1,
                    ladder_price_max_ticks: 2,
                    ladder_levels: 4,
                    executed_qty_scaled: 25_000_000,
                    executed_levels: 2,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        let t = trigger_from_proto(&msg);
        let Some(TriggerDetails::Ladder(ladder)) = t.details.as_ref() else {
            panic!("expected ladder details");
        };
        assert_eq!(ladder.executed_levels, 2);
        assert_eq!(
            ladder.executed_qty.as_ref().unwrap().as_scaled(),
            25_000_000
        );
    }

    #[test]
    fn trigger_status_from_label_validates() {
        assert_eq!(
            trigger_status_from_label("armed").unwrap(),
            TriggerStatus::StatusArmed
        );
        assert_eq!(
            trigger_status_from_label("canceled").unwrap(),
            TriggerStatus::StatusCanceled
        );
        assert!(trigger_status_from_label("nope").is_err());
    }

    /// POLY-9303: a canceled trigger reads `canceled`, the contract's
    /// spelling, as its events and the order statuses do. The British
    /// `cancelled` is neither returned nor accepted as a filter.
    #[test]
    fn canceled_trigger_reads_and_filters_as_canceled() {
        let read = trigger_from_proto(&ProtoTrigger {
            trigger_id: 5,
            status: TriggerStatus::StatusCanceled.into(),
            ..Default::default()
        });
        let canceled = trigger_mutation_from_cancel(&CancelTriggerResponse {
            trigger_id: 5,
            status: TriggerStatus::StatusCanceled.into(),
            ..Default::default()
        })
        .unwrap();
        let labels = [
            (
                "trigger_status_label",
                trigger_status_label(TriggerStatus::StatusCanceled).to_owned(),
            ),
            ("trigger read", read.status),
            ("cancel result", canceled.status),
        ];
        for (from, label) in labels {
            assert_eq!(label, "canceled", "{from}");
        }
        assert!(crate::models::TRIGGER_STATUS_VALUES.contains(&"canceled"));
        assert!(!crate::models::TRIGGER_STATUS_VALUES.contains(&"cancelled"));
        let status = trigger_status_from_label("cancelled").expect_err("status alias");
        assert!(status.contains("canceled, failed"), "{status}");
        let event = trigger_event_type_from_label("cancelled").expect_err("event type alias");
        assert!(event.contains("canceled"), "{event}");
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
    fn oco_answers_name_the_oco_and_its_legs() {
        let created = create_oco_from_proto(&CreateOcoResponse {
            order_list_id: 70,
            client_list_id: "oco-1".into(),
            take_profit_trigger_id: 71,
            stop_loss_trigger_id: 72,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            created,
            CreateOcoResult {
                order_list_id: format_uint64_id(70),
                client_list_id: "oco-1".into(),
                take_profit_trigger_id: format_uint64_id(71),
                stop_loss_trigger_id: format_uint64_id(72),
            }
        );
        for broken in [
            CreateOcoResponse::default(),
            CreateOcoResponse {
                order_list_id: 0,
                ..created_oco()
            },
            CreateOcoResponse {
                client_list_id: " ".into(),
                ..created_oco()
            },
            CreateOcoResponse {
                take_profit_trigger_id: 0,
                ..created_oco()
            },
            CreateOcoResponse {
                stop_loss_trigger_id: 0,
                ..created_oco()
            },
        ] {
            let err = create_oco_from_proto(&broken).unwrap_err();
            assert!(err.mutation_outcome_unknown(), "{err:?}");
        }

        let canceled = cancel_oco_from_proto(&CancelOcoResponse {
            order_list_id: 70,
            status: TriggerStatus::StatusCanceled.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(canceled.order_list_id, format_uint64_id(70));
        assert_eq!(canceled.status, "canceled");
        assert!(cancel_oco_from_proto(&CancelOcoResponse::default()).is_err());
        assert!(
            cancel_oco_from_proto(&CancelOcoResponse {
                status: TriggerStatus::StatusCanceled.into(),
                ..Default::default()
            })
            .is_err()
        );
    }

    fn created_oco() -> CreateOcoResponse {
        CreateOcoResponse {
            order_list_id: 70,
            client_list_id: "oco-1".into(),
            take_profit_trigger_id: 71,
            stop_loss_trigger_id: 72,
            ..Default::default()
        }
    }

    #[test]
    fn batch_cancel_triggers_fails_closed_on_unknown_status_or_counts() {
        use crate::proto::triggers::v1::BatchCancelTriggersResultItem as Item;
        let item = |status: batch_cancel_triggers_result_item::Status, id: u64| Item {
            status: status.into(),
            trigger_id: id,
            ..Default::default()
        };
        let ok = batch_cancel_triggers_from_proto(&BatchCancelTriggersResponse {
            results: vec![
                item(batch_cancel_triggers_result_item::Status::Accepted, 42),
                Item {
                    code: "TRIGGER_NOT_FOUND".into(),
                    ..item(batch_cancel_triggers_result_item::Status::Rejected, 43)
                },
            ],
            accepted_count: 1,
            rejected_count: 1,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(ok.results[0].status, "accepted");
        assert_eq!(ok.results[0].trigger_id, format_uint64_id(42));
        assert_eq!(ok.results[0].code, "");
        assert_eq!(ok.results[1].status, "rejected");
        assert_eq!(ok.results[1].trigger_id, format_uint64_id(43));
        assert_eq!(ok.results[1].code, "TRIGGER_NOT_FOUND");
        assert_eq!((ok.accepted_count, ok.rejected_count), (1, 1));

        let mismatch = BatchCancelTriggersResponse {
            results: vec![item(
                batch_cancel_triggers_result_item::Status::Accepted,
                42,
            )],
            accepted_count: 2,
            ..Default::default()
        };
        let err = batch_cancel_triggers_from_proto(&mismatch).unwrap_err();
        assert!(err.to_string().contains("counts mismatch"), "{err}");

        let unknown = BatchCancelTriggersResponse {
            results: vec![item(
                batch_cancel_triggers_result_item::Status::StatusUnspecified,
                42,
            )],
            ..Default::default()
        };
        let err = batch_cancel_triggers_from_proto(&unknown).unwrap_err();
        assert!(err.to_string().contains("unknown item status"), "{err}");
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
                terminal_reason: Some(trigger_event::TerminalReason::CancelReason(
                    TriggerCancelReason::TRIGGER_CANCEL_REASON_USER_REQUEST.into(),
                )),
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
            terminal_reason: Some(trigger_event::TerminalReason::FailureReason(
                TriggerFailureReason::TRIGGER_FAILURE_REASON_INSUFFICIENT_FUNDS.into(),
            )),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(failed.event_type, "failed");
        assert_eq!(failed.failure_reason, "insufficient_funds");
        assert!(failed.cancel_reason.is_empty());
        assert_eq!(event.trigger_type, "take_profit");
        assert_eq!(event.subaccount_id, format_uint64_id(9));
        assert_eq!(event.child_seq, 3);
        assert_eq!(event.child_order_id, format_uint64_id(77));
        assert_eq!(event.fire_price.as_ref().unwrap().as_ticks(), 100);
        assert_eq!(event.cancel_reason, "user_request");
        assert!(event.failure_reason.is_empty());
    }

    #[test]
    fn newer_trigger_terminal_reasons_are_named() {
        let cancels = [
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_ACCOUNT_REPAIR,
                "account_repair",
            ),
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_PAIR_DRAIN_TRIGGER,
                "pair_drain_trigger",
            ),
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_PAIR_DRAIN_TWAP,
                "pair_drain_twap",
            ),
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_PAIR_DRAIN_LADDER,
                "pair_drain_ladder",
            ),
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_PAIR_DRAIN_ATTACHED_LEG,
                "pair_drain_attached_leg",
            ),
            (
                TriggerCancelReason::TRIGGER_CANCEL_REASON_MARKET_HALTED,
                "market_halted",
            ),
        ];
        for (reason, want) in cancels {
            let trigger = trigger_from_proto(&ProtoTrigger {
                trigger_id: 1,
                terminal_reason: Some(trigger::TerminalReason::CancelReason(reason.into())),
                ..Default::default()
            });
            assert_eq!(trigger.cancel_reason, want);
            let event = trigger_event_from_proto(&ProtoTriggerEvent {
                trigger_id: 1,
                terminal_reason: Some(trigger_event::TerminalReason::CancelReason(reason.into())),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(event.cancel_reason, want);
        }

        let failures = [
            (
                TriggerFailureReason::TRIGGER_FAILURE_REASON_INDEX_PRICE_UNAVAILABLE,
                "index_price_unavailable",
            ),
            (
                TriggerFailureReason::TRIGGER_FAILURE_REASON_TRIGGER_WOULD_TRIGGER_IMMEDIATELY,
                "trigger_would_trigger_immediately",
            ),
            // A trigger a halted market refuses ends `failed` with the named
            // reason 8, on the trigger and on its `failed` event.
            (
                TriggerFailureReason::TRIGGER_FAILURE_REASON_MARKET_HALTED,
                "market_halted",
            ),
        ];
        for (reason, want) in failures {
            let trigger = trigger_from_proto(&ProtoTrigger {
                trigger_id: 1,
                terminal_reason: Some(trigger::TerminalReason::FailureReason(reason.into())),
                ..Default::default()
            });
            assert_eq!(trigger.failure_reason, want);
            let event = trigger_event_from_proto(&ProtoTriggerEvent {
                trigger_id: 1,
                terminal_reason: Some(trigger_event::TerminalReason::FailureReason(reason.into())),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(event.failure_reason, want);
        }
    }

    #[test]
    fn trigger_terminal_reasons_map_oneof_and_unspecified_is_empty() {
        let canceled = trigger_from_proto(&ProtoTrigger {
            trigger_id: 5,
            terminal_reason: Some(trigger::TerminalReason::CancelReason(
                TriggerCancelReason::TRIGGER_CANCEL_REASON_OCO.into(),
            )),
            ..Default::default()
        });
        assert_eq!(canceled.cancel_reason, "oco");
        assert!(canceled.failure_reason.is_empty());

        let unspecified = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 6,
            terminal_reason: Some(trigger_event::TerminalReason::CancelReason(
                TriggerCancelReason::TRIGGER_CANCEL_REASON_UNSPECIFIED.into(),
            )),
            ..Default::default()
        })
        .unwrap();
        assert!(unspecified.cancel_reason.is_empty());
        assert!(unspecified.failure_reason.is_empty());

        let unknown = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 7,
            terminal_reason: Some(trigger_event::TerminalReason::FailureReason(
                buffa::EnumValue::Unknown(321),
            )),
            ..Default::default()
        })
        .unwrap();
        assert!(unknown.cancel_reason.is_empty());
        assert_eq!(
            unknown.failure_reason,
            "UNKNOWN_TRIGGER_FAILURE_REASON(321)"
        );

        // Unknown reasons still decode with their number.
        let unknown = trigger_from_proto(&ProtoTrigger {
            trigger_id: 8,
            terminal_reason: Some(trigger::TerminalReason::FailureReason(
                buffa::EnumValue::Unknown(321),
            )),
            ..Default::default()
        });
        assert_eq!(
            unknown.failure_reason,
            "UNKNOWN_TRIGGER_FAILURE_REASON(321)"
        );
        let unknown_event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 8,
            terminal_reason: Some(trigger_event::TerminalReason::FailureReason(
                buffa::EnumValue::Unknown(321),
            )),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            unknown_event.failure_reason,
            "UNKNOWN_TRIGGER_FAILURE_REASON(321)"
        );
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
        assert_eq!(
            trigger_event_type_from_label("activated").unwrap(),
            TriggerEventType::EventActivated
        );
        assert_eq!(
            trigger_event_type_from_label("armed").unwrap(),
            TriggerEventType::EventArmed
        );
        assert_eq!(
            trigger_event_type_from_label("fire_refused").unwrap(),
            TriggerEventType::EventFireRefused
        );
        assert!(trigger_event_type_from_label("nope").is_err());
    }

    #[test]
    fn trigger_event_decodes_a_refused_fire_and_its_reason() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 6,
            trigger_type: TriggerType::TakeProfit.into(),
            event_type: TriggerEventType::EventFireRefused.into(),
            child_seq: 1,
            fire_price_ticks: Some(104_000_000_000),
            fire_refusal_reason: Some(TriggerFailureReason::RateLimited.into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.event_type, "fire_refused");
        assert_eq!(event.fire_refusal_reason, "rate_limited");
        assert_eq!(event.child_seq, 1);
        assert!(event.child_order_id.is_empty());
        assert_eq!(event.fire_price.unwrap().format(), "104");
        // A refused fire is not terminal: no terminal reason is set.
        assert!(event.cancel_reason.is_empty() && event.failure_reason.is_empty());

        for (reason, want) in [
            (
                TriggerFailureReason::AccountRepairing.into(),
                "account_repairing".to_owned(),
            ),
            (
                TriggerFailureReason::MarketHalted.into(),
                "market_halted".to_owned(),
            ),
            (
                TriggerFailureReason::MinNotional.into(),
                "min_notional".to_owned(),
            ),
            (
                TriggerFailureReason::SelfTradePrevented.into(),
                "self_trade_prevented".to_owned(),
            ),
            (
                buffa::EnumValue::Unknown(321),
                "UNKNOWN_TRIGGER_FAILURE_REASON(321)".to_owned(),
            ),
        ] {
            let event = trigger_event_from_proto(&ProtoTriggerEvent {
                trigger_id: 6,
                event_type: TriggerEventType::EventFireRefused.into(),
                fire_refusal_reason: Some(reason),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(event.fire_refusal_reason, want);
        }

        // Only a refused fire carries a refusal reason.
        let fired = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 6,
            event_type: TriggerEventType::EventFired.into(),
            ..Default::default()
        })
        .unwrap();
        assert!(fired.fire_refusal_reason.is_empty());
    }

    #[test]
    fn account_repairing_is_a_named_trigger_failure_reason() {
        let failed = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 7,
            event_type: TriggerEventType::EventFailed.into(),
            terminal_reason: Some(trigger_event::TerminalReason::FailureReason(
                TriggerFailureReason::AccountRepairing.into(),
            )),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(failed.failure_reason, "account_repairing");
    }

    #[test]
    fn self_trade_prevented_is_a_refused_fire_that_names_its_child_order() {
        // Self-trade prevention expired the attached exit's order: the leg
        // stays live, and `child_seq` names the order that was placed and
        // expired. It is no terminal reason.
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 6,
            trigger_type: TriggerType::StopLoss.into(),
            event_type: TriggerEventType::EventFireRefused.into(),
            child_seq: 2,
            child_order_id: 77,
            fire_price_ticks: Some(104_000_000_000),
            fire_refusal_reason: Some(
                TriggerFailureReason::TRIGGER_FAILURE_REASON_SELF_TRADE_PREVENTED.into(),
            ),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.event_type, "fire_refused");
        assert_eq!(event.fire_refusal_reason, "self_trade_prevented");
        assert_eq!(event.child_seq, 2);
        assert_eq!(event.child_order_id, format_uint64_id(77));
        assert!(event.cancel_reason.is_empty() && event.failure_reason.is_empty());
    }

    #[test]
    fn trigger_event_decodes_trailing_activation() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 4,
            trigger_type: TriggerType::TrailingStop.into(),
            event_type: TriggerEventType::EventActivated.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.event_type, "activated");
    }

    #[test]
    fn trigger_event_decodes_attached_leg_armed() {
        let event = trigger_event_from_proto(&ProtoTriggerEvent {
            trigger_id: 5,
            trigger_type: TriggerType::TakeProfit.into(),
            event_type: TriggerEventType::EventArmed.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(event.event_type, "armed");
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
