//! Price alert decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::errors::{Error, Result};
use crate::models::{PriceAlert, PriceAlertsPage, UpdateAlertResult};
use crate::proto::alerts::v1::{
    AlertChannel, AlertDirection, AlertPriceSource, AlertStatus, ListAlertsResponse,
    PriceAlert as ProtoPriceAlert, UpdateAlertResponse, UpdateOutcome,
};
use crate::types::Price;

const UNSPECIFIED: &str = "unspecified";

fn direction_label(value: &buffa::EnumValue<AlertDirection>) -> String {
    match value.as_known() {
        Some(AlertDirection::ABOVE) => "above",
        Some(AlertDirection::BELOW) => "below",
        _ => UNSPECIFIED,
    }
    .to_owned()
}

fn price_source_label(value: &buffa::EnumValue<AlertPriceSource>) -> String {
    match value.as_known() {
        Some(AlertPriceSource::LAST_PRICE) => "last",
        Some(AlertPriceSource::INDEX_PRICE) => "index",
        _ => UNSPECIFIED,
    }
    .to_owned()
}

fn status_label(value: &buffa::EnumValue<AlertStatus>) -> String {
    match value.as_known() {
        Some(AlertStatus::ACTIVE) => "active",
        Some(AlertStatus::TRIGGERED) => "triggered",
        Some(AlertStatus::CANCELLED) => "cancelled",
        _ => UNSPECIFIED,
    }
    .to_owned()
}

fn channel_label(value: &buffa::EnumValue<AlertChannel>) -> String {
    match value.as_known() {
        Some(AlertChannel::IN_APP) => "in_app",
        Some(AlertChannel::TELEGRAM) => "telegram",
        _ => UNSPECIFIED,
    }
    .to_owned()
}

fn outcome_label(value: &buffa::EnumValue<UpdateOutcome>) -> String {
    match value.as_known() {
        Some(UpdateOutcome::APPLIED) => "applied",
        Some(UpdateOutcome::UNCHANGED) => "unchanged",
        Some(UpdateOutcome::REPLAYED) => "replayed",
        _ => UNSPECIFIED,
    }
    .to_owned()
}

fn alert_price(ticks: i64, field: &str, symbol: &str) -> Result<Price> {
    let symbol = (!symbol.is_empty()).then(|| symbol.to_owned());
    Price::from_ticks(ticks, symbol)
        .map_err(|_| Error::response_contract("PriceAlert", format!("{field} {ticks} is negative")))
}

/// Decode an alert; `symbol` is its market's name from the catalogs, empty
/// when unknown. A negative price breaks the response contract.
pub fn price_alert_from_proto(msg: &ProtoPriceAlert, symbol: String) -> Result<PriceAlert> {
    let triggered_at = msg.triggered_at.as_option().cloned();
    let triggered_price = if triggered_at.is_some() || msg.triggered_price_ticks != 0 {
        Some(alert_price(
            msg.triggered_price_ticks,
            "triggered_price_ticks",
            &symbol,
        )?)
    } else {
        None
    };
    Ok(PriceAlert {
        alert_id: format_uint64_id(msg.alert_id),
        symbol_id: msg.symbol_id,
        direction: direction_label(&msg.direction),
        price_source: price_source_label(&msg.price_source),
        price: alert_price(msg.price_ticks, "price_ticks", &symbol)?,
        reference_price: alert_price(msg.reference_price_ticks, "reference_price_ticks", &symbol)?,
        armed: msg.armed,
        status: status_label(&msg.status),
        channels: msg.channels.iter().map(channel_label).collect(),
        revision: msg.revision,
        created_at: msg.created_at.as_option().cloned(),
        updated_at: msg.updated_at.as_option().cloned(),
        triggered_at,
        triggered_price,
        symbol,
    })
}

/// The alert of a create, cancel or update response; a response without one,
/// or with an alert that has no `alert_id`, breaks the response contract.
pub fn required_price_alert(
    alert: Option<&ProtoPriceAlert>,
    context: &str,
    symbol_of: impl Fn(u32) -> String,
) -> Result<PriceAlert> {
    let alert = alert.ok_or_else(|| Error::response_contract(context, "response has no alert"))?;
    if alert.alert_id == 0 {
        return Err(Error::response_contract(
            context,
            "response alert has no alert_id",
        ));
    }
    price_alert_from_proto(alert, symbol_of(alert.symbol_id))
}

pub fn price_alerts_page_from_proto(
    msg: &ListAlertsResponse,
    symbol_of: impl Fn(u32) -> String,
) -> Result<PriceAlertsPage> {
    Ok(PriceAlertsPage {
        alerts: msg
            .alerts
            .iter()
            .map(|alert| price_alert_from_proto(alert, symbol_of(alert.symbol_id)))
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

pub fn update_alert_result_from_proto(
    msg: &UpdateAlertResponse,
    symbol_of: impl Fn(u32) -> String,
) -> Result<UpdateAlertResult> {
    Ok(UpdateAlertResult {
        alert: required_price_alert(msg.alert.as_option(), "UpdateAlert", symbol_of)?,
        outcome: outcome_label(&msg.outcome),
    })
}
