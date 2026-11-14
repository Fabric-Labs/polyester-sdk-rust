//! Server build version and status event decoders.

use crate::codecs::decode::changelog_enum_label;
use crate::codecs::scalars::format_id;
use crate::models::{ServerVersion, StatusEvent, StatusEventUpdate, StatusEventsPage};
use crate::proto::system::v1::{
    GetVersionResponse, ListStatusEventsResponse, StatusEvent as ProtoStatusEvent,
    StatusEventUpdate as ProtoStatusEventUpdate,
};

pub fn server_version_from_proto(msg: &GetVersionResponse) -> ServerVersion {
    ServerVersion {
        version: msg.version.clone(),
        commit: msg.commit.clone(),
        environment: msg.environment.clone(),
    }
}

fn status_event_update_from_proto(msg: &ProtoStatusEventUpdate) -> StatusEventUpdate {
    StatusEventUpdate {
        id: msg.id,
        message: msg.message.clone(),
        created_at: msg.created_at.as_option().cloned(),
    }
}

pub fn status_event_from_proto(msg: &ProtoStatusEvent) -> StatusEvent {
    StatusEvent {
        id: format_id(msg.id),
        kind: changelog_enum_label(&msg.kind),
        state: changelog_enum_label(&msg.state),
        title: msg.title.clone(),
        message: msg.message.clone(),
        start_time: msg.start_time.as_option().cloned(),
        end_time: msg.end_time.as_option().cloned(),
        services: msg.services.iter().map(changelog_enum_label).collect(),
        markets: msg.markets.clone(),
        updated_at: msg.updated_at.as_option().cloned(),
        impact: changelog_enum_label(&msg.impact),
        assets: msg.assets.clone(),
        cancelled_at: msg.cancelled_at.as_option().cloned(),
        updates: msg
            .updates
            .iter()
            .map(status_event_update_from_proto)
            .collect(),
        details_url: msg.details_url.clone(),
    }
}

pub fn status_events_page_from_proto(msg: &ListStatusEventsResponse) -> StatusEventsPage {
    StatusEventsPage {
        events: msg.events.iter().map(status_event_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::system::v1::{
        AffectedService, StatusEventImpact, StatusEventKind, StatusEventState,
    };
    use buffa_types::google::protobuf::Timestamp;

    fn ts(seconds: i64, nanos: i32) -> buffa::MessageField<Timestamp> {
        Timestamp {
            seconds,
            nanos,
            ..Default::default()
        }
        .into()
    }

    #[test]
    fn status_event_reads_labels_notes_and_optional_fields() {
        let msg = ListStatusEventsResponse {
            events: vec![
                ProtoStatusEvent {
                    id: 7_433_029_815_562_201_289,
                    kind: StatusEventKind::INCIDENT.into(),
                    state: StatusEventState::IN_PROGRESS.into(),
                    title: "SOL deposits delayed".into(),
                    message: "SOL deposits are slow.".into(),
                    start_time: ts(1_794_515_400, 0),
                    services: vec![
                        AffectedService::DEPOSITS.into(),
                        AffectedService::MARKET_DATA.into(),
                        buffa::EnumValue::Unknown(42),
                    ],
                    assets: vec!["SOL".into()],
                    impact: StatusEventImpact::PARTIAL_OUTAGE.into(),
                    details_url: Some("https://example.com/a".into()),
                    updates: vec![ProtoStatusEventUpdate {
                        id: 1,
                        message: "Credits are resuming.".into(),
                        created_at: ts(1_794_516_300, 5),
                        ..Default::default()
                    }],
                    updated_at: ts(1_794_516_300, 5),
                    ..Default::default()
                },
                ProtoStatusEvent {
                    id: 9,
                    kind: StatusEventKind::MAINTENANCE.into(),
                    state: StatusEventState::CANCELLED.into(),
                    start_time: ts(1_794_600_000, 0),
                    end_time: ts(1_794_603_600, 0),
                    services: vec![AffectedService::TRADING.into()],
                    markets: vec!["BTC-USDT".into()],
                    cancelled_at: ts(1_794_550_000, 0),
                    ..Default::default()
                },
            ],
            next_page_token: "p2".into(),
            ..Default::default()
        };
        let page = status_events_page_from_proto(&msg);
        assert_eq!(page.next_page_token, "p2");
        let incident = &page.events[0];
        assert_eq!(incident.id, format_id(7_433_029_815_562_201_289));
        assert_eq!(incident.kind, "incident");
        assert_eq!(incident.state, "in_progress");
        assert_eq!(incident.impact, "partial_outage");
        assert_eq!(
            incident.services,
            ["deposits", "market_data", "unknown(42)"]
        );
        assert_eq!(incident.assets, ["SOL"]);
        assert!(incident.markets.is_empty());
        // No expected end yet, not cancelled: absent, not the epoch.
        assert_eq!(incident.end_time, None);
        assert_eq!(incident.cancelled_at, None);
        assert_eq!(
            incident.details_url.as_deref(),
            Some("https://example.com/a")
        );
        assert_eq!(incident.updates.len(), 1);
        assert_eq!(incident.updates[0].id, 1);
        assert_eq!(incident.updates[0].message, "Credits are resuming.");
        assert_eq!(incident.updates[0].created_at.as_ref().unwrap().nanos, 5);

        let maintenance = &page.events[1];
        assert_eq!(maintenance.kind, "maintenance");
        assert_eq!(maintenance.state, "cancelled");
        // Maintenance carries no impact.
        assert_eq!(maintenance.impact, "");
        assert_eq!(maintenance.services, ["trading"]);
        assert_eq!(maintenance.markets, ["BTC-USDT"]);
        assert_eq!(
            maintenance.end_time.as_ref().unwrap().seconds,
            1_794_603_600
        );
        assert_eq!(
            maintenance.cancelled_at.as_ref().unwrap().seconds,
            1_794_550_000
        );
        // An event without an announcement has no link, not an empty one.
        assert_eq!(maintenance.details_url, None);
    }

    /// Every affected service reads as its lowercase label.
    #[test]
    fn status_event_service_labels_cover_every_service() {
        use crate::models::{
            STATUS_EVENT_SERVICE_ACCOUNTS, STATUS_EVENT_SERVICE_API, STATUS_EVENT_SERVICE_APP,
            STATUS_EVENT_SERVICE_DEPOSITS, STATUS_EVENT_SERVICE_MARKET_DATA,
            STATUS_EVENT_SERVICE_QUICKSWAP, STATUS_EVENT_SERVICE_TRADING,
            STATUS_EVENT_SERVICE_TRANSFERS, STATUS_EVENT_SERVICE_WITHDRAWALS,
        };
        use buffa::Enumeration;

        let event = status_event_from_proto(&ProtoStatusEvent {
            services: AffectedService::values()
                .iter()
                .filter(|service| **service != AffectedService::SERVICE_UNSPECIFIED)
                .map(|service| (*service).into())
                .collect(),
            ..Default::default()
        });
        assert_eq!(
            event.services,
            [
                STATUS_EVENT_SERVICE_TRADING,
                STATUS_EVENT_SERVICE_MARKET_DATA,
                STATUS_EVENT_SERVICE_DEPOSITS,
                STATUS_EVENT_SERVICE_WITHDRAWALS,
                STATUS_EVENT_SERVICE_TRANSFERS,
                STATUS_EVENT_SERVICE_ACCOUNTS,
                STATUS_EVENT_SERVICE_API,
                STATUS_EVENT_SERVICE_QUICKSWAP,
                STATUS_EVENT_SERVICE_APP,
            ]
        );
    }
}
