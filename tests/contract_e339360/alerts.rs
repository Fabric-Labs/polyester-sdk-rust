//! Price alerts (`alerts.v1.PriceAlertService`), on devnet only: request
//! encoding, decoding, paging and refusals.

use super::super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use futures_util::TryStreamExt;
use polyester::models::{
    AlertChannel, AlertDirection, AlertPriceSource, AlertStatus, CreateAlertParams,
    ListAlertsOptions, UpdateAlertParams,
};
use polyester::proto::alerts::v1::{
    AlertChannel as ProtoChannel, AlertDirection as ProtoDirection,
    AlertPriceSource as ProtoPriceSource, AlertStatus as ProtoStatus, CancelAlertRequest,
    CancelAlertResponse, CreateAlertRequest, CreateAlertResponse, ErrorCode, ErrorDetail,
    ListAlertsRequest, ListAlertsResponse, PriceAlert as ProtoAlert, UpdateAlertRequest,
    UpdateAlertResponse, UpdateOutcome,
};
use polyester::types::Price;
use polyester::{Client, Config, Error};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const CREATE: &str = "/alerts.v1.PriceAlertService/CreateAlert";
const LIST: &str = "/alerts.v1.PriceAlertService/ListAlerts";
const UPDATE: &str = "/alerts.v1.PriceAlertService/UpdateAlert";
const CANCEL: &str = "/alerts.v1.PriceAlertService/CancelAlert";

fn client(server: &MockHttpServer) -> Client {
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 5,
                "quote_quantity_scale": 6
            }]
        }))
        .unwrap();
    client
}

fn proto_alert() -> ProtoAlert {
    ProtoAlert {
        alert_id: 1234,
        symbol_id: 1,
        direction: ProtoDirection::BELOW.into(),
        price_source: ProtoPriceSource::INDEX_PRICE.into(),
        price_ticks: 90_000_000_000_000,
        reference_price_ticks: 95_000_500_000_000,
        armed: true,
        status: ProtoStatus::ACTIVE.into(),
        channels: vec![ProtoChannel::IN_APP.into(), ProtoChannel::TELEGRAM.into()],
        revision: 1,
        ..Default::default()
    }
}

fn connect_error(status: u16, code: &str, detail: ErrorCode) -> HttpScript {
    let detail = ErrorDetail {
        code: detail.into(),
        ..Default::default()
    };
    let body = json!({
        "code": code,
        "message": "refused",
        "details": [{
            "type": "alerts.v1.ErrorDetail",
            "value": base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_vec()),
        }],
    })
    .to_string();
    HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Content-Length".into(), body.len().to_string()),
        ],
        body: body.into_bytes(),
    }
}

fn price(decimal: &str) -> Price {
    Price::from_decimal_str(decimal, None).unwrap()
}

/// Create names the market by its catalog ID, sends the price in 1e9 ticks,
/// leaves an unset price source to the server and reads the alert back with
/// its market name, exact prices and lowercase labels.
#[tokio::test]
async fn create_alert_encodes_the_request_and_decodes_the_alert() {
    let seen = Arc::new(Mutex::new(None));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, CREATE);
        *seen_handler.lock().unwrap() =
            Some(CreateAlertRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&CreateAlertResponse {
            alert: proto_alert().into(),
            ..Default::default()
        })
    })
    .await;
    let alert = client(&server)
        .alerts
        .create(
            CreateAlertParams::new(
                "BTC-USDT",
                AlertDirection::Below,
                price("90000"),
                vec![AlertChannel::InApp, AlertChannel::Telegram],
            )
            .with_idempotency_key("run-7:tool-2"),
        )
        .await
        .unwrap();

    let req = seen.lock().unwrap().take().unwrap();
    assert_eq!(req.symbol_id, 1);
    assert_eq!(req.direction.as_known(), Some(ProtoDirection::BELOW));
    assert_eq!(req.price_ticks, 90_000_000_000_000);
    assert_eq!(
        req.price_source.as_known(),
        Some(ProtoPriceSource::PRICE_SOURCE_UNSPECIFIED)
    );
    assert_eq!(
        req.channels,
        [
            buffa::EnumValue::from(ProtoChannel::IN_APP),
            buffa::EnumValue::from(ProtoChannel::TELEGRAM)
        ]
    );
    assert_eq!(req.idempotency_key, "run-7:tool-2");

    assert_eq!(
        alert.alert_id,
        polyester::codecs::scalars::format_uint64_id(1234)
    );
    assert_eq!(alert.symbol_id, 1);
    assert_eq!(alert.symbol, "BTC-USDT");
    assert_eq!(alert.direction, "below");
    assert_eq!(alert.price_source, "index");
    assert_eq!(alert.price.as_ticks(), 90_000_000_000_000);
    assert_eq!(alert.reference_price.as_ticks(), 95_000_500_000_000);
    assert!(alert.armed);
    assert_eq!(alert.status, "active");
    assert_eq!(alert.channels, ["in_app", "telegram"]);
    assert_eq!(alert.revision, 1);
    assert_eq!(alert.triggered_price, None);
}

/// A chosen price source and a market given by ID are sent as given.
#[tokio::test]
async fn create_alert_by_symbol_id_with_last_price() {
    let seen = Arc::new(Mutex::new(None));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        *seen_handler.lock().unwrap() =
            Some(CreateAlertRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&CreateAlertResponse {
            alert: proto_alert().into(),
            ..Default::default()
        })
    })
    .await;
    client(&server)
        .alerts
        .create(
            CreateAlertParams::for_symbol_id(
                1,
                AlertDirection::Above,
                price("100000.5"),
                vec![AlertChannel::Telegram],
            )
            .with_price_source(AlertPriceSource::LastPrice),
        )
        .await
        .unwrap();
    let req = seen.lock().unwrap().take().unwrap();
    assert_eq!(req.symbol_id, 1);
    assert_eq!(req.direction.as_known(), Some(ProtoDirection::ABOVE));
    assert_eq!(req.price_ticks, 100_000_500_000_000);
    assert_eq!(
        req.price_source.as_known(),
        Some(ProtoPriceSource::LAST_PRICE)
    );
    assert!(req.idempotency_key.is_empty());
}

/// Malformed creates, updates and lists are refused before anything is sent.
#[tokio::test]
async fn alert_inputs_are_validated_before_sending() {
    let server = MockHttpServer::spawn(|_| HttpScript::NotFound).await;
    let client = client(&server);
    let create = |channels: Vec<AlertChannel>, decimal: &str| {
        CreateAlertParams::new("BTC-USDT", AlertDirection::Above, price(decimal), channels)
    };
    let refusals = [
        create(vec![], "1"),
        create(vec![AlertChannel::InApp, AlertChannel::InApp], "1"),
        create(
            vec![
                AlertChannel::InApp,
                AlertChannel::Telegram,
                AlertChannel::InApp,
            ],
            "1",
        ),
        create(vec![AlertChannel::InApp], "0"),
        create(vec![AlertChannel::InApp], "1").with_idempotency_key("bad key!"),
        create(vec![AlertChannel::InApp], "1").with_idempotency_key("x".repeat(129)),
        CreateAlertParams::new(
            "NOPE-USDT",
            AlertDirection::Above,
            price("1"),
            vec![AlertChannel::InApp],
        ),
        CreateAlertParams::new(
            "BTC-USDT",
            AlertDirection::Above,
            Price::from_decimal_str("1", Some("ETH-USDT".into())).unwrap(),
            vec![AlertChannel::InApp],
        ),
    ];
    for params in refusals {
        let err = client.alerts.create(params.clone()).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{params:?}: {err:?}");
    }
    for params in [
        UpdateAlertParams::new("1234", 0),
        UpdateAlertParams::new("", 1),
        UpdateAlertParams::new("1234", 1).with_price(price("0")),
        UpdateAlertParams::new("1234", 1).with_channels(vec![]),
    ] {
        let err = client.alerts.update(params.clone()).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{params:?}: {err:?}");
    }
    let mut too_many = ListAlertsOptions::default();
    too_many.limit = Some(201);
    let mut repeated = ListAlertsOptions::default();
    repeated.statuses = vec![AlertStatus::Active, AlertStatus::Active];
    for opts in [too_many, repeated] {
        let err = client.alerts.list(opts.clone()).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{opts:?}: {err:?}");
    }
    assert_eq!(server.requests.load(Ordering::SeqCst), 0);
}

/// The update mask names exactly the fields the caller set; with none it is
/// empty, which sets a fired alert again. The outcome is read back.
#[tokio::test]
async fn update_alert_builds_its_mask_from_the_fields_set() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, UPDATE);
        seen_handler
            .lock()
            .unwrap()
            .push(UpdateAlertRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&UpdateAlertResponse {
            alert: proto_alert().into(),
            outcome: UpdateOutcome::REPLAYED.into(),
            ..Default::default()
        })
    })
    .await;
    let client = client(&server);
    let result = client
        .alerts
        .update(
            UpdateAlertParams::new(polyester::codecs::scalars::format_uint64_id(1234), 3)
                .with_price(price("85000"))
                .with_idempotency_key("run-7:tool-3"),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome, "replayed");
    assert_eq!(
        result.alert.alert_id,
        polyester::codecs::scalars::format_uint64_id(1234)
    );
    client
        .alerts
        .update(UpdateAlertParams::new("1234", 4))
        .await
        .unwrap();
    client
        .alerts
        .update(
            UpdateAlertParams::new("1234", 5)
                .with_direction(AlertDirection::Above)
                .with_channels(vec![AlertChannel::Telegram]),
        )
        .await
        .unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].alert_id, 1234);
    assert_eq!(seen[0].expected_revision, 3);
    assert_eq!(seen[0].idempotency_key, "run-7:tool-3");
    assert_eq!(
        seen[0].update.as_option().unwrap().price_ticks,
        85_000_000_000_000
    );
    assert_eq!(
        seen[0].update_mask.as_option().unwrap().paths,
        ["price_ticks"]
    );
    // A decimal ID is read as the number it spells.
    assert_eq!(seen[1].alert_id, 1234);
    assert!(
        seen[1].update_mask.as_option().is_some(),
        "the mask is required"
    );
    assert!(seen[1].update_mask.as_option().unwrap().paths.is_empty());
    assert!(
        seen[1].update.as_option().is_some(),
        "the update is required"
    );
    assert_eq!(
        seen[2].update_mask.as_option().unwrap().paths,
        ["direction", "channels"]
    );
    let update = seen[2].update.as_option().unwrap();
    assert_eq!(update.direction.as_known(), Some(ProtoDirection::ABOVE));
    assert_eq!(
        update.channels,
        [buffa::EnumValue::from(ProtoChannel::TELEGRAM)]
    );
}

/// A fired alert reads its trigger time and price; a cancelled one its
/// contract spelling.
#[tokio::test]
async fn cancel_alert_and_read_a_triggered_alert() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| match request.path.as_str() {
        CANCEL => {
            seen_handler.lock().unwrap().push(
                CancelAlertRequest::decode_from_slice(&request.body)
                    .unwrap()
                    .alert_id,
            );
            let mut alert = proto_alert();
            alert.status = ProtoStatus::CANCELLED.into();
            connect_proto_ok(&CancelAlertResponse {
                alert: alert.into(),
                ..Default::default()
            })
        }
        LIST => {
            let mut alert = proto_alert();
            alert.status = ProtoStatus::TRIGGERED.into();
            alert.armed = false;
            alert.triggered_price_ticks = 89_999_000_000_000;
            alert.triggered_at = buffa_types::google::protobuf::Timestamp {
                seconds: 1_794_405_600,
                ..Default::default()
            }
            .into();
            alert.price_source = buffa::EnumValue::from(9);
            connect_proto_ok(&ListAlertsResponse {
                alerts: vec![alert],
                ..Default::default()
            })
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = client(&server);
    let cancelled = client.alerts.cancel("1234").await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert_eq!(*seen.lock().unwrap(), [1234]);

    let page = client
        .alerts
        .list(ListAlertsOptions::default())
        .await
        .unwrap();
    let fired = &page.alerts[0];
    assert_eq!(fired.status, "triggered");
    assert_eq!(
        fired.triggered_price.as_ref().unwrap().as_ticks(),
        89_999_000_000_000
    );
    assert_eq!(fired.triggered_at.as_ref().unwrap().seconds, 1_794_405_600);
    assert_eq!(fired.price_source, "unspecified");
    assert_eq!(page.next_page_token, "");
}

/// The list filters are sent by contract value and market ID; the stream
/// follows page tokens with the same filters.
#[tokio::test]
async fn list_alerts_filters_and_pages() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, LIST);
        let req = ListAlertsRequest::decode_from_slice(&request.body).unwrap();
        let token = req.page_token.clone();
        seen_handler.lock().unwrap().push(req);
        let alert = |id| ProtoAlert {
            alert_id: id,
            ..proto_alert()
        };
        let (alerts, next) = match token.as_str() {
            "" => (vec![alert(3), alert(2)], "p2"),
            "p2" => (vec![alert(1)], ""),
            other => panic!("unexpected page token {other}"),
        };
        connect_proto_ok(&ListAlertsResponse {
            alerts,
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    let client = client(&server);
    let mut opts = ListAlertsOptions::default();
    opts.statuses = vec![AlertStatus::Active, AlertStatus::Triggered];
    opts.symbol = Some("BTC-USDT".into());
    opts.limit = Some(2);
    let all: Vec<_> = client.alerts.all(opts).try_collect().await.unwrap();
    let ids: Vec<_> = all.iter().map(|alert| alert.alert_id.clone()).collect();
    let format = polyester::codecs::scalars::format_uint64_id;
    assert_eq!(ids, [format(3), format(2), format(1)]);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for req in seen.iter() {
        assert_eq!(req.symbol_id, 1);
        assert_eq!(req.limit, 2);
        assert_eq!(
            req.statuses,
            [
                buffa::EnumValue::from(ProtoStatus::ACTIVE),
                buffa::EnumValue::from(ProtoStatus::TRIGGERED)
            ]
        );
    }
    assert_eq!(seen[1].page_token, "p2");
}

/// A sub-account key is refused as a permission denial; a missing price
/// changed nothing and is retryable; an unavailable service leaves a
/// mutation's outcome unknown, as for other services; the other refusals keep
/// their contract code.
#[tokio::test]
async fn alert_refusals_map_to_sdk_errors() {
    let cases = [
        (403, "permission_denied", ErrorCode::SUBACCOUNT_KEY_REFUSED),
        (503, "unavailable", ErrorCode::PRICE_UNAVAILABLE),
        (503, "unavailable", ErrorCode::SERVICE_UNAVAILABLE),
        (400, "failed_precondition", ErrorCode::REVISION_CONFLICT),
        (409, "already_exists", ErrorCode::IDEMPOTENCY_CONFLICT),
        (
            400,
            "failed_precondition",
            ErrorCode::ALERT_UPDATE_LIMIT_REACHED,
        ),
        (404, "not_found", ErrorCode::ALERT_NOT_FOUND),
    ];
    for (status, code, detail) in cases {
        let server = MockHttpServer::spawn(move |_| connect_error(status, code, detail)).await;
        let err = client(&server)
            .alerts
            .update(UpdateAlertParams::new("1234", 1))
            .await
            .unwrap_err();
        match detail {
            ErrorCode::SUBACCOUNT_KEY_REFUSED => {
                assert!(
                    matches!(&err, Error::PermissionDenied { code, .. } if code == "SUBACCOUNT_KEY_REFUSED"),
                    "{err:?}"
                );
                assert!(!err.is_retryable());
            }
            ErrorCode::PRICE_UNAVAILABLE => {
                assert!(
                    matches!(&err, Error::Api { code, .. } if code == "PRICE_UNAVAILABLE"),
                    "{err:?}"
                );
                assert!(err.is_retryable(), "{detail:?}: {err:?}");
                assert!(!err.mutation_outcome_unknown(), "{detail:?}: {err:?}");
            }
            ErrorCode::SERVICE_UNAVAILABLE => {
                assert!(
                    matches!(&err, Error::Api { code, .. } if code == "SERVICE_UNAVAILABLE"),
                    "{err:?}"
                );
                assert!(!err.is_retryable(), "{detail:?}: {err:?}");
                assert!(err.mutation_outcome_unknown(), "{detail:?}: {err:?}");
            }
            _ => {
                let name = buffa::Enumeration::proto_name(&detail);
                assert!(
                    matches!(&err, Error::Api { code, .. } if code == name),
                    "{detail:?}: {err:?}"
                );
                assert!(!err.is_retryable() && !err.mutation_outcome_unknown());
            }
        }
    }
}
