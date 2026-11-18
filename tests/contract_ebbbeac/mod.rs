//! Public contract ebbbeac (exchange release 0.93.2), through the public
//! services against a local server: account statement exports, asset
//! information and status events. The statement export calls follow the
//! 0.93.5 contract, which removed `LedgerReadService.ExportStatement`.

use super::hardening_support::{MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use futures_util::TryStreamExt;
use polyester::proto::ledger::read::v1::{
    CancelStatementExportRequest, CancelStatementExportResponse,
    GetStatementExportDownloadUrlRequest, GetStatementExportDownloadUrlResponse, StatementExport,
    StatementExportStatus,
};
use polyester::proto::marketdata::v1::{
    AssetEditorialInfo, AssetInfo, ListAssetInfoRequest, ListAssetInfoResponse,
};
use polyester::proto::system::v1::{
    ListStatusEventsRequest, ListStatusEventsResponse, StatusEvent, StatusEventKind,
    StatusEventState,
};
use polyester::services::ListStatusEventsOptions;
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};

const DOWNLOAD_URL: &str = "/ledger.read.v1.StatementExportService/GetStatementExportDownloadUrl";
const CANCEL: &str = "/ledger.read.v1.StatementExportService/CancelStatementExport";
const ASSET_INFO: &str = "/marketdata.v1.MarketDataService/ListAssetInfo";
const STATUS_EVENTS: &str = "/system.v1.SystemService/ListStatusEvents";
const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const SIGNED_HEADER: &str = "X-API-KEY-ID";
/// A stand-in for the signed URL the API returns; it must never be printed.
const FILE_URL: &str = "https://files.example/statement-export?signature=fake-signature-marker";

/// Each request a server saw: whether it was signed, and its decoded body.
type Seen<T> = Arc<Mutex<Vec<(bool, T)>>>;

/// Each statement export request a server saw: its path, whether it was
/// signed, and the export ID it named.
type SeenExports = Arc<Mutex<Vec<(String, bool, u64)>>>;

async fn statement_export_server() -> (MockHttpServer, SeenExports) {
    let seen: SeenExports = Arc::default();
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        let signed = request.has_header(SIGNED_HEADER);
        match request.path.as_str() {
            DOWNLOAD_URL => {
                let req =
                    GetStatementExportDownloadUrlRequest::decode_from_slice(&request.body).unwrap();
                log.lock()
                    .unwrap()
                    .push((request.path.clone(), signed, req.export_id));
                connect_proto_ok(&GetStatementExportDownloadUrlResponse {
                    url: FILE_URL.into(),
                    expires_at: buffa_types::google::protobuf::Timestamp {
                        seconds: 1_763_337_660,
                        ..Default::default()
                    }
                    .into(),
                    filename: format!("statement-{}.csv", req.export_id),
                    ..Default::default()
                })
            }
            CANCEL => {
                let req = CancelStatementExportRequest::decode_from_slice(&request.body).unwrap();
                log.lock()
                    .unwrap()
                    .push((request.path.clone(), signed, req.export_id));
                connect_proto_ok(&CancelStatementExportResponse {
                    export: StatementExport {
                        export_id: req.export_id,
                        status: StatementExportStatus::STATEMENT_EXPORT_STATUS_CANCELED.into(),
                        idempotency_key: "statement-2026".into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                })
            }
            other => panic!("unexpected request {other}"),
        }
    })
    .await;
    (server, seen)
}

/// The download URL and the cancel are signed statement reads of one export
/// named by its base58 ID. The URL is returned as sent, but the result's
/// `Debug` never prints it; the canceled export reads `canceled`.
#[tokio::test]
async fn statement_export_download_url_and_cancel_are_signed() {
    let (server, seen) = statement_export_server().await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let export_id = polyester::codecs::scalars::format_uint64_id(7);
    let id = polyester::codecs::scalars::parse_account_id(&export_id, "export_id").unwrap();

    let url = client
        .statements
        .get_export_download_url(&export_id)
        .await
        .unwrap();
    assert_eq!(url.url, FILE_URL);
    assert_eq!(url.filename, format!("statement-{id}.csv"));
    assert_eq!(url.expires_at.seconds, 1_763_337_660);
    let rendered = format!("{url:?}");
    assert!(!rendered.contains("fake-signature-marker"), "{rendered}");
    assert!(!rendered.contains("files.example"), "{rendered}");

    let canceled = client.statements.cancel_export(&export_id).await.unwrap();
    assert_eq!(
        (canceled.export_id.as_str(), canceled.status.as_str()),
        (export_id.as_str(), "canceled")
    );

    let requests = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(
        requests,
        [
            (DOWNLOAD_URL.to_owned(), true, id),
            (CANCEL.to_owned(), true, id)
        ]
    );
}

/// An export ID that is not a base58 public ID, and a call without an API
/// key, are refused before anything is sent.
#[tokio::test]
async fn statement_export_download_url_and_cancel_refuse_before_sending() {
    let (server, seen) = statement_export_server().await;
    let signed = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    for export_id in ["", " ", "0", "1", "100"] {
        let err = signed
            .statements
            .get_export_download_url(export_id)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "{export_id:?}: {err:?}"
        );
        let err = signed
            .statements
            .cancel_export(export_id)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Validation(_)),
            "{export_id:?}: {err:?}"
        );
    }

    let anonymous = Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let export_id = polyester::codecs::scalars::format_uint64_id(7);
    let err = anonymous
        .statements
        .get_export_download_url(&export_id)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Auth { .. }), "{err:?}");
    let err = anonymous
        .statements
        .cancel_export(&export_id)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Auth { .. }), "{err:?}");
    assert!(seen.lock().unwrap().is_empty());
}

/// Asset information is public: read without credentials and never signed,
/// with the requested IDs sent as given.
#[tokio::test]
async fn asset_info_is_public_and_unsigned() {
    let seen: Seen<ListAssetInfoRequest> = Arc::default();
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, ASSET_INFO);
        let req = ListAssetInfoRequest::decode_from_slice(&request.body).unwrap();
        log.lock()
            .unwrap()
            .push((request.has_header(SIGNED_HEADER), req));
        connect_proto_ok(&ListAssetInfoResponse {
            asset_info: vec![AssetInfo {
                asset_id: 2,
                editorial: AssetEditorialInfo {
                    website_url: "https://bitcoin.org".into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    })
    .await;
    for key in [false, true] {
        let client = Client::new(Config {
            api_url: server.base_url(),
            api_key_id: key.then(|| "ak_test".into()),
            api_private_key: key.then(|| TEST_KEY.into()),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let infos = client.market_data.list_asset_info(&[2, 1]).await.unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].asset_id, 2);
        assert_eq!(infos[0].editorial.website_url, "https://bitcoin.org");
        assert_eq!(infos[0].editorial.issue_price, None);
    }
    let all = Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    all.market_data.list_asset_info(&[]).await.unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|(signed, _)| !signed));
    assert_eq!(requests[0].1.asset_ids, [2, 1]);
    assert!(requests[2].1.asset_ids.is_empty());
}

/// Status events are public and never signed; the paging walk sends the
/// same states and kinds with each page token.
#[tokio::test]
async fn status_events_are_public_and_resend_their_filters_with_each_token() {
    let seen: Seen<ListStatusEventsRequest> = Arc::default();
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, STATUS_EVENTS);
        let req = ListStatusEventsRequest::decode_from_slice(&request.body).unwrap();
        let (id, next) = if req.page_token.is_empty() {
            (2, "e2")
        } else {
            (1, "")
        };
        log.lock()
            .unwrap()
            .push((request.has_header(SIGNED_HEADER), req));
        connect_proto_ok(&ListStatusEventsResponse {
            events: vec![StatusEvent {
                id,
                kind: StatusEventKind::MAINTENANCE.into(),
                state: StatusEventState::SCHEDULED.into(),
                ..Default::default()
            }],
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let opts = ListStatusEventsOptions {
        states: vec!["scheduled".into(), "in_progress".into()],
        kinds: vec!["maintenance".into()],
        limit: Some(1),
        page_token: None,
    };

    let page = client.list_status_events(opts.clone()).await.unwrap();
    assert_eq!(page.next_page_token, "e2");
    assert_eq!(page.events[0].kind, "maintenance");
    assert_eq!(page.events[0].state, "scheduled");

    let all: Vec<_> = client.status_events(opts).try_collect().await.unwrap();
    let ids: Vec<_> = all.iter().map(|event| event.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            polyester::codecs::scalars::format_id(2),
            polyester::codecs::scalars::format_id(1)
        ]
    );

    let requests = seen.lock().unwrap();
    let tokens: Vec<_> = requests
        .iter()
        .map(|(_, r)| r.page_token.as_str())
        .collect();
    assert_eq!(tokens, ["", "", "e2"]);
    for (signed, req) in requests.iter() {
        assert!(!signed);
        assert_eq!(
            req.states,
            vec![
                buffa::EnumValue::from(StatusEventState::SCHEDULED),
                buffa::EnumValue::from(StatusEventState::IN_PROGRESS)
            ]
        );
        assert_eq!(
            req.kinds,
            vec![buffa::EnumValue::from(StatusEventKind::MAINTENANCE)]
        );
        assert_eq!(req.limit, 1);
    }
}
