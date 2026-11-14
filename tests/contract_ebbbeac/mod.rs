//! Public contract ebbbeac (exchange release 0.93.2), through the public
//! services against a local server: account statement exports, asset
//! information and status events.

use super::hardening_support::{MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use futures_util::TryStreamExt;
use polyester::proto::ledger::read::v1::{ExportStatementRequest, ExportStatementResponse};
use polyester::proto::marketdata::v1::{
    AssetEditorialInfo, AssetInfo, ListAssetInfoRequest, ListAssetInfoResponse,
};
use polyester::proto::system::v1::{
    ListStatusEventsRequest, ListStatusEventsResponse, StatusEvent, StatusEventKind,
    StatusEventState,
};
use polyester::services::{ExportStatementOptions, ListStatusEventsOptions};
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

const EXPORT: &str = "/ledger.read.v1.LedgerReadService/ExportStatement";
const ASSET_INFO: &str = "/marketdata.v1.MarketDataService/ListAssetInfo";
const STATUS_EVENTS: &str = "/system.v1.SystemService/ListStatusEvents";
const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const SIGNED_HEADER: &str = "X-API-KEY-ID";

const PAGE_1: &str = "time,entry_id,type,account,asset,amount,fee,balance,link_id,flow_id\r\n\
    2026-10-02T14:03:11.000000Z,27419385013475392777,deposit,funding,USDT,100,0,100,,flow_8d1f2c\r\n";
const PAGE_2: &str =
    "2026-10-03T08:12:45.123456Z,27419385013475392811,trade,trading,USDT,-5000,2.5,4997.5,77,\r\n";

/// Each request a server saw: whether it was signed, and its decoded body.
type Seen<T> = Arc<Mutex<Vec<(bool, T)>>>;

async fn statement_server() -> (MockHttpServer, Seen<ExportStatementRequest>) {
    let seen: Seen<ExportStatementRequest> = Arc::default();
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, EXPORT);
        let req = ExportStatementRequest::decode_from_slice(&request.body).unwrap();
        let (csv, next) = if req.page_token.is_empty() {
            (PAGE_1, "s2")
        } else {
            (PAGE_2, "")
        };
        log.lock()
            .unwrap()
            .push((request.has_header(SIGNED_HEADER), req));
        connect_proto_ok(&ExportStatementResponse {
            csv: csv.into(),
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    (server, seen)
}

fn statement_options() -> ExportStatementOptions {
    ExportStatementOptions {
        start_time: Some(UNIX_EPOCH + Duration::new(1_790_812_800, 0)),
        end_time: Some(UNIX_EPOCH + Duration::new(1_793_491_200, 250_000_000)),
        asset_id: Some(2),
        ..Default::default()
    }
}

/// A statement export is a signed ledger read: the configured default
/// sub-account applies when the request names none, every page is sent with
/// the same fields, and the pages concatenate into one CSV whose rows read
/// by header name.
#[tokio::test]
async fn statement_export_is_signed_and_concatenates_its_pages() {
    let (server, seen) = statement_server().await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some("Bht3tfQZv8E".into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let default_sub = polyester::codecs::scalars::parse_account_id("Bht3tfQZv8E", "s").unwrap();

    let statement = client.statements.export(statement_options()).await.unwrap();
    assert_eq!(statement.csv, format!("{PAGE_1}{PAGE_2}"));
    let rows = statement.rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].flow_id, "flow_8d1f2c");
    assert_eq!(
        (rows[1].kind.as_str(), rows[1].fee.as_str()),
        ("trade", "2.5")
    );

    let requests = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(requests.len(), 2);
    for (index, (signed, req)) in requests.iter().enumerate() {
        assert!(signed, "page {index} was not signed");
        assert_eq!(req.subaccount_id, Some(default_sub), "page {index}");
        assert_eq!(req.asset_id, 2);
        let start = req.start_time.as_option().unwrap();
        let end = req.end_time.as_option().unwrap();
        assert_eq!((start.seconds, start.nanos), (1_790_812_800, 0));
        assert_eq!((end.seconds, end.nanos), (1_793_491_200, 250_000_000));
    }
    assert_eq!(requests[0].1.page_token, "");
    assert_eq!(requests[1].1.page_token, "s2");

    // One page; an explicit sub-account overrides the default.
    let page = client
        .statements
        .export_page(ExportStatementOptions {
            subaccount_id: Some(77),
            asset_id: None,
            ..statement_options()
        })
        .await
        .unwrap();
    assert_eq!(
        (page.csv.as_str(), page.next_page_token.as_str()),
        (PAGE_1, "s2")
    );
    let (_, req) = seen.lock().unwrap().pop().unwrap();
    assert_eq!((req.subaccount_id, req.asset_id), (Some(77), 0));
}

/// The period is required and a statement needs credentials: both are
/// refused before anything is sent.
#[tokio::test]
async fn statement_export_refuses_a_missing_period_or_key_without_sending() {
    let (server, seen) = statement_server().await;
    let signed = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    for opts in [
        ExportStatementOptions {
            start_time: None,
            ..statement_options()
        },
        ExportStatementOptions {
            end_time: None,
            ..statement_options()
        },
        ExportStatementOptions {
            subaccount_id: Some(0),
            ..statement_options()
        },
    ] {
        let err = signed.statements.export_page(opts).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
    }

    let anonymous = Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let err = anonymous
        .statements
        .export(statement_options())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Auth(_)), "{err:?}");
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
