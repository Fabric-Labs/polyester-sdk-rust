//! POLY-9228: catalog startup when a connection drops or stalls during a
//! catalog read.
//!
//! A local keep-alive HTTP/1.1 Connect API decides each request by a plan:
//! answer it, drop the connection without answering (what a proxy closing
//! its tunnel looks like to the client), or hold it until the client gives
//! up. Startup must retry a dropped catalog read on a fresh connection,
//! surface a stalled one within its deadline, and keep spot trading
//! independent of the funding (deposit/withdraw) config.

use super::hardening_support::RefusedPort;
use buffa::Message as _;
use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use polyester::proto::chain::zipper::v1::{
    AssetConfig as ZipperAsset, GetDepositWithdrawConfigResponse,
};
use polyester::proto::marketdata::v1::{AssetConfig, GetSpotConfigResponse, PairConfig};
use polyester::{Client, Config, Error, Price, Quantity};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub(crate) const SPOT: &str = "GetSpotConfig";
pub(crate) const FUNDING: &str = "GetDepositWithdrawConfig";
const CREATE_ORDER: &str = "CreateOrder";
const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Action {
    Answer,
    /// Close the connection without answering.
    Drop,
    /// Never answer.
    Stall,
    /// Answer 503 `unavailable`, with the server's failure headers.
    Unavailable,
}

pub(crate) type Plan = Arc<dyn Fn(&str, usize) -> Action + Send + Sync>;

pub(crate) struct Api {
    pub base_url: String,
    pub connections: Arc<AtomicUsize>,
    calls: Arc<Mutex<HashMap<String, usize>>>,
    ws_url: String,
    _refused_ws: RefusedPort,
}

impl Api {
    pub(crate) async fn spawn(plan: Plan) -> Self {
        Self::spawn_with(plan, Vec::new()).await
    }

    /// `failure_headers` are sent with every answer that is not a catalog.
    pub(crate) async fn spawn_with(plan: Plan, failure_headers: Vec<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base_url = format!("http://{}", listener.local_addr().expect("addr"));
        let connections = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(Mutex::new(HashMap::new()));
        let (accepted, seen) = (connections.clone(), calls.clone());
        let failure_headers = Arc::new(failure_headers);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(
                    stream,
                    plan.clone(),
                    seen.clone(),
                    failure_headers.clone(),
                ));
            }
        });
        let refused = RefusedPort::bind();
        let ws_url = format!("ws://{}/connection/websocket", refused.addr());
        Self {
            base_url,
            connections,
            calls,
            ws_url,
            _refused_ws: refused,
        }
    }

    pub(crate) fn count(&self, operation: &str) -> usize {
        *self.calls.lock().unwrap().get(operation).unwrap_or(&0)
    }

    pub(crate) fn config(&self, timeout: Duration) -> Config {
        Config {
            api_url: self.base_url.clone(),
            ws_url: self.ws_url.clone(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            default_account_id: Some("1".into()),
            timeout,
            hydrate_catalogs: true,
            allow_insecure_http: true,
            ..Default::default()
        }
    }
}

async fn serve(
    mut stream: TcpStream,
    plan: Plan,
    calls: Arc<Mutex<HashMap<String, usize>>>,
    failure_headers: Arc<Vec<(String, String)>>,
) {
    let mut buf = Vec::new();
    loop {
        let Some((path, consumed)) = read_request(&mut stream, &mut buf).await else {
            return;
        };
        buf.drain(..consumed);
        let operation = path.rsplit('/').next().unwrap_or_default().to_owned();
        let seen = {
            let mut calls = calls.lock().unwrap();
            let count = calls.entry(operation.clone()).or_insert(0);
            *count += 1;
            *count
        };
        let action = plan(&operation, seen);
        match action {
            Action::Drop => return,
            Action::Stall => {
                let mut sink = [0u8; 1024];
                while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
                return;
            }
            Action::Answer | Action::Unavailable => {}
        }
        let unavailable = action == Action::Unavailable;
        let (status, headers, body) = match operation.as_str() {
            _ if unavailable => (
                503,
                failure_headers.to_vec(),
                br#"{"code":"unavailable","message":"edge unavailable"}"#.to_vec(),
            ),
            SPOT => (200, Vec::new(), spot_config().encode_to_bytes().to_vec()),
            FUNDING => (200, Vec::new(), funding_config().encode_to_bytes().to_vec()),
            _ => (
                400,
                failure_headers.to_vec(),
                br#"{"code":"failed_precondition","message":"stub"}"#.to_vec(),
            ),
        };
        let content_type = if status == 200 {
            "application/proto"
        } else {
            "application/json"
        };
        let mut head = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (name, value) in &headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        if stream.write_all(head.as_bytes()).await.is_err()
            || stream.write_all(&body).await.is_err()
        {
            return;
        }
    }
}

/// Reads one request; returns its path and how many bytes it took.
async fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<(String, usize)> {
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            let total = end + 4 + length;
            while buf.len() < total {
                let mut chunk = [0u8; 4096];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return None,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let path = head.split_whitespace().nth(1)?.to_owned();
            return Some((path, total));
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

fn spot_config() -> GetSpotConfigResponse {
    let asset = |asset: &str, scale| AssetConfig {
        asset: asset.into(),
        quantity_scale: scale,
        market_data_volume_scale: scale,
        ..Default::default()
    };
    GetSpotConfigResponse {
        assets: vec![asset("BTC", 8), asset("USDT", 6)],
        pairs: vec![PairConfig {
            symbol_id: 1,
            symbol: "BTC-USDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            base_quantity_scale: 8,
            quote_quantity_scale: 6,
            reference_price_scale: 9,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn funding_config() -> GetDepositWithdrawConfigResponse {
    GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        assets: vec![ZipperAsset {
            asset: "USDT".into(),
            ledger_id: 2,
            quantity_scale: 6,
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) fn plan(f: impl Fn(&str, usize) -> Action + Send + Sync + 'static) -> Plan {
    Arc::new(f)
}

pub(crate) fn drop_once(target: &'static str) -> Plan {
    plan(move |op, seen| {
        if op == target && seen == 1 {
            Action::Drop
        } else {
            Action::Answer
        }
    })
}

pub(crate) fn stall(target: &'static str) -> Plan {
    plan(move |op, _| {
        if op == target {
            Action::Stall
        } else {
            Action::Answer
        }
    })
}

fn limit_buy() -> CreateOrderParams {
    let mut params = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
    params.quantity =
        Some(Quantity::from_decimal_str("0.001", 8, Some("BTC-USDT".into()), Some(1)).unwrap());
    params.price = Some(Price::from_decimal_str("100", Some("BTC-USDT".into())).unwrap());
    params.time_in_force = Some(CreateTimeInForce::Gtc);
    params
}

/// The order reaches the server: it was not refused for its catalogs.
pub(crate) async fn assert_spot_order_reaches_server(client: &Client, api: &Api) {
    let before = api.count(CREATE_ORDER);
    let err = tokio::time::timeout(Duration::from_secs(10), client.orders.create(limit_buy()))
        .await
        .expect("order call ended")
        .expect_err("the stub refuses every order");
    assert!(
        !matches!(err, Error::CatalogHydration(_)),
        "spot order refused for its catalogs: {err}"
    );
    assert_eq!(api.count(CREATE_ORDER), before + 1, "order not sent: {err}");
}

#[tokio::test]
async fn funding_read_dropped_once_recovers_and_spot_trading_starts() {
    let api = Api::spawn(drop_once(FUNDING)).await;
    let client = Client::new(api.config(Duration::from_secs(5))).expect("client");
    // No explicit wait: the order waits for the catalogs itself.
    assert_spot_order_reaches_server(&client, &api).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.catalogs.ledger_id_for_asset("USDT").is_none() {
        assert!(Instant::now() < deadline, "the funding config never loaded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(api.count(FUNDING), 2);
}

#[tokio::test]
async fn spot_read_dropped_once_recovers() {
    let api = Api::spawn(drop_once(SPOT)).await;
    let client = Client::new(api.config(Duration::from_secs(5))).expect("client");
    assert_spot_order_reaches_server(&client, &api).await;
    assert_eq!(api.count(SPOT), 2);
}

#[tokio::test]
async fn persistent_funding_stall_leaves_spot_trading_ready() {
    let api = Api::spawn(stall(FUNDING)).await;
    let started = Instant::now();
    let client = Client::new(api.config(Duration::from_millis(500))).expect("client");
    tokio::time::timeout(Duration::from_secs(5), client.wait_for_catalogs())
        .await
        .expect("wait ended")
        .expect("spot catalogs ready");
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "spot readiness waited {:?} for the funding config",
        started.elapsed()
    );
    assert_spot_order_reaches_server(&client, &api).await;
}

#[tokio::test]
async fn a_dropped_order_is_never_retried() {
    let api = Api::spawn(plan(|op, _| {
        if op == CREATE_ORDER {
            Action::Drop
        } else {
            Action::Answer
        }
    }))
    .await;
    let client = Client::new(api.config(Duration::from_secs(5))).expect("client");
    client.wait_for_catalogs().await.expect("catalogs");
    let err = client
        .orders
        .create(limit_buy())
        .await
        .expect_err("a dropped order");
    assert!(err.mutation_outcome_unknown(), "{err:?}");
    assert_eq!(api.count(CREATE_ORDER), 1);
}

fn catalog_load(err: &Error) -> &polyester::CatalogLoadFailure {
    match err {
        Error::CatalogLoad(failure) => failure,
        other => panic!("want Error::CatalogLoad, got {other:?}"),
    }
}

#[tokio::test]
async fn persistent_funding_stall_fails_within_the_deadline() {
    let api = Api::spawn(stall(FUNDING)).await;
    let mut config = api.config(Duration::from_millis(500));
    config.catalog_timeout = Some(Duration::from_millis(1500));
    let started = Instant::now();
    let client = Client::new(config).expect("client");
    client.wait_for_catalogs().await.expect("spot catalogs");
    let err = client
        .wait_for_funding_catalog()
        .await
        .expect_err("the funding config stalls");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the stall surfaced after {:?}",
        started.elapsed()
    );
    let failure = catalog_load(&err);
    assert_eq!(
        (failure.catalog, failure.operation, failure.failure),
        (
            "funding",
            "chain.zipper.v1.ZipperService/GetDepositWithdrawConfig",
            "timeout"
        )
    );
    assert!(failure.attempts >= 2, "{failure:?}");
    assert_eq!(failure.attempts as usize, api.count(FUNDING));
    // The spot read and each funding read had a connection of their own.
    assert!(
        api.connections.load(Ordering::SeqCst) > failure.attempts as usize,
        "retries reused the stalled connection"
    );
    let text = err.to_string();
    assert!(
        text.starts_with("catalog hydration failed: funding config (chain.zipper.v1.ZipperService/GetDepositWithdrawConfig) timed out after"),
        "{text}"
    );
    assert!(
        std::error::Error::source(&err).is_some(),
        "the cause is kept"
    );
    let refused = client
        .zipper
        .subscribe_zipped_asset_supply(true)
        .await
        .map(drop)
        .expect_err("the supply stream needs the funding config");
    assert!(matches!(refused, Error::CatalogHydration(_)), "{refused:?}");
    assert!(refused.to_string().contains("funding config"), "{refused}");
    assert_eq!(client.catalogs.ledger_id_for_asset("USDT"), None);
    // Trading still works once funding has failed.
    assert_spot_order_reaches_server(&client, &api).await;
}

#[tokio::test]
async fn persistent_spot_stall_fails_closed_within_the_deadline() {
    let api = Api::spawn(stall(SPOT)).await;
    let mut config = api.config(Duration::from_millis(500));
    config.catalog_timeout = Some(Duration::from_millis(1500));
    let started = Instant::now();
    let client = Client::new(config).expect("client");
    let err = client.wait_for_catalogs().await.expect_err("spot stalls");
    assert!(started.elapsed() < Duration::from_secs(2));
    let failure = catalog_load(&err);
    assert_eq!((failure.catalog, failure.failure), ("spot", "timeout"));
    let refused = client
        .orders
        .create(limit_buy())
        .await
        .expect_err("no spot config");
    assert!(matches!(refused, Error::CatalogHydration(_)), "{refused:?}");
    assert!(refused.to_string().contains("spot config"), "{refused}");
    assert_eq!(api.count(CREATE_ORDER), 0);
    client
        .wait_for_funding_catalog()
        .await
        .expect("the funding config loaded on its own");
}

#[tokio::test]
async fn persistently_dropped_funding_read_names_a_closed_connection() {
    let api = Api::spawn(plan(|op, _| {
        if op == FUNDING {
            Action::Drop
        } else {
            Action::Answer
        }
    }))
    .await;
    let client = Client::new(api.config(Duration::from_secs(5))).expect("client");
    let err = client
        .wait_for_funding_catalog()
        .await
        .expect_err("every funding read drops");
    let failure = catalog_load(&err);
    assert_eq!(
        (failure.failure, failure.attempts),
        ("connection_closed", 3)
    );
    assert_eq!(api.count(FUNDING), 3);
    assert!(
        err.to_string()
            .contains("connection closed after 3 attempts"),
        "{err}"
    );
    assert!(
        matches!(failure.source, Error::Server(_)),
        "{:?}",
        failure.source
    );
}

#[tokio::test]
async fn unavailable_funding_read_stops_after_three_attempts_with_its_reference() {
    let api = Api::spawn_with(
        plan(|op, _| {
            if op == FUNDING {
                Action::Unavailable
            } else {
                Action::Answer
            }
        }),
        vec![("cf-ray".into(), "a5711ea9d8262a9a-LAX".into())],
    )
    .await;
    let client = Client::new(api.config(Duration::from_secs(2))).expect("client");
    client.wait_for_catalogs().await.expect("spot catalogs");
    let err = client
        .wait_for_funding_catalog()
        .await
        .expect_err("the funding config is unavailable");
    let failure = catalog_load(&err);
    assert_eq!((failure.failure, failure.attempts), ("unavailable", 3));
    assert_eq!(api.count(FUNDING), 3);
    assert_eq!(
        failure.response_ref.as_deref(),
        Some("a5711ea9d8262a9a-LAX")
    );
    assert!(
        err.to_string()
            .contains("response ref a5711ea9d8262a9a-LAX"),
        "{err}"
    );
}

#[test]
fn catalog_timeout_must_be_positive() {
    let err = match Client::new(Config {
        catalog_timeout: Some(Duration::ZERO),
        hydrate_catalogs: false,
        ..Default::default()
    }) {
        Ok(_) => panic!("a zero catalog_timeout"),
        Err(err) => err,
    };
    assert!(
        matches!(&err, Error::Validation(msg) if msg.contains("catalog_timeout")),
        "{err}"
    );
}
