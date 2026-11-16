//! The contract's time-window filters on `orders.list_history_with` and
//! `trades.list_with`, against a local server in binary and JSON: each bound
//! reaches the request with its presence kept (unset stays unset, an explicit
//! 0 is sent), and a start after a non-zero end is refused before sending.

use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::models::{ListOrderHistoryOpts, ListUserTradesOpts};
use polyester::proto::orders::v1::{
    GetOrderHistoryRequest, GetOrderHistoryResponse, GetUserTradesRequest, GetUserTradesResponse,
};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const HISTORY: &str = "/orders.v1.OrdersReadService/GetOrderHistory";
const TRADES: &str = "/orders.v1.OrdersReadService/GetUserTrades";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];

/// The bounds a caller passes, as `(start_ts_ns, end_ts_ns)`, each sent as
/// given.
const SENT: [(Option<u64>, Option<u64>); 8] = [
    (None, None),
    (Some(1_700_000_000_000_000_000), None),
    (None, Some(1_700_000_003_600_000_000)),
    (
        Some(1_700_000_000_000_000_000),
        Some(1_700_000_003_600_000_000),
    ),
    (
        Some(1_700_000_000_000_000_000),
        Some(1_700_000_000_000_000_000),
    ),
    (Some(0), None),
    (None, Some(0)),
    // An explicit end of 0 is the contract's empty page, whatever the start.
    (Some(1_700_000_000_000_000_000), Some(0)),
];

/// A start after a non-zero end: the contract's ordered-range refusal.
const REFUSED: [(u64, u64); 3] = [
    (1_700_000_003_600_000_001, 1_700_000_003_600_000_000),
    (2, 1),
    (u64::MAX, 1),
];

fn answer<M: buffa::Message + serde::Serialize>(wire: WireFormat, msg: &M) -> HttpScript {
    let (content_type, body) = match wire {
        WireFormat::Binary => ("application/proto", msg.encode_to_vec()),
        WireFormat::Json => ("application/json", serde_json::to_vec(msg).unwrap()),
    };
    HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), content_type.into())],
        body,
    }
}

fn request<M: buffa::Message + serde::de::DeserializeOwned + Default>(
    wire: WireFormat,
    body: &[u8],
) -> M {
    match wire {
        WireFormat::Binary => M::decode_from_slice(body).expect("binary request"),
        WireFormat::Json => serde_json::from_slice(body).expect("JSON request"),
    }
}

struct Server {
    client: Client,
    http: MockHttpServer,
    history: Arc<Mutex<Vec<GetOrderHistoryRequest>>>,
    trades: Arc<Mutex<Vec<GetUserTradesRequest>>>,
}

async fn server(wire: WireFormat) -> Server {
    let history = Arc::new(Mutex::new(Vec::new()));
    let trades = Arc::new(Mutex::new(Vec::new()));
    let (h, t) = (history.clone(), trades.clone());
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        HISTORY => {
            h.lock().unwrap().push(request(wire, &req.body));
            answer(wire, &GetOrderHistoryResponse::default())
        }
        TRADES => {
            t.lock().unwrap().push(request(wire, &req.body));
            answer(wire, &GetUserTradesResponse::default())
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: wire,
        ..Default::default()
    })
    .expect("client");
    Server {
        client,
        http,
        history,
        trades,
    }
}

#[tokio::test]
async fn order_history_sends_the_time_window_as_given() {
    for wire in WIRES {
        let s = server(wire).await;
        for (start_ts_ns, end_ts_ns) in SENT {
            s.client
                .orders
                .list_history_with(ListOrderHistoryOpts {
                    start_ts_ns,
                    end_ts_ns,
                    ..Default::default()
                })
                .await
                .unwrap();
            let sent = s.history.lock().unwrap().last().cloned().unwrap();
            assert_eq!(
                (sent.start_ts_ns, sent.end_ts_ns),
                (start_ts_ns, end_ts_ns),
                "{wire:?}"
            );
        }
    }
}

#[tokio::test]
async fn user_trades_send_the_time_window_as_given() {
    for wire in WIRES {
        let s = server(wire).await;
        for (start_ts_ns, end_ts_ns) in SENT {
            s.client
                .trades
                .list_with(ListUserTradesOpts {
                    start_ts_ns,
                    end_ts_ns,
                    ..Default::default()
                })
                .await
                .unwrap();
            let sent = s.trades.lock().unwrap().last().cloned().unwrap();
            assert_eq!(
                (sent.start_ts_ns, sent.end_ts_ns),
                (start_ts_ns, end_ts_ns),
                "{wire:?}"
            );
        }
    }
}

fn assert_range_refusal(err: &Error, method: &str) {
    assert!(matches!(err, Error::Validation(_)), "{method}: {err}");
    let text = err.to_string();
    assert!(
        text.contains(method) && text.contains("start_ts_ns") && text.contains("end_ts_ns"),
        "{text}"
    );
}

#[tokio::test]
async fn a_start_after_a_non_zero_end_is_refused_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        for (start, end) in REFUSED {
            let err = s
                .client
                .orders
                .list_history_with(ListOrderHistoryOpts {
                    start_ts_ns: Some(start),
                    end_ts_ns: Some(end),
                    ..Default::default()
                })
                .await
                .expect_err("unordered history range");
            assert_range_refusal(&err, "orders.list_history");
            let err = s
                .client
                .trades
                .list_with(ListUserTradesOpts {
                    start_ts_ns: Some(start),
                    end_ts_ns: Some(end),
                    ..Default::default()
                })
                .await
                .expect_err("unordered trades range");
            assert_range_refusal(&err, "trades.list");
        }
        assert_eq!(
            s.http.requests.load(Ordering::SeqCst),
            0,
            "{wire:?}: a refused range must send nothing"
        );
    }
}
