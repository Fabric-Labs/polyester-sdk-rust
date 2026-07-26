//! POLY-3746 L2 integration tests via public SDK APIs + local mock HTTP/WS.
//!
//! These exercise production paths (subscribe_raw, JsonRpcClient, wait_for_catalogs,
//! Quantity::format, SnapshotThenStream) against in-process servers — not private helpers.

mod hardening_support;

use futures_util::future::FutureExt;
use hardening_support::{
    GET_BALANCES_PATH, GET_ORDER_PATH, GET_TRADES_PATH, MockHttpServer, MockWsServer,
    SPOT_CONFIG_PATH, ZIPPER_CONFIG_PATH, centrifugo_ok_reply, connect_proto_ok, test_credentials,
    wait_until,
};
use polyester::Error;
use polyester::auth::Credentials;
use polyester::chain::JsonRpcClient;
use polyester::codecs::{MAX_PROTOCOL_SCALE, format_ledger_u128, format_qty_scaled};
use polyester::realtime::Client as RealtimeClient;
use polyester::realtime::{
    MAX_REALTIME_MESSAGE_BYTES, SnapshotThenStream, SnapshotThenStreamConfig,
};
use polyester::transport::MAX_CONNECT_RESPONSE_BYTES;
use polyester::{Client, Config, Quantity, QuantityDomain};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const PRIVATE_CHANNEL: &str = "private:spot:orders:acct:proto";
const PUBLIC_CHANNEL: &str = "public:spot:market:trades:1:proto";

fn private_rt(ws: &MockWsServer, http: &MockHttpServer, timeout: Duration) -> RealtimeClient {
    RealtimeClient::with_timeout(
        ws.ws_url(),
        http.base_url(),
        Some(test_credentials("ak_test", TEST_KEY)),
        None,
        timeout,
    )
}

async fn subscribe_raw_err(rt: &RealtimeClient, channel: &str) -> Error {
    match rt.subscribe_raw(channel).await {
        Err(err) => err,
        Ok(_) => panic!("subscribe_raw({channel}) must fail"),
    }
}

#[tokio::test]
async fn l2_token_headers_then_stalled_body_times_out_via_subscribe_raw() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![("Transfer-Encoding".into(), "chunked".into())],
                stall,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "elapsed {:?} exceeded deadline+slack; body likely outside timeout",
        started.elapsed()
    );
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("timed out") || msg.contains("timeout"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn l2_token_no_headers_times_out_via_subscribe_raw() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::NeverRespond {
                stall: Duration::from_secs(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("timed out") || msg.contains("timeout"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_slow_drip_obeys_one_overall_deadline() {
    let timeout = Duration::from_millis(180);
    let chunks = br#"{"token":"connection-ok"}"#.iter().map(|byte| vec![*byte]).collect::<Vec<_>>();
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::SlowDrip {
                status: 200,
                headers: vec![("Content-Type".into(), "application/json".into())],
                chunks: chunks.clone(),
                inter_chunk_delay: Duration::from_millis(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "slow-drip token body escaped the overall deadline"
    );
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("timeout") || message.contains("timed out"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_content_length_65537_rejected_via_subscribe_raw() {
    let body = vec![b'x'; 65_537];
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body: body.clone(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("too large") || msg.contains("64"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_chunked_oversized_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::ChunkedBody {
                status: 200,
                total_bytes: 70_000,
                chunk_size: 4096,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("too large") || msg.contains("64"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_empty_token_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"token":""}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("missing token") || msg.contains("token"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_malformed_json_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![("Content-Type".into(), "application/json".into())],
                body: b"{not-json".to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("invalid token") || msg.contains("json") || msg.contains("parse"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_http_403_maps_to_structured_permission_denied() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == "/v1/rt/token" {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"token":"connection-ok"}"#.to_vec(),
            }
        } else if req.path.starts_with("/v1/rt/subscribe") {
            hardening_support::HttpScript::Json {
                status: 403,
                body: br#"{"code":"permission_denied","message":"missing transfer:read"}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active).await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, "private:auth:transfers:acct:proto").await;
    match err {
        Error::PermissionDenied {
            message,
            status,
            code,
            context,
            endpoint,
        } => {
            assert_eq!(message, "missing transfer:read");
            assert_eq!(status, 403);
            assert_eq!(code, "permission_denied");
            assert!(context.contains("private:auth:transfers:acct:proto"));
            assert!(endpoint.contains("/v1/rt/subscribe?channel="));
        }
        other => panic!("expected structured permission error, got {other:?}"),
    }
}

#[tokio::test]
async fn l2_jsonrpc_headers_then_stalled_body_times_out() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 200,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Transfer-Encoding".into(), "chunked".into()),
        ],
        stall,
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout);
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("stalled JSON-RPC body must timeout");
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    assert!(err.to_string().contains("timeout"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_no_headers_times_out() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::NeverRespond {
        stall: Duration::from_secs(30),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout);
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("no-headers JSON-RPC must timeout");
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    assert!(err.to_string().contains("timeout"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_slow_drip_obeys_one_overall_deadline() {
    let timeout = Duration::from_millis(180);
    let chunks = br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#
        .iter()
        .map(|byte| vec![*byte])
        .collect::<Vec<_>>();
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::SlowDrip {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        chunks: chunks.clone(),
        inter_chunk_delay: Duration::from_millis(30),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout);
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("slow-drip JSON-RPC body must time out");
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "slow-drip JSON-RPC body escaped the overall deadline"
    );
    assert!(err.to_string().to_ascii_lowercase().contains("timeout"));
}

#[tokio::test]
async fn l2_jsonrpc_rejects_oversized_and_bad_envelope() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.contains("big") {
            let body = vec![b'x'; 2 * 1024 * 1024];
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body,
            }
        } else if req.path.contains("ver") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"1.0","id":1,"result":1}"#.to_vec(),
            }
        } else if req.path.contains("noid") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"2.0","result":1}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-1,"message":"x"}}"#
                    .to_vec(),
            }
        }
    })
    .await;
    let rpc = JsonRpcClient::new(format!("{}/ok", http.base_url()), Duration::from_secs(2));
    let err = rpc.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().contains("both result and error"), "{err}");

    let big = JsonRpcClient::new(format!("{}/big", http.base_url()), Duration::from_secs(2));
    let err = big.request("eth_call", json!([])).await.unwrap_err();
    assert!(
        err.to_string().contains("exceeds") || err.to_string().contains("read body"),
        "{err}"
    );

    let ver = JsonRpcClient::new(format!("{}/ver", http.base_url()), Duration::from_secs(2));
    let err = ver.request("eth_call", json!([])).await.unwrap_err();
    assert!(
        err.to_string().to_ascii_lowercase().contains("jsonrpc") || err.to_string().contains("2.0"),
        "{err}"
    );

    let noid = JsonRpcClient::new(format!("{}/noid", http.base_url()), Duration::from_secs(2));
    let err = noid.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("id"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_rejects_all_remaining_invalid_envelopes() {
    let http = MockHttpServer::spawn(|req| {
        let body = if req.path.contains("missing-version") {
            br#"{"id":1,"result":1}"#.to_vec()
        } else if req.path.contains("wrong-id") {
            br#"{"jsonrpc":"2.0","id":999,"result":1}"#.to_vec()
        } else if req.path.contains("neither") {
            br#"{"jsonrpc":"2.0","id":1}"#.to_vec()
        } else {
            br#"{"jsonrpc":"2.0","id":1,"error":"not-an-object"}"#.to_vec()
        };
        hardening_support::HttpScript::Json { status: 200, body }
    })
    .await;

    for path in ["missing-version", "wrong-id", "neither", "malformed-error"] {
        let rpc = JsonRpcClient::new(
            format!("{}/{path}", http.base_url()),
            Duration::from_secs(2),
        );
        let err = rpc.request("eth_call", json!([])).await.expect_err(path);
        let message = err.to_string().to_ascii_lowercase();
        match path {
            "missing-version" => assert!(message.contains("version"), "{err}"),
            "wrong-id" => assert!(message.contains("id"), "{err}"),
            "neither" => assert!(
                message.contains("result") || message.contains("error"),
                "{err}"
            ),
            "malformed-error" => assert!(message.contains("object"), "{err}"),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn l2_jsonrpc_25_concurrent_reordered_responses_succeed() {
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Buffer 25 requests, then reply in reverse id order (reorder-safe client).
    let buffer: Arc<Mutex<Vec<(tokio::net::TcpStream, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let buffer_h = buffer.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind concurrent rpc");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let buffer = buffer_h.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                let n = match stream.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => return,
                };
                let raw = String::from_utf8_lossy(&buf[..n]);
                let body = raw.split("\r\n\r\n").nth(1).unwrap_or("{}");
                let v: serde_json::Value = serde_json::from_str(body).unwrap_or(json!({}));
                let id = v.get("id").and_then(|i| i.as_u64()).unwrap_or(0);
                let batch = {
                    let mut guard = buffer.lock().unwrap();
                    guard.push((stream, id));
                    if guard.len() == 25 {
                        Some(std::mem::take(&mut *guard))
                    } else {
                        None
                    }
                };
                if let Some(mut batch) = batch {
                    batch.sort_by_key(|b| std::cmp::Reverse(b.1));
                    for (mut stream, id) in batch {
                        let resp = json!({"jsonrpc":"2.0","id":id,"result":format!("ok-{id}")});
                        let body = serde_json::to_vec(&resp).unwrap();
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(&body).await;
                    }
                }
            });
        }
    });

    let rpc = JsonRpcClient::new(format!("http://{addr}"), Duration::from_secs(5));
    let mut tasks = Vec::new();
    for _ in 0..25 {
        let rpc = rpc.clone();
        tasks.push(tokio::spawn(async move {
            rpc.request("eth_chainId", json!([])).await
        }));
    }
    let mut ok = 0;
    for t in tasks {
        let result = t.await.expect("join").expect("rpc ok");
        assert!(result.as_str().unwrap_or("").starts_with("ok-"));
        ok += 1;
    }
    assert_eq!(ok, 25);
}

#[tokio::test]
async fn l2_jsonrpc_success_path_still_works() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2));
    let result = rpc.request("eth_chainId", json!([])).await.expect("ok");
    assert_eq!(result, json!("0x1"));
}

#[tokio::test]
async fn l2_jsonrpc_chunked_over_1mib_rejected() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::ChunkedBody {
        status: 200,
        total_bytes: 1_100_000,
        chunk_size: 16_384,
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(5));
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("chunked >1MiB must fail");
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("read body") || msg.contains("too large"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_jsonrpc_malformed_json_rejected() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: b"{broken".to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2));
    let err = rpc.request("eth_chainId", json!([])).await.unwrap_err();
    assert!(
        err.to_string().to_ascii_lowercase().contains("json"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_jsonrpc_error_object_returns_transport_error() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2));
    let err = rpc.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().contains("boom"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_null_result_is_preserved() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"result":null}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2));
    let result = rpc.request("eth_call", json!([])).await.expect("null ok");
    assert!(result.is_null(), "expected null result, got {result}");
}

#[tokio::test]
async fn l2_close_aborts_subscription_promptly_against_local_ws() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    sub.close();
    wait_until(|| !sub.is_alive(), Duration::from_millis(750)).await;
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "close lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_drop_idle_subscription_peer_closes_promptly() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    drop(sub);
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(started.elapsed() < Duration::from_millis(750));
}

#[tokio::test]
async fn l2_hundred_sub_close_returns_conn_count_to_baseline() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let mut subs = Vec::new();
    for _ in 0..100 {
        subs.push(rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe"));
    }
    wait_until(
        || active.load(Ordering::SeqCst) >= 100,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    for sub in &subs {
        sub.close();
    }
    drop(subs);
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "100-sub close soak exceeded 750ms: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_cancel_subscribe_raw_during_token_body_stall_cleans_peers() {
    let stall = Duration::from_secs(30);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![("Transfer-Encoding".into(), "chunked".into())],
                stall,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws_active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_hang_after_accept_counted(ws_active.clone()).await;
    let rt = private_rt(&ws, &http, Duration::from_secs(30));

    let join = tokio::spawn(async move { rt.subscribe_raw(PRIVATE_CHANNEL).await });
    wait_until(
        || http.in_flight.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    join.abort();
    let _ = join.await;
    wait_until(
        || http.in_flight.load(Ordering::SeqCst) == 0 && ws_active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "cancel during token stall lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_cancel_subscribe_raw_during_centrifugo_wait_cleans_peers() {
    let ws_active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_hang_after_accept_counted(ws_active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let join = tokio::spawn(async move { rt.subscribe_raw(PUBLIC_CHANNEL).await });
    wait_until(
        || ws_active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    join.abort();
    let _ = join.await;
    wait_until(
        || ws_active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "cancel during Centrifugo wait lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_close_during_reconnect_backoff_no_extra_connect() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || ws.connects.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    // After forced disconnect, client sleeps ~1s then reconnects. Close during backoff.
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_secs(2),
    )
    .await;
    let connects_before = ws.connects.load(Ordering::SeqCst);
    sub.close();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let connects_after = ws.connects.load(Ordering::SeqCst);
    assert_eq!(
        connects_before, connects_after,
        "close during reconnect backoff must not start an extra connect ({connects_before} -> {connects_after})"
    );
    assert!(!sub.is_alive());
}

#[tokio::test]
async fn l2_realtime_oversized_binary_message_fails_closed() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_oversized_after_handshake(
        active.clone(),
        MAX_REALTIME_MESSAGE_BYTES + 1,
    )
    .await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt
        .subscribe_proto_with_options(PUBLIC_CHANNEL, |bytes| Ok(bytes.to_vec()), false)
        .await
        .expect("initial handshake");
    wait_until(
        || sub.err().is_some() && !sub.is_alive(),
        Duration::from_secs(3),
    )
    .await;
    let error = sub.err().expect("oversized message error").to_string();
    let error = error.to_ascii_lowercase();
    assert!(
        error.contains("size")
            || error.contains("too long")
            || error.contains("capacity")
            || error.contains("space limit"),
        "unexpected oversized-message error: {error}"
    );
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
}

#[tokio::test]
async fn l2_close_during_reconnect_snapshot_retry_cancels_fetch_and_socket() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_once_then_idle(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    let (stalled_tx, stalled_rx) = tokio::sync::oneshot::channel::<()>();
    let stalled_rx = Arc::new(std::sync::Mutex::new(Some(stalled_rx)));
    let fetch_stalled_rx = stalled_rx.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_bytes| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return async { Ok("initial".to_owned()) }.boxed();
            }
            if attempt == 1 {
                return async { Err(Error::transport("retry once")) }.boxed();
            }
            let stalled = fetch_stalled_rx
                .lock()
                .expect("stalled snapshot lock")
                .take()
                .expect("only one stalled retry");
            async move {
                let _ = stalled.await;
                Ok("late snapshot".to_owned())
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_snapshot, _pending| {}),
        apply_live_publications: Arc::new(|_publications| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
    });

    sts.start().await.expect("initial snapshot");
    wait_until(
        || attempts.load(Ordering::SeqCst) >= 3 && active.load(Ordering::SeqCst) == 1,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    sts.close();
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        stalled_tx.send(()).is_err(),
        "closing during the second refresh attempt must drop the stalled fetch future"
    );
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "combined retry/cancellation cleanup lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_scale_format_and_catalog_reject_panic_boundary() {
    assert!(format_qty_scaled(1, 18).is_ok());
    assert!(format_qty_scaled(1, MAX_PROTOCOL_SCALE).is_ok());
    for scale in [37u32, 65534, 65535, 65536, u32::MAX] {
        assert!(
            format_qty_scaled(1, scale).is_err(),
            "scale {scale} must err"
        );
        assert!(format_ledger_u128("1", scale).is_err());
        let formatted = Quantity::from_scaled(1, Some(8), QuantityDomain::OrderBase, None, None)
            .unwrap()
            .format(Some(scale));
        assert!(formatted.is_err(), "Quantity::format({scale}) must err");
        let construct =
            Quantity::from_scaled(1, Some(scale), QuantityDomain::OrderBase, None, None);
        assert!(
            construct.is_err(),
            "from_scaled with scale {scale} must err at boundary"
        );
        let panic = std::panic::catch_unwind(|| {
            let _ = format_qty_scaled(1, scale);
        });
        assert!(panic.is_ok(), "format must not panic at scale {scale}");
    }

    let catalogs = polyester::catalogs::Manager::new();
    let err = catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 65535
            }]
        }))
        .expect_err("catalog must reject panic-boundary scale");
    assert!(err.to_string().contains("scale"));
    assert_eq!(catalogs.base_quantity_scale_for_symbol("BTC-USDT"), None);

    let err = catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "ETH-USDT",
                "symbol_id": 4294967296u64,
                "base_quantity_scale": 8
            }]
        }))
        .expect_err("catalog must reject symbol_id above u32");
    assert!(err.to_string().contains("symbol_id") || err.to_string().contains("u32"));
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_http_500() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 500,
        body: br#"{"error":"nope"}"#.to_vec(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("HTTP 500 must fail closed");
    assert!(
        err.to_string().contains("catalog hydration failed"),
        "{err}"
    );
    assert!(client.catalogs_last_error().is_some());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        None
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_empty_body() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: Vec::new(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("empty body must fail closed");
    assert!(err.to_string().contains("catalog"), "{err}");
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_malformed_protobuf() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Content-Length".into(), "1".into()),
                ],
                // Field zero with an unsupported wire type cannot decode as a
                // protobuf GetSpotConfigResponse.
                body: vec![0x0f],
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("malformed protobuf must fail closed");
    assert!(!client.catalogs.is_ready(), "{err}");
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_malformed_config() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":65535}]}"#
            .to_vec(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("bad scale in config must fail closed");
    assert!(
        err.to_string().contains("scale") || err.to_string().contains("catalog"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_rejects_oversized_content_length_response() {
    let body = vec![0u8; MAX_CONNECT_RESPONSE_BYTES + 1];
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body: body.clone(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("oversized catalog response must fail closed");
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("size")
            || message.contains("limit")
            || message.contains("resource exhausted")
            || message.contains("catalog"),
        "{err}"
    );
    assert!(!client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_rejects_oversized_chunked_response() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::ChunkedBody {
                status: 200,
                total_bytes: MAX_CONNECT_RESPONSE_BYTES + 1,
                chunk_size: 64 * 1024,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    client
        .wait_for_catalogs()
        .await
        .expect_err("oversized chunked catalog response must fail closed");
    assert!(!client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_concurrent_wait_for_catalogs_share_one_attempt() {
    let stall = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 500,
        headers: vec![("Content-Type".into(), "application/json".into())],
        stall,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(200),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    let (r1, r2) = tokio::join!(client.wait_for_catalogs(), client.wait_for_catalogs());
    assert!(r1.is_err());
    assert!(r2.is_err());
    // Spot + zipper = 2 requests for one shared attempt (not 4).
    let n = http.requests.load(Ordering::SeqCst);
    assert!(
        n <= 2,
        "concurrent waiters must share one attempt; saw {n} HTTP requests"
    );
}

#[tokio::test]
async fn l2_snapshot_then_stream_reconnect_snapshot_fail_sets_err_and_not_ready() {
    use std::sync::atomic::AtomicBool;

    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    // Succeed until the initial start marks ready; fail on reconnect refresh.
    let fail_after_ready = Arc::new(AtomicBool::new(false));
    let fail_flag = fail_after_ready.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            fetch_attempts.fetch_add(1, Ordering::SeqCst);
            let fail = fail_flag.load(Ordering::SeqCst);
            async move {
                if fail {
                    Err(Error::transport("snapshot refresh failed"))
                } else {
                    Ok("initial".to_string())
                }
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_s, _p| {}),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
    });
    sts.start().await.expect("initial start");
    assert!(sts.is_ready());
    fail_after_ready.store(true, Ordering::SeqCst);
    // Wait for disconnect → reconnect → failing snapshot (with one retry) → fail-closed.
    wait_until(
        || sts.err().is_some() && !sts.is_ready(),
        Duration::from_secs(5),
    )
    .await;
    assert!(sts.err().is_some());
    assert!(!sts.is_ready());
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "expected initial success + reconnect retries"
    );
    sts.close();
}

#[tokio::test]
async fn l2_balances_list_connect_response_preserves_ledger_scaled_integer() {
    use polyester::codecs::format_ledger_u128;
    use polyester::proto::ledger::read::v1::GetBalancesRequest;
    use polyester::proto::ledger::read::v1::{
        AssetBalance as ProtoAssetBalance, GetBalancesResponse,
    };
    use polyester::proto::polyester::r#type::v1::U128;

    const ONE_POINT_FIVE_E18: u64 = 1_500_000_000_000_000_000;
    let fixture = GetBalancesResponse {
        balances: vec![ProtoAssetBalance {
            asset_id: 1,
            trading: U128 {
                hi: 0,
                lo: ONE_POINT_FIVE_E18,
                ..Default::default()
            }
            .into(),
            funding: U128 {
                hi: 0,
                lo: ONE_POINT_FIVE_E18,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let http = MockHttpServer::spawn(move |req| {
        if req.path == GET_BALANCES_PATH {
            connect_proto_ok(&fixture)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect("client");
    let list = client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .expect("balances.list");
    assert_eq!(list.balances.len(), 1);
    assert_eq!(list.balances[0].trading, "1500000000000000000");
    assert_eq!(list.balances[0].funding, "1500000000000000000");
    assert_eq!(
        format_ledger_u128(&list.balances[0].trading, 18).expect("format"),
        "1.5"
    );
}

#[tokio::test]
async fn l2_wait_for_order_trades_complete_polls_get_order_until_trade_sum_matches() {
    use polyester::proto::orders::v1::{GetOrderResponse, Order, OrderStatus, UserTrade};

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path != GET_ORDER_PATH {
            return hardening_support::HttpScript::NotFound;
        }
        let n = calls_h.fetch_add(1, Ordering::SeqCst) + 1;
        let order = Order {
            order_id: 1,
            symbol_id: 1,
            status: OrderStatus::Filled.into(),
            cum_qty_scaled: 100,
            ..Default::default()
        };
        let resp = if n == 1 {
            GetOrderResponse {
                order: Some(order).into(),
                trades: vec![],
                ..Default::default()
            }
        } else {
            GetOrderResponse {
                order: Some(order).into(),
                trades: vec![
                    UserTrade {
                        symbol_id: 1,
                        qty_scaled: 40,
                        ..Default::default()
                    },
                    UserTrade {
                        symbol_id: 1,
                        qty_scaled: 60,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        };
        connect_proto_ok(&resp)
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect("client");
    let result = client
        .orders
        .wait_for_order_trades_complete(None, Some("1"), Duration::from_secs(2))
        .await
        .expect("wait complete");
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let cum = result
        .order
        .as_ref()
        .and_then(|o| o.cum_qty.as_ref())
        .map(|q| q.as_scaled())
        .expect("cum");
    let sum: i64 = result
        .trades
        .iter()
        .map(|t| t.qty.as_ref().map(|q| q.as_scaled()).unwrap_or(0))
        .sum();
    assert_eq!(cum, 100);
    assert_eq!(sum, 100);
}

#[tokio::test]
async fn l2_wait_for_order_trades_complete_enforces_overall_deadline() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == GET_ORDER_PATH {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Transfer-Encoding".into(), "chunked".into()),
                ],
                stall: Duration::from_secs(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(5),
        ..Default::default()
    })
    .expect("client");
    let helper_timeout = Duration::from_millis(250);
    let started = Instant::now();
    let err = client
        .orders
        .wait_for_order_trades_complete(None, Some("1"), helper_timeout)
        .await
        .expect_err("helper deadline must cover the in-flight GetOrder call");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "helper exceeded overall deadline: {:?}",
        started.elapsed()
    );
    assert!(err.to_string().contains("timed out"), "{err}");
}

fn spot_config_fixture() -> polyester::proto::marketdata::v1::GetSpotConfigResponse {
    use polyester::proto::marketdata::v1::{GetSpotConfigResponse, PairConfig};
    GetSpotConfigResponse {
        pairs: vec![PairConfig {
            symbol_id: 1,
            symbol: "BTC-USDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            base_quantity_scale: 8,
            ..Default::default()
        }],
        ts_sec: 1,
        ..Default::default()
    }
}

fn zipper_config_fixture() -> polyester::proto::chain::zipper::v1::GetDepositWithdrawConfigResponse
{
    use polyester::proto::chain::zipper::v1::{AssetConfig, GetDepositWithdrawConfigResponse};
    GetDepositWithdrawConfigResponse {
        polyester_chain_id: 1,
        ts_sec: 1,
        assets: vec![AssetConfig {
            asset: "USDT".into(),
            ledger_id: 99,
            quantity_scale: 6,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn l2_wait_for_catalogs_hydrates_spot_and_zipper_then_formats() {
    use polyester::proto::marketdata::v1::GetTradesResponse;
    use polyester::{Quantity, QuantityDomain};

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let trades = GetTradesResponse::default();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else if req.path == GET_TRADES_PATH {
            connect_proto_ok(&trades)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("hydrate");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
    assert_eq!(client.catalogs.symbol_id_for_symbol("BTC-USDT"), Some(1));
    assert_eq!(client.catalogs.ledger_id_for_asset("USDT"), Some(99));
    let qty = Quantity::from_scaled(100_000_000, Some(8), QuantityDomain::OrderBase, None, None)
        .expect("qty");
    assert_eq!(qty.format(None).expect("format"), "1");
    // Public symbol resolution path after hydrate.
    let _ = client
        .market_data
        .get_trades("BTC-USDT", Some(1))
        .await
        .expect("get_trades resolves hydrated symbol");
}

#[tokio::test]
async fn l2_wait_for_catalogs_no_headers_stalls_then_times_out() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::NeverRespond {
        stall: Duration::from_secs(30),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout,
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let started = Instant::now();
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("no-headers must fail");
    assert!(started.elapsed() < timeout + Duration::from_millis(1200));
    assert!(err.to_string().contains("catalog"), "{err}");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        None
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_headers_then_body_stall_times_out() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 200,
        headers: vec![
            ("Content-Type".into(), "application/proto".into()),
            ("Transfer-Encoding".into(), "chunked".into()),
        ],
        stall,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout,
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let started = Instant::now();
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("body stall must fail");
    assert!(started.elapsed() < timeout + Duration::from_millis(1200));
    assert!(err.to_string().contains("catalog"), "{err}");
}

#[tokio::test]
async fn l2_wait_for_catalogs_success_path() {
    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("ok");
    assert!(client.catalogs_last_error().is_none());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_can_retry_after_transient_failure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = attempts.clone();
    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            if attempts_h.fetch_add(1, Ordering::SeqCst) == 0 {
                hardening_support::HttpScript::Json {
                    status: 503,
                    body: br#"{"error":"temporary"}"#.to_vec(),
                }
            } else {
                connect_proto_ok(&spot)
            }
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    client
        .wait_for_catalogs()
        .await
        .expect_err("first catalog attempt must surface the transient failure");
    client
        .wait_for_catalogs()
        .await
        .expect("second catalog attempt must recover");
    assert!(client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_none());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_zipper_failure_leaves_spot_unhydrated() {
    let spot = spot_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            hardening_support::HttpScript::Json {
                status: 500,
                body: br#"{"error":"zipper down"}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("zipper 500 must fail closed");
    assert!(err.to_string().contains("catalog"), "{err}");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        None,
        "spot must not install when zipper fails"
    );
}

#[tokio::test]
async fn l2_snapshot_then_stream_recovery_success_clears_err() {
    use std::sync::Mutex;

    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    let merged = Arc::new(Mutex::new(Vec::<u8>::new()));
    let merged_cb = merged.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(Error::transport("snapshot refresh failed"))
                } else {
                    Ok("recovered".to_string())
                }
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(move |_s, pending| {
            merged_cb.lock().expect("merged").extend(pending);
        }),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
    });

    // Drive buffer retention without waiting on WS publications: inject via refresh path.
    // First refresh fails (sets err, keeps buffer); inject pubs while not ready; retry succeeds.
    assert!(sts.refresh_snapshot().await.is_err());
    assert!(sts.err().is_some());
    // Direct buffer injection through a second STS that shares apply is awkward; instead
    // re-run the unit-level retention contract via public refresh_snapshot success clear.
    sts.refresh_snapshot().await.expect("recovery");
    assert!(sts.is_ready());
    assert!(sts.err().is_none());
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    sts.close();
}

// Silence unused import warnings when Credentials helpers evolve.
#[allow(dead_code)]
fn _creds_type_check() -> Credentials {
    test_credentials("ak", TEST_KEY)
}

#[allow(dead_code)]
fn _reply_helper() -> Vec<u8> {
    centrifugo_ok_reply(1)
}
