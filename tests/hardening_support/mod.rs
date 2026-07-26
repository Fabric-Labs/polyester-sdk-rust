//! Local mock HTTP/WS servers for POLY-3746 L2 tests.

use buffa::Message as BuffaMessage;
use futures_util::{SinkExt, StreamExt};
use polyester::auth::Credentials;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::{Request as WsRequest, Response as WsResponse};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

pub struct ParsedRequest {
    #[allow(dead_code)]
    pub method: String,
    pub path: String,
}

pub enum HttpScript {
    NotFound,
    NeverRespond {
        stall: Duration,
    },
    Json {
        status: u16,
        body: Vec<u8>,
    },
    Raw {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    HeadersThenStall {
        status: u16,
        headers: Vec<(String, String)>,
        stall: Duration,
    },
    /// Chunked transfer that keeps sending until `total_bytes` then stops.
    ChunkedBody {
        status: u16,
        total_bytes: usize,
        chunk_size: usize,
    },
}

pub struct MockHttpServer {
    base_url: String,
    pub requests: Arc<AtomicUsize>,
    pub in_flight: Arc<AtomicUsize>,
    _join: tokio::task::JoinHandle<()>,
}

impl MockHttpServer {
    pub async fn spawn(
        handler: impl Fn(ParsedRequest) -> HttpScript + Send + Sync + 'static,
    ) -> Self {
        Self::spawn_counted(handler).await
    }

    pub async fn spawn_counted(
        handler: impl Fn(ParsedRequest) -> HttpScript + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
        let addr = listener.local_addr().expect("addr");
        let handler = Arc::new(handler);
        let requests = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let requests_task = requests.clone();
        let in_flight_task = in_flight.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                let requests = requests_task.clone();
                let in_flight = in_flight_task.clone();
                tokio::spawn(async move {
                    in_flight.fetch_add(1, Ordering::SeqCst);
                    let _guard = ConnGuard(in_flight);
                    let mut buf = vec![0u8; 16 * 1024];
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    requests.fetch_add(1, Ordering::SeqCst);
                    let raw = String::from_utf8_lossy(&buf[..n]);
                    let mut lines = raw.split("\r\n");
                    let request_line = lines.next().unwrap_or("");
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_owned();
                    let path = parts.next().unwrap_or("/").to_owned();
                    let path = path.split('?').next().unwrap_or(&path).to_owned();
                    match handler(ParsedRequest { method, path }) {
                        HttpScript::NotFound => {
                            let _ = stream
                                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                                .await;
                        }
                        HttpScript::NeverRespond { stall } => {
                            let mut peek = [0u8; 1];
                            tokio::select! {
                                _ = tokio::time::sleep(stall) => {}
                                _ = stream.read(&mut peek) => {}
                            }
                        }
                        HttpScript::Json { status, body } => {
                            let head = format!(
                                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            );
                            let _ = stream.write_all(head.as_bytes()).await;
                            let _ = stream.write_all(&body).await;
                        }
                        HttpScript::Raw {
                            status,
                            headers,
                            body,
                        } => {
                            let mut head = format!("HTTP/1.1 {status} OK\r\n");
                            for (k, v) in headers {
                                head.push_str(&format!("{k}: {v}\r\n"));
                            }
                            head.push_str("Connection: close\r\n\r\n");
                            let _ = stream.write_all(head.as_bytes()).await;
                            let _ = stream.write_all(&body).await;
                        }
                        HttpScript::HeadersThenStall {
                            status,
                            headers,
                            stall,
                        } => {
                            let mut head = format!("HTTP/1.1 {status} OK\r\n");
                            for (k, v) in headers {
                                head.push_str(&format!("{k}: {v}\r\n"));
                            }
                            head.push_str("\r\n");
                            let _ = stream.write_all(head.as_bytes()).await;
                            let _ = stream.flush().await;
                            // Exit early when the client aborts (E6 cancel-orphan).
                            let mut peek = [0u8; 1];
                            tokio::select! {
                                _ = tokio::time::sleep(stall) => {}
                                _ = stream.read(&mut peek) => {}
                            }
                        }
                        HttpScript::ChunkedBody {
                            status,
                            total_bytes,
                            chunk_size,
                        } => {
                            let head = format!(
                                "HTTP/1.1 {status} OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                            );
                            let _ = stream.write_all(head.as_bytes()).await;
                            let mut sent = 0usize;
                            while sent < total_bytes {
                                let n = (total_bytes - sent).min(chunk_size.max(1));
                                let chunk = vec![b'x'; n];
                                let header = format!("{n:x}\r\n");
                                if stream.write_all(header.as_bytes()).await.is_err() {
                                    break;
                                }
                                if stream.write_all(&chunk).await.is_err() {
                                    break;
                                }
                                if stream.write_all(b"\r\n").await.is_err() {
                                    break;
                                }
                                sent += n;
                            }
                            let _ = stream.write_all(b"0\r\n\r\n").await;
                        }
                    }
                });
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            requests,
            in_flight,
            _join: join,
        }
    }

    pub fn base_url(&self) -> String {
        self.base_url.clone()
    }
}

pub struct MockWsServer {
    addr: String,
    pub connects: Arc<AtomicUsize>,
    #[allow(dead_code)]
    pub active: Arc<AtomicUsize>,
    _join: tokio::task::JoinHandle<()>,
}

impl MockWsServer {
    pub fn ws_url(&self) -> String {
        format!("ws://{}/connection/websocket", self.addr)
    }

    pub async fn spawn_hang_after_accept() -> Self {
        Self::spawn_hang_after_accept_counted(Arc::new(AtomicUsize::new(0))).await
    }

    pub async fn spawn_hang_after_accept_counted(active: Arc<AtomicUsize>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ws");
        let addr = listener.local_addr().expect("addr").to_string();
        let connects = Arc::new(AtomicUsize::new(0));
        let connects_task = connects.clone();
        let active_task = active.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let active = active_task.clone();
                let connects = connects_task.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_protobuf_ws(stream).await else {
                        return;
                    };
                    connects.fetch_add(1, Ordering::SeqCst);
                    active.fetch_add(1, Ordering::SeqCst);
                    let _guard = ConnGuard(active);
                    while let Some(Ok(_)) = ws.next().await {}
                });
            }
        });
        Self {
            addr,
            connects,
            active,
            _join: join,
        }
    }

    pub async fn spawn_centrifugo_public(active: Arc<AtomicUsize>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ws");
        let addr = listener.local_addr().expect("addr").to_string();
        let connects = Arc::new(AtomicUsize::new(0));
        let connects_task = connects.clone();
        let active_task = active.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let active = active_task.clone();
                let connects = connects_task.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_protobuf_ws(stream).await else {
                        return;
                    };
                    connects.fetch_add(1, Ordering::SeqCst);
                    active.fetch_add(1, Ordering::SeqCst);
                    let _guard = ConnGuard(active);
                    let mut replies = 0u8;
                    while let Some(msg) = ws.next().await {
                        let Ok(msg) = msg else { break };
                        match msg {
                            Message::Binary(_) if replies < 2 => {
                                replies += 1;
                                let id = u32::from(replies);
                                let _ = ws
                                    .send(Message::Binary(centrifugo_ok_reply(id).into()))
                                    .await;
                            }
                            Message::Ping(p) => {
                                let _ = ws.send(Message::Pong(p)).await;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                });
            }
        });
        Self {
            addr,
            connects,
            active,
            _join: join,
        }
    }

    /// Accept connect+subscribe, then drop the socket so the client reconnects.
    pub async fn spawn_centrifugo_disconnect_after_handshake(active: Arc<AtomicUsize>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ws");
        let addr = listener.local_addr().expect("addr").to_string();
        let connects = Arc::new(AtomicUsize::new(0));
        let connects_task = connects.clone();
        let active_task = active.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let active = active_task.clone();
                let connects = connects_task.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_protobuf_ws(stream).await else {
                        return;
                    };
                    connects.fetch_add(1, Ordering::SeqCst);
                    active.fetch_add(1, Ordering::SeqCst);
                    let _guard = ConnGuard(active);
                    let mut replies = 0u8;
                    while let Some(msg) = ws.next().await {
                        let Ok(msg) = msg else { break };
                        if let Message::Binary(_) = msg
                            && replies < 2
                        {
                            replies += 1;
                            let _ = ws
                                .send(Message::Binary(
                                    centrifugo_ok_reply(u32::from(replies)).into(),
                                ))
                                .await;
                            if replies == 2 {
                                break;
                            }
                        }
                    }
                });
            }
        });
        Self {
            addr,
            connects,
            active,
            _join: join,
        }
    }
}

struct ConnGuard(Arc<AtomicUsize>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn accept_protobuf_ws(
    stream: tokio::net::TcpStream,
) -> Result<WebSocketStream<tokio::net::TcpStream>, ()> {
    accept_hdr_async(
        stream,
        #[allow(clippy::result_large_err)]
        |req: &WsRequest<()>, mut response: WsResponse<()>| {
            let proto = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if proto.split(',').any(|p| p.trim() == "centrifuge-protobuf") {
                response.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    "centrifuge-protobuf".parse().unwrap(),
                );
            }
            Ok(response)
        },
    )
    .await
    .map_err(|_| ())
}

/// Encode a Centrifugo protobuf Reply with `id` and no error.
pub fn centrifugo_ok_reply(id: u32) -> Vec<u8> {
    let mut message = Vec::new();
    // Field 1 (id), wire type 0 (varint).
    message.push(0x08);
    put_varint(&mut message, u64::from(id));
    length_delimit(message)
}

fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn length_delimit(message: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, message.len() as u64);
    out.extend(message);
    out
}

pub fn test_credentials(key_id: &str, private_hex: &str) -> Credentials {
    Credentials::new(key_id, private_hex).expect("test credentials")
}

/// Connect unary protobuf success response (`Content-Type: application/proto`).
pub fn connect_proto_ok<M: BuffaMessage>(msg: &M) -> HttpScript {
    HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/proto".into())],
        body: msg.encode_to_bytes().to_vec(),
    }
}

pub const SPOT_CONFIG_PATH: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
pub const ZIPPER_CONFIG_PATH: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";
pub const GET_ORDER_PATH: &str = "/orders.v1.OrdersReadService/GetOrder";
pub const GET_BALANCES_PATH: &str = "/ledger.read.v1.LedgerReadService/GetBalances";
pub const GET_TRADES_PATH: &str = "/marketdata.v1.MarketDataService/GetTrades";

pub async fn wait_until(mut pred: impl FnMut() -> bool, timeout: Duration) {
    let start = std::time::Instant::now();
    while !pred() {
        if start.elapsed() > timeout {
            panic!("condition not met within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
