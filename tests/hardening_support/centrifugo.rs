//! A real Centrifugo-shaped socket server and token/list API for tests. Counts
//! only the operation each counter names.

use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const WAIT: Duration = Duration::from_secs(10);

/// Per channel, its latest connection; `None` asks it to close.
type Connections = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Option<Vec<u8>>>>>>;

/// Real Centrifugo-shaped socket server: answers connect and subscribe, then
/// writes frames pushed to the channel's latest connection.
pub struct Centrifugo {
    pub url: String,
    conns: Connections,
    subscribed: Mutex<mpsc::UnboundedReceiver<String>>,
    pub connects: Arc<AtomicUsize>,
    on_subscribed: Arc<Mutex<Option<Vec<u8>>>>,
    silent: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Centrifugo {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn channel_in(command: &[u8]) -> String {
    let find = |needle: &[u8]| command.windows(needle.len()).position(|w| w == needle);
    let start = [find(b"private:"), find(b"public:")]
        .into_iter()
        .flatten()
        .min()
        .expect("subscribe command names a channel");
    let len = usize::from(command[start - 1]);
    String::from_utf8_lossy(&command[start..start + len]).into_owned()
}

impl Centrifugo {
    pub async fn spawn() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let conns: Connections = Arc::default();
        let connects = Arc::new(AtomicUsize::new(0));
        let (subscribed_tx, subscribed) = mpsc::unbounded_channel();
        let accept_conns = conns.clone();
        let accept_connects = connects.clone();
        let on_subscribed: Arc<Mutex<Option<Vec<u8>>>> = Arc::default();
        let accept_hook = on_subscribed.clone();
        let silent = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let accept_silent = silent.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let conns = accept_conns.clone();
                let subscribed = subscribed_tx.clone();
                let connects = accept_connects.clone();
                let on_subscribed = accept_hook.clone();
                let silent = accept_silent.clone();
                tokio::spawn(async move {
                    let accepted = tokio_tungstenite::accept_hdr_async(
                        socket,
                        |_: &_,
                         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            response.headers_mut().insert(
                                "sec-websocket-protocol",
                                "centrifuge-protobuf".parse().unwrap(),
                            );
                            Ok(response)
                        },
                    )
                    .await;
                    let Ok(mut ws) = accepted else { return };
                    connects.fetch_add(1, Ordering::SeqCst);
                    if silent.load(Ordering::SeqCst) {
                        while let Some(Ok(_)) = ws.next().await {}
                        return;
                    }
                    let mut channel = String::new();
                    for id in 1..=2u32 {
                        let Some(Ok(command)) = ws.next().await else {
                            return;
                        };
                        if id == 2 {
                            channel = channel_in(&command.into_data());
                        }
                        if ws
                            .send(WsMessage::Binary(centrifugo_ok_reply(id).into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let (tx, mut rx) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
                    conns.lock().unwrap().insert(channel.clone(), tx.clone());
                    if let Some(frame) = on_subscribed.lock().unwrap().clone() {
                        let _ = tx.send(Some(frame));
                    }
                    let _ = subscribed.send(channel);
                    loop {
                        tokio::select! {
                            frame = rx.recv() => match frame {
                                Some(Some(frame)) => {
                                    if ws.send(WsMessage::Binary(frame.into())).await.is_err() {
                                        return;
                                    }
                                }
                                // A drop request closes this connection.
                                Some(None) | None => {
                                    let _ = ws.close(None).await;
                                    return;
                                }
                            },
                            message = ws.next() => match message {
                                Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => return,
                                Some(Ok(_)) => {}
                            },
                        }
                    }
                });
            }
        });
        Self {
            url,
            conns,
            subscribed: Mutex::new(subscribed),
            connects,
            on_subscribed,
            silent,
            task,
        }
    }

    /// Later connections are accepted but never confirmed.
    pub fn stop_confirming(&self) {
        self.silent.store(true, Ordering::SeqCst);
    }

    /// Writes `frame` right after every subscribe from now on.
    pub fn after_subscribe(&self, frame: Vec<u8>) {
        *self.on_subscribed.lock().unwrap() = Some(frame);
    }

    pub async fn await_subscribed(&self, channel: &str) {
        let mut rx = std::mem::replace(
            &mut *self.subscribed.lock().unwrap(),
            mpsc::unbounded_channel().1,
        );
        let found = tokio::time::timeout(WAIT, async {
            while let Some(got) = rx.recv().await {
                if got == channel {
                    return true;
                }
            }
            false
        })
        .await;
        *self.subscribed.lock().unwrap() = rx;
        assert!(matches!(found, Ok(true)), "{channel} was not subscribed");
    }

    /// Waits for `count` subscribes on any channel, in any order.
    pub async fn await_subscribes(&self, count: usize) {
        let mut rx = std::mem::replace(
            &mut *self.subscribed.lock().unwrap(),
            mpsc::unbounded_channel().1,
        );
        let mut seen = 0;
        let finished = tokio::time::timeout(3 * WAIT, async {
            while seen < count && rx.recv().await.is_some() {
                seen += 1;
            }
        })
        .await;
        *self.subscribed.lock().unwrap() = rx;
        assert!(
            finished.is_ok(),
            "only {seen} of {count} subscribes arrived"
        );
    }

    pub fn push(&self, channel: &str, frame: Vec<u8>) {
        self.conns
            .lock()
            .unwrap()
            .get(channel)
            .expect("channel connected")
            .send(Some(frame))
            .unwrap();
    }

    pub fn drop_connection(&self, channel: &str) {
        self.conns
            .lock()
            .unwrap()
            .get(channel)
            .expect("channel connected")
            .send(None)
            .unwrap();
    }
}

fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        buf.push(if value == 0 { byte } else { byte | 0x80 });
        if value == 0 {
            return;
        }
    }
}

fn put_field(buf: &mut Vec<u8>, field: u64, payload: &[u8]) {
    put_varint(buf, (field << 3) | 2);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// `Reply.push { channel = 2, unsubscribe = 7 { code = 2, reason = 3 } }`.
pub fn unsubscribe_frame(channel: &str, code: u64, reason: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_varint(&mut body, 2 << 3);
    put_varint(&mut body, code);
    put_field(&mut body, 3, reason.as_bytes());
    let mut push = Vec::new();
    put_field(&mut push, 2, channel.as_bytes());
    put_field(&mut push, 7, &body);
    let mut reply = Vec::new();
    put_field(&mut reply, 4, &push);
    let mut frame = Vec::new();
    put_varint(&mut frame, reply.len() as u64);
    frame.extend_from_slice(&reply);
    frame
}

/// `Reply { id }`.
pub fn centrifugo_ok_reply(id: u32) -> Vec<u8> {
    let mut reply = Vec::new();
    put_varint(&mut reply, 1 << 3);
    put_varint(&mut reply, u64::from(id));
    let mut frame = Vec::new();
    put_varint(&mut frame, reply.len() as u64);
    frame.extend_from_slice(&reply);
    frame
}

/// `Reply.push.pub.data`.
pub fn centrifugo_publication(payload: &[u8]) -> Vec<u8> {
    let mut publication = Vec::new();
    put_field(&mut publication, 4, payload);
    let mut push = Vec::new();
    put_field(&mut push, 4, &publication);
    let mut reply = Vec::new();
    put_field(&mut reply, 4, &push);
    let mut frame = Vec::new();
    put_varint(&mut frame, reply.len() as u64);
    frame.extend_from_slice(&reply);
    frame
}

type Responder = dyn Fn(&str) -> (u16, String) + Send + Sync;

/// Local HTTP API answering by path, counting requests per path.
pub struct ApiMock {
    pub url: String,
    counts: Arc<Mutex<HashMap<String, usize>>>,
    respond: Arc<Mutex<Arc<Responder>>>,
    /// While it holds a path, requests to that path wait without blocking a worker.
    held: tokio::sync::watch::Sender<Option<String>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ApiMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ApiMock {
    pub async fn spawn(respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let counts: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
        let responder: Arc<Responder> = Arc::new(respond);
        let respond = Arc::new(Mutex::new(responder));
        let task_counts = counts.clone();
        let task_respond = respond.clone();
        let (held, held_rx) = tokio::sync::watch::channel(None::<String>);
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let counts = task_counts.clone();
                let respond = task_respond.clone();
                let mut held = held_rx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let raw = String::from_utf8_lossy(&buf[..n]);
                    let mut request_line = raw.lines().next().unwrap_or("").split_whitespace();
                    let method = request_line.next().unwrap_or("");
                    let target = request_line.next().unwrap_or("/");
                    let path = target.split('?').next().unwrap_or(target).to_owned();
                    let (status, body) = if method == "GET" {
                        *counts.lock().unwrap().entry(path.clone()).or_insert(0) += 1;
                        let _ = held
                            .wait_for(|hold| hold.as_deref() != Some(path.as_str()))
                            .await;
                        let responder = respond.lock().unwrap().clone();
                        responder(&path)
                    } else {
                        (404, String::new())
                    };
                    let response = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            url,
            counts,
            respond,
            held,
            task,
        }
    }

    /// Holds later and waiting requests to `path` until [`Self::release`].
    pub fn hold(&self, path: &str) {
        self.held.send_replace(Some(path.to_owned()));
    }

    pub fn release(&self) {
        self.held.send_replace(None);
    }

    pub fn set_respond(&self, respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static) {
        *self.respond.lock().unwrap() = Arc::new(respond);
    }

    pub fn count(&self, path: &str) -> usize {
        self.counts.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}
