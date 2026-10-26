//! Account order streams over the private WebSocket (`/v1/ws/private`).
//!
//! One connection carries every account's subscription, and each account has
//! one server subscription shared by all of its consumers. A subscription
//! resumes from its last applied cursor after a sequence gap, an expired
//! token or a reconnect.

use crate::auth::Credentials;
use crate::codecs::decode::order_from_proto;
use crate::codecs::decode::orders_unit_from_proto;
use crate::errors::{Error, Result};
use crate::models::{Order, OrdersUnit, PrivateOrdersEvent};
use crate::proto::privatews::v1::{
    Auth, Channel, ClientMessage, ServerMessage, Subscribe, SubscribeRefresh, Unsubscribe,
    client_message, request_error, server_message,
};
use buffa::{Enumeration, Message as _};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rand_core::{OsRng, RngCore};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::{
    self, Message,
    client::IntoClientRequest,
    http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
    protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
};

/// Path of the private WebSocket on the API host.
pub const PRIVATE_WS_PATH: &str = "/v1/ws/private";
/// WebSocket subprotocol for binary protobuf frames.
pub const PRIVATE_WS_SUBPROTOCOL: &str = "polyester.v1.proto";

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// A connection that stayed up this long resets the reconnect backoff.
const STABLE_CONNECTION: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// A malformed frame (4400) and a rejected connect token (4403) recur on every reconnect.
const TERMINAL_CLOSE_CODES: [u16; 2] = [4400, 4403];
const HEARTBEAT_CLOSE_CODE: u16 = 4000;
const ABNORMAL_CLOSE_CODE: u16 = 1006;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(crate) type WsSink = Pin<Box<dyn Sink<Message, Error = tungstenite::Error> + Send>>;
pub(crate) type WsStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Message, tungstenite::Error>> + Send>>;

/// Tokens and sockets for the stream: the live API in production, a local
/// fake server in tests.
pub(crate) trait Transport: Send + Sync + 'static {
    fn connect_token(&self) -> BoxFuture<Result<String>>;
    fn subscribe_token(&self, channel: String) -> BoxFuture<Result<String>>;
    fn open(&self) -> BoxFuture<Result<(WsSink, WsStream)>>;
}

/// The private WebSocket of an API host, authenticated with signed realtime tokens.
pub(crate) struct LiveTransport {
    pub(crate) url: String,
    pub(crate) api_url: String,
    pub(crate) credentials: Credentials,
    pub(crate) timeout: Duration,
    pub(crate) allow_insecure_http: bool,
}

/// `wss://<host>/v1/ws/private` for an `https://` API URL, `ws://` for `http://`.
pub(crate) fn private_ws_url(api_url: &str, allow_insecure_http: bool) -> Result<String> {
    let parsed = crate::transport::validate_http_url(api_url, allow_insecure_http)?;
    let scheme = if parsed.scheme() == "https" {
        "wss"
    } else {
        "ws"
    };
    let rest = api_url
        .trim()
        .trim_end_matches('/')
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or_default();
    let url = format!("{scheme}://{rest}{PRIVATE_WS_PATH}");
    crate::transport::validate_ws_url(&url, allow_insecure_http)?;
    Ok(url)
}

impl Transport for LiveTransport {
    fn connect_token(&self) -> BoxFuture<Result<String>> {
        let credentials = self.credentials.clone();
        let api_url = self.api_url.clone();
        let (timeout, insecure) = (self.timeout, self.allow_insecure_http);
        Box::pin(async move {
            super::auth::fetch_connection_token(&credentials, &api_url, timeout, insecure).await
        })
    }

    fn subscribe_token(&self, channel: String) -> BoxFuture<Result<String>> {
        let credentials = self.credentials.clone();
        let api_url = self.api_url.clone();
        let (timeout, insecure) = (self.timeout, self.allow_insecure_http);
        Box::pin(async move {
            super::auth::fetch_subscription_token(
                &credentials,
                &api_url,
                &channel,
                timeout,
                insecure,
            )
            .await
        })
    }

    fn open(&self) -> BoxFuture<Result<(WsSink, WsStream)>> {
        let url = self.url.clone();
        Box::pin(async move {
            let mut request = url
                .as_str()
                .into_client_request()
                .map_err(|e| Error::realtime(format!("private WebSocket request: {e}")))?;
            request.headers_mut().insert(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static(PRIVATE_WS_SUBPROTOCOL),
            );
            let config = WebSocketConfig::default()
                .max_message_size(Some(super::MAX_REALTIME_MESSAGE_BYTES))
                .max_frame_size(Some(super::MAX_REALTIME_MESSAGE_BYTES));
            let (ws, response) = tokio::time::timeout(
                CONNECT_TIMEOUT,
                tokio_tungstenite::connect_async_with_config(request, Some(config), false),
            )
            .await
            .map_err(|_| Error::realtime("private WebSocket connect timed out"))?
            .map_err(|e| Error::realtime(format!("private WebSocket connect: {e}")))?;
            if response
                .headers()
                .get(SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
                != Some(PRIVATE_WS_SUBPROTOCOL)
            {
                return Err(Error::realtime(format!(
                    "server did not negotiate the {PRIVATE_WS_SUBPROTOCOL} WebSocket subprotocol"
                )));
            }
            let (sink, stream) = ws.split();
            Ok((Box::pin(sink) as WsSink, Box::pin(stream) as WsStream))
        })
    }
}

/// Shared handle to the stream task of one realtime client; cheap to clone.
#[derive(Clone)]
pub(crate) struct PrivateOrderStreams {
    commands: Arc<Mutex<Option<mpsc::UnboundedSender<Command>>>>,
    max_queue: usize,
}

impl PrivateOrderStreams {
    pub(crate) fn new(max_queue: usize) -> Self {
        Self {
            commands: Arc::default(),
            max_queue: max_queue.max(1),
        }
    }

    /// Adds a consumer of `account`'s order stream. Must run inside a Tokio runtime.
    pub(crate) fn connect(
        &self,
        account: u64,
        transport: impl FnOnce() -> Arc<dyn Transport>,
    ) -> PrivateOrdersSubscription {
        let (tx, rx) = mpsc::channel(self.max_queue);
        let ended = Arc::new(Mutex::new(None));
        let id = next_consumer_id();
        let mut command = Command::Add {
            account,
            consumer: Consumer {
                id,
                tx,
                ended: ended.clone(),
            },
        };
        let mut guard = super::lock_unpoisoned(&self.commands);
        // A stream task outlives its last consumer until the client is dropped;
        // one whose runtime shut down is replaced on the caller's runtime.
        if let Some(commands) = guard.as_ref() {
            match commands.send(command) {
                Ok(()) => {
                    return PrivateOrdersSubscription {
                        rx,
                        id,
                        commands: commands.clone(),
                        ended,
                    };
                }
                Err(mpsc::error::SendError(returned)) => command = returned,
            }
        }
        let (commands, commands_rx) = mpsc::unbounded_channel();
        commands
            .send(command)
            .expect("a new stream task receives its first command");
        tokio::spawn(Actor::new(transport(), commands_rx).run());
        *guard = Some(commands.clone());
        PrivateOrdersSubscription {
            rx,
            id,
            commands,
            ended,
        }
    }
}

fn next_consumer_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// One consumer of an account's live order stream.
///
/// Events arrive in order through [`Self::recv`]. Dropping the handle or
/// calling [`Self::close`] leaves the stream; the server subscription ends
/// when its last consumer leaves.
pub struct PrivateOrdersSubscription {
    rx: mpsc::Receiver<PrivateOrdersEvent>,
    id: u64,
    commands: mpsc::UnboundedSender<Command>,
    ended: Arc<Mutex<Option<Error>>>,
}

impl PrivateOrdersSubscription {
    /// The next event, or `None` once the stream ended.
    pub async fn recv(&mut self) -> Option<PrivateOrdersEvent> {
        self.rx.recv().await
    }

    /// Like [`Self::recv`], but a stream that ended because of an error
    /// (a terminal stream error, or a full queue because the consumer did not
    /// keep up) returns that error instead of `None`.
    pub async fn recv_result(&mut self) -> Result<Option<PrivateOrdersEvent>> {
        match self.rx.recv().await {
            Some(event) => Ok(Some(event)),
            None => match super::lock_unpoisoned(&self.ended).take() {
                Some(err) => Err(err),
                None => Ok(None),
            },
        }
    }

    /// Leaves the stream. Buffered events stay readable.
    pub fn close(&self) {
        let _ = self.commands.send(Command::Remove { consumer: self.id });
    }
}

impl Drop for PrivateOrdersSubscription {
    fn drop(&mut self) {
        self.close();
    }
}

enum Command {
    Add { account: u64, consumer: Consumer },
    Remove { consumer: u64 },
}

struct Consumer {
    id: u64,
    tx: mpsc::Sender<PrivateOrdersEvent>,
    ended: Arc<Mutex<Option<Error>>>,
}

enum OpenFailure {
    Token(Error),
    Socket(Error),
}

enum Internal {
    Opened {
        generation: u64,
        result: std::result::Result<(String, WsSink, WsStream), OpenFailure>,
    },
    Frame {
        conn: u64,
        frame: Message,
    },
    Closed {
        conn: u64,
        code: u16,
        reason: String,
    },
    SubscribeToken {
        conn: u64,
        request: String,
        result: Result<String>,
    },
    RefreshToken {
        conn: u64,
        account: u64,
        sub: String,
        result: Result<String>,
    },
    ReauthToken {
        conn: u64,
        result: Result<String>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Pending,
    Connected,
    Disconnected,
}

/// One server subscription per account, shared by every consumer of that account.
struct Subscription {
    /// Distinguishes this subscription from a later one of the same account.
    id: u64,
    channel: String,
    consumers: Vec<Consumer>,
    /// Server subscription ID; empty until Subscribed.
    sub: String,
    seq: u64,
    /// Last applied resume position, carried across resubscribes and reconnects.
    cursor: String,
    staged: Option<Vec<(u64, Order)>>,
    /// The live view as last applied, for consumers that join later.
    orders: Option<BTreeMap<u64, Order>>,
    /// The last connection state consumers were told about.
    state: State,
    /// Consumers were told the view is stale and not yet that it caught up.
    stale: bool,
    /// The current server subscription reached its read boundary (`ready`).
    ready: bool,
    /// Subscribe attempts since the subscription last delivered data; drives retry backoff.
    retries: u32,
    retry_at: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Subscribe,
    Refresh,
}

#[derive(Clone, Copy)]
struct PendingRequest {
    kind: RequestKind,
    account: u64,
    subscription: u64,
}

struct Connection {
    id: u64,
    out: mpsc::UnboundedSender<Message>,
    reader: tokio::task::JoinHandle<()>,
    opened_at: Instant,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// A frame decoded into SDK models before its sequence number is applied.
enum Decoded {
    Page(Vec<(u64, Order)>),
    Unit(Vec<u64>, Vec<u64>, OrdersUnit),
    Other,
}

struct Actor {
    transport: Arc<dyn Transport>,
    commands: mpsc::UnboundedReceiver<Command>,
    internal_tx: mpsc::UnboundedSender<Internal>,
    internal_rx: mpsc::UnboundedReceiver<Internal>,
    subs: HashMap<u64, Subscription>,
    pending: HashMap<String, PendingRequest>,
    by_sub: HashMap<String, u64>,
    /// IDs of Unsubscribe requests awaiting their answer.
    unsubscribing: HashSet<String>,
    /// Accounts whose last consumer left; torn down once the current batch of work is done.
    idle: HashSet<u64>,
    conn: Option<Connection>,
    /// A socket whose writes can no longer be queued; closed after the current event.
    unwritable: Option<u64>,
    /// A connect token fetch or socket open for the next connection is in flight.
    connecting: bool,
    /// Bumped on shutdown so an open that outlived it is discarded.
    generation: u64,
    next_conn: u64,
    next_subscription: u64,
    next_request: u64,
    reconnect_attempt: u32,
    reconnect_at: Option<Instant>,
    heartbeat_at: Option<Instant>,
    jitter: u64,
}

/// 0.5 s doubling to 10 s, jittered over the upper half of each step.
fn backoff(attempt: u32, jitter: &mut u64) -> Duration {
    let ceiling = (BACKOFF_BASE.as_millis() as u64)
        .saturating_mul(1u64 << (attempt + 1).min(16))
        .min(BACKOFF_MAX.as_millis() as u64);
    *jitter ^= *jitter << 13;
    *jitter ^= *jitter >> 7;
    *jitter ^= *jitter << 17;
    let floor = ceiling / 2;
    Duration::from_millis(floor + *jitter % (ceiling - floor + 1))
}

/// Token failures that recur on every retry: rejected credentials or a bad request.
fn is_terminal_token_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Auth(_) | Error::PermissionDenied { .. } | Error::Validation(_)
    )
}

fn decode(msg: &ServerMessage) -> Result<Decoded> {
    use server_message::Frame;
    Ok(match msg.frame.as_ref() {
        Some(Frame::SnapshotPage(page)) => Decoded::Page(
            page.orders
                .iter()
                .map(|order| Ok((order.order_id, order_from_proto(order)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
        Some(Frame::Unit(unit)) => Decoded::Unit(
            unit.orders.iter().map(|order| order.order_id).collect(),
            unit.removed_order_ids.clone(),
            orders_unit_from_proto(unit)?,
        ),
        _ => Decoded::Other,
    })
}

impl Actor {
    fn new(transport: Arc<dyn Transport>, commands: mpsc::UnboundedReceiver<Command>) -> Self {
        let (internal_tx, internal_rx) = mpsc::unbounded_channel();
        let mut jitter = OsRng.next_u64();
        if jitter == 0 {
            jitter = 0x9e37_79b9_7f4a_7c15;
        }
        Self {
            transport,
            commands,
            internal_tx,
            internal_rx,
            subs: HashMap::new(),
            pending: HashMap::new(),
            by_sub: HashMap::new(),
            unsubscribing: HashSet::new(),
            idle: HashSet::new(),
            conn: None,
            unwritable: None,
            connecting: false,
            generation: 0,
            next_conn: 0,
            next_subscription: 0,
            next_request: 0,
            reconnect_attempt: 0,
            reconnect_at: None,
            heartbeat_at: None,
            jitter,
        }
    }

    async fn run(mut self) {
        loop {
            let deadline = self.next_deadline();
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else { break };
                    self.on_command(command);
                    // Commands queued together run as one batch, so a consumer that
                    // leaves and joins again keeps the server subscription.
                    while let Ok(command) = self.commands.try_recv() {
                        self.on_command(command);
                    }
                }
                Some(event) = self.internal_rx.recv() => self.on_internal(event),
                _ = sleep_until(deadline), if deadline.is_some() => self.on_timers(),
            }
            self.finish_event();
        }
        self.shutdown();
    }

    /// Work deferred until the current event is handled.
    fn finish_event(&mut self) {
        if let Some(conn) = self.unwritable.take() {
            self.on_closed(conn, ABNORMAL_CLOSE_CODE, "write failed");
        }
        self.teardown_idle();
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.subs
            .values()
            .filter_map(|s| s.retry_at)
            .chain(self.reconnect_at)
            .chain(self.heartbeat_at)
            .min()
    }

    fn on_command(&mut self, command: Command) {
        match command {
            Command::Add { account, consumer } => self.add(account, consumer),
            Command::Remove { consumer } => {
                for (account, s) in &mut self.subs {
                    if let Some(index) = s.consumers.iter().position(|c| c.id == consumer) {
                        s.consumers.remove(index);
                        if s.consumers.is_empty() {
                            self.idle.insert(*account);
                        }
                        return;
                    }
                }
            }
        }
    }

    fn add(&mut self, account: u64, consumer: Consumer) {
        if let Some(s) = self.subs.get_mut(&account) {
            // Give a consumer that joins a running subscription what the others already know.
            let mut catch_up = Vec::new();
            if s.state == State::Connected {
                catch_up.push(PrivateOrdersEvent::Connected);
            }
            if let Some(orders) = &s.orders {
                catch_up.push(PrivateOrdersEvent::Snapshot(
                    orders.values().cloned().collect(),
                ));
            }
            if s.ready {
                catch_up.push(PrivateOrdersEvent::Ready);
            }
            if s.stale {
                catch_up.push(PrivateOrdersEvent::Stale(true));
            }
            if catch_up.into_iter().all(|event| offer(&consumer, event)) {
                s.consumers.push(consumer);
            }
            return;
        }
        self.next_subscription += 1;
        let channel = format!(
            "private:spot:orders:{}",
            crate::codecs::scalars::format_id(account)
        );
        self.subs.insert(
            account,
            Subscription {
                id: self.next_subscription,
                channel,
                consumers: vec![consumer],
                sub: String::new(),
                seq: 0,
                cursor: String::new(),
                staged: None,
                orders: None,
                state: State::Pending,
                stale: false,
                ready: false,
                retries: 0,
                retry_at: None,
            },
        );
        if self.conn.is_some() {
            self.subscribe(account);
        } else if !self.connecting && self.reconnect_at.is_none() {
            self.open();
        }
    }

    /// Fetches the connect token first: the server closes a socket that sends no auth within 5 seconds.
    fn open(&mut self) {
        self.connecting = true;
        let generation = self.generation;
        let transport = self.transport.clone();
        let internal = self.internal_tx.clone();
        tokio::spawn(async move {
            let result = match transport.connect_token().await {
                Err(err) => Err(OpenFailure::Token(err)),
                Ok(token) => match transport.open().await {
                    Ok((sink, stream)) => Ok((token, sink, stream)),
                    Err(err) => Err(OpenFailure::Socket(err)),
                },
            };
            let _ = internal.send(Internal::Opened { generation, result });
        });
    }

    fn on_internal(&mut self, event: Internal) {
        match event {
            Internal::Opened { generation, result } => {
                if generation != self.generation {
                    return;
                }
                self.connecting = false;
                match result {
                    Ok((token, sink, stream)) => self.attach(token, sink, stream),
                    Err(OpenFailure::Token(err)) if is_terminal_token_error(&err) => {
                        self.terminate_all(err);
                    }
                    Err(OpenFailure::Token(err) | OpenFailure::Socket(err)) => {
                        self.report_all(&err);
                        self.schedule_reconnect();
                    }
                }
            }
            Internal::Frame { conn, frame } => self.on_frame(conn, frame),
            Internal::Closed { conn, code, reason } => self.on_closed(conn, code, &reason),
            Internal::SubscribeToken {
                conn,
                request,
                result,
            } => self.on_subscribe_token(conn, request, result),
            Internal::RefreshToken {
                conn,
                account,
                sub,
                result,
            } => self.on_refresh_token(conn, account, sub, result),
            Internal::ReauthToken { conn, result } => {
                if self.conn_id() != Some(conn) {
                    return;
                }
                match result {
                    Ok(token) => self.send(client_message::Op::from(Auth {
                        token,
                        ..Default::default()
                    })),
                    Err(err) if is_terminal_token_error(&err) => self.terminate_all(err),
                    Err(err) => self.report_all(&err),
                }
            }
        }
    }

    fn attach(&mut self, token: String, mut sink: WsSink, mut stream: WsStream) {
        self.next_conn += 1;
        let id = self.next_conn;
        let (out, mut out_rx) = mpsc::unbounded_channel::<Message>();
        let writer_internal = self.internal_tx.clone();
        tokio::spawn(async move {
            while let Some(message) = out_rx.recv().await {
                let closing = matches!(message, Message::Close(_));
                let reason = match tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)).await {
                    Ok(Ok(())) if closing => break,
                    Ok(Ok(())) => continue,
                    Ok(Err(err)) => format!("write failed: {err}"),
                    Err(_) => "write timed out".to_owned(),
                };
                // A socket that cannot be written is dead even while frames still
                // arrive: report it so the stream reconnects.
                let _ = writer_internal.send(Internal::Closed {
                    conn: id,
                    code: ABNORMAL_CLOSE_CODE,
                    reason,
                });
                return;
            }
            let _ = tokio::time::timeout(WRITE_TIMEOUT, sink.close()).await;
        });
        let internal = self.internal_tx.clone();
        let reader = tokio::spawn(async move {
            let (code, reason) = loop {
                match stream.next().await {
                    Some(Ok(frame @ (Message::Binary(_) | Message::Text(_)))) => {
                        if internal.send(Internal::Frame { conn: id, frame }).is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break frame.map_or((ABNORMAL_CLOSE_CODE, String::new()), |frame| {
                            (u16::from(frame.code), frame.reason.to_string())
                        });
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => break (ABNORMAL_CLOSE_CODE, err.to_string()),
                    None => break (ABNORMAL_CLOSE_CODE, String::new()),
                }
            };
            let _ = internal.send(Internal::Closed {
                conn: id,
                code,
                reason,
            });
        });
        self.conn = Some(Connection {
            id,
            out,
            reader,
            opened_at: Instant::now(),
        });
        self.send(client_message::Op::from(Auth {
            token,
            ..Default::default()
        }));
        self.arm_heartbeat();
        let accounts: Vec<u64> = self.subs.keys().copied().collect();
        for account in accounts {
            self.subscribe(account);
        }
    }

    fn conn_id(&self) -> Option<u64> {
        self.conn.as_ref().map(|conn| conn.id)
    }

    /// Queues a frame on the current socket. A socket whose writer has ended
    /// is closed once the current event is handled.
    fn send(&mut self, op: client_message::Op) {
        if let Some(conn) = &self.conn {
            let message = ClientMessage {
                op: Some(op),
                ..Default::default()
            };
            if conn
                .out
                .send(Message::Binary(message.encode_to_vec().into()))
                .is_err()
            {
                self.unwritable = Some(conn.id);
            }
        }
    }

    fn next_request_id(&mut self) -> String {
        self.next_request += 1;
        self.next_request.to_string()
    }

    fn subscribe(&mut self, account: u64) {
        let (Some(conn), Some(s)) = (self.conn_id(), self.subs.get(&account)) else {
            return;
        };
        let channel = s.channel.clone();
        let subscription = s.id;
        let request = self.next_request_id();
        self.pending.insert(
            request.clone(),
            PendingRequest {
                kind: RequestKind::Subscribe,
                account,
                subscription,
            },
        );
        let token = self.transport.subscribe_token(channel);
        let internal = self.internal_tx.clone();
        tokio::spawn(async move {
            let result = token.await;
            let _ = internal.send(Internal::SubscribeToken {
                conn,
                request,
                result,
            });
        });
    }

    fn on_subscribe_token(&mut self, conn: u64, request: String, result: Result<String>) {
        let Some(pending) = self.pending.get(&request).copied() else {
            return;
        };
        match result {
            Err(err) => {
                self.pending.remove(&request);
                if is_terminal_token_error(&err) {
                    self.terminate(pending.account, err);
                } else {
                    self.report(pending.account, &err);
                    self.retry_subscribe(pending.account);
                }
            }
            Ok(token) => {
                if self.conn_id() != Some(conn) {
                    return;
                }
                let cursor = self
                    .subs
                    .get(&pending.account)
                    .map(|s| s.cursor.clone())
                    .unwrap_or_default();
                self.send(client_message::Op::from(Subscribe {
                    id: request,
                    channel: Channel::ORDERS.into(),
                    account: pending.account,
                    token,
                    cursor,
                    ..Default::default()
                }));
            }
        }
    }

    /// Subscribes again after a backoff that grows until the subscription delivers data.
    fn retry_subscribe(&mut self, account: u64) {
        if let Some(s) = self.subs.get_mut(&account) {
            let delay = backoff(s.retries, &mut self.jitter);
            s.retries += 1;
            s.retry_at = Some(Instant::now() + delay);
        }
    }

    fn refresh(&mut self, account: u64) {
        let (Some(conn), Some(s)) = (self.conn_id(), self.subs.get(&account)) else {
            return;
        };
        let sub = s.sub.clone();
        let token = self.transport.subscribe_token(s.channel.clone());
        let internal = self.internal_tx.clone();
        tokio::spawn(async move {
            let result = token.await;
            let _ = internal.send(Internal::RefreshToken {
                conn,
                account,
                sub,
                result,
            });
        });
    }

    fn on_refresh_token(&mut self, conn: u64, account: u64, sub: String, result: Result<String>) {
        let Some(s) = self.subs.get(&account) else {
            return;
        };
        if self.conn_id() != Some(conn) || s.sub != sub {
            return;
        }
        let subscription = s.id;
        match result {
            // An unrefreshed subscription ends with TOKEN_EXPIRED and resubscribes.
            Err(err) => self.report(account, &err),
            Ok(token) => {
                let request = self.next_request_id();
                self.pending.insert(
                    request.clone(),
                    PendingRequest {
                        kind: RequestKind::Refresh,
                        account,
                        subscription,
                    },
                );
                self.send(client_message::Op::from(SubscribeRefresh {
                    id: request,
                    sub,
                    token,
                    ..Default::default()
                }));
            }
        }
    }

    /// Drops the server subscription and subscribes again from the last applied
    /// cursor: at once the first time, then with backoff until it delivers data.
    fn resubscribe(&mut self, account: u64) {
        let Some(s) = self.subs.get_mut(&account) else {
            return;
        };
        let sub = std::mem::take(&mut s.sub);
        s.seq = 0;
        s.ready = false;
        s.staged = None;
        let retry = s.retries > 0;
        if !retry {
            s.retries = 1;
        }
        // Consumers see the gap as a close and the replacement's Subscribed as a reopen.
        let notify = s.state == State::Connected;
        if notify {
            s.state = State::Disconnected;
        }
        if !sub.is_empty() {
            self.by_sub.remove(&sub);
            self.unsubscribe(sub);
        }
        if retry {
            self.retry_subscribe(account);
        } else {
            self.subscribe(account);
        }
        if notify {
            self.deliver(account, PrivateOrdersEvent::Disconnected);
        }
    }

    fn unsubscribe(&mut self, sub: String) {
        let request = self.next_request_id();
        self.unsubscribing.insert(request.clone());
        self.send(client_message::Op::from(Unsubscribe {
            id: request,
            sub,
            ..Default::default()
        }));
    }

    fn on_frame(&mut self, conn: u64, frame: Message) {
        use server_message::Frame;
        if self.conn_id() != Some(conn) {
            return;
        }
        self.arm_heartbeat();
        let Message::Binary(bytes) = frame else {
            self.report_all(&Error::realtime(
                "expected a binary private WebSocket frame",
            ));
            return;
        };
        let msg = match ServerMessage::decode_from_slice(&bytes) {
            Ok(msg) => msg,
            Err(err) => {
                self.report_all(&Error::realtime(format!(
                    "private WebSocket frame decode: {err}"
                )));
                return;
            }
        };
        let decoded = match decode(&msg) {
            Ok(decoded) => decoded,
            Err(err) => {
                self.report_all(&err);
                return;
            }
        };

        match msg.frame.as_ref() {
            Some(Frame::Subscribed(subscribed)) => {
                let request = self
                    .pending
                    .get(&subscribed.id)
                    .copied()
                    .filter(|request| request.kind == RequestKind::Subscribe);
                let Some(request) = request else {
                    // Its consumers left before the server confirmed it.
                    if !msg.sub.is_empty() {
                        self.unsubscribe(msg.sub.clone());
                    }
                    return;
                };
                self.pending.remove(&subscribed.id);
                let Some(s) = self.subs.get_mut(&request.account) else {
                    return;
                };
                s.sub = msg.sub.clone();
                s.seq = 0;
                s.ready = false;
                let connected = s.state != State::Connected;
                s.state = State::Connected;
                // A new subscription reports Unavailable itself if its view is still behind.
                let cleared = std::mem::take(&mut s.stale);
                self.by_sub.insert(msg.sub.clone(), request.account);
                if connected {
                    self.deliver(request.account, PrivateOrdersEvent::Connected);
                }
                if cleared {
                    self.deliver(request.account, PrivateOrdersEvent::Stale(false));
                }
                return;
            }
            Some(Frame::SubscribeRefreshed(refreshed)) => {
                self.pending.remove(&refreshed.id);
                return;
            }
            Some(Frame::Unsubscribed(unsubscribed)) => {
                if self.unsubscribing.remove(&unsubscribed.id) {
                    return;
                }
            }
            Some(Frame::Error(error)) => {
                self.on_request_error(&msg.sub, error);
                return;
            }
            Some(Frame::TokenExpiring(_)) => {
                self.reauthenticate();
                return;
            }
            _ => {}
        }

        let Some(&account) = self.by_sub.get(&msg.sub) else {
            return;
        };
        let Some(s) = self.subs.get_mut(&account) else {
            return;
        };
        if msg.seq > 0 {
            if msg.seq != s.seq + 1 {
                self.resubscribe(account);
                return;
            }
            s.seq = msg.seq;
        }

        match (msg.frame.as_ref(), decoded) {
            (Some(Frame::SnapshotBegin(_)), _) => s.staged = Some(Vec::new()),
            (Some(Frame::SnapshotPage(_)), Decoded::Page(orders)) => {
                if let Some(staged) = s.staged.as_mut() {
                    staged.extend(orders);
                }
            }
            (Some(Frame::SnapshotEnd(end)), _) => {
                let staged = s.staged.take();
                let Some(staged) = staged.filter(|staged| staged.len() == end.count as usize)
                else {
                    s.cursor.clear();
                    self.resubscribe(account);
                    return;
                };
                s.cursor = end.cursor.clone();
                s.retries = 0;
                let orders: Vec<Order> = staged.iter().map(|(_, order)| order.clone()).collect();
                s.orders = Some(staged.into_iter().collect());
                self.deliver(account, PrivateOrdersEvent::Snapshot(orders));
            }
            (Some(Frame::Unit(unit)), Decoded::Unit(ids, removed, decoded)) => {
                s.cursor = unit.next_cursor.clone();
                s.retries = 0;
                if let Some(live) = s.orders.as_mut() {
                    for (id, order) in ids.into_iter().zip(&decoded.orders) {
                        live.insert(id, order.clone());
                    }
                    for id in removed {
                        live.remove(&id);
                    }
                }
                self.deliver(account, PrivateOrdersEvent::Unit(decoded));
            }
            (Some(Frame::Covered(covered)), _) => {
                s.cursor = covered.cursor.clone();
                s.retries = 0;
            }
            // A resumed subscription on a quiet account delivers no data, only this.
            (Some(Frame::Ready(_)), _) => {
                s.retries = 0;
                if !std::mem::replace(&mut s.ready, true) {
                    self.deliver(account, PrivateOrdersEvent::Ready);
                }
            }
            (Some(Frame::Unavailable(_)), _) => {
                s.stale = true;
                self.deliver(account, PrivateOrdersEvent::Stale(true));
            }
            (Some(Frame::Available(_)), _) => {
                s.stale = false;
                self.deliver(account, PrivateOrdersEvent::Stale(false));
            }
            (Some(Frame::SubscribeExpiring(_)), _) => self.refresh(account),
            (Some(Frame::Unsubscribed(_)), _) => {
                // The server ended it: its token expired, or for a reason this client does not know.
                let sub = std::mem::take(&mut s.sub);
                self.by_sub.remove(&sub);
                self.resubscribe(account);
            }
            // ResyncRequired is followed by a snapshot that replaces the state;
            // frame kinds this client does not know are skipped after counting their seq.
            _ => {}
        }
    }

    fn on_request_error(&mut self, sub: &str, error: &crate::proto::privatews::v1::RequestError) {
        if !error.id.is_empty() && self.unsubscribing.remove(&error.id) {
            return;
        }
        let request = if error.id.is_empty() {
            None
        } else {
            match self.pending.remove(&error.id) {
                Some(request) => Some(request),
                // The answer to a request already answered, or whose subscription has since gone.
                None => return,
            }
        };
        let code = error
            .code
            .as_known()
            .map(|code| code.proto_name().to_owned())
            .unwrap_or_else(|| format!("UNKNOWN({})", error.code.to_i32()));
        let err = Error::Api {
            message: if error.message.is_empty() {
                format!("private order stream request failed: {code}")
            } else {
                error.message.clone()
            },
            code,
            metadata: Vec::new(),
        };
        let account = request
            .map(|request| request.account)
            .or_else(|| self.by_sub.get(sub).copied());
        let Some(account) = account else {
            self.report_all(&err);
            return;
        };
        let known = error.code.as_known();
        match request.map(|request| request.kind) {
            Some(RequestKind::Subscribe) => {
                if known == Some(request_error::Code::CURSOR_INVALID) {
                    if let Some(s) = self.subs.get_mut(&account) {
                        s.cursor.clear();
                    }
                    self.subscribe(account);
                } else {
                    self.terminate(account, err);
                }
            }
            _ if known == Some(request_error::Code::SUBSCRIPTION_UNKNOWN)
                && self.subs.get(&account).is_some_and(|s| s.sub == sub) =>
            {
                if let Some(s) = self.subs.get_mut(&account) {
                    s.sub.clear();
                }
                self.by_sub.remove(sub);
                self.resubscribe(account);
            }
            _ => self.report(account, &err),
        }
    }

    fn reauthenticate(&mut self) {
        let Some(conn) = self.conn_id() else {
            return;
        };
        let token = self.transport.connect_token();
        let internal = self.internal_tx.clone();
        tokio::spawn(async move {
            let result = token.await;
            let _ = internal.send(Internal::ReauthToken { conn, result });
        });
    }

    fn on_closed(&mut self, conn: u64, code: u16, reason: &str) {
        let Some(current) = &self.conn else {
            return;
        };
        if current.id != conn {
            return;
        }
        let stable = current.opened_at.elapsed() >= STABLE_CONNECTION;
        self.detach();
        if stable {
            self.reconnect_attempt = 0;
        }
        if self.subs.is_empty() {
            return;
        }
        if TERMINAL_CLOSE_CODES.contains(&code) {
            let reason = if reason.is_empty() {
                String::new()
            } else {
                format!(": {reason}")
            };
            self.terminate_all(Error::realtime(format!(
                "private WebSocket closed with code {code}{reason}"
            )));
            return;
        }
        self.schedule_reconnect();
        // A close that reconnects is not an error.
        let accounts: Vec<u64> = self.subs.keys().copied().collect();
        for account in accounts {
            if let Some(s) = self.subs.get_mut(&account)
                && s.state == State::Connected
            {
                s.state = State::Disconnected;
                self.deliver(account, PrivateOrdersEvent::Disconnected);
            }
        }
    }

    fn schedule_reconnect(&mut self) {
        let delay = backoff(self.reconnect_attempt, &mut self.jitter);
        self.reconnect_attempt += 1;
        self.reconnect_at = Some(Instant::now() + delay);
    }

    fn arm_heartbeat(&mut self) {
        self.heartbeat_at = Some(Instant::now() + HEARTBEAT_TIMEOUT);
    }

    fn on_timers(&mut self) {
        let now = Instant::now();
        if self.heartbeat_at.is_some_and(|at| at <= now) {
            self.heartbeat_at = None;
            if let Some(conn) = self.conn_id() {
                // A stale socket may not report its close for a long time: treat it as closed now.
                self.close_socket(HEARTBEAT_CLOSE_CODE, "heartbeat timeout");
                self.on_closed(conn, HEARTBEAT_CLOSE_CODE, "heartbeat timeout");
            }
        }
        if self.reconnect_at.is_some_and(|at| at <= now) {
            self.reconnect_at = None;
            if !self.subs.is_empty() && self.conn.is_none() && !self.connecting {
                self.open();
            }
        }
        let due: Vec<u64> = self
            .subs
            .iter()
            .filter(|(_, s)| s.retry_at.is_some_and(|at| at <= now))
            .map(|(account, _)| *account)
            .collect();
        for account in due {
            if let Some(s) = self.subs.get_mut(&account) {
                s.retry_at = None;
                if s.sub.is_empty() {
                    self.subscribe(account);
                }
            }
        }
    }

    fn close_socket(&self, code: u16, reason: &'static str) {
        if let Some(conn) = &self.conn {
            let _ = conn.out.send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })));
        }
    }

    fn detach(&mut self) {
        self.conn = None;
        self.heartbeat_at = None;
        self.pending.clear();
        self.by_sub.clear();
        self.unsubscribing.clear();
        for s in self.subs.values_mut() {
            s.retry_at = None;
            s.sub.clear();
            s.seq = 0;
            s.ready = false;
            s.staged = None;
        }
    }

    fn shutdown(&mut self) {
        self.close_socket(1000, "");
        self.detach();
        self.generation += 1;
        self.connecting = false;
        self.reconnect_at = None;
        self.reconnect_attempt = 0;
    }

    fn teardown_idle(&mut self) {
        for account in std::mem::take(&mut self.idle) {
            if self
                .subs
                .get(&account)
                .is_some_and(|s| s.consumers.is_empty())
            {
                self.teardown(account);
            }
        }
    }

    fn teardown(&mut self, account: u64) -> Option<Subscription> {
        let mut s = self.subs.remove(&account)?;
        s.retry_at = None;
        let sub = std::mem::take(&mut s.sub);
        if !sub.is_empty() {
            self.by_sub.remove(&sub);
            if self.conn.is_some() {
                self.unsubscribe(sub);
            }
        }
        self.pending
            .retain(|_, request| request.subscription != s.id);
        if self.subs.is_empty() {
            self.shutdown();
        }
        Some(s)
    }

    /// Reports `error`, then ends the account's stream for every consumer.
    fn terminate(&mut self, account: u64, error: Error) {
        self.report(account, &error);
        let Some(s) = self.teardown(account) else {
            return;
        };
        let told = s.state == State::Disconnected;
        for consumer in s.consumers {
            *super::lock_unpoisoned(&consumer.ended) = Some(error.clone());
            if !told {
                let _ = consumer.tx.try_send(PrivateOrdersEvent::Disconnected);
            }
        }
    }

    fn terminate_all(&mut self, error: Error) {
        let accounts: Vec<u64> = self.subs.keys().copied().collect();
        for account in accounts {
            self.terminate(account, error.clone());
        }
    }

    fn report(&mut self, account: u64, error: &Error) {
        self.deliver(account, PrivateOrdersEvent::Error(error.clone()));
    }

    fn report_all(&mut self, error: &Error) {
        let accounts: Vec<u64> = self.subs.keys().copied().collect();
        for account in accounts {
            self.report(account, error);
        }
    }

    /// Delivers to every consumer of the account. A consumer whose queue is
    /// full leaves the stream with a queue overflow instead of losing events.
    fn deliver(&mut self, account: u64, event: PrivateOrdersEvent) {
        let Some(s) = self.subs.get_mut(&account) else {
            return;
        };
        s.consumers
            .retain(|consumer| offer(consumer, event.clone()));
        if s.consumers.is_empty() {
            self.idle.insert(account);
        }
    }
}

/// Queues an event for a consumer; false when the consumer must leave the stream.
fn offer(consumer: &Consumer, event: PrivateOrdersEvent) -> bool {
    match consumer.tx.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            *super::lock_unpoisoned(&consumer.ended) = Some(Error::queue_overflow(
                "private order stream queue full; consumer too slow",
            ));
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests;
