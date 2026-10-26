//! The order stream against a local fake private WebSocket server.
//!
//! Each socket is a real WebSocket over an in-process duplex pipe, and time is
//! paused: Tokio advances the clock only once every task waits, so heartbeat,
//! backoff and token timing are deterministic and the tests take no wall time.

use super::*;
use crate::codecs::decode::order_from_proto;
use crate::codecs::scalars::format_id;
use crate::models::{OrderStreamEvent, OrderUnitEvent};
use crate::proto::orders::v1::{Order as ProtoOrder, OrderStatus};
use crate::proto::privatews::v1::{
    Available, Covered, Ready, RequestError, ResyncRequired, SnapshotBegin, SnapshotEnd,
    SnapshotPage, SubscribeExpiring, SubscribeRefreshed, Subscribed, TokenExpiring, Unavailable,
    Unit, UnitEvent, order_event, resync_required, server_message::Frame,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::DuplexStream;
use tokio_tungstenite::WebSocketStream;

const ACCOUNT: u64 = 42;
const WAIT: Duration = Duration::from_secs(60);

struct FakeState {
    servers: mpsc::UnboundedSender<FakeServer>,
    connect_tokens: AtomicUsize,
    subscribe_tokens: AtomicUsize,
    subscribe_channels: Mutex<Vec<String>>,
    subscribe_error: Mutex<Option<Error>>,
    /// How the client's outgoing writes on the current socket behave.
    writes: Arc<Mutex<Writes>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Writes {
    Pass,
    /// Every write waits forever, as on a peer that stopped reading.
    Block,
    /// Every write fails, as on a reset connection.
    Fail,
}

/// The client's sink, with writes that can be made to block or fail.
struct ControlledSink {
    inner: WsSink,
    writes: Arc<Mutex<Writes>>,
}

impl Sink<Message> for ControlledSink {
    type Error = tungstenite::Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let writes = *self.writes.lock().unwrap();
        match writes {
            Writes::Pass => self.inner.as_mut().poll_ready(cx),
            Writes::Block => std::task::Poll::Pending,
            Writes::Fail => std::task::Poll::Ready(Err(tungstenite::Error::ConnectionClosed)),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> std::result::Result<(), Self::Error> {
        self.inner.as_mut().start_send(item)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.as_mut().poll_flush(cx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.as_mut().poll_close(cx)
    }
}

#[derive(Clone)]
struct FakeTransport(Arc<FakeState>);

impl Transport for FakeTransport {
    fn connect_token(&self) -> BoxFuture<Result<String>> {
        let n = self.0.connect_tokens.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move { Ok(format!("connect-{n}")) })
    }

    fn subscribe_token(&self, channel: String) -> BoxFuture<Result<String>> {
        self.0.subscribe_channels.lock().unwrap().push(channel);
        let n = self.0.subscribe_tokens.fetch_add(1, Ordering::SeqCst) + 1;
        let error = self.0.subscribe_error.lock().unwrap().take();
        Box::pin(async move {
            match error {
                Some(error) => Err(error),
                None => Ok(format!("sub-{n}")),
            }
        })
    }

    fn open(&self) -> BoxFuture<Result<(WsSink, WsStream)>> {
        let servers = self.0.servers.clone();
        let writes = self.0.writes.clone();
        *writes.lock().unwrap() = Writes::Pass;
        Box::pin(async move {
            let (client_io, server_io) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(server_io)
                    .await
                    .expect("server handshake");
                let _ = servers.send(FakeServer { ws });
            });
            let (ws, _) = tokio_tungstenite::client_async("ws://fake/v1/ws/private", client_io)
                .await
                .map_err(|e| Error::realtime(e.to_string()))?;
            let (sink, stream) = ws.split();
            let sink = ControlledSink {
                inner: Box::pin(sink),
                writes,
            };
            Ok((Box::pin(sink) as WsSink, Box::pin(stream) as WsStream))
        })
    }
}

struct FakeServer {
    ws: WebSocketStream<DuplexStream>,
}

impl FakeServer {
    /// The next client operation.
    async fn recv(&mut self) -> client_message::Op {
        loop {
            let frame = tokio::time::timeout(WAIT, self.ws.next())
                .await
                .expect("client frame in time")
                .expect("socket open")
                .expect("readable frame");
            match frame {
                Message::Binary(bytes) => {
                    return ClientMessage::decode_from_slice(&bytes)
                        .unwrap()
                        .op
                        .expect("an operation");
                }
                Message::Close(frame) => panic!("client closed the socket: {frame:?}"),
                _ => {}
            }
        }
    }

    /// The close code the client sends next, skipping other frames.
    async fn closed(&mut self) -> u16 {
        loop {
            match tokio::time::timeout(WAIT, self.ws.next())
                .await
                .expect("close in time")
            {
                Some(Ok(Message::Close(frame))) => {
                    return frame.map_or(1005, |frame| u16::from(frame.code));
                }
                Some(Ok(_)) => {}
                other => panic!("socket ended without a close frame: {other:?}"),
            }
        }
    }

    async fn auth(&mut self) -> String {
        match self.recv().await {
            client_message::Op::Auth(auth) => auth.token,
            other => panic!("expected auth, got {other:?}"),
        }
    }

    async fn subscribe(&mut self) -> Subscribe {
        match self.recv().await {
            client_message::Op::Subscribe(subscribe) => *subscribe,
            other => panic!("expected subscribe, got {other:?}"),
        }
    }

    async fn send(&mut self, sub: &str, seq: u64, frame: impl Into<Frame>) {
        let msg = ServerMessage {
            sub: sub.into(),
            seq,
            frame: Some(frame.into()),
            ..Default::default()
        };
        self.send_raw(msg.encode_to_vec()).await;
    }

    async fn send_raw(&mut self, bytes: Vec<u8>) {
        self.ws.send(Message::Binary(bytes.into())).await.unwrap();
    }

    async fn close(&mut self, code: u16) {
        self.ws
            .close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: "".into(),
            }))
            .await
            .unwrap();
    }

    /// Confirms a subscribe request as `sub`.
    async fn confirm(&mut self, request: &Subscribe, sub: &str) {
        self.send(
            sub,
            0,
            Subscribed {
                id: request.id.clone(),
                account: request.account,
                expires_ts_sec: 1_761_123_456,
                ..Default::default()
            },
        )
        .await;
    }

    /// Sends a one-page snapshot as seq `first..first+3` and returns the next seq.
    async fn snapshot(&mut self, sub: &str, first: u64, cursor: &str, ids: &[u64]) -> u64 {
        self.send(sub, first, SnapshotBegin::default()).await;
        self.send(
            sub,
            first + 1,
            SnapshotPage {
                orders: ids.iter().map(|id| proto_order(*id, 1)).collect(),
                ..Default::default()
            },
        )
        .await;
        self.send(
            sub,
            first + 2,
            SnapshotEnd {
                cursor: cursor.into(),
                count: ids.len() as u32,
                ..Default::default()
            },
        )
        .await;
        first + 3
    }
}

struct Harness {
    streams: PrivateOrderStreams,
    transport: FakeTransport,
    servers: mpsc::UnboundedReceiver<FakeServer>,
}

impl Harness {
    fn new() -> Self {
        Self::with_queue(64)
    }

    fn with_queue(max_queue: usize) -> Self {
        let (servers_tx, servers) = mpsc::unbounded_channel();
        let transport = FakeTransport(Arc::new(FakeState {
            servers: servers_tx,
            connect_tokens: AtomicUsize::new(0),
            subscribe_tokens: AtomicUsize::new(0),
            subscribe_channels: Mutex::new(Vec::new()),
            subscribe_error: Mutex::new(None),
            writes: Arc::new(Mutex::new(Writes::Pass)),
        }));
        Self {
            streams: PrivateOrderStreams::new(max_queue),
            transport,
            servers,
        }
    }

    fn connect(&self, account: u64) -> PrivateOrdersSubscription {
        let transport = self.transport.clone();
        self.streams
            .connect(account, move || Arc::new(transport) as Arc<dyn Transport>)
    }

    async fn server(&mut self) -> FakeServer {
        tokio::time::timeout(WAIT, self.servers.recv())
            .await
            .expect("a socket in time")
            .expect("transport alive")
    }

    /// Accepts the next socket, checks auth, and confirms its subscribe request as `sub`.
    async fn accept(&mut self, sub: &str) -> (FakeServer, Subscribe) {
        let mut server = self.server().await;
        assert!(server.auth().await.starts_with("connect-"));
        let request = server.subscribe().await;
        server.confirm(&request, sub).await;
        (server, request)
    }

    fn connect_tokens(&self) -> usize {
        self.transport.0.connect_tokens.load(Ordering::SeqCst)
    }

    fn subscribe_channels(&self) -> Vec<String> {
        self.transport.0.subscribe_channels.lock().unwrap().clone()
    }
}

fn proto_order(id: u64, version: u32) -> ProtoOrder {
    ProtoOrder {
        order_id: id,
        symbol_id: 1,
        status: OrderStatus::Working.into(),
        price_ticks: 2_420_000_000_000,
        version,
        ..Default::default()
    }
}

fn order(id: u64, version: u32) -> Order {
    order_from_proto(&proto_order(id, version)).unwrap()
}

async fn next(consumer: &mut PrivateOrdersSubscription) -> PrivateOrdersEvent {
    tokio::time::timeout(WAIT, consumer.recv())
        .await
        .expect("an event in time")
        .expect("stream open")
}

async fn expect_connected(consumer: &mut PrivateOrdersSubscription) {
    match next(consumer).await {
        PrivateOrdersEvent::Connected => {}
        other => panic!("expected Connected, got {other:?}"),
    }
}

async fn expect_disconnected(consumer: &mut PrivateOrdersSubscription) {
    match next(consumer).await {
        PrivateOrdersEvent::Disconnected => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

async fn expect_snapshot(consumer: &mut PrivateOrdersSubscription) -> Vec<Order> {
    match next(consumer).await {
        PrivateOrdersEvent::Snapshot(orders) => orders,
        other => panic!("expected Snapshot, got {other:?}"),
    }
}

async fn expect_unit(consumer: &mut PrivateOrdersSubscription) -> OrdersUnit {
    match next(consumer).await {
        PrivateOrdersEvent::Unit(unit) => unit,
        other => panic!("expected Unit, got {other:?}"),
    }
}

async fn expect_ready(consumer: &mut PrivateOrdersSubscription) {
    match next(consumer).await {
        PrivateOrdersEvent::Ready => {}
        other => panic!("expected Ready, got {other:?}"),
    }
}

async fn expect_stale(consumer: &mut PrivateOrdersSubscription) -> bool {
    match next(consumer).await {
        PrivateOrdersEvent::Stale(stale) => stale,
        other => panic!("expected Stale, got {other:?}"),
    }
}

async fn expect_error(consumer: &mut PrivateOrdersSubscription) -> Error {
    match next(consumer).await {
        PrivateOrdersEvent::Error(error) => error,
        other => panic!("expected Error, got {other:?}"),
    }
}

async fn expect_end(consumer: &mut PrivateOrdersSubscription) {
    let end = tokio::time::timeout(WAIT, consumer.recv())
        .await
        .expect("the stream ends in time");
    assert!(end.is_none(), "expected the end of the stream, got {end:?}");
}

/// No event arrives while the stream sits idle for `idle`.
async fn expect_quiet(consumer: &mut PrivateOrdersSubscription, idle: Duration) {
    if let Ok(event) = tokio::time::timeout(idle, consumer.recv()).await {
        panic!("expected no event, got {event:?}");
    }
}

fn unit(prev: &str, next_cursor: &str, ids: &[u64]) -> Unit {
    Unit {
        prev_cursor: prev.into(),
        next_cursor: next_cursor.into(),
        orders: ids.iter().map(|id| proto_order(*id, 2)).collect(),
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn authenticates_first_then_subscribes_with_the_base_channel_token() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let mut server = harness.server().await;

    assert_eq!(server.auth().await, "connect-1");
    let request = server.subscribe().await;
    assert_eq!(request.channel.as_known(), Some(Channel::ORDERS));
    assert_eq!(request.account, ACCOUNT);
    assert_eq!(request.token, "sub-1");
    assert_eq!(request.cursor, "");
    // The subscribe token is for the base channel: no retired `:proto` suffix.
    assert_eq!(
        harness.subscribe_channels(),
        vec![format!("private:spot:orders:{}", format_id(ACCOUNT))]
    );

    server.confirm(&request, "s1").await;
    expect_connected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn installs_a_paged_snapshot_only_at_snapshot_end() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;

    server
        .send(
            "s1",
            1,
            SnapshotBegin {
                epoch: 3,
                ..Default::default()
            },
        )
        .await;
    for (seq, id) in [(2, 1), (3, 2)] {
        server
            .send(
                "s1",
                seq,
                SnapshotPage {
                    orders: vec![proto_order(id, 1)],
                    ..Default::default()
                },
            )
            .await;
    }
    // Nothing is delivered while the snapshot is staged.
    expect_quiet(&mut consumer, Duration::from_millis(500)).await;
    server
        .send(
            "s1",
            4,
            SnapshotEnd {
                cursor: "c1".into(),
                count: 2,
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        expect_snapshot(&mut consumer).await,
        vec![order(1, 1), order(2, 1)]
    );
}

#[tokio::test(start_paused = true)]
async fn delivers_units_with_orders_removals_and_events() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1, 2]).await;
    expect_snapshot(&mut consumer).await;

    let mut change = unit("c1", "c2", &[1]);
    change.removed_order_ids = vec![2];
    change.events = vec![UnitEvent {
        kind: Some(
            crate::proto::privatews::v1::OrderEvent {
                order_id: 1,
                version: 2,
                kind: order_event::Kind::CANCELED.into(),
                reason_code: 7,
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    }];
    server.send("s1", seq, change).await;

    let delivered = expect_unit(&mut consumer).await;
    assert_eq!(delivered.orders, vec![order(1, 2)]);
    assert_eq!(delivered.removed_order_ids, vec![format_uint64(2)]);
    assert_eq!(
        delivered.events,
        vec![OrderUnitEvent::Order(OrderStreamEvent {
            order_id: format_uint64(1),
            version: 2,
            kind: "canceled".into(),
            reason_code: 7,
            replacement_order_id: None,
            fill: None,
        })]
    );

    // A consumer that joins now catches up on the applied view: order 1 at
    // version 2, order 2 removed.
    let mut late = harness.connect(ACCOUNT);
    expect_connected(&mut late).await;
    assert_eq!(expect_snapshot(&mut late).await, vec![order(1, 2)]);
}

fn format_uint64(id: u64) -> String {
    crate::codecs::scalars::format_uint64_id(id)
}

#[tokio::test(start_paused = true)]
async fn a_sequence_gap_resubscribes_from_the_last_applied_cursor() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    // seq 5 skips seq 4.
    server.send("s1", seq + 1, unit("c1", "c9", &[1])).await;
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s1"),
        other => panic!("expected unsubscribe, got {other:?}"),
    }
    let request = server.subscribe().await;
    assert_eq!(request.cursor, "c1");
    expect_disconnected(&mut consumer).await;

    // Frames for the dropped subscription are ignored.
    server.send("s1", seq + 2, unit("c9", "c10", &[1])).await;
    server.confirm(&request, "s2").await;
    expect_connected(&mut consumer).await;
    server.send("s2", 1, unit("c1", "c2", &[1])).await;
    assert_eq!(expect_unit(&mut consumer).await.orders, vec![order(1, 2)]);
}

#[tokio::test(start_paused = true)]
async fn an_unknown_frame_kind_is_skipped_but_counted() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    // A frame kind from a newer contract: field 999, which this client does not know.
    let mut bytes = ServerMessage {
        sub: "s1".into(),
        seq,
        ..Default::default()
    }
    .encode_to_vec();
    bytes.extend_from_slice(&[0xBA, 0x3E, 0x00]);
    server.send_raw(bytes).await;
    server.send("s1", seq + 1, unit("c1", "c2", &[1])).await;

    assert_eq!(expect_unit(&mut consumer).await.orders, vec![order(1, 2)]);
    // No resubscribe: the next client frame is the unsubscribe when the consumer leaves.
    drop(consumer);
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s1"),
        other => panic!("expected the closing unsubscribe, got {other:?}"),
    }
    assert_eq!(server.closed().await, 1000);
}

#[tokio::test(start_paused = true)]
async fn resync_required_replaces_the_state_with_the_next_snapshot() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1, 2]).await;
    assert_eq!(expect_snapshot(&mut consumer).await.len(), 2);

    server
        .send(
            "s1",
            seq,
            ResyncRequired {
                reason: resync_required::Reason::CURSOR_EXPIRED.into(),
                ..Default::default()
            },
        )
        .await;
    server.snapshot("s1", seq + 1, "c5", &[3]).await;
    assert_eq!(expect_snapshot(&mut consumer).await, vec![order(3, 1)]);

    // The installed state is the new snapshot, and the cursor is its cursor.
    let mut late = harness.connect(ACCOUNT);
    expect_connected(&mut late).await;
    assert_eq!(expect_snapshot(&mut late).await, vec![order(3, 1)]);
    server.close(4408).await;
    expect_disconnected(&mut consumer).await;
    let mut server = harness.server().await;
    server.auth().await;
    assert_eq!(server.subscribe().await.cursor, "c5");
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_that_disagrees_with_its_count_is_taken_again_fresh() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    server.send("s1", 1, SnapshotBegin::default()).await;
    server
        .send(
            "s1",
            2,
            SnapshotEnd {
                cursor: "c1".into(),
                count: 1,
                ..Default::default()
            },
        )
        .await;
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s1"),
        other => panic!("expected unsubscribe, got {other:?}"),
    }
    assert_eq!(server.subscribe().await.cursor, "");
    expect_disconnected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn unavailable_and_available_toggle_the_stale_flag() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    server.send("s1", seq, Unavailable::default()).await;
    assert!(expect_stale(&mut consumer).await);
    // Ready may arrive while the view is stale: it describes the position delivered.
    server.send("s1", seq + 1, Ready::default()).await;
    expect_ready(&mut consumer).await;
    server
        .send(
            "s1",
            seq + 2,
            Covered {
                cursor: "c2".into(),
                ..Default::default()
            },
        )
        .await;
    server.send("s1", seq + 3, Available::default()).await;
    assert!(!expect_stale(&mut consumer).await);
}

#[tokio::test(start_paused = true)]
async fn subscribe_expiring_refreshes_with_a_fresh_subscribe_token() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;

    server.send("s1", 0, SubscribeExpiring::default()).await;
    let refresh = match server.recv().await {
        client_message::Op::SubscribeRefresh(refresh) => refresh,
        other => panic!("expected subscribeRefresh, got {other:?}"),
    };
    assert_eq!(refresh.sub, "s1");
    assert_eq!(refresh.token, "sub-2");
    let channel = format!("private:spot:orders:{}", format_id(ACCOUNT));
    assert_eq!(harness.subscribe_channels(), vec![channel.clone(), channel]);
    server
        .send(
            "s1",
            0,
            SubscribeRefreshed {
                id: refresh.id.clone(),
                ..Default::default()
            },
        )
        .await;
    // The subscription goes on undisturbed.
    server.snapshot("s1", 1, "c1", &[1]).await;
    assert_eq!(expect_snapshot(&mut consumer).await, vec![order(1, 1)]);
}

#[tokio::test(start_paused = true)]
async fn token_expiring_reauthenticates_with_a_fresh_connect_token() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;

    server
        .send(
            "",
            0,
            TokenExpiring {
                expires_ts_sec: 1_761_123_456,
                ..Default::default()
            },
        )
        .await;
    assert_eq!(server.auth().await, "connect-2");
    assert_eq!(harness.connect_tokens(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_heartbeat_timeout_reconnects_and_resumes_from_the_cursor() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    // Heartbeats keep the connection: 2.5 s apart is under the 3 s timeout.
    let heartbeat = crate::proto::privatews::v1::Heartbeat::default();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        server.send("", 0, heartbeat.clone()).await;
    }
    expect_quiet(&mut consumer, Duration::from_millis(2_900)).await;
    assert_eq!(harness.connect_tokens(), 1);

    // Then silence: the client drops the socket at 3 s and reconnects.
    let started = Instant::now();
    expect_disconnected(&mut consumer).await;
    assert_eq!(server.closed().await, HEARTBEAT_CLOSE_CODE);
    let mut server = harness.server().await;
    assert_eq!(server.auth().await, "connect-2");
    let request = server.subscribe().await;
    assert_eq!(request.cursor, "c1");
    // 0.5 to 1 s of backoff after the drop.
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(500), "{waited:?}");
    assert!(waited <= Duration::from_millis(1_100), "{waited:?}");
    server.confirm(&request, "s2").await;
    expect_connected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn close_4408_reconnects_and_resubscribes_with_the_cursor() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;
    server.send("s1", seq, unit("c1", "c2", &[1])).await;
    expect_unit(&mut consumer).await;

    server.close(4408).await;
    expect_disconnected(&mut consumer).await;
    let (_server, request) = harness.accept("s2").await;
    assert_eq!(request.cursor, "c2");
    expect_connected(&mut consumer).await;
    assert_eq!(harness.connect_tokens(), 2);
}

#[tokio::test(start_paused = true)]
async fn close_4403_is_terminal() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;

    server.close(4403).await;
    let error = expect_error(&mut consumer).await;
    assert!(matches!(error, Error::Realtime(_)), "{error:?}");
    assert!(error.to_string().contains("4403"), "{error}");
    expect_disconnected(&mut consumer).await;
    expect_end(&mut consumer).await;
    assert!(matches!(
        consumer.recv_result().await,
        Err(Error::Realtime(_))
    ));

    // No reconnect follows.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(harness.servers.try_recv().is_err());
    assert_eq!(harness.connect_tokens(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_forbidden_subscribe_is_surfaced_and_ends_the_stream() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let mut server = harness.server().await;
    server.auth().await;
    let request = server.subscribe().await;
    server
        .send(
            "",
            0,
            RequestError {
                id: request.id.clone(),
                code: request_error::Code::FORBIDDEN.into(),
                message: "not your account".into(),
                ..Default::default()
            },
        )
        .await;

    match expect_error(&mut consumer).await {
        Error::Api { code, message, .. } => {
            assert_eq!(code, "FORBIDDEN");
            assert_eq!(message, "not your account");
        }
        other => panic!("expected a FORBIDDEN api error, got {other:?}"),
    }
    expect_disconnected(&mut consumer).await;
    expect_end(&mut consumer).await;
    // The idle connection closes normally.
    assert_eq!(server.closed().await, 1000);
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_cursor_is_retried_fresh() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    server.close(1001).await;
    expect_disconnected(&mut consumer).await;
    let mut server = harness.server().await;
    server.auth().await;
    let request = server.subscribe().await;
    assert_eq!(request.cursor, "c1");
    server
        .send(
            "",
            0,
            RequestError {
                id: request.id.clone(),
                code: request_error::Code::CURSOR_INVALID.into(),
                ..Default::default()
            },
        )
        .await;
    let retry = server.subscribe().await;
    assert_eq!(retry.cursor, "");
    server.confirm(&retry, "s2").await;
    expect_connected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn consumers_of_one_account_share_one_server_subscription() {
    let mut harness = Harness::new();
    let mut first = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut first).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut first).await;
    server.send("s1", seq, Unavailable::default()).await;
    assert!(expect_stale(&mut first).await);

    // The second consumer catches up without a second server subscription.
    let mut second = harness.connect(ACCOUNT);
    expect_connected(&mut second).await;
    assert_eq!(expect_snapshot(&mut second).await, vec![order(1, 1)]);
    assert!(expect_stale(&mut second).await);
    assert_eq!(harness.subscribe_channels().len(), 1);

    server.send("s1", seq + 1, unit("c1", "c2", &[1])).await;
    assert_eq!(expect_unit(&mut first).await.orders, vec![order(1, 2)]);
    assert_eq!(expect_unit(&mut second).await.orders, vec![order(1, 2)]);

    // The server subscription ends with its last consumer, then the socket.
    drop(first);
    expect_quiet(&mut second, Duration::from_millis(100)).await;
    drop(second);
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s1"),
        other => panic!("expected unsubscribe, got {other:?}"),
    }
    assert_eq!(server.closed().await, 1000);
}

#[tokio::test(start_paused = true)]
async fn a_transient_subscribe_token_failure_is_reported_and_retried() {
    let mut harness = Harness::new();
    *harness.transport.0.subscribe_error.lock().unwrap() =
        Some(Error::realtime("token service unavailable"));
    let mut consumer = harness.connect(ACCOUNT);
    let mut server = harness.server().await;
    server.auth().await;
    assert!(matches!(
        expect_error(&mut consumer).await,
        Error::Realtime(_)
    ));
    let request = server.subscribe().await;
    assert_eq!(request.token, "sub-2");
    server.confirm(&request, "s1").await;
    expect_connected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn a_consumer_that_falls_behind_leaves_with_a_queue_overflow() {
    let mut harness = Harness::with_queue(1);
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    // Connected fills the queue of one; the snapshot overflows it.
    server.snapshot("s1", 1, "c1", &[1]).await;
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s1"),
        other => panic!("expected unsubscribe, got {other:?}"),
    }
    expect_connected(&mut consumer).await;
    assert!(matches!(
        consumer.recv_result().await,
        Err(Error::QueueOverflow(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn ready_follows_the_snapshot_once_per_server_subscription() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;
    server.send("s1", seq, Ready::default()).await;
    expect_ready(&mut consumer).await;

    // A repeated ready on the same server subscription is not delivered again.
    server.send("s1", seq + 1, Ready::default()).await;
    server.send("s1", seq + 2, unit("c1", "c2", &[1])).await;
    assert_eq!(expect_unit(&mut consumer).await.orders, vec![order(1, 2)]);
}

#[tokio::test(start_paused = true)]
async fn ready_is_delivered_again_after_a_reconnect() {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;
    server.send("s1", seq, Ready::default()).await;
    expect_ready(&mut consumer).await;

    server.close(1001).await;
    expect_disconnected(&mut consumer).await;
    let (mut server, request) = harness.accept("s2").await;
    assert_eq!(request.cursor, "c1");
    expect_connected(&mut consumer).await;
    // A resumed subscription sends units from the cursor, then ready.
    server.send("s2", 1, unit("c1", "c2", &[1])).await;
    expect_unit(&mut consumer).await;
    server.send("s2", 2, Ready::default()).await;
    expect_ready(&mut consumer).await;

    // A consumer joining a stream that is not ready yet after a resubscribe
    // waits for the new subscription's ready.
    server.send("s2", 4, unit("c2", "c3", &[1])).await;
    expect_disconnected(&mut consumer).await;
    match server.recv().await {
        client_message::Op::Unsubscribe(unsubscribe) => assert_eq!(unsubscribe.sub, "s2"),
        other => panic!("expected unsubscribe, got {other:?}"),
    }
    let retry = server.subscribe().await;
    assert_eq!(retry.cursor, "c2");
    let mut late = harness.connect(ACCOUNT);
    expect_disconnected_free_catch_up(&mut late).await;
    server.confirm(&retry, "s3").await;
    expect_connected(&mut consumer).await;
    expect_connected(&mut late).await;
    server.send("s3", 1, Ready::default()).await;
    expect_ready(&mut consumer).await;
    expect_ready(&mut late).await;
}

/// A consumer that joins while the subscription is between server
/// subscriptions catches up on the view only: no Connected and no Ready.
async fn expect_disconnected_free_catch_up(consumer: &mut PrivateOrdersSubscription) {
    assert_eq!(expect_snapshot(consumer).await, vec![order(1, 2)]);
    expect_quiet(consumer, Duration::from_millis(1)).await;
}

#[tokio::test(start_paused = true)]
async fn a_late_joiner_of_a_ready_stream_gets_snapshot_then_ready() {
    let mut harness = Harness::new();
    let mut first = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut first).await;
    let seq = server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut first).await;
    server.send("s1", seq, Ready::default()).await;
    expect_ready(&mut first).await;

    let mut late = harness.connect(ACCOUNT);
    expect_connected(&mut late).await;
    assert_eq!(expect_snapshot(&mut late).await, vec![order(1, 1)]);
    expect_ready(&mut late).await;
    expect_quiet(&mut first, Duration::from_millis(100)).await;
}

/// Makes the client's writes on the current socket block or fail, prompts a
/// client write, and keeps the old socket's heartbeats coming. The stream must
/// still drop the socket and resume from its cursor on a new one, within the
/// write timeout plus the first reconnect backoff.
async fn a_broken_write_path_reconnects(writes: Writes) {
    let mut harness = Harness::new();
    let mut consumer = harness.connect(ACCOUNT);
    let (mut server, _) = harness.accept("s1").await;
    expect_connected(&mut consumer).await;
    server.snapshot("s1", 1, "c1", &[1]).await;
    expect_snapshot(&mut consumer).await;

    *harness.transport.0.writes.lock().unwrap() = writes;
    let started = Instant::now();
    // The client answers this with a write: a subscribeRefresh.
    server.send("s1", 0, SubscribeExpiring::default()).await;

    let heartbeat = crate::proto::privatews::v1::Heartbeat::default();
    let mut replacement = None;
    let mut disconnected = false;
    while started.elapsed() < WRITE_TIMEOUT + Duration::from_secs(2) {
        tokio::select! {
            next = harness.servers.recv() => {
                replacement = next;
                break;
            }
            event = consumer.recv() => {
                match event.expect("stream open") {
                    PrivateOrdersEvent::Disconnected => disconnected = true,
                    PrivateOrdersEvent::Error(error) => panic!("unexpected error: {error}"),
                    other => panic!("unexpected event: {other:?}"),
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                // The old socket stays alive from the server's side.
                server.send("", 0, heartbeat.clone()).await;
            }
        }
    }
    let mut replacement = replacement.expect("a reconnect within the write timeout plus backoff");
    let waited = started.elapsed();
    let limit = match writes {
        Writes::Block => WRITE_TIMEOUT + Duration::from_millis(1_000),
        _ => Duration::from_millis(1_000),
    };
    assert!(
        waited <= limit,
        "reconnected after {waited:?}, limit {limit:?}"
    );
    assert!(disconnected, "consumers were not told about the drop");
    assert_eq!(replacement.auth().await, "connect-2");
    let request = replacement.subscribe().await;
    assert_eq!(request.cursor, "c1");
    replacement.confirm(&request, "s2").await;
    expect_connected(&mut consumer).await;
}

#[tokio::test(start_paused = true)]
async fn a_blocked_write_reconnects_despite_heartbeats() {
    a_broken_write_path_reconnects(Writes::Block).await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_reconnects_despite_heartbeats() {
    a_broken_write_path_reconnects(Writes::Fail).await;
}

#[tokio::test(start_paused = true)]
async fn a_frame_queued_on_a_closed_outgoing_channel_reconnects() {
    let harness = Harness::new();
    let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
    let mut actor = Actor::new(Arc::new(harness.transport.clone()), commands_rx);
    let (tx, mut rx) = mpsc::channel(8);
    actor.on_command(Command::Add {
        account: ACCOUNT,
        consumer: Consumer {
            id: 1,
            tx,
            ended: Arc::default(),
        },
    });
    // A connected subscription on a socket whose writer has already ended.
    let (out, out_rx) = mpsc::unbounded_channel();
    drop(out_rx);
    actor.connecting = false;
    actor.conn = Some(Connection {
        id: 7,
        out,
        reader: tokio::spawn(std::future::pending()),
        opened_at: Instant::now(),
    });
    let s = actor.subs.get_mut(&ACCOUNT).unwrap();
    s.sub = "s1".into();
    s.state = State::Connected;
    s.cursor = "c1".into();

    actor.send(client_message::Op::from(
        crate::proto::privatews::v1::Ping {
            id: "p1".into(),
            ..Default::default()
        },
    ));
    actor.finish_event();

    assert!(actor.conn.is_none(), "the unwritable socket is dropped");
    assert!(actor.reconnect_at.is_some(), "a reconnect is scheduled");
    assert!(matches!(
        rx.try_recv(),
        Ok(PrivateOrdersEvent::Disconnected)
    ));
    assert_eq!(
        actor.subs[&ACCOUNT].cursor, "c1",
        "the cursor is kept to resume"
    );
}

#[test]
fn private_ws_url_uses_the_api_host() {
    assert_eq!(
        private_ws_url("https://api.example.test", false).unwrap(),
        "wss://api.example.test/v1/ws/private"
    );
    assert_eq!(
        private_ws_url("https://api.example.test/", false).unwrap(),
        "wss://api.example.test/v1/ws/private"
    );
    assert_eq!(
        private_ws_url("http://127.0.0.1:8080", false).unwrap(),
        "ws://127.0.0.1:8080/v1/ws/private"
    );
    assert!(private_ws_url("http://api.example.test", false).is_err());
    assert_eq!(
        private_ws_url("http://api.example.test", true).unwrap(),
        "ws://api.example.test/v1/ws/private"
    );
}

#[test]
fn reconnect_backoff_doubles_from_half_a_second_to_ten_seconds() {
    let mut jitter = 7;
    for (attempt, (low, high)) in [
        (0, (500, 1_000)),
        (1, (1_000, 2_000)),
        (2, (2_000, 4_000)),
        (3, (4_000, 8_000)),
        (4, (5_000, 10_000)),
        (12, (5_000, 10_000)),
    ] {
        for _ in 0..50 {
            let delay = backoff(attempt, &mut jitter).as_millis() as u64;
            assert!((low..=high).contains(&delay), "attempt {attempt}: {delay}");
        }
    }
}
