//! Centrifugo websocket realtime client.

mod auth;
mod protocol;
mod snapshot_then_stream;

pub use snapshot_then_stream::{SnapshotErrorFn, SnapshotThenStream, SnapshotThenStreamConfig};

use crate::auth::Credentials;
use crate::errors::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use rand_core::{OsRng, RngCore};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
        protocol::WebSocketConfig,
    },
};

const WS_PATH: &str = "/connection/websocket";
const DEFAULT_QUEUE: usize = 1000;
const CENTRIFUGO_READ_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_INITIAL_CAP: Duration = Duration::from_millis(500);
const RECONNECT_MAX_CAP: Duration = Duration::from_secs(30);
const CENTRIFUGO_PROTOBUF_SUBPROTOCOL: &str = "centrifuge-protobuf";

type ErrorCallback = Arc<dyn Fn(Error) + Send + Sync>;

pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct SubscriptionErrorState {
    last: Option<Error>,
    callback: Option<ErrorCallback>,
}

struct SubscriptionAttempt<'a> {
    stop: &'a mut watch::Receiver<bool>,
    ready: &'a mut Option<oneshot::Sender<Result<()>>>,
    gap: &'a ResubscribeGap,
    error_state: &'a Arc<Mutex<SubscriptionErrorState>>,
    connected: &'a mut bool,
}

fn invoke_error_callback(callback: ErrorCallback, err: Error) {
    // A consumer callback must not be able to kill the websocket worker.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(err)));
}

fn record_subscription_error(state: &Arc<Mutex<SubscriptionErrorState>>, err: Error) {
    let callback = {
        let mut state = lock_unpoisoned(state);
        state.last = Some(err.clone());
        state.callback.clone()
    };
    if let Some(callback) = callback {
        invoke_error_callback(callback, err);
    }
}

fn clear_subscription_error(state: &Arc<Mutex<SubscriptionErrorState>>) {
    lock_unpoisoned(state).last = None;
}

struct ReconnectBackoff {
    failures: u32,
    jitter_state: u64,
}

impl ReconnectBackoff {
    fn new() -> Self {
        let mut rng = OsRng;
        Self::with_seed(rng.next_u64())
    }

    fn with_seed(seed: u64) -> Self {
        Self {
            failures: 0,
            jitter_state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    fn reset(&mut self) {
        self.failures = 0;
    }

    fn next_delay(&mut self) -> Duration {
        let multiplier = 1u64 << self.failures.min(16);
        let cap_ms = (RECONNECT_INITIAL_CAP.as_millis() as u64)
            .saturating_mul(multiplier)
            .min(RECONNECT_MAX_CAP.as_millis() as u64);
        self.failures = self.failures.saturating_add(1);

        // Xorshift64 gives each subscription a cheap independent jitter stream
        // after the OS-random seed, avoiding fleet-wide synchronized reconnects.
        self.jitter_state ^= self.jitter_state << 13;
        self.jitter_state ^= self.jitter_state >> 7;
        self.jitter_state ^= self.jitter_state << 17;
        let floor_ms = cap_ms / 2;
        let delay_ms = floor_ms + self.jitter_state % (cap_ms - floor_ms + 1);
        Duration::from_millis(delay_ms)
    }
}

/// Maximum accepted binary Centrifugo WebSocket message and frame size.
///
/// This bounds transport buffering and the subsequent protobuf decoder copy.
pub const MAX_REALTIME_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Enqueue a publication without blocking. On a full queue, mark the
/// subscription closed and return [`Error::QueueOverflow`] — never silently drop.
pub fn try_enqueue<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    closed: &AtomicBool,
    last_error: &std::sync::Mutex<Option<Error>>,
    message: &str,
) -> bool {
    if closed.load(Ordering::SeqCst) {
        return false;
    }
    match tx.try_send(item) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            closed.store(true, Ordering::SeqCst);
            *lock_unpoisoned(last_error) = Some(Error::queue_overflow(message));
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            closed.store(true, Ordering::SeqCst);
            false
        }
    }
}

fn try_send_direct<T>(tx: &mpsc::Sender<T>, item: T, message: &str) -> Result<bool> {
    match tx.try_send(item) {
        Ok(()) => Ok(true),
        Err(mpsc::error::TrySendError::Full(_)) => Err(Error::queue_overflow(message)),
        Err(mpsc::error::TrySendError::Closed(_)) => Ok(false),
    }
}

/// Realtime websocket client.
#[derive(Clone)]
pub struct Client {
    ws_url: String,
    api_url: String,
    credentials: Option<Credentials>,
    max_queue: usize,
    /// Deadline for private-channel HTTP token exchange (request + body).
    timeout: Duration,
}

impl Client {
    pub fn new(
        ws_url: impl Into<String>,
        api_url: impl Into<String>,
        credentials: Option<Credentials>,
        max_queue: Option<usize>,
    ) -> Self {
        Self::with_timeout(
            ws_url,
            api_url,
            credentials,
            max_queue,
            auth::DEFAULT_TOKEN_REQUEST_TIMEOUT,
        )
    }

    pub fn with_timeout(
        ws_url: impl Into<String>,
        api_url: impl Into<String>,
        credentials: Option<Credentials>,
        max_queue: Option<usize>,
        timeout: Duration,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            api_url: api_url.into(),
            credentials,
            max_queue: max_queue.unwrap_or(DEFAULT_QUEUE).max(1),
            timeout: if timeout.is_zero() {
                auth::DEFAULT_TOKEN_REQUEST_TIMEOUT
            } else {
                timeout
            },
        }
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.timeout
    }

    fn ws_endpoint(&self) -> String {
        let base = self.ws_url.trim_end_matches('/');
        if base.contains(WS_PATH) {
            base.to_owned()
        } else {
            format!("{base}{WS_PATH}")
        }
    }

    fn validate_channel(&self, channel: &str) -> Result<()> {
        if is_private_channel(channel) {
            if self.credentials.is_none() {
                return Err(Error::auth(format!(
                    "Cannot subscribe to private channel \"{channel}\" without API-key credentials"
                )));
            }
            if self.api_url.is_empty() {
                return Err(Error::realtime(
                    "Realtime private channels require api_url".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Subscribe to a protobuf channel and receive raw publication payloads.
    ///
    /// Returns only after the initial websocket handshake and channel
    /// subscription succeed.
    pub async fn subscribe_raw(&self, channel: &str) -> Result<TypedSubscription<Vec<u8>>> {
        self.subscribe_proto(channel, |bytes| Ok(bytes.to_vec()))
            .await
    }

    /// Subscribe to a protobuf Centrifugo channel and decode publications.
    ///
    /// Returns only after the initial websocket handshake and channel
    /// subscription succeed.
    pub async fn subscribe_proto<T, F>(
        &self,
        channel: &str,
        decode: F,
    ) -> Result<TypedSubscription<T>>
    where
        T: Send + 'static,
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
    {
        self.subscribe_proto_with_options(channel, decode, true)
            .await
    }

    /// Subscribe with optional auto-reconnect control.
    ///
    /// Snapshot-then-stream sets `auto_reconnect=false` so it can rebuild REST
    /// state between reconnect attempts.
    pub async fn subscribe_proto_with_options<T, F>(
        &self,
        channel: &str,
        decode: F,
        auto_reconnect: bool,
    ) -> Result<TypedSubscription<T>>
    where
        T: Send + 'static,
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
    {
        self.validate_channel(channel)?;

        let (stop_tx, mut stop_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let error_state = Arc::new(Mutex::new(SubscriptionErrorState::default()));
        let gap = Arc::new(ResubscribeGap::default());
        let (ready_tx, ready_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel::<T>(self.max_queue);
        let decode = Arc::new(decode);

        let this = self.clone();
        let channel = channel.to_owned();
        let alive_task = alive.clone();
        let error_task = error_state.clone();
        let gap_task = gap.clone();
        let task = tokio::spawn(async move {
            let _guard = AliveGuard(alive_task.clone());
            let mut ready = Some(ready_tx);
            let mut backoff = ReconnectBackoff::new();
            while !*stop_rx.borrow() {
                let mut connected_this_attempt = false;
                let mut attempt = SubscriptionAttempt {
                    stop: &mut stop_rx,
                    ready: &mut ready,
                    gap: &gap_task,
                    error_state: &error_task,
                    connected: &mut connected_this_attempt,
                };
                match this
                    .run_proto_subscription_once(&channel, decode.as_ref(), &tx, &mut attempt)
                    .await
                {
                    Ok(()) => break,
                    Err(_) if *stop_rx.borrow() => break,
                    Err(err) if matches!(err, Error::QueueOverflow(_)) => {
                        record_subscription_error(&error_task, err);
                        break;
                    }
                    Err(err) => {
                        if connected_this_attempt {
                            backoff.reset();
                        }
                        record_subscription_error(&error_task, err.clone());
                        if let Some(ready) = ready.take() {
                            let _ = ready.send(Err(err));
                            break;
                        }
                        if *stop_rx.borrow() || !auto_reconnect {
                            break;
                        }
                        // Per-subscription exponential backoff with jitter avoids
                        // synchronized reconnect storms while remaining cancellable.
                        let delay = backoff.next_delay();
                        tokio::select! {
                            _ = stop_rx.changed() => {
                                if *stop_rx.borrow() {
                                    break;
                                }
                            }
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                }
            }
            alive_task.store(false, Ordering::SeqCst);
            drop(tx);
        });
        // If the caller cancels while awaiting readiness, abort the setup task
        // so in-flight HTTP token / WS handshake work does not linger.
        let mut abort_on_cancel = AbortOnDrop(Some(task));
        let ready_result = ready_rx
            .await
            .map_err(|_| Error::realtime("realtime task ended before handshake".to_owned()))
            .and_then(|inner| inner);
        let task = abort_on_cancel
            .0
            .take()
            .expect("subscription task present after ready");
        ready_result?;

        Ok(TypedSubscription {
            rx,
            stop: stop_tx,
            alive,
            task,
            error_state,
            gap,
        })
    }

    async fn handshake_channel<W, R>(
        &self,
        write: &mut W,
        read: &mut R,
        channel: &str,
    ) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: std::fmt::Display,
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        if is_private_channel(channel) {
            let creds = self
                .credentials
                .as_ref()
                .ok_or_else(|| Error::auth("private channel requires credentials"))?;
            let connection_token =
                auth::fetch_connection_token(creds, &self.api_url, self.timeout).await?;
            centrifugo_connect(write, read, Some(&connection_token)).await?;
            let subscription_token =
                auth::fetch_subscription_token(creds, &self.api_url, channel, self.timeout).await?;
            centrifugo_subscribe(write, read, channel, Some(&subscription_token)).await?;
        } else {
            centrifugo_connect(write, read, None).await?;
            centrifugo_subscribe(write, read, channel, None).await?;
        }
        Ok(())
    }

    async fn run_proto_subscription_once<T, F>(
        &self,
        channel: &str,
        decode: &F,
        tx: &mpsc::Sender<T>,
        attempt: &mut SubscriptionAttempt<'_>,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T>,
    {
        let url = self.ws_endpoint();
        let mut request = url
            .into_client_request()
            .map_err(|e| Error::realtime(format!("ws request: {e}")))?;
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(CENTRIFUGO_PROTOBUF_SUBPROTOCOL),
        );
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(MAX_REALTIME_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_REALTIME_MESSAGE_BYTES));
        let (ws, response) = timeout(
            CENTRIFUGO_READ_TIMEOUT,
            connect_async_with_config(request, Some(websocket_config), false),
        )
        .await
        .map_err(|_| Error::realtime("websocket connect timed out".to_owned()))?
        .map_err(|e| Error::realtime(format!("ws connect: {e}")))?;
        if response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            != Some(CENTRIFUGO_PROTOBUF_SUBPROTOCOL)
        {
            return Err(Error::realtime(
                "server did not negotiate centrifuge-protobuf websocket subprotocol".to_owned(),
            ));
        }
        let (mut write, mut read) = ws.split();
        self.handshake_channel(&mut write, &mut read, channel)
            .await?;
        *attempt.connected = true;
        clear_subscription_error(attempt.error_state);
        if let Some(ready) = attempt.ready.take() {
            let _ = ready.send(Ok(()));
        } else {
            // Successful reconnect after the initial handshake: publications may
            // have been lost (no Centrifugo recover/offset). Consumers must treat
            // this as a possible gap.
            attempt.gap.note_resubscribe();
        }

        loop {
            if *attempt.stop.borrow() {
                return Ok(());
            }
            let msg = tokio::select! {
                changed = attempt.stop.changed() => {
                    if changed.is_err() || *attempt.stop.borrow() {
                        return Ok(());
                    }
                    None
                }
                msg = timeout(CENTRIFUGO_READ_TIMEOUT, read.next()) => {
                    Some(match msg {
                        Ok(Some(Ok(msg))) => msg,
                        Ok(Some(Err(e))) => return Err(Error::realtime(e.to_string())),
                        Ok(None) => return Err(Error::realtime("websocket closed".to_owned())),
                        // Half-open TCP: a read timeout is connection death, not a no-op.
                        Err(_) => {
                            return Err(Error::realtime("websocket read timeout".to_owned()));
                        }
                    })
                }
            };
            let Some(msg) = msg else {
                continue;
            };
            match msg {
                Message::Binary(frame) => {
                    for incoming in protocol::decode_replies(&frame)? {
                        match incoming {
                            protocol::Incoming::Ping => {
                                write
                                    .send(Message::Binary(protocol::pong_command().into()))
                                    .await
                                    .map_err(|e| {
                                        Error::realtime(format!("protobuf pong send: {e}"))
                                    })?;
                            }
                            protocol::Incoming::Publication(bytes) => {
                                let item = decode(&bytes)?;
                                if !try_send_direct(
                                    tx,
                                    item,
                                    "typed realtime subscription queue full; consumer too slow",
                                )? {
                                    return Ok(());
                                }
                            }
                            protocol::Incoming::Reply {
                                error: Some(err), ..
                            } => {
                                return Err(centrifugo_protocol_error(err));
                            }
                            protocol::Incoming::Reply { .. } => {}
                        }
                    }
                }
                Message::Text(_) => {
                    return Err(Error::realtime(
                        "received JSON text frame on protobuf websocket".to_owned(),
                    ));
                }
                Message::Ping(payload) => {
                    write
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|e| Error::realtime(format!("ws pong: {e}")))?;
                }
                Message::Close(_) => {
                    return Err(Error::realtime("websocket closed".to_owned()));
                }
                _ => {}
            }
        }
    }
}

#[derive(Default)]
struct ResubscribeGap {
    count: std::sync::atomic::AtomicU64,
    latched: AtomicBool,
}

impl ResubscribeGap {
    fn note_resubscribe(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.latched.store(true, Ordering::SeqCst);
    }
}

/// Handle for a typed protobuf realtime subscription.
///
/// After a transport reconnect the subscription is re-established without a
/// server-side resume cursor. [`Self::resubscribes`] / [`Self::take_resubscribed`]
/// signal that gap: publications may have been lost.
pub struct TypedSubscription<T> {
    rx: mpsc::Receiver<T>,
    stop: watch::Sender<bool>,
    alive: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    error_state: Arc<Mutex<SubscriptionErrorState>>,
    gap: Arc<ResubscribeGap>,
}

impl<T> TypedSubscription<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await
    }

    /// Receive one publication while preserving terminal delivery failures.
    ///
    /// Unlike [`Self::recv`], a closed feed with a recorded error returns that
    /// error instead of being indistinguishable from a clean shutdown.
    pub async fn recv_result(&mut self) -> Result<Option<T>> {
        match self.rx.recv().await {
            Some(item) => Ok(Some(item)),
            None => match self.take_err() {
                Some(err) => Err(err),
                None => Ok(None),
            },
        }
    }

    /// Register a callback for background transport/protocol errors.
    ///
    /// If an error was already recorded, the callback is invoked immediately.
    /// Callback panics are isolated from the websocket worker.
    pub fn set_on_error<F>(&self, callback: F)
    where
        F: Fn(Error) + Send + Sync + 'static,
    {
        let callback: ErrorCallback = Arc::new(callback);
        let current = {
            let mut state = lock_unpoisoned(&self.error_state);
            state.callback = Some(callback.clone());
            state.last.clone()
        };
        if let Some(err) = current {
            invoke_error_callback(callback, err);
        }
    }

    /// True while the background websocket task is still running.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst) && !self.task.is_finished()
    }

    /// Most recent connection or terminal delivery error.
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.error_state).last.clone()
    }

    /// Take and clear the most recent background error.
    pub fn take_err(&self) -> Option<Error> {
        lock_unpoisoned(&self.error_state).last.take()
    }

    /// How many times this subscription successfully reconnected after the
    /// initial connect. Non-zero means the stream may have gaps.
    pub fn resubscribes(&self) -> u64 {
        self.gap.count.load(Ordering::SeqCst)
    }

    /// Reports whether a reconnect/resubscribe happened since the last call and
    /// clears the latch. The initial connect does not set the latch.
    pub fn take_resubscribed(&self) -> bool {
        self.gap.latched.swap(false, Ordering::SeqCst)
    }

    pub fn close(&self) {
        let _ = self.stop.send(true);
        self.task.abort();
    }
}

impl<T> Drop for TypedSubscription<T> {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.task.abort();
    }
}

struct AliveGuard(Arc<AtomicBool>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Aborts a JoinHandle if dropped before the handle is taken (cancel-safe setup).
struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

pub fn is_private_channel(channel: &str) -> bool {
    channel.starts_with("private:")
}

async fn centrifugo_connect<W, R>(write: &mut W, read: &mut R, token: Option<&str>) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    write
        .send(Message::Binary(protocol::connect_command(1, token).into()))
        .await
        .map_err(|e| Error::realtime(format!("connect send: {e}")))?;
    read_centrifugo_reply(write, read, 1).await
}

async fn centrifugo_subscribe<W, R>(
    write: &mut W,
    read: &mut R,
    channel: &str,
    token: Option<&str>,
) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    write
        .send(Message::Binary(
            protocol::subscribe_command(2, channel, token).into(),
        ))
        .await
        .map_err(|e| Error::realtime(format!("subscribe send: {e}")))?;
    read_centrifugo_reply(write, read, 2).await
}

async fn read_centrifugo_reply<W, R>(write: &mut W, read: &mut R, expected_id: u32) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    loop {
        let msg = timeout(Duration::from_secs(10), read.next())
            .await
            .map_err(|_| Error::realtime("centrifugo reply timeout".to_owned()))?
            .ok_or_else(|| Error::realtime("centrifugo closed before reply".to_owned()))?
            .map_err(|e| Error::realtime(format!("centrifugo read: {e}")))?;
        match msg {
            Message::Binary(frame) => {
                for incoming in protocol::decode_replies(&frame)? {
                    match incoming {
                        protocol::Incoming::Reply {
                            id,
                            error: Some(err),
                        } if id == expected_id => return Err(centrifugo_protocol_error(err)),
                        protocol::Incoming::Reply { id, error: None } if id == expected_id => {
                            return Ok(());
                        }
                        protocol::Incoming::Ping => {
                            write
                                .send(Message::Binary(protocol::pong_command().into()))
                                .await
                                .map_err(|e| Error::realtime(format!("protobuf pong send: {e}")))?;
                        }
                        _ => {}
                    }
                }
            }
            Message::Text(_) => {
                return Err(Error::realtime(
                    "received JSON text reply on protobuf websocket".to_owned(),
                ));
            }
            Message::Ping(payload) => {
                write
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|e| Error::realtime(format!("ws pong: {e}")))?;
            }
            Message::Close(_) => {
                return Err(Error::realtime("centrifugo closed before reply".to_owned()));
            }
            _ => {}
        }
    }
}

fn centrifugo_protocol_error(error: protocol::ProtoError) -> Error {
    Error::realtime(format!(
        "centrifugo error {}: {}{}",
        error.code,
        error.message,
        if error.temporary { " (temporary)" } else { "" }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_private_channel_detects_prefix() {
        assert!(is_private_channel("private:spot:orders:acct:proto"));
        assert!(!is_private_channel("public:spot:market:trades:1:proto"));
    }

    #[test]
    fn protobuf_websocket_subprotocol_is_centrifuge_protobuf() {
        assert_eq!(CENTRIFUGO_PROTOBUF_SUBPROTOCOL, "centrifuge-protobuf");
        let value = HeaderValue::from_static(CENTRIFUGO_PROTOBUF_SUBPROTOCOL);
        assert_eq!(value.to_str().unwrap(), "centrifuge-protobuf");
    }

    #[test]
    fn try_enqueue_fails_on_overflow_without_silent_drop() {
        use std::sync::Mutex;

        let (tx, mut rx) = mpsc::channel::<u8>(1);
        let closed = AtomicBool::new(false);
        let last_error = Mutex::new(None);
        assert!(try_enqueue(
            &tx,
            1,
            &closed,
            &last_error,
            "orderbook subscription queue full; consumer too slow"
        ));
        assert!(!try_enqueue(
            &tx,
            2,
            &closed,
            &last_error,
            "orderbook subscription queue full; consumer too slow"
        ));
        assert!(closed.load(Ordering::SeqCst));
        assert!(matches!(
            last_error.lock().unwrap().as_ref(),
            Some(Error::QueueOverflow(_))
        ));
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn direct_subscription_queue_fails_closed_on_overflow() {
        let (tx, mut rx) = mpsc::channel::<u8>(1);
        assert!(try_send_direct(&tx, 1, "full").unwrap());
        assert!(matches!(
            try_send_direct(&tx, 2, "full"),
            Err(Error::QueueOverflow(_))
        ));
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn realtime_queue_capacity_is_never_zero() {
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            Some(0),
        );
        assert_eq!(client.max_queue, 1);
    }

    #[tokio::test]
    async fn subscribe_surfaces_initial_handshake_failure() {
        let client = Client::new("not a websocket URL", "", None, None);

        let result =
            tokio::time::timeout(Duration::from_secs(2), client.subscribe_raw("public:test"))
                .await
                .expect("subscribe should not hang");
        assert!(result.is_err(), "initial handshake error must be returned");
    }

    #[test]
    fn read_timeout_constant_is_positive() {
        assert!(CENTRIFUGO_READ_TIMEOUT > Duration::from_secs(0));
    }

    #[tokio::test]
    async fn dropping_typed_subscription_signals_stop_and_aborts() {
        let (stop, stop_rx) = watch::channel(false);
        let (_tx, rx) = mpsc::channel::<u8>(1);
        let alive = Arc::new(AtomicBool::new(true));
        let (marker_tx, marker_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _marker = marker_tx;
            std::future::pending::<()>().await
        });
        let subscription = TypedSubscription {
            rx,
            stop,
            alive,
            task,
            error_state: Arc::new(Mutex::new(SubscriptionErrorState::default())),
            gap: Arc::new(ResubscribeGap::default()),
        };

        drop(subscription);

        assert!(*stop_rx.borrow(), "close/Drop must signal stop");
        assert!(
            tokio::time::timeout(Duration::from_millis(500), marker_rx)
                .await
                .expect("abort should drop task locals promptly")
                .is_err(),
            "JoinHandle must be aborted on Drop"
        );
    }

    #[test]
    fn reconnect_backoff_is_bounded_exponential_and_jittered() {
        let mut first = ReconnectBackoff::with_seed(1);
        let mut second = ReconnectBackoff::with_seed(2);
        let mut first_delays = Vec::new();
        let mut second_delays = Vec::new();

        for attempt in 0..12 {
            let first_delay = first.next_delay();
            let second_delay = second.next_delay();
            let cap_ms = (RECONNECT_INITIAL_CAP.as_millis() as u64)
                .saturating_mul(1u64 << attempt.min(16))
                .min(RECONNECT_MAX_CAP.as_millis() as u64);
            let floor = Duration::from_millis(cap_ms / 2);
            let cap = Duration::from_millis(cap_ms);
            assert!((floor..=cap).contains(&first_delay));
            assert!((floor..=cap).contains(&second_delay));
            first_delays.push(first_delay);
            second_delays.push(second_delay);
        }

        assert_ne!(
            first_delays, second_delays,
            "independent subscriptions must not share one reconnect schedule"
        );
        assert!(first_delays.iter().all(|delay| *delay <= RECONNECT_MAX_CAP));

        first.reset();
        assert!(first.next_delay() <= RECONNECT_INITIAL_CAP);
    }
}
