//! The connection: signed upgrade, subscription, one writer, ping and
//! reconnect.
//!
//! One task owns the socket. Every frame of a connection, the subscription and
//! pings included, is written by it, and it owns the connection's `seq`: 1 for
//! the first frame, then one more per frame. A new connection starts again at
//! 1 with nothing in flight and the timer gate closed.
//!
//! **Nothing stale crosses a reconnect.** A command is bound to the connection
//! that was ready when it was made and is never written on another; one whose
//! deadline passed while it waited is not written either.
//!
//! **Orders only inside an armed timer.** With [`Config::timer_gate`] set, a
//! create, modify, batch create or batch replace is written only if its
//! deadline, plus the clock margin, ends before the last cancel-all-after
//! acknowledgement's expiry minus the guard. The server admits a command only
//! until its deadline and expires the timer at that expiry, both on its own
//! clock, so every order it admits is admitted before the timer expires and
//! the expiry's cancel sweeps it, whatever the clocks' skew. A timer change
//! closes or tightens the gate as soon as it is written: a disarm closes it,
//! and an arm or refresh bounds it by its own timeout from now, until its
//! acknowledgement shows the timer the server holds.
//!
//! **A missing answer ends the connection.** A command or ping unanswered for
//! [`Config::answer_timeout`], or a frame that does not decode, ends the
//! connection; every command sent on it without an answer is reported as an
//! unknown outcome, and the session reconnects with backoff. A write that the
//! peer does not take ends the connection the same way, by the oldest
//! unanswered frame's answer deadline, and closing the session ends it at
//! once.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::USER_AGENT;
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{WebSocketStream, client_async_with_config};

use super::codec::{self, Answer, CHANNELS, Msg};
use super::{
    BatchCancelAck, BatchCancelOrders, BatchCreateAck, BatchCreateOrders, BatchReplaceAck,
    BatchReplaceOrders, CancelAllAck, CancelAllOrders, Command, Deadline, Event,
    MAX_CANCEL_ON_DISCONNECT_GRACE, MIN_CANCEL_ON_DISCONNECT_GRACE, ModifyOrder, NotSent, OrderAck,
    OrderRef, PATH, PostOnlyOrder, Reject, RejectCode, TimerAck, TradeError, from_unix_ns,
    new_correlation_id, not_sent, unix_ms,
};
use crate::auth::Credentials;
use crate::errors::{Error, Result};
use crate::realtime::{ReconnectBackoff, lock_unpoisoned};
use crate::reconnect::{ReconnectLimiter, upgrade_slot};

type Ws = WebSocketStream<super::upgrade::Recoverable>;

/// Commands that may wait to be written; a full queue refuses the next one
/// ([`NotSent::QueueFull`]).
pub const COMMAND_QUEUE: usize = 1024;
/// Commands that may await an answer; the server takes up to 1024 in flight
/// before TCP pushes back, and the session stops at half of that, so a slow
/// server shows as refusals ([`NotSent::InFlightFull`]) rather than a stalled
/// writer.
pub const MAX_IN_FLIGHT: usize = 512;

const DEFAULT_QUEUE: usize = 1000;
const DEFAULT_PING: Duration = Duration::from_secs(5);
const DEFAULT_ANSWER: Duration = Duration::from_secs(5);
const DEFAULT_GRACE: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds one server message: a cancel-all's listing of 200 orders is under
/// 10 KiB.
const MAX_MESSAGE_BYTES: usize = 1 << 20;

const HEADER_COD_SCOPE: &str = "X-Cancel-On-Disconnect";
const HEADER_COD_GRACE: &str = "X-Cancel-On-Disconnect-Grace-Sec";

/// The cancel-all-after gate's margins ([`Config::timer_gate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerGate {
    /// Room for the client's and server's clocks to step apart.
    pub clock_margin: Duration,
    /// Room before the timer's expiry for its cancel sweep to start.
    pub guard: Duration,
}

impl Default for TimerGate {
    fn default() -> Self {
        Self {
            clock_margin: Duration::from_millis(100),
            guard: Duration::from_millis(500),
        }
    }
}

/// Configures a [`TradeSession`].
#[derive(Clone)]
pub struct Config {
    /// The trading WebSocket URL; [`url`] builds it from a WebSocket host.
    pub url: String,
    /// The API key the upgrade is signed with; the session trades for the
    /// account the key is bound to.
    pub credentials: Credentials,
    /// The cancel-on-disconnect grace period, whole seconds from 10 to 120:
    /// once no session of the account that opted in has been connected for
    /// this long, the account's open orders are cancelled with
    /// [`super::CancelReason::CancelOnDisconnect`]. `None` opts out. Default:
    /// 15 seconds. Every upgrade, reconnects included, opts in until
    /// [`TradeSender::disable_cancel_on_disconnect`] is called.
    pub cancel_on_disconnect: Option<Duration>,
    /// Holds back every order placement whose deadline is not inside the
    /// armed cancel-all-after timer ([`NotSent::NotArmed`]). `None` turns the
    /// gate off. Default: [`TimerGate::default`].
    pub timer_gate: Option<TimerGate>,
    /// Bounds the event queue. A consumer that falls behind by more stops the
    /// session with [`Error::QueueOverflow`] instead of losing an event.
    /// 0: 1000.
    pub queue_size: usize,
    /// How often the session pings the server. Zero: 5 seconds.
    pub ping_interval: Duration,
    /// How long a ping or command may go unanswered before the connection
    /// ends and the command is reported as an unknown outcome. Zero: 5
    /// seconds.
    pub answer_timeout: Duration,
    /// Permit a `ws://` URL on a host that is not loopback.
    pub allow_insecure_http: bool,
    /// Shares a cap on concurrent upgrades with every other session and
    /// stream given the same limiter ([`ReconnectLimiter`]): each upgrade,
    /// the first included, waits for a slot once its backoff and any
    /// `Retry-After` have run out, and holds it only until the upgrade ends.
    /// `None`, the default, connects as soon as the session's own wait ends.
    pub reconnect_limiter: Option<ReconnectLimiter>,
    /// How long an upgrade may take before it fails and is retried.
    #[cfg(test)]
    pub(crate) connect_timeout: Duration,
}

impl Config {
    pub fn new(url: impl Into<String>, credentials: Credentials) -> Self {
        Self {
            url: url.into(),
            credentials,
            cancel_on_disconnect: Some(DEFAULT_GRACE),
            timer_gate: Some(TimerGate::default()),
            queue_size: 0,
            ping_interval: Duration::ZERO,
            answer_timeout: Duration::ZERO,
            allow_insecure_http: false,
            reconnect_limiter: None,
            #[cfg(test)]
            connect_timeout: CONNECT_TIMEOUT,
        }
    }

    /// How long an upgrade may take before it fails.
    #[cfg(not(test))]
    fn connect_timeout(&self) -> Duration {
        CONNECT_TIMEOUT
    }

    #[cfg(test)]
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("url", &crate::transport::url_without_userinfo(&self.url))
            .field("credentials", &self.credentials)
            .field("cancel_on_disconnect", &self.cancel_on_disconnect)
            .field("timer_gate", &self.timer_gate)
            .field("queue_size", &self.queue_size)
            .field("ping_interval", &self.ping_interval)
            .field("answer_timeout", &self.answer_timeout)
            .field("reconnect_limiter", &self.reconnect_limiter)
            .finish_non_exhaustive()
    }
}

/// The trading WebSocket URL on the WebSocket host `base`, such as
/// `wss://<api-host>`.
pub fn url(base: &str) -> Result<String> {
    let mut u = url::Url::parse(base.trim())
        .map_err(|e| Error::validation(format!("tradews base URL: {e}")))?;
    if u.scheme() != "wss" && u.scheme() != "ws" {
        return Err(Error::validation("tradews base URL must use wss or ws"));
    }
    if u.host_str().is_none_or(str::is_empty) {
        return Err(Error::validation("tradews base URL has no host"));
    }
    if u.query().is_some() || u.fragment().is_some() {
        return Err(Error::validation(
            "tradews base URL must not contain a query string or fragment",
        ));
    }
    let path = u.path().trim_end_matches('/');
    let path = if path.ends_with(PATH) {
        path.to_owned()
    } else {
        format!("{path}{PATH}")
    };
    u.set_path(&path);
    Ok(u.to_string())
}

/// The state callers read: the current connection and whether its
/// subscription is confirmed.
#[derive(Debug, Clone, Copy, Default)]
struct Link {
    connection: u64,
    ready: bool,
    closed: bool,
}

struct Shared {
    cfg: Config,
    /// Commands for the writer. The session keeps a sender of its own, so
    /// the queue stays open however many [`TradeSender`]s are dropped.
    commands: mpsc::Sender<Cmd>,
    link: watch::Sender<Link>,
    stop: watch::Sender<bool>,
    err: Mutex<Option<Error>>,
    /// The shutdown fence: set once, synchronously, when either half closes
    /// the session or its events are dropped, before any of the session's
    /// own work runs. A command is refused as [`NotSent::Closed`] once it is
    /// set, at admission and again right before its frame is written, so no
    /// command made or still queued after a close reaches the socket.
    closed: AtomicBool,
    /// Set once by [`TradeSender::disable_cancel_on_disconnect`], never
    /// cleared: every upgrade that reads it afterwards opts out.
    cancel_on_disconnect_disabled: AtomicBool,
}

impl Shared {
    /// Closes the session: the fence first, then the worker's stop signal.
    fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.stop.send_replace(true);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// The cancel-on-disconnect grace the next upgrade opts in with. Each
    /// upgrade reads it once, before it builds its request, and keeps that
    /// value for the request's headers and for checking the server's
    /// confirmation.
    fn cancel_on_disconnect(&self) -> Option<Duration> {
        if self.cancel_on_disconnect_disabled.load(Ordering::SeqCst) {
            None
        } else {
            self.cfg.cancel_on_disconnect
        }
    }
}

/// A command for the writer, bound to the connection that was ready when it
/// was made. `frame` is encoded with `seq` 0.
struct Cmd {
    connection: u64,
    command: Command,
    correlation_id: u64,
    deadline: Deadline,
    /// A `CancelAllAfter`'s timeout.
    timer: Option<Duration>,
    frame: Vec<u8>,
    answer: oneshot::Sender<Result<Answer>>,
}

/// A trading WebSocket session that reconnects on its own. The write methods
/// return each command's answer; [`Self::recv`] delivers the account's order
/// and execution events and the connection's lifecycle. Dropping it closes
/// it: with cancel-on-disconnect, the account's orders are cancelled after
/// the grace period unless another opted-in session is connected.
///
/// [`Self::recv`] takes `&mut self` and every command `&self`, so one value
/// cannot drain events while commands await their answers. To do both from
/// separate tasks, [`Self::split`] it into a [`TradeSender`], which any
/// number of tasks may clone and send with, and the [`TradeEvents`] that one
/// task drains.
pub struct TradeSession {
    sender: TradeSender,
    events: TradeEvents,
}

/// The command half of a [`TradeSession`] ([`TradeSession::split`]): every
/// command method, cheap to clone and shared by any number of tasks, each
/// command awaiting its own answer. Dropping every sender ends no
/// connection: the session runs, and delivers events, until its
/// [`TradeEvents`] is dropped or [`Self::close`] is called.
#[derive(Clone)]
pub struct TradeSender {
    shared: Arc<Shared>,
}

/// The event half of a [`TradeSession`] ([`TradeSession::split`]): the
/// account's order and execution events and the connection's lifecycle, in
/// order. Drain it promptly: a consumer that falls behind by more than
/// [`Config::queue_size`] events stops the session with
/// [`Error::QueueOverflow`].
///
/// It owns the session's life. Dropping it closes the session as
/// [`TradeSession::close`] does: the connection ends, every command already
/// sent and unanswered is answered [`TradeError::OutcomeUnknown`], and every
/// command after it [`NotSent::Closed`]. The close is a fence for every
/// handle, set before the drop returns: a command made through any
/// [`TradeSender`] afterwards, or still queued, is never written. With cancel-on-disconnect, the
/// account's orders are cancelled after the grace period unless another
/// opted-in session is connected.
pub struct TradeEvents {
    rx: mpsc::Receiver<Event>,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl Drop for TradeEvents {
    fn drop(&mut self) {
        self.shared.shutdown();
    }
}

/// Connects to the trading WebSocket. A failure of this first connection is
/// returned; once connected, the session reconnects on its own until closed.
/// Wait for [`TradeSession::ready`] before sending commands.
pub async fn connect(mut config: Config) -> Result<TradeSession> {
    if config.url.is_empty() {
        return Err(Error::validation("tradews Config.url is required"));
    }
    crate::transport::validate_ws_url(&config.url, config.allow_insecure_http)?;
    if let Some(grace) = config.cancel_on_disconnect
        && (grace.subsec_nanos() != 0
            || grace < MIN_CANCEL_ON_DISCONNECT_GRACE
            || grace > MAX_CANCEL_ON_DISCONNECT_GRACE)
    {
        return Err(Error::validation(
            "tradews cancel-on-disconnect grace must be whole seconds from 10 to 120",
        ));
    }
    if config.queue_size == 0 {
        config.queue_size = DEFAULT_QUEUE;
    }
    if config.ping_interval.is_zero() {
        config.ping_interval = DEFAULT_PING;
    }
    if config.answer_timeout.is_zero() {
        config.answer_timeout = DEFAULT_ANSWER;
    }
    // The first upgrade takes a slot of a shared limiter too, so the cap
    // holds for sessions started together as for sessions dropped together.
    let slot = upgrade_slot(config.reconnect_limiter.as_ref()).await;
    let ws = dial(&config, config.cancel_on_disconnect).await;
    drop(slot);
    let ws = ws?;
    let (events, rx) = mpsc::channel(config.queue_size);
    let (commands, cmd_rx) = mpsc::channel(COMMAND_QUEUE);
    let (link, _) = watch::channel(Link::default());
    let (stop, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        cfg: config,
        commands,
        link,
        stop,
        err: Mutex::new(None),
        closed: AtomicBool::new(false),
        cancel_on_disconnect_disabled: AtomicBool::new(false),
    });
    let task = tokio::spawn(run(shared.clone(), ws, cmd_rx, events));
    Ok(TradeSession {
        sender: TradeSender {
            shared: shared.clone(),
        },
        events: TradeEvents { rx, shared, task },
    })
}

fn closed_error() -> Error {
    Error::realtime("tradews session is closed")
}

impl TradeSession {
    /// Splits the session into its command half, which any number of tasks
    /// may clone and send with, and its event half, which one task drains,
    /// as a WebSocket stream splits into its sink and stream. The session
    /// lives until the [`TradeEvents`] is dropped or either half closes it.
    pub fn split(self) -> (TradeSender, TradeEvents) {
        (self.sender, self.events)
    }

    /// The next event, in order. `None` once the session has stopped: after
    /// [`Self::close`], or a terminal error that [`Self::err`] reports.
    pub async fn recv(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// [`TradeSender::ready`].
    pub async fn ready(&self) -> Result<u64> {
        self.sender.ready().await
    }

    /// [`TradeSender::err`].
    pub fn err(&self) -> Option<Error> {
        self.sender.err()
    }

    /// Whether the background task still runs.
    pub fn is_alive(&self) -> bool {
        self.events.is_alive()
    }

    /// Closes the socket and stops the session; queued events stay readable.
    pub fn close(&self) {
        self.sender.close();
    }

    /// [`TradeSender::disable_cancel_on_disconnect`].
    pub fn disable_cancel_on_disconnect(&self) {
        self.sender.disable_cancel_on_disconnect();
    }

    /// [`TradeSender::create_order`].
    pub async fn create_order(
        &self,
        order: &PostOnlyOrder,
        deadline: Deadline,
    ) -> Result<OrderAck> {
        self.sender.create_order(order, deadline).await
    }

    /// [`TradeSender::cancel_order`].
    pub async fn cancel_order(
        &self,
        symbol_id: u32,
        order: &OrderRef,
        deadline: Deadline,
    ) -> Result<OrderAck> {
        self.sender.cancel_order(symbol_id, order, deadline).await
    }

    /// [`TradeSender::modify_order`].
    pub async fn modify_order(&self, modify: &ModifyOrder, deadline: Deadline) -> Result<OrderAck> {
        self.sender.modify_order(modify, deadline).await
    }

    /// [`TradeSender::cancel_all_orders`].
    pub async fn cancel_all_orders(
        &self,
        cancel: &CancelAllOrders,
        deadline: Deadline,
    ) -> Result<CancelAllAck> {
        self.sender.cancel_all_orders(cancel, deadline).await
    }

    /// [`TradeSender::cancel_all_after`].
    pub async fn cancel_all_after(
        &self,
        timeout: Duration,
        deadline: Deadline,
    ) -> Result<TimerAck> {
        self.sender.cancel_all_after(timeout, deadline).await
    }

    /// [`TradeSender::batch_create_orders`].
    pub async fn batch_create_orders(
        &self,
        batch: &BatchCreateOrders,
        deadline: Deadline,
    ) -> Result<BatchCreateAck> {
        self.sender.batch_create_orders(batch, deadline).await
    }

    /// [`TradeSender::batch_replace_orders`].
    pub async fn batch_replace_orders(
        &self,
        batch: &BatchReplaceOrders,
        deadline: Deadline,
    ) -> Result<BatchReplaceAck> {
        self.sender.batch_replace_orders(batch, deadline).await
    }

    /// [`TradeSender::batch_cancel_orders`].
    pub async fn batch_cancel_orders(
        &self,
        batch: &BatchCancelOrders,
        deadline: Deadline,
    ) -> Result<BatchCancelAck> {
        self.sender.batch_cancel_orders(batch, deadline).await
    }
}

impl TradeEvents {
    /// The next event, in order. `None` once the session has stopped: after
    /// [`Self::close`], or a terminal error that [`Self::err`] reports.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// [`TradeSender::err`].
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.shared.err).clone()
    }

    /// Whether the background task still runs.
    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    /// Closes the socket and stops the session; queued events stay readable.
    pub fn close(&self) {
        self.shared.shutdown();
    }
}

impl TradeSender {
    /// Waits until a connection's subscription is confirmed and returns the
    /// connection's number; commands are sent on it from then on. Fails once
    /// the session has stopped.
    pub async fn ready(&self) -> Result<u64> {
        let mut link = self.shared.link.subscribe();
        let state = *link
            .wait_for(|l| l.ready || l.closed)
            .await
            .map_err(|_| closed_error())?;
        if state.closed {
            return Err(self.err().unwrap_or_else(closed_error));
        }
        Ok(state.connection)
    }

    /// The terminal error that stopped the session: [`Error::QueueOverflow`],
    /// a refused API key, a server that does not confirm cancel-on-disconnect
    /// ([`TradeError::NotProtected`]) or speaks another schema.
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.shared.err).clone()
    }

    /// Whether the session still runs.
    pub fn is_alive(&self) -> bool {
        !self.shared.link.borrow().closed
    }

    /// Closes the socket and stops the session; queued events stay readable.
    pub fn close(&self) {
        self.shared.shutdown();
    }

    /// Stops opting into cancel-on-disconnect on every later upgrade of this
    /// session, for every handle: no reconnect after this call returns sends
    /// `X-Cancel-On-Disconnect`, so none renews the account's
    /// cancel-on-disconnect, and none requires the server to confirm it. Use
    /// it in a shutdown or kill sequence, where a reconnect would otherwise
    /// postpone the cancel, with [`Self::cancel_all_after`] as the backstop.
    ///
    /// Each upgrade reads the switch once, before it builds its request. An
    /// upgrade that read it before this call keeps its opt-in, and is still
    /// refused unless the server confirms it; every upgrade that reads it
    /// after this call returns, the one that ends a pending retry wait
    /// included, opts out. The connection open now, and one such upgrade
    /// still in flight, stay opted in until they end; once the last opted-in
    /// connection of the account ends, its open orders are cancelled after
    /// the grace period unless another opted-in session is connected.
    ///
    /// It cannot be undone, by a reconnect or otherwise: open a new session
    /// to opt in again. Calling it again, or on a session configured without
    /// cancel-on-disconnect, changes nothing.
    pub fn disable_cancel_on_disconnect(&self) {
        self.shared
            .cancel_on_disconnect_disabled
            .store(true, Ordering::SeqCst);
    }

    async fn call(
        &self,
        command: Command,
        correlation_id: u64,
        deadline: Deadline,
        frame: Vec<u8>,
    ) -> Result<Answer> {
        self.call_timer(command, correlation_id, deadline, None, frame)
            .await
    }

    async fn call_timer(
        &self,
        command: Command,
        correlation_id: u64,
        deadline: Deadline,
        timer: Option<Duration>,
        frame: Vec<u8>,
    ) -> Result<Answer> {
        if self.shared.is_closed() {
            return Err(not_sent(command, NotSent::Closed));
        }
        let link = *self.shared.link.borrow();
        if link.closed {
            return Err(not_sent(command, NotSent::Closed));
        }
        if !link.ready {
            return Err(not_sent(command, NotSent::NotReady));
        }
        let (answer, rx) = oneshot::channel();
        self.shared
            .commands
            .try_send(Cmd {
                connection: link.connection,
                command,
                correlation_id,
                deadline,
                timer,
                frame,
                answer,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => not_sent(command, NotSent::QueueFull),
                mpsc::error::TrySendError::Closed(_) => not_sent(command, NotSent::Closed),
            })?;
        rx.await.unwrap_or_else(|_| {
            Err(Error::TradeSession(TradeError::OutcomeUnknown {
                command,
                correlation_id,
                reason: "the session ended".into(),
            }))
        })
    }

    /// Places one post-only GTC limit order. A lost answer is
    /// [`TradeError::OutcomeUnknown`], and a refusal with
    /// `ORDER_CREATE_OUTCOME_UNKNOWN` may have applied too: find the order by
    /// its client order ID once the deadline has passed.
    pub async fn create_order(
        &self,
        order: &PostOnlyOrder,
        deadline: Deadline,
    ) -> Result<OrderAck> {
        let correlation_id = new_correlation_id();
        let frame = codec::encode_create(order, correlation_id, &deadline)?;
        match self
            .call(Command::CreateOrder, correlation_id, deadline, frame)
            .await?
        {
            Answer::Order { ack, .. } => Ok(ack),
            other => Err(unexpected(Command::CreateOrder, &other)),
        }
    }

    /// Cancels one order. A lost answer is [`TradeError::OutcomeUnknown`], as
    /// is a refusal with `ORDER_CANCEL_OUTCOME_UNKNOWN`; a cancel is safe to
    /// send again.
    pub async fn cancel_order(
        &self,
        symbol_id: u32,
        order: &OrderRef,
        deadline: Deadline,
    ) -> Result<OrderAck> {
        let correlation_id = new_correlation_id();
        let frame = codec::encode_cancel(symbol_id, order, correlation_id, &deadline)?;
        match self
            .call(Command::CancelOrder, correlation_id, deadline, frame)
            .await?
        {
            Answer::Order { ack, .. } => Ok(ack),
            other => Err(unexpected(Command::CancelOrder, &other)),
        }
    }

    /// Modifies one order: amends it in place or replaces it with a successor,
    /// as [`ModifyOrder::behavior`] says. A lost answer is
    /// [`TradeError::OutcomeUnknown`], as is a refusal with
    /// `ORDER_MODIFY_OUTCOME_UNKNOWN`: resend the same [`ModifyOrder`], whose
    /// correlation ID is the modification's identity, or read the order back.
    pub async fn modify_order(&self, modify: &ModifyOrder, deadline: Deadline) -> Result<OrderAck> {
        let frame = codec::encode_modify(modify, &deadline)?;
        match self
            .call(Command::ModifyOrder, modify.correlation_id, deadline, frame)
            .await?
        {
            Answer::Order { ack, .. } => Ok(ack),
            other => Err(unexpected(Command::ModifyOrder, &other)),
        }
    }

    /// Cancels every open order of the account in the named pairs. A lost
    /// answer is [`TradeError::OutcomeUnknown`], as is a refusal with
    /// `ORDER_CANCEL_OUTCOME_UNKNOWN`: resend the same [`CancelAllOrders`]
    /// within two minutes, which requests no cancellation twice.
    pub async fn cancel_all_orders(
        &self,
        cancel: &CancelAllOrders,
        deadline: Deadline,
    ) -> Result<CancelAllAck> {
        let frame = codec::encode_cancel_all(cancel, &deadline)?;
        match self
            .call(
                Command::CancelAllOrders,
                cancel.correlation_id,
                deadline,
                frame,
            )
            .await?
        {
            Answer::CancelAll(ack) => Ok(ack),
            other => Err(unexpected(Command::CancelAllOrders, &other)),
        }
    }

    /// Arms the account's cancel-all-after timer for `timeout` (10 to 120
    /// whole seconds), refreshes an armed one, or disarms it with
    /// [`Duration::ZERO`]. The timer covers every pair and side. Writing a
    /// disarm closes this connection's timer gate at once, and writing an arm
    /// or refresh bounds it by `timeout` from now; an armed acknowledgement
    /// then opens it until the acknowledged expiry. A lost answer is [`TradeError::OutcomeUnknown`]:
    /// resending is safe.
    pub async fn cancel_all_after(
        &self,
        timeout: Duration,
        deadline: Deadline,
    ) -> Result<TimerAck> {
        let correlation_id = new_correlation_id();
        let frame = codec::encode_cancel_all_after(timeout, correlation_id, &deadline)?;
        match self
            .call_timer(
                Command::CancelAllAfter,
                correlation_id,
                deadline,
                Some(timeout),
                frame,
            )
            .await?
        {
            Answer::Timer(ack) => Ok(ack),
            other => Err(unexpected(Command::CancelAllAfter, &other)),
        }
    }

    /// Places up to 20 post-only orders. A lost answer is
    /// [`TradeError::OutcomeUnknown`], and a refusal with `UNAVAILABLE` or
    /// `ORDER_CREATE_OUTCOME_UNKNOWN` may follow the admission of some items:
    /// resend the same [`BatchCreateOrders`] to replay their results.
    pub async fn batch_create_orders(
        &self,
        batch: &BatchCreateOrders,
        deadline: Deadline,
    ) -> Result<BatchCreateAck> {
        let frame = codec::encode_batch_create(batch, &deadline)?;
        match self
            .call(
                Command::BatchCreateOrders,
                batch.correlation_id,
                deadline,
                frame,
            )
            .await?
        {
            Answer::BatchCreate(ack) => Ok(ack),
            other => Err(unexpected(Command::BatchCreateOrders, &other)),
        }
    }

    /// Modifies up to 50 orders of one pair. A lost answer, or a refusal with
    /// `UNAVAILABLE`, is an unknown outcome: resend the same
    /// [`BatchReplaceOrders`] to learn each item's result.
    pub async fn batch_replace_orders(
        &self,
        batch: &BatchReplaceOrders,
        deadline: Deadline,
    ) -> Result<BatchReplaceAck> {
        let frame = codec::encode_batch_replace(batch, &deadline)?;
        match self
            .call(
                Command::BatchReplaceOrders,
                batch.correlation_id,
                deadline,
                frame,
            )
            .await?
        {
            Answer::BatchReplace(ack) => Ok(ack),
            other => Err(unexpected(Command::BatchReplaceOrders, &other)),
        }
    }

    /// Cancels up to 50 orders. A lost answer, or a refusal with
    /// `UNAVAILABLE`, is an unknown outcome: resend the same
    /// [`BatchCancelOrders`] to request the rest.
    pub async fn batch_cancel_orders(
        &self,
        batch: &BatchCancelOrders,
        deadline: Deadline,
    ) -> Result<BatchCancelAck> {
        let frame = codec::encode_batch_cancel(batch, &deadline)?;
        match self
            .call(
                Command::BatchCancelOrders,
                batch.correlation_id,
                deadline,
                frame,
            )
            .await?
        {
            Answer::BatchCancel(ack) => Ok(ack),
            other => Err(unexpected(Command::BatchCancelOrders, &other)),
        }
    }
}

/// The writer matches every answer to its command's kind first; this is only
/// reached if that check is wrong.
fn unexpected(command: Command, answer: &Answer) -> Error {
    Error::response_contract(
        format!("tradews {command}"),
        format!("answered with {answer:?}"),
    )
}

/// Whether `answer` is the kind that answers `command`.
fn fits(command: Command, answer: &Answer) -> bool {
    use super::sbe::command_type::CommandType as T;
    match (command, answer) {
        (Command::CreateOrder, Answer::Order { command_type, .. }) => {
            *command_type == u8::from(T::CREATE_ORDER)
        }
        (Command::CancelOrder, Answer::Order { command_type, .. }) => {
            *command_type == u8::from(T::CANCEL_ORDER)
        }
        (Command::ModifyOrder, Answer::Order { command_type, .. }) => {
            *command_type == u8::from(T::MODIFY_ORDER)
        }
        (Command::CancelAllOrders, Answer::CancelAll(_))
        | (Command::BatchCreateOrders, Answer::BatchCreate(_))
        | (Command::BatchReplaceOrders, Answer::BatchReplace(_))
        | (Command::BatchCancelOrders, Answer::BatchCancel(_))
        | (Command::CancelAllAfter, Answer::Timer(_))
        | (_, Answer::Reject(_)) => true,
        _ => false,
    }
}

fn answer_correlation(answer: &Answer) -> u64 {
    match answer {
        Answer::Order { ack, .. } => ack.correlation_id,
        Answer::CancelAll(a) => a.correlation_id,
        Answer::Timer(a) => a.correlation_id,
        Answer::BatchCreate(a) => a.correlation_id,
        Answer::BatchReplace(a) => a.correlation_id,
        Answer::BatchCancel(a) => a.correlation_id,
        Answer::Reject(r) => r.correlation_id,
    }
}

/// Signs and opens one connection and, with a `cancel_on_disconnect` grace,
/// opts into cancel-on-disconnect and checks that the server confirmed it.
async fn dial(cfg: &Config, cancel_on_disconnect: Option<Duration>) -> Result<Ws> {
    let mut request = cfg
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| Error::validation(format!("tradews URL: {e}")))?;
    let headers = request.headers_mut();
    if let Ok(agent) = HeaderValue::from_str(&crate::user_agent::user_agent()) {
        headers.insert(USER_AGENT, agent);
    }
    for (name, value) in cfg.credentials.sign_websocket_upgrade(&cfg.url).await? {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::auth(format!("tradews signed header: {e}")))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|e| Error::auth(format!("tradews signed header: {e}")))?;
        headers.insert(name, value);
    }
    if let Some(grace) = cancel_on_disconnect {
        headers.insert(HEADER_COD_SCOPE, HeaderValue::from_static("ACCOUNT"));
        headers.insert(HEADER_COD_GRACE, HeaderValue::from(grace.as_secs()));
    }
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    // A refused upgrade is read whole, its body included, before it is
    // classified: see `super::upgrade`.
    let upgrade = async {
        let (stream, returned) = super::upgrade::open(&cfg.url).await?;
        match client_async_with_config(request, stream, Some(config)).await {
            Ok((mut ws, response)) => {
                ws.get_mut().keep();
                Ok((ws, response))
            }
            Err(tungstenite::Error::Http(response)) => {
                Err(upgrade_error(tungstenite::Error::Http(
                    super::upgrade::complete_refusal(response, returned).await,
                )))
            }
            Err(other) => Err(upgrade_error(other)),
        }
    };
    let (mut ws, response) = tokio::time::timeout(cfg.connect_timeout(), upgrade)
        .await
        .map_err(|_| Error::realtime("tradews connect timed out"))??;
    if cancel_on_disconnect.is_some() && !protected(response.headers()) {
        let _ = ws.close(None).await;
        return Err(Error::TradeSession(TradeError::NotProtected));
    }
    Ok(ws)
}

/// Whether a `101` echoes the cancel-on-disconnect opt-in with a grace
/// period; one without it leaves the session unprotected.
fn protected(headers: &HeaderMap) -> bool {
    let text = |name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
    };
    text(HEADER_COD_SCOPE).is_some_and(|v| v.eq_ignore_ascii_case("ACCOUNT"))
        && text(HEADER_COD_GRACE)
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|s| {
                (MIN_CANCEL_ON_DISCONNECT_GRACE.as_secs()
                    ..=MAX_CANCEL_ON_DISCONNECT_GRACE.as_secs())
                    .contains(&s)
            })
}

/// A refused upgrade's code: the body, as a bare code or JSON with `code`.
fn body_code(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
        && let Some(code) = value.get("code").and_then(|c| c.as_str())
    {
        return code.to_owned();
    }
    if !text.is_empty()
        && text.len() <= 64
        && text
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return text.to_owned();
    }
    String::new()
}

fn upgrade_error(e: tungstenite::Error) -> Error {
    match e {
        tungstenite::Error::Http(response) => {
            let status = response.status().as_u16();
            let code = response
                .body()
                .as_deref()
                .map(body_code)
                .unwrap_or_default();
            let answer = crate::errors::answer_from_headers(status, response.headers());
            if status == 401 || status == 403 {
                let window = response
                    .body()
                    .as_deref()
                    .and_then(crate::errors::TimestampWindow::from_problem_body);
                return crate::errors::gateway_rejection_error(status, &code)
                    .with_timestamp_window(window)
                    .with_answer(answer);
            }
            Error::TradeSession(TradeError::Upgrade {
                status,
                code,
                answer: Some(Box::new(answer)),
            })
        }
        other => Error::realtime(format!("tradews connect: {other}")),
    }
}

/// Whether a failure to connect stops the session instead of a retry.
fn fatal(e: &Error) -> bool {
    match e {
        Error::ApiKeyRejected { .. } | Error::Auth { .. } | Error::Validation(_) => true,
        Error::QueueOverflow(_) => true,
        Error::TradeSession(t) => match t {
            TradeError::NotProtected | TradeError::Schema { .. } => true,
            TradeError::Upgrade { status, .. } => matches!(status, 400 | 404),
            _ => false,
        },
        _ => false,
    }
}

/// A frame sent and not yet answered.
enum Pending {
    Subscribe,
    Ping,
    Command {
        command: Command,
        correlation_id: u64,
        answer: oneshot::Sender<Result<Answer>>,
    },
}

struct InFlight {
    pending: Pending,
    sent: Instant,
}

/// One connection's writer state.
struct Conn {
    number: u64,
    next_seq: u64,
    in_flight: BTreeMap<u64, InFlight>,
    ready: bool,
    /// The timer's expiry from the last armed acknowledgement on this
    /// connection, in Unix milliseconds (server clock); `None` when unarmed.
    armed_until_ms: Option<u64>,
}

impl Conn {
    fn new(number: u64) -> Self {
        Self {
            number,
            next_seq: 1,
            in_flight: BTreeMap::new(),
            ready: false,
            armed_until_ms: None,
        }
    }

    /// A timer change is about to be written: from now on the gate may not
    /// rely on a later expiry than the change can give. A disarm closes it;
    /// an arm or refresh bounds it by now plus its timeout (the clock margin
    /// covers the server applying it a little earlier on its own clock), so
    /// a shorter refresh tightens it at once. Only an acknowledgement opens
    /// it again.
    fn timer_written(&mut self, timeout: Duration) {
        self.armed_until_ms = if timeout.is_zero() {
            None
        } else {
            let now = unix_ms(SystemTime::now()).unwrap_or(0);
            self.armed_until_ms
                .map(|e| e.min(now.saturating_add(millis(timeout))))
        };
    }

    /// The timer state an acknowledgement for `seq` proves. With a later
    /// timer change still unanswered, it can only narrow the gate.
    fn timer_acknowledged(&mut self, seq: u64, expires_ms: Option<u64>) {
        let later = self.in_flight.iter().any(|(s, f)| {
            *s > seq
                && matches!(
                    f.pending,
                    Pending::Command {
                        command: Command::CancelAllAfter,
                        ..
                    }
                )
        });
        self.armed_until_ms = if later {
            match (self.armed_until_ms, expires_ms) {
                (Some(a), Some(b)) => Some(a.min(b)),
                _ => None,
            }
        } else {
            expires_ms
        };
    }

    fn commands_in_flight(&self) -> usize {
        self.in_flight
            .values()
            .filter(|f| matches!(f.pending, Pending::Command { .. }))
            .count()
    }

    /// Why `cmd` may not be written now, if it may not.
    fn refusal(&self, cmd: &Cmd, gate: Option<TimerGate>) -> Option<NotSent> {
        if cmd.connection != self.number {
            return Some(NotSent::Stale);
        }
        if !self.ready {
            return Some(NotSent::NotReady);
        }
        if self.commands_in_flight() >= MAX_IN_FLIGHT {
            return Some(NotSent::InFlightFull);
        }
        let now = unix_ms(SystemTime::now()).unwrap_or(0);
        let ends = cmd.deadline.expires_at_ms();
        if now > ends {
            return Some(NotSent::Expired);
        }
        if let Some(gate) = gate
            && cmd.command.places()
        {
            let armed = self.armed_until_ms.unwrap_or(0);
            let latest = ends.saturating_add(millis(gate.clock_margin));
            if latest >= armed.saturating_sub(millis(gate.guard)) {
                return Some(NotSent::NotArmed {
                    expires_at: self
                        .armed_until_ms
                        .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
                });
            }
        }
        None
    }

    /// The oldest frame unanswered past `timeout`, if any.
    fn overdue(&self, timeout: Duration) -> Option<String> {
        let (seq, flight) = self.in_flight.iter().next()?;
        if flight.sent.elapsed() <= timeout {
            return None;
        }
        Some(match &flight.pending {
            Pending::Subscribe => format!("tradews sent no SubscriptionAck within {timeout:?}"),
            Pending::Ping => format!("tradews sent no Pong to seq {seq} within {timeout:?}"),
            Pending::Command {
                command,
                correlation_id,
                ..
            } => format!(
                "tradews did not answer {command} seq {seq} (correlation {correlation_id}) within {timeout:?}"
            ),
        })
    }

    /// Ends every unanswered command as an unknown outcome; returns their
    /// correlation IDs.
    fn fail_in_flight(&mut self, reason: &str) -> Vec<u64> {
        let mut unanswered = Vec::new();
        for (_, flight) in std::mem::take(&mut self.in_flight) {
            if let Pending::Command {
                command,
                correlation_id,
                answer,
            } = flight.pending
            {
                unanswered.push(correlation_id);
                let _ = answer.send(Err(Error::TradeSession(TradeError::OutcomeUnknown {
                    command,
                    correlation_id,
                    reason: reason.to_owned(),
                })));
            }
        }
        unanswered
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// How a connection ended.
enum Exit {
    /// The session was closed or dropped.
    Stop,
    /// The session cannot go on.
    Fatal(Error),
    /// The connection is lost; the session reconnects.
    Lost(Error),
}

enum Emitted {
    Ok,
    /// The session was dropped.
    Gone,
    /// The consumer fell behind.
    Full,
}

fn emit(events: &mpsc::Sender<Event>, event: Event) -> Emitted {
    match events.try_send(event) {
        Ok(()) => Emitted::Ok,
        Err(mpsc::error::TrySendError::Full(_)) => Emitted::Full,
        Err(mpsc::error::TrySendError::Closed(_)) => Emitted::Gone,
    }
}

fn overflow() -> Error {
    Error::queue_overflow("tradews event queue is full: the consumer fell behind")
}

fn emit_or_exit(events: &mpsc::Sender<Event>, event: Event) -> Option<Exit> {
    match emit(events, event) {
        Emitted::Ok => None,
        Emitted::Gone => Some(Exit::Stop),
        Emitted::Full => Some(Exit::Fatal(overflow())),
    }
}

async fn run(
    shared: Arc<Shared>,
    ws: Ws,
    mut cmds: mpsc::Receiver<Cmd>,
    events: mpsc::Sender<Event>,
) {
    let mut backoff = ReconnectBackoff::new();
    let mut ws = ws;
    let mut connection = 0u64;
    let fatal_error = 'session: loop {
        connection += 1;
        let (mut error, healthy, mut unanswered) =
            match serve(&shared, ws, connection, &mut cmds, &events).await {
                End::Stop => break None,
                End::Fatal(e) => break Some(e),
                End::Lost {
                    error,
                    healthy,
                    unanswered,
                } => (error, healthy, unanswered),
            };
        if healthy {
            backoff.reset();
        }
        ws = loop {
            let delay = crate::realtime::reconnect_wait(backoff.next_delay(), &error);
            let event = Event::Disconnected {
                connection,
                error,
                delay,
                unanswered: std::mem::take(&mut unanswered),
            };
            match emit(&events, event) {
                Emitted::Ok => {}
                Emitted::Gone => break 'session None,
                Emitted::Full => break 'session Some(overflow()),
            }
            if wait(&shared, &mut cmds, tokio::time::sleep(delay))
                .await
                .is_none()
            {
                break 'session None;
            }
            // Only now, with the whole wait over, does the upgrade queue for
            // a slot of a shared limiter: the queue adds to the wait and
            // never shortens it. A close ends the queueing.
            let Some(slot) = wait(
                &shared,
                &mut cmds,
                upgrade_slot(shared.cfg.reconnect_limiter.as_ref()),
            )
            .await
            else {
                break 'session None;
            };
            // The shutdown fence again, right before the upgrade, and the
            // upgrade itself ends when the session closes: no upgrade starts
            // or completes after a close.
            if shared.is_closed() {
                break 'session None;
            }
            let mut stop = shared.stop.subscribe();
            // The switch is read here, once, after the waits: a
            // `disable_cancel_on_disconnect` that returned during them
            // applies to this upgrade.
            let cancel_on_disconnect = shared.cancel_on_disconnect();
            let dialed = tokio::select! {
                biased;
                () = stopped(&mut stop) => break 'session None,
                dialed = dial(&shared.cfg, cancel_on_disconnect) => dialed,
            };
            // The upgrade has ended, accepted, refused or timed out: its
            // slot goes back before anything else.
            drop(slot);
            match dialed {
                Ok(next) => break next,
                Err(e) if fatal(&e) => break 'session Some(e),
                Err(e) => error = e,
            }
        };
    };
    finish(&shared, &mut cmds, fatal_error);
}

/// Resolves once the session is closed. The guard `wait_for` returns is not
/// `Send`, so it is dropped here, before any other branch awaits.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}

/// Waits for `until` between connections, refusing every command as stale;
/// `None` when the session was closed meanwhile, which drops `until`.
async fn wait<T>(
    shared: &Shared,
    cmds: &mut mpsc::Receiver<Cmd>,
    until: impl std::future::Future<Output = T>,
) -> Option<T> {
    tokio::pin!(until);
    let mut stop = shared.stop.subscribe();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => return None,
            done = &mut until => return Some(done),
            cmd = cmds.recv() => match cmd {
                Some(c) => {
                    let reason = if shared.is_closed() { NotSent::Closed } else { NotSent::Stale };
                    let _ = c.answer.send(Err(not_sent(c.command, reason)));
                }
                None => return None,
            },
        }
    }
}

/// Stops the session: records its terminal error and refuses every command
/// still queued.
fn finish(shared: &Shared, cmds: &mut mpsc::Receiver<Cmd>, err: Option<Error>) {
    if let Some(err) = err {
        lock_unpoisoned(&shared.err).get_or_insert(err);
    }
    shared.link.send_modify(|l| {
        l.ready = false;
        l.closed = true;
    });
    cmds.close();
    while let Ok(c) = cmds.try_recv() {
        let _ = c.answer.send(Err(not_sent(c.command, NotSent::Closed)));
    }
}

enum End {
    Stop,
    Fatal(Error),
    Lost {
        error: Error,
        healthy: bool,
        unanswered: Vec<u64>,
    },
}

async fn serve(
    shared: &Shared,
    mut ws: Ws,
    number: u64,
    cmds: &mut mpsc::Receiver<Cmd>,
    events: &mpsc::Sender<Event>,
) -> End {
    let mut conn = Conn::new(number);
    shared.link.send_replace(Link {
        connection: number,
        ready: false,
        closed: false,
    });
    let exit = match emit_or_exit(events, Event::Connected { connection: number }) {
        Some(exit) => exit,
        None => connection(shared, &mut ws, &mut conn, cmds, events).await,
    };
    shared.link.send_modify(|l| l.ready = false);
    let reason = match &exit {
        Exit::Stop => "the session was closed".to_owned(),
        Exit::Fatal(e) | Exit::Lost(e) => format!("the connection ended: {e}"),
    };
    let unanswered = conn.fail_in_flight(&reason);
    if !matches!(exit, Exit::Lost(_)) {
        let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
    }
    match exit {
        Exit::Stop => End::Stop,
        Exit::Fatal(e) => End::Fatal(e),
        Exit::Lost(error) => End::Lost {
            error,
            healthy: conn.ready,
            unanswered,
        },
    }
}

/// Writes `frame` with the connection's next `seq`; it is in flight from
/// before the write, so a failed write leaves it unanswered.
async fn write<S>(
    ws: &mut S,
    conn: &mut Conn,
    pending: Pending,
    mut frame: Vec<u8>,
    timeout: Duration,
    stop: &mut watch::Receiver<bool>,
    closed: &AtomicBool,
) -> std::result::Result<u64, Exit>
where
    S: futures_util::Sink<Message, Error = tungstenite::Error> + Unpin,
{
    // The fence, right before the wire: a command the session was closed
    // before is refused, never written.
    if closed.load(Ordering::SeqCst) {
        if let Pending::Command {
            command, answer, ..
        } = pending
        {
            let _ = answer.send(Err(not_sent(command, NotSent::Closed)));
        }
        return Err(Exit::Stop);
    }
    let seq = conn.next_seq;
    conn.next_seq += 1;
    codec::set_seq(&mut frame, seq);
    conn.in_flight.insert(
        seq,
        InFlight {
            pending,
            sent: Instant::now(),
        },
    );
    // A peer that stops reading must not hold the writer: the write ends by
    // the oldest unanswered frame's answer deadline, or when the session is
    // closed, and the connection's end reports every frame that may have
    // been written as unanswered.
    let oldest = conn
        .in_flight
        .values()
        .map(|f| f.sent)
        .min()
        .expect("the frame is in flight");
    let until = tokio::time::Instant::from_std(oldest + timeout);
    tokio::select! {
        biased;
        () = stopped(stop) => return Err(Exit::Stop),
        sent = ws.send(Message::Binary(frame.into())) => {
            sent.map_err(|e| Exit::Lost(Error::realtime(format!("tradews send: {e}"))))?;
        }
        () = tokio::time::sleep_until(until) => {
            return Err(Exit::Lost(Error::realtime(format!(
                "tradews write of seq {seq} did not complete within the answer timeout of {timeout:?}"
            ))));
        }
    }
    Ok(seq)
}

async fn connection(
    shared: &Shared,
    ws: &mut Ws,
    conn: &mut Conn,
    cmds: &mut mpsc::Receiver<Cmd>,
    events: &mpsc::Sender<Event>,
) -> Exit {
    let cfg = &shared.cfg;
    let mut stop = shared.stop.subscribe();
    let subscribe = codec::encode_subscribe(0);
    if let Err(exit) = write(
        ws,
        conn,
        Pending::Subscribe,
        subscribe,
        cfg.answer_timeout,
        &mut stop,
        &shared.closed,
    )
    .await
    {
        return exit;
    }
    let check = (cfg.ping_interval.min(cfg.answer_timeout) / 4).max(Duration::from_millis(5));
    let mut tick = tokio::time::interval(check);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut last_ping = Instant::now();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => return Exit::Stop,
            frame = ws.next() => match frame {
                Some(Ok(Message::Binary(bytes))) => match codec::decode(&bytes) {
                    Ok(msg) => {
                        if let Some(exit) = on_message(shared, conn, msg, events) {
                            return exit;
                        }
                    }
                    Err(e @ Error::TradeSession(TradeError::Schema { .. })) => return Exit::Fatal(e),
                    Err(e) => return Exit::Lost(e),
                },
                Some(Ok(Message::Close(close))) => {
                    return Exit::Lost(Error::realtime(match close {
                        Some(c) => format!("tradews closed the connection: {} {}", u16::from(c.code), c.reason),
                        None => "tradews closed the connection".to_owned(),
                    }));
                }
                Some(Ok(Message::Text(_))) => {
                    return Exit::Lost(TradeError::frame(0, "a text message on a binary session"));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Exit::Lost(Error::realtime(format!("tradews read: {e}"))),
                None => return Exit::Lost(Error::realtime("tradews connection ended")),
            },
            _ = tick.tick() => {
                if let Some(reason) = conn.overdue(cfg.answer_timeout) {
                    return Exit::Lost(Error::realtime(reason));
                }
                let pinging = conn.in_flight.values().any(|f| matches!(f.pending, Pending::Ping));
                if !pinging && last_ping.elapsed() >= cfg.ping_interval {
                    last_ping = Instant::now();
                    if let Err(exit) = write(ws, conn, Pending::Ping, codec::encode_ping(0), cfg.answer_timeout, &mut stop, &shared.closed).await {
                        return exit;
                    }
                }
            }
            cmd = cmds.recv() => {
                let Some(cmd) = cmd else { return Exit::Stop };
                if shared.is_closed() {
                    let _ = cmd.answer.send(Err(not_sent(cmd.command, NotSent::Closed)));
                    return Exit::Stop;
                }
                if let Some(reason) = conn.refusal(&cmd, cfg.timer_gate) {
                    let _ = cmd.answer.send(Err(not_sent(cmd.command, reason)));
                    continue;
                }
                if let Some(timeout) = cmd.timer {
                    conn.timer_written(timeout);
                }
                let pending = Pending::Command {
                    command: cmd.command,
                    correlation_id: cmd.correlation_id,
                    answer: cmd.answer,
                };
                if let Err(exit) = write(ws, conn, pending, cmd.frame, cfg.answer_timeout, &mut stop, &shared.closed).await {
                    return exit;
                }
            }
        }
    }
}

fn on_message(
    shared: &Shared,
    conn: &mut Conn,
    msg: Msg,
    events: &mpsc::Sender<Event>,
) -> Option<Exit> {
    match msg {
        Msg::SubscriptionAck { seq, channels } => {
            if !matches!(
                conn.in_flight.get(&seq).map(|f| &f.pending),
                Some(Pending::Subscribe)
            ) {
                return Some(Exit::Lost(TradeError::frame(
                    23,
                    format!("a SubscriptionAck for seq {seq}, which is not the Subscribe"),
                )));
            }
            conn.in_flight.remove(&seq);
            if channels & CHANNELS != CHANNELS {
                return Some(Exit::Lost(TradeError::frame(
                    23,
                    format!(
                        "tradews confirmed channels {channels:#b}, not executions, orders and marketResync"
                    ),
                )));
            }
            conn.ready = true;
            shared.link.send_modify(|l| l.ready = true);
            emit_or_exit(
                events,
                Event::Ready {
                    connection: conn.number,
                },
            )
        }
        Msg::Pong { seq } => {
            if !matches!(
                conn.in_flight.get(&seq).map(|f| &f.pending),
                Some(Pending::Ping)
            ) {
                return None;
            }
            let flight = conn.in_flight.remove(&seq)?;
            emit_or_exit(
                events,
                Event::Pong {
                    round_trip: flight.sent.elapsed(),
                },
            )
        }
        Msg::Answer { seq, answer } => on_answer(conn, seq, answer),
        Msg::Order(update) => emit_or_exit(events, Event::Order(update)),
        Msg::Execution(execution) => emit_or_exit(events, Event::Execution(execution)),
        Msg::MarketResync(resync) => emit_or_exit(events, Event::MarketResync(resync)),
    }
}

fn on_answer(conn: &mut Conn, seq: u64, answer: Answer) -> Option<Exit> {
    let out_of_sequence =
        matches!(&answer, Answer::Reject(r) if r.code == RejectCode::SEQUENCE_OUT_OF_ORDER);
    let Some(flight) = conn.in_flight.remove(&seq) else {
        // An answer to no command of this connection, such as a refusal of a
        // frame the server could not read (seq 0): the command it belongs to,
        // if any, stays unanswered and ends the connection when overdue. A
        // connection out of sequence cannot go on.
        return out_of_sequence.then(|| {
            Exit::Lost(Error::realtime(
                "tradews refused a frame as SEQUENCE_OUT_OF_ORDER",
            ))
        });
    };
    let (command, correlation_id, tx) = match flight.pending {
        Pending::Command {
            command,
            correlation_id,
            answer,
        } => (command, correlation_id, answer),
        Pending::Subscribe => {
            let detail = match &answer {
                Answer::Reject(r) => format!("refused with {}", r.code),
                other => format!("answered with {other:?}"),
            };
            return Some(Exit::Lost(Error::realtime(format!(
                "tradews subscription {detail}"
            ))));
        }
        Pending::Ping => {
            return Some(Exit::Lost(TradeError::frame(
                0,
                format!("a Ping (seq {seq}) answered with {answer:?}"),
            )));
        }
    };
    if !fits(command, &answer) || answer_correlation(&answer) != correlation_id {
        // Put it back: the connection's end reports it as unknown.
        conn.in_flight.insert(
            seq,
            InFlight {
                pending: Pending::Command {
                    command,
                    correlation_id,
                    answer: tx,
                },
                sent: flight.sent,
            },
        );
        return Some(Exit::Lost(TradeError::frame(
            0,
            format!("{command} (seq {seq}, correlation {correlation_id}) answered with {answer:?}"),
        )));
    }
    let result = match answer {
        Answer::Reject(r) => Err(Error::TradeSession(TradeError::Rejected(Box::new(
            Reject {
                command,
                correlation_id,
                code: r.code,
                detail: r.detail,
                retry_after: r.retry_after,
                existing_order_id: r.existing_order_id,
                rejected_at: from_unix_ns(r.ts_ns),
            },
        )))),
        Answer::Timer(ack) => {
            conn.timer_acknowledged(seq, ack.expires_at.and_then(unix_ms));
            Ok(Answer::Timer(ack))
        }
        other => Ok(other),
    };
    let _ = tx.send(result);
    out_of_sequence.then(|| {
        Exit::Lost(Error::realtime(
            "tradews refused a frame as SEQUENCE_OUT_OF_ORDER",
        ))
    })
}

#[cfg(test)]
mod write_tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    /// A peer that never takes another byte.
    struct Stalled;

    impl futures_util::Sink<Message> for Stalled {
        type Error = tungstenite::Error;
        fn poll_ready(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }
        fn start_send(self: Pin<&mut Self>, _: Message) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }
        fn poll_close(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    fn command(conn: &mut Conn) -> oneshot::Receiver<Result<Answer>> {
        let (answer, rx) = oneshot::channel();
        // An earlier command, written 80 ms ago and unanswered.
        conn.in_flight.insert(
            conn.next_seq,
            InFlight {
                pending: Pending::Command {
                    command: Command::CreateOrder,
                    correlation_id: 7,
                    answer,
                },
                sent: Instant::now() - Duration::from_millis(80),
            },
        );
        conn.next_seq += 1;
        rx
    }

    #[tokio::test]
    async fn a_stalled_write_ends_by_the_oldest_answer_deadline() {
        let mut conn = Conn::new(1);
        conn.ready = true;
        let earlier = command(&mut conn);
        let (_stop_tx, mut stop) = watch::channel(false);
        let started = Instant::now();
        let ended = tokio::time::timeout(
            Duration::from_secs(2),
            write(
                &mut Stalled,
                &mut conn,
                Pending::Ping,
                codec::encode_ping(0),
                Duration::from_millis(100),
                &mut stop,
                &AtomicBool::new(false),
            ),
        )
        .await
        .expect("the write gives up by the oldest command's answer deadline");
        assert!(matches!(ended, Err(Exit::Lost(_))));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        // The connection's end reports what may have been written as unknown.
        assert_eq!(conn.fail_in_flight("the write stalled"), [7]);
        assert!(matches!(
            earlier.await.unwrap(),
            Err(Error::TradeSession(TradeError::OutcomeUnknown {
                correlation_id: 7,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn closing_the_session_ends_a_stalled_write() {
        let mut conn = Conn::new(1);
        let (stop_tx, mut stop) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stop_tx.send_replace(true);
        });
        let ended = tokio::time::timeout(
            Duration::from_secs(2),
            write(
                &mut Stalled,
                &mut conn,
                Pending::Ping,
                codec::encode_ping(0),
                Duration::from_secs(60),
                &mut stop,
                &AtomicBool::new(false),
            ),
        )
        .await
        .expect("closing ends the write");
        assert!(matches!(ended, Err(Exit::Stop)));
    }

    #[test]
    fn an_older_timer_answer_never_widens_past_a_later_change() {
        let mut conn = Conn::new(1);
        let now = unix_ms(SystemTime::now()).unwrap();
        conn.armed_until_ms = Some(now + 60_000);
        // A disarm is written (seq 5) while an arm's answer (seq 4) is due.
        let (answer, _rx) = oneshot::channel();
        conn.in_flight.insert(
            5,
            InFlight {
                pending: Pending::Command {
                    command: Command::CancelAllAfter,
                    correlation_id: 9,
                    answer,
                },
                sent: Instant::now(),
            },
        );
        conn.timer_written(Duration::ZERO);
        assert_eq!(conn.armed_until_ms, None);
        conn.timer_acknowledged(4, Some(now + 120_000));
        assert_eq!(conn.armed_until_ms, None, "the disarm still stands");
        conn.in_flight.remove(&5);
        conn.timer_acknowledged(5, None);
        assert_eq!(conn.armed_until_ms, None);
        // A refresh to 10 s bounds an armed 60 s gate at once.
        conn.armed_until_ms = Some(now + 60_000);
        conn.timer_written(Duration::from_secs(10));
        assert!(conn.armed_until_ms.unwrap() <= unix_ms(SystemTime::now()).unwrap() + 10_000);
        conn.timer_acknowledged(6, Some(now + 10_050));
        assert_eq!(conn.armed_until_ms, Some(now + 10_050));
    }

    /// One command is mid-write on a peer that takes nothing, three wait in
    /// the queue behind it, and the session is closed: the worker ends the
    /// write and the session as it does on any stop.
    #[tokio::test]
    async fn a_close_behind_a_stalled_write_refuses_the_queue() {
        let credentials = crate::auth::Credentials::new(
            "1",
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
        )
        .unwrap();
        let (commands, mut cmds) = mpsc::channel(8);
        let (link, _) = watch::channel(Link::default());
        let (stop, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            cfg: Config::new("ws://127.0.0.1:9/ws/trade.sbe", credentials),
            commands,
            link,
            stop,
            err: Mutex::new(None),
            closed: AtomicBool::new(false),
            cancel_on_disconnect_disabled: AtomicBool::new(false),
        });
        let mut stop = shared.stop.subscribe();
        let mut conn = Conn::new(1);
        conn.ready = true;
        let deadline = Deadline::within(Duration::from_secs(5)).unwrap();
        let queued: Vec<_> = (2..=4)
            .map(|correlation_id| {
                let (answer, rx) = oneshot::channel();
                shared
                    .commands
                    .try_send(Cmd {
                        connection: 1,
                        command: Command::CancelOrder,
                        correlation_id,
                        deadline,
                        timer: None,
                        frame: codec::encode_ping(0),
                        answer,
                    })
                    .unwrap();
                rx
            })
            .collect();
        let (answer, mid_write) = oneshot::channel();
        let closer = shared.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            closer.shutdown();
        });
        let ended = tokio::time::timeout(
            Duration::from_secs(2),
            write(
                &mut Stalled,
                &mut conn,
                Pending::Command {
                    command: Command::CancelOrder,
                    correlation_id: 1,
                    answer,
                },
                codec::encode_ping(0),
                Duration::from_secs(60),
                &mut stop,
                &shared.closed,
            ),
        )
        .await
        .expect("the close ends the stalled write");
        assert!(matches!(ended, Err(Exit::Stop)));
        // What the worker does on a stop: the connection's unanswered frames,
        // then the session's queue.
        assert_eq!(conn.fail_in_flight("the session was closed"), [1]);
        finish(&shared, &mut cmds, None);
        assert!(matches!(
            mid_write.await.unwrap(),
            Err(Error::TradeSession(TradeError::OutcomeUnknown {
                correlation_id: 1,
                ..
            }))
        ));
        for rx in queued {
            assert!(matches!(
                rx.await.unwrap(),
                Err(Error::TradeSession(TradeError::NotSent {
                    reason: NotSent::Closed,
                    ..
                }))
            ));
        }
        // A write the session was closed before is refused, never written.
        let (answer, late) = oneshot::channel();
        let refused = write(
            &mut Stalled,
            &mut conn,
            Pending::Command {
                command: Command::CancelOrder,
                correlation_id: 5,
                answer,
            },
            codec::encode_ping(0),
            Duration::from_secs(60),
            &mut stop,
            &shared.closed,
        )
        .await;
        assert!(matches!(refused, Err(Exit::Stop)));
        assert!(conn.in_flight.is_empty());
        assert!(matches!(
            late.await.unwrap(),
            Err(Error::TradeSession(TradeError::NotSent {
                reason: NotSent::Closed,
                ..
            }))
        ));
    }
}
