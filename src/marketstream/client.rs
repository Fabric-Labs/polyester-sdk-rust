//! The connection: subscribe, heartbeat, reconnect and event delivery.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::USER_AGENT;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use super::backfill::{Pacing, TradeHistory, backfill_paced};
use super::book::{Book, OrderBook, Refusal};
use super::codec::{self, Msg, WireLevel};
use super::sbe;
use super::trades::Tracker;
use super::{
    BestBidOffer, DepthSnapshot, DepthUpdate, Event, FeedState, Level, MarketStatus, PATH, Pong,
    Renumbered, Side, Stream, StreamError, SubscribeResponse, SubscribeResult, Topic, TopicResult,
    Trade, Trades, TradesBackfill, TradesGap, TradesThrough,
};
use crate::errors::{Error, Result};
use crate::realtime::{ReconnectBackoff, lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use crate::reconnect::{ReconnectLimiter, upgrade_slot};
use crate::types::{Price, Quantity, QuantityDomain};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// What the client needs to know about a trading pair: its base quantity
/// scale, which every quantity on the stream is scaled by, and its symbol.
/// [`crate::catalogs::Manager`] implements it.
pub trait Pairs: Send + Sync {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32>;
    fn symbol(&self, symbol_id: u32) -> Option<String>;
    /// The numeric ID of the pair `symbol`.
    fn symbol_id(&self, symbol: &str) -> Option<u32>;
}

impl Pairs for crate::catalogs::Manager {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32> {
        self.base_quantity_scale_for_symbol_id(symbol_id)
    }
    fn symbol(&self, symbol_id: u32) -> Option<String> {
        self.symbol_for_symbol_id(symbol_id)
    }
    fn symbol_id(&self, symbol: &str) -> Option<u32> {
        self.symbol_id_for_symbol(symbol)
    }
}

/// Reads the spot pairs afresh: the client calls it on every connection,
/// before it subscribes anything on it.
pub trait PairsSource: Send + Sync {
    /// Reads the pairs without making them current anywhere.
    fn load(&self) -> futures_util::future::BoxFuture<'_, Result<LoadedPairs>>;
}

type Install = Box<dyn FnOnce() -> Result<Arc<dyn Pairs>> + Send>;

/// Pairs a [`PairsSource`] read for one connection. The stream installs them
/// only while that connection is current: it runs their install step, which
/// makes them current wherever else they are read (`Client::market_stream`
/// refreshes the client's catalogs, which its trade history reads), and
/// decodes with the pairs the step returns.
pub struct LoadedPairs {
    pairs: Arc<dyn Pairs>,
    install: Option<Install>,
}

impl LoadedPairs {
    /// Pairs that are current once the stream decodes with them.
    pub fn new(pairs: Arc<dyn Pairs>) -> Self {
        Self {
            pairs,
            install: None,
        }
    }

    /// Pairs whose install step `install` makes them current and returns the
    /// pairs to decode with. A failed step installs nothing and is retried
    /// as a failed read.
    pub fn with_install(
        pairs: Arc<dyn Pairs>,
        install: impl FnOnce() -> Result<Arc<dyn Pairs>> + Send + 'static,
    ) -> Self {
        Self {
            pairs,
            install: Some(Box::new(install)),
        }
    }

    /// The pairs read.
    pub fn pairs(&self) -> &Arc<dyn Pairs> {
        &self.pairs
    }

    fn install(self) -> Result<Arc<dyn Pairs>> {
        match self.install {
            Some(install) => install(),
            None => Ok(self.pairs),
        }
    }
}

impl std::fmt::Debug for LoadedPairs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedPairs").finish_non_exhaustive()
    }
}

/// Configures a [`MarketStream`].
#[derive(Clone)]
pub struct Config {
    /// The stream's WebSocket URL; [`url`] builds it from the API's WebSocket
    /// host.
    pub url: String,
    /// Pair scales and symbols, usually the client's catalogs.
    pub pairs: Arc<dyn Pairs>,
    /// Re-reads the pairs on every connection.
    pub reload: Option<Arc<dyn PairsSource>>,
    /// When set, every [`TradesGap`] is recovered from the trade history API
    /// in the background and its outcome delivered as an
    /// [`Event::TradesBackfill`].
    pub history: Option<Arc<dyn TradeHistory>>,
    /// Bounds the event queue. A consumer that falls behind by more fails the
    /// stream with [`Error::QueueOverflow`] instead of losing messages.
    /// 0: 1000.
    pub queue_size: usize,
    /// How often the client pings the server; a pong must answer within
    /// `pong_timeout` or the client reconnects. Zero: 15 and 10 seconds.
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    /// Permit a `ws://` URL on a host that is not loopback.
    pub allow_insecure_http: bool,
    /// Shares a cap on concurrent upgrades with every other stream and
    /// session given the same limiter ([`ReconnectLimiter`]): each upgrade,
    /// the first included, waits for a slot once its backoff and any
    /// `Retry-After` have run out, and holds it only until the upgrade ends.
    /// `None`, the default, connects as soon as the stream's own wait ends.
    pub reconnect_limiter: Option<ReconnectLimiter>,
    pub(crate) pacing: Pacing,
    /// How long an upgrade may take before it fails and is retried.
    #[cfg(test)]
    pub(crate) connect_timeout: Duration,
    /// Holds the stream's task between polls, to schedule races in tests.
    #[cfg(test)]
    pub(crate) gate: Option<Arc<SchedGate>>,
}

impl Config {
    pub fn new(url: impl Into<String>, pairs: Arc<dyn Pairs>) -> Self {
        Self {
            url: url.into(),
            pairs,
            reload: None,
            history: None,
            queue_size: 0,
            ping_interval: Duration::ZERO,
            pong_timeout: Duration::ZERO,
            allow_insecure_http: false,
            reconnect_limiter: None,
            pacing: Pacing::default(),
            #[cfg(test)]
            connect_timeout: CONNECT_TIMEOUT,
            #[cfg(test)]
            gate: None,
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
            .field("history", &self.history.is_some())
            .field("queue_size", &self.queue_size)
            .field("ping_interval", &self.ping_interval)
            .field("pong_timeout", &self.pong_timeout)
            .field("reconnect_limiter", &self.reconnect_limiter)
            .finish_non_exhaustive()
    }
}

/// The market-data stream URL on the WebSocket host `base`, such as
/// `wss://api.testnet.polyester.com`.
pub fn url(base: &str) -> Result<String> {
    let mut u = url::Url::parse(base.trim())
        .map_err(|e| Error::validation(format!("market stream base URL: {e}")))?;
    if u.scheme() != "wss" && u.scheme() != "ws" {
        return Err(Error::validation(
            "market stream base URL must use wss or ws",
        ));
    }
    if u.host_str().is_none_or(str::is_empty) {
        return Err(Error::validation("market stream base URL has no host"));
    }
    if u.query().is_some() {
        return Err(Error::validation(
            "market stream base URL must not contain a query string",
        ));
    }
    if u.fragment().is_some() {
        return Err(Error::validation(
            "market stream base URL must not contain a fragment",
        ));
    }
    let path = u.path().trim_end_matches('/');
    let path = path.strip_suffix("/connection/websocket").unwrap_or(path);
    let path = if path.ends_with(PATH) {
        path.to_owned()
    } else {
        format!("{path}{PATH}")
    };
    u.set_path(&path);
    Ok(u.to_string())
}

const DEFAULT_QUEUE: usize = 1000;
const DEFAULT_PING: Duration = Duration::from_secs(15);
const DEFAULT_PONG: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds one server message: a full book of the widest window fits.
const MAX_MESSAGE_BYTES: usize = 4 << 20;

/// A connection to the market-data stream that reconnects and resubscribes on
/// its own. [`Self::recv`] delivers every stream's messages; the book and
/// quote accessors read the latest state. Dropping it closes it.
pub struct MarketStream {
    rx: mpsc::Receiver<Event>,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

pub(crate) struct Shared {
    cfg: Config,
    /// The pairs every message is decoded with: [`Config::pairs`], then the
    /// pairs last read for the current connection ([`Config::reload`]).
    pairs: RwLock<Arc<dyn Pairs>>,
    /// The current connection's number; a read of the pairs for any other
    /// installs nothing.
    generation: AtomicU64,
    ids: AtomicU64,
    /// Set once a connection delivered a message, so the next reconnect
    /// starts from the shortest backoff.
    healthy: AtomicBool,
    stop: watch::Sender<bool>,
    inner: Mutex<Inner>,
    state: RwLock<State>,
}

#[derive(Default)]
struct Inner {
    closed: bool,
    err: Option<Error>,
    events: Option<mpsc::Sender<Event>>,
    /// The current connection's outbound queue; `None` between connections.
    /// A request is given its ID and queued under this lock, so the server
    /// receives requests in ID order.
    out: Option<mpsc::UnboundedSender<Vec<u8>>>,
    /// The current connection waits for its read of the pairs: it subscribes
    /// nothing, and a request is held as between connections.
    paused: bool,
    /// The topics the caller wants held, in the order first asked: the intent
    /// every topic's owner moves the server toward. A topic leaves it when the
    /// caller unsubscribes or the server refuses it.
    wanted: Vec<Topic>,
    /// The pair each wanted topic was asked for, by symbol: the caller's
    /// intent. A topic's pair ID is only the current connection's mapping
    /// of it, and a new connection maps it again from its own catalog.
    symbols: HashMap<Topic, String>,
    waiting: HashMap<u64, oneshot::Sender<Result<SubscribeResponse>>>,
    /// Every Subscribe and Unsubscribe sent on the current connection, by
    /// request ID: its kind and topics. A response is matched to it by ID
    /// alone; a caller that stops waiting leaves it in place, as the server
    /// acts on the request regardless.
    sent: HashMap<u64, Sent>,
    /// Each topic's subscription on the current connection.
    owners: HashMap<Topic, Owner>,
    /// The ping owed its pong.
    ping: Option<(u64, Instant)>,
    backfills: Vec<AbortHandle>,
}

/// A Subscribe or Unsubscribe sent on the current connection.
struct Sent {
    unsubscribe: bool,
    topics: Vec<Topic>,
}

/// One topic's subscription on the current connection. Every request for the
/// topic, the caller's, a resubscription and a recovery's, goes through it:
/// the request it sent last is its generation, and only the answer to that
/// request is applied. With no request outstanding, the owner moves the
/// server toward the caller's intent ([`Shared::reconcile`]).
#[derive(Debug, Default)]
struct Owner {
    /// The ID of the request whose answer the owner awaits; 0 when none is
    /// outstanding.
    generation: u64,
    /// The server holds the topic, as of the last answer received, whether
    /// or not it was applied.
    held: bool,
    /// The held subscription had a gap, or misses its initial state, and
    /// must be ended and made again.
    recovering: bool,
}

impl Inner {
    /// Gives a request the next ID, records it, makes it its topics'
    /// generation and queues it; the caller holds the lock throughout, so
    /// requests reach the wire in the order of their IDs. `None` without a
    /// connection.
    fn send(&mut self, ids: &AtomicU64, unsubscribe: bool, topics: &[Topic]) -> Option<u64> {
        if self.paused {
            return None;
        }
        let out = self.out.as_ref()?;
        let id = ids.fetch_add(1, Ordering::SeqCst) + 1;
        let frame = if unsubscribe {
            codec::encode_unsubscribe(id, topics)
        } else {
            codec::encode_subscribe(id, topics)
        };
        // A connection that is gone reconnects every stream; its end clears
        // what is recorded here.
        let _ = out.send(frame);
        self.sent.insert(
            id,
            Sent {
                unsubscribe,
                topics: topics.to_vec(),
            },
        );
        for t in topics {
            self.owners.entry(*t).or_default().generation = id;
        }
        Some(id)
    }
}

#[derive(Default)]
struct State {
    books: HashMap<Topic, Book>,
    quotes: HashMap<u32, BestBidOffer>,
    feeds: HashMap<u32, FeedState>,
    trackers: HashMap<u32, Tracker>,
    /// The topics the current connection holds.
    active: HashSet<Topic>,
    /// The active topics whose current subscription delivered its initial
    /// state: a depth or depth20 snapshot, a quote, or the trades baseline.
    initial: HashSet<Topic>,
}

/// Connects to the market-data stream. A failure of this first connection is
/// returned; once connected, the stream reconnects on its own until closed.
/// Subscribe with [`MarketStream::subscribe`].
pub async fn connect(mut config: Config) -> Result<MarketStream> {
    if config.url.is_empty() {
        return Err(Error::validation("market stream Config.url is required"));
    }
    crate::transport::validate_ws_url(&config.url, config.allow_insecure_http)?;
    if config.queue_size == 0 {
        config.queue_size = DEFAULT_QUEUE;
    }
    if config.ping_interval.is_zero() {
        config.ping_interval = DEFAULT_PING;
    }
    if config.pong_timeout.is_zero() {
        config.pong_timeout = DEFAULT_PONG;
    }
    let (events, rx) = mpsc::channel(config.queue_size);
    let (stop, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        pairs: RwLock::new(config.pairs.clone()),
        generation: AtomicU64::new(0),
        cfg: config,
        ids: AtomicU64::new(0),
        healthy: AtomicBool::new(false),
        stop,
        inner: Mutex::new(Inner {
            events: Some(events),
            ..Inner::default()
        }),
        state: RwLock::new(State::default()),
    });
    // The first upgrade takes a slot of a shared limiter too, so the cap
    // holds for streams started together as for streams dropped together.
    let slot = upgrade_slot(shared.cfg.reconnect_limiter.as_ref()).await;
    let ws = shared.dial().await;
    drop(slot);
    let ws = ws?;
    // The first connection's pairs are read before connect returns, so a
    // subscribe right after it uses them.
    if let Some(source) = &shared.cfg.reload {
        let pairs = source.load().await?.install()?;
        *write_unpoisoned(&shared.pairs) = pairs;
    }
    // Subscribe may run as soon as connect returns: the connection is the
    // client's before then.
    let out_rx = shared.attach(false).ok_or_else(closed)?;
    #[cfg(test)]
    let task = match shared.cfg.gate.clone() {
        Some(gate) => tokio::spawn(Gated {
            gate,
            inner: Box::pin(run(shared.clone(), ws, out_rx)),
        }),
        None => tokio::spawn(run(shared.clone(), ws, out_rx)),
    };
    #[cfg(not(test))]
    let task = tokio::spawn(run(shared.clone(), ws, out_rx));
    Ok(MarketStream { rx, shared, task })
}

/// Lets a test stop polling the stream's task, so timers become ready while
/// it waits, and resume it.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct SchedGate {
    held: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

#[cfg(test)]
impl SchedGate {
    pub(crate) fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    pub(crate) fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        if let Some(waker) = lock_unpoisoned(&self.waker).take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
struct Gated<F> {
    gate: Arc<SchedGate>,
    inner: std::pin::Pin<Box<F>>,
}

#[cfg(test)]
impl<F: std::future::Future<Output = ()>> std::future::Future for Gated<F> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.gate.held.load(Ordering::SeqCst) {
            *lock_unpoisoned(&self.gate.waker) = Some(cx.waker().clone());
            return std::task::Poll::Pending;
        }
        self.inner.as_mut().poll(cx)
    }
}

fn closed() -> Error {
    Error::realtime("market stream client is closed")
}

impl MarketStream {
    /// The next message, in order. `None` once the stream has stopped: after
    /// [`Self::close`], or a terminal error that [`Self::err`] reports.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// The terminal error that stopped the stream: [`Error::QueueOverflow`],
    /// or [`Error::MarketStream`] with [`StreamError::Schema`] for a server
    /// this client cannot read.
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.shared.inner).err.clone()
    }

    /// Whether the background task still runs.
    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    /// Stops the stream; queued events stay readable.
    pub fn close(&self) {
        self.shared.shutdown(None);
    }

    /// Subscribes to topics and returns one result per topic, in order. Topics
    /// the server accepts are resubscribed on every reconnect; those it
    /// refuses are dropped. Each topic's pair must be known to
    /// [`Config::pairs`].
    pub async fn subscribe(&self, topics: &[Topic]) -> Result<Vec<TopicResult>> {
        if topics.is_empty() {
            return Err(Error::validation(
                "market stream subscribe needs at least one topic",
            ));
        }
        for t in topics {
            if self
                .shared
                .pairs()
                .base_quantity_scale(t.symbol_id)
                .is_none()
            {
                return Err(Error::validation(format!(
                    "market stream topic {t}: the pair's base quantity scale is unknown; load the catalogs first"
                )));
            }
        }
        self.requests(topics, false).await
    }

    /// Ends subscriptions and returns one result per topic, in order:
    /// [`SubscribeResult::Ok`] when the stream ended,
    /// [`SubscribeResult::NotSubscribed`] when the connection did not hold it.
    /// From the moment the request is sent, the topics are not resubscribed
    /// on reconnect and their books, quotes and trade accounting are dropped,
    /// so subscribing again starts afresh.
    pub async fn unsubscribe(&self, topics: &[Topic]) -> Result<Vec<TopicResult>> {
        if topics.is_empty() {
            return Err(Error::validation(
                "market stream unsubscribe needs at least one topic",
            ));
        }
        self.requests(topics, true).await
    }

    /// Ends every stream of a pair the client subscribed to, as
    /// [`Self::unsubscribe`] does; no results when it holds none.
    pub async fn unsubscribe_symbol(&self, symbol_id: u32) -> Result<Vec<TopicResult>> {
        let mut topics: Vec<Topic> = lock_unpoisoned(&self.shared.inner)
            .wanted
            .iter()
            .copied()
            .filter(|t| t.symbol_id == symbol_id)
            .collect();
        {
            let state = read_unpoisoned(&self.shared.state);
            for s in Stream::ALL {
                let t = Topic::new(symbol_id, s);
                if state.active.contains(&t) && !topics.contains(&t) {
                    topics.push(t);
                }
            }
        }
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        self.unsubscribe(&topics).await
    }

    async fn requests(&self, topics: &[Topic], unsubscribe: bool) -> Result<Vec<TopicResult>> {
        let mut results = Vec::with_capacity(topics.len());
        for chunk in topics.chunks(codec::MAX_ITEMS) {
            results.extend(self.shared.request(chunk, unsubscribe).await?.results);
        }
        Ok(results)
    }

    /// A copy of a pair's depth20 or depth book, up to `levels` per side (all
    /// when 0); `None` before its first snapshot.
    pub fn order_book(&self, symbol_id: u32, stream: Stream, levels: usize) -> Option<OrderBook> {
        let state = read_unpoisoned(&self.shared.state);
        let b = state
            .books
            .get(&Topic::new(symbol_id, stream))
            .filter(|b| b.has)?;
        let take = |n: usize| if levels > 0 { n.min(levels) } else { n };
        Some(OrderBook {
            symbol_id,
            symbol: self.shared.pairs().symbol(symbol_id),
            stream,
            book_epoch: b.epoch,
            update_id: b.update_id,
            levels_per_side: b.window,
            bids: self.shared.levels(symbol_id, &b.bids[..take(b.bids.len())]),
            asks: self.shared.levels(symbol_id, &b.asks[..take(b.asks.len())]),
            stale: !b.synced || state.feeds.get(&symbol_id) == Some(&FeedState::Stale),
        })
    }

    /// A pair's latest best bid and offer; `None` before the first.
    pub fn best_bid_offer(&self, symbol_id: u32) -> Option<BestBidOffer> {
        let state = read_unpoisoned(&self.shared.state);
        let mut quote = state.quotes.get(&symbol_id).cloned()?;
        quote.stale |= state.feeds.get(&symbol_id) == Some(&FeedState::Stale);
        Some(quote)
    }

    /// Whether a topic is current: the connection holds it, its state arrived
    /// from the subscription it holds now (a depth book's snapshot, a quote,
    /// the trades stream's baseline) and the pair's feed is not stale. A lost connection
    /// makes every topic not live at once, before its
    /// [`Event::Disconnected`].
    pub fn live(&self, topic: Topic) -> bool {
        self.shared.live(topic)
    }

    /// A pair's feed state as last reported; [`FeedState::Live`] unless the
    /// server reported otherwise.
    pub fn feed(&self, symbol_id: u32) -> FeedState {
        read_unpoisoned(&self.shared.state)
            .feeds
            .get(&symbol_id)
            .copied()
            .unwrap_or(FeedState::Live)
    }
}

impl Shared {
    /// [`MarketStream::live`].
    fn live(&self, topic: Topic) -> bool {
        let state = read_unpoisoned(&self.state);
        if !state.active.contains(&topic)
            || !state.initial.contains(&topic)
            || state.feeds.get(&topic.symbol_id) == Some(&FeedState::Stale)
        {
            return false;
        }
        match topic.stream {
            Stream::Trades => state
                .trackers
                .get(&topic.symbol_id)
                .is_some_and(Tracker::started),
            Stream::Bbo => state.quotes.get(&topic.symbol_id).is_some_and(|q| !q.stale),
            Stream::Depth20 | Stream::Depth => state.books.get(&topic).is_some_and(|b| b.synced),
        }
    }
}

impl Drop for MarketStream {
    fn drop(&mut self) {
        self.shared.shutdown(None);
        self.task.abort();
    }
}

impl Shared {
    async fn dial(&self) -> Result<Ws> {
        let mut request = self
            .cfg
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| Error::realtime(format!("market stream request: {e}")))?;
        if let Ok(agent) = HeaderValue::from_str(&crate::user_agent::user_agent()) {
            request.headers_mut().insert(USER_AGENT, agent);
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));
        let (ws, _) = tokio::time::timeout(
            self.cfg.connect_timeout(),
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| Error::realtime("market stream connect timed out"))?
        .map_err(|e| match e {
            // A refused upgrade's answer: its status and the delay it asks
            // for, which the next connection waits at least.
            tokio_tungstenite::tungstenite::Error::Http(response) => Error::Realtime {
                message: format!(
                    "market stream connect: the upgrade was refused (HTTP {})",
                    response.status().as_u16()
                ),
                answer: Some(Box::new(crate::errors::answer_from_headers(
                    response.status().as_u16(),
                    response.headers(),
                ))),
            },
            other => Error::realtime(format!("market stream connect: {other}")),
        })?;
        Ok(ws)
    }

    /// Makes a new connection the client's; `None` when the client is closed.
    /// The session subscribes what is wanted then, and a request after this
    /// queues its topics on the connection itself: each topic is sent once,
    /// by whichever its owner records first.
    /// `paused`: the connection subscribes nothing until its read of the
    /// pairs is installed ([`Self::install`]).
    fn attach(&self, paused: bool) -> Option<mpsc::UnboundedReceiver<Vec<u8>>> {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.closed || inner.err.is_some() {
            return None;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        inner.out = Some(tx);
        inner.ping = None;
        inner.paused = paused;
        self.generation.fetch_add(1, Ordering::SeqCst);
        Some(rx)
    }

    /// The pairs messages are decoded with now.
    fn pairs(&self) -> Arc<dyn Pairs> {
        read_unpoisoned(&self.pairs).clone()
    }

    /// Installs pairs read for connection `generation` and ends its pause;
    /// `None`, installing nothing, when another connection has superseded it
    /// or the stream stopped. Every wanted topic follows its pair by symbol
    /// to the pair's new ID; one whose pair is gone ends. Each pair whose ID,
    /// symbol or scale changed loses its state, so no new ID is ever read
    /// with an old pair's symbol or scale. Returns the events to deliver.
    fn install(&self, generation: u64, loaded: LoadedPairs) -> Result<Option<Vec<Event>>> {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.closed
            || inner.err.is_some()
            || self.generation.load(Ordering::SeqCst) != generation
        {
            return Ok(None);
        }
        // Everything is compared with the pairs read before their install
        // step runs, which may change the catalog the old pairs are read
        // from.
        let old = self.pairs();
        let pairs = loaded.pairs.clone();
        let mut renumbered: Vec<Renumbered> = Vec::new();
        let mut ended = Vec::new();
        let mut wanted: Vec<Topic> = Vec::new();
        let mut symbols = HashMap::new();
        for topic in &inner.wanted {
            // The caller's intent, else what the connection's own catalog
            // named the pair.
            let symbol = inner
                .symbols
                .get(topic)
                .cloned()
                .or_else(|| old.symbol(topic.symbol_id));
            let Some(id) = symbol.as_deref().and_then(|s| pairs.symbol_id(s)) else {
                ended.push((*topic, symbol));
                continue;
            };
            if id != topic.symbol_id
                && let Some(symbol) = symbol
                && !renumbered.iter().any(|r| r.symbol == symbol)
            {
                renumbered.push(Renumbered {
                    symbol,
                    from: topic.symbol_id,
                    to: id,
                });
            }
            let moved = Topic::new(id, topic.stream);
            if !wanted.contains(&moved) {
                wanted.push(moved);
            }
            if let Some(symbol) = pairs.symbol(id) {
                symbols.insert(moved, symbol);
            }
        }
        let changed: Vec<u32> = {
            let state = read_unpoisoned(&self.state);
            let held: HashSet<u32> = state
                .books
                .keys()
                .map(|t| t.symbol_id)
                .chain(state.quotes.keys().copied())
                .chain(state.trackers.keys().copied())
                .chain(state.feeds.keys().copied())
                .collect();
            held.into_iter()
                .filter(|id| {
                    old.symbol(*id).is_none()
                        || old.symbol(*id) != pairs.symbol(*id)
                        || old.base_quantity_scale(*id) != pairs.base_quantity_scale(*id)
                })
                .collect()
        };
        // A failed install step changes nothing and is retried as a failed
        // read.
        let current = loaded.install()?;
        {
            let mut state = write_unpoisoned(&self.state);
            for (topic, _) in &ended {
                drop_topic(&mut state, *topic);
            }
            for id in changed {
                for stream in Stream::ALL {
                    drop_topic(&mut state, Topic::new(id, stream));
                }
            }
        }
        inner.wanted = wanted;
        inner.symbols = symbols;
        inner.paused = false;
        *write_unpoisoned(&self.pairs) = current;
        let mut events = vec![Event::PairsReloaded { renumbered }];
        events.extend(ended.into_iter().map(|(topic, symbol)| Event::Ended {
            topic,
            error: Error::MarketStream(StreamError::PairNotListed { topic, symbol }),
        }));
        Ok(Some(events))
    }

    /// Ends a connection: everything it fed becomes stale before the client
    /// reports the loss. Books wait for a new snapshot, quotes for a new
    /// quote and trades for a new baseline. The connection's requests and
    /// owners end with it; state of topics no longer wanted, those of an
    /// Unsubscribe it never answered included, is dropped.
    fn detach(&self) {
        let mut inner = lock_unpoisoned(&self.inner);
        inner.out = None;
        for (_, waiter) in inner.waiting.drain() {
            let _ = waiter.send(Err(Error::realtime(
                "market stream connection lost before the response; the request applies once it reconnects",
            )));
        }
        inner.sent.clear();
        inner.owners.clear();
        let mut state = write_unpoisoned(&self.state);
        let ended: Vec<Topic> = state
            .books
            .keys()
            .copied()
            .chain(
                state
                    .trackers
                    .keys()
                    .map(|s| Topic::new(*s, Stream::Trades)),
            )
            .chain(state.quotes.keys().map(|s| Topic::new(*s, Stream::Bbo)))
            .filter(|t| !inner.wanted.contains(t))
            .collect();
        drop(inner);
        for t in &ended {
            drop_topic(&mut state, *t);
        }
        state.active.clear();
        state.initial.clear();
        state.feeds.clear();
        for b in state.books.values_mut() {
            b.synced = false;
            b.recovering = false;
        }
        for q in state.quotes.values_mut() {
            q.stale = true;
        }
        for t in state.trackers.values_mut() {
            t.restart();
        }
    }

    /// Moves the server toward the caller's intent for each topic `pick`
    /// returns (it runs under the lock) whose owner awaits no answer: a wanted
    /// topic not held is subscribed; a held topic no longer wanted, or one
    /// recovering from a gap, is unsubscribed. A topic whose owner awaits an
    /// answer is reconciled when that answer is applied.
    fn reconcile(&self, pick: impl FnOnce(&Inner) -> Vec<Topic>) {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.closed || inner.out.is_none() || inner.paused {
            return;
        }
        let (mut subs, mut unsubs) = (Vec::new(), Vec::new());
        for t in pick(&inner) {
            let (held, recovering) = match inner.owners.get(&t) {
                Some(o) if o.generation != 0 => continue,
                Some(o) => (o.held, o.recovering),
                None => (false, false),
            };
            let want = inner.wanted.contains(&t);
            if held && (!want || recovering) {
                unsubs.push(t);
            } else if want && !held {
                subs.push(t);
            }
        }
        for chunk in unsubs.chunks(codec::MAX_ITEMS) {
            inner.send(&self.ids, true, chunk);
        }
        for chunk in subs.chunks(codec::MAX_ITEMS) {
            inner.send(&self.ids, false, chunk);
        }
    }

    /// Begins a depth stream's recovery after a gap: its book goes stale and
    /// drops what arrives until a new subscription is made, and its owner
    /// ends the subscription and makes it again, through the same path as
    /// every request of the topic. Every other stream is untouched. A gap
    /// while the stream recovers starts nothing more.
    fn start_recovery(&self, topic: Topic) {
        let start = {
            let mut inner = lock_unpoisoned(&self.inner);
            let want = inner.wanted.contains(&topic);
            let mut state = write_unpoisoned(&self.state);
            let Some(book) = state.books.get_mut(&topic) else {
                return;
            };
            book.synced = false;
            match inner.owners.get_mut(&topic) {
                Some(o) if o.held && want && !o.recovering && !book.recovering => {
                    o.recovering = true;
                    book.recovering = true;
                    true
                }
                _ => false,
            }
        };
        if start {
            self.reconcile(|_| vec![topic]);
        }
    }

    async fn request(&self, topics: &[Topic], unsubscribe: bool) -> Result<SubscribeResponse> {
        let rx = self.issue(topics, unsubscribe)?;
        let mut stop = self.stop.subscribe();
        tokio::select! {
            res = rx => res.unwrap_or_else(|_| Err(closed())),
            _ = stop.wait_for(|s| *s) => Err(closed()),
        }
    }

    /// Records the caller's intent for topics and sends the request that moves
    /// the server toward it, through the topics' owners: the request becomes
    /// each topic's generation, so any answer to an earlier request of those
    /// topics is superseded. The response arrives on the receiver.
    fn issue(
        &self,
        topics: &[Topic],
        unsubscribe: bool,
    ) -> Result<oneshot::Receiver<Result<SubscribeResponse>>> {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.closed || inner.err.is_some() {
            return Err(closed());
        }
        for t in topics {
            if unsubscribe {
                // The caller ended the topic: its state goes now, and a new
                // subscription starts afresh.
                inner.wanted.retain(|w| w != t);
                inner.symbols.remove(t);
                drop_topic(&mut write_unpoisoned(&self.state), *t);
            } else if !inner.wanted.contains(t) {
                inner.wanted.push(*t);
                if let Some(symbol) = self.pairs().symbol(t.symbol_id) {
                    inner.symbols.insert(*t, symbol);
                }
            }
        }
        let Some(id) = inner.send(&self.ids, unsubscribe, topics) else {
            // Between connections: the next connection subscribes what is
            // wanted then, which these topics are, or no longer are.
            return Err(Error::realtime(
                "market stream is reconnecting; the request applies once it is connected",
            ));
        };
        // Kept until the response or the end of the connection, whether or
        // not the caller keeps waiting.
        let (tx, rx) = oneshot::channel();
        inner.waiting.insert(id, tx);
        Ok(rx)
    }

    /// Queues an event. A full queue fails the stream: the consumer is too
    /// slow, and dropping messages would silently corrupt books and trades.
    fn emit(&self, event: Event) -> bool {
        let overflow = {
            let inner = lock_unpoisoned(&self.inner);
            if inner.closed || inner.err.is_some() {
                return false;
            }
            match inner.events.as_ref().map(|tx| tx.try_send(event)) {
                Some(Ok(())) => return true,
                Some(Err(mpsc::error::TrySendError::Full(_))) => true,
                _ => false,
            }
        };
        if overflow {
            self.shutdown(Some(Error::queue_overflow(
                "market stream event queue full; consumer too slow",
            )));
        }
        false
    }

    /// Delivers a trades recovery started under connection `started`. Under a
    /// newer connection its trades are dropped, never delivered: they were
    /// read and decoded with that connection's catalog, which the pairs read
    /// since may have replaced, so the gap's recovery fails instead
    /// ([`StreamError::Superseded`]). The check and the delivery are atomic
    /// with the start of a new connection.
    fn emit_backfill(&self, started: u64, gap: TradesGap, trades: Result<Vec<Trade>>) -> bool {
        let overflow = {
            let inner = lock_unpoisoned(&self.inner);
            if inner.closed || inner.err.is_some() {
                return false;
            }
            let trades = if self.generation.load(Ordering::SeqCst) == started {
                trades
            } else {
                Err(Error::MarketStream(StreamError::Superseded {
                    symbol_id: gap.symbol_id,
                }))
            };
            let event = Event::TradesBackfill(TradesBackfill { gap, trades });
            match inner.events.as_ref().map(|tx| tx.try_send(event)) {
                Some(Ok(())) => return true,
                Some(Err(mpsc::error::TrySendError::Full(_))) => true,
                _ => false,
            }
        };
        if overflow {
            self.shutdown(Some(Error::queue_overflow(
                "market stream event queue full; consumer too slow",
            )));
        }
        false
    }

    /// Stops the stream, recording `err` as its terminal error, and ends every
    /// background recovery.
    fn shutdown(&self, err: Option<Error>) {
        let mut inner = lock_unpoisoned(&self.inner);
        if err.is_none() {
            inner.closed = true;
        } else if inner.err.is_none() && !inner.closed {
            inner.err = err;
        }
        inner.out = None;
        for b in inner.backfills.drain(..) {
            b.abort();
        }
        for (_, waiter) in inner.waiting.drain() {
            let _ = waiter.send(Err(closed()));
        }
        drop(inner);
        self.stop.send_replace(true);
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    fn level(&self, symbol_id: u32, price: i64, qty: i64) -> Result<Level> {
        let pairs = self.pairs();
        let symbol = pairs.symbol(symbol_id);
        let scale = pairs.base_quantity_scale(symbol_id);
        Ok(Level {
            price: Price::from_ticks(price, symbol.clone())?,
            qty: Quantity::from_scaled(
                qty,
                scale,
                QuantityDomain::OrderBase,
                symbol,
                Some(symbol_id),
            )?,
        })
    }

    fn levels(&self, symbol_id: u32, levels: &[WireLevel]) -> Vec<Level> {
        levels
            .iter()
            .filter_map(|l| self.level(symbol_id, l.price, l.qty).ok())
            .collect()
    }
}

/// Serves connections until the stream stops.
async fn run(shared: Arc<Shared>, mut ws: Ws, mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let mut backoff = ReconnectBackoff::new();
    let mut reconnect = false;
    loop {
        let err = session(&shared, ws, out_rx, reconnect).await;
        shared.detach();
        if shared.stopped() {
            break;
        }
        let Some(err) = err else { break };
        if matches!(err, Error::MarketStream(StreamError::Schema { .. })) {
            shared.shutdown(Some(err));
            break;
        }
        if shared.healthy.swap(false, Ordering::SeqCst) {
            backoff.reset();
        }
        let mut err = err;
        let next = loop {
            let delay = crate::realtime::reconnect_wait(backoff.next_delay(), &err);
            if !shared.emit(Event::Disconnected { error: err, delay }) {
                return finish(&shared);
            }
            // The stop signal is the shutdown fence: it wins over a wait that
            // is due at the same moment, is checked again right before every
            // upgrade, and cancels an upgrade in flight, so no upgrade starts
            // after the stream is closed.
            let mut stop = shared.stop.subscribe();
            tokio::select! {
                biased;
                () = stopped(&mut stop) => return finish(&shared),
                () = tokio::time::sleep(delay) => {}
            }
            // Only now, with the whole wait over, does the upgrade queue for
            // a slot of a shared limiter: the queue adds to the wait and
            // never shortens it. A close ends the queueing.
            let slot = tokio::select! {
                biased;
                () = stopped(&mut stop) => return finish(&shared),
                slot = upgrade_slot(shared.cfg.reconnect_limiter.as_ref()) => slot,
            };
            if shared.stopped() {
                return finish(&shared);
            }
            let dialed = tokio::select! {
                biased;
                () = stopped(&mut stop) => return finish(&shared),
                dialed = shared.dial() => dialed,
            };
            // The upgrade has ended, accepted, refused or timed out: its
            // slot goes back before anything else.
            drop(slot);
            match dialed {
                Ok(next) => break next,
                Err(e) => err = e,
            }
            if shared.stopped() {
                return finish(&shared);
            }
        };
        let Some(rx) = shared.attach(shared.cfg.reload.is_some()) else {
            break;
        };
        ws = next;
        out_rx = rx;
        reconnect = true;
    }
    finish(&shared);
}

/// Resolves once the stream is stopped. The guard `wait_for` returns is not
/// `Send`, so it is dropped here.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}

/// Ends delivery: the event channel closes once every event is read.
fn finish(shared: &Shared) {
    let mut inner = lock_unpoisoned(&shared.inner);
    for b in inner.backfills.drain(..) {
        b.abort();
    }
    inner.events = None;
}

/// Serves one attached connection until it fails; `None` when the stream
/// stopped.
async fn session(
    shared: &Arc<Shared>,
    ws: Ws,
    mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    reconnect: bool,
) -> Option<Error> {
    // A new connection holds nothing yet: books wait for their snapshot and
    // trades for their baseline.
    {
        let mut state = write_unpoisoned(&shared.state);
        state.active.clear();
        state.feeds.clear();
        for b in state.books.values_mut() {
            b.synced = false;
        }
        for t in state.trackers.values_mut() {
            t.restart();
        }
    }
    let (mut write, mut read) = ws.split();
    if !shared.emit(Event::Connected { reconnect }) {
        return None;
    }
    // A reconnect reads the pairs afresh first ([`Config::reload`]); the
    // connection subscribes nothing until that read is installed, and a
    // failed read is retried with backoff while the connection stays up.
    // `connect` read the first connection's.
    let generation = shared.generation.load(Ordering::SeqCst);
    let source = if reconnect {
        shared.cfg.reload.clone()
    } else {
        None
    };
    let mut reload = source.as_ref().map(|s| s.load());
    let mut retry: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    let mut reload_backoff = ReconnectBackoff::new();
    if reload.is_none() {
        // Every wanted topic no request is outstanding for is subscribed; the
        // requests are queued, and written in order with any sent since
        // attach.
        shared.reconcile(|inner| inner.wanted.clone());
    }
    let cfg = &shared.cfg;
    let mut stop = shared.stop.subscribe();
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + cfg.ping_interval,
        cfg.ping_interval,
    );
    let check_every =
        (cfg.pong_timeout / 4).clamp(Duration::from_millis(1), Duration::from_secs(1));
    let mut check = tokio::time::interval(check_every);
    // The heartbeat detects a dead connection; the read deadline backs it up.
    let backstop = 2 * (cfg.ping_interval + cfg.pong_timeout);
    let mut deadline = Box::pin(tokio::time::sleep(backstop));
    loop {
        tokio::select! {
            _ = async { let _ = stop.wait_for(|s| *s).await; } => {
                let _ = write.close().await;
                return None;
            }
            loaded = async { reload.as_mut().expect("a read in progress").await }, if reload.is_some() => {
                reload = None;
                match loaded.and_then(|loaded| shared.install(generation, loaded)) {
                    Ok(Some(events)) => {
                        for event in events {
                            if !shared.emit(event) {
                                return None;
                            }
                        }
                        shared.reconcile(|inner| inner.wanted.clone());
                    }
                    // Superseded, or the stream stopped.
                    Ok(None) => {}
                    Err(error) => {
                        let delay = reload_backoff.next_delay();
                        if !shared.emit(Event::PairsReloadFailed { error, delay }) {
                            return None;
                        }
                        retry = Some(Box::pin(tokio::time::sleep(delay)));
                    }
                }
            }
            _ = async { retry.as_mut().expect("a retry scheduled").await }, if retry.is_some() => {
                retry = None;
                reload = source.as_ref().map(|s| s.load());
            }
            frame = out_rx.recv() => {
                let frame = frame?;
                if let Err(e) = write.send(Message::binary(frame)).await {
                    return Some(Error::realtime(format!("market stream write: {e}")));
                }
            }
            _ = ping.tick() => {
                let id = {
                    let mut inner = lock_unpoisoned(&shared.inner);
                    if inner.ping.is_some() {
                        None // the previous ping is still owed its pong
                    } else {
                        let id = shared.ids.fetch_add(1, Ordering::SeqCst) + 1;
                        inner.ping = Some((id, Instant::now()));
                        Some(id)
                    }
                };
                if let Some(id) = id
                    && let Err(e) = write.send(Message::binary(codec::encode_ping(id))).await
                {
                    return Some(Error::realtime(format!("market stream ping: {e}")));
                }
            }
            _ = check.tick() => {
                let late = lock_unpoisoned(&shared.inner).ping.is_some_and(|(_, sent)| sent.elapsed() > cfg.pong_timeout);
                if late {
                    return Some(Error::realtime(format!("market stream pong not received within {:?}", cfg.pong_timeout)));
                }
            }
            _ = &mut deadline => {
                return Some(Error::realtime(format!("market stream received nothing within {backstop:?}")));
            }
            msg = read.next() => {
                deadline.as_mut().reset(tokio::time::Instant::now() + backstop);
                match msg {
                    None => return Some(Error::realtime("market stream closed")),
                    Some(Err(e)) => return Some(Error::realtime(format!("market stream read: {e}"))),
                    Some(Ok(Message::Binary(frame))) => {
                        if let Err(e) = handle(shared, &frame) {
                            return Some(e);
                        }
                    }
                    Some(Ok(Message::Text(_))) => {
                        return Some(StreamError::frame(0, "text message on the binary market stream"));
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|f| format!("{} {}", u16::from(f.code), f.reason)).unwrap_or_default();
                        return Some(Error::realtime(format!("market stream closed by the server {reason}").trim_end().to_owned()));
                    }
                    Some(Ok(_)) => {} // pings are answered by the WebSocket layer
                }
            }
        }
    }
}

/// Applies one server message; an error ends the connection.
pub(crate) fn handle(shared: &Arc<Shared>, frame: &[u8]) -> Result<()> {
    let Some(msg) = codec::decode(frame)? else {
        return Ok(()); // a template this client does not know
    };
    shared.healthy.store(true, Ordering::SeqCst);
    match msg {
        Msg::SubscribeResponse {
            request_id,
            results,
        } => on_subscribe_response(shared, request_id, results),
        Msg::Pong {
            request_id,
            send_ns,
        } => {
            let sent = {
                let mut inner = lock_unpoisoned(&shared.inner);
                match inner.ping {
                    Some((owed, sent)) if owed == request_id => {
                        inner.ping = None;
                        Some(sent)
                    }
                    _ => None,
                }
            };
            if let Some(sent) = sent {
                shared.emit(Event::Pong(Pong {
                    request_id,
                    round_trip: sent.elapsed(),
                    send_ts_ns: send_ns,
                }));
            }
            Ok(())
        }
        Msg::MarketStatus {
            symbol_id,
            feed,
            send_ns,
        } => on_market_status(shared, symbol_id, feed, send_ns),
        Msg::TradesThrough {
            symbol_id,
            epoch,
            last,
            send_ns,
        } => {
            let gaps = with_tracker(
                shared,
                symbol_id,
                |t| t.through(epoch, last).map(|g| (Vec::new(), g)),
                trades_through_template(),
            )?;
            let Some((_, gaps)) = gaps else { return Ok(()) };
            write_unpoisoned(&shared.state)
                .initial
                .insert(Topic::new(symbol_id, Stream::Trades));
            emit_gaps(shared, gaps);
            shared.emit(Event::TradesThrough(TradesThrough {
                symbol_id,
                book_epoch: epoch,
                last_trade_id: last,
                send_ts_ns: send_ns,
            }));
            Ok(())
        }
        Msg::TradesSkipped {
            symbol_id,
            epoch,
            first,
            last,
            ..
        } => {
            let gaps = with_tracker(
                shared,
                symbol_id,
                |t| t.skipped(epoch, first, last).map(|g| (Vec::new(), g)),
                sbe::trades_skipped_codec::SBE_TEMPLATE_ID,
            )?;
            if let Some((_, gaps)) = gaps {
                emit_gaps(shared, gaps);
            }
            Ok(())
        }
        Msg::Trades {
            symbol_id,
            epoch,
            update_id,
            transact_ns,
            send_ns,
            fills,
        } => {
            let template = sbe::trades_codec::SBE_TEMPLATE_ID;
            if fills.iter().any(|f| f.price <= 0 || f.qty <= 0) {
                return Err(StreamError::frame(
                    template,
                    "trade with a price or quantity that is not positive",
                ));
            }
            let Some((fresh, gaps)) =
                with_tracker(shared, symbol_id, |t| t.trades(epoch, &fills), template)?
            else {
                return Ok(());
            };
            emit_gaps(shared, gaps);
            if fresh.is_empty() {
                return Ok(());
            }
            let mut trades = Vec::with_capacity(fresh.len());
            for f in fresh {
                let level = shared.level(symbol_id, f.price, f.qty)?;
                let taker_side = match f.side {
                    sbe::side::Side::BUY => Side::Buy,
                    sbe::side::Side::SELL => Side::Sell,
                    _ => Side::Unknown,
                };
                trades.push(Trade {
                    trade_id: f.trade_id,
                    price: level.price,
                    qty: level.qty,
                    taker_side,
                });
            }
            shared.emit(Event::Trades(Trades {
                symbol_id,
                symbol: shared.pairs().symbol(symbol_id),
                book_epoch: epoch,
                update_id,
                transact_ts_ns: transact_ns,
                send_ts_ns: send_ns,
                trades,
            }));
            Ok(())
        }
        Msg::Bbo {
            symbol_id,
            epoch,
            update_id,
            transact_ns,
            send_ns,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        } => {
            let template = sbe::best_bid_offer_codec::SBE_TEMPLATE_ID;
            let side = |price: Option<i64>, qty: i64| -> Result<Option<Level>> {
                match price {
                    None if qty == 0 => Ok(None),
                    None => Err(StreamError::frame(
                        template,
                        "side without a price has a quantity",
                    )),
                    Some(p) if p > 0 && qty > 0 => shared.level(symbol_id, p, qty).map(Some),
                    Some(_) => Err(StreamError::frame(
                        template,
                        "side with a price or quantity that is not positive",
                    )),
                }
            };
            let (bid, ask) = (side(bid_price, bid_qty)?, side(ask_price, ask_qty)?);
            let quote = {
                let mut state = write_unpoisoned(&shared.state);
                let topic = Topic::new(symbol_id, Stream::Bbo);
                if !state.active.contains(&topic) {
                    return Ok(());
                }
                state.initial.insert(topic);
                // An older state, or a re-send of the current one. A quote
                // kept, stale, from a lost connection stays until one at
                // least as new confirms or replaces it.
                if let Some(prev) = state.quotes.get(&symbol_id)
                    && (epoch < prev.book_epoch
                        || (epoch == prev.book_epoch && update_id < prev.update_id)
                        || (!prev.stale && epoch == prev.book_epoch && update_id == prev.update_id))
                {
                    return Ok(());
                }
                let quote = BestBidOffer {
                    symbol_id,
                    symbol: shared.pairs().symbol(symbol_id),
                    book_epoch: epoch,
                    update_id,
                    transact_ts_ns: transact_ns,
                    send_ts_ns: send_ns,
                    bid,
                    ask,
                    stale: false,
                };
                state.quotes.insert(symbol_id, quote.clone());
                quote
            };
            shared.emit(Event::BestBidOffer(quote));
            Ok(())
        }
        Msg::DepthSnapshot {
            symbol_id,
            stream,
            epoch,
            update_id,
            transact_ns,
            send_ns,
            levels_per_side,
            bids,
            asks,
        } => {
            let Some(stream) = stream else { return Ok(()) };
            let topic = Topic::new(symbol_id, stream);
            let changed = {
                let mut state = write_unpoisoned(&shared.state);
                if !state.active.contains(&topic) {
                    return Ok(());
                }
                let Some(book) = state.books.get_mut(&topic) else {
                    return Ok(());
                };
                // While the stream recovers, only the new subscription's
                // snapshot counts.
                if book.recovering {
                    return Ok(());
                }
                let changed = book
                    .snapshot(epoch, update_id, levels_per_side, &bids, &asks)
                    .map_err(|m| {
                        StreamError::frame(sbe::depth_snapshot_codec::SBE_TEMPLATE_ID, m)
                    })?;
                state.initial.insert(topic);
                changed
            };
            if changed {
                shared.emit(Event::DepthSnapshot(DepthSnapshot {
                    symbol_id,
                    symbol: shared.pairs().symbol(symbol_id),
                    stream,
                    book_epoch: epoch,
                    update_id,
                    levels_per_side,
                    transact_ts_ns: transact_ns,
                    send_ts_ns: send_ns,
                    bids: shared.levels(symbol_id, &bids),
                    asks: shared.levels(symbol_id, &asks),
                }));
            }
            Ok(())
        }
        Msg::DepthUpdate {
            symbol_id,
            epoch,
            first,
            last,
            transact_ns,
            send_ns,
            bids,
            asks,
        } => {
            let topic = Topic::new(symbol_id, Stream::Depth);
            // An update the book cannot apply is a gap: the stream alone
            // recovers. Updates arriving while it recovers are dropped.
            let refused = {
                let mut state = write_unpoisoned(&shared.state);
                if !state.active.contains(&topic) {
                    return Ok(());
                }
                let Some(book) = state.books.get_mut(&topic) else {
                    return Ok(());
                };
                if book.recovering {
                    return Ok(());
                }
                let refused = book.update(epoch, first, last, &bids, &asks).err();
                if refused.is_some() {
                    book.synced = false;
                }
                refused
            };
            match refused {
                Some(Refusal::Gap(_)) => {
                    shared.start_recovery(topic);
                    return Ok(());
                }
                Some(Refusal::Invalid(message)) => {
                    return Err(StreamError::frame(
                        sbe::depth_update_codec::SBE_TEMPLATE_ID,
                        message,
                    ));
                }
                None => {}
            }
            shared.emit(Event::DepthUpdate(DepthUpdate {
                symbol_id,
                symbol: shared.pairs().symbol(symbol_id),
                book_epoch: epoch,
                first_update_id: first,
                last_update_id: last,
                transact_ts_ns: transact_ns,
                send_ts_ns: send_ns,
                bids: shared.levels(symbol_id, &bids),
                asks: shared.levels(symbol_id, &asks),
            }));
            Ok(())
        }
    }
}

fn trades_through_template() -> u16 {
    sbe::trades_through_codec::SBE_TEMPLATE_ID
}

type TrackerResult = std::result::Result<(Vec<codec::Fill>, Vec<TradesGap>), String>;

/// Runs `f` on the accounting of a pair whose trades stream the connection
/// holds; `None` for another pair.
fn with_tracker(
    shared: &Shared,
    symbol_id: u32,
    f: impl FnOnce(&mut Tracker) -> TrackerResult,
    template: u16,
) -> Result<Option<(Vec<codec::Fill>, Vec<TradesGap>)>> {
    let mut state = write_unpoisoned(&shared.state);
    if !state
        .active
        .contains(&Topic::new(symbol_id, Stream::Trades))
    {
        return Ok(None);
    }
    let Some(tracker) = state.trackers.get_mut(&symbol_id) else {
        return Ok(None);
    };
    f(tracker)
        .map(Some)
        .map_err(|m| StreamError::frame(template, m))
}

/// Delivers gaps and, with [`Config::history`], starts their recovery.
fn emit_gaps(shared: &Arc<Shared>, gaps: Vec<TradesGap>) {
    for gap in gaps {
        if !shared.emit(Event::TradesGap(gap)) {
            return;
        }
        let Some(history) = shared.cfg.history.clone() else {
            continue;
        };
        let task_shared = shared.clone();
        let pacing = shared.cfg.pacing;
        let mut inner = lock_unpoisoned(&shared.inner);
        if inner.closed || inner.err.is_some() {
            return;
        }
        // The recovery belongs to this connection and its catalog: a result
        // that arrives under a newer connection is dropped.
        let started = shared.generation.load(Ordering::SeqCst);
        let pairs = shared.pairs();
        let handle = tokio::spawn(async move {
            let trades = backfill_paced(history.as_ref(), Some(pairs.as_ref()), gap, pacing).await;
            task_shared.emit_backfill(started, gap, trades);
        });
        inner.backfills.retain(|h| !h.is_finished());
        inner.backfills.push(handle.abort_handle());
    }
}

/// Applies the answer to a Subscribe or Unsubscribe. Every answer to a
/// request of this connection updates what the server holds, as the server
/// answers in the order it acts ([`observe`]). Only the answer to the
/// topic's current generation is applied to the caller's view, its state and
/// intent ([`apply`]); one to a request a later one superseded is delivered,
/// never applied. Each topic answered is then reconciled with the caller's
/// intent.
fn on_subscribe_response(
    shared: &Shared,
    request_id: u64,
    results: Vec<(u32, Option<Stream>, sbe::subscribe_result::SubscribeResult)>,
) -> Result<()> {
    use sbe::subscribe_result::SubscribeResult as Wire;
    let results: Vec<TopicResult> = results
        .into_iter()
        .map(|(symbol_id, stream, result)| TopicResult {
            topic: stream.map(|s| Topic::new(symbol_id, s)),
            symbol_id,
            result: match result {
                Wire::OK => SubscribeResult::Ok,
                Wire::UNKNOWN_SYMBOL => SubscribeResult::UnknownSymbol,
                Wire::LIMIT_EXCEEDED => SubscribeResult::LimitExceeded,
                Wire::ALREADY_SUBSCRIBED => SubscribeResult::AlreadySubscribed,
                Wire::NOT_SUBSCRIBED => SubscribeResult::NotSubscribed,
                Wire::SYMBOL_DISABLED => SubscribeResult::SymbolDisabled,
                Wire::NullVal => SubscribeResult::Unknown,
            },
        })
        .collect();
    let mut applied = Vec::new();
    let mut broken = None;
    let waiter = {
        let mut inner = lock_unpoisoned(&shared.inner);
        // An answer to no request of this connection is delivered, never
        // applied.
        if let Some(sent) = inner.sent.remove(&request_id) {
            let mut state = write_unpoisoned(&shared.state);
            for r in &results {
                let Some(topic) = r.topic else { continue };
                let want = inner.wanted.contains(&topic);
                let Some(owner) = inner.owners.get_mut(&topic) else {
                    continue;
                };
                observe(owner, &mut state, topic, r.result, sent.unsubscribe);
                if owner.generation != request_id {
                    continue;
                }
                owner.generation = 0;
                applied.push(topic);
                match apply(owner, &mut state, topic, r.result, sent.unsubscribe, want) {
                    Applied::Done => {}
                    Applied::Refused => {
                        inner.wanted.retain(|w| *w != topic);
                        inner.symbols.remove(&topic);
                    }
                    Applied::Unknown => {
                        broken.get_or_insert(Error::MarketStream(StreamError::Unsubscribe {
                            topic,
                            result: r.result,
                        }));
                    }
                }
            }
            for t in &sent.topics {
                // A topic the answer leaves out is no longer awaited: its
                // owner reconciles from what it last knew.
                if let Some(o) = inner.owners.get_mut(t)
                    && o.generation == request_id
                {
                    o.generation = 0;
                    applied.push(*t);
                }
            }
        }
        inner.waiting.remove(&request_id)
    };
    let response = SubscribeResponse {
        request_id,
        results,
    };
    shared.emit(Event::SubscribeResponse(response.clone()));
    if let Some(waiter) = waiter {
        let _ = waiter.send(Ok(response));
    }
    if let Some(err) = broken {
        return Err(err);
    }
    if !applied.is_empty() {
        shared.reconcile(|_| applied);
    }
    Ok(())
}

/// What applying an answer leaves to do.
enum Applied {
    Done,
    /// A refused Subscribe: the topic is not resubscribed.
    Refused,
    /// An Unsubscribe of a held topic answered neither OK nor NOT_SUBSCRIBED:
    /// the server's state is unknown.
    Unknown,
}

/// Records the answer to a topic's current request on the caller's view: its
/// intent and state. [`observe`] has already recorded the server's state
/// after the answer.
///
/// An unsubscribe answered OK or NOT_SUBSCRIBED ended the topic; refused while
/// the server holds the topic, as when an accepted subscribe it superseded
/// made it, the stream the caller ended stays held, so the connection ends,
/// reconnecting every stream without it. A subscribe answered OK made a new
/// subscription, whose snapshot or baseline the topic waits for;
/// ALREADY_SUBSCRIBED keeps the one held. A topic held without the initial
/// state of its subscription, which followed the answer to a superseded
/// request before the topic was held, is ended and subscribed again, as a
/// depth stream after a gap; its trades meanwhile are dropped. A refused
/// subscribe ends the topic: it is not resubscribed. State is kept only for
/// topics the caller wants.
fn apply(
    owner: &mut Owner,
    state: &mut State,
    topic: Topic,
    result: SubscribeResult,
    unsubscribe: bool,
    want: bool,
) -> Applied {
    if unsubscribe {
        if !matches!(result, SubscribeResult::Ok | SubscribeResult::NotSubscribed) {
            if owner.held {
                return Applied::Unknown;
            }
            // Never held: the refusal changes nothing.
            return Applied::Done;
        }
        state.active.remove(&topic);
        if !want {
            drop_topic(state, topic);
        }
        return Applied::Done;
    }
    if !result.subscribed() {
        drop_topic(state, topic);
        return Applied::Refused;
    }
    let fresh = result == SubscribeResult::Ok;
    if !want {
        // Held again by a request the caller has since withdrawn: reconcile
        // ends it.
        state.active.remove(&topic);
        return Applied::Done;
    }
    state.active.insert(topic);
    let missing = !fresh && !state.initial.contains(&topic) && !owner.recovering;
    if missing {
        owner.recovering = true;
    }
    match topic.stream {
        Stream::Trades => {
            let tracker = state
                .trackers
                .entry(topic.symbol_id)
                .or_insert_with(|| Tracker::new(topic.symbol_id));
            if missing {
                tracker.missed = true;
            }
        }
        Stream::Depth20 | Stream::Depth => {
            let book = state
                .books
                .entry(topic)
                .or_insert_with(|| Book::new(topic.stream));
            if missing {
                book.recovering = topic.stream == Stream::Depth;
                book.synced = false;
            }
        }
        Stream::Bbo => {}
    }
    Applied::Done
}

/// Records what an answer says the server did to a topic's subscription,
/// whether or not the answer is applied: an unsubscribe answered OK or
/// NOT_SUBSCRIBED ended it, a subscribe answered OK made a new one, whose
/// initial state is still to come, and ALREADY_SUBSCRIBED kept the one held.
/// Any other answer changed nothing. Whatever the client took from a
/// subscription that ended or was replaced is no longer current
/// ([`invalidate`]).
fn observe(
    owner: &mut Owner,
    state: &mut State,
    topic: Topic,
    result: SubscribeResult,
    unsubscribe: bool,
) {
    match (unsubscribe, result) {
        (true, SubscribeResult::Ok | SubscribeResult::NotSubscribed) => {
            owner.held = false;
            owner.recovering = false;
            invalidate(state, topic);
        }
        (false, SubscribeResult::Ok) => {
            owner.held = true;
            owner.recovering = false;
            invalidate(state, topic);
            if let Some(book) = state.books.get_mut(&topic) {
                book.recovering = false;
            }
        }
        (false, SubscribeResult::AlreadySubscribed) => owner.held = true,
        _ => {}
    }
}

/// Makes a topic not ready once the subscription its state came from ended
/// or was replaced: it waits for the initial state of the next one. A book
/// waits for its snapshot and a quote stays, stale, until one at least as
/// new; trade accounting is kept, so the next baseline reconciles against
/// what was delivered.
fn invalidate(state: &mut State, topic: Topic) {
    state.initial.remove(&topic);
    match topic.stream {
        Stream::Trades => {
            if let Some(tracker) = state.trackers.get_mut(&topic.symbol_id) {
                tracker.restart();
            }
        }
        Stream::Bbo => {
            if let Some(quote) = state.quotes.get_mut(&topic.symbol_id) {
                quote.stale = true;
            }
        }
        Stream::Depth20 | Stream::Depth => {
            if let Some(book) = state.books.get_mut(&topic) {
                book.synced = false;
            }
        }
    }
}

/// Forgets the state of a topic no longer subscribed: its book, quote or
/// trade accounting, and its pair's feed state once the pair holds none.
fn drop_topic(state: &mut State, topic: Topic) {
    state.active.remove(&topic);
    state.initial.remove(&topic);
    match topic.stream {
        Stream::Trades => {
            state.trackers.remove(&topic.symbol_id);
        }
        Stream::Bbo => {
            state.quotes.remove(&topic.symbol_id);
        }
        Stream::Depth20 | Stream::Depth => {
            state.books.remove(&topic);
        }
    }
    if !Stream::ALL
        .iter()
        .any(|s| state.active.contains(&Topic::new(topic.symbol_id, *s)))
    {
        state.feeds.remove(&topic.symbol_id);
    }
}

/// Records a pair's feed state. A stale pair's depth book waits for a
/// replacement snapshot, which the server sends before the pair is live
/// again: LIVE while the book still waits is a gap, and the depth stream
/// alone recovers.
fn on_market_status(
    shared: &Shared,
    symbol_id: u32,
    feed: sbe::feed_state::FeedState,
    send_ns: u64,
) -> Result<()> {
    use sbe::feed_state::FeedState as Wire;
    let feed = match feed {
        Wire::LIVE => FeedState::Live,
        Wire::STALE => FeedState::Stale,
        Wire::NullVal => FeedState::Unknown,
    };
    let topic = Topic::new(symbol_id, Stream::Depth);
    let waiting = {
        let mut state = write_unpoisoned(&shared.state);
        state.feeds.insert(symbol_id, feed);
        let held = state.active.contains(&topic);
        match state.books.get_mut(&topic) {
            Some(book) if held => {
                let waiting = !book.synced;
                if feed == FeedState::Stale {
                    book.synced = false;
                }
                waiting
            }
            _ => false,
        }
    };
    if feed == FeedState::Live && waiting {
        shared.start_recovery(topic);
    }
    shared.emit(Event::MarketStatus(MarketStatus {
        symbol_id,
        feed,
        send_ts_ns: send_ns,
    }));
    Ok(())
}

/// Whether `topic`'s owner is recovering it.
#[cfg(test)]
pub(crate) fn recovery_requested(shared: &Shared, topic: Topic) -> bool {
    lock_unpoisoned(&shared.inner)
        .owners
        .get(&topic)
        .is_some_and(|o| o.recovering)
}

/// Installs pairs reloaded for connection `generation`; false when it is not
/// the current connection's.
#[cfg(test)]
pub(crate) fn install_pairs(shared: &Shared, generation: u64, pairs: Arc<dyn Pairs>) -> bool {
    shared
        .install(generation, LoadedPairs::new(pairs))
        .unwrap()
        .is_some()
}

/// The current connection's generation.
#[cfg(test)]
pub(crate) fn generation(shared: &Shared) -> u64 {
    shared.generation.load(Ordering::SeqCst)
}

/// Whether a depth stream is recovering.
#[cfg(test)]
pub(crate) fn recovering(shared: &Shared, topic: Topic) -> bool {
    read_unpoisoned(&shared.state)
        .books
        .get(&topic)
        .is_some_and(|b| b.recovering)
}

/// Records a caller's Subscribe as sent on the current connection, as the
/// send path does: its topics are wanted and it is their generation.
#[cfg(test)]
pub(crate) fn record_subscribe(shared: &Shared, request_id: u64, topics: Vec<Topic>) {
    let mut inner = lock_unpoisoned(&shared.inner);
    for t in &topics {
        inner.owners.entry(*t).or_default().generation = request_id;
        if !inner.wanted.contains(t) {
            inner.wanted.push(*t);
        }
    }
    inner.sent.insert(
        request_id,
        Sent {
            unsubscribe: false,
            topics,
        },
    );
}

/// Makes the stream's outbound queue a channel the test reads, as a
/// connection would be.
#[cfg(test)]
pub(crate) fn wire(shared: &Shared) -> mpsc::UnboundedReceiver<Vec<u8>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut inner = lock_unpoisoned(&shared.inner);
    inner.out = Some(tx);
    inner.paused = shared.cfg.reload.is_some();
    shared.generation.fetch_add(1, Ordering::SeqCst);
    rx
}

/// Replaces the connection, as the client's session does after a loss:
/// everything is stale, and what is wanted is subscribed on the new
/// connection, whose outbound queue the returned channel is.
#[cfg(test)]
pub(crate) fn reconnect(shared: &Shared) -> mpsc::UnboundedReceiver<Vec<u8>> {
    shared.detach();
    let rx = wire(shared);
    shared.reconcile(|inner| inner.wanted.clone());
    rx
}

/// Sends the caller's Subscribe or Unsubscribe without waiting for it.
#[cfg(test)]
pub(crate) fn issue(shared: &Shared, topics: &[Topic], unsubscribe: bool) -> Result<()> {
    shared.issue(topics, unsubscribe).map(drop)
}

/// The topics the client wants, whether it holds state for each, and its
/// unanswered requests and busy owners: what a settled client is checked by.
#[cfg(test)]
pub(crate) struct Settled {
    pub wanted: Vec<Topic>,
    pub with_state: Vec<Topic>,
    pub unanswered: usize,
    pub busy: Vec<Topic>,
}

#[cfg(test)]
pub(crate) fn settled(shared: &Shared) -> Settled {
    let inner = lock_unpoisoned(&shared.inner);
    let state = read_unpoisoned(&shared.state);
    let with_state = state
        .books
        .keys()
        .copied()
        .chain(state.quotes.keys().map(|s| Topic::new(*s, Stream::Bbo)))
        .chain(
            state
                .trackers
                .keys()
                .map(|s| Topic::new(*s, Stream::Trades)),
        )
        .chain(state.active.iter().copied())
        .collect();
    Settled {
        wanted: inner.wanted.clone(),
        with_state,
        unanswered: inner.sent.len(),
        busy: inner
            .owners
            .iter()
            .filter(|(_, o)| o.generation != 0 || o.recovering)
            .map(|(t, _)| *t)
            .collect(),
    }
}

#[cfg(test)]
pub(crate) fn offline(config: Config) -> (Arc<Shared>, mpsc::Receiver<Event>) {
    let (events, rx) = mpsc::channel(config.queue_size.max(1));
    let (stop, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        pairs: RwLock::new(config.pairs.clone()),
        generation: AtomicU64::new(0),
        cfg: config,
        ids: AtomicU64::new(0),
        healthy: AtomicBool::new(false),
        stop,
        inner: Mutex::new(Inner {
            events: Some(events),
            ..Inner::default()
        }),
        state: RwLock::new(State::default()),
    });
    (shared, rx)
}

#[cfg(test)]
pub(crate) fn depth_book(shared: &Shared, topic: Topic) -> Option<(bool, u64)> {
    read_unpoisoned(&shared.state)
        .books
        .get(&topic)
        .map(|b| (b.synced, b.update_id))
}

#[cfg(test)]
pub(crate) fn feed_of(shared: &Shared, symbol_id: u32) -> Option<FeedState> {
    read_unpoisoned(&shared.state)
        .feeds
        .get(&symbol_id)
        .copied()
}

/// A stream with no connection, fed by [`handle`] in tests.
#[cfg(test)]
pub(crate) fn offline_stream(config: Config) -> MarketStream {
    let (shared, rx) = offline(config);
    MarketStream {
        rx,
        shared,
        task: tokio::spawn(async {}),
    }
}

#[cfg(test)]
impl MarketStream {
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
    pub(crate) fn owe_ping(&self, request_id: u64) {
        lock_unpoisoned(&self.shared.inner).ping = Some((request_id, Instant::now()));
    }
    pub(crate) fn queued(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        while let Ok(e) = self.rx.try_recv() {
            out.push(e);
        }
        out
    }
}

/// [`MarketStream::live`] of the stream `shared` serves.
#[cfg(test)]
pub(crate) fn live_of(shared: &Shared, topic: Topic) -> bool {
    shared.live(topic)
}
