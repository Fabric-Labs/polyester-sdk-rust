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
    Side, Stream, StreamError, SubscribeResponse, SubscribeResult, Topic, TopicResult, Trade,
    Trades, TradesBackfill, TradesGap, TradesThrough,
};
use crate::errors::{Error, Result};
use crate::realtime::{ReconnectBackoff, lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use crate::types::{Price, Quantity, QuantityDomain};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// What the client needs to know about a trading pair: its base quantity
/// scale, which every quantity on the stream is scaled by, and its symbol.
/// [`crate::catalogs::Manager`] implements it.
pub trait Pairs: Send + Sync {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32>;
    fn symbol(&self, symbol_id: u32) -> Option<String>;
}

impl Pairs for crate::catalogs::Manager {
    fn base_quantity_scale(&self, symbol_id: u32) -> Option<u32> {
        self.base_quantity_scale_for_symbol_id(symbol_id)
    }
    fn symbol(&self, symbol_id: u32) -> Option<String> {
        self.symbol_for_symbol_id(symbol_id)
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
    pub(crate) pacing: Pacing,
}

impl Config {
    pub fn new(url: impl Into<String>, pairs: Arc<dyn Pairs>) -> Self {
        Self {
            url: url.into(),
            pairs,
            history: None,
            queue_size: 0,
            ping_interval: Duration::ZERO,
            pong_timeout: Duration::ZERO,
            allow_insecure_http: false,
            pacing: Pacing::default(),
        }
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
    out: Option<mpsc::UnboundedSender<Vec<u8>>>,
    /// The topics to hold, in the order first asked; each is resubscribed on
    /// reconnect until the server refuses or the caller unsubscribes it.
    wanted: Vec<Topic>,
    waiting: HashMap<u64, oneshot::Sender<Result<SubscribeResponse>>>,
    /// Requests in flight that are Unsubscribes: their responses end streams.
    unsubscribes: HashSet<u64>,
    /// The ping owed its pong.
    ping: Option<(u64, Instant)>,
    backfills: Vec<AbortHandle>,
}

#[derive(Default)]
struct State {
    books: HashMap<Topic, Book>,
    quotes: HashMap<u32, BestBidOffer>,
    feeds: HashMap<u32, FeedState>,
    trackers: HashMap<u32, Tracker>,
    /// The topics the current connection holds.
    active: HashSet<Topic>,
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
    let ws = shared.dial().await?;
    // Subscribe may run as soon as connect returns: the connection is the
    // client's before then.
    let (wanted, out_rx) = shared.attach().ok_or_else(closed)?;
    let task = tokio::spawn(run(shared.clone(), ws, wanted, out_rx));
    Ok(MarketStream { rx, shared, task })
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
                .cfg
                .pairs
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
    /// The topics are not resubscribed on reconnect from the moment the
    /// request is sent, and their books, quotes and trade accounting are
    /// dropped once the server answers, so subscribing again starts afresh.
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
            symbol: self.shared.cfg.pairs.symbol(symbol_id),
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
        read_unpoisoned(&self.shared.state)
            .quotes
            .get(&symbol_id)
            .cloned()
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
            CONNECT_TIMEOUT,
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| Error::realtime("market stream connect timed out"))?
        .map_err(|e| Error::realtime(format!("market stream connect: {e}")))?;
        Ok(ws)
    }

    /// Makes a new connection the client's and returns the topics to
    /// subscribe on it, atomically: a request either adds its topics before,
    /// and they are subscribed with these, or sends them on this connection
    /// itself. `None` when the client is closed.
    fn attach(&self) -> Option<(Vec<Topic>, mpsc::UnboundedReceiver<Vec<u8>>)> {
        let mut inner = lock_unpoisoned(&self.inner);
        if inner.closed || inner.err.is_some() {
            return None;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        inner.out = Some(tx);
        inner.ping = None;
        Some((inner.wanted.clone(), rx))
    }

    fn detach(&self) {
        let mut inner = lock_unpoisoned(&self.inner);
        inner.out = None;
        for (_, waiter) in inner.waiting.drain() {
            let _ = waiter.send(Err(Error::realtime(
                "market stream connection lost before the response; the request applies once it reconnects",
            )));
        }
        inner.unsubscribes.clear();
    }

    async fn request(&self, topics: &[Topic], unsubscribe: bool) -> Result<SubscribeResponse> {
        let id = self.ids.fetch_add(1, Ordering::SeqCst) + 1;
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = lock_unpoisoned(&self.inner);
            if inner.closed || inner.err.is_some() {
                return Err(closed());
            }
            for t in topics {
                if unsubscribe {
                    inner.wanted.retain(|w| w != t);
                } else if !inner.wanted.contains(t) {
                    inner.wanted.push(*t);
                }
            }
            let Some(out) = inner.out.clone() else {
                return Err(Error::realtime(
                    "market stream is reconnecting; the request applies once it is connected",
                ));
            };
            inner.waiting.insert(id, tx);
            if unsubscribe {
                inner.unsubscribes.insert(id);
            }
            let frame = if unsubscribe {
                codec::encode_unsubscribe(id, topics)
            } else {
                codec::encode_subscribe(id, topics)
            };
            if out.send(frame).is_err() {
                inner.waiting.remove(&id);
                inner.unsubscribes.remove(&id);
                return Err(Error::realtime(
                    "market stream connection lost; the request applies once it reconnects",
                ));
            }
        }
        let mut stop = self.stop.subscribe();
        tokio::select! {
            res = rx => res.unwrap_or_else(|_| Err(closed())),
            _ = stop.wait_for(|s| *s) => Err(closed()),
        }
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
        let symbol = self.cfg.pairs.symbol(symbol_id);
        let scale = self.cfg.pairs.base_quantity_scale(symbol_id);
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
async fn run(
    shared: Arc<Shared>,
    mut ws: Ws,
    mut wanted: Vec<Topic>,
    mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut backoff = ReconnectBackoff::new();
    let mut reconnect = false;
    loop {
        let err = session(&shared, ws, &wanted, out_rx, reconnect).await;
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
            let delay = backoff.next_delay();
            if !shared.emit(Event::Disconnected { error: err, delay }) {
                return finish(&shared);
            }
            let mut stop = shared.stop.subscribe();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stop.wait_for(|s| *s) => return finish(&shared),
            }
            match shared.dial().await {
                Ok(next) => break next,
                Err(e) => err = e,
            }
            if shared.stopped() {
                return finish(&shared);
            }
        };
        let Some((w, rx)) = shared.attach() else {
            break;
        };
        ws = next;
        wanted = w;
        out_rx = rx;
        reconnect = true;
    }
    finish(&shared);
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
    wanted: &[Topic],
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
    for chunk in wanted.chunks(codec::MAX_ITEMS) {
        let id = shared.ids.fetch_add(1, Ordering::SeqCst) + 1;
        if let Err(e) = write
            .send(Message::binary(codec::encode_subscribe(id, chunk)))
            .await
        {
            return Some(Error::realtime(format!("market stream resubscribe: {e}")));
        }
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
                symbol: shared.cfg.pairs.symbol(symbol_id),
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
                if !state.active.contains(&Topic::new(symbol_id, Stream::Bbo)) {
                    return Ok(());
                }
                if let Some(prev) = state.quotes.get(&symbol_id)
                    && (epoch < prev.book_epoch
                        || (epoch == prev.book_epoch && update_id <= prev.update_id))
                {
                    return Ok(()); // an older state, or a re-send of the same one
                }
                let quote = BestBidOffer {
                    symbol_id,
                    symbol: shared.cfg.pairs.symbol(symbol_id),
                    book_epoch: epoch,
                    update_id,
                    transact_ts_ns: transact_ns,
                    send_ts_ns: send_ns,
                    bid,
                    ask,
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
                book.snapshot(epoch, update_id, levels_per_side, &bids, &asks)
                    .map_err(|m| {
                        StreamError::frame(sbe::depth_snapshot_codec::SBE_TEMPLATE_ID, m)
                    })?
            };
            if changed {
                shared.emit(Event::DepthSnapshot(DepthSnapshot {
                    symbol_id,
                    symbol: shared.cfg.pairs.symbol(symbol_id),
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
            {
                let mut state = write_unpoisoned(&shared.state);
                if !state.active.contains(&topic) {
                    return Ok(());
                }
                let Some(book) = state.books.get_mut(&topic) else {
                    return Ok(());
                };
                if let Err(refusal) = book.update(epoch, first, last, &bids, &asks) {
                    book.synced = false;
                    return Err(match refusal {
                        Refusal::Gap(message) => {
                            Error::MarketStream(StreamError::BookGap { symbol_id, message })
                        }
                        Refusal::Invalid(message) => {
                            StreamError::frame(sbe::depth_update_codec::SBE_TEMPLATE_ID, message)
                        }
                    });
                }
            }
            shared.emit(Event::DepthUpdate(DepthUpdate {
                symbol_id,
                symbol: shared.cfg.pairs.symbol(symbol_id),
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
        let handle = tokio::spawn(async move {
            let trades = backfill_paced(history.as_ref(), gap, pacing).await;
            task_shared.emit(Event::TradesBackfill(TradesBackfill { gap, trades }));
        });
        inner.backfills.retain(|h| !h.is_finished());
        inner.backfills.push(handle.abort_handle());
    }
}

fn on_subscribe_response(
    shared: &Shared,
    request_id: u64,
    results: Vec<(u32, Option<Stream>, sbe::subscribe_result::SubscribeResult)>,
) -> Result<()> {
    use sbe::subscribe_result::SubscribeResult as Wire;
    let unsubscribe = lock_unpoisoned(&shared.inner)
        .unsubscribes
        .contains(&request_id);
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
    {
        let mut state = write_unpoisoned(&shared.state);
        for r in &results {
            let Some(topic) = r.topic else { continue };
            if unsubscribe {
                if !matches!(
                    r.result,
                    SubscribeResult::Ok | SubscribeResult::NotSubscribed
                ) {
                    continue;
                }
                state.active.remove(&topic);
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
            } else if r.result.subscribed() {
                state.active.insert(topic);
                match topic.stream {
                    Stream::Trades => {
                        state
                            .trackers
                            .entry(topic.symbol_id)
                            .or_insert_with(|| Tracker::new(topic.symbol_id));
                    }
                    Stream::Depth20 | Stream::Depth => {
                        state
                            .books
                            .entry(topic)
                            .or_insert_with(|| Book::new(topic.stream));
                    }
                    Stream::Bbo => {}
                }
            }
        }
    }
    let waiter = {
        let mut inner = lock_unpoisoned(&shared.inner);
        if !unsubscribe {
            // Refused: not resubscribed on reconnect.
            for r in &results {
                if let Some(topic) = r.topic
                    && !r.result.subscribed()
                {
                    inner.wanted.retain(|w| *w != topic);
                }
            }
        }
        inner.unsubscribes.remove(&request_id);
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
    Ok(())
}

/// Records a pair's feed state. A stale pair's depth book waits for a
/// replacement snapshot, which the server sends before the pair is live
/// again: LIVE while the book still waits is a gap, recovered by
/// reconnecting.
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
        return Err(Error::MarketStream(StreamError::BookGap {
            symbol_id,
            message: "feed live again before the depth book's replacement snapshot".into(),
        }));
    }
    shared.emit(Event::MarketStatus(MarketStatus {
        symbol_id,
        feed,
        send_ts_ns: send_ns,
    }));
    Ok(())
}

#[cfg(test)]
pub(crate) fn offline(config: Config) -> (Arc<Shared>, mpsc::Receiver<Event>) {
    let (events, rx) = mpsc::channel(config.queue_size.max(1));
    let (stop, _) = watch::channel(false);
    let shared = Arc::new(Shared {
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

#[cfg(test)]
pub(crate) fn force_feed(shared: &Shared, symbol_id: u32, feed: FeedState) {
    write_unpoisoned(&shared.state)
        .feeds
        .insert(symbol_id, feed);
}
