//! A local WebSocket server that counts the upgrades in flight, and the
//! [`crate::ReconnectLimiter`] scenarios both WebSocket clients run against
//! it. No frame leaves this machine.
//!
//! An upgrade is counted from the moment its whole request has arrived until
//! the server decides its answer, before the answer is written: inside the
//! window in which the client holds its slot, which begins before it connects
//! and ends once it has read the answer. The highest count seen is therefore
//! never above the limit unless the client exceeded it. An upgrade the server
//! never answers leaves the count once its hold ends: when its client gives
//! the slot back is not visible here, so its scenario checks the order of the
//! upgrades instead.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use crate::ReconnectLimiter;

/// How long a scenario waits for any one step.
pub(crate) const STEP: Duration = Duration::from_secs(10);

/// Upgrades in flight across every server that shares it.
#[derive(Default)]
pub(crate) struct Gauge {
    now: AtomicUsize,
    max: AtomicUsize,
}

impl Gauge {
    fn enter(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }

    /// The most upgrades seen in flight at once since the last reset.
    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::SeqCst)
    }

    pub(crate) fn reset_max(&self) {
        self.max
            .store(self.now.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

/// How the server answers one upgrade.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Reply {
    Accept,
    /// Refuse it with this status and `Retry-After`.
    Refuse(u16, Option<&'static str>),
    /// Never answer it.
    Hang,
}

/// An upgrade's answer, given after `hold`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Plan {
    pub hold: Duration,
    pub reply: Reply,
}

impl Plan {
    pub(crate) fn accept(hold: Duration) -> Self {
        Self {
            hold,
            reply: Reply::Accept,
        }
    }
}

/// One upgrade the server saw.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Upgrade {
    /// When its whole request had arrived.
    pub arrived: Instant,
    /// When its answer was decided; `None` for one never answered.
    pub answered: Option<Instant>,
}

/// Answers each binary frame of an accepted connection, if it answers it.
pub(crate) type Responder = Arc<dyn Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync>;

struct State {
    gauge: Arc<Gauge>,
    /// Plans for the next upgrades, in order; `default` once they run out.
    plans: Mutex<(VecDeque<Plan>, Plan)>,
    upgrades: Mutex<Vec<Upgrade>>,
    /// Bumped to close every accepted connection at once.
    drop_all: watch::Sender<u64>,
    responder: Responder,
}

pub(crate) struct UpgradeServer {
    pub url: String,
    state: Arc<State>,
}

impl UpgradeServer {
    /// Serves `path` on a free loopback port, counting its upgrades on
    /// `gauge`; `plans` answer the first upgrades and `default` every later
    /// one.
    pub(crate) async fn start(
        path: &str,
        gauge: Arc<Gauge>,
        responder: Responder,
        plans: Vec<Plan>,
        default: Plan,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}{path}", listener.local_addr().unwrap());
        let state = Arc::new(State {
            gauge,
            plans: Mutex::new((plans.into(), default)),
            upgrades: Mutex::new(Vec::new()),
            drop_all: watch::channel(0).0,
            responder,
        });
        let accepting = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(Arc::clone(&accepting), tcp));
            }
        });
        Self { url, state }
    }

    /// Closes every accepted connection at once.
    pub(crate) fn drop_all(&self) {
        self.state.drop_all.send_modify(|n| *n += 1);
    }

    pub(crate) fn upgrades(&self) -> Vec<Upgrade> {
        self.state.upgrades.lock().unwrap().clone()
    }
}

/// Whether the client's whole upgrade request has reached `tcp`, read
/// without taking it off the socket; false once the peer goes away.
async fn request_arrived(tcp: &tokio::net::TcpStream) -> bool {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match tcp.peek(&mut buf).await {
            Ok(0) | Err(_) => return false,
            Ok(n) if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") => {
                // Other processes probe local ports: only an upgrade counts.
                return buf[..n].starts_with(b"GET ");
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(1)).await,
        }
    }
}

// The handshake callback's error type is tungstenite's HTTP response.
#[allow(clippy::result_large_err)]
async fn serve(state: Arc<State>, tcp: tokio::net::TcpStream) {
    if !request_arrived(&tcp).await {
        return;
    }
    let arrived = Instant::now();
    state.gauge.enter();
    let plan = {
        let mut plans = state.plans.lock().unwrap();
        let default = plans.1;
        plans.0.pop_front().unwrap_or(default)
    };
    let index = {
        let mut all = state.upgrades.lock().unwrap();
        all.push(Upgrade {
            arrived,
            answered: None,
        });
        all.len() - 1
    };
    tokio::time::sleep(plan.hold).await;
    if let Reply::Hang = plan.reply {
        state.gauge.leave();
        // Hold the connection, unanswered, until the client leaves.
        let mut tcp = tcp;
        let mut buf = [0u8; 1024];
        while matches!(tcp.read(&mut buf).await, Ok(n) if n > 0) {}
        return;
    }
    let mut drop_all = state.drop_all.subscribe();
    drop_all.borrow_and_update();
    let decided = Arc::clone(&state);
    let callback = move |request: &Request, mut response: Response| {
        // Out of flight before the answer is written, so before the client
        // can read it and give its slot back.
        decided.upgrades.lock().unwrap()[index].answered = Some(Instant::now());
        decided.gauge.leave();
        if let Reply::Refuse(status, retry_after) = plan.reply {
            let mut refusal = ErrorResponse::new(Some("UNAVAILABLE".to_owned()));
            *refusal.status_mut() = StatusCode::from_u16(status).unwrap();
            if let Some(retry_after) = retry_after {
                refusal
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static(retry_after));
            }
            return Err(refusal);
        }
        // The trading WebSocket refuses an upgrade that does not echo its
        // cancel-on-disconnect opt-in; the market-data stream sends none.
        if let Some(grace) = request.headers().get("x-cancel-on-disconnect-grace-sec") {
            let grace = grace.clone();
            let h = response.headers_mut();
            h.insert(
                "x-cancel-on-disconnect",
                HeaderValue::from_static("ACCOUNT"),
            );
            h.insert("x-cancel-on-disconnect-grace-sec", grace);
        }
        Ok(response)
    };
    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
        return;
    };
    loop {
        tokio::select! {
            _ = drop_all.changed() => {
                let _ = ws.close(None).await;
                return;
            }
            frame = ws.next() => match frame {
                Some(Ok(Message::Binary(frame))) => {
                    if let Some(reply) = (state.responder)(&frame)
                        && ws.send(Message::Binary(reply.into())).await.is_err()
                    {
                        return;
                    }
                }
                Some(Ok(_)) => {}
                _ => return,
            },
        }
    }
}

/// A WebSocket client the scenarios drive: a trading session or a market-data
/// stream.
pub(crate) trait Limited: Sized {
    /// The WebSocket path the client connects to.
    const PATH: &'static str;

    /// Answers the client's frames on an accepted connection.
    fn responder() -> Responder;

    /// Connects to `url`.
    fn open(
        url: String,
        limiter: Option<ReconnectLimiter>,
        connect_timeout: Duration,
    ) -> impl Future<Output = Self>;

    /// Waits for the client's next connection, after a loss; returns the
    /// `Retry-After` and wait of every failure it reported meanwhile.
    fn reconnected(&mut self) -> impl Future<Output = Vec<(Option<f64>, Duration)>>;

    fn close(&self);

    fn is_alive(&self) -> bool;
}

async fn server<C: Limited>(gauge: &Arc<Gauge>, plans: Vec<Plan>, default: Plan) -> UpgradeServer {
    UpgradeServer::start(C::PATH, Arc::clone(gauge), C::responder(), plans, default).await
}

const SLOW: Duration = Duration::from_millis(250);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// `n` clients connect at once and then all drop at once, each upgrade held
/// for a while; with a limit of `k`, no more than `k` upgrades are ever in
/// flight, first connections included, and every client gets back. Returns
/// the most upgrades seen in flight while connecting and while reconnecting.
pub(crate) async fn dropped_together<C: Limited>(n: usize, limit: Option<usize>) -> (usize, usize) {
    let gauge = Arc::new(Gauge::default());
    let server = server::<C>(&gauge, Vec::new(), Plan::accept(SLOW)).await;
    let limiter = limit.map(|k| ReconnectLimiter::new(k).unwrap());
    let opened = futures_util::future::join_all((0..n).map(|_| {
        tokio::time::timeout(
            STEP,
            C::open(server.url.clone(), limiter.clone(), DEFAULT_TIMEOUT),
        )
    }))
    .await;
    let mut clients: Vec<C> = opened
        .into_iter()
        .map(|c| c.expect("connects in time"))
        .collect();
    let connecting = gauge.max();
    gauge.reset_max();
    server.drop_all();
    tokio::time::timeout(
        STEP,
        futures_util::future::join_all(clients.iter_mut().map(C::reconnected)),
    )
    .await
    .expect("every client reconnects");
    let reconnecting = gauge.max();
    assert_eq!(server.upgrades().len(), 2 * n, "one reconnect each");
    if let Some(limiter) = &limiter {
        assert_eq!(limiter.upgrades_in_flight(), 0, "every slot given back");
    }
    for c in &clients {
        c.close();
    }
    (connecting, reconnecting)
}

/// Two clients share a limit of one. Each connects, and is then dropped; each
/// reconnect upgrade is answered after a hold by `first`, and the next by an
/// acceptance. The second upgrade starts only once the first has ended, and
/// both clients get back: the first upgrade's slot was given back. Returns
/// how long after the first upgrade arrived the second did.
async fn one_at_a_time<C: Limited>(first: Plan, connect_timeout: Duration) -> Duration {
    let gauge = Arc::new(Gauge::default());
    let limiter = ReconnectLimiter::new(1).unwrap();
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..2 {
        let server = server::<C>(
            &gauge,
            vec![Plan::accept(Duration::ZERO), first],
            Plan::accept(Duration::ZERO),
        )
        .await;
        clients.push(C::open(server.url.clone(), Some(limiter.clone()), connect_timeout).await);
        servers.push(server);
    }
    gauge.reset_max();
    for s in &servers {
        s.drop_all();
    }
    tokio::time::timeout(
        STEP,
        futures_util::future::join_all(clients.iter_mut().map(C::reconnected)),
    )
    .await
    .expect("both clients reconnect");
    assert!(gauge.max() <= 1, "{} upgrades at once", gauge.max());
    assert_eq!(limiter.upgrades_in_flight(), 0, "every slot given back");
    let mut first_reconnects: Vec<Upgrade> = servers.iter().map(|s| s.upgrades()[1]).collect();
    first_reconnects.sort_by_key(|u| u.arrived);
    let [earlier, later] = [first_reconnects[0], first_reconnects[1]];
    if let Some(answered) = earlier.answered {
        assert!(
            later.arrived >= answered,
            "the second upgrade started {:?} before the first was answered",
            answered - later.arrived
        );
    }
    for c in &clients {
        c.close();
    }
    later.arrived - earlier.arrived
}

/// A refused upgrade gives its slot back: the queued client upgrades next.
pub(crate) async fn a_refusal_gives_the_slot_back<C: Limited>() {
    let refuse = Plan {
        hold: SLOW,
        reply: Reply::Refuse(503, None),
    };
    let gap = one_at_a_time::<C>(refuse, DEFAULT_TIMEOUT).await;
    assert!(gap >= SLOW, "the second upgrade waited only {gap:?}");
}

/// An upgrade that times out gives its slot back: the queued client upgrades
/// next, once the first upgrade's connect timeout has run out.
pub(crate) async fn a_timeout_gives_the_slot_back<C: Limited>() {
    let timeout = Duration::from_millis(400);
    let hang = Plan {
        hold: Duration::ZERO,
        reply: Reply::Hang,
    };
    let gap = one_at_a_time::<C>(hang, timeout).await;
    assert!(
        gap >= timeout - Duration::from_millis(50),
        "the second upgrade waited only {gap:?}"
    );
}

/// Two clients share a limit of one; the first reconnect upgrade hangs and
/// holds the slot, so the other client waits for it. Closing the waiting
/// client ends it at once, while the slot is still held, and it makes no
/// upgrade afterwards, not even once the slot is free.
pub(crate) async fn a_close_while_queued_ends_the_wait<C: Limited>() {
    let gauge = Arc::new(Gauge::default());
    let limiter = ReconnectLimiter::new(1).unwrap();
    let hang = Plan {
        hold: Duration::ZERO,
        reply: Reply::Hang,
    };
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..2 {
        let server = server::<C>(&gauge, vec![Plan::accept(Duration::ZERO)], hang).await;
        clients.push(C::open(server.url.clone(), Some(limiter.clone()), DEFAULT_TIMEOUT).await);
        servers.push(server);
    }
    for s in &servers {
        s.drop_all();
    }
    // Wait for one reconnect upgrade, then past the longest first backoff
    // (500 ms), so the other client is queued behind it.
    tokio::time::timeout(STEP, async {
        while servers.iter().map(|s| s.upgrades().len()).sum::<usize>() < 3 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("one reconnect upgrade");
    tokio::time::sleep(Duration::from_millis(700)).await;
    let counts: Vec<usize> = servers.iter().map(|s| s.upgrades().len()).collect();
    assert_eq!(
        counts.iter().sum::<usize>(),
        3,
        "one upgrade at a time: {counts:?}"
    );
    assert_eq!(limiter.upgrades_in_flight(), 1);
    let queued = counts
        .iter()
        .position(|&c| c == 1)
        .expect("a queued client");
    let holding = 1 - queued;

    clients[queued].close();
    tokio::time::timeout(Duration::from_secs(2), async {
        while clients[queued].is_alive() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the close ends the wait while the slot is still held");
    assert_eq!(limiter.upgrades_in_flight(), 1, "the slot is still held");

    // Free the slot: the closed client must not take it.
    clients[holding].close();
    tokio::time::timeout(STEP, async {
        while limiter.upgrades_in_flight() > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("closing the upgrading client gives its slot back");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        servers[queued].upgrades().len(),
        1,
        "no upgrade after the close"
    );
}

/// Three clients share a limit of one, and each first reconnect is refused
/// with `503` and `Retry-After: 0.6` after a hold, so the later clients also
/// wait for the slot. Each client's next upgrade still comes at least 0.6 s
/// after its refusal, and each reports a wait of at least 0.6 s.
pub(crate) async fn retry_after_is_honoured<C: Limited>() {
    let gauge = Arc::new(Gauge::default());
    let limiter = ReconnectLimiter::new(1).unwrap();
    let refuse = Plan {
        hold: Duration::from_millis(200),
        reply: Reply::Refuse(503, Some("0.6")),
    };
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..3 {
        let server = server::<C>(
            &gauge,
            vec![Plan::accept(Duration::ZERO), refuse],
            Plan::accept(Duration::ZERO),
        )
        .await;
        clients.push(C::open(server.url.clone(), Some(limiter.clone()), DEFAULT_TIMEOUT).await);
        servers.push(server);
    }
    gauge.reset_max();
    for s in &servers {
        s.drop_all();
    }
    let failures = tokio::time::timeout(
        STEP,
        futures_util::future::join_all(clients.iter_mut().map(C::reconnected)),
    )
    .await
    .expect("every client reconnects");
    assert!(gauge.max() <= 1, "{} upgrades at once", gauge.max());
    for (server, failures) in servers.iter().zip(&failures) {
        let upgrades = server.upgrades();
        assert_eq!(upgrades.len(), 3, "{upgrades:?}");
        let refused = upgrades[1].answered.expect("the refusal");
        let gap = upgrades[2].arrived - refused;
        assert!(
            gap >= Duration::from_millis(600),
            "upgraded again {gap:?} after a 0.6 s Retry-After"
        );
        let (retry_after, delay) = failures
            .iter()
            .copied()
            .find(|(r, _)| r.is_some())
            .expect("the refusal is reported");
        assert_eq!(retry_after, Some(0.6));
        assert!(delay >= Duration::from_millis(600), "{delay:?}");
    }
    for c in &clients {
        c.close();
    }
}
