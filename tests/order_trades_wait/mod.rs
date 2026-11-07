//! `wait_for_order_trades_complete` over local GetOrder execution pages.
//!
//! A schedule of polls answers GetOrder: a request without a page token starts
//! the next poll (the last one repeats), and page `i` of a poll is requested
//! with page token `t<i>`. Pages can be slow, never answer, or point back at an
//! earlier page token.

use super::hardening_support::{GET_ORDER_PATH, HttpScript, MockHttpServer};
use buffa::Message as _;
use polyester::models::{GetOrderResult, OrderKey};
use polyester::proto::orders::v1::{
    GetOrderRequest, GetOrderResponse, Order, OrderStatus, UserTrade,
};
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// Far above every wait timeout below: a page read that outlives its wait
/// shows up as a multi-second return.
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_MESSAGE: &str = "timed out waiting for order trades";

#[derive(Clone, Debug, Default)]
struct Page {
    trades: Vec<i64>,
    next: String,
    complete: bool,
    delay: Duration,
    hang: bool,
}

fn page(trades: &[i64], next: &str, complete: bool) -> Page {
    Page {
        trades: trades.to_vec(),
        next: next.into(),
        complete,
        ..Default::default()
    }
}

#[derive(Default)]
struct ServerState {
    poll: usize,
    started: bool,
    arrivals: Vec<Instant>,
}

struct TradesServer {
    http: MockHttpServer,
    state: Arc<Mutex<ServerState>>,
}

impl TradesServer {
    async fn spawn(cum: i64, polls: Vec<Vec<Page>>) -> Self {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let handler_state = state.clone();
        let http = MockHttpServer::spawn(move |req| {
            if req.path != GET_ORDER_PATH {
                return HttpScript::NotFound;
            }
            let request = GetOrderRequest::decode_from_slice(&req.body).expect("GetOrderRequest");
            let token = request.page_token.clone();
            let mut state = handler_state.lock().unwrap();
            state.arrivals.push(Instant::now());
            if token.is_empty() {
                if state.started && state.poll < polls.len() - 1 {
                    state.poll += 1;
                }
                state.started = true;
            }
            let pages = &polls[state.poll];
            let index: usize = if token.is_empty() {
                0
            } else {
                token.trim_start_matches('t').parse().expect("page token")
            };
            let page = &pages[index];
            if page.hang {
                return HttpScript::NeverRespond {
                    stall: Duration::from_secs(30),
                };
            }
            let response = GetOrderResponse {
                // Terminal: this SDK's wait also waits for a terminal status.
                order: Some(Order {
                    order_id: 1,
                    symbol_id: 1,
                    status: OrderStatus::Filled.into(),
                    cum_qty_scaled: cum,
                    ..Default::default()
                })
                .into(),
                next_page_token: page.next.clone(),
                executions_complete: page.complete,
                trades: page
                    .trades
                    .iter()
                    .enumerate()
                    .map(|(i, qty)| UserTrade {
                        symbol_id: 1,
                        order_id: 1,
                        match_id: (1000 * index + i + 1) as u64,
                        qty_scaled: *qty,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            HttpScript::RawAfter {
                delay: page.delay,
                status: 200,
                headers: vec![("Content-Type".into(), "application/proto".into())],
                body: response.encode_to_bytes().to_vec(),
            }
        })
        .await;
        Self { http, state }
    }

    fn client(&self) -> Client {
        Client::new(Config {
            api_url: self.http.base_url(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            timeout: HTTP_TIMEOUT,
            ..Default::default()
        })
        .expect("client")
    }

    fn arrivals(&self) -> Vec<Instant> {
        self.state.lock().unwrap().arrivals.clone()
    }
}

async fn wait_trades(client: &Client, timeout: Duration) -> polyester::Result<GetOrderResult> {
    client
        .orders
        .wait_for_order_trades_complete(OrderKey::OrderId("1".into()), timeout)
        .await
}

fn trade_qtys(result: &GetOrderResult) -> Vec<i64> {
    result
        .trades
        .iter()
        .filter_map(|trade| trade.qty.as_ref().map(|qty| qty.as_scaled()))
        .collect()
}

fn is_wait_timeout(error: &Error) -> bool {
    error.to_string().contains(TIMEOUT_MESSAGE)
}

/// The result carries the server's executions_complete from the read it was
/// assembled from, whether that read had one page or several.
#[tokio::test]
async fn l2_wait_keeps_the_final_reads_executions_complete() {
    let cases: Vec<(&str, Vec<Vec<Page>>, bool)> = vec![
        ("single page", vec![vec![page(&[100], "", true)]], true),
        (
            "terminal page of three",
            vec![vec![
                page(&[40], "t1", true),
                page(&[20], "t2", true),
                page(&[40], "", true),
            ]],
            true,
        ),
        (
            "recorded on a later poll",
            vec![
                vec![page(&[40], "t1", false), page(&[], "", false)],
                vec![page(&[40], "t1", true), page(&[60], "", true)],
            ],
            true,
        ),
        (
            "server still reports incomplete",
            vec![vec![page(&[50], "t1", false), page(&[50], "", false)]],
            false,
        ),
    ];
    for (name, polls, want) in cases {
        let server = TradesServer::spawn(100, polls).await;
        let result = wait_trades(&server.client(), Duration::from_secs(2))
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(trade_qtys(&result).iter().sum::<i64>(), 100, "{name}");
        assert_eq!(result.executions_complete, want, "{name}");
    }
}

#[tokio::test]
async fn l2_wait_follows_fast_pages() {
    let server = TradesServer::spawn(
        100,
        vec![vec![
            page(&[10], "t1", true),
            page(&[20], "t2", true),
            page(&[30], "t3", true),
            page(&[40], "", true),
        ]],
    )
    .await;
    let result = wait_trades(&server.client(), Duration::from_secs(1))
        .await
        .expect("wait");
    assert_eq!(trade_qtys(&result), vec![10, 20, 30, 40]);
}

/// Four 35 ms pages need about 140 ms; a 50 ms wait must time out.
#[tokio::test]
async fn l2_wait_times_out_across_slow_pages() {
    let pages = (0..4)
        .map(|i| Page {
            delay: Duration::from_millis(35),
            ..page(&[25], if i < 3 { ["t1", "t2", "t3"][i] } else { "" }, true)
        })
        .collect();
    let server = TradesServer::spawn(100, vec![pages]).await;
    let started = Instant::now();
    match wait_trades(&server.client(), Duration::from_millis(50)).await {
        Ok(result) => panic!(
            "wait with a 50 ms timeout succeeded after {:?} with {} trades",
            started.elapsed(),
            result.trades.len()
        ),
        Err(error) => assert!(is_wait_timeout(&error), "{error}"),
    }
}

/// A page still in flight at the timeout is abandoned, not awaited until the
/// HTTP timeout ends it.
#[tokio::test]
async fn l2_wait_abandons_a_page_at_its_timeout() {
    let server = TradesServer::spawn(
        100,
        vec![vec![
            page(&[40], "t1", false),
            Page {
                hang: true,
                ..Default::default()
            },
        ]],
    )
    .await;
    let started = Instant::now();
    let error = wait_trades(&server.client(), Duration::from_millis(100))
        .await
        .expect_err("wait succeeded on a page that never answers");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(1100),
        "wait with a 100 ms timeout returned after {elapsed:?}: {error}"
    );
    assert!(is_wait_timeout(&error), "{error}");
}

/// A next_page_token that repeats an earlier page token would page until the
/// timeout: the wait stops at once with a response contract error.
#[tokio::test]
async fn l2_wait_rejects_a_repeated_page_token() {
    let server = TradesServer::spawn(
        100,
        vec![vec![
            page(&[10], "t1", false),
            page(&[10], "t2", false),
            page(&[10], "t1", false),
        ]],
    )
    .await;
    let started = Instant::now();
    let error = wait_trades(&server.client(), Duration::from_secs(5))
        .await
        .expect_err("wait succeeded on a repeating page token");
    assert!(
        matches!(error, Error::ResponseContract { .. }),
        "want a response contract error, got {error} after {:?} and {} page reads",
        started.elapsed(),
        server.arrivals().len()
    );
    assert_eq!(server.arrivals().len(), 3);
}

/// splitmix64: every random choice of a schedule comes from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Success,
    Cycle,
    Timeout,
}

struct Schedule {
    cum: i64,
    timeout: Duration,
    polls: Vec<Vec<Page>>,
    outcome: Outcome,
    want_trades: Vec<i64>,
    want_complete: bool,
}

fn random_schedule(seed: u64) -> Schedule {
    let mut r = Rng(seed);
    let mut s = Schedule {
        cum: 100,
        timeout: Duration::from_millis(30 + r.below(270)),
        polls: Vec::new(),
        outcome: Outcome::Timeout,
        want_trades: Vec::new(),
        want_complete: false,
    };
    let n_polls = 1 + r.below(3) as usize;
    for p in 0..n_polls {
        let n_pages = 1 + r.below(5) as usize;
        let adds = p == n_polls - 1 && r.below(5) != 0;
        let mut remaining = s.cum;
        let mut pages = Vec::new();
        for i in 0..n_pages {
            let mut page = Page {
                next: if i < n_pages - 1 {
                    format!("t{}", i + 1)
                } else {
                    String::new()
                },
                complete: r.below(3) != 0,
                ..Default::default()
            };
            page.delay = Duration::from_millis(if r.below(3) == 0 {
                r.below(60)
            } else {
                r.below(4)
            });
            for _ in 0..r.below(3) {
                if remaining <= 1 {
                    break;
                }
                let qty = 1 + r.below(remaining as u64 - 1) as i64;
                page.trades.push(qty);
                remaining -= qty;
            }
            pages.push(page);
        }
        if adds && remaining > 0 {
            pages.last_mut().unwrap().trades.push(remaining);
        }
        match r.below(8) {
            0 => {
                let i = r.below(n_pages as u64) as usize;
                pages[i].hang = true;
            }
            1 if n_pages > 1 => {
                pages[n_pages - 1].next = format!("t{}", 1 + r.below(n_pages as u64 - 1));
            }
            _ => {}
        }
        s.polls.push(pages);
    }
    for pages in &s.polls {
        let mut trades = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut token = String::new();
        let outcome = loop {
            let index: usize = if token.is_empty() {
                0
            } else {
                token.trim_start_matches('t').parse().unwrap()
            };
            let page = &pages[index];
            if page.hang {
                break Some(Outcome::Timeout);
            }
            trades.extend(page.trades.iter().copied());
            if page.next.is_empty() {
                if trades.iter().sum::<i64>() == s.cum {
                    s.want_trades = trades.clone();
                    s.want_complete = page.complete;
                    break Some(Outcome::Success);
                }
                break None;
            }
            if !seen.insert(page.next.clone()) {
                break Some(Outcome::Cycle);
            }
            token = page.next.clone();
        };
        if let Some(outcome) = outcome {
            s.outcome = outcome;
            return s;
        }
    }
    s
}

async fn run_schedule(seed: u64) {
    // Scheduling slack on a loaded machine; the HTTP timeout is 5 s, so an
    // unbounded page read still stands out.
    let slack = Duration::from_millis(750);
    let s = random_schedule(seed);
    let replay = format!(
        "WAIT_SEED={seed} (timeout {:?}, {} polls, outcome {:?})",
        s.timeout,
        s.polls.len(),
        s.outcome
    );
    let server = TradesServer::spawn(s.cum, s.polls.clone()).await;
    let client = server.client();
    let started = Instant::now();
    let deadline = started + s.timeout;
    let result =
        tokio::time::timeout(Duration::from_secs(4), wait_trades(&client, s.timeout)).await;
    let elapsed = started.elapsed();
    let result = result.unwrap_or_else(|_| panic!("{replay}: wait still running after 4 s"));
    assert!(
        elapsed <= s.timeout + slack,
        "{replay}: wait returned after {elapsed:?}"
    );
    for (i, at) in server.arrivals().iter().enumerate() {
        assert!(
            *at <= deadline + slack,
            "{replay}: page read {i} started {:?} after the deadline",
            at.duration_since(deadline)
        );
    }
    match result {
        Ok(result) => {
            assert_eq!(s.outcome, Outcome::Success, "{replay}: wait succeeded");
            assert_eq!(trade_qtys(&result), s.want_trades, "{replay}: trades");
            assert_eq!(
                result.executions_complete, s.want_complete,
                "{replay}: executions_complete"
            );
        }
        Err(Error::ResponseContract { .. }) => {
            assert_eq!(
                s.outcome,
                Outcome::Cycle,
                "{replay}: unexpected contract error"
            );
        }
        Err(error) if is_wait_timeout(&error) => {
            assert!(
                s.outcome == Outcome::Timeout || elapsed >= s.timeout,
                "{replay}: timed out after {elapsed:?}, before the deadline"
            );
        }
        Err(error) => panic!("{replay}: unexpected error {error}"),
    }
}

/// Random schedules vary the polls before trades add up (1-3), pages per poll
/// (1-5), trades per page (0-3), per-page delays (0-60 ms), the wait timeout
/// (30-300 ms), each terminal page's executions_complete, a page that never
/// answers and a repeated next_page_token. Whatever the schedule, the wait
/// returns within its timeout plus slack and starts no page after its
/// deadline; a success is the schedule's, with that read's trades and terminal
/// executions_complete; a repeated token is a contract error; anything else is
/// a timeout once the deadline passed.
///
/// WAIT_SEEDS sets the seed count (default 64); WAIT_SEED replays one seed.
/// Sixteen schedules run at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_wait_random_schedules() {
    let seeds: Vec<u64> = match std::env::var("WAIT_SEED") {
        Ok(seed) => vec![seed.parse().expect("WAIT_SEED")],
        Err(_) => (0..std::env::var("WAIT_SEEDS")
            .map(|count| count.parse().expect("WAIT_SEEDS"))
            .unwrap_or(64))
            .collect(),
    };
    for batch in seeds.chunks(16) {
        let runs: Vec<_> = batch
            .iter()
            .map(|seed| tokio::spawn(run_schedule(*seed)))
            .collect();
        for run in runs {
            if let Err(error) = run.await {
                std::panic::resume_unwind(error.into_panic());
            }
        }
    }
}
