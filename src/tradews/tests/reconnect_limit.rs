//! A [`crate::ReconnectLimiter`] shared by trading sessions, against local
//! servers that count the upgrades in flight
//! ([`crate::reconnect_test_server`]).

use std::sync::Arc;
use std::time::Duration;

use super::session::{credentials, pong, subscription_ack};
use crate::ReconnectLimiter;
use crate::reconnect_test_server::{self as scenario, Limited, Responder};
use crate::tradews::{Config, Event, PATH, TradeSession, connect};

struct Session {
    session: TradeSession,
    connection: u64,
}

impl Limited for Session {
    const PATH: &'static str = PATH;

    fn responder() -> Responder {
        Arc::new(|frame: &[u8]| {
            let template = u16::from_le_bytes([frame[2], frame[3]]);
            let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
            match template {
                6 => Some(subscription_ack(seq)),
                1 => Some(pong(seq)),
                _ => None,
            }
        })
    }

    async fn open(
        url: String,
        limiter: Option<ReconnectLimiter>,
        connect_timeout: Duration,
    ) -> Self {
        let mut config = Config::new(url, credentials());
        config.ping_interval = Duration::from_secs(2);
        config.answer_timeout = Duration::from_secs(2);
        config.reconnect_limiter = limiter;
        config.connect_timeout = connect_timeout;
        Self {
            session: connect(config).await.expect("connects"),
            connection: 1,
        }
    }

    async fn reconnected(&mut self) -> Vec<(Option<f64>, Duration)> {
        let mut failures = Vec::new();
        loop {
            match self.session.recv().await.expect("an event") {
                Event::Disconnected { error, delay, .. } => {
                    failures.push((error.retry_after(), delay));
                }
                Event::Connected { connection } if connection > self.connection => {
                    self.connection = connection;
                    return failures;
                }
                _ => {}
            }
        }
    }

    fn close(&self) {
        self.session.close();
    }

    fn is_alive(&self) -> bool {
        self.session.is_alive()
    }
}

const N: usize = 16;
const K: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_dropped_together_upgrade_at_most_k_at_once() {
    let (connecting, reconnecting) = scenario::dropped_together::<Session>(N, Some(K)).await;
    assert!(
        connecting <= K,
        "{connecting} first upgrades at once, limit {K}"
    );
    assert!(
        reconnecting <= K,
        "{reconnecting} reconnects at once, limit {K}"
    );
    // The cap is reached, so the scenario does contend for it.
    assert_eq!(reconnecting, K);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_limiter_sessions_reconnect_unbounded_as_before() {
    let (connecting, reconnecting) = scenario::dropped_together::<Session>(N, None).await;
    assert!(connecting > K, "{connecting} first upgrades at once");
    assert!(reconnecting > K, "{reconnecting} reconnects at once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_upgrade_gives_its_slot_back() {
    scenario::a_refusal_gives_the_slot_back::<Session>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_upgrade_gives_its_slot_back() {
    scenario::a_timeout_gives_the_slot_back::<Session>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_a_session_waiting_for_a_slot_ends_it_with_no_upgrade() {
    scenario::a_close_while_queued_ends_the_wait::<Session>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_for_a_slot_never_shortens_retry_after() {
    scenario::retry_after_is_honoured::<Session>().await;
}
