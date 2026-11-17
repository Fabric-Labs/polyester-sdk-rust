//! A [`crate::ReconnectLimiter`] shared by market-data streams, against local
//! servers that count the upgrades in flight
//! ([`crate::reconnect_test_server`]).

use std::sync::Arc;
use std::time::Duration;

use super::client::TestPairs;
use crate::ReconnectLimiter;
use crate::marketstream::{Config, Event, MarketStream, PATH, connect};
use crate::reconnect_test_server::{self as scenario, Limited, Responder};

struct Stream(MarketStream);

impl Limited for Stream {
    const PATH: &'static str = PATH;

    fn responder() -> Responder {
        Arc::new(|_: &[u8]| None)
    }

    async fn open(
        url: String,
        limiter: Option<ReconnectLimiter>,
        connect_timeout: Duration,
    ) -> Self {
        let mut config = Config::new(url, Arc::new(TestPairs));
        config.ping_interval = Duration::from_secs(3600);
        config.pong_timeout = Duration::from_secs(3600);
        config.reconnect_limiter = limiter;
        config.connect_timeout = connect_timeout;
        Self(connect(config).await.expect("connects"))
    }

    async fn reconnected(&mut self) -> Vec<(Option<f64>, Duration)> {
        let mut failures = Vec::new();
        loop {
            match self.0.recv().await.expect("an event") {
                Event::Disconnected { error, delay } => failures.push((error.retry_after(), delay)),
                Event::Connected { reconnect: true } => return failures,
                _ => {}
            }
        }
    }

    fn close(&self) {
        self.0.close();
    }

    fn is_alive(&self) -> bool {
        self.0.is_alive()
    }
}

const N: usize = 16;
const K: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_dropped_together_upgrade_at_most_k_at_once() {
    let (connecting, reconnecting) = scenario::dropped_together::<Stream>(N, Some(K)).await;
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
async fn without_a_limiter_streams_reconnect_unbounded_as_before() {
    let (connecting, reconnecting) = scenario::dropped_together::<Stream>(N, None).await;
    assert!(connecting > K, "{connecting} first upgrades at once");
    assert!(reconnecting > K, "{reconnecting} reconnects at once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_upgrade_gives_its_slot_back() {
    scenario::a_refusal_gives_the_slot_back::<Stream>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_upgrade_gives_its_slot_back() {
    scenario::a_timeout_gives_the_slot_back::<Stream>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_a_stream_waiting_for_a_slot_ends_it_with_no_upgrade() {
    scenario::a_close_while_queued_ends_the_wait::<Stream>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_for_a_slot_never_shortens_retry_after() {
    scenario::retry_after_is_honoured::<Stream>().await;
}
