//! A request's timeout also bounds its wait for signing capacity.
//!
//! Every credentials value for one API key takes signing timestamps from one
//! allocator, which refuses to run more than
//! [`crate::auth::MAX_SIGNING_FUTURE_SKEW_MS`] ahead of the clock. A burst of
//! requests can use all of that lead, and a request then waits for the clock
//! before it is sent. That wait is part of the request: it ends at the
//! request's deadline with the error the request's own timeout gives, sends
//! nothing and takes no timestamp, and what follows gets only the time left.
//!
//! The tests hold a key's capacity for an hour, so only a deadline or the
//! allocator's ten-second cap can end the wait.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use crate::auth::Credentials;
use crate::errors::Error;
use crate::proto::auth::v1::MeRequest;
use crate::transport::{Config as TransportConfig, Factory};
use crate::{Client, Config};

pub(crate) const SEED_HEX: &str =
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf";
/// The deadline of the urgent request.
pub(crate) const TIMEOUT: Duration = Duration::from_millis(100);
/// Room for scheduling on a busy host; far below the ten-second cap.
pub(crate) const MARGIN: Duration = Duration::from_millis(500);
/// Ends a test that regresses to the cap instead of waiting it out.
pub(crate) const GUARD: Duration = Duration::from_secs(5);

const ME: &str = "/auth.v1.AuthService/Me";

/// A server that reads each request head and never answers. It counts the
/// requests for `path`, with any query; any other request, such as a port
/// probe, counts for nothing.
pub(crate) async fn silent_server(path: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let count = count.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                    if head.len() > 64 * 1024 {
                        return;
                    }
                }
                let line = head.split(|b| *b == b'\n').next().unwrap_or_default();
                let target = String::from_utf8_lossy(line)
                    .split_whitespace()
                    .nth(1)
                    .map(str::to_owned);
                if target.is_some_and(|t| t.split('?').next() == Some(path)) {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                // Never answer: hold the connection until the client leaves.
                let _ = tokio::time::timeout(Duration::from_secs(30), async {
                    while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
                })
                .await;
            });
        }
    });
    (addr, requests)
}

fn keyed_client(api_url: String, key_id: &str, timeout: Duration) -> Client {
    Client::new(Config {
        api_key_id: Some(key_id.into()),
        api_private_key: Some(SEED_HEX.into()),
        api_url,
        hydrate_catalogs: false,
        timeout,
        ..Default::default()
    })
    .expect("client")
}

#[tokio::test]
async fn a_request_timeout_bounds_its_wait_for_signing_capacity() {
    let key_id = "ak_signing_deadline_connect";
    let (addr, requests) = silent_server(ME).await;
    let client = keyed_client(format!("http://{addr}"), key_id, TIMEOUT);

    // With capacity free, the server never answers: the transport's timeout.
    let transport_timeout = client.auth.me().await.expect_err("no answer");
    assert_eq!(requests.load(Ordering::SeqCst), 1);

    let held = Credentials::new(key_id, SEED_HEX)
        .unwrap()
        .hold_signing_capacity();
    let started = Instant::now();
    let err = tokio::time::timeout(GUARD, client.auth.me())
        .await
        .expect("the request's own timeout ends it, not the ten-second cap")
        .expect_err("capacity is held");
    let elapsed = started.elapsed();

    assert!(
        elapsed < TIMEOUT + MARGIN,
        "took {elapsed:?} with a {TIMEOUT:?} timeout"
    );
    assert!(matches!(err, Error::Transport { .. }), "{err:?}");
    assert_eq!(err.to_string(), transport_timeout.to_string());
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "the expired request was sent"
    );
    let creds = Credentials::new(key_id, SEED_HEX).unwrap();
    assert_eq!(
        creds.last_signing_timestamp(),
        held,
        "a timestamp was taken"
    );
}

#[tokio::test(start_paused = true)]
async fn the_call_gets_only_the_time_left_after_signing() {
    let creds = Credentials::new("ak_signing_deadline_remaining", SEED_HEX).unwrap();
    let factory = Factory::new(
        TransportConfig {
            api_url: "http://127.0.0.1:9".into(),
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        Some(creds.clone()),
    )
    .unwrap();

    // The request waits in the signing queue while 1.25 s of its 2 s pass.
    let queue = creds.hold_signing_queue().await;
    let signing =
        tokio::spawn(async move { factory.sign_options_async(ME, &MeRequest::default()).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_millis(1250)).await;
    drop(queue);

    let options = signing.await.unwrap().expect("signed in time");
    assert_eq!(options.timeout(), Some(Duration::from_millis(750)));
}
