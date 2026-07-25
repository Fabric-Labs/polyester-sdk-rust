//! Read-only live smoke: concurrent auth + public :proto trades subscription.
//!
//!   set -a && source .env && set +a
//!   cargo run --example live_smoke_qa

use futures_util::future::join_all;
use polyester::Client;
use std::env;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if env::var("POLYESTER_API_KEY_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .is_none()
        || env::var("POLYESTER_API_PRIVATE_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .is_none()
    {
        eprintln!("FAIL: missing API key env");
        std::process::exit(2);
    }

    let client = match Client::from_env() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("FAIL: client: {err}");
            std::process::exit(1);
        }
    };
    if let Err(err) = client.wait_for_catalogs().await {
        eprintln!("FAIL: wait_for_catalogs: {err}");
        std::process::exit(1);
    }

    let futs = (0..32).map(|_| {
        let orders = client.orders.clone();
        async move { orders.list_open(None).await }
    });
    let results = join_all(futs).await;
    let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    if !failures.is_empty() {
        eprintln!(
            "FAIL: concurrent list_open: {}/32 errors; sample: {:?}",
            failures.len(),
            failures[0]
        );
        std::process::exit(1);
    }
    println!("OK: concurrent list_open 32/32");

    let symbol = env::var("POLYESTER_TEST_SMOKE_SYMBOL").unwrap_or_else(|_| "BTC-USDT".into());
    let mut sub = match client.market_data.subscribe_trades(&symbol).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("FAIL: subscribe_trades: {err}");
            std::process::exit(1);
        }
    };

    let deadline = Instant::now() + Duration::from_secs(25);
    let mut got = 0usize;
    while Instant::now() < deadline && got < 1 {
        match tokio::time::timeout(Duration::from_secs(3), sub.recv()).await {
            Ok(Some(_)) => got += 1,
            Ok(None) => {
                eprintln!("FAIL: trades subscription ended without publications");
                std::process::exit(1);
            }
            Err(_) => {}
        }
    }
    sub.close();
    if got < 1 {
        eprintln!("FAIL: no public trades publications on {symbol} within 25s");
        std::process::exit(1);
    }
    println!("OK: public trades :proto received {got} publication(s) on {symbol}");
}
