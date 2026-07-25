//! Live integration tests against Polyester (skip without POLYESTER_* credentials).
//!
//! Run:
//! ```bash
//! cargo test --test integration -- --nocapture --test-threads=1
//! ```

// In strict live mode every existing `skip:` path fails closed. This keeps the
// ordinary credential-optional suite ergonomic while preventing release QA
// from reporting success when required behavior was not exercised.
macro_rules! eprintln {
    ($($arg:tt)*) => {{
        let message = format!($($arg)*);
        if crate::support::strict_live_enabled() && message.starts_with("skip:") {
            panic!("strict live mode rejected soft skip: {message}");
        }
        std::eprintln!("{message}");
    }};
}

mod account;
mod account_admin;
mod auth;
mod balances;
mod funded_chain_userop;
mod funded_internal_transfer;
mod funded_market_fill;
mod funded_order_holds;
mod funded_spot_fill;
mod lifecycle_app;
mod market;
mod market_order_mutation;
mod money;
mod orders;
mod orders_mutation;
mod private_realtime;
mod realtime;
mod support;
mod transfers;
mod triggers;
mod triggers_mutation;

use support::load_dotenv;

#[tokio::test]
async fn client_from_env_builds_when_credentials_present() {
    load_dotenv();
    if std::env::var("POLYESTER_API_KEY_ID").is_err()
        || std::env::var("POLYESTER_API_PRIVATE_KEY").is_err()
    {
        eprintln!("skip: POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY required");
        return;
    }
    let client = polyester::Client::from_env().expect("Client::from_env");
    let _ = client;
}
