//! Live integration tests against Polyester (skip without POLYESTER_* credentials).
//!
//! Run:
//! ```bash
//! cargo test --test integration -- --nocapture
//! ```

mod account;
mod account_admin;
mod auth;
mod balances;
mod funded_chain_userop;
mod funded_internal_transfer;
mod funded_market_fill;
mod funded_order_holds;
mod funded_spot_fill;
mod funded_transfer_to_user;
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
