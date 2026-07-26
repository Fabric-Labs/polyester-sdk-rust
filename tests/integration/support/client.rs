//! Live client builders.

use super::env::{has_live_creds, load_dotenv, strict_live_enabled};
use polyester::{Client, Config};

fn fail_or_skip(message: &str) -> Option<Client> {
    if strict_live_enabled() {
        panic!("STRICT_LIVE: {message}");
    }
    eprintln!("skip: {message}");
    None
}

/// Build a live client or soft-skip (returns None) when API-key env is missing.
///
/// Under `POLYESTER_TEST_STRICT_LIVE=1`, missing/bad credentials fail the test
/// instead of soft-skipping (A7 false-green fix).
pub fn require_live_client() -> Option<Client> {
    load_dotenv();
    if !has_live_creds() {
        return fail_or_skip("POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY required");
    }
    match Client::from_env() {
        Ok(client) => Some(client),
        Err(err) => fail_or_skip(&format!("failed to build client: {err}")),
    }
}

/// Second client from `POLYESTER_TEST_MAKER_API_KEY_ID` / `POLYESTER_TEST_MAKER_API_PRIVATE_KEY`.
pub fn maker_client_from_env() -> Option<Client> {
    load_dotenv();
    let key_id = std::env::var("POLYESTER_TEST_MAKER_API_KEY_ID")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())?;
    let private_key = std::env::var("POLYESTER_TEST_MAKER_API_PRIVATE_KEY")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())?;

    let mut config = Config {
        api_key_id: Some(key_id),
        api_private_key: Some(private_key),
        hydrate_catalogs: true,
        ..Default::default()
    };
    if let Ok(url) = std::env::var("POLYESTER_API_URL")
        && !url.trim().is_empty()
    {
        config.api_url = url;
    }
    if let Ok(url) = std::env::var("POLYESTER_WS_URL")
        && !url.trim().is_empty()
    {
        config.ws_url = url;
    }
    match Client::new(config) {
        Ok(c) => Some(c),
        Err(err) => {
            eprintln!("skip: maker client failed: {err}");
            None
        }
    }
}
