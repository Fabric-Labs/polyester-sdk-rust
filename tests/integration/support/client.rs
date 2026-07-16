//! Live client builders.

use super::env::{has_live_creds, load_dotenv};
use polyester::{Client, Config};

/// Build a live client or soft-skip (returns None) when API-key env is missing.
pub fn require_live_client() -> Option<Client> {
    load_dotenv();
    if !has_live_creds() {
        eprintln!("skip: POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY required");
        return None;
    }
    match Client::from_env() {
        Ok(client) => Some(client),
        Err(err) => {
            eprintln!("skip: failed to build client: {err}");
            None
        }
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
