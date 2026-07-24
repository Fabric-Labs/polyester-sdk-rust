use crate::support::{call_required, has_live_creds, load_dotenv, require_live_client};
use polyester::transport::WireFormat;
use polyester::{Client, Config};

#[tokio::test]
async fn auth_me_returns_identity_fields() {
    let Some(client) = require_live_client() else {
        return;
    };
    let me = call_required("auth.me", || client.auth.me()).await;
    assert!(
        !me.account_id.is_empty() && me.account_id != "0"
            || me.api_key_id.is_some()
            || me.username.is_some(),
        "me() should identify the caller: {me:?}"
    );
}

#[tokio::test]
async fn auth_me_works_with_json_wire_format() {
    load_dotenv();
    if !has_live_creds() {
        eprintln!("skip: POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY required");
        return;
    }

    let mut config = Config {
        api_key_id: std::env::var("POLYESTER_API_KEY_ID").ok(),
        api_private_key: std::env::var("POLYESTER_API_PRIVATE_KEY").ok(),
        default_account_id: std::env::var("POLYESTER_ACCOUNT_ID").ok(),
        wire_format: WireFormat::Json,
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

    let client = Client::new(config).expect("JSON client");
    let me = call_required("auth.me (JSON)", || client.auth.me()).await;
    assert!(
        !me.account_id.is_empty() && me.account_id != "0"
            || me.api_key_id.is_some()
            || me.username.is_some(),
        "JSON me() should identify the caller: {me:?}"
    );
}
