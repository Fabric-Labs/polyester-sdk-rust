use crate::support::{call_required, require_live_client};

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
