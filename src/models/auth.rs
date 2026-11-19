//! Auth-related SDK models.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeResult {
    pub account_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_smart_account_address: Option<String>,
    /// The session of the presented access token; `None` when the server
    /// sends none, as for an API key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<MeSession>,
    /// The account scope of the API key that authenticated the call, as its
    /// proto name: `"SCOPE_ROOT"` for a key of the main account,
    /// `"SCOPE_SUBACCOUNT"` for a key bound to one sub-account, or
    /// `"UNKNOWN(n)"` for a scope this version does not know. `None` for a
    /// session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_scope: Option<String>,
    /// The sub-account a `"SCOPE_SUBACCOUNT"` key is bound to, as a base58
    /// ID; `None` otherwise. The key's private channels and account reads
    /// name this sub-account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subaccount_id: Option<String>,
}

impl MeResult {
    /// The account whose private channels and account reads the caller
    /// names: the bound sub-account of a sub-account key, else
    /// `account_id`.
    pub fn account_channel_target(&self) -> &str {
        self.subaccount_id.as_deref().unwrap_or(&self.account_id)
    }
}

/// The assurance of a session, as `auth.me` reads it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MeSession {
    /// Shared by the session's primary and MFA-elevated tokens.
    pub session_id: String,
    /// The session's assurance level as its proto name
    /// (`"PRIMARY_AUTHENTICATED"`, `"MFA_ELEVATED"`, `"FRESH_STEP_UP"`), or
    /// `"UNKNOWN(n)"` for a level this version does not know.
    pub session_level: String,
    /// How the session authenticated, such as `"wallet"` or `"totp"`.
    pub authentication_methods: Vec<String>,
    /// When the session authenticated or was elevated, in Unix milliseconds;
    /// `None` when the server sends no time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UserProfile {
    pub username: String,
    pub bio: String,
    pub website: String,
    pub twitter: String,
    pub twitter_verified: bool,
    pub discord: String,
    pub discord_verified: bool,
    pub avatar_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_username_change_at_ms: Option<i64>,
    pub vip_tier: i32,
    pub username_unlocked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UsernameHistoryEntry {
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UsernameHistoryList {
    pub entries: Vec<UsernameHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AccountIdentity {
    pub account_id: String,
    pub username: String,
    pub avatar_url: String,
    pub root_smart_account_address: String,
}

/// Locally generated Ed25519 keypair for API key creation.
///
/// [`Debug`] redacts secret material so accidental logging cannot leak the
/// private key. Read `secret_key_hex` / `secret_key` explicitly when you need
/// the secret.
#[derive(Clone, PartialEq, Eq)]
pub struct Ed25519Keypair {
    pub public_key_hex: String,
    pub secret_key_hex: String,
    pub public_key: Vec<u8>,
    pub secret_key: Vec<u8>,
}

impl fmt::Debug for Ed25519Keypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ed25519Keypair")
            .field("public_key_hex", &self.public_key_hex)
            .field("secret_key_hex", &"[REDACTED]")
            .field("public_key", &self.public_key)
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_debug_redacts_secret_key() {
        let seed = [0xa0u8; 32];
        let keypair = Ed25519Keypair {
            public_key_hex: "ab".repeat(32),
            secret_key_hex: hex::encode(seed),
            public_key: vec![2; 32],
            secret_key: seed.to_vec(),
        };
        let markers = [&hex::encode(&seed[..8])[..], "160, 160, 160, 160"];
        for (path, rendered) in [
            ("debug", format!("{keypair:?}")),
            ("pretty debug", format!("{keypair:#?}")),
            ("option", format!("{:?}", Some(&keypair))),
            ("vec", format!("{:#?}", vec![keypair.clone()])),
        ] {
            for marker in markers {
                assert!(
                    !rendered.contains(marker),
                    "{path} leaked key material: {rendered}"
                );
            }
            assert!(rendered.contains("[REDACTED]"), "{path}: {rendered}");
        }
    }
}
