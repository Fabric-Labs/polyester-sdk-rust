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
