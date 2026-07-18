//! Auth-related SDK models.

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ed25519Keypair {
    pub public_key_hex: String,
    pub secret_key_hex: String,
    pub public_key: Vec<u8>,
    pub secret_key: Vec<u8>,
}
