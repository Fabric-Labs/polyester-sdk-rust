//! Env gates and dotenv loading.

use std::path::PathBuf;

/// Load dotenv files if present (repo `.env`, then sibling Python SDK `.env`).
pub fn load_dotenv() {
    if env_truthy("POLYESTER_TEST_DISABLE_DOTENV") {
        return;
    }
    let _ = dotenvy::dotenv();
    if std::env::var("POLYESTER_API_KEY_ID").is_err() {
        let sibling =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../polyester-sdk-python/.env");
        let _ = dotenvy::from_path(sibling);
    }
}

/// Truthy env: `1`, `true`, `yes`, `on` (case-insensitive).
pub fn env_truthy(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    }
}

/// Fail every `skip:` path closed during release/staging acceptance QA.
pub fn strict_live_enabled() -> bool {
    load_dotenv();
    env_truthy("POLYESTER_TEST_STRICT_LIVE")
}

/// Soft-skip unless `POLYESTER_TEST_MUTATION` is truthy. Returns false when skipped.
///
/// Under `POLYESTER_TEST_STRICT_LIVE=1`, a missing mutation gate fails closed.
pub fn require_mutation() -> bool {
    load_dotenv();
    if env_truthy("POLYESTER_TEST_MUTATION") {
        true
    } else if strict_live_enabled() {
        panic!("STRICT_LIVE: Set POLYESTER_TEST_MUTATION=1 to run mutation tests");
    } else {
        eprintln!("skip: Set POLYESTER_TEST_MUTATION=1 to run mutation tests");
        false
    }
}

/// Soft-skip unless `POLYESTER_TEST_FUNDED` is truthy.
///
/// Under `POLYESTER_TEST_STRICT_LIVE=1`, a missing funded gate fails closed.
pub fn require_funded() -> bool {
    load_dotenv();
    if env_truthy("POLYESTER_TEST_FUNDED") {
        true
    } else if strict_live_enabled() {
        panic!("STRICT_LIVE: Set POLYESTER_TEST_FUNDED=1 to run funded tests");
    } else {
        eprintln!("skip: Set POLYESTER_TEST_FUNDED=1 to run funded tests");
        false
    }
}

pub fn trade_e2e_enabled() -> bool {
    load_dotenv();
    env_truthy("POLYESTER_TEST_TRADE_E2E")
}

pub fn skip_funding_check() -> bool {
    load_dotenv();
    env_truthy("POLYESTER_TEST_SKIP_FUNDING_CHECK")
}

/// Minimum trading quote balance (human decimal). Default `"10"`.
pub fn min_trading_quote() -> rust_decimal::Decimal {
    load_dotenv();
    let raw = std::env::var("POLYESTER_TEST_MIN_TRADING_QUOTE")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "10".to_owned());
    raw.parse()
        .unwrap_or_else(|_| rust_decimal::Decimal::from(10))
}

pub fn internal_transfer_dest() -> Option<String> {
    load_dotenv();
    std::env::var("POLYESTER_TEST_INTERNAL_TRANSFER_DEST")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

pub fn env_trade_symbol() -> Option<String> {
    load_dotenv();
    std::env::var("POLYESTER_TEST_TRADE_SYMBOL")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

pub fn has_live_creds() -> bool {
    load_dotenv();
    std::env::var("POLYESTER_API_KEY_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .is_some()
        && std::env::var("POLYESTER_API_PRIVATE_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .is_some()
}

/// Long Centrifugo heartbeat test (35s). Off by default so CI stays fast.
pub fn realtime_heartbeat_enabled() -> bool {
    load_dotenv();
    env_truthy("POLYESTER_TEST_REALTIME_HEARTBEAT")
}
