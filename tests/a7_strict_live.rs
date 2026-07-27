//! POLY-3746 A7: STRICT_LIVE fail-closed + live harness counts (fast tests).
//!
//! Suite commands (document / CI):
//! - Public (no API key): `cargo test --test integration money::`
//! - Credentialed live: `POLYESTER_TEST_STRICT_LIVE=1 cargo test --test integration -- --test-threads=1`
//! - Min executed floor (default 5): `POLYESTER_TEST_MIN_EXECUTED=5` under STRICT_LIVE

use polyester::{Client, Config};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

fn cargo_bin() -> &'static str {
    static CARGO: OnceLock<String> = OnceLock::new();
    CARGO.get_or_init(|| env!("CARGO").to_owned())
}

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

/// Serialize cargo subprocesses so they share one compile lock / warm target.
fn cargo_subprocess_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HarnessCounts {
    passed: usize,
    failed: usize,
    ignored: usize,
}

impl HarnessCounts {
    fn executed(self) -> usize {
        self.passed + self.failed
    }
}

/// Parse `test result: ok. N passed; M failed; K ignored` (or `FAILED.`).
fn parse_harness_counts(output: &str) -> Option<HarnessCounts> {
    for line in output.lines().rev() {
        let line = line.trim();
        if !line.starts_with("test result:") {
            continue;
        }
        let mut passed = 0usize;
        let mut failed = 0usize;
        let mut ignored = 0usize;
        for part in line.split(';') {
            let part = part.trim();
            if let Some(rest) = part.strip_suffix(" passed") {
                passed = rest
                    .rsplit_once(' ')
                    .map(|(_, n)| n)
                    .unwrap_or(rest)
                    .parse()
                    .ok()?;
            } else if let Some(rest) = part.strip_suffix(" failed") {
                failed = rest
                    .rsplit_once(' ')
                    .map(|(_, n)| n)
                    .unwrap_or(rest)
                    .parse()
                    .ok()?;
            } else if let Some(rest) = part.strip_suffix(" ignored") {
                ignored = rest
                    .rsplit_once(' ')
                    .map(|(_, n)| n)
                    .unwrap_or(rest)
                    .parse()
                    .ok()?;
            }
        }
        return Some(HarnessCounts {
            passed,
            failed,
            ignored,
        });
    }
    None
}

/// Mirrors the integration `eprintln!` soft-skip → panic contract under STRICT_LIVE.
fn reject_soft_skip_if_strict(strict: bool, message: &str) -> Result<(), String> {
    if strict && message.starts_with("skip:") {
        return Err(format!("strict live mode rejected soft skip: {message}"));
    }
    Ok(())
}

#[test]
fn a7_parse_harness_counts_from_cargo_summary() {
    let sample = r#"
running 3 tests
test money::a ... ok
test money::b ... ok
test money::c ... FAILED

failures:

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
"#;
    let counts = parse_harness_counts(sample).expect("parse");
    assert_eq!(
        counts,
        HarnessCounts {
            passed: 2,
            failed: 1,
            ignored: 0
        }
    );
    assert_eq!(counts.executed(), 3);
}

#[test]
fn a7_strict_live_soft_skip_contract_fails_closed_fast() {
    assert!(
        reject_soft_skip_if_strict(false, "skip: missing creds").is_ok(),
        "non-strict must allow soft skip"
    );
    let err = reject_soft_skip_if_strict(
        true,
        "skip: POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY required",
    )
    .expect_err("STRICT_LIVE must fail closed on soft skip");
    assert!(err.contains("strict live"));
}

#[test]
fn a7_malformed_private_key_fails_closed() {
    let err = match Client::new(Config {
        api_key_id: Some("ak_test".into()),
        api_private_key: Some("not-a-valid-hex-key".into()),
        hydrate_catalogs: false,
        ..Default::default()
    }) {
        Ok(_) => panic!("malformed key must fail"),
        Err(err) => err,
    };
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("private")
            || msg.contains("key")
            || msg.contains("hex")
            || msg.contains("auth"),
        "{err}"
    );
}

#[test]
fn a7_config_debug_redacts_private_key() {
    let config = Config {
        api_key_id: Some("ak_test".into()),
        api_private_key: Some("super-secret-private-key".into()),
        ..Default::default()
    };
    let rendered = format!("{config:?}");
    assert!(rendered.contains("ak_test"));
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("super-secret-private-key"));
}

#[test]
fn a7_permission_declarations_cover_private_realtime_groups() {
    // Keep in sync with tests/integration/private_realtime.rs module docs (F-24 / B6b).
    let src = include_str!("integration/private_realtime.rs");
    for needle in [
        "address_book.subscribe",
        "transfers.subscribe",
        "balances.subscribe",
        "transfer:read",
        "trading read",
        "ledger read",
        "auth admin read",
    ] {
        assert!(
            src.contains(needle),
            "private_realtime permission fixture missing declaration: {needle}"
        );
    }
}

#[test]
fn a7_non_dry_run_cancel_all_tests_require_dedicated_account_gate() {
    let integration = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
    let mut unguarded = Vec::new();
    let mut unserialized_mutations = Vec::new();
    for entry in std::fs::read_dir(integration).expect("read integration tests") {
        let path = entry.expect("integration entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("read integration test");
        if source.contains(".cancel_all(")
            && source.contains(", false,")
            && !source.contains("require_account_wide_cleanup")
        {
            unguarded.push(path.clone());
        }
        if source.contains("require_mutation()") && !source.contains("mutation_test_guard().await")
        {
            unserialized_mutations.push(path);
        }
    }
    assert!(
        unguarded.is_empty(),
        "unguarded cancel_all tests: {unguarded:?}"
    );
    assert!(
        unserialized_mutations.is_empty(),
        "state-changing tests without the shared mutation guard: {unserialized_mutations:?}"
    );
}

#[test]
fn a7_public_vs_credentialed_suite_filters_documented() {
    // Public suite: pure money constructors (no network / no API key).
    // Credentialed suite: full integration target under STRICT_LIVE.
    let main = include_str!("integration/main.rs");
    assert!(
        main.contains("cargo test --test integration money::"),
        "integration main must document the public suite command"
    );
    assert!(
        main.contains("POLYESTER_TEST_STRICT_LIVE=1"),
        "integration main must document the credentialed STRICT_LIVE command"
    );
}

#[test]
fn a7_strict_live_missing_creds_fails_client_from_env_skip() {
    let _guard = cargo_subprocess_lock().lock().expect("cargo lock");
    let output = Command::new(cargo_bin())
        .args([
            "test",
            "--test",
            "integration",
            "client_from_env_builds_when_credentials_present",
            "--",
            "--exact",
            "--nocapture",
        ])
        .current_dir(manifest_dir())
        .env("POLYESTER_TEST_STRICT_LIVE", "1")
        .env_remove("POLYESTER_API_KEY_ID")
        .env_remove("POLYESTER_API_PRIVATE_KEY")
        .env("POLYESTER_TEST_DISABLE_DOTENV", "1")
        .env("CARGO_TERM_COLOR", "never")
        .output()
        .expect("spawn cargo test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        !combined.contains("no test target named"),
        "integration test target missing:\n{combined}"
    );
    assert!(
        !output.status.success(),
        "STRICT_LIVE with missing creds must fail, got success.\n{combined}"
    );
    assert!(
        combined.contains("strict live")
            || combined.contains("STRICT_LIVE")
            || combined.contains("soft skip")
            || combined.contains("rejected"),
        "expected strict-live failure message, got:\n{combined}"
    );
}

#[test]
fn a7_public_suite_harness_counts_and_min_executed_floor() {
    let _guard = cargo_subprocess_lock().lock().expect("cargo lock");
    let min_floor: usize = std::env::var("POLYESTER_TEST_MIN_EXECUTED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    let output = Command::new(cargo_bin())
        .args([
            "test",
            "--test",
            "integration",
            "money::",
            "--",
            "--nocapture",
        ])
        .current_dir(manifest_dir())
        .env("POLYESTER_TEST_DISABLE_DOTENV", "1")
        .env_remove("POLYESTER_TEST_STRICT_LIVE")
        .env("CARGO_TERM_COLOR", "never")
        .output()
        .expect("spawn public suite");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        output.status.success(),
        "public money:: suite must pass:\n{combined}"
    );
    let counts = parse_harness_counts(&combined).unwrap_or_else(|| {
        panic!("failed to parse harness counts from:\n{combined}");
    });
    eprintln!(
        "A7 live harness counts: executed={} skipped={} failed={} (public money::)",
        counts.executed(),
        counts.ignored,
        counts.failed
    );
    assert_eq!(counts.failed, 0);
    assert!(
        counts.executed() >= min_floor,
        "public suite release gate requires at least {min_floor} executed tests; got {}",
        counts.executed()
    );
}
