//! POLY-8563: every live test that writes opts in to writes first.
//!
//! The live suite runs with read-only expectations unless
//! `POLYESTER_TEST_MUTATION` is set, so a live test that calls a client
//! method that changes server state must call `require_mutation()` (or a
//! stricter gate) before it. This reads the live tests' source and checks.

use std::path::Path;

/// Leading words of client methods that change server state.
const WRITE_PREFIXES: &[&str] = &[
    "create", "update", "delete", "copy", "modify", "replace", "cancel", "batch", "withdraw",
    "transfer", "pause", "resume", "start", "stop", "revoke", "claim", "rotate", "set",
];
/// Write-like names that only read.
const READS: &[&str] = &["create_subscription"];
/// Calls that opt a live test in to writes.
const GATES: &[&str] = &[
    "require_mutation()",
    "require_funded(",
    "require_account_wide_cleanup(",
];

/// The body of the function that starts at `from`, braces balanced.
fn body(source: &str, from: usize) -> &str {
    let open = from + source[from..].find('{').expect("function body");
    let mut depth = 0usize;
    for (offset, ch) in source[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open..=open + offset];
                }
            }
            _ => {}
        }
    }
    &source[open..]
}

/// `<receiver>.<service>.<method>(` calls in `body` whose method writes.
fn service_writes(body: &str) -> Vec<String> {
    let mut writes = Vec::new();
    for (index, _) in body.match_indices('(') {
        let head = &body[..index];
        let method_start = head
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(0, |i| i + 1);
        let method = &head[method_start..];
        if method_start == 0 || !head[..method_start].ends_with('.') {
            continue;
        }
        let owner = &head[..method_start - 1];
        let service_start = owner
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(0, |i| i + 1);
        let service = &owner[service_start..];
        // A service is reached through a client: `client.orders.create(`.
        if service.is_empty() || service_start == 0 || !owner[..service_start].ends_with('.') {
            continue;
        }
        let writes_state = WRITE_PREFIXES.iter().any(|prefix| method.starts_with(prefix))
            && !READS.contains(&method);
        let dry_run = match method {
            // cancel_all(symbol, dry_run, request_id)
            "cancel_all" => call_args(&body[index..]).get(1).is_some_and(|arg| arg == "true"),
            _ => method.starts_with("cancel_all") && body.contains("dry_run: true"),
        };
        if writes_state && !dry_run {
            writes.push(format!("{service}.{method}"));
        }
    }
    writes
}

/// The top-level arguments of the call whose `(` starts `call`, trimmed.
fn call_args(call: &str) -> Vec<String> {
    let mut args = vec![String::new()];
    let mut depth = 0usize;
    for ch in call.chars() {
        match ch {
            '(' | '[' | '{' => {
                depth += 1;
                if depth == 1 {
                    continue;
                }
            }
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            ',' if depth == 1 => {
                args.push(String::new());
                continue;
            }
            _ => {}
        }
        args.last_mut().unwrap().push(ch);
    }
    args.iter().map(|arg| arg.trim().to_owned()).collect()
}

#[test]
fn every_live_test_that_writes_requires_a_write_gate() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
    let mut ungated = Vec::new();
    let mut checked = 0;
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("live tests")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    files.sort();
    for path in files {
        let source = std::fs::read_to_string(&path).expect("read live test");
        for (at, _) in source.match_indices("#[tokio::test") {
            let fn_at = at + source[at..].find("fn ").expect("test fn");
            let name: String = source[fn_at + 3..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            let body = body(&source, fn_at);
            checked += 1;
            let writes = service_writes(body);
            if !writes.is_empty() && !GATES.iter().any(|gate| body.contains(gate)) {
                let file = path.file_name().unwrap().to_string_lossy();
                ungated.push(format!("{file}: {name} calls {writes:?}"));
            }
        }
    }
    assert!(checked > 50, "checked only {checked} live tests");
    assert!(
        ungated.is_empty(),
        "live tests write without require_mutation():\n  {}",
        ungated.join("\n  ")
    );
}
