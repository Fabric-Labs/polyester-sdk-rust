//! Every RPC the SDK sends is signed or not as the contract says.
//!
//! `testdata/rpc_auth.tsv` lists every RPC of the public contract as `public`
//! (marked `polyester.api.public`: sent unsigned, callable without an API
//! key) or `signed`. This test reads the generated Connect clients under
//! `src/gen/connect` and the handwritten wrappers under `src`: a wrapper
//! signs a call by sending it through a generated `*_with_options` method
//! with the options [`crate::services::unary::await_auth`] and its siblings
//! sign, and sends a public call through the plain generated method inside
//! `await_public` or `await_public_connect`. It fails when the inventory and
//! the generated clients disagree on the set of RPCs, when a `public` RPC is
//! signed (a client without a key could not send it), or when a `signed` RPC
//! is sent unsigned.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Public,
    Signed,
}

fn inventory() -> BTreeMap<String, Access> {
    let mut out = BTreeMap::new();
    for line in include_str!("../testdata/rpc_auth.tsv").lines() {
        if line.starts_with('#') || line.starts_with("procedure\t") || line.is_empty() {
            continue;
        }
        let (procedure, access) = line.split_once('\t').expect("procedure<TAB>access");
        let access = match access {
            "public" => Access::Public,
            "signed" => Access::Signed,
            other => panic!("{procedure}: unknown access {other:?}"),
        };
        assert!(
            out.insert(procedure.to_owned(), access).is_none(),
            "{procedure} is listed twice"
        );
    }
    out
}

/// Generated client method name to the procedures it sends. Two services may
/// share a method name.
fn generated_methods() -> BTreeMap<String, BTreeSet<String>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/gen/connect");
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let source = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        let mut pending: Option<String> = None;
        for line in source.lines() {
            let line = line.trim();
            if let Some(rest) = line.split_once("Sends a request to ").map(|(_, r)| r) {
                pending = Some(rest.trim_end_matches('.').to_owned());
            } else if let Some(rest) = line.strip_prefix("pub async fn ")
                && let Some(procedure) = pending.take()
            {
                let name = rest.split('(').next().unwrap().to_owned();
                out.entry(name).or_default().insert(procedure);
            }
        }
    }
    out
}

fn handwritten_sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let name = path.file_name().unwrap();
                if name != "gen" && name != "connect_gen" && name != "proto" {
                    walk(&path, out);
                }
            } else if path.extension().is_some_and(|e| e == "rs")
                && !path.ends_with("rpc_auth_routing_tests.rs")
            {
                out.push((path.clone(), std::fs::read_to_string(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut out);
    out
}

/// The identifiers called as methods (`.name(`) in `text`.
fn method_calls(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    for (i, _) in text.match_indices('.') {
        let mut end = i + 1;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        if end > i + 1 && bytes.get(end) == Some(&b'(') {
            out.push(text[i + 1..end].to_owned());
        }
    }
    out
}

/// The text between the parentheses of a call whose `(` is at `open`.
fn call_arguments(text: &str, open: usize) -> &str {
    let mut depth = 0usize;
    for (i, c) in text[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &text[open + 1..open + i];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced call at byte {open}");
}

#[test]
fn inventory_lists_every_generated_rpc() {
    let listed: BTreeSet<String> = inventory().into_keys().collect();
    let generated: BTreeSet<String> = generated_methods().into_values().flatten().collect();
    let missing: Vec<_> = generated.difference(&listed).collect();
    let gone: Vec<_> = listed.difference(&generated).collect();
    assert!(
        missing.is_empty() && gone.is_empty(),
        "testdata/rpc_auth.tsv is out of date with the contract: \
         not listed {missing:?}, listed but gone {gone:?}"
    );
}

#[test]
fn wrappers_sign_exactly_the_signed_rpcs() {
    let access = inventory();
    let methods = generated_methods();
    let access_of = |name: &str| -> Option<(String, Access)> {
        let procedures = methods.get(name)?;
        let kinds: BTreeSet<_> = procedures.iter().map(|p| access[p] as u8).collect();
        assert_eq!(
            kinds.len(),
            1,
            "generated method {name} sends {procedures:?} with different access; \
             tell them apart here"
        );
        let first = procedures.iter().next().unwrap().clone();
        let kind = access[&first];
        Some((first, kind))
    };
    let mut failures = Vec::new();
    let mut signed_sent = BTreeSet::new();
    let mut public_sent = BTreeSet::new();
    for (path, source) in handwritten_sources() {
        let file = path
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .display()
            .to_string();
        // Signed: a generated `*_with_options` call, whose options are signed.
        for name in method_calls(&source) {
            let Some(base) = name.strip_suffix("_with_options") else {
                continue;
            };
            if let Some((procedure, kind)) = access_of(base) {
                if kind == Access::Public {
                    failures.push(format!(
                        "{file}: {procedure} is public in the contract but signed; \
                         send it with unary::await_public so a client without a key can call it"
                    ));
                }
                signed_sent.insert(procedure);
            }
        }
        // Unsigned: a plain generated call inside `await_public(...)`.
        for helper in ["await_public(", "await_public_connect("] {
            for (at, _) in source.match_indices(helper) {
                // Skip the helpers' own definitions.
                if source[..at].ends_with("fn ") {
                    continue;
                }
                let args = call_arguments(&source, at + helper.len() - 1);
                let sent: Vec<_> = method_calls(args)
                    .into_iter()
                    .filter_map(|name| access_of(&name))
                    .collect();
                if sent.is_empty() {
                    failures.push(format!(
                        "{file}: {helper}..) sends no generated client call this test can see"
                    ));
                }
                for (procedure, kind) in sent {
                    if kind == Access::Signed {
                        failures.push(format!(
                            "{file}: {procedure} needs a signature in the contract but is sent unsigned"
                        ));
                    }
                    public_sent.insert(procedure);
                }
            }
        }
    }
    let both: Vec<_> = signed_sent.intersection(&public_sent).collect();
    if !both.is_empty() {
        failures.push(format!("sent both signed and unsigned: {both:?}"));
    }
    assert!(!public_sent.is_empty() && !signed_sent.is_empty());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn public_lifecycle_flow_reads_are_public() {
    let access = inventory();
    for procedure in [
        "/chain.lifecycle.v1.LifecycleReadService/ListFlows",
        "/chain.lifecycle.v1.LifecycleReadService/GetFlowById",
        "/chain.lifecycle.v1.LifecycleReadService/ListFlowsByTx",
    ] {
        assert_eq!(access[procedure], Access::Public, "{procedure}");
    }
    for procedure in [
        "/chain.lifecycle.v1.LifecycleReadService/ListAccountFlows",
        "/chain.lifecycle.v1.LifecycleReadService/GetAccountFlow",
    ] {
        assert_eq!(access[procedure], Access::Signed, "{procedure}");
    }
}
