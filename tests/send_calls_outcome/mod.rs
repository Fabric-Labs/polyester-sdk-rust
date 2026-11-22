//! `send_calls` keeps the UserOperation hash once the send may have been
//! accepted.
//!
//! A local mock plays the chain RPC, the bundler and the paymaster on
//! 127.0.0.1 and stores every UserOperation it accepts. It computes each
//! operation's hash itself, from the JSON the SDK sent, so the hash the SDK
//! reports is checked against what the bundler holds. No chain is contacted;
//! the owner key is a throwaway test value.

use super::hardening_support::{HttpScript, MockHttpServer};
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::SolValue;
use polyester::Error;
use polyester::chain::{
    ChainCall, POLYESTER_DEVNET_ENVIRONMENT, PolyesterSmartAccount, SendCallsResult,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const OWNER_KEY: &str = "0x4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f"; // throwaway

fn withdraw() -> ChainCall {
    ChainCall {
        to: "0x1111111111111111111111111111111111111111".into(),
        data: vec![0xde, 0xad, 0xbe, 0xef],
        value: 0,
    }
}

fn hex_bytes(value: &Value) -> Vec<u8> {
    let text = value.as_str().expect("hex string");
    hex::decode(text.trim_start_matches("0x")).expect("hex")
}

fn quantity(value: &Value) -> U256 {
    let text = value.as_str().expect("quantity");
    U256::from_str_radix(text.trim_start_matches("0x"), 16).expect("quantity")
}

fn word(hi: U256, lo: U256) -> B256 {
    B256::from((hi << 128) | lo)
}

/// EntryPoint v0.7 `getUserOpHash` of a JSON UserOperation, as a bundler
/// computes it.
fn mock_user_operation_hash(op: &Value, entry_point: &str, chain_id: u64) -> String {
    let init_code = match op.get("factory") {
        Some(factory) => [hex_bytes(factory), hex_bytes(&op["factoryData"])].concat(),
        None => Vec::new(),
    };
    let paymaster_and_data = match op.get("paymaster") {
        Some(paymaster) => [
            hex_bytes(paymaster),
            quantity(&op["paymasterVerificationGasLimit"]).to_be_bytes::<32>()[16..].to_vec(),
            quantity(&op["paymasterPostOpGasLimit"]).to_be_bytes::<32>()[16..].to_vec(),
            hex_bytes(&op["paymasterData"]),
        ]
        .concat(),
        None => Vec::new(),
    };
    let sender: Address = op["sender"].as_str().unwrap().parse().unwrap();
    let packed = (
        sender,
        quantity(&op["nonce"]),
        keccak256(init_code),
        keccak256(hex_bytes(&op["callData"])),
        word(
            quantity(&op["verificationGasLimit"]),
            quantity(&op["callGasLimit"]),
        ),
        quantity(&op["preVerificationGas"]),
        word(
            quantity(&op["maxPriorityFeePerGas"]),
            quantity(&op["maxFeePerGas"]),
        ),
        keccak256(paymaster_and_data),
    )
        .abi_encode();
    let entry_point: Address = entry_point.parse().unwrap();
    let outer = (keccak256(packed), entry_point, U256::from(chain_id)).abi_encode();
    format!("0x{}", hex::encode(keccak256(outer)))
}

/// The mock chain RPC + bundler + paymaster state and its failure switches.
#[derive(Default)]
struct State {
    /// Hashes of the operations `eth_sendUserOperation` accepted, in order.
    operations: Vec<String>,
    /// (A) receipt polls answer HTTP 503 this many times.
    receipt_503s: usize,
    /// (B) the send's answer is delayed this long after it is stored.
    send_delay: Duration,
    /// (C) the send is stored, then answered with this HTTP status.
    send_status: Option<u16>,
    /// A definite refusal: the send is answered with this error, not stored.
    send_error: Option<Value>,
    /// The send is stored, then answered with this JSON-RPC error (HTTP 200).
    stored_send_error: Option<Value>,
    /// The send is stored, then answered with this result instead of its hash.
    send_reply: Option<Value>,
    /// Receipt reads answer null while true.
    receipt_missing: bool,
    /// Receipt and status reads are answered this late.
    receipt_delay: Duration,
    status_delay: Duration,
}

struct Bundler {
    state: Arc<Mutex<State>>,
    http: MockHttpServer,
}

impl Bundler {
    async fn spawn() -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let chain_id = POLYESTER_DEVNET_ENVIRONMENT.chain_id;
        let http = MockHttpServer::spawn(move |request| {
            let body: Value = serde_json::from_slice(&request.body).expect("json-rpc request");
            let (status, answer, delay) = handle(&shared, chain_id, &body);
            match answer {
                None => HttpScript::Json {
                    status,
                    body: b"upstream unavailable".to_vec(),
                },
                Some(mut answer) => {
                    answer["jsonrpc"] = json!("2.0");
                    answer["id"] = body["id"].clone();
                    HttpScript::RawAfter {
                        delay,
                        status,
                        headers: vec![("Content-Type".into(), "application/json".into())],
                        body: serde_json::to_vec(&answer).unwrap(),
                    }
                }
            }
        })
        .await;
        Self { state, http }
    }

    fn held(&self) -> Vec<String> {
        self.state.lock().unwrap().operations.clone()
    }

    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    fn account(&self, timeout: Duration) -> PolyesterSmartAccount {
        let mut environment = POLYESTER_DEVNET_ENVIRONMENT.clone();
        environment.rpc_url = self.http.base_url();
        environment.account_abstraction.bundler_url = self.http.base_url();
        environment.account_abstraction.paymaster_url = self.http.base_url();
        PolyesterSmartAccount::new(OWNER_KEY, Some(environment), 0, timeout).expect("account")
    }
}

fn handle(state: &Mutex<State>, chain_id: u64, body: &Value) -> (u16, Option<Value>, Duration) {
    let params = &body["params"];
    let mut state = state.lock().unwrap();
    let ok = |result: Value| -> (u16, Option<Value>, Duration) {
        (200, Some(json!({ "result": result })), Duration::ZERO)
    };
    match body["method"].as_str().unwrap_or_default() {
        "eth_getCode" => ok(json!("0x")),
        "eth_call" => {
            // getNonce(sender, key): sequence 0 under the requested key.
            let data = hex_bytes(&params[0]["data"]);
            let key: U256 = U256::from_be_slice(&data[36..68]);
            let nonce: U256 = key << 64;
            ok(json!(format!(
                "0x{}",
                hex::encode(nonce.to_be_bytes::<32>())
            )))
        }
        "pimlico_getUserOperationGasPrice" => {
            let tier = json!({"maxFeePerGas": "0x3b9aca00", "maxPriorityFeePerGas": "0x3b9aca00"});
            ok(json!({"slow": tier, "standard": tier, "fast": tier}))
        }
        "pm_sponsorUserOperation" => ok(json!({
            "callGasLimit": "0x186a0",
            "verificationGasLimit": "0x30d40",
            "preVerificationGas": "0xc350",
            "paymasterVerificationGasLimit": "0xea60",
            "paymasterPostOpGasLimit": "0x1",
            "paymaster": "0x3333333333333333333333333333333333333333",
            "paymasterData": "0x1234",
        })),
        "eth_sendUserOperation" => {
            if let Some(error) = &state.send_error {
                return (200, Some(json!({ "error": error })), Duration::ZERO);
            }
            let hash = mock_user_operation_hash(&params[0], params[1].as_str().unwrap(), chain_id);
            state.operations.push(hash.clone());
            if let Some(status) = state.send_status {
                return (status, None, Duration::ZERO);
            }
            if let Some(error) = &state.stored_send_error {
                return (200, Some(json!({ "error": error })), Duration::ZERO);
            }
            if let Some(reply) = &state.send_reply {
                return (200, Some(json!({ "result": reply })), Duration::ZERO);
            }
            (200, Some(json!({ "result": hash })), state.send_delay)
        }
        "eth_getUserOperationReceipt" => {
            if state.receipt_503s > 0 {
                state.receipt_503s -= 1;
                return (503, None, Duration::ZERO);
            }
            let hash = params[0].as_str().unwrap_or_default();
            let result = if !state.receipt_missing
                && state.operations.iter().any(|held| held == hash)
            {
                json!({
                    "userOpHash": hash,
                    "success": true,
                    "receipt": {"transactionHash": format!("0x{}", "ab".repeat(32)), "status": "0x1"},
                })
            } else {
                Value::Null
            };
            (200, Some(json!({ "result": result })), state.receipt_delay)
        }
        "pimlico_getUserOperationStatus" => (
            200,
            Some(json!({ "result": {"status": "not_found"} })),
            state.status_delay,
        ),
        method => (
            200,
            Some(
                json!({"error": {"code": -32601, "message": format!("method {method} not found")}}),
            ),
            Duration::ZERO,
        ),
    }
}

/// The error names the one stored operation's hash and reads as outcome unknown.
fn assert_unknown_with_hash(error: &Error, bundler: &Bundler) -> String {
    let held = bundler.held();
    assert_eq!(held.len(), 1, "bundler holds {held:?}");
    assert!(
        error.to_string().contains(&held[0]),
        "error does not name the hash: {error}"
    );
    assert!(error.mutation_outcome_unknown(), "{error:?}");
    assert!(!error.is_retryable(), "{error:?}");
    assert_eq!(error.user_operation_hash(), Some(held[0].as_str()));
    held[0].clone()
}

/// The documented recovery: wait on the hash; never sign a second operation.
async fn recover_by_waiting(account: &PolyesterSmartAccount, bundler: &Bundler, hash: &str) {
    let receipt = account
        .wait_for_receipt(hash, Duration::from_secs(3), Duration::from_millis(50))
        .await
        .expect("wait_for_receipt");
    assert_eq!(receipt.user_operation_hash, hash);
    assert!(receipt.success);
    assert_eq!(bundler.held(), vec![hash.to_string()]);
}

async fn send(account: &PolyesterSmartAccount, receipt_timeout: Duration) -> Result<String, Error> {
    match account
        .send_calls(&[withdraw()], true, receipt_timeout)
        .await?
    {
        SendCallsResult::Receipt(receipt) => {
            assert!(receipt.success);
            Ok(receipt.user_operation_hash)
        }
        SendCallsResult::Hash(hash) => panic!("wait returned a bare hash {hash}"),
    }
}

#[tokio::test]
async fn mode_a_receipt_poll_503_is_retried_and_returns_the_receipt() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.receipt_503s = 1);
    let account = bundler.account(Duration::from_secs(2));
    let hash = send(&account, Duration::from_secs(10))
        .await
        .expect("send_calls");
    assert_eq!(bundler.held(), vec![hash]);
}

#[tokio::test]
async fn mode_a_receipt_polls_failing_past_the_deadline_keep_the_hash() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.receipt_503s = usize::MAX);
    let account = bundler.account(Duration::from_secs(2));
    let started = Instant::now();
    let error = send(&account, Duration::from_millis(1500))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(10));
    let hash = assert_unknown_with_hash(&error, &bundler);
    bundler.set(|s| s.receipt_503s = 0);
    recover_by_waiting(&account, &bundler, &hash).await;
}

#[tokio::test]
async fn mode_b_send_answer_after_client_timeout_keeps_the_hash() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.send_delay = Duration::from_millis(1500));
    let account = bundler.account(Duration::from_millis(500));
    let error = send(&account, Duration::from_secs(3)).await.unwrap_err();
    let hash = assert_unknown_with_hash(&error, &bundler);
    bundler.set(|s| s.send_delay = Duration::ZERO);
    recover_by_waiting(&account, &bundler, &hash).await;
}

#[tokio::test]
async fn mode_c_send_answered_504_keeps_the_hash() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.send_status = Some(504));
    let account = bundler.account(Duration::from_secs(2));
    let error = send(&account, Duration::from_secs(3)).await.unwrap_err();
    let hash = assert_unknown_with_hash(&error, &bundler);
    assert_eq!(error.answer().map(|answer| answer.status), Some(504));
    recover_by_waiting(&account, &bundler, &hash).await;
}

#[tokio::test]
async fn without_wait_returns_the_hash_the_bundler_holds() {
    let bundler = Bundler::spawn().await;
    let account = bundler.account(Duration::from_secs(2));
    match account
        .send_calls(&[withdraw()], false, Duration::ZERO)
        .await
        .expect("send_calls")
    {
        SendCallsResult::Hash(hash) => assert_eq!(bundler.held(), vec![hash]),
        SendCallsResult::Receipt(receipt) => panic!("no wait returned a receipt {receipt:?}"),
    }
}

#[tokio::test]
async fn definite_refusal_of_the_send_is_not_outcome_unknown() {
    for code in [-32602i64, -32500, -32507, -32601] {
        let bundler = Bundler::spawn().await;
        bundler.set(|s| {
            s.send_error = Some(json!({"code": code, "message": "invalid UserOperation"}))
        });
        let account = bundler.account(Duration::from_secs(2));
        let error = send(&account, Duration::from_secs(3)).await.unwrap_err();
        assert!(
            matches!(&error, Error::JsonRpc { code: c, .. } if *c == code),
            "{error:?}"
        );
        assert!(!error.mutation_outcome_unknown(), "{error:?}");
        assert_eq!(error.user_operation_hash(), None);
        assert!(bundler.held().is_empty());
    }
}

#[tokio::test]
async fn internal_error_answer_to_the_send_keeps_the_hash() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.send_error = Some(json!({"code": -32603, "message": "internal error"})));
    let account = bundler.account(Duration::from_secs(2));
    let error = send(&account, Duration::from_secs(3)).await.unwrap_err();
    assert!(error.mutation_outcome_unknown(), "{error:?}");
    assert!(
        error
            .user_operation_hash()
            .is_some_and(|hash| hash.starts_with("0x")),
        "{error:?}"
    );
    let Error::UserOperationOutcomeUnknown { source, .. } = &error else {
        panic!("{error:?}");
    };
    assert!(
        matches!(**source, Error::JsonRpc { code: -32603, .. }),
        "{source:?}"
    );
}

#[tokio::test]
async fn unrecognized_error_code_after_storing_keeps_the_hash() {
    // An HTTP 200 JSON-RPC error whose code is not a defined refusal proves
    // nothing: here the bundler stored the operation before answering it.
    for code in [504i64, 1, -32000, -32099, -32506, -32603] {
        let bundler = Bundler::spawn().await;
        bundler.set(|s| {
            s.stored_send_error = Some(json!({"code": code, "message": "gateway timeout"}))
        });
        let account = bundler.account(Duration::from_secs(2));
        let error = send(&account, Duration::from_secs(3)).await.unwrap_err();
        let hash = assert_unknown_with_hash(&error, &bundler);
        let Error::UserOperationOutcomeUnknown { source, .. } = &error else {
            panic!("{error:?}");
        };
        assert!(
            matches!(**source, Error::JsonRpc { code: c, .. } if c == code),
            "{source:?}"
        );
        assert!(source.mutation_outcome_unknown(), "{source:?}");
        recover_by_waiting(&account, &bundler, &hash).await;
    }
}

#[tokio::test]
async fn malformed_error_object_with_a_refusal_code_keeps_the_hash() {
    // A refusal code proves a refusal only in a well-formed error object: an
    // integer code and a string message. Here the bundler stored the operation.
    let errors = [
        json!({"code": -32602}),
        json!({"code": -32602, "message": null}),
        json!({"code": -32500, "message": 7}),
        json!({"code": "-32602", "message": "invalid UserOperation"}),
        json!({"code": -32602.0, "message": "invalid UserOperation"}),
        json!({"code": true, "message": "invalid UserOperation"}),
    ];
    for error in errors {
        let bundler = Bundler::spawn().await;
        bundler.set(|s| s.stored_send_error = Some(error.clone()));
        let account = bundler.account(Duration::from_secs(2));
        let outcome = send(&account, Duration::from_secs(3)).await.unwrap_err();
        let hash = assert_unknown_with_hash(&outcome, &bundler);
        let Error::UserOperationOutcomeUnknown { source, .. } = &outcome else {
            panic!("{outcome:?}");
        };
        assert!(
            matches!(**source, Error::Transport { .. }),
            "{error}: {source:?}"
        );
        recover_by_waiting(&account, &bundler, &hash).await;
    }
}

#[tokio::test]
async fn answer_naming_another_hash_keeps_the_computed_hash() {
    let replies = [
        json!(format!("0x{}", "cd".repeat(32))),
        json!(""),
        json!(7),
        Value::Null,
    ];
    for reply in replies {
        for wait in [true, false] {
            let bundler = Bundler::spawn().await;
            bundler.set(|s| s.send_reply = Some(reply.clone()));
            let account = bundler.account(Duration::from_secs(2));
            let error = account
                .send_calls(&[withdraw()], wait, Duration::from_secs(3))
                .await
                .unwrap_err();
            let hash = assert_unknown_with_hash(&error, &bundler);
            assert_ne!(reply.as_str(), Some(hash.as_str()));
            recover_by_waiting(&account, &bundler, &hash).await;
        }
    }
}

#[tokio::test]
async fn receipt_answered_after_the_deadline_is_not_returned() {
    let bundler = Bundler::spawn().await;
    bundler.set(|s| s.receipt_delay = Duration::from_millis(1500));
    let account = bundler.account(Duration::from_secs(5));
    let started = Instant::now();
    let error = send(&account, Duration::from_millis(400))
        .await
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_millis(1200),
        "send_calls took {:?} on a 400ms receipt budget",
        started.elapsed()
    );
    let hash = assert_unknown_with_hash(&error, &bundler);
    bundler.set(|s| s.receipt_delay = Duration::ZERO);
    recover_by_waiting(&account, &bundler, &hash).await;
}

#[tokio::test]
async fn stalled_status_read_ends_with_the_wait() {
    let bundler = Bundler::spawn().await;
    let account = bundler.account(Duration::from_secs(2));
    let SendCallsResult::Hash(hash) = account
        .send_calls(&[withdraw()], false, Duration::ZERO)
        .await
        .expect("send_calls")
    else {
        panic!("no wait returned a receipt");
    };
    bundler.set(|s| {
        s.receipt_missing = true;
        s.status_delay = Duration::from_secs(3);
    });
    let account = bundler.account(Duration::from_secs(5));
    let started = Instant::now();
    account
        .wait_for_receipt(&hash, Duration::from_millis(400), Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_millis(1200),
        "wait_for_receipt took {:?} on a 400ms budget",
        started.elapsed()
    );
    bundler.set(|s| {
        s.receipt_missing = false;
        s.status_delay = Duration::ZERO;
    });
    recover_by_waiting(&account, &bundler, &hash).await;
}

#[test]
fn json_rpc_error_answers_are_unknown_unless_their_code_proves_a_refusal() {
    let answer = |code: i64| Error::JsonRpc {
        method: "eth_sendUserOperation".into(),
        code,
        message: "refused".into(),
    };
    for code in [
        -32700, -32600, -32601, -32602, -32500, -32505, -32507, -32508,
    ] {
        assert!(!answer(code).mutation_outcome_unknown(), "{code}");
    }
    for code in [504, 0, -32506, -32509, -32603, -32000, -32099] {
        assert!(answer(code).mutation_outcome_unknown(), "{code}");
        assert!(!answer(code).is_retryable(), "{code}");
    }
}
