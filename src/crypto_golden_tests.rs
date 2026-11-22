//! Golden signing vectors: fixed keys and messages, and the exact signatures,
//! recovery ids and addresses the SDK produces for them.
//!
//! secp256k1 signing is RFC 6979 deterministic ECDSA and Ed25519 is
//! deterministic by construction, so a dependency upgrade must reproduce every
//! byte. The expected values live in `testdata/crypto_golden_vectors.json`;
//! a change there is a change to what the SDK signs.
//!
//! Independent anchors: the well-known secp256k1 test keys derive their
//! published addresses, and the first RFC 8032 Ed25519 vector signs the empty
//! message to its published signature.

use std::time::Duration;

use alloy_primitives::U256;
use serde_json::{Value, json};

use crate::auth::Credentials;
use crate::chain::{
    POLYESTER_DEVNET_ENVIRONMENT, POLYESTER_TESTNET_ENVIRONMENT, PolyesterChainEnvironment,
    PolyesterSmartAccount, sign_safe_user_operation,
};

const VECTORS: &str = include_str!("../testdata/crypto_golden_vectors.json");

/// secp256k1 owner keys: the smallest and largest valid scalars, the Hardhat
/// account #0 key and the EIP-155 example key.
const SECP256K1_KEYS: [&str; 4] = [
    "0x0000000000000000000000000000000000000000000000000000000000000001",
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    "0x4646464646464646464646464646464646464646464646464646464646464646",
    "0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364140",
];

/// Ed25519 API-key seeds: the shared public test seed 00..1f, the all-zero
/// seed and the RFC 8032 test 1 seed.
const ED25519_SEEDS: [&str; 3] = [
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    "0000000000000000000000000000000000000000000000000000000000000000",
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
];

struct SafeOpCase {
    name: &'static str,
    environment: &'static str,
    salt_nonce: u64,
    nonce: u128,
    init_code: &'static str,
    call_data: &'static str,
    call_gas_limit: u64,
    verification_gas_limit: u64,
    pre_verification_gas: u64,
    max_fee_per_gas: u64,
    max_priority_fee_per_gas: u64,
    paymaster_and_data: &'static str,
    valid_after: u64,
    valid_until: u64,
}

const SAFE_OP_CASES: [SafeOpCase; 3] = [
    SafeOpCase {
        name: "devnet-empty",
        environment: "devnet",
        salt_nonce: 0,
        nonce: 0,
        init_code: "",
        call_data: "",
        call_gas_limit: 0,
        verification_gas_limit: 0,
        pre_verification_gas: 0,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        paymaster_and_data: "",
        valid_after: 0,
        valid_until: 0,
    },
    SafeOpCase {
        name: "devnet-call",
        environment: "devnet",
        salt_nonce: 7,
        nonce: 3,
        init_code: "",
        call_data: "7bb3742800000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002",
        call_gas_limit: 210_000,
        verification_gas_limit: 450_000,
        pre_verification_gas: 55_000,
        max_fee_per_gas: 2_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        paymaster_and_data: "",
        valid_after: 1_700_000_000,
        valid_until: 1_700_003_600,
    },
    SafeOpCase {
        name: "testnet-deploy-paymaster",
        environment: "testnet",
        salt_nonce: u64::MAX,
        nonce: u128::MAX,
        init_code: "4e1dcf7ad4e460cfd30791ccc4f9c8a4f820ec671688f0b9",
        call_data: "deadbeef",
        call_gas_limit: u64::MAX,
        verification_gas_limit: 1,
        pre_verification_gas: u64::MAX,
        max_fee_per_gas: u64::MAX,
        max_priority_fee_per_gas: 1,
        paymaster_and_data: "0000000000000039cd5e3c2b1d6d2f0ce3a8f0b8000000000000000000000000000186a000000000000000000000000000000000c0ffee",
        valid_after: 1,
        valid_until: (1 << 48) - 1,
    },
];

/// Payloads for [`Credentials::sign_payload`].
const ED25519_PAYLOADS: [&str; 3] = ["", "00", "706f6c7965737465722d676f6c64656e"];

struct RequestCase {
    method: &'static str,
    url: &'static str,
    body: &'static str,
    timestamp_ms: &'static str,
    recv_window_ms: Option<u32>,
}

const REQUEST_CASES: [RequestCase; 3] = [
    RequestCase {
        method: "GET",
        url: "https://api.example.com/v1/orders?symbol=BTC-USDC&limit=10",
        body: "",
        timestamp_ms: "123",
        recv_window_ms: None,
    },
    RequestCase {
        method: "POST",
        url: "https://api.example.com/orders.v1.OrderService/PlaceOrder",
        body: "{\"symbol\":\"ETH-USDC\",\"side\":\"BUY\"}",
        timestamp_ms: "1700000000000",
        recv_window_ms: Some(5_000),
    },
    RequestCase {
        method: "POST",
        url: "https://api.example.com/auth.v1.AuthService/Me",
        body: "\u{0}\u{1}binary",
        timestamp_ms: "1",
        recv_window_ms: None,
    },
];

fn environment(name: &str) -> PolyesterChainEnvironment {
    match name {
        "devnet" => POLYESTER_DEVNET_ENVIRONMENT.clone(),
        "testnet" => POLYESTER_TESTNET_ENVIRONMENT.clone(),
        other => panic!("unknown environment {other}"),
    }
}

fn bytes(hex_text: &str) -> Vec<u8> {
    hex::decode(hex_text).expect("fixture hex")
}

fn secp256k1_vectors() -> Value {
    let mut out = Vec::new();
    for key in SECP256K1_KEYS {
        let signing_key =
            k256::ecdsa::SigningKey::from_slice(&bytes(&key[2..])).expect("fixture key");
        let mut signatures = Vec::new();
        for case in &SAFE_OP_CASES {
            let env = environment(case.environment);
            let account = PolyesterSmartAccount::new(
                key,
                Some(env.clone()),
                case.salt_nonce,
                Duration::from_secs(1),
            )
            .expect("smart account");
            let packed = sign_safe_user_operation(
                &signing_key,
                &env,
                &account.address,
                U256::from(case.nonce),
                &bytes(case.init_code),
                &bytes(case.call_data),
                case.call_gas_limit,
                case.verification_gas_limit,
                case.pre_verification_gas,
                case.max_fee_per_gas,
                case.max_priority_fee_per_gas,
                &bytes(case.paymaster_and_data),
                case.valid_after,
                case.valid_until,
            )
            .expect("sign SafeOp");
            assert_eq!(packed.len(), 12 + 65, "{}", case.name);
            signatures.push(json!({
                "case": case.name,
                "safe": account.address,
                "r": hex::encode(&packed[12..44]),
                "s": hex::encode(&packed[44..76]),
                "v": packed[76],
                "packed": hex::encode(&packed),
            }));
        }
        let owner = PolyesterSmartAccount::new(key, None, 0, Duration::from_secs(1))
            .expect("smart account")
            .owner_address;
        out.push(json!({ "key": key, "owner": owner, "safe_ops": signatures }));
    }
    Value::Array(out)
}

fn ed25519_vectors() -> Value {
    let mut out = Vec::new();
    for seed in ED25519_SEEDS {
        let credentials = Credentials::new("ak_golden", seed).expect("credentials");
        let payloads: Vec<Value> = ED25519_PAYLOADS
            .iter()
            .map(|payload| {
                json!({
                    "payload": payload,
                    "signature": hex::encode(credentials.sign_payload(&bytes(payload))),
                })
            })
            .collect();
        let requests: Vec<Value> = REQUEST_CASES
            .iter()
            .map(|case| {
                let headers = credentials
                    .sign_request_with_recv_window(
                        case.method,
                        case.url,
                        case.body.as_bytes(),
                        Some(case.timestamp_ms),
                        case.recv_window_ms,
                    )
                    .expect("sign request");
                json!({
                    "method": case.method,
                    "url": case.url,
                    "timestamp_ms": case.timestamp_ms,
                    "recv_window_ms": case.recv_window_ms,
                    "headers": headers,
                })
            })
            .collect();
        out.push(json!({ "seed": seed, "payloads": payloads, "requests": requests }));
    }
    Value::Array(out)
}

fn computed() -> Value {
    json!({ "secp256k1": secp256k1_vectors(), "ed25519": ed25519_vectors() })
}

#[test]
fn signing_matches_golden_vectors() {
    let expected: Value = serde_json::from_str(VECTORS).expect("golden vectors JSON");
    let actual = computed();
    assert_eq!(
        actual,
        expected,
        "signing output changed; computed:\n{}",
        serde_json::to_string_pretty(&actual).unwrap()
    );
}

/// The fixtures are only as strong as their coverage: both recovery ids
/// appear, every `s` is low-S normalized, and the independent published
/// anchors hold.
#[test]
fn golden_vectors_cover_both_recovery_ids_and_published_anchors() {
    // secp256k1 group order n, halved (rounded down).
    const HALF_ORDER: &str = "7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0";
    let expected: Value = serde_json::from_str(VECTORS).expect("golden vectors JSON");
    let ops: Vec<&Value> = expected["secp256k1"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|key| key["safe_ops"].as_array().unwrap().iter())
        .collect();
    let vs: Vec<u64> = ops.iter().map(|op| op["v"].as_u64().unwrap()).collect();
    assert!(vs.contains(&27) && vs.contains(&28), "v values: {vs:?}");
    for op in &ops {
        let s = op["s"].as_str().unwrap();
        assert!(s.len() == 64 && s <= HALF_ORDER, "high s: {s}");
    }

    let owner = |key: &str| {
        expected["secp256k1"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["key"] == key)
            .unwrap()["owner"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(
        owner(SECP256K1_KEYS[0]),
        "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
    );
    assert_eq!(
        owner(SECP256K1_KEYS[1]),
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    );
    assert_eq!(
        owner(SECP256K1_KEYS[2]),
        "0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F"
    );

    let rfc8032 = Credentials::new("ak_golden", ED25519_SEEDS[2]).unwrap();
    assert_eq!(
        hex::encode(rfc8032.sign_payload(b"")),
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    );
}

/// k256 0.14 reduces a prehash modulo the group order n before RFC 6979
/// seeds the nonce, as RFC 6979 specifies; k256 0.13 seeded with the
/// unreduced value. So for a prehash >= n (a keccak256 SafeOp hash is one
/// with probability about 2^-128) the deterministic signature differs from
/// earlier releases. Both are valid: each signature here verifies and
/// recovers to the signer's key at the boundary.
#[test]
fn prehashes_at_the_group_order_sign_verifiably() {
    use crate::k256::ecdsa::signature::hazmat::PrehashVerifier;
    use crate::k256::ecdsa::{SigningKey, VerifyingKey};

    const HALF_ORDER: &str = "7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0";
    let signing_key = SigningKey::from_slice(&bytes(&SECP256K1_KEYS[0][2..])).unwrap(); // scalar 1
    let verifying_key = signing_key.verifying_key();
    for (name, prehash) in [
        (
            "n - 1",
            "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364140",
        ),
        (
            "n",
            "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
        ),
        (
            "n + 1",
            "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364142",
        ),
    ] {
        let prehash = bytes(prehash);
        let (signature, recovery_id) = signing_key.sign_prehash_recoverable(&prehash);
        verifying_key
            .verify_prehash(&prehash, &signature)
            .unwrap_or_else(|err| panic!("prehash {name}: {err}"));
        let recovered = VerifyingKey::recover_from_prehash(&prehash, &signature, recovery_id)
            .unwrap_or_else(|err| panic!("prehash {name}: {err}"));
        assert_eq!(&recovered, verifying_key, "prehash {name}");
        let s = hex::encode(&signature.to_bytes()[32..]);
        assert!(s.as_str() <= HALF_ORDER, "prehash {name}: high s {s}");
    }
}
