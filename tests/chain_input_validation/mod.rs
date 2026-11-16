//! Safe prediction refuses an owner nobody controls, and `get_nonce` refuses an
//! `eth_call` result that is not one 32-byte ABI word, such as `"0x"`.

use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::Error;
use polyester::chain::{
    POLYESTER_DEVNET_ENVIRONMENT, PolyesterSmartAccount, predict_polyester_smart_account_address,
    predict_safe_address, predict_safe_address_with_data,
};
use serde_json::{Value, json};
use std::time::Duration;

const OWNER: &str = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf";
const SALT0: &str = "0xA244Ed1dc6B46C75F37E0119054fFa45E76c9B6f";
/// Private key 1, whose address is `OWNER`.
const OWNER_KEY: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

/// Owners that are not an address someone controls: not 20 bytes of hex, or the
/// zero address, which the Safe contract refuses as an owner.
fn invalid_owners() -> Vec<String> {
    vec![
        String::new(),
        "   ".into(),
        "nothex".into(),
        "0x".into(),
        "0x1234".into(),
        format!("0x{}", "a".repeat(39)),
        format!("0x{}", "a".repeat(41)),
        format!("0x{}zz", "a".repeat(38)),
        format!("0xé{}", "a".repeat(38)),
        format!("0x{}", "0".repeat(40)),
        "0".repeat(40),
    ]
}

fn assert_validation_naming(result: polyester::Result<String>, field: &str, owner: &str) {
    match result {
        Err(Error::Validation(message)) => {
            assert!(message.contains(field), "owner {owner:?}: {message}")
        }
        other => panic!("owner {owner:?}: {other:?}, want Error::Validation naming {field}"),
    }
}

#[test]
fn predict_safe_address_refuses_an_owner_that_is_not_an_address() {
    for owner in invalid_owners() {
        assert_validation_naming(
            predict_safe_address(&owner, 0, None),
            "owner_address",
            &owner,
        );
        assert_validation_naming(
            predict_polyester_smart_account_address(&owner, 0, None),
            "owner_address",
            &owner,
        );
        assert_validation_naming(
            predict_safe_address_with_data(&[OWNER, &owner], 0, None, None, None)
                .map(|predicted| predicted.address),
            "owners[1]",
            &owner,
        );
    }
}

#[test]
fn predict_safe_address_accepts_every_address_spelling() {
    let bare = OWNER.trim_start_matches("0x");
    for owner in [
        OWNER.to_string(),
        OWNER.to_lowercase(),
        format!("0x{}", bare.to_uppercase()),
        bare.to_string(),
        format!(" {OWNER}\n"),
    ] {
        assert_eq!(
            predict_safe_address(&owner, 0, None).unwrap(),
            SALT0,
            "{owner:?}"
        );
    }
}

/// A smart account whose RPC endpoint answers every `eth_call` with `result`.
async fn account_answering(result: Value) -> (MockHttpServer, PolyesterSmartAccount) {
    let http = MockHttpServer::spawn(move |request| {
        let body: Value = serde_json::from_slice(&request.body).expect("json-rpc request");
        HttpScript::Json {
            status: 200,
            body: serde_json::to_vec(
                &json!({"jsonrpc": "2.0", "id": body["id"], "result": result}),
            )
            .unwrap(),
        }
    })
    .await;
    let mut environment = POLYESTER_DEVNET_ENVIRONMENT.clone();
    environment.rpc_url = http.base_url();
    let account =
        PolyesterSmartAccount::new(OWNER_KEY, Some(environment), 0, Duration::from_secs(5))
            .expect("account");
    (http, account)
}

#[tokio::test]
async fn get_nonce_decodes_one_abi_word() {
    for (word, want) in [
        (format!("0x{}", "0".repeat(64)), "0"),
        (format!("0x{}5", "0".repeat(63)), "5"),
        // key 1, sequence 0: the packed nonce (key << 64) | seq.
        (
            format!("0x{}1{}", "0".repeat(47), "0".repeat(16)),
            "18446744073709551616",
        ),
    ] {
        let (_http, account) = account_answering(json!(word)).await;
        let nonce = account.get_nonce(Some(0)).await.expect("nonce");
        assert_eq!(nonce.to_string(), want, "{word}");
    }
}

#[tokio::test]
async fn get_nonce_refuses_a_result_that_is_not_one_abi_word() {
    for bad in [
        "0x".to_string(),
        String::new(),
        "0x0".into(),
        "0x1".into(),
        "0x05".into(),
        format!("0x{}", "0".repeat(63)),
        format!("0x{}", "0".repeat(66)),
        format!("0x{}zz", "0".repeat(62)),
        format!("0x{}é", "0".repeat(62)),
        "0".repeat(64),
        "not hex".into(),
    ] {
        let (_http, account) = account_answering(json!(bad)).await;
        match account.get_nonce(Some(0)).await {
            Err(Error::Transport(message)) => {
                assert!(message.contains("eth_call"), "{bad:?}: {message}")
            }
            other => panic!("result {bad:?}: {other:?}, want Error::Transport"),
        }
    }
}
