//! `quote_zipper_fee` returns an error, never panics, for a malformed `eth_call`
//! result at any of its three calls.

use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::Error;
use polyester::chain::{JsonRpcClient, quote_zipper_fee};
use serde_json::{Value, json};
use std::time::Duration;

const ENDPOINT: &str = "0x1111111111111111111111111111111111111111";
const TOKEN: &str = "0x2222222222222222222222222222222222222222";
const FEE_FACTORY_WORD: &str = "0x0000000000000000000000003333333333333333333333333333333333333333";
const DECIMALS_WORD: &str = "0x0000000000000000000000000000000000000000000000000000000000000012";
const FEE_WORD: &str = "0x00000000000000000000000000000000000000000000000000000000000003e8";

/// Malformed `eth_call` results: non-ASCII (a multibyte character where the
/// address would be sliced), short, odd-length, non-hex and empty.
fn malformed() -> Vec<String> {
    vec![
        format!("0xé{}", "a".repeat(39)),
        format!("0x{}é", "0".repeat(62)),
        "0xé".into(),
        "0x1234".into(),
        format!("0x{}", "0".repeat(63)),
        format!("0x{}zz", "0".repeat(62)),
        "not hex".into(),
        "0x".into(),
        String::new(),
    ]
}

/// Answers `feeFactory()`, `decimals()` and `getFee(..)` by selector; the call
/// at `bad_call` gets `bad`.
async fn rpc_server(bad_call: usize, bad: String) -> MockHttpServer {
    MockHttpServer::spawn(move |request| {
        let body: Value = serde_json::from_slice(&request.body).expect("json-rpc request");
        let call = match body["params"][0]["to"].as_str().unwrap_or_default() {
            ENDPOINT => 0, // feeFactory()
            TOKEN => 1,    // decimals()
            _ => 2,        // getFee(uint16,address) at the fee factory
        };
        let result = if call == bad_call {
            bad.clone()
        } else {
            [FEE_FACTORY_WORD, DECIMALS_WORD, FEE_WORD][call].to_string()
        };
        HttpScript::Json {
            status: 200,
            body: serde_json::to_vec(
                &json!({"jsonrpc": "2.0", "id": body["id"], "result": result}),
            )
            .unwrap(),
        }
    })
    .await
}

#[tokio::test]
async fn quote_zipper_fee_decodes_well_formed_results() {
    let http = rpc_server(usize::MAX, String::new()).await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(5)).expect("rpc");
    let quote = quote_zipper_fee(56, TOKEN, ENDPOINT, None, Some(&rpc))
        .await
        .expect("quote");
    assert_eq!(
        quote.fee_factory,
        "0x3333333333333333333333333333333333333333"
    );
    assert_eq!(quote.z_token_decimals, 18);
    assert_eq!(quote.fee, alloy_primitives::U256::from(1000u64));
}

#[tokio::test]
async fn quote_zipper_fee_refuses_malformed_results_without_panicking() {
    for bad_call in 0..3 {
        for bad in malformed() {
            let http = rpc_server(bad_call, bad.clone()).await;
            let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(5)).expect("rpc");
            let outcome = tokio::spawn(async move {
                quote_zipper_fee(56, TOKEN, ENDPOINT, None, Some(&rpc)).await
            })
            .await;
            let result = outcome
                .unwrap_or_else(|panic| panic!("call {bad_call} result {bad:?} panicked: {panic}"));
            match result {
                Err(Error::Transport { message, .. }) => {
                    assert!(message.contains("eth_call"), "{message}")
                }
                other => panic!("call {bad_call} result {bad:?}: {other:?}"),
            }
        }
    }
}
