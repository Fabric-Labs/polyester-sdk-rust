//! Zipper fee quoting for Funding → external withdraws.

use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};
use serde_json::json;

use crate::chain::environment::{POLYESTER_DEVNET_ENVIRONMENT, PolyesterChainEnvironment};
use crate::chain::rpc::JsonRpcClient;
use crate::errors::{Error, Result};

sol! {
    function feeFactory() external view returns (address);
    function decimals() external view returns (uint8);
    function getFee(uint16 chainId, address zToken) external view returns (uint256);
}

/// Result of quoting a Zipper network fee for withdraws.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipperFeeQuote {
    pub fee: U256,
    pub z_token_decimals: u8,
    pub fee_factory: String,
    pub zipper_endpoint: String,
}

/// Quote Zipper network fee via `feeFactory.getFee(uint16,address)`.
///
/// Use the returned `fee` (or a small buffer above it) as `max_fee` for
/// [`crate::chain::encode_funding_withdraw_to_chain`].
pub async fn quote_zipper_fee(
    chain_id: u16,
    z_token: &str,
    zipper_endpoint: &str,
    environment: Option<&PolyesterChainEnvironment>,
    rpc: Option<&JsonRpcClient>,
) -> Result<ZipperFeeQuote> {
    if chain_id == 0 {
        return Err(Error::validation("chain_id must be a uint16 > 0"));
    }
    let token = normalize_address(z_token, "z_token")?;
    let endpoint = normalize_address(zipper_endpoint, "zipper_endpoint")?;

    let env = environment.unwrap_or(&POLYESTER_DEVNET_ENVIRONMENT);
    let owned_client;
    let client = match rpc {
        Some(c) => c,
        None => {
            owned_client =
                JsonRpcClient::new(env.rpc_url.as_str(), std::time::Duration::from_secs(60))?;
            &owned_client
        }
    };

    let ff_raw = eth_call(client, &endpoint, feeFactoryCall {}.abi_encode()).await?;
    let fee_factory = address_from_eth_call_result(&ff_raw)?;

    let decimals_raw = eth_call(client, &token, decimalsCall {}.abi_encode()).await?;
    let decimals = u256_from_eth_call_result(&decimals_raw)?;
    let z_token_decimals = u8::try_from(decimals)
        .map_err(|_| Error::validation(format!("decimals out of range: {decimals}")))?;

    let token_addr: Address = token
        .parse()
        .map_err(|_| Error::validation("z_token is not a valid hex address"))?;
    let fee_raw = eth_call(
        client,
        &fee_factory,
        getFeeCall {
            chainId: chain_id,
            zToken: token_addr,
        }
        .abi_encode(),
    )
    .await?;
    let fee = u256_from_eth_call_result(&fee_raw)?;

    Ok(ZipperFeeQuote {
        fee,
        z_token_decimals,
        fee_factory,
        zipper_endpoint: endpoint,
    })
}

async fn eth_call(client: &JsonRpcClient, to: &str, data: Vec<u8>) -> Result<String> {
    let result = client
        .request(
            "eth_call",
            json!([
                {
                    "to": to,
                    "data": format!("0x{}", hex::encode(data)),
                },
                "latest"
            ]),
        )
        .await?;
    result
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::transport("eth_call result is not a hex string"))
}

fn address_from_eth_call_result(raw: &str) -> Result<String> {
    let word = eth_call_word(raw, "address")?;
    if word[..12].iter().any(|byte| *byte != 0) {
        return Err(Error::transport(
            "eth_call address result is not an ABI-encoded address",
        ));
    }
    Ok(format!("0x{}", hex::encode(&word[12..])))
}

fn u256_from_eth_call_result(raw: &str) -> Result<U256> {
    eth_call_word(raw, "uint256").map(U256::from_be_bytes)
}

/// The one 32-byte ABI word that an `eth_call` of a function returning a single
/// static value answers: `0x` and exactly 64 hex digits. Anything else, such as
/// non-ASCII, short, odd-length or non-hex text from the RPC endpoint, is an
/// [`Error::Transport`]; the text is not echoed.
fn eth_call_word(raw: &str, kind: &str) -> Result<[u8; 32]> {
    let malformed = || {
        Error::transport(format!(
            "eth_call {kind} result is not a 0x-prefixed 32-byte hex word ({} bytes of text)",
            raw.len()
        ))
    };
    let hex_body = raw.trim().strip_prefix("0x").ok_or_else(malformed)?;
    let mut word = [0u8; 32];
    hex::decode_to_slice(hex_body, &mut word).map_err(|_| malformed())?;
    Ok(word)
}

fn normalize_address(value: &str, field: &str) -> Result<String> {
    let addr = value.trim();
    if !addr.starts_with("0x") || addr.len() != 42 {
        return Err(Error::validation(format!(
            "{field} must be a 20-byte 0x-prefixed address"
        )));
    }
    if hex::decode(&addr[2..]).is_err() {
        return Err(Error::validation(format!(
            "{field} is not a valid hex address"
        )));
    }
    Ok(addr.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_factory_selector() {
        assert_eq!(
            feeFactoryCall::SELECTOR.as_slice(),
            &alloy_primitives::keccak256(b"feeFactory()")[..4]
        );
    }

    #[test]
    fn get_fee_encode_round_trip() {
        let token: Address = "0x5555555555555555555555555555555555555555"
            .parse()
            .unwrap();
        let data = getFeeCall {
            chainId: 56,
            zToken: token,
        }
        .abi_encode();
        assert_eq!(&data[..4], getFeeCall::SELECTOR.as_slice());
        let decoded = getFeeCall::abi_decode(&data).unwrap();
        assert_eq!(decoded.chainId, 56);
        assert_eq!(decoded.zToken, token);
    }

    #[test]
    fn malformed_eth_call_results_are_errors() {
        let word = format!("0x{}", "0".repeat(64));
        for raw in [
            format!("0xé{}", "a".repeat(39)),
            format!("0x{}é", "0".repeat(62)),
            "0x1234".to_string(),
            format!("0x{}", "0".repeat(63)),
            format!("0x{}0", "0".repeat(64)),
            format!("0x{}zz", "0".repeat(62)),
            "0x".to_string(),
            String::new(),
            "0".repeat(64),
        ] {
            assert!(
                matches!(address_from_eth_call_result(&raw), Err(Error::Transport(_))),
                "address {raw:?}"
            );
            assert!(
                matches!(u256_from_eth_call_result(&raw), Err(Error::Transport(_))),
                "uint256 {raw:?}"
            );
        }
        assert_eq!(u256_from_eth_call_result(&word).unwrap(), U256::ZERO);
        let dirty = format!("0x01{}", "0".repeat(62));
        assert!(address_from_eth_call_result(&dirty).is_err());
    }

    #[test]
    fn address_from_padded_eth_call() {
        let raw = "0x000000000000000000000000ae6b981be9b73421eb1ba5372d1a4a937d63fffb";
        assert_eq!(
            address_from_eth_call_result(raw).unwrap(),
            "0xae6b981be9b73421eb1ba5372d1a4a937d63fffb"
        );
    }
}
