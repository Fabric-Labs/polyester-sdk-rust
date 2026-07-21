//! Zipper fee quoting for Funding → external withdraws.

use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};
use serde_json::json;

use crate::chain::environment::{POLYESTER_TESTNET_ENVIRONMENT, PolyesterChainEnvironment};
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

    let env = environment.unwrap_or(&POLYESTER_TESTNET_ENVIRONMENT);
    let owned_client;
    let client = match rpc {
        Some(c) => c,
        None => {
            owned_client = JsonRpcClient::new(env.rpc_url, std::time::Duration::from_secs(60));
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
    let hex_body = raw.trim().trim_start_matches("0x").to_ascii_lowercase();
    if hex_body.len() < 40 {
        return Err(Error::transport(format!(
            "eth_call address result too short: {raw}"
        )));
    }
    Ok(format!("0x{}", &hex_body[hex_body.len() - 40..]))
}

fn u256_from_eth_call_result(raw: &str) -> Result<U256> {
    let hex_body = raw.trim().trim_start_matches("0x");
    U256::from_str_radix(hex_body, 16)
        .map_err(|_| Error::transport(format!("eth_call u256 decode failed: {raw}")))
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
    fn address_from_padded_eth_call() {
        let raw = "0x000000000000000000000000ae6b981be9b73421eb1ba5372d1a4a937d63fffb";
        assert_eq!(
            address_from_eth_call_result(raw).unwrap(),
            "0xae6b981be9b73421eb1ba5372d1a4a937d63fffb"
        );
    }
}
