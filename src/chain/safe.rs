//! CREATE2 Polyester Safe address prediction (permissionless / TS parity).

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, sol};
use std::sync::LazyLock;

use crate::chain::environment::{
    POLYESTER_TESTNET_ENVIRONMENT, PolyesterChainEnvironment, SafeDeploymentConfig,
};
use crate::errors::{Error, Result};

// SafeProxy creation bytecode from @safe-global/safe-contracts v1.4.1
// (must match SafeProxyFactory.proxyCreationCode on Polyester).
static SAFE_PROXY_CREATION_CODE: LazyLock<Vec<u8>> = LazyLock::new(|| {
    hex::decode(concat!(
        "608060405234801561001057600080fd5b506040516101e63803806101e68339818101604052602081101561003357600080fd5b",
        "8101908080519060200190929190505050600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffff",
        "ffffffffffffffffffffffff1614156100ca576040517f08c379a0000000000000000000000000000000000000000000000000",
        "0000000081526004018080602001828103825260228152602001806101c46022913960400191505060405180910390fd5b8060",
        "00806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffff",
        "ffffffffff1602179055505060ab806101196000396000f3fe608060405273ffffffffffffffffffffffffffffffffffffffff",
        "600054167fa619486e000000000000000000000000000000000000000000000000000000006000351415605057806000526020",
        "6000f35b3660008037600080366000845af43d6000803e60008114156070573d6000fd5b3d6000f3fea2646970667358221220",
        "03d1488ee65e08fa41e58e888a9865554c535f2c77126a82cb4c0f917f31441364736f6c63430007060033496e76616c696420",
        "73696e676c65746f6e20616464726573732070726f7669646564",
    ))
    .expect("SAFE_PROXY_CREATION_CODE hex")
});

sol! {
    function setup(
        address[] owners,
        uint256 threshold,
        address to,
        bytes data,
        address fallbackHandler,
        address paymentToken,
        uint256 payment,
        address paymentReceiver
    );
    function enableModules(address[] modules);
    function multiSend(bytes transactions);
    function createProxyWithNonce(address singleton, bytes initializer, uint256 saltNonce);
}

/// Predicted Safe address plus factory deploy payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictedSafe {
    pub address: String,
    pub initializer: Vec<u8>,
    pub factory_calldata: Vec<u8>,
}

fn parse_address(value: &str) -> Result<Address> {
    let text = value.trim();
    let hex = text.strip_prefix("0x").unwrap_or(text);
    if hex.len() != 40 {
        return Err(Error::validation(
            "address must be a 20-byte 0x-prefixed hex string",
        ));
    }
    let raw = hex::decode(hex).map_err(|_| Error::validation("address is not valid hex"))?;
    Ok(Address::from_slice(&raw))
}

fn encode_internal_tx(to: Address, data: &[u8], value: U256, operation: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 20 + 32 + 32 + data.len());
    out.push(operation);
    out.extend_from_slice(to.as_slice());
    out.extend_from_slice(&value.to_be_bytes::<32>());
    out.extend_from_slice(&U256::from(data.len()).to_be_bytes::<32>());
    out.extend_from_slice(data);
    out
}

fn get_initializer(
    owners: &[Address],
    threshold: u64,
    safe: &SafeDeploymentConfig,
) -> Result<Vec<u8>> {
    let module = parse_address(safe.safe_4337_module_address)?;
    let setup_addr = parse_address(safe.safe_module_setup_address)?;
    let multi_send = parse_address(safe.multi_send_address)?;

    let enable_modules = enableModulesCall {
        modules: vec![module],
    }
    .abi_encode();

    let multi_calls = [encode_internal_tx(
        setup_addr,
        &enable_modules,
        U256::ZERO,
        1,
    )];
    let packed: Vec<u8> = multi_calls.into_iter().flatten().collect();
    let multi_send_calldata = multiSendCall {
        transactions: Bytes::from(packed),
    }
    .abi_encode();

    Ok(setupCall {
        owners: owners.to_vec(),
        threshold: U256::from(threshold),
        to: multi_send,
        data: Bytes::from(multi_send_calldata),
        fallbackHandler: module,
        paymentToken: Address::ZERO,
        payment: U256::ZERO,
        paymentReceiver: Address::ZERO,
    }
    .abi_encode())
}

/// Deterministic CREATE2 Safe address + factory deploy data (zero RPC).
pub fn predict_safe_address_with_data(
    owners: &[&str],
    salt_nonce: u64,
    threshold: Option<u64>,
    safe: Option<&SafeDeploymentConfig>,
    environment: Option<&PolyesterChainEnvironment>,
) -> Result<PredictedSafe> {
    if owners.is_empty() {
        return Err(Error::validation("owners must be non-empty"));
    }
    let env = environment.unwrap_or(&POLYESTER_TESTNET_ENVIRONMENT);
    let cfg = safe.unwrap_or(&env.account_abstraction.safe);
    let owner_addrs: Result<Vec<Address>> = owners.iter().map(|o| parse_address(o)).collect();
    let owner_addrs = owner_addrs?;
    let thresh = threshold.unwrap_or(owner_addrs.len() as u64);
    let initializer = get_initializer(&owner_addrs, thresh, cfg)?;
    let singleton = parse_address(cfg.safe_singleton_address)?;
    let factory = parse_address(cfg.safe_proxy_factory_address)?;

    let factory_calldata = createProxyWithNonceCall {
        singleton,
        initializer: Bytes::copy_from_slice(&initializer),
        saltNonce: U256::from(salt_nonce),
    }
    .abi_encode();

    let mut deployment_code = SAFE_PROXY_CREATION_CODE.clone();
    let mut singleton_word = [0u8; 32];
    singleton_word[12..].copy_from_slice(singleton.as_slice());
    deployment_code.extend_from_slice(&singleton_word);

    let mut salt_material = [0u8; 64];
    salt_material[..32].copy_from_slice(keccak256(&initializer).as_slice());
    salt_material[32..].copy_from_slice(&U256::from(salt_nonce).to_be_bytes::<32>());
    let salt = B256::from(keccak256(salt_material));
    let init_code_hash = B256::from(keccak256(&deployment_code));
    let address = factory.create2(salt, init_code_hash);

    Ok(PredictedSafe {
        address: address.to_checksum(None),
        initializer,
        factory_calldata,
    })
}

/// Predict the Polyester Safe for a single owner (main account = salt 0).
pub fn predict_safe_address(
    owner_address: &str,
    salt_nonce: u64,
    environment: Option<&PolyesterChainEnvironment>,
) -> Result<String> {
    Ok(
        predict_safe_address_with_data(&[owner_address], salt_nonce, None, None, environment)?
            .address,
    )
}

/// Alias matching the TypeScript `predictPolyesterSmartAccountAddress` name.
pub fn predict_polyester_smart_account_address(
    owner_address: &str,
    salt_nonce: u64,
    environment: Option<&PolyesterChainEnvironment>,
) -> Result<String> {
    predict_safe_address(owner_address, salt_nonce, environment)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf";
    const SALT0: &str = "0xA244Ed1dc6B46C75F37E0119054fFa45E76c9B6f";
    const SALT7: &str = "0x4AEdcc90537f9fb3828E6b431E5A16Cdc473D6f0";

    #[test]
    fn predict_safe_address_salt_zero_matches_typescript() {
        assert_eq!(predict_safe_address(OWNER, 0, None).unwrap(), SALT0);
    }

    #[test]
    fn predict_safe_address_salt_seven_matches_typescript() {
        assert_eq!(predict_safe_address(OWNER, 7, None).unwrap(), SALT7);
    }

    #[test]
    fn predict_safe_address_with_data_includes_factory_calldata() {
        let predicted = predict_safe_address_with_data(&[OWNER], 0, None, None, None).unwrap();
        assert_eq!(predicted.address, SALT0);
        assert!(hex::encode(&predicted.initializer).starts_with("b63e800d"));
        assert!(hex::encode(&predicted.factory_calldata).starts_with("1688f0b9"));
    }

    #[test]
    fn proxy_creation_code_length() {
        // 972 hex chars from Safe 1.4.1 proxyCreationCode (Python SAFE_PROXY_CREATION_CODE).
        assert_eq!(SAFE_PROXY_CREATION_CODE.len(), 486);
    }
}
