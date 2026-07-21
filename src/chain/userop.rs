//! ERC-4337 EntryPoint v0.7 Safe UserOperation helpers (Pimlico-compatible).

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolStruct, eip712_domain, sol};
use k256::ecdsa::SigningKey;
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::chain::calldata::ChainCall;
use crate::chain::environment::{POLYESTER_TESTNET_ENVIRONMENT, PolyesterChainEnvironment};
use crate::chain::rpc::JsonRpcClient;
use crate::chain::safe::predict_safe_address_with_data;
use crate::errors::{Error, Result};

pub const USER_OPERATION_GAS_BUFFER_BPS: u64 = 2_000;
pub const USER_OPERATION_MIN_GAS_BUFFER: u64 = 50_000;

const STUB_ECDSA_SIGNATURE: [u8; 65] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf0,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x7a, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
    0x1c,
];

sol! {
    function executeUserOpWithErrorString(address to, uint256 value, bytes data, uint8 operation);
    function getNonce(address sender, uint192 key);

    #[derive(Debug)]
    struct SafeOp {
        address safe;
        uint256 nonce;
        bytes initCode;
        bytes callData;
        uint128 verificationGasLimit;
        uint128 callGasLimit;
        uint256 preVerificationGas;
        uint128 maxPriorityFeePerGas;
        uint128 maxFeePerGas;
        bytes paymasterAndData;
        uint48 validAfter;
        uint48 validUntil;
        address entryPoint;
    }
}

/// Receipt for a submitted UserOperation.
#[derive(Debug, Clone)]
pub struct UserOperationReceipt {
    pub user_operation_hash: String,
    pub transaction_hash: String,
    pub success: bool,
    pub raw: Value,
}

/// Result of [`PolyesterSmartAccount::send_calls`].
#[derive(Debug, Clone)]
pub enum SendCallsResult {
    Hash(String),
    Receipt(UserOperationReceipt),
}

/// Apply the standard gas buffer (20% or +50k, whichever is larger).
pub fn add_user_operation_gas_buffer(gas: u64) -> u64 {
    let percent = gas.saturating_mul(USER_OPERATION_GAS_BUFFER_BPS) / 10_000;
    gas.saturating_add(percent.max(USER_OPERATION_MIN_GAS_BUFFER))
}

/// Encode Safe4337Module.executeUserOpWithErrorString for a single call.
pub fn encode_execute_user_op_call_data(call: &ChainCall) -> Result<Vec<u8>> {
    let to = parse_address(&call.to)?;
    Ok(executeUserOpWithErrorStringCall {
        to,
        value: U256::from(call.value),
        data: Bytes::copy_from_slice(&call.data),
        operation: 0,
    }
    .abi_encode())
}

/// Pack paymaster fields into EntryPoint v0.7 `paymasterAndData`.
pub fn pack_paymaster_and_data(
    paymaster: Option<&str>,
    paymaster_verification_gas_limit: u64,
    paymaster_post_op_gas_limit: u64,
    paymaster_data: &[u8],
) -> Result<Vec<u8>> {
    let Some(paymaster) = paymaster else {
        return Ok(Vec::new());
    };
    let addr = parse_address(paymaster)?;
    let mut out = Vec::with_capacity(20 + 16 + 16 + paymaster_data.len());
    out.extend_from_slice(addr.as_slice());
    out.extend_from_slice(&u128::from(paymaster_verification_gas_limit).to_be_bytes());
    out.extend_from_slice(&u128::from(paymaster_post_op_gas_limit).to_be_bytes());
    out.extend_from_slice(paymaster_data);
    Ok(out)
}

/// Stub signature used while requesting paymaster sponsorship.
pub fn stub_signature() -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(&STUB_ECDSA_SIGNATURE);
    out
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

fn address_from_signing_key(key: &SigningKey) -> Address {
    let vk = key.verifying_key();
    // SEC1 uncompressed encoding: 0x04 || x || y (65 bytes)
    let encoded = vk.to_encoded_point(false);
    let hash = keccak256(&encoded.as_bytes()[1..]);
    Address::from_slice(&hash[12..])
}

fn parse_private_key(owner_private_key: &str) -> Result<SigningKey> {
    let hex = owner_private_key
        .trim()
        .strip_prefix("0x")
        .unwrap_or(owner_private_key.trim());
    let raw =
        hex::decode(hex).map_err(|_| Error::validation("owner_private_key is not valid hex"))?;
    if raw.len() != 32 {
        return Err(Error::validation("owner_private_key must be 32 bytes"));
    }
    SigningKey::from_slice(&raw)
        .map_err(|e| Error::validation(format!("invalid secp256k1 private key: {e}")))
}

fn hex_int(value: u64) -> String {
    format!("0x{value:x}")
}

fn hex_u256(value: U256) -> String {
    format!("0x{value:x}")
}

fn as_u64(value: &Value) -> Result<u64> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| Error::transport("numeric field exceeds u64")),
        Value::String(s) => {
            let text = s.trim();
            if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
                u64::from_str_radix(hex, 16)
                    .map_err(|e| Error::transport(format!("invalid hex int: {e}")))
            } else {
                text.parse::<u64>()
                    .map_err(|e| Error::transport(format!("invalid int: {e}")))
            }
        }
        _ => Err(Error::transport(format!("cannot convert {value} to int"))),
    }
}

fn encode_hex(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

fn decode_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    if hex.is_empty() {
        return Ok(Vec::new());
    }
    hex::decode(hex).map_err(|e| Error::transport(format!("invalid hex bytes: {e}")))
}

/// EIP-712 SafeOp signature packed as uint48/uint48/bytes (single EOA owner).
pub fn sign_safe_user_operation(
    signing_key: &SigningKey,
    environment: &PolyesterChainEnvironment,
    sender: &str,
    nonce: U256,
    init_code: &[u8],
    call_data: &[u8],
    call_gas_limit: u64,
    verification_gas_limit: u64,
    pre_verification_gas: u64,
    max_fee_per_gas: u64,
    max_priority_fee_per_gas: u64,
    paymaster_and_data: &[u8],
    valid_after: u64,
    valid_until: u64,
) -> Result<Vec<u8>> {
    let module = parse_address(
        environment
            .account_abstraction
            .safe
            .safe_4337_module_address,
    )?;
    let entry_point = parse_address(environment.account_abstraction.entry_point.address)?;
    let safe = parse_address(sender)?;

    if valid_after > (1u64 << 48) - 1 || valid_until > (1u64 << 48) - 1 {
        return Err(Error::validation("uint48 overflow"));
    }

    let message = SafeOp {
        safe,
        nonce,
        initCode: Bytes::copy_from_slice(init_code),
        callData: Bytes::copy_from_slice(call_data),
        verificationGasLimit: u128::from(verification_gas_limit),
        callGasLimit: u128::from(call_gas_limit),
        preVerificationGas: U256::from(pre_verification_gas),
        maxPriorityFeePerGas: u128::from(max_priority_fee_per_gas),
        maxFeePerGas: u128::from(max_fee_per_gas),
        paymasterAndData: Bytes::copy_from_slice(paymaster_and_data),
        validAfter: alloy_primitives::Uint::<48, 1>::from(valid_after),
        validUntil: alloy_primitives::Uint::<48, 1>::from(valid_until),
        entryPoint: entry_point,
    };

    let domain = eip712_domain! {
        chain_id: environment.chain_id,
        verifying_contract: module,
    };
    let hash: B256 = message.eip712_signing_hash(&domain);
    let (sig, recid) = signing_key
        .sign_prehash_recoverable(hash.as_slice())
        .map_err(|e| Error::validation(format!("failed to sign SafeOp: {e}")))?;

    let mut packed = Vec::with_capacity(12 + 65);
    packed.extend_from_slice(&u48_be_bytes(valid_after));
    packed.extend_from_slice(&u48_be_bytes(valid_until));
    packed.extend_from_slice(&sig.to_bytes());
    packed.push(u8::from(recid) + 27);
    Ok(packed)
}

fn u48_be_bytes(value: u64) -> [u8; 6] {
    let full = value.to_be_bytes();
    [full[2], full[3], full[4], full[5], full[6], full[7]]
}

/// Owner-key smart account: derive Safe, build/sign/submit Funding UserOps.
pub struct PolyesterSmartAccount {
    signing_key: SigningKey,
    pub environment: PolyesterChainEnvironment,
    pub salt_nonce: u64,
    pub address: String,
    pub owner_address: String,
    /// Safe `setup` initializer used for CREATE2 prediction / undeployed initCode.
    pub initializer: Vec<u8>,
    factory_calldata: Vec<u8>,
    rpc: JsonRpcClient,
    bundler: JsonRpcClient,
    paymaster: JsonRpcClient,
}

impl PolyesterSmartAccount {
    pub fn new(
        owner_private_key: &str,
        environment: Option<PolyesterChainEnvironment>,
        salt_nonce: u64,
        timeout: Duration,
    ) -> Result<Self> {
        let signing_key = parse_private_key(owner_private_key)?;
        let owner = address_from_signing_key(&signing_key);
        let owner_address = owner.to_checksum(None);
        let environment = environment.unwrap_or_else(|| POLYESTER_TESTNET_ENVIRONMENT.clone());
        let predicted = predict_safe_address_with_data(
            &[&owner_address],
            salt_nonce,
            None,
            None,
            Some(&environment),
        )?;
        let aa = &environment.account_abstraction;
        Ok(Self {
            signing_key,
            rpc: JsonRpcClient::new(environment.rpc_url, timeout),
            bundler: JsonRpcClient::new(aa.bundler_url, timeout),
            paymaster: JsonRpcClient::new(aa.paymaster_url, timeout),
            environment,
            salt_nonce,
            address: predicted.address,
            owner_address,
            initializer: predicted.initializer,
            factory_calldata: predicted.factory_calldata,
        })
    }

    pub async fn is_deployed(&self) -> Result<bool> {
        let code = self
            .rpc
            .request("eth_getCode", json!([self.address, "latest"]))
            .await?;
        let text = code.as_str().unwrap_or("");
        Ok(!matches!(text, "" | "0x" | "0x0"))
    }

    /// Return the next EntryPoint nonce.
    ///
    /// Matches viem/permissionless: when `key` is `None`, use a fresh
    /// timestamp-based nonce key (`Date.now()` millis) so ops are not stuck on
    /// key `0` (Polyester's bundler rejects some key-0 mempool submissions).
    pub async fn get_nonce(&self, key: Option<u128>) -> Result<U256> {
        // u128 always fits in uint192 (2^192-1).
        let nonce_key = match key {
            Some(k) => k,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::transport(format!("system clock error: {e}")))?
                .as_millis(),
        };
        let ep = self.environment.account_abstraction.entry_point.address;
        let data = getNonceCall {
            sender: parse_address(&self.address)?,
            key: alloy_primitives::Uint::<192, 3>::from(nonce_key),
        }
        .abi_encode();
        let result = self
            .rpc
            .request(
                "eth_call",
                json!([{ "to": ep, "data": encode_hex(&data) }, "latest"]),
            )
            .await?;
        let hex = result
            .as_str()
            .ok_or_else(|| Error::transport("eth_call returned non-string"))?;
        let raw = decode_hex_bytes(hex)?;
        if raw.len() > 32 {
            return Err(Error::transport("eth_call nonce overflow"));
        }
        let mut word = [0u8; 32];
        word[32 - raw.len()..].copy_from_slice(&raw);
        Ok(U256::from_be_bytes(word))
    }

    pub async fn send_calls(
        &self,
        calls: &[ChainCall],
        wait: bool,
        receipt_timeout: Duration,
    ) -> Result<SendCallsResult> {
        if calls.is_empty() {
            return Err(Error::validation("at least one call is required"));
        }
        if calls.len() != 1 {
            return Err(Error::validation(
                "multi-call UserOps are not implemented yet; submit one ChainCall at a time",
            ));
        }
        let call_data = encode_execute_user_op_call_data(&calls[0])?;

        let deployed = self.is_deployed().await?;
        let mut factory: Option<String> = None;
        let mut factory_data: Option<Vec<u8>> = None;
        let mut init_code = Vec::new();
        if !deployed {
            let factory_addr = self
                .environment
                .account_abstraction
                .safe
                .safe_proxy_factory_address;
            factory = Some(parse_address(factory_addr)?.to_checksum(None));
            factory_data = Some(self.factory_calldata.clone());
            let mut code = parse_address(factory_addr)?.as_slice().to_vec();
            code.extend_from_slice(&self.factory_calldata);
            init_code = code;
        }

        let nonce = self.get_nonce(None).await?;
        let gas_price = self
            .paymaster
            .request("pimlico_getUserOperationGasPrice", json!([]))
            .await?;
        let fast = gas_price
            .get("fast")
            .ok_or_else(|| Error::transport("gas price missing fast tier"))?;
        let max_fee = as_u64(
            fast.get("maxFeePerGas")
                .ok_or_else(|| Error::transport("missing maxFeePerGas"))?,
        )?;
        let max_prio = as_u64(
            fast.get("maxPriorityFeePerGas")
                .ok_or_else(|| Error::transport("missing maxPriorityFeePerGas"))?,
        )?;

        let mut user_op = json!({
            "sender": self.address,
            "nonce": hex_u256(nonce),
            "callData": encode_hex(&call_data),
            "callGasLimit": hex_int(0),
            "verificationGasLimit": hex_int(0),
            "preVerificationGas": hex_int(0),
            "maxFeePerGas": hex_int(max_fee),
            "maxPriorityFeePerGas": hex_int(max_prio),
            "signature": encode_hex(&stub_signature()),
        });
        if let (Some(f), Some(fd)) = (&factory, &factory_data) {
            user_op["factory"] = json!(f);
            user_op["factoryData"] = json!(encode_hex(fd));
        }

        let entry_point = self.environment.account_abstraction.entry_point.address;

        // Sponsor once for estimates, buffer gas (incl. paymaster), then re-sponsor so
        // paymasterData matches the final limits. Polyester's paymaster often returns
        // paymasterPostOpGasLimit=1; without a floor the bundler accepts then rejects.
        let sponsored = self
            .paymaster
            .request("pm_sponsorUserOperation", json!([user_op, entry_point]))
            .await?;

        let call_gas = add_user_operation_gas_buffer(as_u64(
            sponsored
                .get("callGasLimit")
                .ok_or_else(|| Error::transport("sponsored missing callGasLimit"))?,
        )?);
        let verification_gas = add_user_operation_gas_buffer(as_u64(
            sponsored
                .get("verificationGasLimit")
                .ok_or_else(|| Error::transport("sponsored missing verificationGasLimit"))?,
        )?);
        let pre_verification = add_user_operation_gas_buffer(as_u64(
            sponsored
                .get("preVerificationGas")
                .ok_or_else(|| Error::transport("sponsored missing preVerificationGas"))?,
        )?);
        let pm_ver = add_user_operation_gas_buffer(
            sponsored
                .get("paymasterVerificationGasLimit")
                .map(as_u64)
                .transpose()?
                .unwrap_or(0),
        )
        .max(USER_OPERATION_MIN_GAS_BUFFER);
        let pm_post = add_user_operation_gas_buffer(
            sponsored
                .get("paymasterPostOpGasLimit")
                .map(as_u64)
                .transpose()?
                .unwrap_or(0),
        )
        .max(USER_OPERATION_MIN_GAS_BUFFER * 2);

        let mut buffered_op = user_op.clone();
        buffered_op["callGasLimit"] = json!(hex_int(call_gas));
        buffered_op["verificationGasLimit"] = json!(hex_int(verification_gas));
        buffered_op["preVerificationGas"] = json!(hex_int(pre_verification));
        buffered_op["paymasterVerificationGasLimit"] = json!(hex_int(pm_ver));
        buffered_op["paymasterPostOpGasLimit"] = json!(hex_int(pm_post));

        let sponsored = self
            .paymaster
            .request("pm_sponsorUserOperation", json!([buffered_op, entry_point]))
            .await?;

        // Keep the exact buffered limits we asked the paymaster to cover. Taking
        // higher sponsor-returned callGas without re-binding paymasterData causes
        // bundler accept-then-reject.
        let paymaster = sponsored
            .get("paymaster")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let pm_data_hex = sponsored
            .get("paymasterData")
            .and_then(|v| v.as_str())
            .unwrap_or("0x");
        let pm_data_bytes = decode_hex_bytes(pm_data_hex)?;

        let paymaster_and_data =
            pack_paymaster_and_data(paymaster.as_deref(), pm_ver, pm_post, &pm_data_bytes)?;
        let signature = sign_safe_user_operation(
            &self.signing_key,
            &self.environment,
            &self.address,
            nonce,
            &init_code,
            &call_data,
            call_gas,
            verification_gas,
            pre_verification,
            max_fee,
            max_prio,
            &paymaster_and_data,
            0,
            0,
        )?;

        let mut final_op = json!({
            "sender": self.address,
            "nonce": hex_u256(nonce),
            "callData": encode_hex(&call_data),
            "callGasLimit": hex_int(call_gas),
            "verificationGasLimit": hex_int(verification_gas),
            "preVerificationGas": hex_int(pre_verification),
            "maxFeePerGas": hex_int(max_fee),
            "maxPriorityFeePerGas": hex_int(max_prio),
            "signature": encode_hex(&signature),
        });
        if let (Some(f), Some(fd)) = (&factory, &factory_data) {
            final_op["factory"] = json!(f);
            final_op["factoryData"] = json!(encode_hex(fd));
        }
        if let Some(pm) = &paymaster {
            final_op["paymaster"] = json!(parse_address(pm)?.to_checksum(None));
            final_op["paymasterVerificationGasLimit"] = json!(hex_int(pm_ver));
            final_op["paymasterPostOpGasLimit"] = json!(hex_int(pm_post));
            final_op["paymasterData"] = json!(encode_hex(&pm_data_bytes));
        }

        let user_op_hash = self
            .bundler
            .request("eth_sendUserOperation", json!([final_op, entry_point]))
            .await?;
        let hash = user_op_hash
            .as_str()
            .ok_or_else(|| Error::transport("eth_sendUserOperation returned non-string"))?
            .to_string();
        if !wait {
            return Ok(SendCallsResult::Hash(hash));
        }
        Ok(SendCallsResult::Receipt(
            self.wait_for_receipt(&hash, receipt_timeout, Duration::from_secs(1))
                .await?,
        ))
    }

    pub async fn wait_for_receipt(
        &self,
        user_operation_hash: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<UserOperationReceipt> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let raw = self
                .bundler
                .request("eth_getUserOperationReceipt", json!([user_operation_hash]))
                .await?;
            if !raw.is_null() {
                let receipt = raw.get("receipt").cloned().unwrap_or(Value::Null);
                let success = raw
                    .get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or_else(|| {
                        matches!(
                            receipt.get("status"),
                            Some(Value::Number(n)) if n.as_u64() == Some(1)
                        ) || matches!(
                            receipt.get("status").and_then(|v| v.as_str()),
                            Some("0x1" | "0x01")
                        )
                    });
                let tx_hash = receipt
                    .get("transactionHash")
                    .or_else(|| raw.get("transactionHash"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                return Ok(UserOperationReceipt {
                    user_operation_hash: user_operation_hash.to_string(),
                    transaction_hash: tx_hash,
                    success,
                    raw,
                });
            }
            match self
                .bundler
                .request(
                    "pimlico_getUserOperationStatus",
                    json!([user_operation_hash]),
                )
                .await
            {
                Ok(status) => {
                    if status.get("status").and_then(|v| v.as_str()) == Some("rejected") {
                        return Err(Error::transport(format!(
                            "bundler rejected UserOperation {user_operation_hash}: {status}"
                        )));
                    }
                }
                Err(_) => {}
            }
            tokio::time::sleep(poll_interval).await;
        }
        Err(Error::transport(format!(
            "timed out waiting for UserOperation receipt {user_operation_hash}"
        )))
    }
}

/// Alias matching the task naming (`SmartAccount`).
pub type SmartAccount = PolyesterSmartAccount;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_user_op_selector() {
        // executeUserOpWithErrorString
        assert_eq!(
            hex::encode(executeUserOpWithErrorStringCall::SELECTOR),
            "541d63c8"
        );
        let call = ChainCall {
            to: "0x1111111111111111111111111111111111111111".into(),
            data: vec![0xab, 0xcd],
            value: 0,
        };
        let encoded = encode_execute_user_op_call_data(&call).unwrap();
        assert_eq!(
            &encoded[..4],
            executeUserOpWithErrorStringCall::SELECTOR.as_slice()
        );
    }

    #[test]
    fn get_nonce_selector() {
        // getNonce(address,uint192)
        assert_eq!(hex::encode(getNonceCall::SELECTOR), "35567e1a");
    }

    #[test]
    fn gas_buffer_applies_minimum() {
        assert_eq!(add_user_operation_gas_buffer(100), 50_100);
    }

    #[test]
    fn gas_buffer_applies_percent_when_larger() {
        // 20% of 1_000_000 = 200_000 > 50_000
        assert_eq!(add_user_operation_gas_buffer(1_000_000), 1_200_000);
    }

    #[test]
    fn stub_signature_length() {
        assert_eq!(stub_signature().len(), 12 + 65);
    }

    #[test]
    fn smart_account_predicts_safe_from_key_one() {
        // private key 0x01 → owner 0x7E5F4552...
        let account = PolyesterSmartAccount::new(
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            None,
            0,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(
            account.owner_address,
            "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
        );
        assert_eq!(
            account.address,
            "0xA244Ed1dc6B46C75F37E0119054fFa45E76c9B6f"
        );
        assert!(hex::encode(&account.initializer).starts_with("b63e800d"));
    }
}
