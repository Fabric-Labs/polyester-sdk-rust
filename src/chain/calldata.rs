//! ABI calldata encoders matching TypeScript polyester-features chain-actions.

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::{SolCall, sol};

use crate::errors::{Error, Result};

sol! {
    #[derive(Debug, PartialEq, Eq)]
    struct WithdrawRequest {
        uint16 chainId;
        address zToken;
        bytes withdrawDestination;
        uint256 zAmount;
        uint256 maxFee;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct GuardApprovalTuple {
        uint192 nonceSpace;
        uint256 deadline;
        bytes signature;
    }

    function deposit(bytes32 uAssetId, uint256 uAmount);
    function depositTo(address toAccount, bytes32 uAssetId, uint256 uAmount);
    function withdrawToChain(WithdrawRequest request);
    function setExternalDestinationAllowlistRequired(bool required, GuardApprovalTuple guardSigIfFalse);
    function setInternalAccountAllowlistRequired(bool required, GuardApprovalTuple guardSigIfFalse);
    function addAllowedExternalDestinations(
        uint16 chainId,
        bytes[] destinations,
        GuardApprovalTuple approval
    );
    function removeAllowedExternalDestinations(
        uint16 chainId,
        bytes[] destinations,
        GuardApprovalTuple approval
    );
    function addAllowedInternalAccounts(address[] accounts, GuardApprovalTuple approval);
    function removeAllowedInternalAccounts(address[] accounts, GuardApprovalTuple approval);
    function initializeSigner(address signer);
    function rotateSigner(address newSigner, GuardApprovalTuple approval);
}

/// Contract call payload for a smart-account UserOperation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainCall {
    pub to: String,
    pub data: Vec<u8>,
    pub value: u128,
}

/// Guard approval tuple `(uint192 nonceSpace, uint256 deadline, bytes signature)`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GuardApproval {
    pub nonce_space: U256,
    pub deadline: U256,
    pub signature: Vec<u8>,
}

/// Encode `TradingGateway.deposit(bytes32 uAssetId, uint256 uAmount)`.
pub fn encode_trading_gateway_deposit(
    trading_gateway: &str,
    u_asset_id: &str,
    quantity_scaled: U256,
) -> Result<ChainCall> {
    if quantity_scaled.is_zero() {
        return Err(Error::validation("quantity_scaled must be > 0"));
    }
    let to = normalize_address(trading_gateway, "trading_gateway")?;
    let asset = normalize_bytes32(u_asset_id, "u_asset_id")?;
    let data = depositCall {
        uAssetId: asset,
        uAmount: quantity_scaled,
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `TradingGateway.depositTo(address,bytes32,uint256)`.
pub fn encode_trading_gateway_deposit_to(
    trading_gateway: &str,
    to_account: &str,
    u_asset_id: &str,
    quantity_scaled: U256,
) -> Result<ChainCall> {
    if quantity_scaled.is_zero() {
        return Err(Error::validation("quantity_scaled must be > 0"));
    }
    let to = normalize_address(trading_gateway, "trading_gateway")?;
    let account = parse_address(to_account, "to_account")?;
    let asset = normalize_bytes32(u_asset_id, "u_asset_id")?;
    let data = depositToCall {
        toAccount: account,
        uAssetId: asset,
        uAmount: quantity_scaled,
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `FundingAccount.withdrawToChain((uint16,address,bytes,uint256,uint256))`.
pub fn encode_funding_withdraw_to_chain(
    funding_account: &str,
    chain_id: u16,
    z_token: &str,
    withdraw_destination: &[u8],
    z_amount: U256,
    max_fee: U256,
) -> Result<ChainCall> {
    if chain_id == 0 {
        return Err(Error::validation("chain_id must be a uint16 > 0"));
    }
    if z_amount.is_zero() {
        return Err(Error::validation("z_amount must be > 0"));
    }
    if z_amount <= max_fee {
        return Err(Error::validation("z_amount must be greater than max_fee"));
    }
    if withdraw_destination.is_empty() {
        return Err(Error::validation("withdraw_destination must not be empty"));
    }

    let to = normalize_address(funding_account, "funding_account")?;
    let token = parse_address(z_token, "z_token")?;
    let data = withdrawToChainCall {
        request: WithdrawRequest {
            chainId: chain_id,
            zToken: token,
            withdrawDestination: Bytes::copy_from_slice(withdraw_destination),
            zAmount: z_amount,
            maxFee: max_fee,
        },
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

fn resolve_guard_tuple(approval: Option<GuardApproval>) -> GuardApprovalTuple {
    let guard = approval.unwrap_or_default();
    GuardApprovalTuple {
        nonceSpace: alloy_primitives::Uint::<192, 3>::from(guard.nonce_space),
        deadline: guard.deadline,
        signature: Bytes::from(guard.signature),
    }
}

/// Encode `setExternalDestinationAllowlistRequired(bool,(uint192,uint256,bytes))`.
///
/// When `required` is true, `approval` may be `None` (empty guard tuple).
pub fn encode_set_external_destination_allowlist_required(
    funding_account: &str,
    required: bool,
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    let to = normalize_address(funding_account, "funding_account")?;
    let data = setExternalDestinationAllowlistRequiredCall {
        required,
        guardSigIfFalse: resolve_guard_tuple(approval),
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `setInternalAccountAllowlistRequired(bool,(uint192,uint256,bytes))`.
pub fn encode_set_internal_account_allowlist_required(
    funding_account: &str,
    required: bool,
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    let to = normalize_address(funding_account, "funding_account")?;
    let data = setInternalAccountAllowlistRequiredCall {
        required,
        guardSigIfFalse: resolve_guard_tuple(approval),
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `addAllowedExternalDestinations(uint16,bytes[],(uint192,uint256,bytes))`.
pub fn encode_add_allowed_external_destinations(
    funding_account: &str,
    chain_id: u16,
    destinations: &[Vec<u8>],
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    encode_external_destinations(
        funding_account,
        chain_id,
        destinations,
        approval,
        |chain_id, destinations, approval| {
            addAllowedExternalDestinationsCall {
                chainId: chain_id,
                destinations,
                approval,
            }
            .abi_encode()
        },
    )
}

/// Encode `removeAllowedExternalDestinations(uint16,bytes[],(uint192,uint256,bytes))`.
pub fn encode_remove_allowed_external_destinations(
    funding_account: &str,
    chain_id: u16,
    destinations: &[Vec<u8>],
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    encode_external_destinations(
        funding_account,
        chain_id,
        destinations,
        approval,
        |chain_id, destinations, approval| {
            removeAllowedExternalDestinationsCall {
                chainId: chain_id,
                destinations,
                approval,
            }
            .abi_encode()
        },
    )
}

fn encode_external_destinations(
    funding_account: &str,
    chain_id: u16,
    destinations: &[Vec<u8>],
    approval: Option<GuardApproval>,
    pack: impl FnOnce(u16, Vec<Bytes>, GuardApprovalTuple) -> Vec<u8>,
) -> Result<ChainCall> {
    if chain_id == 0 {
        return Err(Error::validation("chain_id must be a uint16 > 0"));
    }
    if destinations.is_empty() {
        return Err(Error::validation("destinations must be non-empty"));
    }
    if destinations.iter().any(|d| d.is_empty()) {
        return Err(Error::validation("destinations entries must not be empty"));
    }
    let to = normalize_address(funding_account, "funding_account")?;
    let dest_bytes: Vec<Bytes> = destinations
        .iter()
        .map(|d| Bytes::copy_from_slice(d))
        .collect();
    let data = pack(chain_id, dest_bytes, resolve_guard_tuple(approval));
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `addAllowedInternalAccounts(address[],(uint192,uint256,bytes))`.
pub fn encode_add_allowed_internal_accounts(
    funding_account: &str,
    accounts: &[&str],
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    encode_internal_accounts(
        funding_account,
        accounts,
        approval,
        |accounts, approval| {
            addAllowedInternalAccountsCall {
                accounts,
                approval,
            }
            .abi_encode()
        },
    )
}

/// Encode `removeAllowedInternalAccounts(address[],(uint192,uint256,bytes))`.
pub fn encode_remove_allowed_internal_accounts(
    funding_account: &str,
    accounts: &[&str],
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    encode_internal_accounts(
        funding_account,
        accounts,
        approval,
        |accounts, approval| {
            removeAllowedInternalAccountsCall {
                accounts,
                approval,
            }
            .abi_encode()
        },
    )
}

fn encode_internal_accounts(
    funding_account: &str,
    accounts: &[&str],
    approval: Option<GuardApproval>,
    pack: impl FnOnce(Vec<Address>, GuardApprovalTuple) -> Vec<u8>,
) -> Result<ChainCall> {
    if accounts.is_empty() {
        return Err(Error::validation("accounts must be non-empty"));
    }
    let to = normalize_address(funding_account, "funding_account")?;
    let addrs = accounts
        .iter()
        .map(|a| parse_address(a, "accounts"))
        .collect::<Result<Vec<_>>>()?;
    let data = pack(addrs, resolve_guard_tuple(approval));
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `GuardRegistry.initializeSigner(address)`.
pub fn encode_initialize_guard_signer(guard_registry: &str, signer: &str) -> Result<ChainCall> {
    let to = normalize_address(guard_registry, "guard_registry")?;
    let signer_addr = parse_address(signer, "signer")?;
    let data = initializeSignerCall {
        signer: signer_addr,
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
}

/// Encode `GuardRegistry.rotateSigner(address,(uint192,uint256,bytes))`.
pub fn encode_rotate_guard_signer(
    guard_registry: &str,
    new_signer: &str,
    approval: Option<GuardApproval>,
) -> Result<ChainCall> {
    let to = normalize_address(guard_registry, "guard_registry")?;
    let signer_addr = parse_address(new_signer, "new_signer")?;
    let data = rotateSignerCall {
        newSigner: signer_addr,
        approval: resolve_guard_tuple(approval),
    }
    .abi_encode();
    Ok(ChainCall {
        to,
        data,
        value: 0,
    })
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

fn parse_address(value: &str, field: &str) -> Result<Address> {
    let normalized = normalize_address(value, field)?;
    normalized
        .parse::<Address>()
        .map_err(|_| Error::validation(format!("{field} is not a valid hex address")))
}

fn normalize_bytes32(value: &str, field: &str) -> Result<B256> {
    let text = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if text.len() != 64 {
        return Err(Error::validation(format!(
            "{field} must be 32 bytes (64 hex chars)"
        )));
    }
    let raw = hex::decode(text)
        .map_err(|_| Error::validation(format!("{field} is not valid hex")))?;
    Ok(B256::from_slice(&raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRADING_GATEWAY: &str = "0x4444444444444444444444444444444444444444";
    const FUNDING_ACCOUNT: &str = "0x1111111111111111111111111111111111111111";
    const INTERNAL_ACCOUNT: &str = "0x3333333333333333333333333333333333333333";
    const U_ASSET_ID: &str =
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const Z_TOKEN: &str = "0x5555555555555555555555555555555555555555";

    #[test]
    fn encode_deposit_selector_and_args() {
        let call = encode_trading_gateway_deposit(
            TRADING_GATEWAY,
            U_ASSET_ID,
            U256::from(1_000_000u64),
        )
        .unwrap();
        assert_eq!(call.to, TRADING_GATEWAY.to_ascii_lowercase());
        assert_eq!(call.value, 0);
        assert_eq!(&call.data[..4], depositCall::SELECTOR.as_slice());
        let decoded = depositCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.uAmount, U256::from(1_000_000u64));
    }

    #[test]
    fn encode_deposit_to_selector() {
        let call = encode_trading_gateway_deposit_to(
            TRADING_GATEWAY,
            INTERNAL_ACCOUNT,
            U_ASSET_ID,
            U256::from(1_000_000u64),
        )
        .unwrap();
        assert_eq!(&call.data[..4], depositToCall::SELECTOR.as_slice());
        let decoded = depositToCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.toAccount, parse_address(INTERNAL_ACCOUNT, "to").unwrap());
    }

    #[test]
    fn encode_withdraw_to_chain_selector_and_tuple() {
        let destination = hex::decode("1234").unwrap();
        let call = encode_funding_withdraw_to_chain(
            FUNDING_ACCOUNT,
            56,
            Z_TOKEN,
            &destination,
            U256::from(2_000_000u64),
            U256::from(1000u64),
        )
        .unwrap();
        assert_eq!(&call.data[..4], withdrawToChainCall::SELECTOR.as_slice());
        let decoded = withdrawToChainCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.request.chainId, 56);
        assert_eq!(
            decoded.request.withdrawDestination.as_ref(),
            destination.as_slice()
        );
        assert_eq!(decoded.request.zAmount, U256::from(2_000_000u64));
        assert_eq!(decoded.request.maxFee, U256::from(1000u64));
    }

    #[test]
    fn encode_allowlist_required() {
        let call =
            encode_set_external_destination_allowlist_required(FUNDING_ACCOUNT, true, None)
                .unwrap();
        assert_eq!(
            &call.data[..4],
            setExternalDestinationAllowlistRequiredCall::SELECTOR.as_slice()
        );
        let decoded =
            setExternalDestinationAllowlistRequiredCall::abi_decode(&call.data).unwrap();
        assert!(decoded.required);
        assert_eq!(decoded.guardSigIfFalse.nonceSpace, alloy_primitives::Uint::ZERO);
        assert_eq!(decoded.guardSigIfFalse.deadline, U256::ZERO);
        assert!(decoded.guardSigIfFalse.signature.is_empty());
    }

    #[test]
    fn encode_internal_account_allowlist_required() {
        let call =
            encode_set_internal_account_allowlist_required(FUNDING_ACCOUNT, true, None).unwrap();
        assert_eq!(
            &call.data[..4],
            setInternalAccountAllowlistRequiredCall::SELECTOR.as_slice()
        );
        let decoded = setInternalAccountAllowlistRequiredCall::abi_decode(&call.data).unwrap();
        assert!(decoded.required);
    }

    #[test]
    fn encode_add_remove_allowed_external_destinations() {
        let destinations = vec![vec![0x12, 0x34], vec![0xab, 0xcd]];
        let approval = GuardApproval {
            nonce_space: U256::from(7u64),
            deadline: U256::from(123u64),
            signature: vec![0xab, 0xcd],
        };
        let add = encode_add_allowed_external_destinations(
            FUNDING_ACCOUNT,
            56,
            &destinations,
            Some(approval),
        )
        .unwrap();
        assert_eq!(
            &add.data[..4],
            addAllowedExternalDestinationsCall::SELECTOR.as_slice()
        );
        let decoded = addAllowedExternalDestinationsCall::abi_decode(&add.data).unwrap();
        assert_eq!(decoded.chainId, 56);
        assert_eq!(decoded.destinations.len(), 2);
        assert_eq!(decoded.destinations[0].as_ref(), &[0x12, 0x34]);
        assert_eq!(decoded.approval.nonceSpace, alloy_primitives::Uint::from(7));

        let remove =
            encode_remove_allowed_external_destinations(FUNDING_ACCOUNT, 56, &destinations, None)
                .unwrap();
        assert_eq!(
            &remove.data[..4],
            removeAllowedExternalDestinationsCall::SELECTOR.as_slice()
        );
    }

    #[test]
    fn encode_add_remove_allowed_internal_accounts() {
        let accounts = [INTERNAL_ACCOUNT, "0x6666666666666666666666666666666666666666"];
        let add =
            encode_add_allowed_internal_accounts(FUNDING_ACCOUNT, &accounts, None).unwrap();
        assert_eq!(
            &add.data[..4],
            addAllowedInternalAccountsCall::SELECTOR.as_slice()
        );
        let decoded = addAllowedInternalAccountsCall::abi_decode(&add.data).unwrap();
        assert_eq!(decoded.accounts.len(), 2);
        assert_eq!(
            decoded.accounts[0],
            parse_address(INTERNAL_ACCOUNT, "to").unwrap()
        );

        let remove =
            encode_remove_allowed_internal_accounts(FUNDING_ACCOUNT, &accounts, None).unwrap();
        assert_eq!(
            &remove.data[..4],
            removeAllowedInternalAccountsCall::SELECTOR.as_slice()
        );
    }

    #[test]
    fn encode_initialize_and_rotate_guard_signer() {
        const GUARD_REGISTRY: &str = "0xd71F60FD6f784Cc0aD8c25441568C48705D95f64";
        const SIGNER: &str = "0x7777777777777777777777777777777777777777";

        let init = encode_initialize_guard_signer(GUARD_REGISTRY, SIGNER).unwrap();
        assert_eq!(init.to, GUARD_REGISTRY.to_ascii_lowercase());
        assert_eq!(&init.data[..4], initializeSignerCall::SELECTOR.as_slice());
        let decoded = initializeSignerCall::abi_decode(&init.data).unwrap();
        assert_eq!(decoded.signer, parse_address(SIGNER, "signer").unwrap());

        let rotate = encode_rotate_guard_signer(
            GUARD_REGISTRY,
            SIGNER,
            Some(GuardApproval {
                nonce_space: U256::from(1u64),
                deadline: U256::from(999u64),
                signature: vec![0x01, 0x02],
            }),
        )
        .unwrap();
        assert_eq!(&rotate.data[..4], rotateSignerCall::SELECTOR.as_slice());
        let decoded = rotateSignerCall::abi_decode(&rotate.data).unwrap();
        assert_eq!(decoded.newSigner, parse_address(SIGNER, "signer").unwrap());
        assert_eq!(decoded.approval.nonceSpace, alloy_primitives::Uint::from(1));
        assert_eq!(decoded.approval.deadline, U256::from(999u64));
    }

    #[test]
    fn deposit_rejects_zero() {
        let err =
            encode_trading_gateway_deposit(TRADING_GATEWAY, U_ASSET_ID, U256::ZERO).unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }

    #[test]
    fn withdraw_rejects_amount_not_greater_than_fee() {
        let err = encode_funding_withdraw_to_chain(
            FUNDING_ACCOUNT,
            1,
            Z_TOKEN,
            &[0x12, 0x34],
            U256::from(100u64),
            U256::from(100u64),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }
}
