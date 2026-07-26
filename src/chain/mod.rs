//! On-chain Funding and smart-account helpers.
//!
//! Encodes FundingAccount / TradingGateway calldata and can submit wallet-signed
//! UserOperations (owner key → derive Polyester Safe → bundler). Separate from
//! the API-key Connect surface.

mod calldata;
mod destination;
mod environment;
mod fees;
mod rpc;
mod safe;
mod userop;

pub use calldata::{
    ChainCall, GuardApproval, encode_add_allowed_external_destinations,
    encode_add_allowed_internal_accounts, encode_funding_withdraw_to_chain,
    encode_initialize_guard_signer, encode_remove_allowed_external_destinations,
    encode_remove_allowed_internal_accounts, encode_rotate_guard_signer,
    encode_set_external_destination_allowlist_required,
    encode_set_internal_account_allowlist_required, encode_trading_gateway_deposit,
    encode_trading_gateway_deposit_to,
};
pub use destination::encode_withdraw_destination;
pub use environment::{
    AccountAbstractionEnvironment, ContractsEnvironment, EntryPointConfig,
    POLYESTER_TESTNET_ENVIRONMENT, PolyesterChainEnvironment, SafeDeploymentConfig,
};
pub use fees::{ZipperFeeQuote, quote_zipper_fee};
pub use rpc::JsonRpcClient;
pub use safe::{
    PredictedSafe, predict_polyester_smart_account_address, predict_safe_address,
    predict_safe_address_with_data,
};
pub use userop::{
    PolyesterSmartAccount, SendCallsResult, SmartAccount, USER_OPERATION_GAS_BUFFER_BPS,
    USER_OPERATION_MIN_GAS_BUFFER, UserOperationReceipt, add_user_operation_gas_buffer,
    encode_execute_user_op_call_data, pack_paymaster_and_data, sign_safe_user_operation,
    stub_signature,
};
