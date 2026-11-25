//! Deposit / withdraw / internal-transfer decoders.

use super::money::decode_asset_amount_u128;
use crate::codecs::scalars::{LEDGER_SCALE, format_ledger_u128, u128_to_str};
use crate::errors::{Error, Result};
use crate::models::{
    DepositAddress, DepositAddressesList, InternalTransferBatchLegResult,
    InternalTransferBatchResult, InternalTransferResult, ResolvedDestination,
    UnavailableWalletAsset, WalletBalance, WalletBalances, WithdrawDestinationValidation,
    WithdrawIntentResult,
};
use crate::proto::chain::deposit::v1::{
    CreateDepositAddressResponse, DepositAddress as ProtoDepositAddress,
    ListDepositAddressesResponse,
};
use crate::proto::chain::withdraw::v1::{
    CreateTradingWithdrawResponse, CreateWalletTradingWithdrawResponse, GetWalletBalancesResponse,
    ValidateWithdrawDestinationResponse, WalletBalance as ProtoWalletBalance,
    WithdrawDestinationValidationCode,
};
use crate::proto::transfer::v1::{
    AbandonInternalTransferBatchResponse, BatchCreateInternalTransfersResponse,
    CreateInternalTransferResponse, GetInternalTransferBatchResponse,
    InternalTransferBatchLegResult as ProtoInternalTransferBatchLegResult,
    InternalTransferBatchLegStatus, InternalTransferBatchStatus, InternalTransferStatus,
};
use crate::types::QuantityDomain;

fn withdraw_destination_validation_code_label(
    value: &buffa::EnumValue<WithdrawDestinationValidationCode>,
) -> String {
    match value.as_known() {
        Some(WithdrawDestinationValidationCode::RESULT_UNSPECIFIED) => "unspecified".to_owned(),
        Some(WithdrawDestinationValidationCode::VALID) => "valid".to_owned(),
        Some(WithdrawDestinationValidationCode::INVALID_ADDRESS) => "invalid_address".to_owned(),
        Some(WithdrawDestinationValidationCode::UNSUPPORTED_CHAIN) => {
            "unsupported_chain".to_owned()
        }
        Some(WithdrawDestinationValidationCode::POLYESTER_SMART_ACCOUNT) => {
            "polyester_smart_account".to_owned()
        }
        Some(WithdrawDestinationValidationCode::TOKEN_CONTRACT) => "token_contract".to_owned(),
        Some(WithdrawDestinationValidationCode::DENYLISTED_ADDRESS) => {
            "denylisted_address".to_owned()
        }
        Some(WithdrawDestinationValidationCode::SWAP_DEPOSIT_ADDRESS) => {
            "swap_deposit_address".to_owned()
        }
        None => format!("unknown_code_{}", value.to_i32()),
    }
}

pub fn withdraw_destination_validation_from_proto(
    msg: &ValidateWithdrawDestinationResponse,
) -> WithdrawDestinationValidation {
    WithdrawDestinationValidation {
        valid: msg.valid,
        code: withdraw_destination_validation_code_label(&msg.code),
        message: msg.message.clone(),
        canonical_destination_address: msg.canonical_destination_address.clone(),
    }
}

pub fn deposit_address_from_proto(msg: &ProtoDepositAddress) -> DepositAddress {
    DepositAddress {
        chain_id: msg.chain_id,
        deposit_address: msg.deposit_address.clone(),
    }
}

pub fn deposit_addresses_list_from_proto(
    msg: &ListDepositAddressesResponse,
) -> DepositAddressesList {
    DepositAddressesList {
        addresses: msg
            .deposit_addresses
            .iter()
            .map(deposit_address_from_proto)
            .collect(),
    }
}

pub fn create_deposit_address_from_proto(
    msg: &CreateDepositAddressResponse,
) -> Result<DepositAddress> {
    let address = msg.deposit_address.as_option().ok_or_else(|| {
        Error::transport("invalid CreateDepositAddress response: missing deposit_address")
    })?;
    if address.deposit_address.trim().is_empty() {
        return Err(Error::transport(
            "invalid CreateDepositAddress response: empty deposit address",
        ));
    }
    Ok(deposit_address_from_proto(address))
}

pub fn withdraw_intent_from_proto(
    msg: &CreateTradingWithdrawResponse,
) -> Result<WithdrawIntentResult> {
    if msg.intent_id.trim().is_empty() {
        return Err(Error::transport(
            "invalid CreateTradingWithdraw response: missing intent_id",
        ));
    }
    Ok(WithdrawIntentResult {
        intent_id: msg.intent_id.clone(),
        status: String::new(),
        flow_id: String::new(),
    })
}

pub fn withdraw_intent_from_wallet_proto(
    msg: &CreateWalletTradingWithdrawResponse,
) -> Result<WithdrawIntentResult> {
    if msg.intent_id.trim().is_empty() {
        return Err(Error::transport(
            "invalid CreateWalletTradingWithdraw response: missing intent_id",
        ));
    }
    Ok(WithdrawIntentResult {
        intent_id: msg.intent_id.clone(),
        status: String::new(),
        flow_id: String::new(),
    })
}

fn internal_transfer_status_label(status: &buffa::EnumValue<InternalTransferStatus>) -> String {
    match status.as_known() {
        Some(InternalTransferStatus::Accepted) => "accepted".to_owned(),
        Some(InternalTransferStatus::Rejected) => "rejected".to_owned(),
        Some(InternalTransferStatus::Failed) => "failed".to_owned(),
        Some(InternalTransferStatus::Unspecified) => String::new(),
        None => format!("UNKNOWN({})", status.to_i32()),
    }
}

pub fn internal_transfer_from_proto(
    msg: &CreateInternalTransferResponse,
) -> Result<InternalTransferResult> {
    if msg.request_id.trim().is_empty() || msg.transfer_id.trim().is_empty() {
        return Err(Error::transport(
            "invalid CreateInternalTransfer response: missing request_id or transfer_id",
        ));
    }
    let asset_id = msg.asset_id;
    let quantity = msg.amount_e18.as_option().and_then(|u| {
        decode_asset_amount_u128(
            u.hi,
            u.lo,
            Some(LEDGER_SCALE),
            QuantityDomain::LedgerE18,
            Some(asset_id),
        )
    });
    Ok(InternalTransferResult {
        request_id: msg.request_id.clone(),
        transfer_id: msg.transfer_id.clone(),
        asset_id,
        asset_code: msg.asset_code.clone(),
        quantity,
        status: internal_transfer_status_label(&msg.status),
        flow_id: msg.flow_id.clone(),
    })
}

/// A credited leg's resolved destination: its root account as a canonical
/// base58 public ID, its sub-account, when it names one, the same way, and
/// its smart-account address as `0x` and 40 hex digits.
fn credited_destination(
    op: &str,
    index: usize,
    destination: &crate::proto::transfer::v1::ResolvedDestination,
) -> Result<()> {
    let refuse = |what: String| {
        Error::response_contract(op, format!("credited leg {index} destination {what}"))
    };
    let public_id = |field: &str, value: &str| {
        crate::codecs::scalars::id_to_u64(value, field)
            .map(drop)
            .map_err(|_| refuse(format!("has an invalid {field} {value:?}")))
    };
    if destination.root_account_public_id.is_empty() {
        return Err(refuse("has no root_account_public_id".into()));
    }
    public_id(
        "root_account_public_id",
        &destination.root_account_public_id,
    )?;
    if !destination.subaccount_public_id.is_empty() {
        public_id("subaccount_public_id", &destination.subaccount_public_id)?;
    }
    let address = &destination.smart_account_address;
    if address.is_empty() {
        return Err(refuse("has no smart_account_address".into()));
    }
    let hex = address.strip_prefix("0x").unwrap_or_default();
    if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(refuse(format!(
            "has an invalid smart_account_address {address:?}"
        )));
    }
    Ok(())
}

/// Decode a batch of internal transfers sent with `requested_legs` legs.
///
/// Refused with [`Error::ResponseContract`], as [`internal_transfer_batch`]
/// says, and when the number of leg results is not the number of legs
/// requested. The batch may have moved funds: retry it with the same
/// idempotency key, or read it with `GetInternalTransferBatch`, to learn its
/// outcome.
pub fn internal_transfer_batch_from_proto(
    msg: &BatchCreateInternalTransfersResponse,
    requested_legs: usize,
) -> Result<InternalTransferBatchResult> {
    const OP: &str = "BatchCreateInternalTransfers";
    if msg.legs.len() != requested_legs {
        return Err(Error::response_contract(
            OP,
            format!("{} leg results for {requested_legs} legs", msg.legs.len()),
        ));
    }
    internal_transfer_batch(OP, &msg.request_id, &msg.status, &msg.legs, false)
}

/// Decode a batch of internal transfers read back by its request ID.
///
/// Refused with [`Error::ResponseContract`], as [`internal_transfer_batch`]
/// says, and when the reply has no request ID (whatever its status) or no
/// legs: a batch has at least one leg.
pub fn internal_transfer_batch_from_get_response(
    msg: &GetInternalTransferBatchResponse,
) -> Result<InternalTransferBatchResult> {
    const OP: &str = "GetInternalTransferBatch";
    if msg.legs.is_empty() {
        return Err(Error::response_contract(OP, "batch has no legs"));
    }
    internal_transfer_batch(OP, &msg.request_id, &msg.status, &msg.legs, true)
}

/// Decode the answer to abandoning a batch's idempotency key.
///
/// `BATCH_ABANDONED` is status `abandoned` with no request ID and no legs:
/// nothing moved under the key and nothing ever will. Any other answer is the
/// batch submitted under the key, refused with [`Error::ResponseContract`] as
/// [`internal_transfer_batch`] says, and when it has no legs. An abandoned
/// answer with a request ID or legs is refused too. Abandoning again with the
/// same key is safe.
pub fn internal_transfer_batch_from_abandon_response(
    msg: &AbandonInternalTransferBatchResponse,
) -> Result<InternalTransferBatchResult> {
    const OP: &str = "AbandonInternalTransferBatch";
    if msg.status.as_known() == Some(InternalTransferBatchStatus::BATCH_ABANDONED) {
        if !msg.request_id.is_empty() || !msg.legs.is_empty() {
            return Err(Error::response_contract(
                OP,
                format!(
                    "abandoned batch has request_id {:?} and {} leg results",
                    msg.request_id,
                    msg.legs.len()
                ),
            ));
        }
        return Ok(InternalTransferBatchResult {
            request_id: String::new(),
            status: "abandoned".to_owned(),
            legs: Vec::new(),
        });
    }
    if msg.legs.is_empty() {
        return Err(Error::response_contract(OP, "batch has no legs"));
    }
    internal_transfer_batch(OP, &msg.request_id, &msg.status, &msg.legs, false)
}

/// A batch's outcome. Refused with [`Error::ResponseContract`]: a batch
/// status that is unset or unknown to this SDK; an accepted or processing
/// batch without its request ID (any batch without one when `read`); leg
/// results out of request order; a leg without a status; a credited leg
/// without its transfer ID or flow ID; a processing leg without its transfer
/// ID.
fn internal_transfer_batch(
    op: &str,
    request_id: &str,
    status: &buffa::EnumValue<InternalTransferBatchStatus>,
    legs: &[ProtoInternalTransferBatchLegResult],
    read: bool,
) -> Result<InternalTransferBatchResult> {
    let status = match status.as_known() {
        Some(InternalTransferBatchStatus::BATCH_ACCEPTED) => "accepted",
        Some(InternalTransferBatchStatus::BATCH_PROCESSING) => "processing",
        Some(InternalTransferBatchStatus::BATCH_REFUSED) => "refused",
        _ => {
            return Err(Error::response_contract(
                op,
                format!("batch status {} is not known", status.to_i32()),
            ));
        }
    };
    if read || status != "refused" {
        super::common::require_str_id(op, "request_id", request_id)?;
    }
    let legs = legs
        .iter()
        .enumerate()
        .map(|(index, leg)| {
            if usize::try_from(leg.leg_index).ok() != Some(index) {
                return Err(Error::response_contract(
                    op,
                    format!("leg result {index} has leg_index {}", leg.leg_index),
                ));
            }
            let status = match leg.status.as_known() {
                Some(InternalTransferBatchLegStatus::LEG_STATUS_UNSPECIFIED) => {
                    return Err(Error::response_contract(
                        op,
                        format!("leg {index} has no status"),
                    ));
                }
                Some(InternalTransferBatchLegStatus::LEG_CREDITED) => {
                    super::common::require_str_id(op, "transfer_id", &leg.transfer_id)?;
                    super::common::require_str_id(op, "flow_id", &leg.flow_id)?;
                    // A credited leg has passed its checks and moved: the
                    // contract sets its credit time, echoes its asset and
                    // amount, and resolves its asset code and destination.
                    let missing = if leg.credited_at_ts_ns == 0 {
                        Some("credited_at_ts_ns")
                    } else if leg.asset_id == 0 {
                        Some("asset_id")
                    } else if leg.amount_e18.as_option().is_none() {
                        Some("amount_e18")
                    } else if leg.asset_code.is_empty() {
                        Some("asset_code")
                    } else if leg.destination.as_option().is_none() {
                        Some("destination")
                    } else {
                        None
                    };
                    if let Some(field) = missing {
                        return Err(Error::response_contract(
                            op,
                            format!("credited leg {index} has no {field}"),
                        ));
                    }
                    if let Some(destination) = leg.destination.as_option() {
                        credited_destination(op, index, destination)?;
                    }
                    "credited".to_owned()
                }
                Some(InternalTransferBatchLegStatus::LEG_PROCESSING) => {
                    super::common::require_str_id(op, "transfer_id", &leg.transfer_id)?;
                    "processing".to_owned()
                }
                Some(InternalTransferBatchLegStatus::LEG_NOT_MOVED) => "not_moved".to_owned(),
                None => format!("UNKNOWN({})", leg.status.to_i32()),
            };
            let quantity = leg
                .amount_e18
                .as_option()
                .map(|u| super::dust::e18_amount(op, "amount_e18", Some(u), Some(leg.asset_id)))
                .transpose()?;
            Ok(InternalTransferBatchLegResult {
                leg_index: leg.leg_index,
                status,
                transfer_id: leg.transfer_id.clone(),
                flow_id: leg.flow_id.clone(),
                credited_at_ts_ns: leg.credited_at_ts_ns,
                asset_id: leg.asset_id,
                asset_code: leg.asset_code.clone(),
                quantity,
                destination: leg.destination.as_option().map(|d| ResolvedDestination {
                    root_account_public_id: d.root_account_public_id.clone(),
                    subaccount_public_id: d.subaccount_public_id.clone(),
                    smart_account_address: d.smart_account_address.clone(),
                }),
                error_code: super::dust::transfer_reason_label(&leg.error_code),
                error_message: leg.error_message.clone(),
            })
        })
        .collect::<Result<_>>()?;
    Ok(InternalTransferBatchResult {
        request_id: request_id.to_owned(),
        status: status.to_owned(),
        legs,
    })
}

fn wallet_balance_from_proto(msg: &ProtoWalletBalance) -> Result<WalletBalance> {
    let amount = msg.amount_e18.as_option().ok_or_else(|| {
        Error::response_contract(
            "GetWalletBalances",
            format!(
                "balance of zipped asset {} has no amount_e18",
                msg.zipped_asset_id
            ),
        )
    })?;
    let amount_e18 = u128_to_str(amount.hi, amount.lo);
    let amount = format_ledger_u128(&amount_e18, LEDGER_SCALE)?;
    Ok(WalletBalance {
        zipped_asset_id: msg.zipped_asset_id,
        unified_asset_id: msg.unified_asset_id,
        chain_id: msg.chain_id,
        ztoken_address: msg.ztoken_address.clone(),
        amount_e18,
        amount,
    })
}

pub fn wallet_balances_from_proto(msg: &GetWalletBalancesResponse) -> Result<WalletBalances> {
    Ok(WalletBalances {
        smart_account_address: msg.smart_account_address.clone(),
        balances: msg
            .balances
            .iter()
            .map(wallet_balance_from_proto)
            .collect::<Result<_>>()?,
        unavailable_assets: msg
            .unavailable_assets
            .iter()
            .map(|asset| UnavailableWalletAsset {
                zipped_asset_id: asset.zipped_asset_id,
                unified_asset_id: asset.unified_asset_id,
                chain_id: asset.chain_id,
                ztoken_address: asset.ztoken_address.clone(),
                reason: super::changelog::changelog_enum_label(&asset.reason),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::polyester::r#type::v1::U128;
    use crate::proto::transfer::v1::InternalTransferStatus;

    #[test]
    fn deposit_addresses_list_maps_rows() {
        let msg = ListDepositAddressesResponse {
            deposit_addresses: vec![ProtoDepositAddress {
                chain_id: 1,
                deposit_address: "0xabc".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let list = deposit_addresses_list_from_proto(&msg);
        assert_eq!(list.addresses.len(), 1);
        assert_eq!(list.addresses[0].chain_id, 1);
        assert_eq!(list.addresses[0].deposit_address, "0xabc");
    }

    #[test]
    fn create_deposit_address_rejects_missing_required_entity() {
        let err = create_deposit_address_from_proto(&CreateDepositAddressResponse::default())
            .expect_err("missing deposit address must fail closed");
        assert!(err.to_string().contains("missing deposit_address"));
    }

    #[test]
    fn internal_transfer_maps_u128_quantity() {
        let msg = CreateInternalTransferResponse {
            request_id: "req".into(),
            transfer_id: "xfer".into(),
            asset_id: 7,
            asset_code: "USDT".into(),
            amount_e18: U128 {
                hi: 0,
                lo: 1_000_000_000_000_000_000,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let result = internal_transfer_from_proto(&msg).unwrap();
        assert_eq!(result.request_id, "req");
        assert_eq!(result.transfer_id, "xfer");
        assert_eq!(result.asset_id, 7);
        assert_eq!(
            result.quantity.as_ref().unwrap().as_scaled(),
            1_000_000_000_000_000_000
        );
    }

    fn one_token_receipt(status: InternalTransferStatus) -> CreateInternalTransferResponse {
        CreateInternalTransferResponse {
            request_id: "req".into(),
            transfer_id: "xfer".into(),
            asset_id: 7,
            asset_code: "USDT".into(),
            amount_e18: U128 {
                hi: 0,
                lo: 1_000_000_000_000_000_000,
                ..Default::default()
            }
            .into(),
            status: status.into(),
            ..Default::default()
        }
    }

    /// POLY-8824: receipts that differ only in their status decode to
    /// different results.
    #[test]
    fn poly_8824_internal_transfer_receipt_keeps_its_status() {
        let decode = |status| internal_transfer_from_proto(&one_token_receipt(status)).unwrap();
        let accepted = decode(InternalTransferStatus::Accepted);
        let rejected = decode(InternalTransferStatus::Rejected);
        let failed = decode(InternalTransferStatus::Failed);
        assert_ne!(accepted, rejected);
        assert_ne!(accepted, failed);
        assert_ne!(rejected, failed);
        assert_eq!(accepted.status, "accepted");
        assert_eq!(rejected.status, "rejected");
        assert_eq!(failed.status, "failed");
        assert_eq!(decode(InternalTransferStatus::Unspecified).status, "");
        let mut future = one_token_receipt(InternalTransferStatus::Accepted);
        future.status = buffa::EnumValue::Unknown(42);
        assert_eq!(
            internal_transfer_from_proto(&future).unwrap().status,
            "UNKNOWN(42)"
        );
        // The identity and quantity controls are unchanged.
        assert_eq!(
            (accepted.request_id.as_str(), accepted.transfer_id.as_str()),
            ("req", "xfer")
        );
        assert_eq!(
            rejected.quantity.as_ref().map(|q| q.as_scaled()),
            Some(1_000_000_000_000_000_000)
        );
        let mut missing_id = one_token_receipt(InternalTransferStatus::Rejected);
        missing_id.transfer_id.clear();
        assert!(internal_transfer_from_proto(&missing_id).is_err());
    }

    #[test]
    fn withdraw_intent_maps_id() {
        let msg = CreateTradingWithdrawResponse {
            intent_id: "intent-1".into(),
            ..Default::default()
        };
        assert_eq!(
            withdraw_intent_from_proto(&msg).unwrap().intent_id,
            "intent-1"
        );
    }

    #[test]
    fn withdraw_destination_validation_maps_codes() {
        let cases = [
            (
                WithdrawDestinationValidationCode::RESULT_UNSPECIFIED,
                "unspecified",
            ),
            (WithdrawDestinationValidationCode::VALID, "valid"),
            (
                WithdrawDestinationValidationCode::INVALID_ADDRESS,
                "invalid_address",
            ),
            (
                WithdrawDestinationValidationCode::UNSUPPORTED_CHAIN,
                "unsupported_chain",
            ),
            (
                WithdrawDestinationValidationCode::POLYESTER_SMART_ACCOUNT,
                "polyester_smart_account",
            ),
            (
                WithdrawDestinationValidationCode::TOKEN_CONTRACT,
                "token_contract",
            ),
            (
                WithdrawDestinationValidationCode::DENYLISTED_ADDRESS,
                "denylisted_address",
            ),
            (
                WithdrawDestinationValidationCode::SWAP_DEPOSIT_ADDRESS,
                "swap_deposit_address",
            ),
        ];
        for (code, expected) in cases {
            let msg = ValidateWithdrawDestinationResponse {
                valid: code == WithdrawDestinationValidationCode::VALID,
                code: code.into(),
                message: "msg".into(),
                canonical_destination_address: if code == WithdrawDestinationValidationCode::VALID {
                    "0xabc".into()
                } else {
                    String::new()
                },
                ..Default::default()
            };
            let got = withdraw_destination_validation_from_proto(&msg);
            assert_eq!(got.code, expected);
            assert_eq!(got.message, "msg");
        }
        let unknown = ValidateWithdrawDestinationResponse {
            code: buffa::EnumValue::from(99),
            ..Default::default()
        };
        assert_eq!(
            withdraw_destination_validation_from_proto(&unknown).code,
            "unknown_code_99"
        );
    }

    #[test]
    fn singular_mutations_reject_empty_success_responses() {
        assert!(withdraw_intent_from_proto(&CreateTradingWithdrawResponse::default()).is_err());
        assert!(
            withdraw_intent_from_wallet_proto(&CreateWalletTradingWithdrawResponse::default())
                .is_err()
        );
        assert!(internal_transfer_from_proto(&CreateInternalTransferResponse::default()).is_err());
    }

    /// POLY-9102: the receipt's lifecycle flow ID reaches the result, so the
    /// transfer can be read back as a flow.
    #[test]
    fn poly_9102_internal_transfer_receipt_keeps_its_flow_id() {
        let mut receipt = one_token_receipt(InternalTransferStatus::Accepted);
        receipt.flow_id = "flow-7f3a".into();
        let result = internal_transfer_from_proto(&receipt).unwrap();
        assert_eq!(result.flow_id, "flow-7f3a");
        assert_eq!(
            internal_transfer_from_proto(&one_token_receipt(InternalTransferStatus::Accepted))
                .unwrap()
                .flow_id,
            ""
        );
    }

    fn batch_leg(
        index: u32,
        status: InternalTransferBatchLegStatus,
        transfer_id: &str,
        flow_id: &str,
    ) -> ProtoInternalTransferBatchLegResult {
        ProtoInternalTransferBatchLegResult {
            leg_index: index,
            status: status.into(),
            transfer_id: transfer_id.into(),
            flow_id: flow_id.into(),
            asset_id: 1,
            amount_e18: crate::proto::polyester::r#type::v1::U128 {
                hi: 0,
                lo: 500_000_000_000_000_000,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    /// A credited leg whose destination `edit` changes.
    fn credited_to(
        edit: impl FnOnce(&mut crate::proto::transfer::v1::ResolvedDestination),
    ) -> ProtoInternalTransferBatchLegResult {
        let mut leg = credited(0);
        let mut destination = leg.destination.as_option().cloned().unwrap();
        edit(&mut destination);
        leg.destination = destination.into();
        leg
    }

    fn credited(index: u32) -> ProtoInternalTransferBatchLegResult {
        let mut leg = batch_leg(
            index,
            InternalTransferBatchLegStatus::LEG_CREDITED,
            "92640625967207661803151833376978068587",
            "flow_8d1f2c",
        );
        leg.credited_at_ts_ns = 1_763_330_000_000_000_000 + u64::from(index);
        leg.asset_code = "BTC".into();
        leg.destination = crate::proto::transfer::v1::ResolvedDestination {
            root_account_public_id: "AAo8yKqygTg".into(),
            smart_account_address: "0x2222222222222222222222222222222222222222".into(),
            ..Default::default()
        }
        .into();
        leg
    }

    #[test]
    fn batch_decodes_each_leg_in_order() {
        use crate::proto::transfer::v1::{ErrorCode, ResolvedDestination as ProtoDestination};
        let msg = BatchCreateInternalTransfersResponse {
            request_id: "92640625967207661803151833376978068001".into(),
            status: InternalTransferBatchStatus::BATCH_ACCEPTED.into(),
            legs: vec![
                {
                    let mut leg = credited(0);
                    leg.destination = ProtoDestination {
                        root_account_public_id: "AAo8yKqygTg".into(),
                        subaccount_public_id: "Bht3tfQZv8E".into(),
                        smart_account_address: "0x2222222222222222222222222222222222222222".into(),
                        ..Default::default()
                    }
                    .into();
                    leg
                },
                credited(1),
            ],
            ..Default::default()
        };
        let got = internal_transfer_batch_from_proto(&msg, 2).unwrap();
        assert_eq!(
            (got.request_id.as_str(), got.status.as_str()),
            ("92640625967207661803151833376978068001", "accepted")
        );
        assert_eq!(
            got.legs[0].destination,
            Some(ResolvedDestination {
                root_account_public_id: "AAo8yKqygTg".into(),
                subaccount_public_id: "Bht3tfQZv8E".into(),
                smart_account_address: "0x2222222222222222222222222222222222222222".into(),
            })
        );
        let statuses: Vec<_> = got.legs.iter().map(|l| l.status.as_str()).collect();
        assert_eq!(statuses, ["credited", "credited"]);
        assert_eq!(
            (
                got.legs[1].transfer_id.as_str(),
                got.legs[1].flow_id.as_str(),
                got.legs[1].credited_at_ts_ns
            ),
            (
                "92640625967207661803151833376978068587",
                "flow_8d1f2c",
                1_763_330_000_000_000_001
            )
        );
        assert_eq!(
            got.legs[0].quantity.as_ref().map(|q| q.as_scaled()),
            Some(500_000_000_000_000_000)
        );
        assert_eq!(got.legs[0].error_code, None);

        // Processing: legs carry their transfer ID, not yet a flow.
        let processing = GetInternalTransferBatchResponse {
            request_id: "7".into(),
            status: InternalTransferBatchStatus::BATCH_PROCESSING.into(),
            legs: vec![batch_leg(
                0,
                InternalTransferBatchLegStatus::LEG_PROCESSING,
                "8",
                "",
            )],
            ..Default::default()
        };
        let got = internal_transfer_batch_from_get_response(&processing).unwrap();
        assert_eq!(
            (got.status.as_str(), got.legs[0].status.as_str()),
            ("processing", "processing")
        );
        assert_eq!(
            (got.legs[0].flow_id.as_str(), got.legs[0].credited_at_ts_ns),
            ("", 0)
        );

        // Refused before processing: no request ID, nothing moved, and only
        // the leg that caused it carries a code.
        let mut cause = batch_leg(1, InternalTransferBatchLegStatus::LEG_NOT_MOVED, "", "");
        cause.error_code = ErrorCode::ERROR_CODE_INSUFFICIENT_FUNDS.into();
        cause.error_message = "insufficient funds".into();
        let refused_batch = BatchCreateInternalTransfersResponse {
            status: InternalTransferBatchStatus::BATCH_REFUSED.into(),
            legs: vec![
                batch_leg(0, InternalTransferBatchLegStatus::LEG_NOT_MOVED, "", ""),
                cause,
            ],
            ..Default::default()
        };
        let got = internal_transfer_batch_from_proto(&refused_batch, 2).unwrap();
        assert_eq!(
            (got.request_id.as_str(), got.status.as_str()),
            ("", "refused")
        );
        let statuses: Vec<_> = got.legs.iter().map(|l| l.status.as_str()).collect();
        assert_eq!(statuses, ["not_moved", "not_moved"]);
        assert_eq!(
            (
                got.legs[0].error_code.as_deref(),
                got.legs[1].error_code.as_deref()
            ),
            (None, Some("insufficient_funds"))
        );

        // A leg status newer than this SDK is kept, not refused.
        let mut future = credited(0);
        future.status = buffa::EnumValue::from(9);
        let got = internal_transfer_batch_from_proto(
            &BatchCreateInternalTransfersResponse {
                request_id: "7".into(),
                status: InternalTransferBatchStatus::BATCH_ACCEPTED.into(),
                legs: vec![future],
                ..Default::default()
            },
            1,
        )
        .unwrap();
        assert_eq!(got.legs[0].status, "UNKNOWN(9)");
    }

    /// A batch reply that breaks its contract is refused, never read as an
    /// outcome: the batch may have moved funds.
    #[test]
    fn batch_refuses_replies_that_break_the_contract() {
        use InternalTransferBatchLegStatus::{
            LEG_CREDITED, LEG_PROCESSING, LEG_STATUS_UNSPECIFIED,
        };
        let reply = |status: i32, request_id: &str, legs| BatchCreateInternalTransfersResponse {
            request_id: request_id.into(),
            status: buffa::EnumValue::from(status),
            legs,
            ..Default::default()
        };
        let cases = [
            ("unset status", reply(0, "7", vec![credited(0)]), 1),
            ("unknown status", reply(9, "7", vec![credited(0)]), 1),
            (
                "removed submitted status",
                reply(1, "7", vec![credited(0)]),
                1,
            ),
            (
                "accepted without request_id",
                reply(4, "", vec![credited(0)]),
                1,
            ),
            (
                "processing without request_id",
                reply(3, "", vec![batch_leg(0, LEG_PROCESSING, "8", "")]),
                1,
            ),
            ("missing leg", reply(4, "7", vec![credited(0)]), 2),
            (
                "extra leg",
                reply(4, "7", vec![credited(0), credited(0)]),
                1,
            ),
            ("out of order", reply(4, "7", vec![credited(1)]), 1),
            (
                "leg without status",
                reply(4, "7", vec![batch_leg(0, LEG_STATUS_UNSPECIFIED, "8", "")]),
                1,
            ),
            (
                "credited without transfer_id",
                reply(4, "7", vec![batch_leg(0, LEG_CREDITED, "", "flow_8d1f2c")]),
                1,
            ),
            (
                "credited without flow_id",
                reply(4, "7", vec![batch_leg(0, LEG_CREDITED, "8", "")]),
                1,
            ),
            (
                "processing without transfer_id",
                reply(3, "7", vec![batch_leg(0, LEG_PROCESSING, "", "")]),
                1,
            ),
            (
                "credited without credited_at_ts_ns",
                reply(
                    4,
                    "7",
                    vec![ProtoInternalTransferBatchLegResult {
                        credited_at_ts_ns: 0,
                        ..credited(0)
                    }],
                ),
                1,
            ),
            (
                "credited without asset_id",
                reply(
                    4,
                    "7",
                    vec![ProtoInternalTransferBatchLegResult {
                        asset_id: 0,
                        ..credited(0)
                    }],
                ),
                1,
            ),
            (
                "credited without amount_e18",
                reply(
                    4,
                    "7",
                    vec![ProtoInternalTransferBatchLegResult {
                        amount_e18: Default::default(),
                        ..credited(0)
                    }],
                ),
                1,
            ),
            (
                "credited without asset_code",
                reply(
                    4,
                    "7",
                    vec![ProtoInternalTransferBatchLegResult {
                        asset_code: String::new(),
                        ..credited(0)
                    }],
                ),
                1,
            ),
            (
                "credited destination without root ID",
                reply(
                    4,
                    "7",
                    vec![credited_to(|d| d.root_account_public_id.clear())],
                ),
                1,
            ),
            (
                "credited destination with a non-base58 root ID",
                reply(
                    4,
                    "7",
                    vec![credited_to(|d| d.root_account_public_id = "0OIl".into())],
                ),
                1,
            ),
            (
                "credited destination without address",
                reply(
                    4,
                    "7",
                    vec![credited_to(|d| d.smart_account_address.clear())],
                ),
                1,
            ),
            (
                "credited destination with a malformed address",
                reply(
                    4,
                    "7",
                    vec![credited_to(|d| d.smart_account_address = "0x22".into())],
                ),
                1,
            ),
            (
                "credited destination with a non-base58 sub-account ID",
                reply(
                    4,
                    "7",
                    vec![credited_to(|d| {
                        d.subaccount_public_id = "not base58!".into()
                    })],
                ),
                1,
            ),
            (
                "credited without destination",
                reply(
                    4,
                    "7",
                    vec![ProtoInternalTransferBatchLegResult {
                        destination: Default::default(),
                        ..credited(0)
                    }],
                ),
                1,
            ),
        ];
        for (label, msg, requested) in cases {
            let err = internal_transfer_batch_from_proto(&msg, requested).unwrap_err();
            assert!(
                matches!(err, Error::ResponseContract { .. }),
                "{label}: {err:?}"
            );
        }

        // A read names its batch whatever the status, and has legs.
        let read = |request_id: &str, legs| GetInternalTransferBatchResponse {
            request_id: request_id.into(),
            status: InternalTransferBatchStatus::BATCH_REFUSED.into(),
            legs,
            ..Default::default()
        };
        for (label, msg) in [
            (
                "refused read without request_id",
                read(
                    "",
                    vec![batch_leg(
                        0,
                        InternalTransferBatchLegStatus::LEG_NOT_MOVED,
                        "",
                        "",
                    )],
                ),
            ),
            ("read without legs", read("7", vec![])),
        ] {
            let err = internal_transfer_batch_from_get_response(&msg).unwrap_err();
            assert!(
                matches!(err, Error::ResponseContract { .. }),
                "{label}: {err:?}"
            );
        }
    }
}
