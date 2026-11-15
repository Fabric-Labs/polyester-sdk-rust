//! Deposit / withdraw / internal-transfer decoders.

use super::money::decode_asset_amount_u128;
use crate::codecs::scalars::{LEDGER_SCALE, format_ledger_u128, u128_to_str};
use crate::errors::{Error, Result};
use crate::models::{
    DepositAddress, DepositAddressesList, InternalTransferBatchLegResult,
    InternalTransferBatchResult, InternalTransferResult, ResolvedDestination, WalletBalance,
    WalletBalances, WithdrawDestinationValidation, WithdrawIntentResult,
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
    BatchCreateInternalTransfersResponse, CreateInternalTransferResponse,
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

/// Decode a batch of internal transfers sent with `requested_legs` legs.
///
/// Refused with [`Error::ResponseContract`]: a batch status that is unset or
/// unknown to this SDK; a submitted batch without its request ID; another
/// number of leg results than legs requested, or leg results out of request
/// order; a leg without a status; an accepted leg without its request ID or
/// transfer ID; a pending leg without its request ID. The batch may have
/// moved funds: retry it with the same idempotency key to read its outcome.
pub fn internal_transfer_batch_from_proto(
    msg: &BatchCreateInternalTransfersResponse,
    requested_legs: usize,
) -> Result<InternalTransferBatchResult> {
    const OP: &str = "BatchCreateInternalTransfers";
    let status = match msg.status.as_known() {
        Some(InternalTransferBatchStatus::BATCH_SUBMITTED) => {
            super::common::require_str_id(OP, "request_id", &msg.request_id)?;
            "submitted"
        }
        Some(InternalTransferBatchStatus::BATCH_REFUSED) => "refused",
        _ => {
            return Err(Error::response_contract(
                OP,
                format!("batch status {} is not known", msg.status.to_i32()),
            ));
        }
    };
    if msg.legs.len() != requested_legs {
        return Err(Error::response_contract(
            OP,
            format!("{} leg results for {requested_legs} legs", msg.legs.len()),
        ));
    }
    let legs = msg
        .legs
        .iter()
        .enumerate()
        .map(|(index, leg)| {
            if usize::try_from(leg.leg_index).ok() != Some(index) {
                return Err(Error::response_contract(
                    OP,
                    format!("leg result {index} has leg_index {}", leg.leg_index),
                ));
            }
            let status = match leg.status.as_known() {
                Some(InternalTransferBatchLegStatus::LEG_STATUS_UNSPECIFIED) => {
                    return Err(Error::response_contract(
                        OP,
                        format!("leg {index} has no status"),
                    ));
                }
                Some(InternalTransferBatchLegStatus::LEG_ACCEPTED) => {
                    super::common::require_str_id(OP, "request_id", &leg.request_id)?;
                    super::common::require_str_id(OP, "transfer_id", &leg.transfer_id)?;
                    "accepted".to_owned()
                }
                Some(InternalTransferBatchLegStatus::LEG_PENDING) => {
                    super::common::require_str_id(OP, "request_id", &leg.request_id)?;
                    "pending".to_owned()
                }
                Some(InternalTransferBatchLegStatus::LEG_REFUSED) => "refused".to_owned(),
                Some(InternalTransferBatchLegStatus::LEG_NOT_SUBMITTED) => {
                    "not_submitted".to_owned()
                }
                None => format!("UNKNOWN({})", leg.status.to_i32()),
            };
            let quantity = leg
                .amount_e18
                .as_option()
                .map(|u| super::dust::e18_amount(OP, "amount_e18", Some(u), Some(leg.asset_id)))
                .transpose()?;
            Ok(InternalTransferBatchLegResult {
                leg_index: leg.leg_index,
                status,
                request_id: leg.request_id.clone(),
                transfer_id: leg.transfer_id.clone(),
                flow_id: leg.flow_id.clone(),
                accepted_at_ts_ns: leg.accepted_at_ts_ns,
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
        request_id: msg.request_id.clone(),
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
        request_id: &str,
        transfer_id: &str,
    ) -> crate::proto::transfer::v1::InternalTransferBatchLegResult {
        crate::proto::transfer::v1::InternalTransferBatchLegResult {
            leg_index: index,
            status: status.into(),
            request_id: request_id.into(),
            transfer_id: transfer_id.into(),
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

    #[test]
    fn batch_decodes_each_leg_in_order() {
        use crate::proto::transfer::v1::{ErrorCode, ResolvedDestination as ProtoDestination};
        let mut refused = batch_leg(2, InternalTransferBatchLegStatus::LEG_REFUSED, "r3", "");
        refused.error_code = ErrorCode::ERROR_CODE_INSUFFICIENT_FUNDS.into();
        refused.error_message = "insufficient funds".into();
        let msg = BatchCreateInternalTransfersResponse {
            request_id: "batch-1".into(),
            status: InternalTransferBatchStatus::BATCH_SUBMITTED.into(),
            legs: vec![
                {
                    let mut leg =
                        batch_leg(0, InternalTransferBatchLegStatus::LEG_ACCEPTED, "r1", "t1");
                    leg.destination = ProtoDestination {
                        root_account_public_id: "AAo8yKqygTg".into(),
                        subaccount_public_id: "Bht3tfQZv8E".into(),
                        smart_account_address: "0x2222222222222222222222222222222222222222".into(),
                        ..Default::default()
                    }
                    .into();
                    leg
                },
                batch_leg(1, InternalTransferBatchLegStatus::LEG_PENDING, "r2", ""),
                refused,
                batch_leg(3, InternalTransferBatchLegStatus::LEG_NOT_SUBMITTED, "", ""),
            ],
            ..Default::default()
        };
        let got = internal_transfer_batch_from_proto(&msg, 4).unwrap();
        assert_eq!(
            (got.request_id.as_str(), got.status.as_str()),
            ("batch-1", "submitted")
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
        assert_eq!(
            statuses,
            ["accepted", "pending", "refused", "not_submitted"]
        );
        assert_eq!(
            got.legs[0].quantity.as_ref().map(|q| q.as_scaled()),
            Some(500_000_000_000_000_000)
        );
        assert_eq!(
            got.legs[2].error_code.as_deref(),
            Some("insufficient_funds")
        );
        assert_eq!(got.legs[0].error_code, None);

        let refused_batch = BatchCreateInternalTransfersResponse {
            status: InternalTransferBatchStatus::BATCH_REFUSED.into(),
            legs: vec![batch_leg(
                0,
                InternalTransferBatchLegStatus::LEG_REFUSED,
                "",
                "",
            )],
            ..Default::default()
        };
        let got = internal_transfer_batch_from_proto(&refused_batch, 1).unwrap();
        assert_eq!(
            (got.request_id.as_str(), got.status.as_str()),
            ("", "refused")
        );
    }

    /// A batch reply that breaks its contract is refused, never read as an
    /// outcome: the batch may have moved funds.
    #[test]
    fn batch_refuses_replies_that_break_the_contract() {
        let ok_leg = || batch_leg(0, InternalTransferBatchLegStatus::LEG_ACCEPTED, "r", "t");
        let reply = |status: i32, request_id: &str, legs| BatchCreateInternalTransfersResponse {
            request_id: request_id.into(),
            status: buffa::EnumValue::from(status),
            legs,
            ..Default::default()
        };
        let cases = [
            ("unset status", reply(0, "b", vec![ok_leg()]), 1),
            ("unknown status", reply(9, "b", vec![ok_leg()]), 1),
            (
                "submitted without request_id",
                reply(1, "", vec![ok_leg()]),
                1,
            ),
            ("missing leg", reply(1, "b", vec![ok_leg()]), 2),
            ("extra leg", reply(1, "b", vec![ok_leg(), ok_leg()]), 1),
            (
                "out of order",
                reply(
                    1,
                    "b",
                    vec![batch_leg(
                        1,
                        InternalTransferBatchLegStatus::LEG_ACCEPTED,
                        "r",
                        "t",
                    )],
                ),
                1,
            ),
            (
                "leg without status",
                reply(
                    1,
                    "b",
                    vec![batch_leg(
                        0,
                        InternalTransferBatchLegStatus::LEG_STATUS_UNSPECIFIED,
                        "r",
                        "",
                    )],
                ),
                1,
            ),
            (
                "accepted without transfer_id",
                reply(
                    1,
                    "b",
                    vec![batch_leg(
                        0,
                        InternalTransferBatchLegStatus::LEG_ACCEPTED,
                        "r",
                        "",
                    )],
                ),
                1,
            ),
            (
                "accepted without request_id",
                reply(
                    1,
                    "b",
                    vec![batch_leg(
                        0,
                        InternalTransferBatchLegStatus::LEG_ACCEPTED,
                        "",
                        "t",
                    )],
                ),
                1,
            ),
            (
                "pending without request_id",
                reply(
                    1,
                    "b",
                    vec![batch_leg(
                        0,
                        InternalTransferBatchLegStatus::LEG_PENDING,
                        "",
                        "",
                    )],
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
    }
}
