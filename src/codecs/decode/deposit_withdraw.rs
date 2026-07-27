//! Deposit / withdraw / internal-transfer decoders.

use super::money::decode_asset_amount_u128;
use crate::codecs::scalars::LEDGER_SCALE;
use crate::errors::{Error, Result};
use crate::models::{
    DepositAddress, DepositAddressesList, InternalTransferResult, WithdrawIntentResult,
};
use crate::proto::chain::deposit::v1::{
    CreateDepositAddressResponse, DepositAddress as ProtoDepositAddress,
    ListDepositAddressesResponse,
};
use crate::proto::chain::withdraw::v1::{
    CreateTradingWithdrawResponse, CreateWalletTradingWithdrawResponse,
};
use crate::proto::transfer::v1::CreateInternalTransferResponse;
use crate::types::QuantityDomain;

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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::polyester::r#type::v1::U128;

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
    fn singular_mutations_reject_empty_success_responses() {
        assert!(withdraw_intent_from_proto(&CreateTradingWithdrawResponse::default()).is_err());
        assert!(
            withdraw_intent_from_wallet_proto(&CreateWalletTradingWithdrawResponse::default())
                .is_err()
        );
        assert!(internal_transfer_from_proto(&CreateInternalTransferResponse::default()).is_err());
    }
}
