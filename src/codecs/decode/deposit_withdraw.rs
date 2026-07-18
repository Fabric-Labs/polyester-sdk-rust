//! Deposit / withdraw / internal-transfer decoders.

use super::money::decode_asset_amount_u128;
use crate::codecs::scalars::LEDGER_SCALE;
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

pub fn create_deposit_address_from_proto(msg: &CreateDepositAddressResponse) -> DepositAddress {
    msg.deposit_address
        .as_option()
        .map(deposit_address_from_proto)
        .unwrap_or(DepositAddress {
            chain_id: 0,
            deposit_address: String::new(),
        })
}

pub fn withdraw_intent_from_proto(msg: &CreateTradingWithdrawResponse) -> WithdrawIntentResult {
    WithdrawIntentResult {
        intent_id: msg.intent_id.clone(),
        status: String::new(),
        flow_id: String::new(),
    }
}

pub fn withdraw_intent_from_wallet_proto(
    msg: &CreateWalletTradingWithdrawResponse,
) -> WithdrawIntentResult {
    WithdrawIntentResult {
        intent_id: msg.intent_id.clone(),
        status: String::new(),
        flow_id: String::new(),
    }
}

pub fn internal_transfer_from_proto(
    msg: &CreateInternalTransferResponse,
) -> InternalTransferResult {
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
    InternalTransferResult {
        request_id: msg.request_id.clone(),
        transfer_id: msg.transfer_id.clone(),
        asset_id,
        asset_code: msg.asset_code.clone(),
        quantity,
    }
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
        let result = internal_transfer_from_proto(&msg);
        assert_eq!(result.request_id, "req");
        assert_eq!(result.transfer_id, "xfer");
        assert_eq!(result.asset_id, 7);
        assert_eq!(
            result.quantity.as_ref().unwrap().scaled,
            1_000_000_000_000_000_000
        );
    }

    #[test]
    fn withdraw_intent_maps_id() {
        let msg = CreateTradingWithdrawResponse {
            intent_id: "intent-1".into(),
            ..Default::default()
        };
        assert_eq!(withdraw_intent_from_proto(&msg).intent_id, "intent-1");
    }
}
