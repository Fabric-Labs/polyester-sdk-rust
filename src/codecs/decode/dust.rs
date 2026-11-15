//! Dust conversion decoders (`transfer.v1.DustConversionService`).

use crate::codecs::decode::common::require_str_id;
use crate::codecs::decode::money::decode_asset_amount_u128;
use crate::codecs::scalars::LEDGER_SCALE;
use crate::errors::{Error, Result};
use crate::models::{
    DustAssetPrice, DustBalance, DustBalances, DustConversion, DustConversionsList, DustQuote,
    DustSource,
};
use crate::proto::polyester::r#type::v1::U128;
use crate::proto::transfer::v1::{
    CreateDustConversionResponse, DustAssetEligibility, DustAssetPrice as ProtoDustAssetPrice,
    DustBalance as ProtoDustBalance, DustConversion as ProtoDustConversion, DustConversionState,
    ErrorCode, GetDustConversionResponse, ListDustBalancesResponse, ListDustConversionsResponse,
    QuoteDustConversionResponse,
};
use crate::types::{AssetAmount, QuantityDomain};
use buffa::Enumeration;

/// A known enum value as a lowercase label without `prefix`, empty for the
/// zero value, and `UNKNOWN(<n>)` for a value this SDK does not know.
pub(crate) fn prefixed_label<T: Enumeration>(value: &buffa::EnumValue<T>, prefix: &str) -> String {
    if value.to_i32() == 0 {
        return String::new();
    }
    match value.as_known() {
        Some(known) => {
            let name = known.proto_name();
            name.strip_prefix(prefix)
                .unwrap_or(name)
                .to_ascii_lowercase()
        }
        None => format!("UNKNOWN({})", value.to_i32()),
    }
}

/// A `transfer.v1.ErrorCode` given as a reason, as an order's reject reason
/// is spelled: lowercase without `ERROR_CODE_` (`dust_price_moved`),
/// `UNKNOWN_ERROR_CODE(<n>)` for a code this SDK does not know, `None` when
/// unset.
pub(crate) fn transfer_reason_label(value: &buffa::EnumValue<ErrorCode>) -> Option<String> {
    (value.to_i32() != 0).then(|| match value.as_known() {
        Some(known) => {
            let name = known.proto_name();
            name.strip_prefix("ERROR_CODE_")
                .unwrap_or(name)
                .to_ascii_lowercase()
        }
        None => crate::errors::known_or_unknown_code(None, value.to_i32()),
    })
}

/// An exact 18-decimal amount; an absent `U128` is zero, and one this SDK
/// cannot represent (above 2^127 - 1) is a response-contract error.
pub(crate) fn e18_amount(
    operation: &str,
    field: &str,
    value: Option<&U128>,
    asset_id: Option<u32>,
) -> Result<AssetAmount> {
    let (hi, lo) = value.map_or((0, 0), |v| (v.hi, v.lo));
    decode_asset_amount_u128(
        hi,
        lo,
        Some(LEDGER_SCALE),
        QuantityDomain::LedgerE18,
        asset_id,
    )
    .ok_or_else(|| Error::response_contract(operation, format!("{field} is out of range")))
}

fn optional_e18_amount(
    operation: &str,
    field: &str,
    value: Option<&U128>,
    asset_id: Option<u32>,
) -> Result<Option<AssetAmount>> {
    value
        .map(|v| e18_amount(operation, field, Some(v), asset_id))
        .transpose()
}

fn dust_balance(operation: &str, msg: &ProtoDustBalance) -> Result<DustBalance> {
    Ok(DustBalance {
        asset_id: msg.asset_id,
        amount: e18_amount(
            operation,
            "amount_e18",
            msg.amount_e18.as_option(),
            Some(msg.asset_id),
        )?,
        eligibility: prefixed_label::<DustAssetEligibility>(
            &msg.eligibility,
            "DUST_ASSET_ELIGIBILITY_",
        ),
        usd_value: optional_e18_amount(
            operation,
            "usd_value_e18",
            msg.usd_value_e18.as_option(),
            None,
        )?,
    })
}

fn dust_price(operation: &str, msg: &ProtoDustAssetPrice) -> Result<DustAssetPrice> {
    let price = rust_decimal::Decimal::try_from_i128_with_scale(
        i128::from(msg.price_ticks),
        msg.price_scale,
    )
    .map_err(|_| {
        Error::response_contract(
            operation,
            format!(
                "price of asset {} has scale {}, beyond what this SDK represents exactly",
                msg.asset_id, msg.price_scale
            ),
        )
    })?;
    Ok(DustAssetPrice {
        asset_id: msg.asset_id,
        price_ticks: msg.price_ticks,
        price_scale: msg.price_scale,
        price,
        as_of: msg.as_of.as_option().cloned(),
    })
}

pub fn dust_balances_from_proto(msg: &ListDustBalancesResponse) -> Result<DustBalances> {
    const OP: &str = "ListDustBalances";
    Ok(DustBalances {
        target_asset_id: msg.target_asset_id,
        targets: msg.targets.clone(),
        fee_bps: msg.fee_bps,
        balances: msg
            .balances
            .iter()
            .map(|b| dust_balance(OP, b))
            .collect::<Result<_>>()?,
        gross_usd: e18_amount(OP, "gross_usd_e18", msg.gross_usd_e18.as_option(), None)?,
        payout: e18_amount(
            OP,
            "payout_e18",
            msg.payout_e18.as_option(),
            Some(msg.target_asset_id),
        )?,
        target_available: msg.target_available,
    })
}

pub fn dust_quote_from_proto(msg: &QuoteDustConversionResponse) -> Result<DustQuote> {
    const OP: &str = "QuoteDustConversion";
    Ok(DustQuote {
        quote_id: (!msg.quote_id.is_empty()).then(|| msg.quote_id.clone()),
        expires_at: msg.expires_at.as_option().cloned(),
        target_asset_id: msg.target_asset_id,
        fee_bps: msg.fee_bps,
        gross_usd: e18_amount(OP, "gross_usd_e18", msg.gross_usd_e18.as_option(), None)?,
        payout: e18_amount(
            OP,
            "payout_e18",
            msg.payout_e18.as_option(),
            Some(msg.target_asset_id),
        )?,
        balances: msg
            .balances
            .iter()
            .map(|b| dust_balance(OP, b))
            .collect::<Result<_>>()?,
        prices: msg
            .prices
            .iter()
            .map(|p| dust_price(OP, p))
            .collect::<Result<_>>()?,
    })
}

/// A conversion, refused with [`Error::ResponseContract`] without its
/// conversion ID.
fn dust_conversion(operation: &str, msg: &ProtoDustConversion) -> Result<DustConversion> {
    require_str_id(operation, "conversion_id", &msg.conversion_id)?;
    let target = Some(msg.target_asset_id);
    Ok(DustConversion {
        conversion_id: msg.conversion_id.clone(),
        quote_id: msg.quote_id.clone(),
        state: prefixed_label::<DustConversionState>(&msg.state, "DUST_CONVERSION_STATE_"),
        refusal_code: transfer_reason_label(&msg.refusal_code),
        target_asset_id: msg.target_asset_id,
        sources: msg
            .sources
            .iter()
            .map(|s| {
                Ok(DustSource {
                    asset_id: s.asset_id,
                    amount: e18_amount(
                        operation,
                        "amount_e18",
                        s.amount_e18.as_option(),
                        Some(s.asset_id),
                    )?,
                })
            })
            .collect::<Result<_>>()?,
        payout: e18_amount(operation, "payout_e18", msg.payout_e18.as_option(), target)?,
        quoted_payout: e18_amount(
            operation,
            "quoted_payout_e18",
            msg.quoted_payout_e18.as_option(),
            target,
        )?,
        fee: e18_amount(operation, "fee_e18", msg.fee_e18.as_option(), target)?,
        fee_bps: msg.fee_bps,
        created_at: msg.created_at.as_option().cloned(),
        completed_at: msg.completed_at.as_option().cloned(),
    })
}

/// The conversion a `CreateDustConversion` reply returns; a reply without it
/// is an [`Error::ResponseContract`].
pub fn dust_conversion_from_create_response(
    msg: &CreateDustConversionResponse,
) -> Result<DustConversion> {
    const OP: &str = "CreateDustConversion";
    let conversion = msg
        .conversion
        .as_option()
        .ok_or_else(|| Error::response_contract(OP, "missing conversion"))?;
    dust_conversion(OP, conversion)
}

/// The conversion a `GetDustConversion` reply returns; a reply without it is
/// an [`Error::ResponseContract`].
pub fn dust_conversion_from_get_response(
    msg: &GetDustConversionResponse,
) -> Result<DustConversion> {
    const OP: &str = "GetDustConversion";
    let conversion = msg
        .conversion
        .as_option()
        .ok_or_else(|| Error::response_contract(OP, "missing conversion"))?;
    dust_conversion(OP, conversion)
}

pub fn dust_conversions_list_from_proto(
    msg: &ListDustConversionsResponse,
) -> Result<DustConversionsList> {
    Ok(DustConversionsList {
        conversions: msg
            .conversions
            .iter()
            .map(|c| dust_conversion("ListDustConversions", c))
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u128(value: u128) -> U128 {
        U128 {
            hi: (value >> 64) as u64,
            lo: value as u64,
            ..Default::default()
        }
    }

    fn conversion(id: &str) -> ProtoDustConversion {
        ProtoDustConversion {
            conversion_id: id.into(),
            quote_id: "dq_1".into(),
            state: DustConversionState::DUST_CONVERSION_STATE_REFUSED.into(),
            refusal_code: ErrorCode::ERROR_CODE_DUST_PRICE_MOVED.into(),
            target_asset_id: 3,
            sources: vec![crate::proto::transfer::v1::DustSource {
                asset_id: 10,
                amount_e18: u128(5_000_000_000_000_000_000).into(),
                ..Default::default()
            }],
            payout_e18: u128(5_390_000_000_000_000_000).into(),
            quoted_payout_e18: u128(5_400_000_000_000_000_001).into(),
            fee_e18: u128(110_000_000_000_000_000).into(),
            fee_bps: 200,
            ..Default::default()
        }
    }

    #[test]
    fn quote_decodes_exact_amounts_prices_and_labels() {
        let msg = QuoteDustConversionResponse {
            quote_id: "dq_6f1d".into(),
            target_asset_id: 3,
            fee_bps: 200,
            gross_usd_e18: u128(5_500_000_000_000_000_000).into(),
            payout_e18: u128(5_390_000_000_000_000_000).into(),
            balances: vec![
                ProtoDustBalance {
                    asset_id: 10,
                    amount_e18: u128(1).into(),
                    eligibility: DustAssetEligibility::DUST_ASSET_ELIGIBILITY_ELIGIBLE.into(),
                    usd_value_e18: u128(2_500_000_000_000_000_000).into(),
                    ..Default::default()
                },
                ProtoDustBalance {
                    asset_id: 11,
                    amount_e18: u128(7).into(),
                    eligibility: DustAssetEligibility::DUST_ASSET_ELIGIBILITY_NO_PRICE.into(),
                    ..Default::default()
                },
                ProtoDustBalance {
                    asset_id: 12,
                    eligibility: buffa::EnumValue::from(99),
                    ..Default::default()
                },
            ],
            prices: vec![ProtoDustAssetPrice {
                asset_id: 10,
                price_ticks: 500_000_000,
                price_scale: 9,
                ..Default::default()
            }],
            ..Default::default()
        };
        let quote = dust_quote_from_proto(&msg).unwrap();
        assert_eq!(quote.quote_id.as_deref(), Some("dq_6f1d"));
        assert_eq!(quote.payout.as_scaled(), 5_390_000_000_000_000_000);
        assert_eq!(quote.payout.asset_id(), Some(3));
        assert_eq!(quote.gross_usd.as_scaled(), 5_500_000_000_000_000_000);
        assert_eq!(quote.balances[0].eligibility, "eligible");
        assert_eq!(quote.balances[0].amount.as_scaled(), 1);
        assert_eq!(
            quote.balances[0]
                .usd_value
                .as_ref()
                .map(AssetAmount::as_scaled),
            Some(2_500_000_000_000_000_000)
        );
        assert_eq!(quote.balances[1].eligibility, "no_price");
        assert!(quote.balances[1].usd_value.is_none());
        assert_eq!(quote.balances[2].eligibility, "UNKNOWN(99)");
        assert_eq!(quote.prices[0].price.to_string(), "0.500000000");
        let nothing = dust_quote_from_proto(&QuoteDustConversionResponse::default()).unwrap();
        assert!(nothing.quote_id.is_none());
        assert_eq!(nothing.payout.as_scaled(), 0);
    }

    #[test]
    fn amounts_beyond_the_sdk_range_are_refused() {
        let msg = ListDustBalancesResponse {
            payout_e18: u128(u128::MAX).into(),
            ..Default::default()
        };
        assert!(matches!(
            dust_balances_from_proto(&msg),
            Err(Error::ResponseContract { .. })
        ));
        let price = QuoteDustConversionResponse {
            prices: vec![ProtoDustAssetPrice {
                price_scale: 40,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            dust_quote_from_proto(&price),
            Err(Error::ResponseContract { .. })
        ));
    }

    #[test]
    fn conversion_decodes_state_refusal_and_exact_amounts() {
        let got = dust_conversion_from_get_response(&GetDustConversionResponse {
            conversion: conversion("17").into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(got.conversion_id, "17");
        assert_eq!(got.state, "refused");
        assert_eq!(got.refusal_code.as_deref(), Some("dust_price_moved"));
        assert_eq!(got.quoted_payout.as_scaled(), 5_400_000_000_000_000_001);
        assert_eq!(got.sources[0].amount.asset_id(), Some(10));
        let mut processing = conversion("18");
        processing.state = DustConversionState::DUST_CONVERSION_STATE_PROCESSING.into();
        processing.refusal_code = buffa::EnumValue::from(0);
        let got = dust_conversion_from_create_response(&CreateDustConversionResponse {
            conversion: processing.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!((got.state.as_str(), got.refusal_code), ("processing", None));
        let mut future = conversion("19");
        future.refusal_code = buffa::EnumValue::from(999);
        let list = dust_conversions_list_from_proto(&ListDustConversionsResponse {
            conversions: vec![future],
            next_page_token: "n".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            list.conversions[0].refusal_code.as_deref(),
            Some("UNKNOWN_ERROR_CODE(999)")
        );
    }

    /// A conversion reply must carry the conversion and its ID.
    #[test]
    fn conversion_replies_require_the_conversion_and_its_id() {
        assert!(matches!(
            dust_conversion_from_create_response(&CreateDustConversionResponse::default()),
            Err(Error::ResponseContract { .. })
        ));
        assert!(matches!(
            dust_conversion_from_get_response(&GetDustConversionResponse::default()),
            Err(Error::ResponseContract { .. })
        ));
        assert!(matches!(
            dust_conversion_from_get_response(&GetDustConversionResponse {
                conversion: conversion("").into(),
                ..Default::default()
            }),
            Err(Error::ResponseContract { .. })
        ));
        assert!(matches!(
            dust_conversions_list_from_proto(&ListDustConversionsResponse {
                conversions: vec![conversion("")],
                ..Default::default()
            }),
            Err(Error::ResponseContract { .. })
        ));
    }
}
