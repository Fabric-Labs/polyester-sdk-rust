//! Dust conversion (`transfer.v1.DustConversionService`).

use super::limits::{
    DUST_CONVERSION_ID_MAX_LEN, DUST_PAGE_TOKEN_MAX_LEN, DUST_QUOTE_ID_MAX_LEN,
    IDEMPOTENCY_KEY_MAX_LEN, LIST_DUST_CONVERSIONS_LIMIT_MAX, check_len, check_limit,
    check_page_token,
};
use super::{ServiceContext, scope, unary};
use crate::codecs::decode::{
    dust_balances_from_proto, dust_conversion_from_create_response,
    dust_conversion_from_get_response, dust_conversions_list_from_proto, dust_quote_from_proto,
};
use crate::connect::transfer::v1::DustConversionServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    DustBalances, DustConversion, DustConversionsList, DustQuote, SubAccountScope,
};
use crate::proto::transfer::v1::{
    CreateDustConversionRequest, GetDustConversionRequest, ListDustBalancesRequest,
    ListDustConversionsRequest, QuoteDustConversionRequest,
};
use crate::transport::SharedTransport;

/// Dust conversion: turn dust, Trading balances too small to trade, into one
/// target asset (USDT to start) at index prices less a disclosed fee. A
/// conversion moves every source and credits the payout atomically, or moves
/// nothing.
///
/// A balance is dust when no open market accepts an order for it at the
/// current index price and it is worth at most 10 USD; one conversion takes
/// at most 20 balances worth at most 200 USD together; an account converts
/// once every 6 hours (refused conversions excepted), and each target has a
/// daily limit. The usual flow is [`Self::quote`], [`Self::convert`] within
/// 30 seconds, then [`Self::get_conversion`] while the state is
/// `processing`.
///
/// Every call is signed. An API key needs `CONVERT_DUST` on its policy for
/// quotes and conversions (and, for a sub-account, on the sub-account's
/// policy too), and `READ_BALANCES` for [`Self::list_balances`]. No step-up
/// is needed: the value stays in the account. Each call takes the account as
/// [`SubAccountScope`]; [`SubAccountScope::Id`] 0 is refused.
///
/// Errors to expect: `ERROR_CODE_DUST_QUOTE_EXPIRED`,
/// `ERROR_CODE_DUST_PRICE_MOVED`, `ERROR_CODE_DUST_BALANCE_CHANGED`,
/// `ERROR_CODE_DUST_NOT_ELIGIBLE`, `ERROR_CODE_DUST_CONVERSION_FAILED` and
/// `ERROR_CODE_DUST_TARGET_UNAVAILABLE` mean quote again; they are final and
/// not retryable as sent. `ERROR_CODE_DUST_WINDOW_ACTIVE` (converted within
/// 6 hours, or a conversion still processing) and
/// `ERROR_CODE_DUST_DAILY_LIMIT` mean try much later and are not retryable;
/// `ERROR_CODE_DUST_BUSY` is retryable shortly
/// ([`crate::errors::Error::is_retryable`]).
#[derive(Clone)]
pub struct DustConversionService {
    ctx: ServiceContext,
}

impl DustConversionService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> DustConversionServiceClient<SharedTransport> {
        DustConversionServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn subaccount(&self, subaccount_id: SubAccountScope) -> Result<u64> {
        Ok(scope::resolve_subaccount(&self.ctx, subaccount_id)?.unwrap_or(0))
    }

    /// The account's available Trading balances with their eligibility for
    /// `target_asset_id` (0 for the first target), and the payout the eligible
    /// ones would bring now. Reserves nothing.
    pub async fn list_balances(
        &self,
        target_asset_id: u32,
        subaccount_id: SubAccountScope,
    ) -> Result<DustBalances> {
        let req = ListDustBalancesRequest {
            subaccount_id: self.subaccount(subaccount_id)?,
            target_asset_id,
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.DustConversionService/ListDustBalances",
            req,
            |req, opts| client.list_dust_balances_with_options(req, opts),
        )
        .await?
        .into_owned();
        dust_balances_from_proto(&resp)
    }

    /// Price the account's dust into `target_asset_id` (required, one of
    /// [`DustBalances::targets`]). The quote is valid for 30 seconds and
    /// reserves nothing; its `quote_id` is `None` when nothing converts.
    pub async fn quote(
        &self,
        target_asset_id: u32,
        subaccount_id: SubAccountScope,
    ) -> Result<DustQuote> {
        if target_asset_id == 0 {
            return Err(Error::validation(
                "quote_dust_conversion target_asset_id is required",
            ));
        }
        let req = QuoteDustConversionRequest {
            subaccount_id: self.subaccount(subaccount_id)?,
            target_asset_id,
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.DustConversionService/QuoteDustConversion",
            req,
            |req, opts| client.quote_dust_conversion_with_options(req, opts),
        )
        .await?
        .into_owned();
        dust_quote_from_proto(&resp)
    }

    /// Convert `quote_id` (from [`Self::quote`], at most 30 seconds old) at
    /// current index prices, under `idempotency_key` (1 to 128 characters,
    /// sent as given). Retrying with the same key returns the same
    /// conversion. The account must be the quote's.
    ///
    /// The conversion is `completed`, or `processing` when its outcome is
    /// not known yet: poll [`Self::get_conversion`] with its
    /// `conversion_id`. A reply without the conversion or its ID is an
    /// [`Error::ResponseContract`]; it may have converted, so retry with the
    /// same key.
    pub async fn convert(
        &self,
        quote_id: &str,
        idempotency_key: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<DustConversion> {
        const OP: &str = "create_dust_conversion";
        check_len(OP, "quote_id", quote_id, DUST_QUOTE_ID_MAX_LEN)?;
        check_len(
            OP,
            "idempotency_key",
            idempotency_key,
            IDEMPOTENCY_KEY_MAX_LEN,
        )?;
        let req = CreateDustConversionRequest {
            subaccount_id: self.subaccount(subaccount_id)?,
            idempotency_key: idempotency_key.to_owned(),
            quote_id: quote_id.to_owned(),
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.DustConversionService/CreateDustConversion",
            req,
            |req, opts| client.create_dust_conversion_with_options(req, opts),
        )
        .await?
        .into_owned();
        dust_conversion_from_create_response(&resp)
    }

    /// One of the account's conversions. `conversion_id` is the decimal ID a
    /// conversion carries (1 to 39 digits, no leading zero).
    pub async fn get_conversion(
        &self,
        conversion_id: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<DustConversion> {
        const OP: &str = "get_dust_conversion";
        check_len(
            OP,
            "conversion_id",
            conversion_id,
            DUST_CONVERSION_ID_MAX_LEN,
        )?;
        let bytes = conversion_id.as_bytes();
        if !(bytes[0].is_ascii_digit() && bytes[0] != b'0' && bytes.iter().all(u8::is_ascii_digit))
        {
            return Err(Error::validation(format!(
                "{OP} conversion_id {conversion_id:?} is not a conversion ID: decimal digits without a leading zero"
            )));
        }
        let req = GetDustConversionRequest {
            subaccount_id: self.subaccount(subaccount_id)?,
            conversion_id: conversion_id.to_owned(),
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.DustConversionService/GetDustConversion",
            req,
            |req, opts| client.get_dust_conversion_with_options(req, opts),
        )
        .await?
        .into_owned();
        dust_conversion_from_get_response(&resp)
    }

    /// One page of the account's conversions, newest first. `limit` 0 asks
    /// for the server's default (20); above 100 is refused. Pass the previous
    /// page's `next_page_token` to continue.
    pub async fn list_conversions(
        &self,
        limit: u32,
        page_token: Option<&str>,
        subaccount_id: SubAccountScope,
    ) -> Result<DustConversionsList> {
        const OP: &str = "list_dust_conversions";
        check_limit(OP, limit, LIST_DUST_CONVERSIONS_LIMIT_MAX)?;
        let page_token = page_token.unwrap_or_default();
        check_page_token(OP, page_token, DUST_PAGE_TOKEN_MAX_LEN)?;
        let req = ListDustConversionsRequest {
            subaccount_id: self.subaccount(subaccount_id)?,
            limit,
            page_token: page_token.to_owned(),
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.DustConversionService/ListDustConversions",
            req,
            |req, opts| client.list_dust_conversions_with_options(req, opts),
        )
        .await?
        .into_owned();
        dust_conversions_list_from_proto(&resp)
    }
}
