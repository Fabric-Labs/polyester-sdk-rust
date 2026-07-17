use super::ServiceContext;
use super::unary;
use crate::codecs::decode::{
    create_deposit_address_from_proto, deposit_addresses_list_from_proto,
    deposit_withdraw_config_from_proto, withdraw_intent_from_proto,
    withdraw_intent_from_wallet_proto,
};
use crate::connect::chain::deposit::v1::DepositAddressServiceClient;
use crate::connect::chain::withdraw::v1::WithdrawServiceClient;
use crate::connect::chain::zipper::v1::ZipperServiceClient;
use crate::errors::Result;
use crate::models::{
    DepositAddress, DepositAddressesList, DepositWithdrawConfig, WithdrawIntentResult,
};
use crate::proto::chain::deposit::v1::{CreateDepositAddressRequest, ListDepositAddressesRequest};
use crate::proto::chain::withdraw::v1::{
    CreateTradingWithdrawRequest, CreateWalletTradingWithdrawRequest,
};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigRequest;

#[derive(Clone)]
pub struct DepositService {
    ctx: ServiceContext,
}

impl DepositService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn list_addresses(&self) -> Result<DepositAddressesList> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let req = ListDepositAddressesRequest::default();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/ListDepositAddresses",
            req,
            |req, opts| client.list_deposit_addresses_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(deposit_addresses_list_from_proto(&resp))
    }

    pub async fn create_address(&self, req: CreateDepositAddressRequest) -> Result<DepositAddress> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/CreateDepositAddress",
            req,
            |req, opts| client.create_deposit_address_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(create_deposit_address_from_proto(&resp))
    }
}

#[derive(Clone)]
pub struct WithdrawService {
    ctx: ServiceContext,
}

impl WithdrawService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub fn connect_client(&self) -> WithdrawServiceClient<crate::transport::SharedTransport> {
        WithdrawServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    pub async fn create_trading_withdraw(
        &self,
        req: CreateTradingWithdrawRequest,
    ) -> Result<WithdrawIntentResult> {
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.withdraw.v1.WithdrawService/CreateTradingWithdraw",
            req,
            |req, opts| client.create_trading_withdraw_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(withdraw_intent_from_proto(&resp))
    }

    pub async fn create_wallet_trading_withdraw(
        &self,
        req: CreateWalletTradingWithdrawRequest,
    ) -> Result<WithdrawIntentResult> {
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.withdraw.v1.WithdrawService/CreateWalletTradingWithdraw",
            req,
            |req, opts| client.create_wallet_trading_withdraw_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(withdraw_intent_from_wallet_proto(&resp))
    }
}

#[derive(Clone)]
pub struct ZipperService {
    ctx: ServiceContext,
}

impl ZipperService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn get_deposit_withdraw_config(&self) -> Result<DepositWithdrawConfig> {
        let client = ZipperServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        let resp = unary::await_public(
            client.get_deposit_withdraw_config(GetDepositWithdrawConfigRequest::default()),
        )
        .await?
        .into_owned();
        Ok(deposit_withdraw_config_from_proto(&resp))
    }
}
