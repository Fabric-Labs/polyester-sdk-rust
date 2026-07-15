use super::ServiceContext;
use super::unary;
use crate::connect::chain::deposit::v1::DepositAddressServiceClient;
use crate::connect::chain::withdraw::v1::WithdrawServiceClient;
use crate::connect::chain::zipper::v1::ZipperServiceClient;
use crate::errors::Result;
use crate::proto::chain::deposit::v1::{CreateDepositAddressRequest, ListDepositAddressesRequest};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigRequest;

#[derive(Clone)]
pub struct DepositService {
    ctx: ServiceContext,
}

impl DepositService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn list_addresses(
        &self,
    ) -> Result<crate::proto::chain::deposit::v1::ListDepositAddressesResponse> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let req = ListDepositAddressesRequest::default();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/ListDepositAddresses",
            req,
            |req, opts| client.list_deposit_addresses_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn create_address(
        &self,
        req: CreateDepositAddressRequest,
    ) -> Result<crate::proto::chain::deposit::v1::CreateDepositAddressResponse> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/CreateDepositAddress",
            req,
            |req, opts| client.create_deposit_address_with_options(req, opts),
        )
        .await?
        .into_owned())
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
}

#[derive(Clone)]
pub struct ZipperService {
    ctx: ServiceContext,
}

impl ZipperService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn get_deposit_withdraw_config(
        &self,
    ) -> Result<crate::proto::chain::zipper::v1::GetDepositWithdrawConfigResponse> {
        let client = ZipperServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        Ok(unary::await_public(
            client.get_deposit_withdraw_config(GetDepositWithdrawConfigRequest::default()),
        )
        .await?
        .into_owned())
    }
}
