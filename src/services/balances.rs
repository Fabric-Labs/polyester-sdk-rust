use super::ServiceContext;
use super::unary;
use crate::connect::ledger::read::v1::LedgerReadServiceClient;
use crate::errors::Result;
use crate::proto::ledger::read::v1::GetBalancesRequest;

#[derive(Clone)]
pub struct BalancesService {
    ctx: ServiceContext,
}

impl BalancesService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> LedgerReadServiceClient<crate::transport::SharedTransport> {
        LedgerReadServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    pub async fn list(
        &self,
        req: GetBalancesRequest,
    ) -> Result<crate::proto::ledger::read::v1::GetBalancesResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetBalances",
            req,
            |req, opts| client.get_balances_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn list_transfers(
        &self,
        req: crate::proto::ledger::read::v1::ListTransfersRequest,
    ) -> Result<crate::proto::ledger::read::v1::ListTransfersResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/ListTransfers",
            req,
            |req, opts| client.list_transfers_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn list_holds(
        &self,
        req: crate::proto::ledger::read::v1::ListHoldsRequest,
    ) -> Result<crate::proto::ledger::read::v1::ListHoldsResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/ListHolds",
            req,
            |req, opts| client.list_holds_with_options(req, opts),
        )
        .await?
        .into_owned())
    }
}
