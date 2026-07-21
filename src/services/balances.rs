use super::ServiceContext;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    balance_history_from_proto, balances_list_from_proto, equity_history_from_proto,
    holds_list_from_proto, transfers_list_from_proto,
};
use crate::connect::ledger::read::v1::LedgerReadServiceClient;
use crate::errors::Result;
use crate::models::{
    AssetBalance, BalanceHistory, BalancesList, EquityHistory, HoldsList, TransfersList,
};
use crate::proto::ledger::read::v1::{
    GetBalanceHistoryRequest, GetBalancesRequest, GetEquityHistorySeriesRequest,
};

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

    pub async fn list(&self, req: GetBalancesRequest) -> Result<BalancesList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetBalances",
            req,
            |req, opts| client.get_balances_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(balances_list_from_proto(&resp))
    }

    pub async fn get_balance_history(
        &self,
        req: GetBalanceHistoryRequest,
    ) -> Result<BalanceHistory> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetBalanceHistory",
            req,
            |req, opts| client.get_balance_history_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(balance_history_from_proto(&resp))
    }

    pub async fn get_equity_history(
        &self,
        req: GetEquityHistorySeriesRequest,
    ) -> Result<EquityHistory> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetEquityHistorySeries",
            req,
            |req, opts| client.get_equity_history_series_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(equity_history_from_proto(&resp))
    }

    pub async fn list_transfers(
        &self,
        req: crate::proto::ledger::read::v1::ListTransfersRequest,
    ) -> Result<TransfersList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/ListTransfers",
            req,
            |req, opts| client.list_transfers_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(transfers_list_from_proto(&resp))
    }

    pub async fn list_holds(
        &self,
        req: crate::proto::ledger::read::v1::ListHoldsRequest,
    ) -> Result<HoldsList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/ListHolds",
            req,
            |req, opts| client.list_holds_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(holds_list_from_proto(&resp))
    }

    /// Subscribe to private balance updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<AssetBalance>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:ledger:balances:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::asset_balance_from_bytes)
            .await
    }
}
