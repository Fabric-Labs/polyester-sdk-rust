use super::ServiceContext;
use super::unary;
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::connect::marketoverview::v1::MarketOverviewServiceClient;
use crate::connect::orderbook::v1::OrderbookServiceClient;
use crate::errors::Result;
use crate::proto::marketdata::v1::GetSpotConfigRequest;
use crate::proto::marketoverview::v1::ListMarketOverviewRequest;
use crate::proto::orderbook::v1::GetOrderBookRequest;

#[derive(Clone)]
pub struct MarketDataService {
    ctx: ServiceContext,
}

impl MarketDataService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn get_spot_config(
        &self,
    ) -> Result<crate::proto::marketdata::v1::GetSpotConfigResponse> {
        let client = MarketDataServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        Ok(
            unary::await_public(client.get_spot_config(GetSpotConfigRequest::default()))
                .await?
                .into_owned(),
        )
    }
}

#[derive(Clone)]
pub struct MarketOverviewService {
    ctx: ServiceContext,
}

impl MarketOverviewService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn list(
        &self,
        limit: Option<u32>,
    ) -> Result<crate::proto::marketoverview::v1::ListMarketOverviewResponse> {
        let req = ListMarketOverviewRequest {
            limit: limit.unwrap_or_default(),
            ..Default::default()
        };
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        Ok(unary::await_public(client.list_market_overview(req))
            .await?
            .into_owned())
    }
}

#[derive(Clone)]
pub struct OrderbookService {
    ctx: ServiceContext,
}

impl OrderbookService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn get(
        &self,
        req: GetOrderBookRequest,
    ) -> Result<crate::proto::orderbook::v1::GetOrderBookResponse> {
        let client = OrderbookServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        Ok(unary::await_public(client.get_order_book(req))
            .await?
            .into_owned())
    }
}
