use super::ServiceContext;
use super::unary;
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::connect::marketoverview::v1::MarketOverviewServiceClient;
use crate::connect::orderbook::v1::OrderbookServiceClient;
use crate::errors::{Error, Result};
use crate::proto::marketdata::v1::{
    GetCandlesRequest, GetSpotConfigRequest, GetTradesRequest, Timeframe,
};
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

    fn client(&self) -> MarketDataServiceClient<crate::transport::SharedTransport> {
        MarketDataServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        )
    }

    pub async fn get_spot_config(
        &self,
    ) -> Result<crate::proto::marketdata::v1::GetSpotConfigResponse> {
        Ok(unary::await_public(
            self.client()
                .get_spot_config(GetSpotConfigRequest::default()),
        )
        .await?
        .into_owned())
    }

    /// Recent public trades for a symbol (resolves `symbol_id` via catalogs after hydrate).
    pub async fn get_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<crate::proto::marketdata::v1::GetTradesResponse> {
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        let req = GetTradesRequest {
            symbol_id,
            limit: limit.unwrap_or(0),
            ..Default::default()
        };
        Ok(unary::await_public(self.client().get_trades(req))
            .await?
            .into_owned())
    }

    /// Candle series for a symbol. `interval` accepts values like `"1m"`, `"MIN_1"`, `"5m"`.
    pub async fn get_candles(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
    ) -> Result<crate::proto::marketdata::v1::GetCandlesResponse> {
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        let timeframe = parse_timeframe(interval)?;
        let req = GetCandlesRequest {
            symbol_id,
            timeframe: timeframe.into(),
            limit: limit.unwrap_or(0),
            ..Default::default()
        };
        Ok(unary::await_public(self.client().get_candles(req))
            .await?
            .into_owned())
    }

    /// Subscribe to public spot trades for a symbol (requires `realtime` feature + hydrated catalogs).
    #[cfg(feature = "realtime")]
    pub async fn subscribe_trades(&self, symbol: &str) -> Result<crate::realtime::Subscription> {
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        let channel = format!("public:spot:market:trades:{symbol_id}:proto");
        self.ctx.realtime.subscribe_raw(&channel).await
    }
}

fn parse_timeframe(interval: &str) -> Result<Timeframe> {
    let key = interval.trim().to_ascii_lowercase().replace('_', "");
    let tf = match key.as_str() {
        "1s" | "sec1" => Timeframe::Sec1,
        "1m" | "min1" => Timeframe::Min1,
        "5m" | "min5" => Timeframe::Min5,
        "15m" | "min15" => Timeframe::Min15,
        "30m" | "min30" => Timeframe::Min30,
        "1h" | "hour1" => Timeframe::Hour1,
        "4h" | "hour4" => Timeframe::Hour4,
        "12h" | "hour12" => Timeframe::Hour12,
        "1d" | "day1" => Timeframe::Day1,
        "1w" | "week1" => Timeframe::Week1,
        "1mo" | "month1" => Timeframe::Month1,
        _ => {
            return Err(Error::validation(format!(
                "unsupported candle interval {interval:?}"
            )));
        }
    };
    Ok(tf)
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
