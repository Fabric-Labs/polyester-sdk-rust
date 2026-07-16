use super::ServiceContext;
use super::unary;
use crate::codecs::decode::{
    candles_from_proto, depth_enum_for_levels, market_overview_list_from_proto,
    market_trades_from_proto, orderbook_from_proto,
};
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::connect::marketoverview::v1::MarketOverviewServiceClient;
use crate::connect::orderbook::v1::OrderbookServiceClient;
use crate::errors::{Error, Result};
use crate::models::{CandlesResult, MarketOverviewList, MarketTradesResult, OrderbookData};
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

    /// Spot pair catalog. Returns the proto snapshot (Go exposes a raw map escape hatch).
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
    pub async fn get_trades(&self, symbol: &str, limit: Option<u32>) -> Result<MarketTradesResult> {
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
        let resp = unary::await_public(self.client().get_trades(req))
            .await?
            .into_owned();
        Ok(market_trades_from_proto(&resp))
    }

    /// Candle series for a symbol. `interval` accepts values like `"1m"`, `"MIN_1"`, `"5m"`.
    pub async fn get_candles(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
    ) -> Result<CandlesResult> {
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
        let volume_scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol_id(symbol_id);
        let req = GetCandlesRequest {
            symbol_id,
            timeframe: timeframe.into(),
            limit: limit.unwrap_or(0),
            ..Default::default()
        };
        let resp = unary::await_public(self.client().get_candles(req))
            .await?
            .into_owned();
        Ok(candles_from_proto(&resp, volume_scale))
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

    pub async fn list(&self, limit: Option<u32>) -> Result<MarketOverviewList> {
        let req = ListMarketOverviewRequest {
            limit: limit.unwrap_or_default(),
            ..Default::default()
        };
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        let resp = unary::await_public(client.list_market_overview(req))
            .await?
            .into_owned();
        Ok(market_overview_list_from_proto(&resp))
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

    /// Snapshot orderbook for `symbol`. `depth` maps like Go (`None` / `0` → depth 5 bucket).
    pub async fn get(&self, symbol: &str, depth: Option<u32>) -> Result<OrderbookData> {
        let depth_levels = depth.unwrap_or(0);
        let depth_enum = if depth_levels == 0 {
            crate::proto::orderbook::v1::Depth::DepthUnspecified
        } else {
            depth_enum_for_levels(depth_levels)
        };
        // Record the requested depth for the model; unspecified defaults to 50 server-side.
        let reported_depth = if depth_levels == 0 { 50 } else { depth_levels };
        let req = GetOrderBookRequest {
            symbol: symbol.to_owned(),
            depth: depth_enum.into(),
            ..Default::default()
        };
        let quantity_scale = self.ctx.catalogs.base_quantity_scale_for_symbol(symbol);
        let client = OrderbookServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        let resp = unary::await_public(client.get_order_book(req))
            .await?
            .into_owned();
        Ok(orderbook_from_proto(
            &resp,
            symbol,
            reported_depth,
            quantity_scale,
        ))
    }
}
