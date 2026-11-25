use super::ServiceContext;
use super::limits::{
    CANDLES_LIMIT_MAX, MARKET_OVERVIEW_LIMIT_MAX, MARKET_TRADES_LIMIT_MAX, check_limit,
};
use super::unary;
use crate::codecs::decode::{
    DEPTH_TIERS, asset_info_list_from_proto, candles_columns_from_proto, candles_from_proto,
    currency_conversion_config_from_proto, currency_conversion_rates_from_proto,
    depth_tier_for_levels, index_price_candles_from_proto, market_overview_list_from_proto,
    market_trades_from_proto, orderbook_from_proto, orderbook_snapshot_depth,
    orderbook_stream_tier, require_published_stream_depth, spot_config_from_proto,
    spot_volume_history_from_proto,
};
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::connect::marketoverview::v1::MarketOverviewServiceClient;
use crate::connect::orderbook::v1::OrderbookServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    AssetInfo, Candle, CandlesResult, CurrencyConversionConfig, CurrencyConversionRates,
    GetCandlesOpts, GetIndexPriceCandlesOpts, GetTradesOpts, IndexPriceCandlesResult,
    MarketOverviewList, MarketTradesResult, OrderbookData, SpotConfig, SpotVolumeHistory,
};
use crate::models::{MarketOverviewEntry, MarketTrade, OrderBookDeltaUpdate};
use crate::proto::marketdata::v1::{
    GetCandlesColumnsRequest, GetCandlesRequest, GetIndexPriceCandlesRequest, GetSpotConfigRequest,
    GetTradesRequest, ListAssetInfoRequest, Timeframe,
};
use crate::proto::marketoverview::v1::{
    GetCurrencyConversionConfigRequest, GetCurrencyConversionRatesRequest,
    GetSpotVolumeHistoryRequest, ListMarketOverviewRequest,
};
use crate::proto::orderbook::v1::GetOrderBookRequest;
use crate::proto::polyester::r#type::v1::DepthTier;
use crate::types::money::Quantity;
use buffa_types::google::protobuf::Timestamp;

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
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn resolve_symbol_id(
        &self,
        symbol: Option<&str>,
        symbol_id: Option<u32>,
        label: &str,
    ) -> Result<u32> {
        if let Some(id) = symbol_id.filter(|id| *id != 0) {
            self.ctx
                .catalogs
                .require_symbol_matches_id(symbol, id, label)?;
            return Ok(id);
        }
        let Some(symbol) = symbol.filter(|s| !s.is_empty()) else {
            return Err(Error::validation(format!(
                "{label} requires symbol or symbol_id"
            )));
        };
        self.ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })
    }

    fn require_market_data_volume_scale(&self, symbol_id: u32, label: &str) -> Result<u32> {
        self.ctx
            .catalogs
            .market_data_volume_scale_for_symbol_id(symbol_id)
            .ok_or_else(|| {
                Error::validation(format!(
                    "{label} requires a hydrated market_data_volume_scale for symbol_id {symbol_id}"
                ))
            })
    }

    fn require_quantity_scale(&self, symbol_id: u32, label: &str) -> Result<u32> {
        self.ctx
            .catalogs
            .base_quantity_scale_for_symbol_id(symbol_id)
            .ok_or_else(|| {
                Error::validation(format!(
                    "{label} requires a catalog quantity scale for symbol_id {symbol_id}; \
                     wait_for_catalogs or hydrate the spot catalog first"
                ))
            })
    }

    fn timestamp_field(secs: Option<i64>) -> buffa::MessageField<Timestamp> {
        match secs {
            Some(seconds) => Timestamp {
                seconds,
                nanos: 0,
                ..Default::default()
            }
            .into(),
            None => buffa::MessageField::none(),
        }
    }

    /// Spot pair catalog (Go `SpotConfig` raw-map escape hatch) with typed
    /// order limits. A malformed order limit is an error.
    pub async fn get_spot_config(&self) -> Result<SpotConfig> {
        let resp = unary::await_public(
            self.client()
                .get_spot_config(GetSpotConfigRequest::default()),
        )
        .await?
        .into_owned();
        spot_config_from_proto(&resp)
    }

    /// Asset information (`marketdata.v1.MarketDataService/ListAssetInfo`):
    /// team-maintained descriptions, official links and known launch dates
    /// and prices, and market datapoints (supply, market capitalization,
    /// rank) with their source and observation time.
    /// Public: no API key is needed and the request is never signed.
    ///
    /// Pass the numeric asset IDs to read (the spot config's `ledger_id`), or
    /// none for every saved entry. Duplicate and unknown IDs are ignored, and
    /// assets with neither an editorial nor market datapoints are left out;
    /// an asset with datapoints only has an empty editorial. The result is
    /// complete, with no paging, in ascending `asset_id` order.
    ///
    /// At most 1000 IDs, each from 1 to 2147483647, as the contract allows;
    /// any other list is refused with [`Error::Validation`] before sending.
    pub async fn list_asset_info(&self, asset_ids: &[u32]) -> Result<Vec<AssetInfo>> {
        super::limits::check_asset_info_ids(asset_ids)?;
        let req = ListAssetInfoRequest {
            asset_ids: asset_ids.to_vec(),
            ..Default::default()
        };
        // "/marketdata.v1.MarketDataService/ListAssetInfo"
        let resp = unary::await_public(self.client().list_asset_info(req))
            .await?
            .into_owned();
        asset_info_list_from_proto(&resp)
    }

    /// Recent public trades for a symbol (resolves `symbol_id` via catalogs after hydrate).
    pub async fn get_trades(&self, symbol: &str, limit: Option<u32>) -> Result<MarketTradesResult> {
        self.get_trades_with(GetTradesOpts {
            symbol: Some(symbol.to_owned()),
            limit,
            ..Default::default()
        })
        .await
    }

    /// The trades of pair `symbol_id` after `after_trade_id`, decoded with
    /// `quantity_scale` rather than the client's catalog: the market-data
    /// stream reads its trade gaps with the catalog of the connection that
    /// found them.
    pub(crate) async fn trades_after_scaled(
        &self,
        symbol_id: u32,
        quantity_scale: u32,
        after_trade_id: u64,
        limit: u32,
    ) -> Result<MarketTradesResult> {
        let req = GetTradesRequest {
            symbol_id,
            limit,
            after_trade_id: Some(after_trade_id),
            ..Default::default()
        };
        let resp = unary::await_public(self.client().get_trades(req))
            .await?
            .into_owned();
        market_trades_from_proto(&resp, quantity_scale)
    }

    pub async fn get_trades_with(&self, opts: GetTradesOpts) -> Result<MarketTradesResult> {
        let limit = opts.limit.unwrap_or(0);
        check_limit("get_trades", limit, MARKET_TRADES_LIMIT_MAX)?;
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let symbol_id =
            self.resolve_symbol_id(opts.symbol.as_deref(), opts.symbol_id, "get_trades")?;
        let quantity_scale = self.require_quantity_scale(symbol_id, "get_trades")?;
        let page_token = opts.page_token.unwrap_or_default();
        if opts.after_trade_id.is_some()
            && (opts.start.is_some() || opts.end.is_some() || !page_token.is_empty())
        {
            return Err(Error::validation(
                "get_trades after_trade_id cannot be combined with start, end or page_token",
            ));
        }
        let req = GetTradesRequest {
            symbol_id,
            limit,
            start_time: Self::timestamp_field(opts.start),
            end_time: Self::timestamp_field(opts.end),
            page_token,
            after_trade_id: opts.after_trade_id,
            ..Default::default()
        };
        let resp = unary::await_public(self.client().get_trades(req))
            .await?
            .into_owned();
        market_trades_from_proto(&resp, quantity_scale)
    }

    /// Candle series for a symbol, ordered newest-first by `ts_sec`.
    ///
    /// `interval` is a documented label or `Timeframe` enum name exactly as
    /// written, such as `"1m"`, `"MIN_1"`, `"5m"` or `"1mo"`; anything else,
    /// an empty string or `"1M"` included, is a validation error before any
    /// catalog wait or request. When incomplete candles are requested, the
    /// current open candle is prepended.
    pub async fn get_candles(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
    ) -> Result<CandlesResult> {
        self.get_candles_with(GetCandlesOpts {
            symbol: Some(symbol.to_owned()),
            timeframe: interval.to_owned(),
            limit,
            ..Default::default()
        })
        .await
    }

    pub async fn get_candles_with(&self, opts: GetCandlesOpts) -> Result<CandlesResult> {
        check_limit("get_candles", opts.limit.unwrap_or(0), CANDLES_LIMIT_MAX)?;
        // An unknown timeframe is refused before the catalog wait or any request.
        candle_timeframe(&opts)?;
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let (req, volume_scale) = self.build_candles_request(&opts)?;
        let symbol_id = req.symbol_id;
        let resp = unary::await_public(self.client().get_candles(req))
            .await?
            .into_owned();
        candles_from_proto(
            &resp,
            volume_scale,
            self.ctx
                .catalogs
                .reference_price_scale_for_symbol_id(symbol_id),
        )
    }

    /// Latest candle for a symbol/timeframe, or `None` when the market has no rows.
    pub async fn get_current_candle(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Option<Candle>> {
        let result = self
            .get_candles_with(GetCandlesOpts {
                symbol: Some(symbol.to_owned()),
                timeframe: timeframe.to_owned(),
                limit: Some(1),
                include_incomplete: true,
                ..Default::default()
            })
            .await?;
        Ok(newest_candle(result))
    }

    /// Columnar OHLCV candles decoded into row-oriented [`CandlesResult`].
    ///
    /// Each primary row's [`Candle::is_closed`] is the server's `is_closed`
    /// flag, as [`Self::get_candles`] reports it: with `include_incomplete`
    /// the current open candle is appended last on the first page and is the
    /// only row that reads `false`; it does not count toward `limit`.
    /// Reference rows are always closed buckets and read `true`.
    ///
    /// From a server that predates the flag (an empty `is_closed` column),
    /// every row reads `true` except, on a first page (no `page_token`) read
    /// with `include_incomplete`, the newest primary row, which reads `false`,
    /// meaning "not known to be closed".
    pub async fn get_candles_columns(&self, opts: GetCandlesOpts) -> Result<CandlesResult> {
        check_limit(
            "get_candles_columns",
            opts.limit.unwrap_or(0),
            CANDLES_LIMIT_MAX,
        )?;
        // An unknown timeframe is refused before the catalog wait or any request.
        candle_timeframe(&opts)?;
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let (base, volume_scale) = self.build_candles_request(&opts)?;
        // Only a first page can hold the open candle: a continuation page
        // holds only older, closed candles.
        let may_hold_open_candle = base.include_incomplete && base.page_token.is_empty();
        let req = GetCandlesColumnsRequest {
            symbol_id: base.symbol_id,
            timeframe: base.timeframe,
            limit: base.limit,
            start_time: base.start_time,
            end_time: base.end_time,
            include_incomplete: base.include_incomplete,
            include_reference: base.include_reference,
            page_token: base.page_token,
            ..Default::default()
        };
        let resp = unary::await_public(self.client().get_candles_columns(req))
            .await?
            .into_owned();
        // A server that predates the `is_closed` column leaves it empty.
        let flags_provided = !resp.is_closed.is_empty();
        let mut result = candles_columns_from_proto(
            &resp,
            volume_scale,
            self.ctx
                .catalogs
                .reference_price_scale_for_symbol_id(base.symbol_id),
        )?;
        if may_hold_open_candle
            && !flags_provided
            && let Some(newest) = result.candles.last_mut()
        {
            // The open candle, when one is in range, comes last in the
            // oldest-first primary series, unflagged.
            newest.is_closed = false;
        }
        Ok(result)
    }

    /// Index price candles for a symbol
    /// (`marketdata.v1.MarketDataService/GetIndexPriceCandles`), newest first
    /// by `ts_sec`: one continuous series of the market's composite reference
    /// candles before the bucket containing its listing, and its index price
    /// candles from that bucket on, each marked with its
    /// [`IndexPriceCandle::source`](crate::models::IndexPriceCandle::source).
    /// Public: no API key is needed and the request is never signed.
    ///
    /// `interval` is read as [`Self::get_candles`] reads it, except that one
    /// second (`"1s"`, `"SEC_1"`) has no index price candles; that, an empty
    /// string or any other unknown label is a validation error before any
    /// catalog wait or request.
    pub async fn get_index_price_candles(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
    ) -> Result<IndexPriceCandlesResult> {
        self.get_index_price_candles_with(GetIndexPriceCandlesOpts {
            symbol: Some(symbol.to_owned()),
            timeframe: interval.to_owned(),
            limit,
            ..Default::default()
        })
        .await
    }

    /// [`Self::get_index_price_candles`] with a time range and paging.
    ///
    /// Only closed candles are returned; a bucket without a valid index price
    /// is omitted, so a missing `ts_sec` is a gap. Prices use the pair's
    /// `reference_price_scale`, and a pair whose catalog has none is refused
    /// with [`Error::Validation`] before sending. A `limit` above 10000 is
    /// refused before sending; `None` takes the server's default of 500.
    /// Follow `next_page_token` with the same symbol, timeframe, `start` and
    /// `end`; a page token of another candle method is refused by the API.
    pub async fn get_index_price_candles_with(
        &self,
        opts: GetIndexPriceCandlesOpts,
    ) -> Result<IndexPriceCandlesResult> {
        let limit = opts.limit.unwrap_or(0);
        check_limit("get_index_price_candles", limit, CANDLES_LIMIT_MAX)?;
        // An unknown timeframe is refused before the catalog wait or any request.
        let timeframe = parse_index_price_timeframe(&opts.timeframe)?;
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let symbol_id = self.resolve_symbol_id(
            opts.symbol.as_deref(),
            opts.symbol_id,
            "get_index_price_candles",
        )?;
        let price_scale = self
            .ctx
            .catalogs
            .reference_price_scale_for_symbol_id(symbol_id)
            .ok_or_else(|| {
                Error::validation(format!(
                    "index price candles require reference_price_scale for symbol_id {symbol_id}"
                ))
            })?;
        let req = GetIndexPriceCandlesRequest {
            symbol_id,
            timeframe: timeframe.into(),
            limit,
            start_time: Self::timestamp_field(opts.start),
            end_time: Self::timestamp_field(opts.end),
            page_token: opts.page_token.unwrap_or_default(),
            ..Default::default()
        };
        // "/marketdata.v1.MarketDataService/GetIndexPriceCandles"
        let resp = unary::await_public(self.client().get_index_price_candles(req))
            .await?
            .into_owned();
        index_price_candles_from_proto(&resp, price_scale)
    }

    fn build_candles_request(&self, opts: &GetCandlesOpts) -> Result<(GetCandlesRequest, u32)> {
        let symbol_id =
            self.resolve_symbol_id(opts.symbol.as_deref(), opts.symbol_id, "get_candles")?;
        let timeframe = candle_timeframe(opts)?;
        let volume_scale = self.require_market_data_volume_scale(symbol_id, "get_candles")?;
        if opts.include_reference
            && self
                .ctx
                .catalogs
                .reference_price_scale_for_symbol_id(symbol_id)
                .is_none()
        {
            return Err(Error::validation(format!(
                "candle reference prices require reference_price_scale for symbol_id {symbol_id}"
            )));
        }
        let req = GetCandlesRequest {
            symbol_id,
            timeframe: timeframe.into(),
            limit: opts.limit.unwrap_or(0),
            start_time: Self::timestamp_field(opts.start),
            end_time: Self::timestamp_field(opts.end),
            include_incomplete: opts.include_incomplete,
            include_reference: opts.include_reference,
            page_token: opts.page_token.clone().unwrap_or_default(),
            ..Default::default()
        };
        Ok((req, volume_scale))
    }

    /// Subscribe to public spot trades for a symbol (requires `realtime` feature + hydrated catalogs).
    ///
    /// Each publication batches trades; every trade arrives on its own, in
    /// execution order.
    pub async fn subscribe_trades(
        &self,
        symbol: &str,
    ) -> Result<crate::realtime::TypedSubscription<MarketTrade>> {
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        let quantity_scale = self.require_quantity_scale(symbol_id, "subscribe_trades")?;
        let channel = format!("public:spot:market:trades:{symbol_id}:proto");
        self.ctx
            .realtime
            .subscribe_proto_batch(
                &channel,
                crate::codecs::decode::market_trade_batch_from_bytes(quantity_scale),
            )
            .await
    }

    /// Subscribe to public candle updates (requires `realtime` feature + hydrated catalogs).
    pub async fn subscribe_candles(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<crate::realtime::TypedSubscription<Candle>> {
        // An unknown timeframe is refused before the catalog wait or subscribing.
        let resolved = parse_timeframe(timeframe)?;
        // Decoded with the pair's catalog scales.
        self.ctx.wait_for_catalogs().await?;
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        // Live Centrifugo channels use human labels (`1m`), not REST enum names (`MIN_1`).
        let channel_tf = crate::codecs::decode::timeframe_label(resolved);
        if channel_tf.is_empty() {
            return Err(Error::validation(format!(
                "unsupported candle interval {timeframe:?}"
            )));
        }
        let volume_scale = self.require_market_data_volume_scale(symbol_id, "subscribe_candles")?;
        let channel = format!("public:spot:market:candles:{channel_tf}:{symbol_id}:proto");
        let decode = crate::codecs::decode::candle_point_from_bytes(
            symbol_id,
            channel_tf.to_owned(),
            volume_scale,
        );
        self.ctx.realtime.subscribe_proto(&channel, decode).await
    }

    /// Channel segment for candle subscriptions after alias normalization.
    #[cfg(test)]
    pub(crate) fn candle_channel_timeframe(timeframe: &str) -> Result<&'static str> {
        let resolved = parse_timeframe(timeframe)?;
        let label = crate::codecs::decode::timeframe_label(resolved);
        if label.is_empty() {
            return Err(Error::validation(format!(
                "unsupported candle interval {timeframe:?}"
            )));
        }
        Ok(label)
    }
}

/// Every accepted candle timeframe, exactly as written: a documented label
/// (`"1s"`, `"1m"`, `"3m"`, … `"1w"`, `"1mo"`) or a `Timeframe` enum name
/// (`"SEC_1"`, `"MIN_1"`, … `"MONTH_1"`). The Go and Python SDKs accept the
/// same set.
const CANDLE_TIMEFRAMES: [(&str, &str, Timeframe); 16] = [
    ("1s", "SEC_1", Timeframe::Sec1),
    ("1m", "MIN_1", Timeframe::Min1),
    ("3m", "MIN_3", Timeframe::Min3),
    ("5m", "MIN_5", Timeframe::Min5),
    ("15m", "MIN_15", Timeframe::Min15),
    ("30m", "MIN_30", Timeframe::Min30),
    ("1h", "HOUR_1", Timeframe::Hour1),
    ("2h", "HOUR_2", Timeframe::Hour2),
    ("4h", "HOUR_4", Timeframe::Hour4),
    ("6h", "HOUR_6", Timeframe::Hour6),
    ("8h", "HOUR_8", Timeframe::Hour8),
    ("12h", "HOUR_12", Timeframe::Hour12),
    ("1d", "DAY_1", Timeframe::Day1),
    ("3d", "DAY_3", Timeframe::Day3),
    ("1w", "WEEK_1", Timeframe::Week1),
    ("1mo", "MONTH_1", Timeframe::Month1),
];

/// Reads a candle timeframe exactly as written (see [`CANDLE_TIMEFRAMES`]).
/// No other case, spelling or padding is read, so `"1M"` is never taken for
/// `"1m"` (one minute) nor for `"1mo"` (one month); anything else is a
/// validation error.
fn parse_timeframe(interval: &str) -> Result<Timeframe> {
    CANDLE_TIMEFRAMES
        .iter()
        .find(|(label, name, _)| interval == *label || interval == *name)
        .map(|(_, _, timeframe)| *timeframe)
        .ok_or_else(|| {
            Error::validation(format!(
                "unsupported candle interval {interval:?}; use a label such as \"1m\", \"1h\", \"1d\" or \"1mo\" (one month), or an enum name such as \"MIN_1\""
            ))
        })
}

/// The timeframe a candles call reads: [`GetCandlesOpts::timeframe`] exactly
/// as written. An empty one is refused like any other unknown label; leaving
/// the field out through [`GetCandlesOpts::default`] asks for `"1m"`.
fn candle_timeframe(opts: &GetCandlesOpts) -> Result<Timeframe> {
    parse_timeframe(&opts.timeframe)
}

/// The timeframe an index price candles call reads: any candle timeframe
/// [`parse_timeframe`] reads except one second, which has no index price
/// candles.
fn parse_index_price_timeframe(interval: &str) -> Result<Timeframe> {
    match parse_timeframe(interval)? {
        Timeframe::Sec1 => Err(Error::validation(format!(
            "unsupported index price candle interval {interval:?}; one second has no index price candles, use \"1m\" or longer"
        ))),
        timeframe => Ok(timeframe),
    }
}

fn newest_candle(result: CandlesResult) -> Option<Candle> {
    result.candles.into_iter().next()
}

/// Fills each row's symbol and formatted base volume from the catalogs, for
/// list rows and live rows alike.
fn enrich_market_overview_rows(
    catalogs: &crate::catalogs::Manager,
    rows: &mut [MarketOverviewEntry],
) -> Result<()> {
    for market in rows {
        if market.symbol.is_empty() {
            market.symbol = catalogs.display_symbol(market.symbol_id);
        }
        // Book sizes are base quantities: give them the pair's scale and symbol.
        if let Some(scale) = catalogs.base_quantity_scale_for_symbol_id(market.symbol_id) {
            for qty in [&mut market.best_bid_qty, &mut market.best_ask_qty] {
                if let Some(raw) = qty.as_ref() {
                    *qty = Some(
                        Quantity::from_scaled(
                            raw.as_scaled(),
                            Some(scale),
                            raw.domain(),
                            Some(market.symbol.clone()).filter(|symbol| !symbol.is_empty()),
                            Some(market.symbol_id),
                        )
                        .map_err(|e| {
                            Error::validation(format!("market overview book size invalid: {e}"))
                        })?,
                    );
                }
            }
        }
        if let (Some(raw), Some(scale)) = (
            market.volume_24h_base_scaled.as_deref(),
            catalogs.market_data_volume_scale_for_symbol_id(market.symbol_id),
        ) && let Ok(value) = raw.parse::<i64>()
        {
            market.volume_24h_base = Some(
                crate::codecs::scalars::format_qty_scaled(value, scale).map_err(|e| {
                    Error::validation(format!("market overview base volume scale invalid: {e}"))
                })?,
            );
        }
    }
    Ok(())
}

/// Decodes a live overview batch and enriches its rows like list rows.
fn enriched_market_overview_batch(
    catalogs: &crate::catalogs::Manager,
    payload: &[u8],
) -> Result<MarketOverviewList> {
    let mut batch = crate::codecs::decode::market_overview_batch_from_bytes(payload)?;
    enrich_market_overview_rows(catalogs, &mut batch.markets)?;
    Ok(batch)
}

/// Options for [`MarketOverviewService::list`].
#[derive(Debug, Clone, Default)]
pub struct ListMarketOverviewOptions {
    pub symbols: Option<Vec<String>>,
    pub limit: Option<u32>,
    /// Opaque token from the previous page; reuse the same filters.
    pub page_token: Option<String>,
    pub include_sparklines: bool,
}

impl From<Option<u32>> for ListMarketOverviewOptions {
    fn from(limit: Option<u32>) -> Self {
        Self {
            limit,
            ..Default::default()
        }
    }
}

/// Options for managed market-overview subscriptions.
#[derive(Debug, Clone, Default)]
pub struct MarketOverviewCreateSubscriptionOptions {
    pub symbols: Option<Vec<String>>,
    pub limit: Option<u32>,
    pub include_sparklines: bool,
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
        opts: impl Into<ListMarketOverviewOptions>,
    ) -> Result<MarketOverviewList> {
        let opts = opts.into();
        check_limit(
            "market_overview.list",
            opts.limit.unwrap_or_default(),
            MARKET_OVERVIEW_LIMIT_MAX,
        )?;
        self.ctx.wait_for_catalogs().await?;
        let req = ListMarketOverviewRequest {
            symbol_id: self
                .ctx
                .catalogs
                .resolve_symbol_ids(opts.symbols.as_deref())?,
            // Proto u32: 0 means omit / server default (typically 50).
            limit: opts.limit.unwrap_or_default(),
            page_token: opts.page_token.unwrap_or_default(),
            include_sparklines: opts.include_sparklines,
            ..Default::default()
        };
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_public(client.list_market_overview(req))
            .await?
            .into_owned();
        let mut list = market_overview_list_from_proto(&resp)?;
        enrich_market_overview_rows(&self.ctx.catalogs, &mut list.markets)?;
        Ok(list)
    }

    /// Trailing 24-hour USD volume samples for configured spot pairs.
    ///
    /// Omit both filters to select every configured pair. Send at most 2,000
    /// distinct positive pair IDs. Do not sum the overlapping samples.
    pub async fn get_spot_volume_history(
        &self,
        symbols: Option<&[String]>,
        symbol_ids: Option<&[u32]>,
    ) -> Result<SpotVolumeHistory> {
        const MAX_SYMBOL_IDS: usize = 2000;
        if symbols.is_some_and(|items| !items.is_empty())
            && symbol_ids.is_some_and(|items| !items.is_empty())
        {
            return Err(Error::validation(
                "market_overview.get_spot_volume_history accepts only one of symbols or symbol_ids",
            ));
        }
        let mut resolved = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if let Some(ids) = symbol_ids {
            for id in ids {
                if *id == 0 {
                    return Err(Error::validation(
                        "market_overview.get_spot_volume_history symbol_ids must be positive",
                    ));
                }
                if seen.insert(*id) {
                    resolved.push(*id);
                }
            }
        } else if let Some(symbols) = symbols {
            if !symbols.is_empty() {
                self.ctx.wait_for_catalogs().await?;
            }
            for id in self.ctx.catalogs.resolve_symbol_ids(Some(symbols))? {
                if seen.insert(id) {
                    resolved.push(id);
                }
            }
        }
        if resolved.len() > MAX_SYMBOL_IDS {
            return Err(Error::validation(
                "market_overview.get_spot_volume_history accepts at most 2000 symbol_ids",
            ));
        }
        // Pairs are labelled from the catalogs.
        self.ctx.settle_catalogs().await;
        let req = GetSpotVolumeHistoryRequest {
            symbol_id: resolved,
            ..Default::default()
        };
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_public(client.get_spot_volume_history(req))
            .await?
            .into_owned();
        let mut history = spot_volume_history_from_proto(&resp);
        for pair in &mut history.pairs {
            if pair.symbol.is_empty() {
                pair.symbol = self.ctx.catalogs.display_symbol(pair.symbol_id);
            }
        }
        Ok(history)
    }

    /// Supported fiat and stablecoin display metadata.
    ///
    /// Entries are ordered by code. Names, symbols, and fraction digits are
    /// presentation defaults; fraction digits do not specify rate precision.
    /// Configuration remains available before rates are observed.
    pub async fn get_currency_conversion_config(&self) -> Result<CurrencyConversionConfig> {
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_public(
            client.get_currency_conversion_config(GetCurrencyConversionConfigRequest::default()),
        )
        .await?
        .into_owned();
        Ok(currency_conversion_config_from_proto(&resp))
    }

    /// Fiat units per USD and USD per stablecoin unit.
    ///
    /// Fiat `units_per_usd_e8` is currency units per 1 USD at 1e8 scale (USD
    /// identity is 100_000_000). Stablecoin `usd_per_unit_e8` is observed USD
    /// per unit at the same scale. A missing fiat snapshot or omitted
    /// stablecoin is unobserved, not zero. The request fails with unavailable
    /// (HTTP 503) before any observation exists.
    pub async fn get_currency_conversion_rates(&self) -> Result<CurrencyConversionRates> {
        let client = MarketOverviewServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_public(
            client.get_currency_conversion_rates(GetCurrencyConversionRatesRequest::default()),
        )
        .await?
        .into_owned();
        Ok(currency_conversion_rates_from_proto(&resp))
    }

    /// Subscribe to public market overview batches.
    pub async fn subscribe(
        &self,
    ) -> Result<crate::realtime::TypedSubscription<MarketOverviewList>> {
        // Rows are labelled and scaled from the catalogs.
        self.ctx.settle_catalogs().await;
        self.ctx
            .realtime
            .subscribe_proto("public:spot:market_overview:updates:proto", {
                let catalogs = self.ctx.catalogs.clone();
                move |payload: &[u8]| enriched_market_overview_batch(&catalogs, payload)
            })
            .await
    }

    /// Snapshot-then-stream merged overview rows.
    pub async fn create_subscription(
        &self,
        opts: MarketOverviewCreateSubscriptionOptions,
    ) -> Result<crate::marketoverview::Subscription> {
        use crate::realtime::{SnapshotThenStream, SnapshotThenStreamConfig};
        use std::collections::HashMap;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        use tokio::sync::mpsc;

        // Snapshot fetch uses the list default when omitted/zero.
        let limit = opts.limit.filter(|n| *n > 0).unwrap_or(50);
        if opts
            .symbols
            .as_ref()
            .is_some_and(|symbols| !symbols.is_empty())
        {
            self.ctx.wait_for_catalogs().await?;
        }
        let symbol_ids: std::collections::HashSet<u32> = self
            .ctx
            .catalogs
            .resolve_symbol_ids(opts.symbols.as_deref())?
            .into_iter()
            .collect();
        // Keep the original display list for snapshot fetch; wire IDs are
        // resolved again in `list`.
        let symbols = (!symbol_ids.is_empty()).then_some(opts.symbols.clone().unwrap_or_default());
        let include_sparklines = opts.include_sparklines;
        let channel = "public:spot:market_overview:updates:proto".to_owned();

        let by_symbol_id: Arc<Mutex<HashMap<u32, MarketOverviewEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error: Arc<Mutex<Option<crate::Error>>> = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<Vec<MarketOverviewEntry>>(50);
        let tx_slot: Arc<Mutex<Option<mpsc::Sender<Vec<MarketOverviewEntry>>>>> =
            Arc::new(Mutex::new(Some(tx)));
        let stream_slot: Arc<
            Mutex<Option<SnapshotThenStream<MarketOverviewList, MarketOverviewList>>>,
        > = Arc::new(Mutex::new(None));

        let emit = {
            let by_symbol_id = by_symbol_id.clone();
            let closed = closed.clone();
            let last_error = last_error.clone();
            let tx_slot = tx_slot.clone();
            let stream_slot = stream_slot.clone();
            Arc::new(move || {
                if closed.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let rows: Vec<MarketOverviewEntry> =
                    crate::realtime::lock_unpoisoned(&by_symbol_id)
                        .values()
                        .cloned()
                        .collect();
                let Some(tx) = crate::realtime::lock_unpoisoned(&tx_slot).as_ref().cloned() else {
                    return;
                };
                if !crate::realtime::try_enqueue(
                    &tx,
                    rows,
                    &closed,
                    &last_error,
                    "market overview subscription queue full; consumer too slow",
                ) && let Some(err @ Error::QueueOverflow(_)) =
                    crate::realtime::lock_unpoisoned(&last_error).clone()
                {
                    let _ = crate::realtime::lock_unpoisoned(&tx_slot).take();
                    if let Some(stream) = crate::realtime::lock_unpoisoned(&stream_slot).as_ref() {
                        stream.fail(err);
                    }
                }
            }) as Arc<dyn Fn() + Send + Sync>
        };

        // The channel carries every market; a filtered subscription keeps
        // only its own, in buffered and live updates alike.
        let apply_rows = {
            let by_symbol_id = by_symbol_id.clone();
            Arc::new(move |rows: Vec<MarketOverviewEntry>| {
                let mut map = crate::realtime::lock_unpoisoned(&by_symbol_id);
                for mut row in rows {
                    if symbol_ids.is_empty() || symbol_ids.contains(&row.symbol_id) {
                        // Live publications carry no sparklines: keep the snapshot's.
                        if row.sparklines.is_empty()
                            && let Some(previous) = map.get(&row.symbol_id)
                        {
                            row.sparklines = previous.sparklines.clone();
                        }
                        map.insert(row.symbol_id, row);
                    }
                }
            }) as Arc<dyn Fn(Vec<MarketOverviewEntry>) + Send + Sync>
        };

        let svc = self.clone();
        let fetch_symbols = symbols.clone();
        let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client: self.ctx.realtime.clone(),
            channel,
            decode: {
                let catalogs = self.ctx.catalogs.clone();
                Arc::new(move |payload: &[u8]| enriched_market_overview_batch(&catalogs, payload))
            },
            fetch_snapshot: Arc::new(move || {
                let svc = svc.clone();
                let symbols = fetch_symbols.clone();
                Box::pin(async move {
                    svc.list(ListMarketOverviewOptions {
                        symbols,
                        limit: Some(limit),
                        include_sparklines,
                        ..Default::default()
                    })
                    .await
                })
            }),
            read_publication: Arc::new(|batch: MarketOverviewList| vec![batch]),
            apply_snapshot: {
                let apply_rows = apply_rows.clone();
                let emit = emit.clone();
                let by_symbol_id = by_symbol_id.clone();
                Arc::new(
                    move |snapshot: MarketOverviewList, buffered: Vec<MarketOverviewList>| {
                        crate::realtime::lock_unpoisoned(&by_symbol_id).clear();
                        apply_rows(snapshot.markets);
                        for batch in buffered {
                            apply_rows(batch.markets);
                        }
                        emit();
                    },
                )
            },
            apply_live_publications: {
                let apply_rows = apply_rows.clone();
                let emit = emit.clone();
                Arc::new(move |batches: Vec<MarketOverviewList>| {
                    for batch in batches {
                        apply_rows(batch.markets);
                    }
                    emit();
                })
            },
            max_buffered: 2000,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });
        *crate::realtime::lock_unpoisoned(&stream_slot) = Some(stream.clone());

        let subscription = crate::marketoverview::Subscription::new(
            rx,
            stream.clone(),
            closed,
            last_error,
            tx_slot,
        );
        if let Err(err) = stream.start().await {
            subscription.close();
            return Err(err);
        }
        Ok(subscription)
    }
}

/// Options for managed orderbook subscriptions.
#[derive(Debug, Clone, Default)]
pub struct CreateSubscriptionOptions {
    pub symbol: String,
    pub symbol_id: Option<u32>,
    pub depth: Option<u32>,
    pub bucket: Option<String>,
}

#[derive(Clone)]
pub struct OrderbookService {
    ctx: ServiceContext,
}

impl OrderbookService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Snapshot orderbook for `symbol`, `depth` levels per side: one of the
    /// contract's depth tiers, 1, 5, 10, 20, 50, 100, 200, 500, 1000 or 5000
    /// ([`crate::codecs::decode::DEPTH_TIERS`]), sent as that tier. `None`
    /// sends no tier, and the server serves 50. Any other count, 0 included,
    /// is refused with [`Error::Validation`] before sending. The result's `depth` is the
    /// tier the server served.
    ///
    /// Order snapshots by `(book_epoch, book_seq)` with
    /// [`crate::codecs::decode::compare_snapshots`]. A snapshot is a frozen view:
    /// never apply stream updates to it, and fetch it again to refresh it. Its
    /// `ts` is the server's snapshot time, `None` when the reply has none.
    pub async fn get(&self, symbol: &str, depth: Option<u32>) -> Result<OrderbookData> {
        let tier = match depth {
            None => DepthTier::DEPTH_UNSPECIFIED,
            Some(levels) => depth_tier_for_levels(levels).ok_or_else(|| {
                Error::validation(format!(
                    "orderbook depth {levels} is no depth tier; use one of {DEPTH_TIERS:?}, or None for 50"
                ))
            })?,
        };
        let depth = orderbook_snapshot_depth(depth.unwrap_or(0));
        // Levels are decoded with the pair's catalog quantity scale.
        self.ctx.wait_for_catalogs().await?;
        let req = GetOrderBookRequest {
            symbol_id: self.ctx.catalogs.require_symbol_id(symbol)?,
            depth: tier.into(),
            ..Default::default()
        };
        let quantity_scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "orderbook get requires a catalog quantity scale for {symbol}; \
                     wait_for_catalogs or hydrate the spot catalog first"
                ))
            })?;
        let client = OrderbookServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_public(client.get_order_book(req))
            .await?
            .into_owned();
        orderbook_from_proto(&resp, symbol, depth, quantity_scale)
    }

    /// Subscribe to the raw orderbook deltas of one published stream depth,
    /// [`crate::codecs::decode::ORDERBOOK_STREAM_DEPTHS`]; any other depth is a
    /// validation error (requires `realtime` feature).
    pub async fn subscribe_deltas(
        &self,
        symbol_id: u32,
        depth: Option<u32>,
    ) -> Result<crate::realtime::TypedSubscription<OrderBookDeltaUpdate>> {
        let stream_depth = require_published_stream_depth(depth.unwrap_or(50))?;
        let channel =
            format!("public:spot:orderbook:deltas:depth:{stream_depth}:{symbol_id}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::orderbook_delta_from_bytes)
            .await
    }

    /// Snapshot-then-stream orderbook merging (requires `realtime` feature).
    ///
    /// `depth` is any level count from 1 to the deepest published stream tier,
    /// 50 when `None`. The book rides the smallest tier covering it, snapshot
    /// included, and each view is trimmed to it; a depth no tier covers is a
    /// validation error before connecting.
    ///
    /// Each view's `ts` is the server time of the latest snapshot or delta
    /// applied to the book, `None` when that one carried none; a stale or
    /// out-of-sequence delta, which is not applied, leaves it unchanged.
    pub async fn create_subscription(
        &self,
        opts: CreateSubscriptionOptions,
    ) -> Result<crate::orderbook::Subscription> {
        use crate::orderbook::{
            BookSide, apply_delta, build_orderbook_data, levels_from_orderbook_side,
            parse_bucket_ticks,
        };
        use crate::realtime::{SnapshotThenStream, SnapshotThenStreamConfig};
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        use tokio::sync::mpsc;

        let mut symbol = opts.symbol;
        let depth = opts.depth.unwrap_or(50);
        let stream_depth = orderbook_stream_tier(depth)?;
        let bucket_ticks = Arc::new(Mutex::new(parse_bucket_ticks(
            opts.bucket.as_deref().unwrap_or(""),
        )?));
        let mut resolved_symbol_id = opts.symbol_id;
        if self.ctx.hydrate_catalogs_enabled {
            // Levels are decoded with the pair's catalog quantity scale.
            self.ctx.wait_for_catalogs().await?;
            if resolved_symbol_id.is_none() {
                resolved_symbol_id = Some(self.ctx.catalogs.require_symbol_id(&symbol)?);
            }
        }
        let resolved_symbol_id =
            resolved_symbol_id.or_else(|| self.ctx.catalogs.symbol_id_for_symbol(&symbol));
        let Some(symbol_id) = resolved_symbol_id.filter(|id| *id != 0) else {
            return Err(Error::validation(format!(
                "symbol_id is required for orderbook subscriptions ({symbol:?})"
            )));
        };
        if let Some(catalog_id) = self.ctx.catalogs.symbol_id_for_symbol(&symbol)
            && catalog_id != symbol_id
        {
            return Err(Error::validation(format!(
                "orderbook symbol {symbol:?} does not match symbol_id {symbol_id}"
            )));
        }
        if symbol.trim().is_empty() {
            // Selected by symbol_id alone: the catalog names the pair, whose
            // quantity scale decodes the levels.
            symbol = self
                .ctx
                .catalogs
                .symbol_for_symbol_id(symbol_id)
                .ok_or_else(|| {
                    Error::validation(format!(
                        "orderbook subscription for symbol_id {symbol_id} needs that pair in \
                         the spot catalog; wait_for_catalogs or hydrate the spot catalog first"
                    ))
                })?;
        }
        let channel =
            format!("public:spot:orderbook:deltas:depth:{stream_depth}:{symbol_id}:proto");
        let quantity_scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(&symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "orderbook subscription requires a catalog quantity scale for {symbol}; \
                     wait_for_catalogs or hydrate the spot catalog first"
                ))
            })?;

        let state = Arc::new(Mutex::new(BookState {
            bids: BookSide::new(),
            asks: BookSide::new(),
            book_seq: 0,
            available: false,
            ts: None,
            awaiting_reset: false,
            last_view_unavailable: false,
            recheck_pending: false,
            recheck_delay: UNAVAILABLE_SNAPSHOT_INITIAL_DELAY,
            recheck_generation: 0,
        }));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error: Arc<Mutex<Option<crate::Error>>> = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<OrderbookData>(200);
        // Dropping this slot closes the consumer channel so recv() cannot hang
        // forever after close/error.
        let tx_slot: Arc<Mutex<Option<mpsc::Sender<OrderbookData>>>> =
            Arc::new(Mutex::new(Some(tx)));
        // Filled after stream construction so synchronous emit callbacks can
        // terminate the underlying socket on local queue overflow.
        let stream_slot: Arc<
            Mutex<Option<SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>>>,
        > = Arc::new(Mutex::new(None));

        let emit = {
            let state = state.clone();
            let bucket_ticks = bucket_ticks.clone();
            let closed = closed.clone();
            let last_error = last_error.clone();
            let tx_slot = tx_slot.clone();
            let stream_slot = stream_slot.clone();
            let symbol = symbol.clone();
            Arc::new(move || {
                if closed.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let (bids, asks, book_seq, available, ts) = {
                    let mut s = crate::realtime::lock_unpoisoned(&state);
                    // A book that missed an update is not current until the
                    // next reset, whatever its snapshot said.
                    let available = s.available && !s.awaiting_reset;
                    s.last_view_unavailable = !available;
                    (s.bids.clone(), s.asks.clone(), s.book_seq, available, s.ts)
                };
                let ticks = *crate::realtime::lock_unpoisoned(&bucket_ticks);
                let data = match build_orderbook_data(
                    &symbol,
                    depth,
                    book_seq,
                    &bids,
                    &asks,
                    ticks,
                    quantity_scale,
                    available,
                ) {
                    Ok(data) => OrderbookData { ts, ..data },
                    Err(err) => {
                        closed.store(true, std::sync::atomic::Ordering::SeqCst);
                        *crate::realtime::lock_unpoisoned(&last_error) = Some(err.clone());
                        let _ = crate::realtime::lock_unpoisoned(&tx_slot).take();
                        if let Some(stream) =
                            crate::realtime::lock_unpoisoned(&stream_slot).as_ref()
                        {
                            stream.fail(err);
                        }
                        return;
                    }
                };
                let Some(tx) = crate::realtime::lock_unpoisoned(&tx_slot).as_ref().cloned() else {
                    return;
                };
                if !crate::realtime::try_enqueue(
                    &tx,
                    data,
                    &closed,
                    &last_error,
                    "orderbook subscription queue full; consumer too slow",
                ) && let Some(err @ Error::QueueOverflow(_)) =
                    crate::realtime::lock_unpoisoned(&last_error).clone()
                {
                    let _ = crate::realtime::lock_unpoisoned(&tx_slot).take();
                    if let Some(stream) = crate::realtime::lock_unpoisoned(&stream_slot).as_ref() {
                        stream.fail(err);
                    }
                }
            }) as Arc<dyn Fn() + Send + Sync>
        };

        // Applies one delta to the book without emitting a view, and reports
        // whether the view changed. An update that does not continue the book
        // (a sequence gap, or a malformed update) leaves it out of sync: the
        // next view reports it unavailable and later updates are ignored until
        // a reset, the full view the publisher sends about every 30 seconds
        // and after a gap, or a new snapshot replaces it. A snapshot fetched
        // now would not line up with the stream either, so none is requested.
        let apply_one = {
            let state = state.clone();
            Arc::new(move |delta: OrderBookDeltaUpdate| {
                let mut s = crate::realtime::lock_unpoisoned(&state);
                if s.awaiting_reset && !delta.reset {
                    return false;
                }
                let BookState {
                    bids,
                    asks,
                    book_seq,
                    ..
                } = &mut *s;
                let (new_seq, out_of_sync) = apply_delta(bids, asks, *book_seq, &delta);
                let advanced = new_seq != *book_seq;
                *book_seq = new_seq;
                if advanced {
                    // Applied: the book is now this update's state, at its
                    // time. A stale or out-of-sequence update is not applied
                    // and leaves the book's time as it was.
                    s.ts = delta.ts;
                }
                if out_of_sync {
                    // The first gap of an episode changes the view: it is no
                    // longer current.
                    let first = !s.awaiting_reset;
                    s.awaiting_reset = true;
                    return first;
                }
                if delta.reset && advanced {
                    s.awaiting_reset = false;
                }
                true
            }) as Arc<dyn Fn(OrderBookDeltaUpdate) -> bool + Send + Sync>
        };
        // A live delta is one book state: one view.
        let handle_delta = {
            let apply_one = apply_one.clone();
            let emit = emit.clone();
            Arc::new(move |delta: OrderBookDeltaUpdate| {
                let applied = apply_one(delta);
                if applied {
                    emit();
                }
                applied
            }) as Arc<dyn Fn(OrderBookDeltaUpdate) -> bool + Send + Sync>
        };

        // An unavailable book's levels are unknown, not empty: ask again later,
        // with a paced delay, until it is available or the stream stops.
        let recheck_unavailable_snapshot = {
            let state = state.clone();
            let stream_slot = stream_slot.clone();
            Arc::new(move |available: bool| {
                let mut s = crate::realtime::lock_unpoisoned(&state);
                if available {
                    // A pending request for the older unavailable snapshot is obsolete.
                    s.recheck_delay = UNAVAILABLE_SNAPSHOT_INITIAL_DELAY;
                    s.recheck_pending = false;
                    s.recheck_generation = s.recheck_generation.wrapping_add(1);
                    return;
                }
                if s.recheck_pending {
                    return;
                }
                let Some(stream) = crate::realtime::lock_unpoisoned(&stream_slot)
                    .as_ref()
                    .cloned()
                else {
                    return;
                };
                s.recheck_pending = true;
                let delay = s.recheck_delay;
                s.recheck_delay = delay.saturating_mul(2).min(UNAVAILABLE_SNAPSHOT_MAX_DELAY);
                let generation = s.recheck_generation;
                drop(s);
                let state = state.clone();
                stream.request_refresh_after(delay, move || {
                    let mut s = crate::realtime::lock_unpoisoned(&state);
                    if s.recheck_generation != generation {
                        return false;
                    }
                    s.recheck_pending = false;
                    true
                });
            }) as Arc<dyn Fn(bool) + Send + Sync>
        };

        let publications = crate::orderbook::PublicationClock::new();
        let svc = self.clone();
        let fetch_symbol = symbol.clone();
        let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client: self.ctx.realtime.clone(),
            channel,
            decode: Arc::new(crate::codecs::decode::orderbook_delta_from_bytes),
            fetch_snapshot: Arc::new(move || {
                let svc = svc.clone();
                let symbol = fetch_symbol.clone();
                // At the tier, not the requested depth: deltas diff the tier's whole
                // view, and a level promoted into a trimmed view must already be known.
                Box::pin(async move { svc.get(&symbol, Some(stream_depth)).await })
            }),
            read_publication: {
                let publications = publications.clone();
                Arc::new(move |delta: OrderBookDeltaUpdate| {
                    publications.note();
                    vec![delta]
                })
            },
            apply_snapshot: {
                let state = state.clone();
                let apply_one = apply_one.clone();
                let emit = emit.clone();
                let stream_slot = stream_slot.clone();
                let last_error = last_error.clone();
                let recheck_unavailable_snapshot = recheck_unavailable_snapshot.clone();
                Arc::new(
                    move |snapshot: OrderbookData, buffered: Vec<OrderBookDeltaUpdate>| {
                        let parsed_seq = match snapshot.book_seq.parse::<u64>() {
                            Ok(seq) => seq,
                            Err(_) => {
                                // Fail toward refresh — never treat garbage as seq 0 silently.
                                *crate::realtime::lock_unpoisoned(&last_error) =
                                    Some(Error::realtime(
                                        "orderbook snapshot book_seq is not a valid u64".to_owned(),
                                    ));
                                if let Some(stream) =
                                    crate::realtime::lock_unpoisoned(&stream_slot).as_ref()
                                {
                                    stream.request_refresh();
                                }
                                return;
                            }
                        };
                        // A repeated unavailable answer adds no view.
                        let repeated = {
                            let mut s = crate::realtime::lock_unpoisoned(&state);
                            s.bids = levels_from_orderbook_side(&snapshot.bids);
                            s.asks = levels_from_orderbook_side(&snapshot.asks);
                            s.book_seq = parsed_seq;
                            s.available = snapshot.available;
                            s.ts = snapshot.ts;
                            s.awaiting_reset = false;
                            !snapshot.available && s.last_view_unavailable && buffered.is_empty()
                        };
                        // The snapshot and the deltas buffered behind it are one
                        // book state, so they yield one view: a view per delta
                        // would queue stale copies of it. A buffered update that
                        // does not continue the snapshot leaves that view
                        // unavailable until a reset, which may be buffered too.
                        for delta in buffered {
                            apply_one(delta);
                        }
                        if !repeated {
                            emit();
                        }
                        recheck_unavailable_snapshot(snapshot.available);
                    },
                )
            },
            apply_live_publications: {
                let handle_delta = handle_delta.clone();
                Arc::new(move |deltas: Vec<OrderBookDeltaUpdate>| {
                    for delta in deltas {
                        handle_delta(delta);
                    }
                })
            },
            max_buffered: 200,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });
        *crate::realtime::lock_unpoisoned(&stream_slot) = Some(stream.clone());

        let subscription = crate::orderbook::Subscription::new(
            rx,
            stream.clone(),
            closed,
            bucket_ticks,
            emit,
            last_error,
            tx_slot,
        )
        .with_publication_clock(publications);
        if let Err(err) = stream.start().await {
            subscription.close();
            return Err(err);
        }
        Ok(subscription)
    }
}

struct BookState {
    bids: crate::orderbook::BookSide,
    asks: crate::orderbook::BookSide,
    book_seq: u64,
    /// Whether the last snapshot was current; see
    /// [`OrderbookData::available`].
    available: bool,
    /// The server time of the latest snapshot or update applied to the book,
    /// `None` when that one carried none; see [`OrderbookData::ts`].
    ts: Option<std::time::SystemTime>,
    /// An update did not continue the book: it is out of sync until the next
    /// reset or snapshot.
    awaiting_reset: bool,
    /// Whether the last view emitted reported the book unavailable.
    last_view_unavailable: bool,
    /// A request for an unavailable snapshot is scheduled.
    recheck_pending: bool,
    /// Delay before the next request for an unavailable snapshot.
    recheck_delay: std::time::Duration,
    /// Invalidates a scheduled request once a newer snapshot is available.
    recheck_generation: u64,
}

/// A snapshot that came back unavailable is requested again after these delays:
/// the first delay doubles up to the limit, for as long as the stream runs.
const UNAVAILABLE_SNAPSHOT_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
const UNAVAILABLE_SNAPSHOT_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(8);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn after_trade_id_cannot_be_combined_with_time_or_page_filters() {
        let client = crate::Client::new(crate::Config {
            api_url: "http://127.0.0.1:1".into(),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(serde_json::json!({
                "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
            }))
            .unwrap();
        let base = GetTradesOpts {
            symbol: Some("BTC-USDT".into()),
            after_trade_id: Some(0),
            ..Default::default()
        };
        for opts in [
            GetTradesOpts {
                start: Some(1_700_000_000),
                ..base.clone()
            },
            GetTradesOpts {
                end: Some(1_700_000_000),
                ..base.clone()
            },
            GetTradesOpts {
                page_token: Some("next".into()),
                ..base.clone()
            },
        ] {
            let err = client.market_data.get_trades_with(opts).await.unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{err:?}");
            assert!(err.to_string().contains("after_trade_id"), "{err}");
        }
    }

    // POLY-9019: list and live rows carry the best bid and ask sizes at the
    // pair's base quantity scale, with its symbol.
    #[test]
    fn overview_rows_size_the_top_of_book_from_the_catalogs() {
        use buffa::Message as _;
        let catalogs = crate::catalogs::Manager::new();
        catalogs
            .hydrate_spot_config_json(serde_json::json!({
                "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
            }))
            .unwrap();
        let payload = crate::proto::marketoverview::v1::MarketOverviewBatch {
            markets: vec![crate::proto::marketoverview::v1::MarketOverview {
                symbol_id: 1,
                best_bid_ticks: 74_883_900_000_000,
                best_bid_qty_scaled: 6_166_686,
                best_ask_ticks: 74_921_360_000_000,
                best_ask_qty_scaled: 5_532_396,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        let batch = enriched_market_overview_batch(&catalogs, &payload).unwrap();
        let row = &batch.markets[0];
        for (qty, scaled) in [
            (&row.best_bid_qty, 6_166_686),
            (&row.best_ask_qty, 5_532_396),
        ] {
            let qty = qty.as_ref().expect("size is set");
            assert_eq!(qty.as_scaled(), scaled);
            assert_eq!(qty.scale(), Some(8));
            assert_eq!(qty.symbol(), Some("BTC-USDT"));
            assert_eq!(qty.symbol_id(), Some(1));
        }
        assert_eq!(row.symbol, "BTC-USDT");
    }

    #[test]
    fn candle_channel_accepts_every_published_timeframe() {
        for (alias, label) in [
            ("3m", "3m"),
            ("MIN_3", "3m"),
            ("2h", "2h"),
            ("HOUR_6", "6h"),
            ("8h", "8h"),
            ("DAY_3", "3d"),
        ] {
            assert_eq!(
                MarketDataService::candle_channel_timeframe(alias).unwrap(),
                label,
                "alias {alias}"
            );
        }
    }

    /// Exactly the documented labels and the enum names are accepted, the
    /// same set the Go and Python SDKs accept. Another case or spelling of a
    /// label (`"1M"` is neither one minute nor one month), an enum name in
    /// another case or without its underscore, the unspecified value, padding
    /// and side names are refused.
    #[test]
    fn candle_timeframes_accept_only_the_documented_set() {
        let accepted = [
            ("1s", Timeframe::Sec1, "1s"),
            ("SEC_1", Timeframe::Sec1, "1s"),
            ("1m", Timeframe::Min1, "1m"),
            ("MIN_1", Timeframe::Min1, "1m"),
            ("3m", Timeframe::Min3, "3m"),
            ("MIN_3", Timeframe::Min3, "3m"),
            ("5m", Timeframe::Min5, "5m"),
            ("MIN_5", Timeframe::Min5, "5m"),
            ("15m", Timeframe::Min15, "15m"),
            ("MIN_15", Timeframe::Min15, "15m"),
            ("30m", Timeframe::Min30, "30m"),
            ("MIN_30", Timeframe::Min30, "30m"),
            ("1h", Timeframe::Hour1, "1h"),
            ("HOUR_1", Timeframe::Hour1, "1h"),
            ("2h", Timeframe::Hour2, "2h"),
            ("HOUR_2", Timeframe::Hour2, "2h"),
            ("4h", Timeframe::Hour4, "4h"),
            ("HOUR_4", Timeframe::Hour4, "4h"),
            ("6h", Timeframe::Hour6, "6h"),
            ("HOUR_6", Timeframe::Hour6, "6h"),
            ("8h", Timeframe::Hour8, "8h"),
            ("HOUR_8", Timeframe::Hour8, "8h"),
            ("12h", Timeframe::Hour12, "12h"),
            ("HOUR_12", Timeframe::Hour12, "12h"),
            ("1d", Timeframe::Day1, "1d"),
            ("DAY_1", Timeframe::Day1, "1d"),
            ("3d", Timeframe::Day3, "3d"),
            ("DAY_3", Timeframe::Day3, "3d"),
            ("1w", Timeframe::Week1, "1w"),
            ("WEEK_1", Timeframe::Week1, "1w"),
            ("1mo", Timeframe::Month1, "1mo"),
            ("MONTH_1", Timeframe::Month1, "1mo"),
        ];
        assert_eq!(accepted.len(), CANDLE_TIMEFRAMES.len() * 2);
        for (label, timeframe, channel) in accepted {
            assert_eq!(parse_timeframe(label).unwrap(), timeframe, "{label}");
            assert_eq!(
                MarketDataService::candle_channel_timeframe(label).unwrap(),
                channel,
                "{label}"
            );
        }
        for label in [
            "1M",
            "1H",
            "1D",
            "1S",
            "1W",
            "1MO",
            "1Mo",
            "15M",
            "min_1",
            "min1",
            "MIN1",
            "Min_1",
            "month_1",
            "month1",
            "sec1",
            "TIMEFRAME_UNSPECIFIED",
            "BUY",
            "SELL",
            "buy",
            "sell",
            " 1m",
            "1m ",
            "",
            "1",
            "60",
            "1min",
            "1month",
        ] {
            assert!(
                matches!(parse_timeframe(label), Err(Error::Validation(_))),
                "{label:?} accepted"
            );
            assert!(
                MarketDataService::candle_channel_timeframe(label).is_err(),
                "{label:?} accepted"
            );
        }
    }

    /// Index price candles take every candle timeframe, in either spelling,
    /// except one second, and refuse what `parse_timeframe` refuses, an
    /// empty timeframe included.
    #[test]
    fn index_price_timeframes_are_the_candle_timeframes_but_one_second() {
        for (label, name, timeframe) in CANDLE_TIMEFRAMES {
            for spelling in [label, name] {
                let parsed = parse_index_price_timeframe(spelling);
                if timeframe == Timeframe::Sec1 {
                    assert!(
                        matches!(&parsed, Err(Error::Validation(msg)) if msg.contains("one second")),
                        "{spelling:?}: {parsed:?}"
                    );
                } else {
                    assert_eq!(parsed.unwrap(), timeframe, "{spelling:?}");
                }
            }
        }
        for spelling in ["", " 1h", "1H", "1M", "hour_1", "TIMEFRAME_UNSPECIFIED"] {
            assert!(
                matches!(
                    parse_index_price_timeframe(spelling),
                    Err(Error::Validation(_))
                ),
                "{spelling:?} accepted"
            );
        }
    }

    /// A pair whose catalog row has no reference price scale cannot have its
    /// index price candles decoded, so the call is refused before sending
    /// (the client's API URL refuses every connection).
    #[tokio::test]
    async fn index_price_candles_need_the_reference_price_scale() {
        let client = crate::Client::new(crate::Config {
            api_url: "http://127.0.0.1:1".into(),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(serde_json::json!({
                "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
            }))
            .unwrap();
        let err = client
            .market_data
            .get_index_price_candles("BTC-USDT", "1h", None)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Validation(msg) if msg.contains("reference_price_scale")),
            "{err:?}"
        );
    }

    #[test]
    fn candle_channel_normalizes_aliases_to_human_label() {
        for alias in ["1m", "MIN_1"] {
            assert_eq!(
                MarketDataService::candle_channel_timeframe(alias).unwrap(),
                "1m",
                "alias {alias}"
            );
        }
        assert_eq!(
            MarketDataService::candle_channel_timeframe("1h").unwrap(),
            "1h"
        );
    }

    #[test]
    fn current_candle_selects_first_newest_row() {
        let candle = |ts_sec| Candle {
            ts_sec,
            open: "1".into(),
            high: "1".into(),
            low: "1".into(),
            close: "1".into(),
            volume: "1".into(),
            quote_volume: String::new(),
            symbol_id: 1,
            timeframe: "1m".into(),
            is_closed: true,
        };
        let newest = newest_candle(CandlesResult {
            symbol_id: 1,
            timeframe: "1m".into(),
            candles: vec![candle(20), candle(10)],
            reference_candles: Vec::new(),
            next_page_token: String::new(),
        })
        .expect("current candle");
        assert_eq!(newest.ts_sec, 20);
    }

    #[tokio::test]
    async fn orderbook_close_unblocks_recv() {
        let (tx, rx) = mpsc::channel::<crate::models::OrderbookData>(2);
        let tx_slot = Arc::new(Mutex::new(Some(tx)));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(Mutex::new(None));
        let stream =
            crate::realtime::SnapshotThenStream::new(crate::realtime::SnapshotThenStreamConfig {
                client: crate::realtime::Client::new(
                    "wss://example.invalid",
                    "https://example.invalid",
                    None,
                    None,
                ),
                channel: "public:test".into(),
                decode: Arc::new(|_: &[u8]| {
                    Ok(crate::models::OrderBookDeltaUpdate {
                        symbol_id: 1,
                        book_seq_start: 1,
                        book_seq_end: 1,
                        reset: false,
                        bids: vec![],
                        asks: vec![],
                        ts: None,
                    })
                }),
                fetch_snapshot: Arc::new(|| {
                    Box::pin(async {
                        Ok(crate::models::OrderbookData {
                            symbol: "BTC-USDT".into(),
                            depth: 1,
                            book_seq: "1".into(),
                            book_epoch: "1".into(),
                            bids: vec![],
                            asks: vec![],
                            available: true,
                            ts: None,
                        })
                    })
                }),
                read_publication: Arc::new(|d| vec![d]),
                apply_snapshot: Arc::new(|_, _| {}),
                apply_live_publications: Arc::new(|_| {}),
                max_buffered: 10,
                on_reconnect: None,
                on_snapshot_refresh: None,
                on_error: None,
            });
        let mut sub = crate::orderbook::Subscription::new(
            rx,
            stream,
            closed,
            Arc::new(Mutex::new(0)),
            Arc::new(|| {}),
            last_error,
            tx_slot,
        );
        sub.close();
        let finished =
            tokio::time::timeout(std::time::Duration::from_secs(1), sub.updates().recv())
                .await
                .expect("recv must not hang after close");
        assert!(finished.is_none());
    }
    #[tokio::test]
    async fn ticket_8511_terminal_transport_unblocks_recv() {
        let (tx, rx) = mpsc::channel::<crate::models::OrderbookData>(2);
        let tx_slot = Arc::new(Mutex::new(Some(tx)));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(Mutex::new(None));
        let stream =
            crate::realtime::SnapshotThenStream::new(crate::realtime::SnapshotThenStreamConfig {
                client: crate::realtime::Client::new(
                    "wss://example.invalid",
                    "https://example.invalid",
                    None,
                    None,
                ),
                channel: "public:test".into(),
                decode: Arc::new(|_: &[u8]| {
                    Ok(crate::models::OrderBookDeltaUpdate {
                        symbol_id: 1,
                        book_seq_start: 1,
                        book_seq_end: 1,
                        reset: false,
                        bids: vec![],
                        asks: vec![],
                        ts: None,
                    })
                }),
                fetch_snapshot: Arc::new(|| {
                    Box::pin(async {
                        Ok(crate::models::OrderbookData {
                            symbol: "BTC-USDT".into(),
                            depth: 1,
                            book_seq: "1".into(),
                            book_epoch: "1".into(),
                            bids: vec![],
                            asks: vec![],
                            available: true,
                            ts: None,
                        })
                    })
                }),
                read_publication: Arc::new(|d| vec![d]),
                apply_snapshot: Arc::new(|_, _| {}),
                apply_live_publications: Arc::new(|_| {}),
                max_buffered: 10,
                on_reconnect: None,
                on_snapshot_refresh: None,
                on_error: None,
            });
        let mut sub = crate::orderbook::Subscription::new(
            rx,
            stream.clone(),
            closed,
            Arc::new(Mutex::new(0)),
            Arc::new(|| {}),
            last_error,
            tx_slot.clone(),
        );
        let errors = Arc::new(Mutex::new(Vec::new()));
        let reported = errors.clone();
        sub.set_on_error(move |error| reported.lock().unwrap().push(error));
        tx_slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .try_send(crate::models::OrderbookData {
                symbol: "BTC-USDT".into(),
                depth: 1,
                book_seq: "1".into(),
                book_epoch: String::new(),
                bids: vec![],
                asks: vec![],
                available: true,
                ts: None,
            })
            .unwrap();
        stream.fail(Error::transport("terminal transport failure"));
        assert_eq!(
            sub.updates()
                .recv()
                .await
                .expect("queued view must drain")
                .book_seq,
            "1"
        );
        let finished =
            tokio::time::timeout(std::time::Duration::from_secs(1), sub.updates().recv())
                .await
                .expect("recv must not hang after close");
        assert!(finished.is_none());
        assert!(matches!(sub.err(), Some(Error::Transport { .. })));
        assert_eq!(errors.lock().unwrap().len(), 1);
        assert_eq!(
            sub.err().unwrap().to_string(),
            errors.lock().unwrap()[0].to_string()
        );
    }
}
