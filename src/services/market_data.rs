use super::ServiceContext;
use super::unary;
use crate::codecs::decode::{
    candles_columns_from_proto, candles_from_proto, depth_enum_for_levels,
    market_overview_list_from_proto, market_trades_from_proto, orderbook_from_proto,
    spot_config_from_proto,
};
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::connect::marketoverview::v1::MarketOverviewServiceClient;
use crate::connect::orderbook::v1::OrderbookServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    Candle, CandlesResult, GetCandlesOpts, GetTradesOpts, MarketOverviewList, MarketTradesResult,
    OrderbookData, SpotConfig,
};
#[cfg(feature = "realtime")]
use crate::models::{MarketOverviewEntry, MarketTrade, OrderBookDeltaUpdate};
use crate::proto::marketdata::v1::{
    GetCandlesColumnsRequest, GetCandlesRequest, GetSpotConfigRequest, GetTradesRequest, Timeframe,
};
use crate::proto::marketoverview::v1::ListMarketOverviewRequest;
use crate::proto::orderbook::v1::GetOrderBookRequest;
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
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        )
    }

    fn resolve_symbol_id(
        &self,
        symbol: Option<&str>,
        symbol_id: Option<u32>,
        label: &str,
    ) -> Result<u32> {
        if let Some(id) = symbol_id.filter(|id| *id != 0) {
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

    /// Spot pair catalog (Go `SpotConfig` raw-map escape hatch).
    pub async fn get_spot_config(&self) -> Result<SpotConfig> {
        let resp = unary::await_public(
            self.client()
                .get_spot_config(GetSpotConfigRequest::default()),
        )
        .await?
        .into_owned();
        Ok(spot_config_from_proto(&resp))
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

    pub async fn get_trades_with(&self, opts: GetTradesOpts) -> Result<MarketTradesResult> {
        let symbol_id =
            self.resolve_symbol_id(opts.symbol.as_deref(), opts.symbol_id, "get_trades")?;
        let req = GetTradesRequest {
            symbol_id,
            limit: opts.limit.unwrap_or(0),
            start_time: Self::timestamp_field(opts.start),
            end_time: Self::timestamp_field(opts.end),
            page_token: opts.page_token.unwrap_or_default(),
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
        self.get_candles_with(GetCandlesOpts {
            symbol: Some(symbol.to_owned()),
            timeframe: interval.to_owned(),
            limit,
            ..Default::default()
        })
        .await
    }

    pub async fn get_candles_with(&self, opts: GetCandlesOpts) -> Result<CandlesResult> {
        let (req, volume_scale) = self.build_candles_request(&opts)?;
        let resp = unary::await_public(self.client().get_candles(req))
            .await?
            .into_owned();
        Ok(candles_from_proto(&resp, volume_scale))
    }

    /// Latest candle for a symbol/timeframe (limit=1, include_incomplete).
    pub async fn get_current_candle(&self, symbol: &str, timeframe: &str) -> Result<Candle> {
        let result = self
            .get_candles_with(GetCandlesOpts {
                symbol: Some(symbol.to_owned()),
                timeframe: timeframe.to_owned(),
                limit: Some(1),
                include_incomplete: true,
                ..Default::default()
            })
            .await?;
        Ok(result.candles.into_iter().next_back().unwrap_or(Candle {
            ts_sec: 0,
            open: String::new(),
            high: String::new(),
            low: String::new(),
            close: String::new(),
            volume: String::new(),
            symbol_id: 0,
            timeframe: timeframe.to_owned(),
        }))
    }

    /// Columnar OHLCV candles decoded into row-oriented [`CandlesResult`].
    pub async fn get_candles_columns(&self, opts: GetCandlesOpts) -> Result<CandlesResult> {
        let (base, volume_scale) = self.build_candles_request(&opts)?;
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
        Ok(candles_columns_from_proto(&resp, volume_scale))
    }

    fn build_candles_request(&self, opts: &GetCandlesOpts) -> Result<(GetCandlesRequest, u32)> {
        let symbol_id =
            self.resolve_symbol_id(opts.symbol.as_deref(), opts.symbol_id, "get_candles")?;
        let timeframe_label = if opts.timeframe.is_empty() {
            "1m"
        } else {
            opts.timeframe.as_str()
        };
        let timeframe = parse_timeframe(timeframe_label)?;
        let volume_scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol_id(symbol_id);
        let req = GetCandlesRequest {
            symbol_id,
            timeframe: timeframe.into(),
            limit: opts.limit.unwrap_or(0),
            start_time: Self::timestamp_field(opts.start),
            end_time: Self::timestamp_field(opts.end),
            include_incomplete: opts.include_incomplete,
            page_token: opts.page_token.clone().unwrap_or_default(),
            ..Default::default()
        };
        Ok((req, volume_scale))
    }

    /// Subscribe to public spot trades for a symbol (requires `realtime` feature + hydrated catalogs).
    #[cfg(feature = "realtime")]
    pub async fn subscribe_trades(
        &self,
        symbol: &str,
    ) -> Result<crate::realtime::TypedSubscription<MarketTrade>> {
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
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::market_trade_from_bytes)
            .await
    }

    /// Subscribe to public candle updates (requires `realtime` feature + hydrated catalogs).
    #[cfg(feature = "realtime")]
    pub async fn subscribe_candles(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<crate::realtime::TypedSubscription<Candle>> {
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        // Validate timeframe aliases match get_candles.
        let _ = parse_timeframe(timeframe)?;
        let volume_scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol_id(symbol_id);
        let channel = format!("public:spot:market:candles:{timeframe}:{symbol_id}:proto");
        let decode = crate::codecs::decode::candle_point_from_bytes(
            symbol_id,
            timeframe.to_owned(),
            volume_scale,
        );
        self.ctx.realtime.subscribe_proto(&channel, decode).await
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

/// Options for [`MarketOverviewService::list`].
#[derive(Debug, Clone, Default)]
pub struct ListMarketOverviewOptions {
    pub symbols: Option<Vec<String>>,
    pub limit: Option<u32>,
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
        let req = ListMarketOverviewRequest {
            symbols: opts.symbols.unwrap_or_default(),
            limit: opts.limit.unwrap_or_default(),
            include_sparklines: opts.include_sparklines,
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

    /// Subscribe to public market overview batches (requires `realtime` feature).
    #[cfg(feature = "realtime")]
    pub async fn subscribe(
        &self,
    ) -> Result<crate::realtime::TypedSubscription<MarketOverviewList>> {
        self.ctx
            .realtime
            .subscribe_proto(
                "public:spot:market_overview:updates:proto",
                crate::codecs::decode::market_overview_batch_from_bytes,
            )
            .await
    }

    /// Snapshot-then-stream merged overview rows (requires `realtime` feature).
    #[cfg(feature = "realtime")]
    pub async fn create_subscription(
        &self,
        opts: MarketOverviewCreateSubscriptionOptions,
    ) -> Result<crate::marketoverview::Subscription> {
        use crate::realtime::{SnapshotThenStream, SnapshotThenStreamConfig};
        use std::collections::HashMap;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        use tokio::sync::mpsc;

        let limit = opts.limit.filter(|n| *n > 0).unwrap_or(50);
        let symbols = opts.symbols.clone();
        let include_sparklines = opts.include_sparklines;
        let channel = "public:spot:market_overview:updates:proto".to_owned();

        let by_symbol_id: Arc<Mutex<HashMap<u32, MarketOverviewEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error: Arc<Mutex<Option<crate::Error>>> = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<Vec<MarketOverviewEntry>>(50);

        let emit = {
            let by_symbol_id = by_symbol_id.clone();
            let closed = closed.clone();
            let last_error = last_error.clone();
            let tx = tx.clone();
            Arc::new(move || {
                if closed.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let rows: Vec<MarketOverviewEntry> = by_symbol_id
                    .lock()
                    .expect("overview lock")
                    .values()
                    .cloned()
                    .collect();
                let _ = crate::realtime::try_enqueue(
                    &tx,
                    rows,
                    &closed,
                    &last_error,
                    "market overview subscription queue full; consumer too slow",
                );
            }) as Arc<dyn Fn() + Send + Sync>
        };

        let apply_rows = {
            let by_symbol_id = by_symbol_id.clone();
            Arc::new(move |rows: Vec<MarketOverviewEntry>| {
                let mut map = by_symbol_id.lock().expect("overview lock");
                for row in rows {
                    map.insert(row.symbol_id, row);
                }
            }) as Arc<dyn Fn(Vec<MarketOverviewEntry>) + Send + Sync>
        };

        let svc = self.clone();
        let fetch_symbols = symbols.clone();
        let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client: self.ctx.realtime.clone(),
            channel,
            decode: Arc::new(crate::codecs::decode::market_overview_batch_from_bytes),
            fetch_snapshot: Arc::new(move || {
                let svc = svc.clone();
                let symbols = fetch_symbols.clone();
                Box::pin(async move {
                    svc.list(ListMarketOverviewOptions {
                        symbols,
                        limit: Some(limit),
                        include_sparklines,
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
                        by_symbol_id.lock().expect("overview lock").clear();
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
        });

        let subscription =
            crate::marketoverview::Subscription::new(rx, stream.clone(), closed, last_error);
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

    /// Subscribe to public orderbook delta updates (requires `realtime` feature).
    #[cfg(feature = "realtime")]
    pub async fn subscribe_deltas(
        &self,
        symbol_id: u32,
        depth: Option<u32>,
    ) -> Result<crate::realtime::TypedSubscription<OrderBookDeltaUpdate>> {
        let ws_depth = depth.unwrap_or(50).clamp(1, 500);
        let channel = format!("public:spot:orderbook:deltas:depth:{ws_depth}:{symbol_id}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::orderbook_delta_from_bytes)
            .await
    }

    /// Snapshot-then-stream orderbook merging (requires `realtime` feature).
    #[cfg(feature = "realtime")]
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

        let symbol = opts.symbol;
        let depth = opts.depth.unwrap_or(50);
        let ws_depth = depth.clamp(1, 500);
        let resolved_symbol_id = opts
            .symbol_id
            .or_else(|| self.ctx.catalogs.symbol_id_for_symbol(&symbol));
        let Some(symbol_id) = resolved_symbol_id.filter(|id| *id != 0) else {
            return Err(Error::validation(format!(
                "symbol_id is required for orderbook subscriptions ({symbol:?})"
            )));
        };
        let channel = format!("public:spot:orderbook:deltas:depth:{ws_depth}:{symbol_id}:proto");
        let quantity_scale = self.ctx.catalogs.base_quantity_scale_for_symbol(&symbol);
        let bucket_ticks = Arc::new(Mutex::new(parse_bucket_ticks(
            opts.bucket.as_deref().unwrap_or(""),
        )));

        let state = Arc::new(Mutex::new(BookState {
            bids: BookSide::new(),
            asks: BookSide::new(),
            book_seq: 0,
        }));
        let closed = Arc::new(AtomicBool::new(false));
        let last_error: Arc<Mutex<Option<crate::Error>>> = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<OrderbookData>(200);

        let emit = {
            let state = state.clone();
            let bucket_ticks = bucket_ticks.clone();
            let closed = closed.clone();
            let last_error = last_error.clone();
            let tx = tx.clone();
            let symbol = symbol.clone();
            Arc::new(move || {
                if closed.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let (bids, asks, book_seq) = {
                    let s = state.lock().expect("book lock");
                    (s.bids.clone(), s.asks.clone(), s.book_seq)
                };
                let ticks = *bucket_ticks.lock().expect("bucket lock");
                let data = build_orderbook_data(
                    &symbol,
                    ws_depth,
                    book_seq,
                    &bids,
                    &asks,
                    ticks,
                    quantity_scale,
                );
                let _ = crate::realtime::try_enqueue(
                    &tx,
                    data,
                    &closed,
                    &last_error,
                    "orderbook subscription queue full; consumer too slow",
                );
            }) as Arc<dyn Fn() + Send + Sync>
        };

        // Placeholder stream handle filled after construction for gap refresh.
        let stream_slot: Arc<
            Mutex<Option<SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>>>,
        > = Arc::new(Mutex::new(None));

        let handle_delta = {
            let state = state.clone();
            let emit = emit.clone();
            let stream_slot = stream_slot.clone();
            Arc::new(move |delta: OrderBookDeltaUpdate| {
                let needs_refresh = {
                    let mut s = state.lock().expect("book lock");
                    let BookState {
                        bids,
                        asks,
                        book_seq,
                    } = &mut *s;
                    let (new_seq, needs_refresh) = apply_delta(bids, asks, *book_seq, &delta);
                    *book_seq = new_seq;
                    needs_refresh
                };
                if needs_refresh {
                    if let Some(stream) = stream_slot.lock().expect("stream slot").as_ref() {
                        stream.request_refresh();
                    }
                    return;
                }
                emit();
            }) as Arc<dyn Fn(OrderBookDeltaUpdate) + Send + Sync>
        };

        let svc = self.clone();
        let fetch_symbol = symbol.clone();
        let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client: self.ctx.realtime.clone(),
            channel,
            decode: Arc::new(crate::codecs::decode::orderbook_delta_from_bytes),
            fetch_snapshot: {
                let state = state.clone();
                Arc::new(move || {
                    let svc = svc.clone();
                    let symbol = fetch_symbol.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        let snap = svc.get(&symbol, Some(ws_depth)).await?;
                        let mut s = state.lock().expect("book lock");
                        s.bids = levels_from_orderbook_side(&snap.bids);
                        s.asks = levels_from_orderbook_side(&snap.asks);
                        s.book_seq = snap.book_seq.parse().unwrap_or(0);
                        Ok(snap)
                    })
                })
            },
            read_publication: Arc::new(|delta: OrderBookDeltaUpdate| vec![delta]),
            apply_snapshot: {
                let handle_delta = handle_delta.clone();
                let emit = emit.clone();
                Arc::new(
                    move |_snapshot: OrderbookData, buffered: Vec<OrderBookDeltaUpdate>| {
                        for delta in buffered {
                            handle_delta(delta);
                        }
                        emit();
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
        });
        *stream_slot.lock().expect("stream slot") = Some(stream.clone());

        let subscription = crate::orderbook::Subscription::new(
            rx,
            stream.clone(),
            closed,
            bucket_ticks,
            emit,
            last_error,
        );
        if let Err(err) = stream.start().await {
            subscription.close();
            return Err(err);
        }
        Ok(subscription)
    }
}

#[cfg(feature = "realtime")]
struct BookState {
    bids: crate::orderbook::BookSide,
    asks: crate::orderbook::BookSide,
    book_seq: i64,
}
