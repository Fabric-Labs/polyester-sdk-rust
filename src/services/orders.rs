use super::ServiceContext;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    batch_cancel_from_proto, batch_create_from_proto, batch_modify_from_proto,
    cancel_all_after_from_proto, cancel_all_from_proto, get_order_from_proto,
    modify_order_from_proto, order_mutation_from_cancel, order_mutation_from_create,
    orders_list_from_history, orders_list_from_open, user_trades_list_from_proto,
};
use crate::codecs::scalars::id_to_u64;
use crate::connect::orders::v1::{OrdersReadServiceClient, OrdersServiceClient};
use crate::errors::{Error, Result};
use crate::models::{
    AttachedRisk, BatchCancelItem, BatchCancelOrdersResult, BatchCreateOrdersResult,
    BatchModifyItem, BatchModifyOrdersResult, CancelAllAfterResult, CancelAllOpts,
    CancelAllOrdersResult, CancelOrderParams, CreateOrderParams, CreateOrderType, CreateSide,
    CreateTimeInForce, GetOrderOpts, GetOrderResult, ListOpenOrdersOpts, ListOrderHistoryOpts,
    MaxSlippage, ModifyOrderParams, ModifyOrderResult, Order, OrderMutationResult, OrdersList,
    RiskLeg, TrailingDistance, TrailingStop, UserTrade, UserTradesList,
};
use crate::proto::orders::v1::{
    BatchCancelItem as ProtoBatchCancelItem, BatchCancelOrdersRequest, BatchCreateOrdersRequest,
    BatchModifyItem as ProtoBatchModifyItem, BatchModifyOrdersRequest, CancelAllAfterRequest,
    CancelAllOrdersRequest, CancelOrderRequest, CreateOrderRequest, GetOpenOrdersRequest,
    GetOrderHistoryRequest, GetOrderRequest, GetUserTradesRequest, LimitFok, LimitGtc, LimitIoc,
    MarketIoc, ModifyBehavior, ModifyOrderRequest, OrderIntent, RiskExecution, RiskLimitGtc,
    RiskPolicy, Side, StopLossPolicy, TakeProfitPolicy, TrailingStopPolicy, batch_modify_item,
    cancel_order_request, get_order_request, modify_order_request, order_intent, risk_execution,
    risk_policy, trailing_stop_policy,
};
use crate::types::{Price, Quantity, resolve_price_ticks, resolve_qty_scaled};

#[derive(Clone)]
pub struct OrdersService {
    ctx: ServiceContext,
}

impl OrdersService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn write_client(&self) -> OrdersServiceClient<crate::transport::SharedTransport> {
        OrdersServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    fn read_client(&self) -> OrdersReadServiceClient<crate::transport::SharedTransport> {
        OrdersReadServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    pub async fn list_open(&self, subaccount_id: Option<u64>) -> Result<OrdersList> {
        self.list_open_with(ListOpenOrdersOpts {
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn list_open_with(&self, opts: ListOpenOrdersOpts) -> Result<OrdersList> {
        let req = GetOpenOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, opts.subaccount_id)?,
            page_token: opts.page_token.unwrap_or_default(),
            limit: opts.limit,
            include_attached_risk: Some(opts.include_attached_risk),
            include_attached_risk_state: Some(opts.include_attached_risk_state),
            ..Default::default()
        };
        let client = self.read_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetOpenOrders",
            req,
            |req, opts| client.get_open_orders_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(orders_list_from_open(&resp))
    }

    pub async fn list_history(
        &self,
        subaccount_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<OrdersList> {
        self.list_history_with(ListOrderHistoryOpts {
            subaccount_id,
            limit,
            ..Default::default()
        })
        .await
    }

    pub async fn list_history_with(&self, opts: ListOrderHistoryOpts) -> Result<OrdersList> {
        let mut symbol_ids = Vec::new();
        if let Some(sid) = opts.symbol_id {
            symbol_ids.push(sid);
        } else if let Some(ref symbol) = opts.symbol {
            let resolved = self
                .ctx
                .catalogs
                .symbol_id_for_symbol(symbol)
                .ok_or_else(|| {
                    Error::validation(format!(
                        "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                    ))
                })?;
            symbol_ids.push(resolved);
        }
        let req = GetOrderHistoryRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, opts.subaccount_id)?,
            symbol_id: symbol_ids,
            page_token: opts.page_token.unwrap_or_default(),
            limit: opts.limit,
            include_attached_risk: Some(opts.include_attached_risk),
            include_attached_risk_state: Some(opts.include_attached_risk_state),
            ..Default::default()
        };
        let client = self.read_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetOrderHistory",
            req,
            |req, opts| client.get_order_history_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(orders_list_from_history(&resp))
    }

    pub async fn get(
        &self,
        client_order_id: Option<&str>,
        order_id: Option<&str>,
        subaccount_id: Option<u64>,
    ) -> Result<GetOrderResult> {
        self.get_with(GetOrderOpts {
            client_order_id: client_order_id.map(|s| s.to_owned()),
            order_id: order_id.map(|s| s.to_owned()),
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn get_with(&self, opts: GetOrderOpts) -> Result<GetOrderResult> {
        let key = if let Some(cid) = opts.client_order_id.as_deref().filter(|s| !s.is_empty()) {
            Some(get_order_request::Key::ClientOrderId(cid.to_owned()))
        } else if let Some(oid) = opts.order_id.as_deref().filter(|s| !s.is_empty()) {
            Some(get_order_request::Key::OrderId(id_to_u64(oid, "order_id")?))
        } else {
            return Err(Error::validation(
                "orders.get requires client_order_id or order_id",
            ));
        };
        let req = GetOrderRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, opts.subaccount_id)?,
            key,
            include_attached_risk: Some(opts.include_attached_risk),
            include_attached_risk_state: Some(opts.include_attached_risk_state),
            ..Default::default()
        };
        let client = self.read_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetOrder",
            req,
            |req, opts| client.get_order_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(get_order_from_proto(&resp))
    }

    /// Build the transport-independent [`OrderIntent`] shared by single and batch
    /// create. The flat public params (`order_type`/`time_in_force`/`post_only`)
    /// are mapped onto the appropriate execution variant.
    fn order_intent_from_params(&self, params: &CreateOrderParams) -> Result<OrderIntent> {
        let scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(&params.symbol);
        let qty = resolve_qty_scaled(
            &params.quantity,
            scale,
            Some(&params.symbol),
            self.ctx.catalogs.symbol_id_for_symbol(&params.symbol),
        )?;
        let mut intent = OrderIntent {
            symbol: params.symbol.clone(),
            qty_scaled: qty,
            side: match params.side {
                CreateSide::Buy => Side::Buy.into(),
                CreateSide::Sell => Side::Sell.into(),
            },
            ..Default::default()
        };
        if let Some(id) = params.client_order_id.as_ref() {
            intent.client_order_id = id.clone();
        }
        let post_only = params.post_only.unwrap_or(false);
        intent.execution = Some(match params.order_type {
            CreateOrderType::Market => {
                if post_only {
                    return Err(Error::validation(
                        "post_only is not supported for market orders",
                    ));
                }
                let mut market = MarketIoc::default();
                if let Some(ref_price) = params.market_client_ref_price.as_ref() {
                    market.client_ref_price_ticks =
                        resolve_price_ticks(ref_price, Some(&params.symbol))?;
                }
                order_intent::Execution::MarketIoc(Box::new(market))
            }
            CreateOrderType::Limit => {
                let price = params.price.as_ref().ok_or_else(|| {
                    Error::validation(
                        "price is required for limit orders (use Price::from_decimal or Price::from_ticks)",
                    )
                })?;
                let price_ticks = resolve_price_ticks(price, Some(&params.symbol))?;
                match params.time_in_force {
                    Some(CreateTimeInForce::Ioc) => {
                        if post_only {
                            return Err(Error::validation(
                                "post_only is not supported for ioc limit orders",
                            ));
                        }
                        order_intent::Execution::LimitIoc(Box::new(LimitIoc {
                            price_ticks,
                            ..Default::default()
                        }))
                    }
                    Some(CreateTimeInForce::Fok) => {
                        if post_only {
                            return Err(Error::validation(
                                "post_only is not supported for fok limit orders",
                            ));
                        }
                        order_intent::Execution::LimitFok(Box::new(LimitFok {
                            price_ticks,
                            ..Default::default()
                        }))
                    }
                    // gtc or unspecified
                    _ => order_intent::Execution::LimitGtc(Box::new(LimitGtc {
                        price_ticks,
                        post_only,
                        ..Default::default()
                    })),
                }
            }
        });
        if let Some(risk) = params.attached_risk.as_ref() {
            *intent.attached_risk.get_or_insert_default() =
                Self::encode_attached_risk(risk, Some(&params.symbol))?;
        }
        Ok(intent)
    }

    fn encode_create_params(&self, params: &CreateOrderParams) -> Result<CreateOrderRequest> {
        let order = self.order_intent_from_params(params)?;
        let mut req = CreateOrderRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            ..Default::default()
        };
        *req.order.get_or_insert_default() = order;
        Ok(req)
    }

    /// Map the flat public [`RiskLeg`] (`order_type`/`limit_price`) onto a child
    /// [`RiskExecution`] variant. `trigger_price_source` is no longer part of the
    /// policy wire and is ignored.
    fn encode_risk_child(leg: &RiskLeg, symbol: Option<&str>) -> Result<RiskExecution> {
        let child_ty = leg.order_type.unwrap_or(CreateOrderType::Market);
        let execution = match (child_ty, leg.limit_price.as_ref()) {
            (CreateOrderType::Market, None) => risk_execution::Execution::MarketIoc(Box::default()),
            (CreateOrderType::Market, Some(_)) => {
                return Err(Error::validation(
                    "attached_risk MARKET child must not set limit_price",
                ));
            }
            (CreateOrderType::Limit, Some(price)) => {
                risk_execution::Execution::LimitGtc(Box::new(RiskLimitGtc {
                    price_ticks: resolve_price_ticks(price, symbol)?,
                    ..Default::default()
                }))
            }
            (CreateOrderType::Limit, None) => {
                return Err(Error::validation(
                    "attached_risk LIMIT child requires limit_price",
                ));
            }
        };
        Ok(RiskExecution {
            execution: Some(execution),
            ..Default::default()
        })
    }

    fn encode_take_profit(leg: &RiskLeg, symbol: Option<&str>) -> Result<TakeProfitPolicy> {
        let mut policy = TakeProfitPolicy {
            trigger_price_ticks: resolve_price_ticks(&leg.trigger_price, symbol)?,
            ..Default::default()
        };
        *policy.child.get_or_insert_default() = Self::encode_risk_child(leg, symbol)?;
        Ok(policy)
    }

    fn encode_stop_loss(leg: &RiskLeg, symbol: Option<&str>) -> Result<StopLossPolicy> {
        let mut policy = StopLossPolicy {
            trigger_price_ticks: resolve_price_ticks(&leg.trigger_price, symbol)?,
            ..Default::default()
        };
        *policy.child.get_or_insert_default() = Self::encode_risk_child(leg, symbol)?;
        Ok(policy)
    }

    fn encode_trailing_stop(
        stop: &TrailingStop,
        symbol: Option<&str>,
    ) -> Result<TrailingStopPolicy> {
        // `trigger_price_source`/`order_type` were dropped from the trailing-stop
        // policy wire; the child is an implicit market execution.
        let mut proto = TrailingStopPolicy::default();
        if let Some(activation) = stop.activation_price.as_ref() {
            proto.activation_price_ticks = resolve_price_ticks(activation, symbol)?;
        }
        proto.trailing_distance = Some(match stop.distance {
            TrailingDistance::Ticks(v) => {
                trailing_stop_policy::TrailingDistance::TrailingDistanceTicks(v)
            }
            TrailingDistance::Bps(v) => {
                trailing_stop_policy::TrailingDistance::TrailingDistanceBps(v)
            }
        });
        if let Some(slip) = stop.max_slippage {
            proto.max_slippage = Some(match slip {
                MaxSlippage::Ticks(v) => trailing_stop_policy::MaxSlippage::MaxSlippageTicks(v),
                MaxSlippage::Bps(v) => trailing_stop_policy::MaxSlippage::MaxSlippageBps(v),
            });
        }
        Ok(proto)
    }

    fn encode_attached_risk(risk: &AttachedRisk, symbol: Option<&str>) -> Result<RiskPolicy> {
        if risk.stop_loss.is_some() && risk.trailing_stop.is_some() {
            return Err(Error::validation(
                "attached_risk allows at most one of stop_loss or trailing_stop",
            ));
        }
        if risk.take_profit.is_none() && risk.stop_loss.is_none() && risk.trailing_stop.is_none() {
            return Err(Error::validation(
                "attached_risk requires take_profit and/or a stop leg",
            ));
        }
        let mut proto = RiskPolicy {
            oco: risk.oco,
            ..Default::default()
        };
        if let Some(tp) = risk.take_profit.as_ref() {
            *proto.take_profit.get_or_insert_default() = Self::encode_take_profit(tp, symbol)?;
        }
        if let Some(sl) = risk.stop_loss.as_ref() {
            proto.stop_leg = Some(risk_policy::StopLeg::StopLoss(Box::new(
                Self::encode_stop_loss(sl, symbol)?,
            )));
        } else if let Some(ts) = risk.trailing_stop.as_ref() {
            proto.stop_leg = Some(risk_policy::StopLeg::TrailingStop(Box::new(
                Self::encode_trailing_stop(ts, symbol)?,
            )));
        }
        Ok(proto)
    }

    fn request_id(prefix: &str) -> String {
        format!(
            "{prefix}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }

    fn modify_behavior(label: &str) -> Result<ModifyBehavior> {
        match label.to_ascii_lowercase().as_str() {
            "amend_or_replace" => Ok(ModifyBehavior::AmendOrReplace),
            "amend_only" => Ok(ModifyBehavior::AmendOnly),
            "replace_only" => Ok(ModifyBehavior::ReplaceOnly),
            _ => Err(Error::validation(
                "behavior must be amend_or_replace, amend_only, or replace_only",
            )),
        }
    }

    fn encode_modify_params(&self, params: ModifyOrderParams) -> Result<ModifyOrderRequest> {
        let has_order = params.order_id.as_ref().is_some_and(|s| !s.is_empty());
        let has_client = params
            .client_order_id
            .as_ref()
            .is_some_and(|s| !s.is_empty());
        if has_order == has_client {
            return Err(Error::validation(
                "modify requires exactly one of order_id or client_order_id",
            ));
        }
        if params.new_price.is_none()
            && params.new_qty.is_none()
            && params.new_attached_risk.is_none()
        {
            return Err(Error::validation(
                "modify requires new_price, new_qty, and/or new_attached_risk",
            ));
        }
        let scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(&params.symbol);
        let mut req = ModifyOrderRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            request_id: params.request_id.unwrap_or_else(|| Self::request_id("mod")),
            ..Default::default()
        };
        if has_order {
            req.key = Some(modify_order_request::Key::OrderId(id_to_u64(
                params.order_id.as_deref().unwrap(),
                "order_id",
            )?));
        } else {
            req.key = Some(modify_order_request::Key::ClientOrderId(
                params.client_order_id.unwrap_or_default(),
            ));
        }
        if let Some(price) = params.new_price.as_ref() {
            req.new_price_ticks = Some(resolve_price_ticks(price, Some(&params.symbol))?);
        }
        if let Some(qty) = params.new_qty.as_ref() {
            req.new_qty_scaled = Some(resolve_qty_scaled(
                qty,
                scale,
                Some(&params.symbol),
                self.ctx.catalogs.symbol_id_for_symbol(&params.symbol),
            )?);
        }
        if let Some(risk) = params.new_attached_risk.as_ref() {
            *req.new_attached_risk.get_or_insert_default() =
                Self::encode_attached_risk(risk, Some(&params.symbol))?;
        }
        if let Some(behavior) = params.behavior.as_deref() {
            req.behavior = Self::modify_behavior(behavior)?.into();
        }
        if let Some(ncid) = params.new_client_order_id {
            req.new_client_order_id = ncid;
        }
        Ok(req)
    }

    /// Place an order. Quantity and price must be `Quantity` / `Price` wrappers.
    pub async fn create(&self, params: CreateOrderParams) -> Result<OrderMutationResult> {
        let req = self.encode_create_params(&params)?;
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CreateOrder",
            req,
            |req, opts| client.create_order_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(order_mutation_from_create(&resp))
    }

    pub async fn batch_create(
        &self,
        items: Vec<CreateOrderParams>,
        subaccount_id: Option<u64>,
        request_id: Option<String>,
    ) -> Result<BatchCreateOrdersResult> {
        if items.is_empty() {
            return Err(Error::validation("batch_create requires at least one item"));
        }
        let mut encoded = Vec::with_capacity(items.len());
        for item in &items {
            encoded.push(self.order_intent_from_params(item)?);
        }
        let req = BatchCreateOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            request_id: request_id.unwrap_or_else(|| Self::request_id("batch-create")),
            items: encoded,
            ..Default::default()
        };
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/BatchCreateOrders",
            req,
            |req, opts| client.batch_create_orders_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(batch_create_from_proto(&resp))
    }

    pub async fn batch_cancel(
        &self,
        items: Vec<BatchCancelItem>,
        subaccount_id: Option<u64>,
        request_id: Option<String>,
    ) -> Result<BatchCancelOrdersResult> {
        if items.is_empty() {
            return Err(Error::validation("batch_cancel requires at least one item"));
        }
        let mut proto_items = Vec::with_capacity(items.len());
        for item in items {
            let has_order = item.order_id.as_ref().is_some_and(|s| !s.is_empty());
            let has_client = item.client_order_id.as_ref().is_some_and(|s| !s.is_empty());
            if has_order == has_client {
                return Err(Error::validation(
                    "each batch cancel item requires exactly one of order_id or client_order_id",
                ));
            }
            let mut proto = ProtoBatchCancelItem::default();
            if has_order {
                proto.order_id = id_to_u64(item.order_id.as_deref().unwrap(), "order_id")?;
            }
            if has_client {
                proto.client_order_id = item.client_order_id.unwrap_or_default();
            }
            if let Some(sid) = item.symbol_id {
                proto.symbol_id = sid;
            }
            proto_items.push(proto);
        }
        let req = BatchCancelOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            request_id: request_id.unwrap_or_else(|| Self::request_id("batch-cancel")),
            items: proto_items,
            ..Default::default()
        };
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/BatchCancelOrders",
            req,
            |req, opts| client.batch_cancel_orders_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(batch_cancel_from_proto(&resp))
    }

    pub async fn batch_modify(
        &self,
        items: Vec<BatchModifyItem>,
        symbol: Option<&str>,
        subaccount_id: Option<u64>,
        request_id: Option<String>,
        behavior_default: Option<&str>,
        allow_partial: bool,
    ) -> Result<BatchModifyOrdersResult> {
        if items.is_empty() {
            return Err(Error::validation("batch_modify requires at least one item"));
        }
        let scale_symbol = symbol.unwrap_or("");
        let scale = Self::resolve_batch_modify_scale(&self.ctx.catalogs, scale_symbol, &items)?;
        let mut proto_items = Vec::with_capacity(items.len());
        for item in items {
            let has_order = item.order_id.as_ref().is_some_and(|s| !s.is_empty());
            let has_client = item.client_order_id.as_ref().is_some_and(|s| !s.is_empty());
            if has_order == has_client {
                return Err(Error::validation(
                    "each batch item requires exactly one of order_id or client_order_id",
                ));
            }
            if item.new_price.is_none()
                && item.new_qty.is_none()
                && item.new_attached_risk.is_none()
            {
                return Err(Error::validation(
                    "each batch item requires new_price, new_qty, and/or new_attached_risk",
                ));
            }
            let mut proto = ProtoBatchModifyItem::default();
            if has_order {
                proto.key = Some(batch_modify_item::Key::OrderId(id_to_u64(
                    item.order_id.as_deref().unwrap(),
                    "order_id",
                )?));
            } else {
                proto.key = Some(batch_modify_item::Key::ClientOrderId(
                    item.client_order_id.unwrap_or_default(),
                ));
            }
            if let Some(price) = item.new_price.as_ref() {
                proto.new_price_ticks = Some(resolve_price_ticks(price, symbol)?);
            }
            if let Some(qty) = item.new_qty.as_ref() {
                proto.new_qty_scaled = Some(resolve_qty_scaled(
                    qty,
                    scale,
                    symbol,
                    symbol.and_then(|s| self.ctx.catalogs.symbol_id_for_symbol(s)),
                )?);
            }
            if let Some(risk) = item.new_attached_risk.as_ref() {
                *proto.new_attached_risk.get_or_insert_default() =
                    Self::encode_attached_risk(risk, symbol)?;
            }
            if let Some(behavior) = item.behavior.as_deref() {
                proto.behavior = Self::modify_behavior(behavior)?.into();
            }
            if let Some(ncid) = item.new_client_order_id {
                proto.new_client_order_id = ncid;
            }
            proto_items.push(proto);
        }
        let mut req = BatchModifyOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            request_id: request_id.unwrap_or_else(|| Self::request_id("batch-mod")),
            items: proto_items,
            allow_partial,
            ..Default::default()
        };
        if let Some(behavior) = behavior_default {
            req.behavior_default = Self::modify_behavior(behavior)?.into();
        }
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/BatchModifyOrders",
            req,
            |req, opts| client.batch_modify_orders_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(batch_modify_from_proto(&resp))
    }

    pub async fn cancel_all_after(
        &self,
        timeout_sec: u32,
        symbol: Option<&str>,
        subaccount_id: Option<u64>,
        request_id: Option<String>,
    ) -> Result<CancelAllAfterResult> {
        let req = CancelAllAfterRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            timeout_sec,
            symbol: symbol.unwrap_or("").to_owned(),
            request_id: request_id.unwrap_or_else(|| Self::request_id("cancel-after")),
            ..Default::default()
        };
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CancelAllAfter",
            req,
            |req, opts| client.cancel_all_after_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(cancel_all_after_from_proto(&resp))
    }

    pub async fn cancel(&self, req: CancelOrderRequest) -> Result<OrderMutationResult> {
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CancelOrder",
            req,
            |req, opts| client.cancel_order_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(order_mutation_from_cancel(&resp))
    }

    pub async fn cancel_with(&self, params: CancelOrderParams) -> Result<OrderMutationResult> {
        let has_order = params.order_id.as_ref().is_some_and(|s| !s.is_empty());
        let has_client = params
            .client_order_id
            .as_ref()
            .is_some_and(|s| !s.is_empty());
        if has_order == has_client {
            return Err(Error::validation(
                "cancel requires exactly one of order_id or client_order_id",
            ));
        }
        let symbol_id = if let Some(sid) = params.symbol_id {
            sid
        } else {
            params
                .symbol
                .as_deref()
                .and_then(|s| self.ctx.catalogs.symbol_id_for_symbol(s))
                .unwrap_or(0)
        };
        let key = if has_order {
            Some(cancel_order_request::Key::OrderId(id_to_u64(
                params.order_id.as_deref().unwrap(),
                "order_id",
            )?))
        } else {
            Some(cancel_order_request::Key::ClientOrderId(
                params.client_order_id.unwrap_or_default(),
            ))
        };
        let req = CancelOrderRequest {
            symbol_id,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            key,
            ..Default::default()
        };
        self.cancel(req).await
    }

    pub async fn cancel_by_client_order_id(
        &self,
        client_order_id: &str,
        symbol: Option<&str>,
        subaccount_id: Option<u64>,
    ) -> Result<OrderMutationResult> {
        self.cancel_with(CancelOrderParams {
            client_order_id: Some(client_order_id.to_owned()),
            symbol: symbol.map(|s| s.to_owned()),
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn cancel_by_order_id(
        &self,
        order_id: &str,
        subaccount_id: Option<u64>,
    ) -> Result<OrderMutationResult> {
        self.cancel_with(CancelOrderParams {
            order_id: Some(order_id.to_owned()),
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn cancel_all(
        &self,
        symbol: Option<&str>,
        dry_run: bool,
        subaccount_id: Option<u64>,
    ) -> Result<CancelAllOrdersResult> {
        self.cancel_all_with(CancelAllOpts {
            symbol: symbol.map(|s| s.to_owned()),
            dry_run,
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn cancel_all_with(&self, opts: CancelAllOpts) -> Result<CancelAllOrdersResult> {
        let mut req = CancelAllOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, opts.subaccount_id)?,
            symbol: opts.symbol.unwrap_or_default(),
            dry_run: opts.dry_run,
            request_id: opts
                .request_id
                .unwrap_or_else(|| Self::request_id("sdk-cancel-all")),
            ..Default::default()
        };
        if let Some(side) = opts.side.as_deref() {
            req.side = Self::parse_side(side)?.into();
        }
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CancelAllOrders",
            req,
            |req, opts| client.cancel_all_orders_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(cancel_all_from_proto(&resp))
    }

    fn parse_side(side: &str) -> Result<Side> {
        match side.to_ascii_lowercase().as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(Error::validation("side must be buy or sell")),
        }
    }

    /// Modify an order. `new_price` / `new_qty` must be `Price` / `Quantity` wrappers.
    pub async fn modify(&self, params: ModifyOrderParams) -> Result<ModifyOrderResult> {
        let req = self.encode_modify_params(params)?;
        let client = self.write_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/ModifyOrder",
            req,
            |req, opts| client.modify_order_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(modify_order_from_proto(&resp))
    }

    pub fn create_params(
        symbol: impl Into<String>,
        side: CreateSide,
        order_type: CreateOrderType,
        quantity: Quantity,
        price: Option<Price>,
    ) -> CreateOrderParams {
        CreateOrderParams {
            symbol: symbol.into(),
            side,
            order_type,
            quantity,
            price,
            time_in_force: None,
            client_order_id: None,
            subaccount_id: None,
            post_only: None,
            market_client_ref_price: None,
            attached_risk: None,
        }
    }

    /// Resolve quantity scale for batch modify without inventing scale 8.
    ///
    /// When `symbol` is present, use the catalog. When absent, every `new_qty`
    /// must already carry a known scale; otherwise fail loudly.
    pub(crate) fn resolve_batch_modify_scale(
        catalogs: &crate::catalogs::Manager,
        symbol: &str,
        items: &[BatchModifyItem],
    ) -> Result<u32> {
        if !symbol.is_empty() {
            return Ok(catalogs.base_quantity_scale_for_symbol(symbol));
        }
        let mut inferred: Option<u32> = None;
        for item in items {
            let Some(qty) = item.new_qty.as_ref() else {
                continue;
            };
            let Some(scale) = qty.scale else {
                return Err(Error::validation(
                    "batch_modify requires symbol when new_qty has no known scale",
                ));
            };
            match inferred {
                None => inferred = Some(scale),
                Some(existing) if existing != scale => {
                    return Err(Error::validation(
                        "batch_modify without symbol requires consistent new_qty scales",
                    ));
                }
                _ => {}
            }
        }
        Ok(inferred.unwrap_or(0))
    }

    /// Subscribe to private order updates for an account.
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<Order>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:orders:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::order_from_bytes)
            .await
    }
}

#[derive(Clone)]
pub struct TradesService {
    ctx: ServiceContext,
}

impl TradesService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn list(
        &self,
        subaccount_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<UserTradesList> {
        let req = GetUserTradesRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            limit,
            ..Default::default()
        };
        let client = OrdersReadServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetUserTrades",
            req,
            |req, opts| client.get_user_trades_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(user_trades_list_from_proto(&resp))
    }

    /// Subscribe to private user trade updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<UserTrade>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:trades:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::user_trade_from_bytes)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::scalars::format_id;
    use buffa::Message;
    use serde_json::json;

    fn client() -> crate::Client {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client.catalogs.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 7,
                "base_quantity_scale": 8
            }]
        }));
        client
    }

    fn create_params(quantity: Quantity, price: Price) -> CreateOrderParams {
        CreateOrderParams {
            symbol: "BTC-USDT".into(),
            side: CreateSide::Buy,
            order_type: CreateOrderType::Limit,
            quantity,
            price: Some(price),
            time_in_force: Some(CreateTimeInForce::Gtc),
            client_order_id: Some("order-equivalence".into()),
            subaccount_id: None,
            post_only: Some(true),
            market_client_ref_price: None,
            attached_risk: None,
        }
    }

    #[test]
    fn decimal_and_scaled_create_encode_identically() {
        let client = client();
        let decimal = create_params(
            Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            Price::from_decimal_str("50000", Some("BTC-USDT".into())).unwrap(),
        );
        let scaled = create_params(
            Quantity::from_scaled(
                10_000_000,
                Some(8),
                crate::QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(7),
            )
            .unwrap(),
            Price::from_ticks(50_000_000_000, Some("BTC-USDT".into())).unwrap(),
        );

        let decimal_wire = client.orders.encode_create_params(&decimal).unwrap();
        let scaled_wire = client.orders.encode_create_params(&scaled).unwrap();
        assert_eq!(decimal_wire.encode_to_vec(), scaled_wire.encode_to_vec());
    }

    fn modify_params(new_price: Option<Price>, new_qty: Option<Quantity>) -> ModifyOrderParams {
        ModifyOrderParams {
            symbol: "BTC-USDT".into(),
            order_id: Some("1".into()),
            client_order_id: None,
            subaccount_id: None,
            request_id: Some("modify-equivalence".into()),
            new_price,
            new_qty,
            new_attached_risk: None,
            behavior: Some("amend_or_replace".into()),
            new_client_order_id: None,
        }
    }

    #[test]
    fn decimal_and_scaled_modify_encode_identically() {
        let client = client();
        let decimal = modify_params(
            Some(Price::from_decimal_str("50001", Some("BTC-USDT".into())).unwrap()),
            Some(Quantity::from_decimal_str("0.2", 8, Some("BTC-USDT".into()), Some(7)).unwrap()),
        );
        let scaled = modify_params(
            Some(Price::from_ticks(50_001_000_000, Some("BTC-USDT".into())).unwrap()),
            Some(
                Quantity::from_scaled(
                    20_000_000,
                    Some(8),
                    crate::QuantityDomain::OrderBase,
                    Some("BTC-USDT".into()),
                    Some(7),
                )
                .unwrap(),
            ),
        );

        let decimal_wire = client.orders.encode_modify_params(decimal).unwrap();
        let scaled_wire = client.orders.encode_modify_params(scaled).unwrap();
        assert_eq!(decimal_wire.encode_to_vec(), scaled_wire.encode_to_vec());
    }

    #[test]
    fn batch_modify_rejects_missing_symbol_without_qty_scale() {
        let catalogs = crate::catalogs::Manager::new();
        let items = vec![BatchModifyItem {
            order_id: Some(format_id(4)),
            client_order_id: None,
            new_price: None,
            new_qty: Some(
                Quantity::from_scaled(1, None, crate::QuantityDomain::OrderBase, None, None)
                    .unwrap(),
            ),
            new_attached_risk: None,
            behavior: None,
            new_client_order_id: None,
        }];
        let err = OrdersService::resolve_batch_modify_scale(&catalogs, "", &items).unwrap_err();
        assert!(
            err.to_string().contains("requires symbol"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn batch_modify_allows_missing_symbol_when_qty_scale_known() {
        let catalogs = crate::catalogs::Manager::new();
        let items = vec![BatchModifyItem {
            order_id: Some(format_id(4)),
            client_order_id: None,
            new_price: None,
            new_qty: Some(
                Quantity::from_scaled(1, Some(8), crate::QuantityDomain::OrderBase, None, None)
                    .unwrap(),
            ),
            new_attached_risk: None,
            behavior: None,
            new_client_order_id: None,
        }];
        assert_eq!(
            OrdersService::resolve_batch_modify_scale(&catalogs, "", &items).unwrap(),
            8
        );
    }

    #[test]
    fn modify_validates_key_and_patch() {
        let client = client();
        let no_key = ModifyOrderParams {
            order_id: None,
            ..modify_params(Some(Price::from_ticks(1, None).unwrap()), None)
        };
        assert!(client.orders.encode_modify_params(no_key).is_err());

        let no_patch = modify_params(None, None);
        assert!(client.orders.encode_modify_params(no_patch).is_err());
    }

    #[test]
    fn attached_risk_encodes_on_create_and_modify() {
        use crate::models::{AttachedRisk, RiskLeg, TriggerPriceSourceKind};

        let client = client();
        let risk = AttachedRisk {
            take_profit: Some(RiskLeg {
                trigger_price: Price::from_ticks(51_000_000_000, Some("BTC-USDT".into())).unwrap(),
                trigger_price_source: Some(TriggerPriceSourceKind::LastPrice),
                order_type: Some(CreateOrderType::Market),
                limit_price: None,
            }),
            stop_loss: Some(RiskLeg {
                trigger_price: Price::from_ticks(49_000_000_000, Some("BTC-USDT".into())).unwrap(),
                trigger_price_source: Some(TriggerPriceSourceKind::LastPrice),
                order_type: Some(CreateOrderType::Limit),
                limit_price: Some(
                    Price::from_ticks(48_900_000_000, Some("BTC-USDT".into())).unwrap(),
                ),
            }),
            trailing_stop: None,
            oco: true,
        };

        let mut create = create_params(
            Quantity::from_scaled(
                10_000_000,
                Some(8),
                crate::QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(7),
            )
            .unwrap(),
            Price::from_ticks(50_000_000_000, Some("BTC-USDT".into())).unwrap(),
        );
        create.attached_risk = Some(risk.clone());
        let create_wire = client.orders.encode_create_params(&create).unwrap();
        let order = create_wire.order.as_option().unwrap();
        assert!(order.attached_risk.is_set());
        assert!(order.attached_risk.as_option().unwrap().oco);

        let mut modify = modify_params(None, None);
        modify.new_attached_risk = Some(risk);
        let modify_wire = client.orders.encode_modify_params(modify).unwrap();
        assert!(modify_wire.new_attached_risk.is_set());
    }
}
