use super::ServiceContext;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    cancel_all_from_proto, get_order_from_proto, modify_order_from_proto,
    order_mutation_from_cancel, order_mutation_from_create, orders_list_from_history,
    orders_list_from_open, user_trades_list_from_proto,
};
use crate::codecs::scalars::id_to_u64;
use crate::connect::orders::v1::{OrdersReadServiceClient, OrdersServiceClient};
use crate::errors::{Error, Result};
use crate::models::{
    CancelAllOrdersResult, CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce,
    GetOrderResult, ModifyOrderResult, OrderMutationResult, OrdersList, UserTradesList,
};
use crate::proto::orders::v1::{
    CancelAllOrdersRequest, CancelOrderRequest, CreateOrderRequest, GetOpenOrdersRequest,
    GetOrderHistoryRequest, GetOrderRequest, GetUserTradesRequest, ModifyOrderRequest, OrderType,
    Side, TimeInForce, cancel_order_request, get_order_request,
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
        let req = GetOpenOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
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
        let req = GetOrderHistoryRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            limit,
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
        let key = if let Some(cid) = client_order_id {
            Some(get_order_request::Key::ClientOrderId(cid.to_owned()))
        } else if let Some(oid) = order_id {
            Some(get_order_request::Key::OrderId(
                id_to_u64(oid, "order_id")?,
            ))
        } else {
            return Err(Error::validation(
                "orders.get requires client_order_id or order_id",
            ));
        };
        let req = GetOrderRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            key,
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

    /// Place an order. Quantity and price must be `Quantity` / `Price` wrappers.
    pub async fn create(&self, params: CreateOrderParams) -> Result<OrderMutationResult> {
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
        let mut req = CreateOrderRequest {
            symbol: params.symbol.clone(),
            qty_scaled: qty,
            side: match params.side {
                CreateSide::Buy => Side::Buy.into(),
                CreateSide::Sell => Side::Sell.into(),
            },
            order_type: match params.order_type {
                CreateOrderType::Limit => OrderType::Limit.into(),
                CreateOrderType::Market => OrderType::Market.into(),
            },
            ..Default::default()
        };
        if let Some(price) = params.price.as_ref() {
            req.price_ticks = resolve_price_ticks(price, Some(&params.symbol))?;
        } else if matches!(params.order_type, CreateOrderType::Limit) {
            return Err(Error::validation(
                "price is required for limit orders (use Price::from_decimal or Price::from_ticks)",
            ));
        }
        if let Some(ref_price) = params.market_client_ref_price.as_ref() {
            req.market_client_ref_price_ticks =
                resolve_price_ticks(ref_price, Some(&params.symbol))?;
        }
        if let Some(tif) = params.time_in_force {
            req.time_in_force = match tif {
                CreateTimeInForce::Gtc => TimeInForce::Gtc.into(),
                CreateTimeInForce::Ioc => TimeInForce::Ioc.into(),
                CreateTimeInForce::Fok => TimeInForce::Fok.into(),
            };
        }
        if let Some(id) = params.client_order_id {
            req.client_order_id = id;
        }
        req.subaccount_id = scope::optional_subaccount(&self.ctx, params.subaccount_id)?;
        if let Some(v) = params.post_only {
            req.post_only = v;
        }

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

    pub async fn cancel_by_client_order_id(
        &self,
        client_order_id: &str,
        symbol: Option<&str>,
        subaccount_id: Option<u64>,
    ) -> Result<OrderMutationResult> {
        let symbol_id = symbol
            .and_then(|s| self.ctx.catalogs.symbol_id_for_symbol(s))
            .unwrap_or(0);
        let req = CancelOrderRequest {
            symbol_id,
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            key: Some(cancel_order_request::Key::ClientOrderId(
                client_order_id.to_owned(),
            )),
            ..Default::default()
        };
        self.cancel(req).await
    }

    pub async fn cancel_all(
        &self,
        symbol: Option<&str>,
        dry_run: bool,
        subaccount_id: Option<u64>,
    ) -> Result<CancelAllOrdersResult> {
        let req = CancelAllOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            symbol: symbol.unwrap_or("").to_owned(),
            dry_run,
            request_id: format!(
                "sdk-cancel-all-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ),
            ..Default::default()
        };
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

    pub async fn modify(&self, req: ModifyOrderRequest) -> Result<ModifyOrderResult> {
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
        }
    }

    /// Subscribe to private order updates for an account (requires `realtime` feature).
    #[cfg(feature = "realtime")]
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::Subscription> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:orders:{account}:proto");
        self.ctx.realtime.subscribe_raw(&channel).await
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
}
