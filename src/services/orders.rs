use super::ServiceContext;
use super::scope;
use super::unary;
use crate::connect::orders::v1::{OrdersReadServiceClient, OrdersServiceClient};
use crate::errors::{Error, Result};
use crate::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
use crate::proto::orders::v1::{
    CancelOrderRequest, CreateOrderRequest, GetOpenOrdersRequest, GetOrderHistoryRequest,
    GetUserTradesRequest, ModifyOrderRequest, OrderType, Side, TimeInForce,
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

    pub async fn list_open(
        &self,
        subaccount_id: Option<u64>,
    ) -> Result<crate::proto::orders::v1::GetOpenOrdersResponse> {
        let req = GetOpenOrdersRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        };
        let client = self.read_client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetOpenOrders",
            req,
            |req, opts| client.get_open_orders_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn list_history(
        &self,
        subaccount_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<crate::proto::orders::v1::GetOrderHistoryResponse> {
        let req = GetOrderHistoryRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            limit,
            ..Default::default()
        };
        let client = self.read_client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetOrderHistory",
            req,
            |req, opts| client.get_order_history_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    /// Place an order. Quantity and price must be `Quantity` / `Price` wrappers.
    pub async fn create(
        &self,
        params: CreateOrderParams,
    ) -> Result<crate::proto::orders::v1::CreateOrderResponse> {
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
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CreateOrder",
            req,
            |req, opts| client.create_order_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn cancel(
        &self,
        req: CancelOrderRequest,
    ) -> Result<crate::proto::orders::v1::CancelOrderResponse> {
        let client = self.write_client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/CancelOrder",
            req,
            |req, opts| client.cancel_order_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn modify(
        &self,
        req: ModifyOrderRequest,
    ) -> Result<crate::proto::orders::v1::ModifyOrderResponse> {
        let client = self.write_client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersService/ModifyOrder",
            req,
            |req, opts| client.modify_order_with_options(req, opts),
        )
        .await?
        .into_owned())
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
        }
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
    ) -> Result<crate::proto::orders::v1::GetUserTradesResponse> {
        let req = GetUserTradesRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            limit,
            ..Default::default()
        };
        let client = OrdersReadServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/orders.v1.OrdersReadService/GetUserTrades",
            req,
            |req, opts| client.get_user_trades_with_options(req, opts),
        )
        .await?
        .into_owned())
    }
}
