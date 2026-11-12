//! Price alerts (`alerts.v1.PriceAlertService`).
//!
//! Available on devnet; not yet routed on testnet. Alerts belong to the root
//! account: an API key scoped to a sub-account is refused with
//! [`Error::PermissionDenied`] and code `SUBACCOUNT_KEY_REFUSED`.

use super::{ServiceContext, changelog::paginate, unary};
use crate::codecs::decode::{
    price_alerts_page_from_proto, required_price_alert, update_alert_result_from_proto,
};
use crate::codecs::scalars::id_to_u64;
use crate::connect::alerts::v1::PriceAlertServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    AlertChannel, AlertDirection, AlertPriceSource, AlertStatus, CreateAlertParams,
    ListAlertsOptions, PriceAlert, PriceAlertsPage, UpdateAlertParams, UpdateAlertResult,
};
use crate::proto::alerts::v1::{
    AlertChannel as ProtoChannel, AlertDirection as ProtoDirection,
    AlertPriceSource as ProtoPriceSource, AlertStatus as ProtoStatus, AlertUpdate,
    CancelAlertRequest, CreateAlertRequest, ListAlertsRequest, UpdateAlertRequest,
};
use crate::transport::SharedTransport;
use crate::types::Price;
use buffa_types::google::protobuf::FieldMask;
use futures_util::Stream;

/// Largest `ListAlerts` page.
const MAX_LIST_LIMIT: u32 = 200;
/// Longest idempotency key.
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;

#[derive(Clone)]
pub struct AlertsService {
    ctx: ServiceContext,
}

impl AlertsService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> PriceAlertServiceClient<SharedTransport> {
        PriceAlertServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn symbol_of(&self) -> impl Fn(u32) -> String + '_ {
        |symbol_id| self.ctx.catalogs.display_symbol(symbol_id)
    }

    /// Create an alert that fires once when the market's price crosses
    /// `params.price`. The current price is recorded as its
    /// `reference_price`; an alert whose condition already holds starts
    /// unarmed. Refused before sending: no or repeated channels, more than
    /// two, a price that is not positive or is bound to another market, a
    /// malformed idempotency key, or a market the catalogs don't know by name.
    /// A price bound to a market is checked against the catalogs' name for
    /// the ID sent, and refused when they don't know it. An answer without
    /// the alert, or with an alert whose ID is 0, is
    /// [`Error::ResponseContract`].
    pub async fn create(&self, params: CreateAlertParams) -> Result<PriceAlert> {
        let channels = channels(&params.channels)?;
        let price_ticks = positive_price(&params.price)?;
        let idempotency_key = idempotency_key(params.idempotency_key.as_deref())?;
        let symbol_id = self
            .market(&params.symbol, params.symbol_id, &params.price)
            .await?;
        let req = CreateAlertRequest {
            symbol_id,
            direction: direction(params.direction).into(),
            price_source: params
                .price_source
                .map_or(ProtoPriceSource::PRICE_SOURCE_UNSPECIFIED, price_source)
                .into(),
            price_ticks,
            channels,
            idempotency_key,
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/alerts.v1.PriceAlertService/CreateAlert",
            req,
            |req, opts| client.create_alert_with_options(req, opts),
        )
        .await?
        .into_owned();
        required_price_alert(resp.alert.as_option(), "CreateAlert", self.symbol_of())
    }

    /// One page of the root account's alerts, newest first.
    pub async fn list(&self, opts: ListAlertsOptions) -> Result<PriceAlertsPage> {
        let req = self.list_request(&opts).await?;
        self.list_page(req).await
    }

    /// Every alert matching `opts` from `opts.page_token` on, newest first,
    /// fetching pages as the stream is polled.
    pub fn all(
        &self,
        opts: ListAlertsOptions,
    ) -> impl Stream<Item = Result<PriceAlert>> + Send + 'static {
        let svc = self.clone();
        paginate(
            "price alert pagination",
            opts.page_token.clone(),
            move |page_token| {
                let svc = svc.clone();
                let opts = ListAlertsOptions {
                    page_token: Some(page_token),
                    ..opts.clone()
                };
                async move {
                    let page = svc.list(opts).await?;
                    Ok((page.alerts, page.next_page_token))
                }
            },
        )
    }

    /// Change an alert's direction, price or channels: the fields set on
    /// `params` are sent with an update mask naming them. With none set the
    /// mask is empty, which sets a fired alert again (and leaves an active
    /// one `"unchanged"`). Updating a fired alert sets it again. The price
    /// must be unbound: a price bound to a market, such as an alert's own
    /// `price`, is refused with [`Error::Validation`] before sending, since
    /// the update names the alert, not its market. An answer
    /// without the alert, or with an alert whose ID is 0, is
    /// [`Error::ResponseContract`] whatever its outcome.
    pub async fn update(&self, params: UpdateAlertParams) -> Result<UpdateAlertResult> {
        let alert_id = alert_id(&params.alert_id)?;
        if params.expected_revision == 0 {
            return Err(Error::validation("expected_revision must be positive"));
        }
        let idempotency_key = idempotency_key(params.idempotency_key.as_deref())?;
        let mut update = AlertUpdate::default();
        let mut paths = Vec::new();
        if let Some(value) = params.direction {
            update.direction = direction(value).into();
            paths.push("direction".to_owned());
        }
        if let Some(price) = &params.price {
            // The update names the alert, not its market, so nothing proves a
            // bound price is for that market.
            if let Some(bound) = price.symbol() {
                return Err(Error::validation(format!(
                    "update alert cannot check a price bound to {bound} against the alert's market; pass an unbound price: Price::from_decimal_str(.., None) or Price::from_ticks(.., None)"
                )));
            }
            update.price_ticks = positive_price(price)?;
            paths.push("price_ticks".to_owned());
        }
        if let Some(values) = &params.channels {
            update.channels = channels(values)?;
            paths.push("channels".to_owned());
        }
        let req = UpdateAlertRequest {
            alert_id,
            update: update.into(),
            update_mask: FieldMask {
                paths,
                ..Default::default()
            }
            .into(),
            expected_revision: params.expected_revision,
            idempotency_key,
            ..Default::default()
        };
        self.ctx.settle_catalogs().await;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/alerts.v1.PriceAlertService/UpdateAlert",
            req,
            |req, opts| client.update_alert_with_options(req, opts),
        )
        .await?
        .into_owned();
        update_alert_result_from_proto(&resp, self.symbol_of())
    }

    /// Cancel an active alert. Cancelling a cancelled alert returns it
    /// unchanged; a fired one is refused with `ALERT_TRIGGERED`. An answer
    /// without the alert, or with an alert whose ID is 0, is
    /// [`Error::ResponseContract`].
    pub async fn cancel(&self, alert_id: &str) -> Result<PriceAlert> {
        let req = CancelAlertRequest {
            alert_id: self::alert_id(alert_id)?,
            ..Default::default()
        };
        self.ctx.settle_catalogs().await;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/alerts.v1.PriceAlertService/CancelAlert",
            req,
            |req, opts| client.cancel_alert_with_options(req, opts),
        )
        .await?
        .into_owned();
        required_price_alert(resp.alert.as_option(), "CancelAlert", self.symbol_of())
    }

    async fn list_request(&self, opts: &ListAlertsOptions) -> Result<ListAlertsRequest> {
        if opts.limit.is_some_and(|limit| limit > MAX_LIST_LIMIT) {
            return Err(Error::validation(format!(
                "list alerts limit must be at most {MAX_LIST_LIMIT}"
            )));
        }
        let mut statuses = Vec::with_capacity(opts.statuses.len());
        for value in &opts.statuses {
            let value = status(*value);
            if statuses.contains(&value.into()) {
                return Err(Error::validation(
                    "list alerts statuses must be distinct, at most 3",
                ));
            }
            statuses.push(value.into());
        }
        if opts.symbol.is_some() {
            self.ctx.wait_for_catalogs().await?;
        } else {
            self.ctx.settle_catalogs().await;
        }
        let symbol_id = self
            .ctx
            .catalogs
            .optional_symbol_id(opts.symbol.as_deref(), opts.symbol_id)?;
        Ok(ListAlertsRequest {
            statuses,
            symbol_id,
            limit: opts.limit.unwrap_or(0),
            page_token: opts.page_token.clone().unwrap_or_default(),
            ..Default::default()
        })
    }

    async fn list_page(&self, req: ListAlertsRequest) -> Result<PriceAlertsPage> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/alerts.v1.PriceAlertService/ListAlerts",
            req,
            |req, opts| client.list_alerts_with_options(req, opts),
        )
        .await?
        .into_owned();
        price_alerts_page_from_proto(&resp, self.symbol_of())
    }

    /// The Connect ID of the market `symbol` / `symbol_id` names, checking
    /// that a price bound to a market is bound to this one.
    async fn market(&self, symbol: &str, symbol_id: Option<u32>, price: &Price) -> Result<u32> {
        let named = !symbol.trim().is_empty();
        if named {
            self.ctx.wait_for_catalogs().await?;
        } else {
            self.ctx.settle_catalogs().await;
        }
        let id = match (named, symbol_id) {
            (_, Some(0)) => {
                return Err(Error::validation(
                    "symbol_id must be non-zero when explicitly supplied",
                ));
            }
            (false, None) => {
                return Err(Error::validation(
                    "create alert requires symbol or symbol_id",
                ));
            }
            (false, Some(id)) => id,
            (true, explicit) => {
                let id = self.ctx.catalogs.require_symbol_id(symbol)?;
                if let Some(explicit) = explicit {
                    self.ctx.catalogs.require_symbol_matches_id(
                        Some(symbol),
                        explicit,
                        "create alert",
                    )?;
                }
                id
            }
        };
        if let Some(market) = self.ctx.catalogs.symbol_for_symbol_id(id) {
            price.compatible_with(Some(&market))?;
        } else if let Some(bound) = price.symbol() {
            return Err(Error::validation(format!(
                "create alert cannot check a price bound to {bound} against symbol_id {id}: the market is not in the catalogs; hydrate catalogs or pass an unbound price"
            )));
        }
        Ok(id)
    }
}

fn alert_id(value: &str) -> Result<u64> {
    let id = id_to_u64(value, "alert_id")?;
    if id == 0 {
        return Err(Error::validation("alert_id must be non-zero"));
    }
    Ok(id)
}

fn positive_price(price: &Price) -> Result<i64> {
    let ticks = price.as_ticks();
    if ticks <= 0 {
        return Err(Error::validation("alert price must be positive"));
    }
    Ok(ticks)
}

fn channels(values: &[AlertChannel]) -> Result<Vec<buffa::EnumValue<ProtoChannel>>> {
    if values.is_empty() || values.len() > 2 {
        return Err(Error::validation(
            "alert channels must be one or two distinct channels",
        ));
    }
    let mut out: Vec<buffa::EnumValue<ProtoChannel>> = Vec::with_capacity(values.len());
    for value in values {
        let channel = match value {
            AlertChannel::InApp => ProtoChannel::IN_APP,
            AlertChannel::Telegram => ProtoChannel::TELEGRAM,
        };
        if out.contains(&channel.into()) {
            return Err(Error::validation(
                "alert channels must be one or two distinct channels",
            ));
        }
        out.push(channel.into());
    }
    Ok(out)
}

/// An optional idempotency key: 1 to 128 of `A-Z a-z 0-9 . _ : -`.
fn idempotency_key(value: Option<&str>) -> Result<String> {
    let Some(key) = value else {
        return Ok(String::new());
    };
    let valid = !key.is_empty()
        && key.len() <= MAX_IDEMPOTENCY_KEY_LEN
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'));
    if !valid {
        return Err(Error::validation(
            "idempotency_key must be 1 to 128 characters of A-Z a-z 0-9 . _ : -",
        ));
    }
    Ok(key.to_owned())
}

fn direction(value: AlertDirection) -> ProtoDirection {
    match value {
        AlertDirection::Above => ProtoDirection::ABOVE,
        AlertDirection::Below => ProtoDirection::BELOW,
    }
}

fn price_source(value: AlertPriceSource) -> ProtoPriceSource {
    match value {
        AlertPriceSource::LastPrice => ProtoPriceSource::LAST_PRICE,
        AlertPriceSource::IndexPrice => ProtoPriceSource::INDEX_PRICE,
    }
}

fn status(value: AlertStatus) -> ProtoStatus {
    match value {
        AlertStatus::Active => ProtoStatus::ACTIVE,
        AlertStatus::Triggered => ProtoStatus::TRIGGERED,
        AlertStatus::Cancelled => ProtoStatus::CANCELLED,
    }
}
