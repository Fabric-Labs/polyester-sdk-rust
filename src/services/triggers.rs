use super::ServiceContext;
use super::correlation_id::require_client_list_id;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    attach_trigger_scale, batch_cancel_triggers_from_proto, cancel_oco_from_proto,
    create_oco_from_proto, get_trigger_from_proto, trigger_events_list_from_proto,
    trigger_mutation_from_cancel, trigger_mutation_from_create, trigger_mutation_from_modify,
    trigger_mutation_from_pause, trigger_mutation_from_resume, triggers_list_from_proto,
};
use crate::codecs::scalars::id_to_u64;
use crate::connect::triggers::v1::TriggersServiceClient;
use crate::errors::{Error, Result};
use crate::models::SubAccountScope;
use crate::models::{
    BatchCancelTriggersResult, CancelOcoResult, CreateOcoParams, CreateOcoResult, CreateOrderType,
    CreateSide, CreateTimeInForce, CreateTriggerParams, CreateTriggerType, FeeAsset,
    ListTriggerEventsOpts, ListTriggersOpts, ModifyTriggerParams, OcoLegParams,
    ResumeTriggerParams, Trigger, TriggerEvent, TriggerEventsList, TriggerMutationResult,
    TriggersList,
};
use crate::proto::orders::v1::{
    FeeAsset as ProtoFeeAsset, RiskExecution, RiskLimitGtc, SelfTradePreventionMode, Side,
    TriggerPriceSource, risk_execution,
};
use crate::proto::triggers::v1::{
    BatchCancelTriggersRequest, CancelOcoRequest, CancelTriggerRequest, ConditionalChildExecution,
    ConditionalTrigger, CreateOcoRequest, CreateTriggerRequest, GetTriggerRequest, LadderTrigger,
    ListTriggerEventsRequest, ListTriggersRequest, ModifyTriggerRequest, OcoIntent, OcoLeg,
    PauseTriggerRequest, ResumeTriggerRequest, TrailingStopTrigger, TriggerIntent, TriggerLimitFok,
    TriggerLimitGtc, TriggerLimitIoc, TriggerMarketIoc, TriggerType, TwapLimitGtc, TwapMarketIoc,
    TwapTrigger, conditional_child_execution, modify_trigger_request, trailing_stop_trigger,
    trigger_intent, trigger_market_ioc, twap_market_ioc, twap_trigger,
};
use crate::types::resolve_qty_scaled;

const MAX_BPS: i32 = 10_000;
/// Most triggers one batch cancel takes.
const MAX_BATCH_CANCEL_TRIGGERS: usize = 50;
/// Slippage cap shared by every trailing stop and every child that sells: a
/// 100% cap would leave a sell no positive price to sell at. A modify does
/// not say which trigger it changes, so it takes up to [`MAX_BPS`], as the
/// contract does, and the server refuses 10000 where it does not apply.
const MAX_SELLING_SLIPPAGE_BPS: i32 = MAX_BPS - 1;
/// Shortest TWAP the contract takes, in milliseconds: the `int64.gte` rule on
/// `TwapTrigger.duration_ms`. A test pins it to the generated field docs.
const TWAP_DURATION_MS_MIN: i64 = 1_000;
/// Shortest TWAP slice interval the contract takes, in milliseconds: the
/// `int64.gte` rule on `TwapTrigger.slice_interval_ms` (whose CEL rule also
/// caps it at the duration). A test pins it to the generated field docs.
const TWAP_SLICE_INTERVAL_MS_MIN: i64 = 100;
/// Ladder level counts the contract takes: the `int32.gte`/`lte` validate
/// rule on `LadderTrigger.levels` is the source. The generated docs do not
/// state this range, so a test pins the literal values.
const LADDER_LEVELS_MIN: i32 = 2;
const LADDER_LEVELS_MAX: i32 = 100;
/// Longest client trigger ID the contract takes, in UTF-8 bytes: the
/// `string.max_bytes` rule on `TriggerIntent.client_trigger_id`, whose
/// `min_len` is 1 and which has no character set. A test pins it to the
/// generated field docs.
const CLIENT_TRIGGER_ID_MAX_BYTES: usize = 36;

/// Codes of the Triggers API's field refusals: a trigger or OCO field the SDK
/// refuses before sending carries the code the server answers for the same
/// mistake, so a pre-checked request and a server refusal read alike.
mod refusal_codes {
    pub const BAD_PRICE: &str = "ERROR_CODE_BAD_PRICE";
    pub const BAD_QTY: &str = "ERROR_CODE_BAD_QTY";
    pub const CLIENT_TRIGGER_ID_INVALID: &str = "ERROR_CODE_CLIENT_TRIGGER_ID_INVALID";
    pub const FEE_ASSET_NOT_ALLOWED: &str = "ERROR_CODE_FEE_ASSET_NOT_ALLOWED";
    pub const LADDER_LEVELS_INVALID: &str = "ERROR_CODE_LADDER_LEVELS_INVALID";
    pub const MAX_SLIPPAGE_INVALID: &str = "ERROR_CODE_MAX_SLIPPAGE_INVALID";
    pub const SLIPPAGE_BPS_INVALID: &str = "ERROR_CODE_SLIPPAGE_BPS_INVALID";
    pub const SLIPPAGE_TICKS_INVALID: &str = "ERROR_CODE_SLIPPAGE_TICKS_INVALID";
    pub const TRAILING_DISTANCE_INVALID: &str = "ERROR_CODE_TRAILING_DISTANCE_INVALID";
    pub const TRIGGER_ACTIVATION_PRICE_INVALID: &str =
        "ERROR_CODE_TRIGGER_ACTIVATION_PRICE_INVALID";
    pub const TRIGGER_PRICE_INVALID: &str = "ERROR_CODE_TRIGGER_PRICE_INVALID";
    pub const TRIGGER_QTY_INVALID: &str = "ERROR_CODE_TRIGGER_QTY_INVALID";
    pub const TWAP_DURATION_INVALID: &str = "ERROR_CODE_TWAP_DURATION_INVALID";
    pub const TWAP_INTERVAL_INVALID: &str = "ERROR_CODE_TWAP_INTERVAL_INVALID";
}
use refusal_codes as codes;

/// A trigger or OCO field refused before sending, as [`Error::Api`] with the
/// server's code for the same mistake and no violation.
fn field_refusal(code: &str, message: impl Into<String>) -> Error {
    Error::Api {
        message: message.into(),
        code: code.to_owned(),
        metadata: Vec::new(),
        violations: Vec::new(),
        retry_after: None,
    }
}

/// A basis-point cap or distance: outside 1 to 10000 (0 clears when
/// `allow_clear`) is `range_code`; within it but above `max`, a slippage cap
/// that would leave a sell no positive price, is `MAX_SLIPPAGE_INVALID`.
fn validate_bps(field: &str, bps: i32, allow_clear: bool, range_code: &str) -> Result<()> {
    validate_bps_at_most(field, bps, allow_clear, MAX_BPS, range_code)
}

fn validate_bps_at_most(
    field: &str,
    bps: i32,
    allow_clear: bool,
    max: i32,
    range_code: &str,
) -> Result<()> {
    if allow_clear && bps == 0 {
        return Ok(());
    }
    if !(1..=MAX_BPS).contains(&bps) {
        return Err(field_refusal(
            range_code,
            format!("{field} must be between 1 and {max}"),
        ));
    }
    if bps > max {
        return Err(field_refusal(
            codes::MAX_SLIPPAGE_INVALID,
            format!("{field} must be between 1 and {max}"),
        ));
    }
    Ok(())
}

/// A price for `symbol` (or any market when `None`) in ticks. A negative price,
/// and 0 unless `allow_zero`, is refused with `code`.
fn coded_price_ticks(
    price: &crate::Price,
    symbol: Option<&str>,
    field: &str,
    allow_zero: bool,
    code: &str,
) -> Result<i64> {
    price.compatible_with(symbol)?;
    let ticks = price.as_ticks();
    if ticks < 0 || (ticks == 0 && !allow_zero) {
        return Err(field_refusal(
            code,
            if allow_zero {
                format!("{field} must not be negative")
            } else {
                format!("{field} must be positive")
            },
        ));
    }
    Ok(ticks)
}

/// A trigger or OCO quantity: one that is not positive is refused with `code`.
fn coded_qty_scaled(
    qty: &crate::Quantity,
    market: &crate::catalogs::Market,
    code: &str,
) -> Result<i64> {
    match resolve_qty_scaled(
        qty,
        market.base_quantity_scale,
        Some(&market.symbol),
        Some(market.symbol_id),
    ) {
        Err(Error::Validation(message)) if qty.as_scaled() <= 0 => {
            Err(field_refusal(code, message))
        }
        other => other,
    }
}

// The flat public helper must not silently discard fields when it selects a
// different strategy or one of the contract's fixed child executions.
fn validate_create_option_applicability(p: &CreateTriggerParams) -> Result<()> {
    use CreateTriggerType::{Ladder, StopLoss, TakeProfit, TrailingStop, Twap};
    for (present, option, supported, label) in [
        (
            p.trailing_distance_ticks.is_some(),
            "trailing_distance_ticks",
            TrailingStop,
            "trailing_stop",
        ),
        (
            p.trailing_distance_bps.is_some(),
            "trailing_distance_bps",
            TrailingStop,
            "trailing_stop",
        ),
        (
            p.activation_price.is_some(),
            "activation_price",
            TrailingStop,
            "trailing_stop",
        ),
        (
            p.twap_duration_ms.is_some(),
            "twap_duration_ms",
            Twap,
            "twap",
        ),
        (
            p.twap_slice_interval_ms.is_some(),
            "twap_slice_interval_ms",
            Twap,
            "twap",
        ),
        (
            p.ladder_price_min.is_some(),
            "ladder_price_min",
            Ladder,
            "ladder",
        ),
        (
            p.ladder_price_max.is_some(),
            "ladder_price_max",
            Ladder,
            "ladder",
        ),
        (p.ladder_levels.is_some(), "ladder_levels", Ladder, "ladder"),
        (
            p.ladder_distribution.is_some(),
            "ladder_distribution",
            Ladder,
            "ladder",
        ),
    ] {
        if present && p.trigger_type != supported {
            return Err(Error::validation(format!(
                "{option} applies only to {label}"
            )));
        }
    }
    // The contract's ladder is LINEAR limit GTC only: a market order type is
    // refused, never sent as resting limit orders.
    if p.trigger_type == Ladder && p.order_type == CreateOrderType::Market {
        return Err(Error::validation(
            "order_type=market does not apply to a ladder: a ladder places limit GTC orders only",
        ));
    }
    if p.trigger_price.is_some() && !matches!(p.trigger_type, StopLoss | TakeProfit) {
        return Err(Error::validation(
            "trigger_price applies only to stop_loss or take_profit",
        ));
    }
    if p.post_only && matches!(p.trigger_type, TrailingStop | Twap) {
        return Err(Error::validation(
            "post_only applies only to limit GTC stops or ladder",
        ));
    }
    if p.limit_price.is_some()
        && (p.trigger_type == Ladder
            || (p.trigger_type != TrailingStop && p.order_type == CreateOrderType::Market))
    {
        return Err(Error::validation(
            "limit_price applies only to limit children or a buy trailing_stop",
        ));
    }
    if (p.trigger_type == Ladder
        || (p.trigger_type == Twap && p.order_type == CreateOrderType::Limit))
        && (p.max_slippage_ticks.is_some() || p.max_slippage_bps.is_some())
    {
        return Err(Error::validation(
            "max_slippage_ticks and max_slippage_bps apply only to market children or trailing_stop",
        ));
    }
    // No trigger child has a GTD execution: refuse it for every strategy
    // rather than send a child with a lifetime the caller did not choose.
    if p.time_in_force == Some(CreateTimeInForce::Gtd) {
        return Err(Error::validation(
            "time_in_force=Gtd is not valid for trigger child orders",
        ));
    }
    // None leaves execution unspecified. Ladder and twap limit slices are GTC.
    // A market child and a trailing stop always execute IOC, so they accept
    // only None or Ioc: another time in force is refused, never sent as IOC.
    if p.trigger_type == Ladder
        || (p.trigger_type == Twap && p.order_type == CreateOrderType::Limit)
    {
        if matches!(
            p.time_in_force,
            Some(CreateTimeInForce::Ioc | CreateTimeInForce::Fok)
        ) {
            return Err(Error::validation(
                "time_in_force must be Gtc for ladder or twap limit slices",
            ));
        }
    } else if p.trigger_type == TrailingStop || p.order_type == CreateOrderType::Market {
        match p.time_in_force {
            Some(CreateTimeInForce::Fok) => {
                return Err(Error::validation(
                    "time_in_force=Fok applies only to stop_loss or take_profit limit children",
                ));
            }
            Some(CreateTimeInForce::Gtc) => {
                return Err(Error::validation(
                    "time_in_force=Gtc applies only to limit children; a market or trailing_stop child executes IOC",
                ));
            }
            _ => {}
        }
    }
    if p.trigger_type == TrailingStop
        && p.side == CreateSide::Sell
        && p.order_type == CreateOrderType::Limit
    {
        return Err(Error::validation(
            "order_type=limit applies only to buy trailing_stop children",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct TriggersService {
    ctx: ServiceContext,
}

impl TriggersService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> TriggersServiceClient<crate::transport::SharedTransport> {
        TriggersServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    /// List triggers.
    ///
    /// Retries `unavailable` (HTTP 503) with the same request: right after a
    /// trigger change the server answers it until the change is readable,
    /// rather than return the trigger's previous state. The retry starts at
    /// 25 ms, doubles up to 500 ms and gives up after about 2 s, returning
    /// the last error.
    pub async fn list(&self, mut req: ListTriggersRequest) -> Result<TriggersList> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.list_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn list_scoped(&self, req: ListTriggersRequest) -> Result<TriggersList> {
        // Triggers are labelled with their symbol from the catalogs.
        self.ctx.settle_catalogs().await;
        let client = self.client();
        let resp = unary::await_auth_retrying_unavailable(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggers",
            req,
            unary::TRIGGER_READ_RETRY,
            |req, opts| client.list_triggers_with_options(req, opts),
        )
        .await?
        .into_owned();
        let mut list = triggers_list_from_proto(&resp)?;
        for trigger in &mut list.triggers {
            self.label_from_catalogs(trigger);
        }
        Ok(list)
    }

    /// List triggers with typed filters: symbol, status, trigger type and
    /// parent order (see [`ListTriggersOpts`]). A filter that does not parse
    /// is refused with `Error::Validation` before anything is sent.
    pub async fn list_with(&self, opts: ListTriggersOpts) -> Result<TriggersList> {
        use crate::codecs::decode::trigger_status_from_label;
        let mut req = ListTriggersRequest {
            // Proto u32: 0 means omit / server default.
            limit: opts.limit.unwrap_or(0),
            ..Default::default()
        };
        if opts.symbol.is_some() || opts.symbol_id.is_some() {
            self.ctx.wait_for_catalogs().await?;
        }
        req.symbol_id = self
            .ctx
            .catalogs
            .optional_symbol_id(opts.symbol.as_deref(), opts.symbol_id)?;
        if let Some(token) = opts.page_token {
            req.page_token = token;
        }
        req.subaccount_id = scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?;
        for label in &opts.status {
            let status = trigger_status_from_label(label).map_err(Error::validation)?;
            req.status.push(status.into());
        }
        if let Some(trigger_type) = opts.trigger_type {
            req.trigger_type = match trigger_type {
                CreateTriggerType::StopLoss => TriggerType::STOP_LOSS,
                CreateTriggerType::TakeProfit => TriggerType::TAKE_PROFIT,
                CreateTriggerType::TrailingStop => TriggerType::TRAILING_STOP,
                CreateTriggerType::Twap => TriggerType::TWAP,
                CreateTriggerType::Ladder => TriggerType::LADDER,
            }
            .into();
        }
        if let Some(parent) = opts.parent_order_id.as_deref() {
            let id = id_to_u64(parent, "parent_order_id")?;
            if id == 0 {
                return Err(Error::validation(
                    "parent_order_id must be a non-zero order ID",
                ));
            }
            req.parent_order_id = Some(id);
        }
        if let Some(oco) = opts.order_list_id.as_deref() {
            let id = id_to_u64(oco, "order_list_id")?;
            if id == 0 {
                return Err(Error::validation("order_list_id must be a non-zero OCO ID"));
            }
            req.order_list_id = Some(id);
        }
        if let Some(key) = opts.client_list_id.as_deref() {
            req.client_list_id = Some(require_client_list_id(key, "client_list_id")?);
        }
        self.list_scoped(req).await
    }

    /// Read one trigger.
    ///
    /// Once a create, modify, pause or resume has answered, this returns the
    /// trigger as changed: it retries `unavailable` (HTTP 503) with the same
    /// request, as [`Self::list`] does, and never falls back to a cached copy.
    pub async fn get(&self, mut req: GetTriggerRequest) -> Result<Option<Trigger>> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.get_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn get_scoped(&self, req: GetTriggerRequest) -> Result<Option<Trigger>> {
        // The trigger is labelled with its symbol from the catalogs.
        self.ctx.settle_catalogs().await;
        let client = self.client();
        let resp = unary::await_auth_retrying_unavailable(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/GetTrigger",
            req,
            unary::TRIGGER_READ_RETRY,
            |req, opts| client.get_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(get_trigger_from_proto(&resp)?.map(|mut trigger| {
            self.label_from_catalogs(&mut trigger);
            trigger
        }))
    }

    /// Names a read trigger's symbol and gives its quantities the pair's base
    /// quantity scale, both from the loaded catalogs. Without them the trigger
    /// keeps an empty symbol and unscaled quantities.
    fn label_from_catalogs(&self, trigger: &mut Trigger) {
        if trigger.symbol.is_empty() {
            trigger.symbol = self.ctx.catalogs.display_symbol(trigger.symbol_id);
        }
        attach_trigger_scale(
            trigger,
            self.ctx
                .catalogs
                .base_quantity_scale_for_symbol_id(trigger.symbol_id),
        );
    }

    /// Retrieve a trigger using the public string ID returned by create and list calls.
    pub async fn get_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<Option<Trigger>> {
        self.get_scoped(GetTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        })
        .await
    }

    fn encode_create_params(&self, params: &CreateTriggerParams) -> Result<CreateTriggerRequest> {
        validate_create_option_applicability(params)?;
        // Resolve the market once: the ID sent, the quantity scale and every
        // symbol check below use the catalog's market, not the caller's text.
        let market = self.ctx.catalogs.require_market(&params.symbol)?;
        let canonical;
        let params = if params.symbol == market.symbol {
            params
        } else {
            canonical = CreateTriggerParams {
                symbol: market.symbol.clone(),
                ..params.clone()
            };
            &canonical
        };
        let qty = coded_qty_scaled(&params.qty, &market, codes::TRIGGER_QTY_INVALID)?;
        let mut intent = TriggerIntent {
            symbol_id: market.symbol_id,
            qty_scaled: qty,
            ..Default::default()
        };
        // The contract's rule is 1 to 36 bytes of UTF-8, with no character
        // set: the ID is sent exactly as given.
        if params.client_trigger_id.is_empty() {
            return Err(field_refusal(
                codes::CLIENT_TRIGGER_ID_INVALID,
                "client_trigger_id is required and must remain stable across retries",
            ));
        }
        if params.client_trigger_id.len() > CLIENT_TRIGGER_ID_MAX_BYTES {
            return Err(field_refusal(
                codes::CLIENT_TRIGGER_ID_INVALID,
                format!(
                    "client_trigger_id must be 1 to {CLIENT_TRIGGER_ID_MAX_BYTES} bytes of UTF-8"
                ),
            ));
        }
        intent.client_trigger_id = params.client_trigger_id.clone();
        if let Some(asset) = params.fee_asset {
            intent.fee_asset = Self::fee_asset(asset, params.side)?.into();
        }
        if let Some(mode) = params.self_trade_prevention_mode.as_deref() {
            intent.self_trade_prevention_mode = Self::stp_mode(mode)?.into();
        }
        let trigger_price_source = match params.trigger_price_source.as_deref() {
            Some(label) => Self::trigger_price_source(label)?,
            None => TriggerPriceSource::TriggerPriceSourceUnspecified,
        };
        if trigger_price_source != TriggerPriceSource::TriggerPriceSourceUnspecified
            && matches!(
                params.trigger_type,
                CreateTriggerType::Twap | CreateTriggerType::Ladder
            )
        {
            return Err(Error::validation(
                "trigger_price_source applies only to stop_loss, take_profit and trailing_stop triggers",
            ));
        }

        let side = match params.side {
            CreateSide::Buy => Side::Buy,
            CreateSide::Sell => Side::Sell,
        };

        intent.strategy = Some(match params.trigger_type {
            CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit => {
                let trigger_price_ticks = params
                    .trigger_price
                    .as_ref()
                    .ok_or_else(|| {
                        field_refusal(
                            codes::TRIGGER_PRICE_INVALID,
                            "stop/take-profit requires trigger_price",
                        )
                    })
                    .and_then(|price| {
                        coded_price_ticks(
                            price,
                            Some(&params.symbol),
                            "trigger_price",
                            false,
                            codes::TRIGGER_PRICE_INVALID,
                        )
                    })?;
                let mut cond = ConditionalTrigger {
                    trigger_price_ticks,
                    side: side.into(),
                    trigger_price_source: trigger_price_source.into(),
                    ..Default::default()
                };
                *cond.child.get_or_insert_default() = self.encode_conditional_child(params)?;
                if matches!(params.trigger_type, CreateTriggerType::StopLoss) {
                    trigger_intent::Strategy::StopLoss(Box::new(cond))
                } else {
                    trigger_intent::Strategy::TakeProfit(Box::new(cond))
                }
            }
            CreateTriggerType::TrailingStop => {
                // A BUY trailing stop carries its maximum price as
                // `limit_price_ticks` and fires a limit IOC child at it; a SELL
                // one has none and fires a market IOC child.
                let mut trailing = TrailingStopTrigger {
                    side: side.into(),
                    trigger_price_source: trigger_price_source.into(),
                    ..Default::default()
                };
                match (params.side, params.limit_price.as_ref()) {
                    (CreateSide::Buy, Some(max_price)) => {
                        max_price.compatible_with(Some(&params.symbol))?;
                        let ticks = max_price.as_ticks();
                        if ticks <= 0 {
                            return Err(field_refusal(
                                codes::BAD_PRICE,
                                "a buy trailing_stop limit_price (its maximum price) must be positive",
                            ));
                        }
                        trailing.limit_price_ticks = ticks;
                    }
                    (CreateSide::Buy, None) => {
                        return Err(Error::validation(
                            "a buy trailing_stop requires limit_price: the maximum price its child pays",
                        ));
                    }
                    (CreateSide::Sell, Some(_)) => {
                        return Err(Error::validation(
                            "limit_price is only valid for a buy trailing_stop (its maximum price)",
                        ));
                    }
                    (CreateSide::Sell, None) => {}
                }
                if params.trailing_distance_ticks.is_none()
                    && params.trailing_distance_bps.is_none()
                {
                    return Err(field_refusal(
                        codes::TRAILING_DISTANCE_INVALID,
                        "trailing_stop requires exactly one of trailing_distance_ticks or trailing_distance_bps",
                    ));
                }
                if params.trailing_distance_ticks.is_some()
                    && params.trailing_distance_bps.is_some()
                {
                    return Err(Error::validation(
                        "trailing_stop requires exactly one of trailing_distance_ticks or trailing_distance_bps",
                    ));
                }
                if params.max_slippage_ticks.is_some() && params.max_slippage_bps.is_some() {
                    return Err(Error::validation(
                        "trailing_stop allows at most one of max_slippage_ticks or max_slippage_bps",
                    ));
                }
                if let Some(ticks) = params.trailing_distance_ticks {
                    if ticks <= 0 {
                        return Err(field_refusal(
                            codes::TRAILING_DISTANCE_INVALID,
                            "trailing_distance_ticks must be positive",
                        ));
                    }
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceTicks(ticks));
                } else if let Some(bps) = params.trailing_distance_bps {
                    validate_bps(
                        "trailing_distance_bps",
                        bps,
                        false,
                        codes::TRAILING_DISTANCE_INVALID,
                    )?;
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(bps));
                } else {
                    return Err(field_refusal(
                        codes::TRAILING_DISTANCE_INVALID,
                        "trailing_stop requires trailing_distance_ticks or trailing_distance_bps",
                    ));
                }
                if let Some(price) = params.activation_price.as_ref() {
                    trailing.activation_price_ticks = coded_price_ticks(
                        price,
                        Some(&params.symbol),
                        "activation_price",
                        true,
                        codes::TRIGGER_ACTIVATION_PRICE_INVALID,
                    )?;
                }
                // A buy that arms at the activation price must be allowed to
                // pay at least that much.
                if matches!(params.side, CreateSide::Buy)
                    && trailing.activation_price_ticks > 0
                    && trailing.limit_price_ticks < trailing.activation_price_ticks
                {
                    return Err(Error::validation(
                        "a buy trailing_stop limit_price (its maximum price) must be at least its activation_price",
                    ));
                }
                if let Some(ticks) = params.max_slippage_ticks {
                    if ticks <= 0 {
                        return Err(field_refusal(
                            codes::SLIPPAGE_TICKS_INVALID,
                            "max_slippage_ticks must be positive",
                        ));
                    }
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(ticks));
                } else if let Some(bps) = params.max_slippage_bps {
                    validate_bps_at_most(
                        "max_slippage_bps",
                        bps,
                        false,
                        MAX_SELLING_SLIPPAGE_BPS,
                        codes::SLIPPAGE_BPS_INVALID,
                    )?;
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(bps));
                }
                trigger_intent::Strategy::TrailingStop(Box::new(trailing))
            }
            CreateTriggerType::Twap => {
                let duration_ms = params.twap_duration_ms.ok_or_else(|| {
                    field_refusal(
                        codes::TWAP_DURATION_INVALID,
                        "twap_duration_ms is required for a twap",
                    )
                })?;
                if duration_ms < TWAP_DURATION_MS_MIN {
                    return Err(field_refusal(
                        codes::TWAP_DURATION_INVALID,
                        format!(
                            "twap_duration_ms {duration_ms} is below the minimum {TWAP_DURATION_MS_MIN}"
                        ),
                    ));
                }
                let slice_interval_ms = params.twap_slice_interval_ms.ok_or_else(|| {
                    field_refusal(
                        codes::TWAP_INTERVAL_INVALID,
                        "twap_slice_interval_ms is required for a twap",
                    )
                })?;
                if slice_interval_ms < TWAP_SLICE_INTERVAL_MS_MIN {
                    return Err(field_refusal(
                        codes::TWAP_INTERVAL_INVALID,
                        format!(
                            "twap_slice_interval_ms {slice_interval_ms} is below the minimum \
                             {TWAP_SLICE_INTERVAL_MS_MIN}"
                        ),
                    ));
                }
                if slice_interval_ms > duration_ms {
                    return Err(field_refusal(
                        codes::TWAP_INTERVAL_INVALID,
                        "twap_slice_interval_ms must not exceed twap_duration_ms",
                    ));
                }
                let mut twap = TwapTrigger {
                    side: side.into(),
                    duration_ms,
                    slice_interval_ms,
                    ..Default::default()
                };
                twap.execution = Some(match params.order_type {
                    CreateOrderType::Market => {
                        let mut ioc = TwapMarketIoc::default();
                        match (params.max_slippage_ticks, params.max_slippage_bps) {
                            (Some(_), Some(_)) => {
                                return Err(Error::validation(
                                    "twap market_ioc allows at most one of max_slippage_ticks or max_slippage_bps",
                                ));
                            }
                            (Some(ticks), None) => {
                                if ticks <= 0 {
                                    return Err(field_refusal(
                                        codes::SLIPPAGE_TICKS_INVALID,
                                        "max_slippage_ticks must be positive",
                                    ));
                                }
                                ioc.max_slippage =
                                    Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(ticks));
                            }
                            (None, Some(bps)) => {
                                // A slice that sells takes no 100% cap: it
                                // would leave no positive price to sell at.
                                let max = match params.side {
                                    CreateSide::Sell => MAX_SELLING_SLIPPAGE_BPS,
                                    CreateSide::Buy => MAX_BPS,
                                };
                                validate_bps_at_most(
                                    "max_slippage_bps",
                                    bps,
                                    false,
                                    max,
                                    codes::SLIPPAGE_BPS_INVALID,
                                )?;
                                ioc.max_slippage =
                                    Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(bps));
                            }
                            (None, None) => {}
                        }
                        twap_trigger::Execution::MarketIoc(Box::new(ioc))
                    }
                    CreateOrderType::Limit => {
                        let price = params.limit_price.as_ref().ok_or_else(|| {
                            field_refusal(codes::BAD_PRICE, "twap limit slices require limit_price")
                        })?;
                        twap_trigger::Execution::LimitGtc(Box::new(TwapLimitGtc {
                            price_ticks: coded_price_ticks(
                                price,
                                Some(&params.symbol),
                                "limit_price",
                                true,
                                codes::BAD_PRICE,
                            )?,
                            ..Default::default()
                        }))
                    }
                });
                trigger_intent::Strategy::Twap(Box::new(twap))
            }
            CreateTriggerType::Ladder => {
                if let Some(dist) = params.ladder_distribution.as_deref() {
                    let dist = dist.trim().to_ascii_lowercase();
                    if !dist.is_empty() && dist != "linear" {
                        return Err(Error::validation(
                            "ladder only supports linear distribution",
                        ));
                    }
                }
                let ladder_price = |price: Option<&crate::Price>, field: &str| {
                    let price = price.ok_or_else(|| {
                        field_refusal(
                            codes::TRIGGER_PRICE_INVALID,
                            format!("ladder requires {field}"),
                        )
                    })?;
                    coded_price_ticks(
                        price,
                        Some(&params.symbol),
                        field,
                        true,
                        codes::TRIGGER_PRICE_INVALID,
                    )
                };
                let price_min_ticks =
                    ladder_price(params.ladder_price_min.as_ref(), "ladder_price_min")?;
                let price_max_ticks =
                    ladder_price(params.ladder_price_max.as_ref(), "ladder_price_max")?;
                let levels = params.ladder_levels.ok_or_else(|| {
                    field_refusal(
                        codes::LADDER_LEVELS_INVALID,
                        "ladder_levels is required for a ladder",
                    )
                })?;
                if !(LADDER_LEVELS_MIN..=LADDER_LEVELS_MAX).contains(&levels) {
                    return Err(field_refusal(
                        codes::LADDER_LEVELS_INVALID,
                        format!(
                            "ladder_levels {levels} is outside {LADDER_LEVELS_MIN}..={LADDER_LEVELS_MAX}"
                        ),
                    ));
                }
                if price_min_ticks <= 0 || price_max_ticks <= price_min_ticks {
                    return Err(field_refusal(
                        codes::TRIGGER_PRICE_INVALID,
                        "ladder prices must be positive and max must exceed min",
                    ));
                }
                let ladder = LadderTrigger {
                    side: side.into(),
                    post_only: params.post_only,
                    price_min_ticks,
                    price_max_ticks,
                    levels,
                    ..Default::default()
                };
                trigger_intent::Strategy::Ladder(Box::new(ladder))
            }
        });
        let mut req = CreateTriggerRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, params.subaccount_id)?,
            ..Default::default()
        };
        *req.trigger.get_or_insert_default() = intent;
        Ok(req)
    }

    /// Map flat (`order_type`, `time_in_force`, `limit_price`, `post_only`) params
    /// onto a stop-loss / take-profit child execution variant.
    fn encode_conditional_child(
        &self,
        params: &CreateTriggerParams,
    ) -> Result<ConditionalChildExecution> {
        let execution = match params.order_type {
            CreateOrderType::Market => {
                if params.post_only {
                    return Err(Error::validation(
                        "post_only is only valid for limit GTC executions",
                    ));
                }
                let max_slippage = match (params.max_slippage_ticks, params.max_slippage_bps) {
                    (Some(_), Some(_)) => {
                        return Err(Error::validation(
                            "a market child allows at most one of max_slippage_ticks or max_slippage_bps",
                        ));
                    }
                    (Some(ticks), None) => {
                        if ticks <= 0 {
                            return Err(field_refusal(
                                codes::SLIPPAGE_TICKS_INVALID,
                                "max_slippage_ticks must be positive",
                            ));
                        }
                        Some(trigger_market_ioc::MaxSlippage::MaxSlippageTicks(ticks))
                    }
                    (None, Some(bps)) => {
                        validate_bps("max_slippage_bps", bps, false, codes::SLIPPAGE_BPS_INVALID)?;
                        // A 100% cap would leave a sell no positive price to sell at.
                        if bps == MAX_BPS && matches!(params.side, CreateSide::Sell) {
                            return Err(field_refusal(
                                codes::MAX_SLIPPAGE_INVALID,
                                "max_slippage_bps of a sell market child must be below 10000",
                            ));
                        }
                        Some(trigger_market_ioc::MaxSlippage::MaxSlippageBps(bps))
                    }
                    (None, None) => None,
                };
                conditional_child_execution::Execution::MarketIoc(Box::new(TriggerMarketIoc {
                    max_slippage,
                    ..Default::default()
                }))
            }
            CreateOrderType::Limit => {
                if params.max_slippage_ticks.is_some() || params.max_slippage_bps.is_some() {
                    return Err(Error::validation(
                        "max_slippage_ticks and max_slippage_bps are only valid for a market child; a limit child is bounded by its limit_price",
                    ));
                }
                let price = params.limit_price.as_ref().ok_or_else(|| {
                    field_refusal(codes::BAD_PRICE, "limit trigger requires limit_price")
                })?;
                let price_ticks = coded_price_ticks(
                    price,
                    Some(&params.symbol),
                    "limit_price",
                    true,
                    codes::BAD_PRICE,
                )?;
                match params.time_in_force {
                    Some(CreateTimeInForce::Ioc) => {
                        if params.post_only {
                            return Err(Error::validation(
                                "post_only is only valid for limit GTC executions",
                            ));
                        }
                        conditional_child_execution::Execution::LimitIoc(Box::new(
                            TriggerLimitIoc {
                                price_ticks,
                                ..Default::default()
                            },
                        ))
                    }
                    Some(CreateTimeInForce::Fok) => {
                        if params.post_only {
                            return Err(Error::validation(
                                "post_only is only valid for limit GTC executions",
                            ));
                        }
                        conditional_child_execution::Execution::LimitFok(Box::new(
                            TriggerLimitFok {
                                price_ticks,
                                ..Default::default()
                            },
                        ))
                    }
                    // gtc or unspecified; validate_create_option_applicability
                    // has already refused gtd
                    _ => conditional_child_execution::Execution::LimitGtc(Box::new(
                        TriggerLimitGtc {
                            price_ticks,
                            post_only: params.post_only,
                            ..Default::default()
                        },
                    )),
                }
            }
        };
        Ok(ConditionalChildExecution {
            execution: Some(execution),
            ..Default::default()
        })
    }

    fn encode_create_oco_params(&self, params: &CreateOcoParams) -> Result<CreateOcoRequest> {
        // Resolve the market once: the ID sent, the quantity scale and the
        // legs' price checks use the catalog's market, not the caller's text.
        let market = self.ctx.catalogs.require_market(&params.symbol)?;
        let qty = coded_qty_scaled(&params.qty, &market, codes::BAD_QTY)?;
        let take_profit =
            self.encode_oco_leg(&params.take_profit, "take_profit", &market.symbol)?;
        let stop_loss = self.encode_oco_leg(&params.stop_loss, "stop_loss", &market.symbol)?;
        // Which way round the legs must be for the side is the server's to
        // refuse.
        let side = match params.side {
            CreateSide::Sell => Side::Sell,
            CreateSide::Buy => Side::Buy,
        };
        let mut oco = OcoIntent {
            symbol_id: market.symbol_id,
            side: side.into(),
            qty_scaled: qty,
            client_list_id: require_client_list_id(&params.client_list_id, "client_list_id")?,
            ..Default::default()
        };
        if let Some(mode) = params.self_trade_prevention_mode.as_deref() {
            let mode = Self::stp_mode(mode)?;
            // The contract does not support DECREMENT on an OCO.
            if mode == SelfTradePreventionMode::Decrement {
                return Err(Error::validation(
                    "self_trade_prevention_mode of an OCO must be expire_taker, expire_maker or expire_both; decrement is not supported on an OCO",
                ));
            }
            oco.self_trade_prevention_mode = mode.into();
        }
        *oco.take_profit.get_or_insert_default() = take_profit;
        *oco.stop_loss.get_or_insert_default() = stop_loss;
        let mut req = CreateOcoRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, params.subaccount_id)?,
            ..Default::default()
        };
        *req.oco.get_or_insert_default() = oco;
        Ok(req)
    }

    /// Map one flat [`OcoLegParams`] onto the leg's trigger price, source and
    /// child [`RiskExecution`]: a market IOC or a limit GTC order.
    fn encode_oco_leg(&self, leg: &OcoLegParams, field: &str, symbol: &str) -> Result<OcoLeg> {
        let trigger_price_ticks = coded_price_ticks(
            &leg.trigger_price,
            Some(symbol),
            &format!("{field} trigger_price"),
            false,
            codes::TRIGGER_PRICE_INVALID,
        )?;
        let execution = match (leg.order_type, leg.limit_price.as_ref()) {
            (CreateOrderType::Market, None) => risk_execution::Execution::MarketIoc(Box::default()),
            (CreateOrderType::Market, Some(_)) => {
                return Err(Error::validation(format!(
                    "{field} market leg must not set limit_price"
                )));
            }
            (CreateOrderType::Limit, Some(price)) => {
                risk_execution::Execution::LimitGtc(Box::new(RiskLimitGtc {
                    price_ticks: coded_price_ticks(
                        price,
                        Some(symbol),
                        &format!("{field} limit_price"),
                        true,
                        codes::BAD_PRICE,
                    )?,
                    ..Default::default()
                }))
            }
            (CreateOrderType::Limit, None) => {
                return Err(field_refusal(
                    codes::BAD_PRICE,
                    format!("{field} limit leg requires limit_price"),
                ));
            }
        };
        // The labels trigger reads use. The contract does not support
        // MARK_PRICE on an OCO leg. Empty sends nothing (last trade price).
        let trigger_price_source = match leg
            .trigger_price_source
            .as_deref()
            .map(|label| label.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") => TriggerPriceSource::TriggerPriceSourceUnspecified,
            Some("last") => TriggerPriceSource::LastPrice,
            Some("index") => TriggerPriceSource::IndexPrice,
            Some("mark") => {
                return Err(Error::validation(format!(
                    "{field} trigger_price_source mark is not supported on an OCO: spot markets have no mark price; use last or index"
                )));
            }
            Some(_) => {
                return Err(Error::validation(format!(
                    "{field} trigger_price_source must be last or index"
                )));
            }
        };
        let mut out = OcoLeg {
            trigger_price_ticks,
            trigger_price_source: trigger_price_source.into(),
            ..Default::default()
        };
        *out.child.get_or_insert_default() = RiskExecution {
            execution: Some(execution),
            ..Default::default()
        };
        Ok(out)
    }

    fn encode_batch_cancel(
        &self,
        trigger_ids: &[String],
        subaccount_id: SubAccountScope,
        request_id: Option<String>,
    ) -> Result<BatchCancelTriggersRequest> {
        if trigger_ids.is_empty() {
            return Err(Error::validation(
                "batch_cancel requires at least one trigger_id",
            ));
        }
        if trigger_ids.len() > MAX_BATCH_CANCEL_TRIGGERS {
            return Err(Error::validation(format!(
                "batch_cancel accepts at most {MAX_BATCH_CANCEL_TRIGGERS} trigger_ids; received {}",
                trigger_ids.len()
            )));
        }
        let mut ids = Vec::with_capacity(trigger_ids.len());
        for raw in trigger_ids {
            let id = id_to_u64(raw, "trigger_id")?;
            if id == 0 {
                return Err(Error::validation(
                    "trigger_id must be a non-zero trigger ID",
                ));
            }
            if ids.contains(&id) {
                return Err(Error::validation(format!(
                    "duplicate trigger_id {raw:?} in batch_cancel"
                )));
            }
            ids.push(id);
        }
        Ok(BatchCancelTriggersRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            request_id: super::orders::OrdersService::coalesce_request_id(
                request_id,
                "batch-cancel-triggers",
            )?,
            trigger_ids: ids,
            ..Default::default()
        })
    }

    fn encode_modify_params(&self, params: &ModifyTriggerParams) -> Result<ModifyTriggerRequest> {
        if params.trigger_price.is_none()
            && params.activation_price.is_none()
            && params.trailing_distance_ticks.is_none()
            && params.trailing_distance_bps.is_none()
            && params.max_slippage_ticks.is_none()
            && params.max_slippage_bps.is_none()
        {
            return Err(Error::validation(
                "modify requires at least one of trigger_price, trailing_distance_ticks, trailing_distance_bps, activation_price, max_slippage_ticks, or max_slippage_bps",
            ));
        }
        let mut req = ModifyTriggerRequest {
            trigger_id: id_to_u64(&params.trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, params.subaccount_id)?,
            symbol_id: self
                .ctx
                .catalogs
                .optional_symbol_id(params.symbol.as_deref(), params.symbol_id)?,
            ..Default::default()
        };
        // Zero targets the trigger's own market, which is not known here; a
        // known market refuses a price bound to another one.
        let market = self.ctx.catalogs.symbol_for_symbol_id(req.symbol_id);
        // A market that can't be named can't be checked against a bound
        // price, so the price is refused rather than sent unchecked.
        let bound = [&params.trigger_price, &params.activation_price]
            .into_iter()
            .flatten()
            .find_map(|price| price.symbol());
        if let (Some(bound), None, true) = (bound, market.as_ref(), req.symbol_id != 0) {
            return Err(Error::validation(format!(
                "triggers.modify cannot check a price bound to {bound} against symbol_id {}: the market is not in the catalogs; hydrate catalogs or pass an unbound price",
                req.symbol_id
            )));
        }
        if let Some(price) = params.trigger_price.as_ref() {
            req.trigger_price_ticks = Some(coded_price_ticks(
                price,
                market.as_deref(),
                "trigger_price",
                true,
                codes::TRIGGER_PRICE_INVALID,
            )?);
        }
        if let Some(price) = params.activation_price.as_ref() {
            req.activation_price_ticks = Some(coded_price_ticks(
                price,
                market.as_deref(),
                "activation_price",
                true,
                codes::TRIGGER_ACTIVATION_PRICE_INVALID,
            )?);
        }
        if params.trailing_distance_ticks.is_some() && params.trailing_distance_bps.is_some() {
            return Err(Error::validation(
                "modify allows at most one trailing distance representation",
            ));
        }
        if params.max_slippage_ticks.is_some() && params.max_slippage_bps.is_some() {
            return Err(Error::validation(
                "modify allows at most one max slippage representation",
            ));
        }
        if let Some(ticks) = params.trailing_distance_ticks {
            if ticks <= 0 {
                return Err(field_refusal(
                    codes::TRAILING_DISTANCE_INVALID,
                    "trailing_distance_ticks must be positive",
                ));
            }
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceTicks(ticks));
        } else if let Some(bps) = params.trailing_distance_bps {
            validate_bps(
                "trailing_distance_bps",
                bps,
                false,
                codes::TRAILING_DISTANCE_INVALID,
            )?;
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceBps(bps));
        }
        if let Some(ticks) = params.max_slippage_ticks {
            if ticks < 0 {
                return Err(field_refusal(
                    codes::SLIPPAGE_TICKS_INVALID,
                    "max_slippage_ticks must be non-negative",
                ));
            }
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageTicks(ticks));
        } else if let Some(bps) = params.max_slippage_bps {
            // 0 clears; up to 10000 for a buy stop-loss or take-profit market
            // child, as at creation. The server refuses 10000 for a trailing
            // stop and a sell one with MAX_SLIPPAGE_INVALID.
            validate_bps("max_slippage_bps", bps, true, codes::SLIPPAGE_BPS_INVALID)?;
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageBps(bps));
        }
        Ok(req)
    }

    fn fee_asset(asset: FeeAsset, side: CreateSide) -> Result<ProtoFeeAsset> {
        match (asset, side) {
            (FeeAsset::Quote, _) => Ok(ProtoFeeAsset::Quote),
            (FeeAsset::Base, CreateSide::Buy) => Ok(ProtoFeeAsset::Base),
            (FeeAsset::Base, CreateSide::Sell) => Err(field_refusal(
                codes::FEE_ASSET_NOT_ALLOWED,
                "fee_asset=base is only valid for BUY triggers",
            )),
        }
    }

    /// Same labels as trigger reads; empty sends nothing (last trade price).
    fn trigger_price_source(label: &str) -> Result<TriggerPriceSource> {
        match label.trim().to_ascii_lowercase().as_str() {
            "" => Ok(TriggerPriceSource::TriggerPriceSourceUnspecified),
            "last" => Ok(TriggerPriceSource::LastPrice),
            "index" => Ok(TriggerPriceSource::IndexPrice),
            "mark" => Err(Error::validation(
                "trigger_price_source mark is not supported: spot markets have no mark price",
            )),
            _ => Err(Error::validation(
                "trigger_price_source must be last or index",
            )),
        }
    }

    /// A trigger create's self-trade prevention mode label, as order reads
    /// spell it, in any case.
    fn stp_mode(label: &str) -> Result<SelfTradePreventionMode> {
        match label.to_ascii_lowercase().as_str() {
            "expire_taker" => Ok(SelfTradePreventionMode::ExpireTaker),
            "expire_maker" => Ok(SelfTradePreventionMode::ExpireMaker),
            "expire_both" => Ok(SelfTradePreventionMode::ExpireBoth),
            "decrement" => Ok(SelfTradePreventionMode::Decrement),
            _ => Err(Error::validation(
                "self_trade_prevention_mode must be expire_taker, expire_maker, expire_both, or decrement",
            )),
        }
    }

    /// Create a trigger. Prices/qty must be `Price` / `Quantity` wrappers.
    pub async fn create(&self, params: CreateTriggerParams) -> Result<TriggerMutationResult> {
        self.ctx.wait_for_catalogs().await?;
        let req = self.encode_create_params(&params)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CreateTrigger",
            req,
            |req, opts| client.create_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_mutation_from_create(&resp)
    }

    /// An unset `subaccount_id` takes the configured default sub-account, as
    /// the typed helpers do; a sub-account set on `req` is sent unchanged.
    /// A trigger that has already ended is refused as for [`Self::cancel_by_id`].
    pub async fn cancel(&self, mut req: CancelTriggerRequest) -> Result<TriggerMutationResult> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.cancel_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn cancel_scoped(&self, req: CancelTriggerRequest) -> Result<TriggerMutationResult> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CancelTrigger",
            req,
            |req, opts| client.cancel_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_mutation_from_cancel(&resp)
    }

    /// Cancel a trigger using the public string ID returned by create and list calls.
    ///
    /// A trigger that has already ended (completed, canceled or failed) is
    /// refused with `ERROR_CODE_TRIGGER_STATUS_INVALID` (HTTP 400) for at
    /// least 60 seconds after it ended, a repeat of a cancel that succeeded
    /// included ([`Error::is_trigger_already_ended`] is true); after that it
    /// may be refused with `ERROR_CODE_TRIGGER_NOT_FOUND`. Both mean "already
    /// ended, nothing to do" for a trigger you know you created; read it with
    /// [`Self::get_by_id`] for how it ended. `TRIGGER_NOT_FOUND` also names an
    /// ID that is none of the account's triggers.
    pub async fn cancel_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<TriggerMutationResult> {
        self.cancel_scoped(CancelTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        })
        .await
    }

    /// Create an OCO: a take-profit and a stop-loss over a quantity the
    /// account already holds, where the first leg to trade cancels the other
    /// once the whole quantity has traded (see [`CreateOcoParams`]). Prices
    /// and quantity must be `Price` / `Quantity` wrappers.
    ///
    /// Reuse `client_list_id` to retry: the identical params with the same ID
    /// return the same OCO. A create sent but not confirmed in time fails
    /// with `TRIGGER_CREATE_OUTCOME_UNKNOWN`
    /// ([`Error::mutation_outcome_unknown`] is true): the OCO may exist, so
    /// list the triggers with its `client_list_id`
    /// ([`ListTriggersOpts::client_list_id`]) before retrying. An OCO whose
    /// trigger price the last price has already reached is rejected on both
    /// legs after the create answers; read the legs for their status.
    pub async fn create_oco(&self, params: CreateOcoParams) -> Result<CreateOcoResult> {
        self.ctx.wait_for_catalogs().await?;
        let req = self.encode_create_oco_params(&params)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CreateOco",
            req,
            |req, opts| client.create_oco_with_options(req, opts),
        )
        .await?
        .into_owned();
        create_oco_from_proto(&resp)
    }

    /// An unset `subaccount_id` takes the configured default sub-account, as
    /// the typed helpers do; a sub-account set on `req` is sent unchanged.
    pub async fn cancel_oco(&self, mut req: CancelOcoRequest) -> Result<CancelOcoResult> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.cancel_oco_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn cancel_oco_scoped(&self, req: CancelOcoRequest) -> Result<CancelOcoResult> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CancelOco",
            req,
            |req, opts| client.cancel_oco_with_options(req, opts),
        )
        .await?
        .into_owned();
        cancel_oco_from_proto(&resp)
    }

    /// Cancel both legs of an OCO using the public `order_list_id` returned by
    /// [`Self::create_oco`]. An ID that is not an OCO of the account, such as
    /// a leg's trigger ID, is refused with `TRIGGER_NOT_FOUND` and nothing is
    /// canceled; cancel a single leg with [`Self::cancel_by_id`], which
    /// cancels both.
    pub async fn cancel_oco_by_id(
        &self,
        order_list_id: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<CancelOcoResult> {
        let order_list_id = id_to_u64(order_list_id, "order_list_id")?;
        if order_list_id == 0 {
            return Err(Error::validation("order_list_id must be a non-zero OCO ID"));
        }
        self.cancel_oco_scoped(CancelOcoRequest {
            order_list_id,
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        })
        .await
    }

    /// Batch-cancel up to 50 distinct triggers by their public IDs. Each is
    /// canceled as [`Self::cancel_by_id`] cancels it and answered on its own,
    /// in request order; one rejection does not stop the others.
    ///
    /// A `request_id` is generated when omitted. Provide a stable non-empty
    /// value when retrying the same logical batch: within 2 minutes, the same
    /// `request_id` with the same trigger IDs returns the original results and
    /// cancels nothing again, so a timed-out batch may be sent again
    /// unchanged; omitting it on retry mints a new id. The same `request_id`
    /// with other trigger IDs rejects every item with
    /// `ERROR_CODE_CONFLICT_IDEMPOTENCY_KEY_REUSE`.
    pub async fn batch_cancel(
        &self,
        trigger_ids: Vec<String>,
        subaccount_id: SubAccountScope,
        request_id: Option<String>,
    ) -> Result<BatchCancelTriggersResult> {
        let req = self.encode_batch_cancel(&trigger_ids, subaccount_id, request_id)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/BatchCancelTriggers",
            req,
            |req, opts| client.batch_cancel_triggers_with_options(req, opts),
        )
        .await?
        .into_owned();
        batch_cancel_triggers_from_proto(&resp)
    }

    /// An unset `subaccount_id` takes the configured default sub-account, as
    /// the typed helpers do; a sub-account set on `req` is sent unchanged.
    /// A trigger that has already ended is refused as for [`Self::pause_by_id`].
    pub async fn pause(&self, mut req: PauseTriggerRequest) -> Result<TriggerMutationResult> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.pause_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn pause_scoped(&self, req: PauseTriggerRequest) -> Result<TriggerMutationResult> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/PauseTrigger",
            req,
            |req, opts| client.pause_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_mutation_from_pause(&resp)
    }

    /// Pause a trigger using the public string ID returned by create and list calls.
    ///
    /// A trigger that has already ended (completed, canceled or failed) is
    /// refused with `ERROR_CODE_TRIGGER_STATUS_INVALID` (HTTP 400) for at
    /// least 60 seconds after it ended, a repeat of a cancel that succeeded
    /// included ([`Error::is_trigger_already_ended`] is true); after that it
    /// may be refused with `ERROR_CODE_TRIGGER_NOT_FOUND`. Both mean "already
    /// ended, nothing to do" for a trigger you know you created; read it with
    /// [`Self::get_by_id`] for how it ended. `TRIGGER_NOT_FOUND` also names an
    /// ID that is none of the account's triggers.
    pub async fn pause_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: SubAccountScope,
    ) -> Result<TriggerMutationResult> {
        self.pause_scoped(PauseTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        })
        .await
    }

    /// An unset `subaccount_id` takes the configured default sub-account, as
    /// the typed helpers do; a sub-account set on `req` is sent unchanged.
    /// A trigger that has already ended is refused as for [`Self::resume_by_id`].
    pub async fn resume(&self, mut req: ResumeTriggerRequest) -> Result<TriggerMutationResult> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.resume_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn resume_scoped(&self, req: ResumeTriggerRequest) -> Result<TriggerMutationResult> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ResumeTrigger",
            req,
            |req, opts| client.resume_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_mutation_from_resume(&resp)
    }

    /// Resume a trigger using the public string ID returned by create and list calls.
    ///
    /// A trigger that has already ended (completed, canceled or failed) is
    /// refused with `ERROR_CODE_TRIGGER_STATUS_INVALID` (HTTP 400) for at
    /// least 60 seconds after it ended, a repeat of a cancel that succeeded
    /// included ([`Error::is_trigger_already_ended`] is true); after that it
    /// may be refused with `ERROR_CODE_TRIGGER_NOT_FOUND`. Both mean "already
    /// ended, nothing to do" for a trigger you know you created; read it with
    /// [`Self::get_by_id`] for how it ended. `TRIGGER_NOT_FOUND` also names an
    /// ID that is none of the account's triggers.
    pub async fn resume_by_id(
        &self,
        trigger_id: &str,
        symbol: Option<&str>,
        subaccount_id: SubAccountScope,
    ) -> Result<TriggerMutationResult> {
        self.resume_with(ResumeTriggerParams {
            trigger_id: trigger_id.to_owned(),
            symbol: symbol.map(str::to_owned),
            symbol_id: None,
            subaccount_id,
        })
        .await
    }

    /// Resume a trigger. `symbol` / `symbol_id` are optional: leave both out
    /// to target the trigger's own market. An API key restricted to some
    /// markets must name the market.
    /// A trigger that has already ended is refused as for
    /// [`Self::resume_by_id`].
    pub async fn resume_with(&self, params: ResumeTriggerParams) -> Result<TriggerMutationResult> {
        if params.symbol.is_some() {
            self.ctx.wait_for_catalogs().await?;
        }
        self.resume_scoped(ResumeTriggerRequest {
            trigger_id: id_to_u64(&params.trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, params.subaccount_id)?,
            symbol_id: self
                .ctx
                .catalogs
                .optional_symbol_id(params.symbol.as_deref(), params.symbol_id)?,
            ..Default::default()
        })
        .await
    }

    /// Modify a trigger. Price fields must be `Price` wrappers. When `symbol`
    /// or `symbol_id` is given, a price bound to another market is refused.
    /// A running ladder or TWAP is refused with
    /// `ERROR_CODE_TRIGGER_NOT_MODIFIABLE`; it supports only cancel.
    ///
    /// A trigger that has already ended (completed, canceled or failed) is
    /// refused with `ERROR_CODE_TRIGGER_STATUS_INVALID` (HTTP 400) for at
    /// least 60 seconds after it ended, a repeat of a cancel that succeeded
    /// included ([`Error::is_trigger_already_ended`] is true); after that it
    /// may be refused with `ERROR_CODE_TRIGGER_NOT_FOUND`. Both mean "already
    /// ended, nothing to do" for a trigger you know you created; read it with
    /// [`Self::get_by_id`] for how it ended. `TRIGGER_NOT_FOUND` also names an
    /// ID that is none of the account's triggers.
    pub async fn modify(&self, params: ModifyTriggerParams) -> Result<TriggerMutationResult> {
        // A symbol is resolved through the catalogs; a symbol ID needs them to
        // check a price bound to a market.
        let bound_price = [&params.trigger_price, &params.activation_price]
            .into_iter()
            .flatten()
            .any(|price| price.symbol().is_some());
        if params.symbol.is_some() || (params.symbol_id.is_some() && bound_price) {
            self.ctx.wait_for_catalogs().await?;
        }
        let req = self.encode_modify_params(&params)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ModifyTrigger",
            req,
            |req, opts| client.modify_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_mutation_from_modify(&resp)
    }

    pub async fn list_events(
        &self,
        mut req: ListTriggerEventsRequest,
    ) -> Result<TriggerEventsList> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        self.list_events_scoped(req).await
    }

    /// Sends `req` with its sub-account as given: the typed helpers resolve
    /// it from a [`SubAccountScope`] first, so [`SubAccountScope::Main`] stays the main
    /// account.
    async fn list_events_scoped(&self, req: ListTriggerEventsRequest) -> Result<TriggerEventsList> {
        let client = self.client();
        let resp = unary::await_auth_retrying_unavailable(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggerEvents",
            req,
            unary::TRIGGER_READ_RETRY,
            |req, opts| client.list_trigger_events_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_events_list_from_proto(&resp)
    }

    /// List a trigger's events by the public trigger ID that create and list
    /// calls return (base58, or decimal), with the filters of
    /// [`ListTriggerEventsOpts`]. The configured default sub-account applies
    /// when `opts.subaccount_id` is `None`. An ID that does not parse, or an
    /// unknown event type, is refused with `Error::Validation` before sending.
    pub async fn list_events_by_id(
        &self,
        trigger_id: &str,
        opts: ListTriggerEventsOpts,
    ) -> Result<TriggerEventsList> {
        use crate::codecs::decode::trigger_event_type_from_label;
        let mut req = ListTriggerEventsRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id: scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?,
            // Proto u32: 0 means omit / server default.
            limit: opts.limit.unwrap_or(0),
            page_token: opts.page_token.unwrap_or_default(),
            ..Default::default()
        };
        if let Some(label) = opts.event_type.as_deref() {
            req.event_type = trigger_event_type_from_label(label)
                .map_err(Error::validation)?
                .into();
        }
        self.list_events_scoped(req).await
    }

    /// Subscribe to private trigger updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<Trigger>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:triggers:{account}:proto");
        // Each trigger takes its pair's symbol and base quantity scale from
        // the catalogs loaded when it arrives, as `get` and `list` do.
        let catalogs = self.ctx.catalogs.clone();
        self.ctx
            .realtime
            .subscribe_proto(&channel, move |bytes: &[u8]| {
                let mut trigger = crate::codecs::decode::trigger_from_bytes(bytes)?;
                trigger.symbol = catalogs.display_symbol(trigger.symbol_id);
                let scale = catalogs.base_quantity_scale_for_symbol_id(trigger.symbol_id);
                attach_trigger_scale(&mut trigger, scale);
                Ok(trigger)
            })
            .await
    }

    /// Subscribe to private trigger events (requires `realtime` feature).
    ///
    /// Each publication batches events; every event arrives on its own,
    /// oldest first per symbol. An event that fails to decode is reported
    /// through the subscription's error callback without dropping the rest
    /// of its batch.
    pub async fn subscribe_events(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<TriggerEvent>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:triggers:events:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto_batch(
                &channel,
                crate::codecs::decode::trigger_event_batch_from_bytes,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa::Message;
    use serde_json::json;

    fn client() -> crate::Client {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": "BTC-USDT",
                    "symbol_id": 7,
                    "base_quantity_scale": 8
                }]
            }))
            .expect("hydrate");
        client
    }

    fn create_params(
        qty: crate::Quantity,
        trigger_price: crate::Price,
        limit_price: crate::Price,
    ) -> CreateTriggerParams {
        CreateTriggerParams {
            symbol: "BTC-USDT".into(),
            trigger_type: CreateTriggerType::StopLoss,
            side: CreateSide::Sell,
            order_type: CreateOrderType::Limit,
            qty,
            trigger_price: Some(trigger_price),
            limit_price: Some(limit_price),
            trigger_price_source: None,
            time_in_force: None,
            subaccount_id: SubAccountScope::Default,
            client_trigger_id: "trigger-equivalence".into(),
            post_only: false,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
            twap_duration_ms: None,
            twap_slice_interval_ms: None,
            ladder_price_min: None,
            ladder_price_max: None,
            ladder_levels: None,
            ladder_distribution: None,
            fee_asset: None,
            self_trade_prevention_mode: None,
        }
    }

    /// `decrement` self-trade prevention is valid on trigger creates, in any
    /// case; another label is refused before sending with a message that
    /// lists the modes. An OCO refuses it (see
    /// `oco_create_refuses_mark_and_decrement_before_sending`).
    #[test]
    fn trigger_creates_send_decrement_self_trade_prevention() {
        let client = client();
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        for label in ["decrement", "DECREMENT"] {
            params.self_trade_prevention_mode = Some(label.into());
            let wire = client.triggers.encode_create_params(&params).unwrap();
            assert_eq!(
                wire.trigger.as_option().unwrap().self_trade_prevention_mode,
                SelfTradePreventionMode::Decrement,
                "{label}"
            );
        }
        params.self_trade_prevention_mode = Some("decrease".into());
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(
            matches!(&err, Error::Validation(message) if message.contains("expire_both, or decrement")),
            "{err:?}"
        );
    }

    #[test]
    fn decimal_and_scaled_trigger_encode_identically() {
        let client = client();
        let decimal = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        let scaled = create_params(
            crate::Quantity::from_scaled(
                10_000_000,
                Some(8),
                crate::QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(7),
            )
            .unwrap(),
            crate::Price::from_ticks(49_000_000_000_000, Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_ticks(48_950_000_000_000, Some("BTC-USDT".into())).unwrap(),
        );

        let decimal_wire = client.triggers.encode_create_params(&decimal).unwrap();
        let scaled_wire = client.triggers.encode_create_params(&scaled).unwrap();
        assert_eq!(decimal_wire.encode_to_vec(), scaled_wire.encode_to_vec());
    }

    #[test]
    fn conditional_trigger_rejects_post_only_outside_limit_gtc() {
        let client = client();
        let qty =
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap();
        let trigger_price =
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap();
        let limit_price = crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap();
        let base = create_params(qty, trigger_price, limit_price.clone());

        for (order_type, time_in_force, child_limit_price) in [
            (CreateOrderType::Market, None, None),
            (
                CreateOrderType::Limit,
                Some(CreateTimeInForce::Ioc),
                Some(limit_price.clone()),
            ),
            (
                CreateOrderType::Limit,
                Some(CreateTimeInForce::Fok),
                Some(limit_price.clone()),
            ),
        ] {
            let params = CreateTriggerParams {
                post_only: true,
                order_type,
                time_in_force,
                limit_price: child_limit_price,
                ..base.clone()
            };
            let err = client.triggers.encode_create_params(&params).unwrap_err();
            assert!(err.to_string().contains("limit GTC"), "{err}");
        }
    }

    fn market_stop(side: CreateSide) -> CreateTriggerParams {
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.side = side;
        params.order_type = CreateOrderType::Market;
        params.time_in_force = None;
        params.limit_price = None;
        params
    }

    fn market_child_slippage(
        wire: &CreateTriggerRequest,
    ) -> Option<crate::proto::triggers::v1::trigger_market_ioc::MaxSlippage> {
        use crate::proto::triggers::v1::trigger_intent::Strategy;
        let intent = wire.trigger.as_option().expect("trigger intent");
        let (Some(Strategy::StopLoss(cond)) | Some(Strategy::TakeProfit(cond))) =
            intent.strategy.as_ref()
        else {
            panic!("expected a conditional strategy, got {:?}", intent.strategy);
        };
        match cond
            .child
            .as_option()
            .and_then(|child| child.execution.as_ref())
        {
            Some(conditional_child_execution::Execution::MarketIoc(market)) => {
                market.max_slippage.clone()
            }
            other => panic!("expected a market child, got {other:?}"),
        }
    }

    #[test]
    fn market_stop_and_take_profit_send_their_slippage_cap() {
        use crate::proto::triggers::v1::trigger_market_ioc::MaxSlippage;

        let client = client();
        let mut stop = market_stop(CreateSide::Sell);
        stop.max_slippage_bps = Some(150);
        let wire = client.triggers.encode_create_params(&stop).unwrap();
        assert_eq!(
            market_child_slippage(&wire),
            Some(MaxSlippage::MaxSlippageBps(150))
        );

        // A BUY stop with a market child is accepted; its cap is absolute ticks here.
        let mut buy_take_profit = market_stop(CreateSide::Buy);
        buy_take_profit.trigger_type = CreateTriggerType::TakeProfit;
        buy_take_profit.max_slippage_ticks = Some(25_000_000_000);
        let wire = client
            .triggers
            .encode_create_params(&buy_take_profit)
            .unwrap();
        assert_eq!(
            market_child_slippage(&wire),
            Some(MaxSlippage::MaxSlippageTicks(25_000_000_000))
        );

        // Without a cap the pair default applies: nothing on the wire.
        let wire = client
            .triggers
            .encode_create_params(&market_stop(CreateSide::Sell))
            .unwrap();
        assert_eq!(market_child_slippage(&wire), None);
    }

    #[test]
    fn conditional_slippage_cap_is_validated_and_limit_children_reject_it() {
        let client = client();
        let mut both = market_stop(CreateSide::Sell);
        both.max_slippage_ticks = Some(1);
        both.max_slippage_bps = Some(1);
        let err = client.triggers.encode_create_params(&both).unwrap_err();
        assert!(err.to_string().contains("at most one"), "{err}");

        let mut zero_ticks = market_stop(CreateSide::Sell);
        zero_ticks.max_slippage_ticks = Some(0);
        assert!(client.triggers.encode_create_params(&zero_ticks).is_err());

        let mut too_many_bps = market_stop(CreateSide::Buy);
        too_many_bps.max_slippage_bps = Some(10_001);
        assert!(client.triggers.encode_create_params(&too_many_bps).is_err());
        // A buy accepts a 100% cap; a sell does not.
        too_many_bps.max_slippage_bps = Some(10_000);
        assert!(client.triggers.encode_create_params(&too_many_bps).is_ok());
        let mut sell_full = market_stop(CreateSide::Sell);
        sell_full.max_slippage_bps = Some(10_000);
        let err = client
            .triggers
            .encode_create_params(&sell_full)
            .unwrap_err();
        assert!(err.to_string().contains("below 10000"), "{err}");

        let mut limit = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        limit.max_slippage_bps = Some(50);
        let err = client.triggers.encode_create_params(&limit).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
        assert!(err.to_string().contains("market child"), "{err}");
    }

    /// A trigger read's price-source label feeds create unchanged.
    #[test]
    fn trigger_read_round_trips_its_trigger_price_source() {
        use crate::codecs::decode::trigger_from_proto;
        use crate::models::TriggerDetails;
        use crate::proto::triggers::v1::{
            StopDetails, TrailingDetails, Trigger as ProtoTrigger, trigger,
            trigger_intent::Strategy,
        };

        let stop_read = trigger_from_proto(&ProtoTrigger {
            trigger_id: 1,
            symbol_id: 7,
            runtime_details: Some(trigger::RuntimeDetails::Stop(Box::new(StopDetails {
                trigger_price_ticks: 49_000_000_000_000,
                trigger_price_source: TriggerPriceSource::IndexPrice.into(),
                ..Default::default()
            }))),
            ..Default::default()
        });
        let Some(TriggerDetails::Stop(stop)) = stop_read.details else {
            panic!("expected stop details");
        };
        let trailing_read = trigger_from_proto(&ProtoTrigger {
            trigger_id: 2,
            symbol_id: 7,
            runtime_details: Some(trigger::RuntimeDetails::Trailing(Box::new(
                TrailingDetails {
                    trailing_distance_bps: 25,
                    trigger_price_source: TriggerPriceSource::IndexPrice.into(),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        });
        let Some(TriggerDetails::Trailing(trailing_details)) = trailing_read.details else {
            panic!("expected trailing details");
        };

        let client = client();
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        for (label, want) in [
            (
                stop.trigger_price_source.clone(),
                TriggerPriceSource::IndexPrice,
            ),
            ("last".to_owned(), TriggerPriceSource::LastPrice),
            (
                String::new(),
                TriggerPriceSource::TriggerPriceSourceUnspecified,
            ),
        ] {
            for trigger_type in [CreateTriggerType::StopLoss, CreateTriggerType::TakeProfit] {
                params.trigger_type = trigger_type;
                params.trigger_price_source = Some(label.clone());
                let intent = client
                    .triggers
                    .encode_create_params(&params)
                    .unwrap()
                    .trigger
                    .expect("trigger intent");
                let (Some(Strategy::StopLoss(cond)) | Some(Strategy::TakeProfit(cond))) =
                    intent.strategy.as_ref()
                else {
                    panic!("expected a conditional strategy, got {:?}", intent.strategy);
                };
                assert_eq!(cond.trigger_price_source, want, "{label:?}");
            }
        }
        params.trigger_price_source = None;
        let intent = client
            .triggers
            .encode_create_params(&params)
            .unwrap()
            .trigger
            .expect("trigger intent");
        let Some(Strategy::TakeProfit(cond)) = intent.strategy.as_ref() else {
            panic!("expected TakeProfit strategy, got {:?}", intent.strategy);
        };
        assert_eq!(
            cond.trigger_price_source,
            TriggerPriceSource::TriggerPriceSourceUnspecified
        );

        let mut trailing = trailing_create(None);
        trailing.trigger_price_source = Some(trailing_details.trigger_price_source.clone());
        let intent = client
            .triggers
            .encode_create_params(&trailing)
            .unwrap()
            .trigger
            .expect("trigger intent");
        let Some(Strategy::TrailingStop(wire)) = intent.strategy.as_ref() else {
            panic!("expected TrailingStop strategy, got {:?}", intent.strategy);
        };
        assert_eq!(wire.trigger_price_source, TriggerPriceSource::IndexPrice);

        // TWAP and ladder triggers have no watched price.
        let mut twap = params.clone();
        twap.trigger_type = CreateTriggerType::Twap;
        twap.trigger_price = None;
        twap.trigger_price_source = Some("index".into());
        let err = client.triggers.encode_create_params(&twap).unwrap_err();
        assert!(err.to_string().contains("trigger_price_source"), "{err}");
        let mut ladder = twap;
        ladder.trigger_type = CreateTriggerType::Ladder;
        ladder.limit_price = None;
        let err = client.triggers.encode_create_params(&ladder).unwrap_err();
        assert!(err.to_string().contains("trigger_price_source"), "{err}");
    }

    #[test]
    fn buy_trailing_stop_sends_its_maximum_price() {
        use crate::proto::triggers::v1::trigger_intent::Strategy;

        let client = client();
        let mut params = trailing_create(None);
        params.side = CreateSide::Buy;
        params.activation_price =
            Some(crate::Price::from_decimal_str("80000", Some("BTC-USDT".into())).unwrap());
        params.limit_price =
            Some(crate::Price::from_decimal_str("85000.25", Some("BTC-USDT".into())).unwrap());
        let wire = client.triggers.encode_create_params(&params).unwrap();
        let intent = wire.trigger.expect("trigger intent");
        let Some(Strategy::TrailingStop(trailing)) = intent.strategy.as_ref() else {
            panic!("expected TrailingStop strategy, got {:?}", intent.strategy);
        };
        assert_eq!(
            trailing.side.as_known(),
            Some(crate::proto::orders::v1::Side::Buy)
        );
        assert_eq!(trailing.limit_price_ticks, 85_000_250_000_000);
        assert_eq!(trailing.activation_price_ticks, 80_000_000_000_000);
        assert_eq!(intent.qty_scaled, 10_000_000);
    }

    #[test]
    fn buy_trailing_stop_requires_a_maximum_price() {
        let client = client();
        let mut params = trailing_create(None);
        params.side = CreateSide::Buy;
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
        assert!(err.to_string().contains("requires limit_price"), "{err}");

        params.limit_price = Some(crate::Price::from_ticks(0, None).unwrap());
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(err.to_string().contains("must be positive"), "{err}");
    }

    #[test]
    fn sell_trailing_stop_rejects_a_maximum_price() {
        let client = client();
        let mut params = trailing_create(None);
        params.limit_price =
            Some(crate::Price::from_decimal_str("85000.25", Some("BTC-USDT".into())).unwrap());
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
        assert!(
            err.to_string()
                .contains("only valid for a buy trailing_stop"),
            "{err}"
        );
    }

    #[test]
    fn trailing_stop_sell_encodes_trailing_strategy() {
        use crate::proto::triggers::v1::trigger_intent::Strategy;

        let client = client();
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::TrailingStop;
        params.order_type = CreateOrderType::Market;
        params.side = CreateSide::Sell;
        params.trailing_distance_bps = Some(100);
        params.trigger_price = None;
        params.limit_price = None;
        let wire = client.triggers.encode_create_params(&params).unwrap();
        let intent = wire.trigger.expect("trigger intent");
        assert_eq!(intent.symbol_id, 7);
        let Some(Strategy::TrailingStop(trailing)) = intent.strategy.as_ref() else {
            panic!("expected TrailingStop strategy, got {:?}", intent.strategy);
        };
        assert_eq!(
            trailing.side.as_known(),
            Some(crate::proto::orders::v1::Side::Sell)
        );
        assert_eq!(trailing.limit_price_ticks, 0);
        assert!(matches!(
            trailing.trailing_distance,
            Some(
                crate::proto::triggers::v1::trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(
                    100
                )
            )
        ));
    }

    #[test]
    fn trigger_create_rejects_missing_ids_fields_and_ambiguous_oneofs() {
        let client = client();
        let base = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );

        let mut missing_id = base.clone();
        missing_id.client_trigger_id = String::new();
        assert!(client.triggers.encode_create_params(&missing_id).is_err());

        let mut missing_trigger_price = base.clone();
        missing_trigger_price.trigger_price = None;
        assert!(
            client
                .triggers
                .encode_create_params(&missing_trigger_price)
                .is_err()
        );

        let mut unknown_source = base.clone();
        unknown_source.trigger_price_source = Some("oracle".into());
        let err = client
            .triggers
            .encode_create_params(&unknown_source)
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");

        let mut trailing = base.clone();
        trailing.trigger_type = CreateTriggerType::TrailingStop;
        trailing.order_type = CreateOrderType::Market;
        trailing.trigger_price = None;
        trailing.limit_price = None;
        trailing.trailing_distance_ticks = Some(10);
        trailing.trailing_distance_bps = Some(10);
        assert!(client.triggers.encode_create_params(&trailing).is_err());

        let mut twap = base.clone();
        twap.trigger_type = CreateTriggerType::Twap;
        assert!(client.triggers.encode_create_params(&twap).is_err());

        let mut ladder = base;
        ladder.trigger_type = CreateTriggerType::Ladder;
        assert!(client.triggers.encode_create_params(&ladder).is_err());
    }

    #[test]
    fn lifecycle_helpers_accept_base58_trigger_ids() {
        let client = client();
        let encoded = bs58::encode(42_u64.to_be_bytes()).into_string();
        let params = ModifyTriggerParams {
            trigger_id: encoded,
            symbol: Some("BTC-USDT".into()),
            trailing_distance_bps: Some(50),
            ..modify_params(None, None)
        };
        let wire = client.triggers.encode_modify_params(&params).unwrap();
        assert_eq!(wire.trigger_id, 42);
    }

    #[test]
    fn trigger_modify_requires_a_patch() {
        let client = client();
        let params = ModifyTriggerParams {
            trigger_id: "2".into(),
            subaccount_id: SubAccountScope::Default,
            symbol: Some("BTC-USDT".into()),
            symbol_id: None,
            trigger_price: None,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
        };
        assert!(client.triggers.encode_modify_params(&params).is_err());

        let nonpositive = ModifyTriggerParams {
            trailing_distance_bps: Some(0),
            ..params.clone()
        };
        assert!(client.triggers.encode_modify_params(&nonpositive).is_err());

        let clear_slippage = ModifyTriggerParams {
            max_slippage_bps: Some(0),
            ..params.clone()
        };
        let cleared = client
            .triggers
            .encode_modify_params(&clear_slippage)
            .unwrap();
        assert_eq!(
            cleared.max_slippage,
            Some(modify_trigger_request::MaxSlippage::MaxSlippageBps(0))
        );

        let preserve = ModifyTriggerParams {
            trailing_distance_bps: Some(50),
            ..params
        };
        let preserved = client.triggers.encode_modify_params(&preserve).unwrap();
        assert!(preserved.activation_price_ticks.is_none());
        assert!(preserved.max_slippage.is_none());
    }

    fn trailing_create(max_slippage_bps: Option<i32>) -> CreateTriggerParams {
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::TrailingStop;
        params.order_type = CreateOrderType::Market;
        params.side = CreateSide::Sell;
        params.trailing_distance_bps = Some(100);
        params.trigger_price = None;
        params.limit_price = None;
        params.max_slippage_bps = max_slippage_bps;
        params
    }

    /// POLY-5724: a trailing distance in ticks is a price delta in 1e-9
    /// quote-unit ticks, the scale of every price tick, sent as given.
    #[test]
    fn trailing_distance_ticks_are_price_ticks() {
        use crate::codecs::decode::trigger_from_proto;
        use crate::models::TriggerDetails;
        use crate::proto::triggers::v1::{
            TrailingDetails, Trigger, modify_trigger_request, trailing_stop_trigger, trigger,
            trigger_intent::Strategy,
        };
        let client = client();
        for (distance, ticks) in [
            ("50", 50_000_000_000_i64),
            ("0.5", 500_000_000),
            ("0.000000001", 1),
        ] {
            // The README's conversion.
            let converted = crate::Price::from_decimal_str(distance, None)
                .unwrap()
                .as_ticks();
            assert_eq!(converted, ticks, "{distance}");

            let mut create = trailing_create(None);
            create.trailing_distance_bps = None;
            create.trailing_distance_ticks = Some(converted);
            let intent = client
                .triggers
                .encode_create_params(&create)
                .unwrap()
                .trigger
                .expect("trigger intent");
            let Some(Strategy::TrailingStop(wire)) = intent.strategy.as_ref() else {
                panic!("expected TrailingStop strategy, got {:?}", intent.strategy);
            };
            assert_eq!(
                wire.trailing_distance,
                Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceTicks(ticks)),
                "create {distance}"
            );

            let modify = ModifyTriggerParams {
                trailing_distance_ticks: Some(converted),
                ..modify_params(None, None)
            };
            let wire = client.triggers.encode_modify_params(&modify).unwrap();
            assert_eq!(
                wire.trailing_distance,
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceTicks(ticks)),
                "modify {distance}"
            );

            let read = trigger_from_proto(&Trigger {
                runtime_details: Some(trigger::RuntimeDetails::Trailing(Box::new(
                    TrailingDetails {
                        trailing_distance_ticks: ticks,
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            });
            let Some(TriggerDetails::Trailing(details)) = read.details else {
                panic!("expected trailing details, got {:?}", read.details);
            };
            assert_eq!(
                details.trailing_distance.map(|price| price.as_ticks()),
                Some(ticks),
                "read {distance}"
            );
        }
    }

    #[test]
    fn trigger_slippage_bps_cap() {
        let client = client();
        client
            .triggers
            .encode_create_params(&trailing_create(Some(9_999)))
            .unwrap();
        assert!(
            client
                .triggers
                .encode_create_params(&trailing_create(Some(10_000)))
                .is_err()
        );

        // POLY-9426: a modify takes 10000 (a buy stop-loss or take-profit
        // market child's cap) and leaves the side check to the server.
        let modify_full = ModifyTriggerParams {
            trigger_id: "2".into(),
            symbol: Some("BTC-USDT".into()),
            max_slippage_bps: Some(10_000),
            ..modify_params(None, None)
        };
        client
            .triggers
            .encode_modify_params(&modify_full)
            .expect("modify takes 10000 bps");
        let modify_too_high = ModifyTriggerParams {
            max_slippage_bps: Some(10_001),
            ..modify_full
        };
        let err = client
            .triggers
            .encode_modify_params(&modify_too_high)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("max_slippage_bps must be between 1 and 10000"),
            "{err}"
        );
    }

    #[test]
    fn twap_market_ioc_encodes_optional_slippage() {
        let client = client();
        let mut params = create_params(
            crate::Quantity::from_decimal_str("1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::Twap;
        params.side = CreateSide::Buy;
        params.order_type = CreateOrderType::Market;
        params.trigger_price = None;
        params.limit_price = None;
        params.twap_duration_ms = Some(60_000);
        params.twap_slice_interval_ms = Some(5_000);
        params.max_slippage_bps = Some(25);
        let req = client.triggers.encode_create_params(&params).unwrap();
        let intent = req.trigger.expect("trigger intent");
        let Some(trigger_intent::Strategy::Twap(twap)) = intent.strategy.as_ref() else {
            panic!("expected twap strategy");
        };
        let Some(twap_trigger::Execution::MarketIoc(ioc)) = twap.execution.as_ref() else {
            panic!("expected market_ioc");
        };
        assert_eq!(
            ioc.max_slippage,
            Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(25))
        );

        params.max_slippage_bps = Some(10_001);
        assert!(client.triggers.encode_create_params(&params).is_err());

        // 5 quote units at Q9 exceeds the i32 range the field used to have.
        params.max_slippage_bps = None;
        params.max_slippage_ticks = Some(5_000_000_000);
        let req = client.triggers.encode_create_params(&params).unwrap();
        let intent = req.trigger.expect("trigger intent");
        let Some(trigger_intent::Strategy::Twap(twap)) = intent.strategy.as_ref() else {
            panic!("expected twap strategy");
        };
        let Some(twap_trigger::Execution::MarketIoc(ioc)) = twap.execution.as_ref() else {
            panic!("expected market_ioc");
        };
        assert_eq!(
            ioc.max_slippage,
            Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(
                5_000_000_000
            ))
        );
    }

    #[test]
    fn poly_4684_standalone_trailing_slippage_bps_boundaries_preexisting() {
        let client = client();
        // Every trailing stop takes at most 9999 bps, whatever its side.
        for side in [CreateSide::Buy, CreateSide::Sell] {
            let trailing = |bps| {
                let mut params = trailing_create(Some(bps));
                params.side = side;
                if matches!(side, CreateSide::Buy) {
                    params.limit_price = Some(
                        crate::Price::from_decimal_str("51000", Some("BTC-USDT".into())).unwrap(),
                    );
                }
                params
            };
            for bps in [1, 9_999] {
                client
                    .triggers
                    .encode_create_params(&trailing(bps))
                    .unwrap_or_else(|e| panic!("{side:?} max_slippage_bps={bps}: {e}"));
            }
            for bps in [0, 10_000] {
                assert!(
                    client
                        .triggers
                        .encode_create_params(&trailing(bps))
                        .is_err(),
                    "{side:?} standalone max_slippage_bps={bps} must be rejected"
                );
            }
        }

        let base = ModifyTriggerParams {
            trigger_id: "2".into(),
            symbol: Some("BTC-USDT".into()),
            ..modify_params(None, None)
        };
        // A modify does not say which trigger it changes: it takes 1 to
        // 10000 as the contract does (POLY-9426), and the server refuses
        // 10000 for a trailing stop.
        for bps in [1, 9_999, 10_000] {
            client
                .triggers
                .encode_modify_params(&ModifyTriggerParams {
                    max_slippage_bps: Some(bps),
                    ..base.clone()
                })
                .unwrap();
        }
        client
            .triggers
            .encode_modify_params(&ModifyTriggerParams {
                max_slippage_bps: Some(0),
                ..base.clone()
            })
            .expect("modify zero is the explicit clear sentinel");
        assert!(
            client
                .triggers
                .encode_modify_params(&ModifyTriggerParams {
                    max_slippage_bps: Some(10_001),
                    ..base
                })
                .is_err()
        );
    }

    #[test]
    fn poly_4689_standalone_trailing_distance_bps_boundaries() {
        let client = client();
        for bps in [1, 10_000] {
            let mut params = trailing_create(None);
            params.trailing_distance_bps = Some(bps);
            client.triggers.encode_create_params(&params).unwrap();
        }
        for bps in [0, 10_001] {
            let mut params = trailing_create(None);
            params.trailing_distance_bps = Some(bps);
            assert!(
                client.triggers.encode_create_params(&params).is_err(),
                "create trailing_distance_bps={bps} must be rejected"
            );
        }

        let base = ModifyTriggerParams {
            trigger_id: "2".into(),
            symbol: Some("BTC-USDT".into()),
            ..modify_params(None, None)
        };
        for bps in [1, 10_000] {
            client
                .triggers
                .encode_modify_params(&ModifyTriggerParams {
                    trailing_distance_bps: Some(bps),
                    ..base.clone()
                })
                .unwrap();
        }
        for bps in [0, 10_001] {
            assert!(
                client
                    .triggers
                    .encode_modify_params(&ModifyTriggerParams {
                        trailing_distance_bps: Some(bps),
                        ..base.clone()
                    })
                    .is_err(),
                "modify trailing_distance_bps={bps} must be rejected"
            );
        }
    }

    #[test]
    fn public_trigger_ids_convert_for_lifecycle_helpers() {
        let encoded = bs58::encode(42_u64.to_be_bytes()).into_string();
        assert_eq!(id_to_u64(&encoded, "trigger_id").unwrap(), 42);
        assert!(id_to_u64("not a trigger id", "trigger_id").is_err());
    }

    fn modify_params(
        trigger_price: Option<crate::Price>,
        activation_price: Option<crate::Price>,
    ) -> ModifyTriggerParams {
        ModifyTriggerParams {
            trigger_id: "2".into(),
            subaccount_id: SubAccountScope::Default,
            symbol: Some("BTC-USDT".into()),
            symbol_id: None,
            trigger_price,
            activation_price,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
        }
    }

    #[test]
    fn spot_triggers_reject_the_mark_price_source() {
        let client = client();
        let mut params = trailing_create(None);
        params.trigger_price_source = Some("mark".into());
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(err.to_string().contains("no mark price"), "{err}");
        params.trigger_price_source = Some("index".into());
        client.triggers.encode_create_params(&params).unwrap();
    }

    #[test]
    fn buy_trailing_stop_limit_price_must_cover_its_activation_price() {
        let client = client();
        let price = |p: &str| crate::Price::from_decimal_str(p, Some("BTC-USDT".into())).unwrap();
        let mut params = trailing_create(None);
        params.side = CreateSide::Buy;
        params.activation_price = Some(price("50000"));
        params.limit_price = Some(price("49999.99"));
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(
            err.to_string().contains("at least its activation_price"),
            "{err}"
        );
        params.limit_price = Some(price("50000"));
        client.triggers.encode_create_params(&params).unwrap();
        // Without an activation price there is nothing to compare.
        params.activation_price = None;
        params.limit_price = Some(price("1"));
        client.triggers.encode_create_params(&params).unwrap();
    }

    #[test]
    fn trigger_modify_without_a_symbol_targets_the_triggers_own_market() {
        let client = client();
        let req = client
            .triggers
            .encode_modify_params(&ModifyTriggerParams {
                symbol: None,
                max_slippage_bps: Some(50),
                ..modify_params(None, None)
            })
            .expect("symbol is optional");
        assert_eq!(req.symbol_id, 0);
        let req = client
            .triggers
            .encode_modify_params(&ModifyTriggerParams {
                max_slippage_bps: Some(50),
                ..modify_params(None, None)
            })
            .unwrap();
        assert_eq!(req.symbol_id, 7, "a named market is still resolved");
    }

    #[test]
    fn decimal_and_scaled_trigger_modify_encode_identically() {
        let client = client();
        let decimal = modify_params(
            Some(crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_decimal_str("49500", Some("BTC-USDT".into())).unwrap()),
        );
        let scaled = modify_params(
            Some(crate::Price::from_ticks(49_000_000_000_000, Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_ticks(49_500_000_000_000, Some("BTC-USDT".into())).unwrap()),
        );
        let decimal_wire = client.triggers.encode_modify_params(&decimal).unwrap();
        let scaled_wire = client.triggers.encode_modify_params(&scaled).unwrap();
        assert_eq!(decimal_wire.encode_to_vec(), scaled_wire.encode_to_vec());
    }

    // A price bound to another market is refused when the trigger's market is
    // given, as trigger creation and order writes do.
    #[test]
    fn modify_refuses_a_price_for_another_market() {
        let client = client();
        let sol = || crate::Price::from_ticks(46_000_000_000_000, Some("SOL-USDT".into())).unwrap();
        for (symbol, symbol_id) in [(Some("BTC-USDT".to_owned()), None), (None, Some(7))] {
            for params in [
                modify_params(Some(sol()), None),
                modify_params(None, Some(sol())),
            ] {
                let params = ModifyTriggerParams {
                    symbol: symbol.clone(),
                    symbol_id,
                    ..params
                };
                let err = client
                    .triggers
                    .encode_modify_params(&params)
                    .expect_err("a SOL-USDT price must not modify a BTC-USDT trigger");
                assert!(
                    err.to_string().contains(
                        "price symbol mismatch: value is for SOL-USDT, destination is BTC-USDT"
                    ),
                    "{err}"
                );
            }
        }
    }

    // An explicit symbol ID the catalogs don't know can't be checked against
    // a bound price, so the price is refused; an unbound one is still sent.
    #[test]
    fn modify_refuses_a_bound_price_it_cannot_check() {
        let client = client();
        for bound_to in ["BTC-USDT", "SOL-USDT"] {
            let price =
                || crate::Price::from_ticks(47_000_000_000_000, Some(bound_to.into())).unwrap();
            for params in [
                modify_params(Some(price()), None),
                modify_params(None, Some(price())),
            ] {
                let params = ModifyTriggerParams {
                    symbol: None,
                    symbol_id: Some(8),
                    ..params
                };
                let err = client
                    .triggers
                    .encode_modify_params(&params)
                    .expect_err("an unknown market can't be checked");
                assert!(
                    err.to_string().contains(&format!(
                        "cannot check a price bound to {bound_to} against symbol_id 8"
                    )),
                    "{err}"
                );
            }
        }
        let unbound = crate::Price::from_ticks(46_000_000_000_000, None).unwrap();
        let params = ModifyTriggerParams {
            symbol: None,
            symbol_id: Some(8),
            ..modify_params(Some(unbound.clone()), Some(unbound))
        };
        let wire = client.triggers.encode_modify_params(&params).unwrap();
        assert_eq!(wire.symbol_id, 8);
        assert_eq!(wire.trigger_price_ticks, Some(46_000_000_000_000));
        assert_eq!(wire.activation_price_ticks, Some(46_000_000_000_000));
        // A known symbol ID still checks against its market.
        let btc = crate::Price::from_ticks(47_000_000_000_000, Some("BTC-USDT".into())).unwrap();
        let params = ModifyTriggerParams {
            symbol: None,
            symbol_id: Some(7),
            ..modify_params(None, Some(btc))
        };
        assert_eq!(
            client
                .triggers
                .encode_modify_params(&params)
                .unwrap()
                .activation_price_ticks,
            Some(47_000_000_000_000)
        );
    }

    #[test]
    fn modify_sends_a_price_for_its_market_or_an_unknown_one() {
        let client = client();
        let btc = crate::Price::from_ticks(47_000_000_000_000, Some("BTC-USDT".into())).unwrap();
        let wire = client
            .triggers
            .encode_modify_params(&modify_params(Some(btc), None))
            .unwrap();
        assert_eq!(wire.trigger_price_ticks, Some(47_000_000_000_000));
        assert_eq!(wire.symbol_id, 7);
        // Without a symbol or symbol ID the trigger's market is not known.
        let sol = crate::Price::from_ticks(46_000_000_000_000, Some("SOL-USDT".into())).unwrap();
        let unknown = ModifyTriggerParams {
            symbol: None,
            ..modify_params(Some(sol), None)
        };
        let wire = client.triggers.encode_modify_params(&unknown).unwrap();
        assert_eq!(wire.trigger_price_ticks, Some(46_000_000_000_000));
        assert_eq!(wire.symbol_id, 0);
    }

    fn twap_create(order_type: CreateOrderType) -> CreateTriggerParams {
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.01", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::Twap;
        params.side = CreateSide::Sell;
        params.order_type = order_type;
        params.trigger_price = None;
        params.time_in_force = None;
        params.twap_duration_ms = Some(60_000);
        params.twap_slice_interval_ms = Some(10_000);
        if order_type == CreateOrderType::Market {
            params.limit_price = None;
        }
        params
    }

    fn ladder_create() -> CreateTriggerParams {
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::Ladder;
        params.trigger_price = None;
        params.limit_price = None;
        params.time_in_force = None;
        params.ladder_price_min =
            Some(crate::Price::from_decimal_str("48000", Some("BTC-USDT".into())).unwrap());
        params.ladder_price_max =
            Some(crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap());
        params.ladder_levels = Some(4);
        params
    }

    /// A create refused before sending with the server's `code` for the same
    /// mistake, as [`Error::Api`] without violations.
    fn expect_refusal(
        client: &crate::Client,
        params: &CreateTriggerParams,
        code: &str,
        mention: &str,
        label: &str,
    ) {
        match client.triggers.encode_create_params(params) {
            Err(Error::Api {
                code: got,
                message,
                violations,
                ..
            }) => {
                assert_eq!(got, code, "{label}: {message}");
                assert!(message.contains(mention), "{label}: {message}");
                assert!(violations.is_empty(), "{label}: {violations:?}");
            }
            other => panic!("{label}: expected {code}, got {other:?}"),
        }
    }

    /// The TWAP limits are the contract's: the generated field docs state the
    /// same bounds, so a contract change that regenerates them breaks this.
    #[test]
    fn twap_limits_match_the_generated_contract_docs() {
        let docs = include_str!("../gen/buffa/triggers.v1.rs");
        let duration = format!(
            "/// Total execution duration in milliseconds, at least {TWAP_DURATION_MS_MIN}.\n"
        );
        assert!(
            docs.contains(&duration),
            "duration docs changed: {duration}"
        );
        let interval = format!(
            "/// Interval between slices in milliseconds, at least {TWAP_SLICE_INTERVAL_MS_MIN} \
             and at most\n    /// duration_ms.\n"
        );
        assert!(
            docs.contains(&interval),
            "slice interval docs changed: {interval}"
        );
        assert_eq!(
            (TWAP_DURATION_MS_MIN, TWAP_SLICE_INTERVAL_MS_MIN),
            (1_000, 100)
        );
    }

    /// A client trigger ID is 1 to 36 bytes of UTF-8, with no character set:
    /// the contract's rule, which the generated field docs state. It is sent
    /// exactly as given, never trimmed.
    #[test]
    fn client_trigger_id_follows_the_contract_rule_and_is_sent_as_given() {
        let docs = include_str!("../gen/buffa/triggers.v1.rs");
        let rule = format!(
            "/// 1 to {CLIENT_TRIGGER_ID_MAX_BYTES} bytes in UTF-8 (up to 36 ASCII characters); empty or longer IDs\n"
        );
        assert!(
            docs.contains(&rule),
            "client_trigger_id docs changed: {rule}"
        );
        let client = client();
        for id in [
            "x".repeat(36),
            " padded ".to_owned(),
            " ".to_owned(),
            "tp #1 \u{e9}t\u{e9}".to_owned(),
            "\u{6b62}".repeat(12),
        ] {
            let mut params = market_stop(CreateSide::Sell);
            params.client_trigger_id = id.clone();
            let wire = client
                .triggers
                .encode_create_params(&params)
                .unwrap_or_else(|err| panic!("{id:?}: {err}"));
            assert_eq!(wire.trigger.as_option().unwrap().client_trigger_id, id);
        }
    }

    /// The contract's `LadderTrigger.levels` validate rule is `gte: 2, lte:
    /// 100`, which the generated field docs state: a contract change that
    /// regenerates them breaks this.
    #[test]
    fn ladder_levels_limits_match_the_generated_contract_docs() {
        let docs = include_str!("../gen/buffa/triggers.v1.rs");
        let levels = format!(
            "/// Number of linearly distributed levels, from {LADDER_LEVELS_MIN} to \
             {LADDER_LEVELS_MAX}.\n"
        );
        assert!(
            docs.contains(&levels),
            "ladder levels docs changed: {levels}"
        );
        assert_eq!((LADDER_LEVELS_MIN, LADDER_LEVELS_MAX), (2, 100));
    }

    /// A TWAP duration or slice interval the contract refuses is refused
    /// before sending: duration at least 1000, slice interval at least 100
    /// and no longer than the duration.
    #[test]
    fn twap_refuses_values_outside_the_contract() {
        let client = client();
        const DURATION: &str = "ERROR_CODE_TWAP_DURATION_INVALID";
        const INTERVAL: &str = "ERROR_CODE_TWAP_INTERVAL_INVALID";
        for (duration, interval, code, mention) in [
            (500, 100, DURATION, "twap_duration_ms"),
            (0, 100, DURATION, "twap_duration_ms"),
            (-1000, 100, DURATION, "twap_duration_ms"),
            (999, 100, DURATION, "twap_duration_ms"),
            (60_000, 0, INTERVAL, "twap_slice_interval_ms"),
            (60_000, -5, INTERVAL, "twap_slice_interval_ms"),
            (60_000, 99, INTERVAL, "twap_slice_interval_ms"),
            (1_000, 1_001, INTERVAL, "must not exceed"),
        ] {
            let mut params = twap_create(CreateOrderType::Market);
            params.twap_duration_ms = Some(duration);
            params.twap_slice_interval_ms = Some(interval);
            expect_refusal(
                &client,
                &params,
                code,
                mention,
                &format!("{duration}/{interval}"),
            );
        }
        let mut params = twap_create(CreateOrderType::Market);
        params.twap_duration_ms = None;
        expect_refusal(&client, &params, DURATION, "required", "no duration");
        let mut params = twap_create(CreateOrderType::Market);
        params.twap_slice_interval_ms = None;
        expect_refusal(&client, &params, INTERVAL, "required", "no interval");
        for (duration, interval) in [
            (60_000, 1_000),
            (1_000, 100),
            (1_000, 1_000),
            (i64::MAX, 100),
            (i64::MAX, i64::MAX),
        ] {
            let mut params = twap_create(CreateOrderType::Market);
            params.twap_duration_ms = Some(duration);
            params.twap_slice_interval_ms = Some(interval);
            let req = client.triggers.encode_create_params(&params).unwrap();
            let Some(trigger_intent::Strategy::Twap(twap)) =
                req.trigger.as_option().and_then(|t| t.strategy.as_ref())
            else {
                panic!("expected twap strategy");
            };
            assert_eq!(
                (twap.duration_ms, twap.slice_interval_ms),
                (duration, interval)
            );
        }
    }

    /// A trigger field refused before sending carries the code the Triggers
    /// API answers for the same mistake, without a violation, so a pre-checked
    /// request and a server refusal read alike. Shape refusals stay
    /// validation errors.
    #[test]
    fn trigger_field_refusals_carry_the_server_codes() {
        let client = client();
        let btc =
            |text: &str| crate::Price::from_decimal_str(text, Some("BTC-USDT".into())).unwrap();
        let zero_price = || crate::Price::from_ticks(0, Some("BTC-USDT".into())).unwrap();
        let mut cases: Vec<(&str, CreateTriggerParams, &str)> = Vec::new();

        let mut p = market_stop(CreateSide::Sell);
        p.client_trigger_id = String::new();
        cases.push((
            "empty client_trigger_id",
            p,
            "ERROR_CODE_CLIENT_TRIGGER_ID_INVALID",
        ));
        let mut p = market_stop(CreateSide::Sell);
        p.client_trigger_id = "x".repeat(37);
        cases.push((
            "long client_trigger_id",
            p,
            "ERROR_CODE_CLIENT_TRIGGER_ID_INVALID",
        ));
        let mut p = market_stop(CreateSide::Sell);
        // 13 three-byte characters: 39 bytes.
        p.client_trigger_id = "\u{6b62}".repeat(13);
        cases.push((
            "39-byte client_trigger_id",
            p,
            "ERROR_CODE_CLIENT_TRIGGER_ID_INVALID",
        ));
        let mut p = market_stop(CreateSide::Sell);
        p.qty = crate::Quantity::from_scaled(
            0,
            Some(8),
            crate::types::QuantityDomain::OrderBase,
            Some("BTC-USDT".into()),
            Some(7),
        )
        .unwrap();
        cases.push(("zero qty", p, "ERROR_CODE_TRIGGER_QTY_INVALID"));
        let mut p = market_stop(CreateSide::Sell);
        p.fee_asset = Some(FeeAsset::Base);
        cases.push(("sell with base fee", p, "ERROR_CODE_FEE_ASSET_NOT_ALLOWED"));
        let mut p = trailing_create(None);
        p.fee_asset = Some(FeeAsset::Base);
        cases.push((
            "sell trailing with base fee",
            p,
            "ERROR_CODE_FEE_ASSET_NOT_ALLOWED",
        ));
        let mut p = twap_create(CreateOrderType::Market);
        p.fee_asset = Some(FeeAsset::Base);
        cases.push((
            "sell twap with base fee",
            p,
            "ERROR_CODE_FEE_ASSET_NOT_ALLOWED",
        ));
        let mut p = market_stop(CreateSide::Sell);
        p.trigger_price = None;
        cases.push(("no trigger price", p, "ERROR_CODE_TRIGGER_PRICE_INVALID"));
        let mut p = market_stop(CreateSide::Sell);
        p.trigger_price = Some(zero_price());
        cases.push(("zero trigger price", p, "ERROR_CODE_TRIGGER_PRICE_INVALID"));
        let mut p = market_stop(CreateSide::Sell);
        p.order_type = CreateOrderType::Limit;
        cases.push(("limit child without price", p, "ERROR_CODE_BAD_PRICE"));
        let mut p = market_stop(CreateSide::Sell);
        p.max_slippage_ticks = Some(0);
        cases.push((
            "zero child tick cap",
            p,
            "ERROR_CODE_SLIPPAGE_TICKS_INVALID",
        ));
        let mut p = market_stop(CreateSide::Buy);
        p.max_slippage_bps = Some(10_001);
        cases.push((
            "child bps above 10000",
            p,
            "ERROR_CODE_SLIPPAGE_BPS_INVALID",
        ));
        let mut p = market_stop(CreateSide::Buy);
        p.max_slippage_bps = Some(0);
        cases.push(("zero child bps", p, "ERROR_CODE_SLIPPAGE_BPS_INVALID"));
        let mut p = market_stop(CreateSide::Sell);
        p.max_slippage_bps = Some(10_000);
        cases.push(("sell child bps 10000", p, "ERROR_CODE_MAX_SLIPPAGE_INVALID"));
        let mut p = trailing_create(None);
        p.trailing_distance_bps = None;
        cases.push((
            "no trailing distance",
            p,
            "ERROR_CODE_TRAILING_DISTANCE_INVALID",
        ));
        let mut p = trailing_create(None);
        p.trailing_distance_bps = Some(10_001);
        cases.push((
            "trailing bps above 10000",
            p,
            "ERROR_CODE_TRAILING_DISTANCE_INVALID",
        ));
        let mut p = trailing_create(None);
        p.trailing_distance_bps = None;
        p.trailing_distance_ticks = Some(0);
        cases.push((
            "zero trailing ticks",
            p,
            "ERROR_CODE_TRAILING_DISTANCE_INVALID",
        ));
        cases.push((
            "trailing bps cap 10000",
            trailing_create(Some(10_000)),
            "ERROR_CODE_MAX_SLIPPAGE_INVALID",
        ));
        cases.push((
            "trailing bps cap 10001",
            trailing_create(Some(10_001)),
            "ERROR_CODE_SLIPPAGE_BPS_INVALID",
        ));
        let mut p = trailing_create(None);
        p.max_slippage_ticks = Some(-1);
        cases.push((
            "negative trailing tick cap",
            p,
            "ERROR_CODE_SLIPPAGE_TICKS_INVALID",
        ));
        let mut p = trailing_create(None);
        p.side = CreateSide::Buy;
        p.limit_price = Some(zero_price());
        cases.push(("zero buy trailing maximum", p, "ERROR_CODE_BAD_PRICE"));
        let mut p = twap_create(CreateOrderType::Market);
        p.max_slippage_bps = Some(10_000);
        cases.push((
            "selling twap bps 10000",
            p,
            "ERROR_CODE_MAX_SLIPPAGE_INVALID",
        ));
        let mut p = twap_create(CreateOrderType::Limit);
        p.limit_price = None;
        cases.push(("twap limit slice without price", p, "ERROR_CODE_BAD_PRICE"));
        let mut p = ladder_create();
        p.ladder_price_max = Some(btc("48000"));
        cases.push((
            "ladder max not above min",
            p,
            "ERROR_CODE_TRIGGER_PRICE_INVALID",
        ));
        let mut p = ladder_create();
        p.ladder_price_min = None;
        cases.push(("ladder without min", p, "ERROR_CODE_TRIGGER_PRICE_INVALID"));
        let mut p = ladder_create();
        p.ladder_price_min = Some(zero_price());
        cases.push(("ladder zero min", p, "ERROR_CODE_TRIGGER_PRICE_INVALID"));
        let mut p = ladder_create();
        p.ladder_levels = None;
        cases.push((
            "ladder without levels",
            p,
            "ERROR_CODE_LADDER_LEVELS_INVALID",
        ));
        for (label, params, code) in cases {
            expect_refusal(&client, &params, code, "", label);
        }

        // Shape refusals keep Error::Validation.
        let mut p = trailing_create(None);
        p.trailing_distance_ticks = Some(5);
        let mut both = p;
        both.trailing_distance_bps = Some(5);
        let mut buy_without_maximum = trailing_create(None);
        buy_without_maximum.side = CreateSide::Buy;
        let mut sell_with_maximum = trailing_create(None);
        sell_with_maximum.limit_price = Some(btc("50000"));
        for (label, params) in [
            ("both trailing distances", both),
            ("buy trailing without maximum", buy_without_maximum),
            ("sell trailing with maximum", sell_with_maximum),
        ] {
            let err = client.triggers.encode_create_params(&params).unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{label}: {err:?}");
        }

        // A buy may select the base fee asset; the pair's
        // allow_buy_fee_from_base is the server's to check.
        let mut buy = market_stop(CreateSide::Buy);
        buy.fee_asset = Some(FeeAsset::Base);
        client.triggers.encode_create_params(&buy).unwrap();
    }

    /// A modify refuses the same fields with the same codes; its caps may be
    /// 0, which clears them.
    #[test]
    fn trigger_modify_refusals_carry_the_server_codes() {
        let client = client();
        let base = ModifyTriggerParams {
            trigger_id: "2".into(),
            subaccount_id: SubAccountScope::Default,
            symbol: Some("BTC-USDT".into()),
            symbol_id: None,
            trigger_price: None,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
        };
        for (label, params, code) in [
            (
                "zero trailing ticks",
                ModifyTriggerParams {
                    trailing_distance_ticks: Some(0),
                    ..base.clone()
                },
                "ERROR_CODE_TRAILING_DISTANCE_INVALID",
            ),
            (
                "zero trailing bps",
                ModifyTriggerParams {
                    trailing_distance_bps: Some(0),
                    ..base.clone()
                },
                "ERROR_CODE_TRAILING_DISTANCE_INVALID",
            ),
            (
                "negative tick cap",
                ModifyTriggerParams {
                    max_slippage_ticks: Some(-1),
                    ..base.clone()
                },
                "ERROR_CODE_SLIPPAGE_TICKS_INVALID",
            ),
            (
                "bps cap above 10000",
                ModifyTriggerParams {
                    max_slippage_bps: Some(10_001),
                    ..base.clone()
                },
                "ERROR_CODE_SLIPPAGE_BPS_INVALID",
            ),
            (
                "negative bps cap",
                ModifyTriggerParams {
                    max_slippage_bps: Some(-1),
                    ..base.clone()
                },
                "ERROR_CODE_SLIPPAGE_BPS_INVALID",
            ),
        ] {
            match client.triggers.encode_modify_params(&params) {
                Err(Error::Api {
                    code: got,
                    violations,
                    ..
                }) => {
                    assert_eq!(got, code, "{label}");
                    assert!(violations.is_empty(), "{label}");
                }
                other => panic!("{label}: expected {code}, got {other:?}"),
            }
        }
        let cleared = ModifyTriggerParams {
            max_slippage_ticks: Some(0),
            ..base
        };
        client.triggers.encode_modify_params(&cleared).unwrap();
    }

    /// Ladder levels the contract refuses (outside 2..=100) are refused
    /// before sending.
    #[test]
    fn ladder_refuses_levels_outside_the_contract() {
        let client = client();
        for levels in [1, 0, 101, -3, i32::MAX] {
            let mut params = ladder_create();
            params.ladder_levels = Some(levels);
            expect_refusal(
                &client,
                &params,
                "ERROR_CODE_LADDER_LEVELS_INVALID",
                "ladder_levels",
                &levels.to_string(),
            );
        }
        for levels in [2, 3, 99, 100] {
            let mut params = ladder_create();
            params.ladder_levels = Some(levels);
            let req = client.triggers.encode_create_params(&params).unwrap();
            let Some(trigger_intent::Strategy::Ladder(ladder)) =
                req.trigger.as_option().and_then(|t| t.strategy.as_ref())
            else {
                panic!("expected ladder strategy");
            };
            assert_eq!(ladder.levels, levels);
        }
    }

    /// POLY-8758: no trigger child has a GTD execution, so a selected Gtd is
    /// refused for every strategy instead of being sent as IOC or GTC.
    #[test]
    fn poly_8758_gtd_is_refused_for_every_trigger_strategy() {
        let client = client();
        let limit_stop = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        let cases = [
            ("market stop", market_stop(CreateSide::Sell)),
            ("limit stop", limit_stop),
            ("twap market", twap_create(CreateOrderType::Market)),
            ("twap limit", twap_create(CreateOrderType::Limit)),
            ("trailing stop", trailing_create(None)),
            ("ladder", ladder_create()),
        ];
        for (label, params) in cases {
            // Control: the strategy encodes without a time in force.
            client
                .triggers
                .encode_create_params(&params)
                .unwrap_or_else(|e| panic!("{label} without time_in_force: {e}"));
            let gtd = CreateTriggerParams {
                time_in_force: Some(CreateTimeInForce::Gtd),
                ..params
            };
            match client.triggers.encode_create_params(&gtd) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains("Gtd"), "{label}: {message}")
                }
                other => panic!("{label} with Gtd: expected a validation error, got {other:?}"),
            }
        }

        // The selections each strategy supports still encode.
        for (label, params, tif) in [
            (
                "market stop ioc",
                market_stop(CreateSide::Sell),
                CreateTimeInForce::Ioc,
            ),
            (
                "twap limit gtc",
                twap_create(CreateOrderType::Limit),
                CreateTimeInForce::Gtc,
            ),
            ("ladder gtc", ladder_create(), CreateTimeInForce::Gtc),
        ] {
            let params = CreateTriggerParams {
                time_in_force: Some(tif),
                ..params
            };
            client
                .triggers
                .encode_create_params(&params)
                .unwrap_or_else(|e| panic!("{label}: {e}"));
        }
    }

    /// POLY-8748: a TWAP market child's tick cap is positive, as every other
    /// create-time tick cap is.
    #[test]
    fn poly_8748_twap_market_refuses_nonpositive_slippage_ticks() {
        let client = client();
        for ticks in [-5, 0] {
            let mut params = twap_create(CreateOrderType::Market);
            params.max_slippage_ticks = Some(ticks);
            expect_refusal(
                &client,
                &params,
                "ERROR_CODE_SLIPPAGE_TICKS_INVALID",
                "max_slippage_ticks",
                &format!("max_slippage_ticks={ticks}"),
            );
        }

        let mut params = twap_create(CreateOrderType::Market);
        params.max_slippage_ticks = Some(1);
        let req = client.triggers.encode_create_params(&params).unwrap();
        let Some(trigger_intent::Strategy::Twap(twap)) =
            req.trigger.as_option().and_then(|t| t.strategy.as_ref())
        else {
            panic!("expected twap strategy");
        };
        let Some(twap_trigger::Execution::MarketIoc(ioc)) = twap.execution.as_ref() else {
            panic!("expected market_ioc");
        };
        assert_eq!(
            ioc.max_slippage,
            Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(1))
        );

        // Controls: no cap sends none, and two caps are still refused.
        let params = twap_create(CreateOrderType::Market);
        client.triggers.encode_create_params(&params).unwrap();
        let mut both = twap_create(CreateOrderType::Market);
        both.max_slippage_ticks = Some(1);
        both.max_slippage_bps = Some(1);
        assert!(client.triggers.encode_create_params(&both).is_err());
    }

    fn btc_price(decimal: &str) -> crate::Price {
        crate::Price::from_decimal_str(decimal, Some("BTC-USDT".into())).unwrap()
    }

    /// A sell OCO over 0.1 BTC: a limit take-profit at 51000 (limit 50990)
    /// and a market stop-loss at 49000.
    fn sell_oco() -> CreateOcoParams {
        let mut params = CreateOcoParams::new(
            "BTC-USDT",
            CreateSide::Sell,
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            OcoLegParams::limit(btc_price("51000"), btc_price("50990")),
            OcoLegParams::market(btc_price("49000")),
        );
        params.client_list_id = "oco-1".into();
        params
    }

    fn leg_execution(leg: &OcoLeg) -> Option<&risk_execution::Execution> {
        leg.child
            .as_option()
            .and_then(|child| child.execution.as_ref())
    }

    /// The contract does not support a mark price source on an OCO leg or
    /// decrement self-trade prevention on an OCO: both are refused before
    /// sending with `Error::Validation`, as a single trigger create refuses
    /// `mark`. `last`, `index` and the expire modes are sent.
    #[test]
    fn oco_create_refuses_mark_and_decrement_before_sending() {
        let client = client();
        for label in ["mark", "MARK"] {
            let mut tp = sell_oco();
            tp.take_profit.trigger_price_source = Some(label.into());
            let mut sl = sell_oco();
            sl.stop_loss.trigger_price_source = Some(label.into());
            for params in [tp, sl] {
                let err = client
                    .triggers
                    .encode_create_oco_params(&params)
                    .unwrap_err();
                assert!(
                    matches!(&err, Error::Validation(m) if m.contains("mark is not supported")),
                    "{label}: {err:?}"
                );
            }
        }
        for label in ["decrement", "Decrement"] {
            let mut params = sell_oco();
            params.self_trade_prevention_mode = Some(label.into());
            let err = client
                .triggers
                .encode_create_oco_params(&params)
                .unwrap_err();
            assert!(
                matches!(&err, Error::Validation(m) if m.contains("decrement is not supported on an OCO")),
                "{label}: {err:?}"
            );
        }
        for (label, source) in [
            ("last", TriggerPriceSource::LastPrice),
            ("index", TriggerPriceSource::IndexPrice),
        ] {
            let mut params = sell_oco();
            params.take_profit.trigger_price_source = Some(label.into());
            let wire = client.triggers.encode_create_oco_params(&params).unwrap();
            let oco = wire.oco.as_option().unwrap();
            assert_eq!(
                oco.take_profit.as_option().unwrap().trigger_price_source,
                source
            );
        }
        for (label, mode) in [
            ("expire_taker", SelfTradePreventionMode::ExpireTaker),
            ("expire_maker", SelfTradePreventionMode::ExpireMaker),
            ("expire_both", SelfTradePreventionMode::ExpireBoth),
        ] {
            let mut params = sell_oco();
            params.self_trade_prevention_mode = Some(label.into());
            let wire = client.triggers.encode_create_oco_params(&params).unwrap();
            assert_eq!(
                wire.oco.as_option().unwrap().self_trade_prevention_mode,
                mode
            );
        }
    }

    #[test]
    fn oco_create_encodes_the_intent_and_both_legs() {
        let client = client();
        let mut params = sell_oco();
        params.client_list_id = "tp-sl:btc/2026-11-15_1".into();
        params.stop_loss.trigger_price_source = Some("index".into());
        params.self_trade_prevention_mode = Some("expire_taker".into());
        params.subaccount_id = SubAccountScope::Id(12);
        let wire = client.triggers.encode_create_oco_params(&params).unwrap();
        assert_eq!(wire.subaccount_id, Some(12));
        let oco = wire.oco.as_option().expect("oco intent");
        assert_eq!(oco.symbol_id, 7);
        assert_eq!(oco.side, Side::Sell);
        assert_eq!(oco.qty_scaled, 10_000_000);
        // The server matches the key exactly: it is sent as given.
        assert_eq!(oco.client_list_id, "tp-sl:btc/2026-11-15_1");
        assert_eq!(
            oco.self_trade_prevention_mode,
            SelfTradePreventionMode::ExpireTaker
        );

        let tp = oco.take_profit.as_option().expect("take_profit");
        assert_eq!(tp.trigger_price_ticks, 51_000_000_000_000);
        assert_eq!(
            tp.trigger_price_source,
            TriggerPriceSource::TriggerPriceSourceUnspecified
        );
        match leg_execution(tp) {
            Some(risk_execution::Execution::LimitGtc(limit)) => {
                assert_eq!(limit.price_ticks, 50_990_000_000_000);
            }
            other => panic!("take_profit: expected a limit GTC child, got {other:?}"),
        }

        let sl = oco.stop_loss.as_option().expect("stop_loss");
        assert_eq!(sl.trigger_price_ticks, 49_000_000_000_000);
        assert_eq!(sl.trigger_price_source, TriggerPriceSource::IndexPrice);
        assert!(
            matches!(
                leg_execution(sl),
                Some(risk_execution::Execution::MarketIoc(_))
            ),
            "stop_loss: expected a market IOC child, got {:?}",
            leg_execution(sl)
        );

        // Unset options send nothing: the server defaults apply.
        let wire = client
            .triggers
            .encode_create_oco_params(&sell_oco())
            .unwrap();
        let oco = wire.oco.as_option().unwrap();
        assert_eq!(wire.subaccount_id, None);
        assert_eq!(
            oco.self_trade_prevention_mode,
            SelfTradePreventionMode::default()
        );
    }

    #[test]
    fn decimal_and_scaled_oco_encode_identically() {
        let client = client();
        let decimal = sell_oco();
        let mut scaled = CreateOcoParams::new(
            "BTC-USDT",
            CreateSide::Sell,
            crate::Quantity::from_scaled(
                10_000_000,
                Some(8),
                crate::QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(7),
            )
            .unwrap(),
            OcoLegParams::limit(
                crate::Price::from_ticks(51_000_000_000_000, Some("BTC-USDT".into())).unwrap(),
                crate::Price::from_ticks(50_990_000_000_000, Some("BTC-USDT".into())).unwrap(),
            ),
            OcoLegParams::market(
                crate::Price::from_ticks(49_000_000_000_000, Some("BTC-USDT".into())).unwrap(),
            ),
        );
        scaled.client_list_id = "oco-1".into();
        assert_eq!(
            client
                .triggers
                .encode_create_oco_params(&decimal)
                .unwrap()
                .encode_to_vec(),
            client
                .triggers
                .encode_create_oco_params(&scaled)
                .unwrap()
                .encode_to_vec()
        );
    }

    #[test]
    fn buy_oco_takes_profit_below_its_stop_loss() {
        let client = client();
        let mut buy = sell_oco();
        buy.side = CreateSide::Buy;
        buy.take_profit = OcoLegParams::market(btc_price("49000"));
        buy.stop_loss = OcoLegParams::limit(btc_price("51000"), btc_price("51010"));
        let wire = client.triggers.encode_create_oco_params(&buy).unwrap();
        let oco = wire.oco.as_option().unwrap();
        assert_eq!(oco.side, Side::Buy);
    }

    /// Which way round the legs must be for the side, and whether a market
    /// has an index price, are the server's to refuse: the SDK sends them.
    /// `mark` is refused before sending (see
    /// `oco_create_refuses_mark_and_decrement_before_sending`).
    #[test]
    fn oco_create_leaves_leg_order_and_supported_sources_to_the_server() {
        let client = client();
        // The sell OCO's legs, kept for a buy, and equal trigger prices.
        let mut swapped = sell_oco();
        swapped.side = CreateSide::Buy;
        let mut equal = sell_oco();
        equal.stop_loss = OcoLegParams::market(btc_price("51000"));
        for (params, side, sl_ticks) in [
            (swapped, Side::Buy, 49_000_000_000_000),
            (equal, Side::Sell, 51_000_000_000_000),
        ] {
            let wire = client.triggers.encode_create_oco_params(&params).unwrap();
            let oco = wire.oco.as_option().unwrap();
            assert_eq!(oco.side, side);
            assert_eq!(
                oco.take_profit.as_option().unwrap().trigger_price_ticks,
                51_000_000_000_000
            );
            assert_eq!(
                oco.stop_loss.as_option().unwrap().trigger_price_ticks,
                sl_ticks
            );
        }

        for (label, want) in [
            ("last", TriggerPriceSource::LastPrice),
            ("index", TriggerPriceSource::IndexPrice),
            (" INDEX ", TriggerPriceSource::IndexPrice),
            ("", TriggerPriceSource::TriggerPriceSourceUnspecified),
        ] {
            let mut params = sell_oco();
            params.take_profit.trigger_price_source = Some(label.into());
            params.stop_loss.trigger_price_source = Some(label.into());
            let wire = client.triggers.encode_create_oco_params(&params).unwrap();
            let oco = wire.oco.as_option().unwrap();
            assert_eq!(
                oco.take_profit.as_option().unwrap().trigger_price_source,
                want,
                "{label:?}"
            );
            assert_eq!(
                oco.stop_loss.as_option().unwrap().trigger_price_source,
                want,
                "{label:?}"
            );
        }
    }

    /// A client OCO ID follows the client order ID rule
    /// (`^[A-Za-z0-9._:/-]{1,36}$`) and is sent unchanged; spaces, other
    /// punctuation and non-ASCII characters are refused before sending.
    #[test]
    fn oco_client_list_id_follows_the_client_order_id_rule() {
        let client = client();
        for key in [
            "tp-sl:btc/2026-11-15_1".to_owned(),
            "x".repeat(36),
            "A.b_C:1/2-3".to_owned(),
        ] {
            let mut params = sell_oco();
            params.client_list_id = key.clone();
            let wire = client
                .triggers
                .encode_create_oco_params(&params)
                .unwrap_or_else(|err| panic!("{key:?}: {err}"));
            let oco = wire.oco.as_option().expect("oco intent");
            assert_eq!(oco.client_list_id, key, "{key:?}");
        }
        for key in [
            " oco-1 ".to_owned(),
            " ".to_owned(),
            "\u{6b62}\u{640d}-\u{00e9}t\u{00e9}".to_owned(),
            "tab\tand/slash".to_owned(),
            "oco#1".to_owned(),
        ] {
            let mut params = sell_oco();
            params.client_list_id = key.clone();
            let err = client
                .triggers
                .encode_create_oco_params(&params)
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("client_list_id contains invalid characters"),
                "{key:?}: {err}"
            );
        }
    }

    #[test]
    fn oco_create_refuses_invalid_params_before_sending() {
        let client = client();
        let refused = |params: CreateOcoParams, want: &str| {
            let err = client
                .triggers
                .encode_create_oco_params(&params)
                .unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{want}: {err:?}");
            assert!(err.to_string().contains(want), "{want}: {err}");
        };
        // A field the server refuses with its own code carries that code.
        let coded = |params: CreateOcoParams, code: &str, want: &str| {
            let err = client
                .triggers
                .encode_create_oco_params(&params)
                .unwrap_err();
            assert!(
                matches!(&err, Error::Api { code: got, violations, .. }
                    if got == code && violations.is_empty()),
                "{want}: {err:?}"
            );
            assert!(err.to_string().contains(want), "{want}: {err}");
        };

        let mut p = sell_oco();
        p.client_list_id = String::new();
        refused(p, "client_list_id must be 1 to 36 characters");
        let mut p = sell_oco();
        p.client_list_id = "x".repeat(37);
        refused(p, "client_list_id must be 1 to 36 characters");
        let mut p = sell_oco();
        p.client_list_id = "\u{30b9}".repeat(12);
        refused(p, "client_list_id contains invalid characters");

        let mut p = sell_oco();
        p.take_profit.limit_price = None;
        coded(
            p,
            "ERROR_CODE_BAD_PRICE",
            "take_profit limit leg requires limit_price",
        );
        let mut p = sell_oco();
        p.qty = crate::Quantity::from_scaled(
            0,
            Some(8),
            crate::types::QuantityDomain::OrderBase,
            Some("BTC-USDT".into()),
            Some(7),
        )
        .unwrap();
        coded(p, "ERROR_CODE_BAD_QTY", "qty must be positive");
        let mut p = sell_oco();
        p.stop_loss.limit_price = Some(btc_price("48990"));
        refused(p, "stop_loss market leg must not set limit_price");
        let mut p = sell_oco();
        p.stop_loss.trigger_price = crate::Price::from_ticks(0, None).unwrap();
        coded(
            p,
            "ERROR_CODE_TRIGGER_PRICE_INVALID",
            "stop_loss trigger_price must be positive",
        );

        let mut p = sell_oco();
        p.take_profit.trigger_price_source = Some("oracle".into());
        refused(p, "take_profit trigger_price_source must be last or index");
        let mut p = sell_oco();
        p.self_trade_prevention_mode = Some("decrease".into());
        refused(p, "self_trade_prevention_mode");

        let mut p = sell_oco();
        p.take_profit.trigger_price =
            crate::Price::from_decimal_str("51000", Some("ETH-USDT".into())).unwrap();
        let err = client.triggers.encode_create_oco_params(&p).unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");

        let mut p = sell_oco();
        p.symbol = "DOGE-USDT".into();
        assert!(client.triggers.encode_create_oco_params(&p).is_err());
    }

    #[test]
    fn batch_cancel_sends_each_trigger_id_once_with_a_request_id() {
        let client = client();
        let base58 = bs58::encode(42_u64.to_be_bytes()).into_string();
        let wire = client
            .triggers
            .encode_batch_cancel(
                &[base58.clone(), "40".into()],
                SubAccountScope::Id(5),
                Some(" retry-1 ".into()),
            )
            .unwrap();
        assert_eq!(wire.trigger_ids, vec![42, 40]);
        assert_eq!(wire.subaccount_id, Some(5));
        assert_eq!(wire.request_id, "retry-1");

        let generated = client
            .triggers
            .encode_batch_cancel(&["43".into()], SubAccountScope::Default, None)
            .unwrap();
        assert!(
            generated.request_id.starts_with("batch-cancel-triggers-"),
            "{}",
            generated.request_id
        );
        assert_eq!(generated.subaccount_id, None);

        let fifty: Vec<String> = (1..=50).map(|n| (n * 1000).to_string()).collect();
        assert_eq!(
            client
                .triggers
                .encode_batch_cancel(&fifty, SubAccountScope::Default, None)
                .unwrap()
                .trigger_ids
                .len(),
            50
        );
    }

    #[test]
    fn batch_cancel_refuses_invalid_requests_before_sending() {
        let client = client();
        let refused = |ids: Vec<String>, request_id: Option<&str>, want: &str| {
            let err = client
                .triggers
                .encode_batch_cancel(
                    &ids,
                    SubAccountScope::Default,
                    request_id.map(str::to_owned),
                )
                .unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{want}: {err:?}");
            assert!(err.to_string().contains(want), "{want}: {err}");
        };
        refused(Vec::new(), None, "at least one trigger_id");
        let fifty_one: Vec<String> = (1..=51).map(|n| (n * 1000).to_string()).collect();
        refused(fifty_one, None, "at most 50 trigger_ids; received 51");
        // The same trigger, once in base58 and once in decimal (a `0` keeps
        // a decimal ID from reading as base58).
        let base58 = bs58::encode(40_u64.to_be_bytes()).into_string();
        refused(vec![base58, "40".into()], None, "duplicate trigger_id");
        refused(vec!["0".into()], None, "non-zero ID");
        refused(vec!["not an id".into()], None, "trigger_id");
        refused(vec!["42".into()], Some("has space"), "request_id");
        refused(
            vec!["42".into()],
            Some("r".repeat(65).as_str()),
            "request_id",
        );
    }
}
