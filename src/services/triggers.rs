use super::ServiceContext;
use super::correlation_id::require_client_style_id;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    get_trigger_from_proto, trigger_events_list_from_proto, trigger_mutation_from_cancel,
    trigger_mutation_from_create, trigger_mutation_from_modify, trigger_mutation_from_pause,
    trigger_mutation_from_resume, triggers_list_from_proto,
};
use crate::codecs::scalars::id_to_u64;
use crate::connect::triggers::v1::TriggersServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    CreateOrderType, CreateSide, CreateTimeInForce, CreateTriggerParams, CreateTriggerType,
    FeeAsset, ListTriggersOpts, ModifyTriggerParams, ResumeTriggerParams, Trigger, TriggerEvent,
    TriggerEventsList, TriggerMutationResult, TriggersList,
};
use crate::proto::orders::v1::{FeeAsset as ProtoFeeAsset, SelfTradePreventionMode, Side};
use crate::proto::triggers::v1::{
    CancelTriggerRequest, ConditionalChildExecution, ConditionalTrigger, CreateTriggerRequest,
    GetTriggerRequest, LadderTrigger, ListTriggerEventsRequest, ListTriggersRequest,
    ModifyTriggerRequest, PauseTriggerRequest, ResumeTriggerRequest, TrailingStopTrigger,
    TriggerIntent, TriggerLimitFok, TriggerLimitGtc, TriggerLimitIoc, TwapLimitGtc, TwapTrigger,
    conditional_child_execution, modify_trigger_request, trailing_stop_trigger, trigger_intent,
    twap_trigger,
};
use crate::types::{resolve_price_ticks, resolve_qty_scaled};

const MAX_BPS: i32 = 10_000;

fn validate_bps(field: &str, bps: i32, allow_clear: bool) -> Result<()> {
    if allow_clear && bps == 0 {
        return Ok(());
    }
    if !(1..=MAX_BPS).contains(&bps) {
        return Err(Error::validation(format!(
            "{field} must be between 1 and {MAX_BPS}"
        )));
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

    pub async fn list(&self, req: ListTriggersRequest) -> Result<TriggersList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggers",
            req,
            |req, opts| client.list_triggers_with_options(req, opts),
        )
        .await?
        .into_owned();
        let mut list = triggers_list_from_proto(&resp);
        for trigger in &mut list.triggers {
            if trigger.symbol.is_empty() {
                trigger.symbol = self.ctx.catalogs.display_symbol(trigger.symbol_id);
            }
        }
        Ok(list)
    }

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
        req.subaccount_id = scope::optional_subaccount(&self.ctx, opts.subaccount_id)?;
        for label in &opts.status {
            let status = trigger_status_from_label(label).map_err(Error::validation)?;
            req.status.push(status.into());
        }
        self.list(req).await
    }

    pub async fn get(&self, req: GetTriggerRequest) -> Result<Option<Trigger>> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/GetTrigger",
            req,
            |req, opts| client.get_trigger_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(get_trigger_from_proto(&resp).map(|mut trigger| {
            if trigger.symbol.is_empty() {
                trigger.symbol = self.ctx.catalogs.display_symbol(trigger.symbol_id);
            }
            trigger
        }))
    }

    /// Retrieve a trigger using the public string ID returned by create and list calls.
    pub async fn get_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: Option<u64>,
    ) -> Result<Option<Trigger>> {
        self.get(GetTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    fn encode_create_params(&self, params: &CreateTriggerParams) -> Result<CreateTriggerRequest> {
        let scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(&params.symbol)
            .or(params.qty.scale())
            .ok_or_else(|| {
                Error::validation(format!(
                    "quantity scale for {:?} is unavailable; await client.wait_for_catalogs() before creating triggers, or pass a scaled Quantity",
                    params.symbol
                ))
            })?;
        let qty = resolve_qty_scaled(
            &params.qty,
            scale,
            Some(&params.symbol),
            self.ctx.catalogs.symbol_id_for_symbol(&params.symbol),
        )?;
        let mut intent = TriggerIntent {
            symbol_id: self.ctx.catalogs.require_symbol_id(&params.symbol)?,
            qty_scaled: qty,
            ..Default::default()
        };
        intent.client_trigger_id =
            require_client_style_id(&params.client_trigger_id, "client_trigger_id")?;
        if let Some(asset) = params.fee_asset {
            intent.fee_asset = Self::fee_asset(asset, params.side)?.into();
        }
        if let Some(mode) = params.self_trade_prevention_mode.as_deref() {
            intent.self_trade_prevention_mode = Self::stp_mode(mode)?.into();
        }
        if params.trigger_price_source.is_some() {
            return Err(Error::validation(
                "trigger_price_source is not supported by the current wire contract",
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
                    .ok_or_else(|| Error::validation("stop/take-profit requires trigger_price"))
                    .and_then(|price| self.resolve_catalog_price(price, &params.symbol))?;
                if trigger_price_ticks <= 0 {
                    return Err(Error::validation("trigger_price must be positive"));
                }
                let mut cond = ConditionalTrigger {
                    trigger_price_ticks,
                    side: side.into(),
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
                // Standalone trailing stops remain SELL market-IOC. Attached
                // trailing risk may use either side (opposite the parent) and is
                // encoded via order AttachedRisk, not this create path.
                if !matches!(params.side, CreateSide::Sell) {
                    return Err(Error::validation("trailing_stop only supports side=sell"));
                }
                let mut trailing = TrailingStopTrigger {
                    side: side.into(),
                    ..Default::default()
                };
                if params.trailing_distance_ticks.is_some()
                    == params.trailing_distance_bps.is_some()
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
                        return Err(Error::validation(
                            "trailing_distance_ticks must be positive",
                        ));
                    }
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceTicks(ticks));
                } else if let Some(bps) = params.trailing_distance_bps {
                    validate_bps("trailing_distance_bps", bps, false)?;
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(bps));
                } else {
                    return Err(Error::validation(
                        "trailing_stop requires trailing_distance_ticks or trailing_distance_bps",
                    ));
                }
                if let Some(price) = params.activation_price.as_ref() {
                    trailing.activation_price_ticks =
                        self.resolve_catalog_price(price, &params.symbol)?;
                }
                if let Some(ticks) = params.max_slippage_ticks {
                    if ticks <= 0 {
                        return Err(Error::validation("max_slippage_ticks must be positive"));
                    }
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(ticks));
                } else if let Some(bps) = params.max_slippage_bps {
                    validate_bps("max_slippage_bps", bps, false)?;
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(bps));
                }
                trigger_intent::Strategy::TrailingStop(Box::new(trailing))
            }
            CreateTriggerType::Twap => {
                let duration_ms = params
                    .twap_duration_ms
                    .filter(|value| *value > 0)
                    .ok_or_else(|| Error::validation("twap requires positive duration_ms"))?;
                let slice_interval_ms = params
                    .twap_slice_interval_ms
                    .filter(|value| *value > 0)
                    .ok_or_else(|| Error::validation("twap requires positive slice_interval_ms"))?;
                if slice_interval_ms > duration_ms {
                    return Err(Error::validation(
                        "twap slice_interval_ms must not exceed duration_ms",
                    ));
                }
                let mut twap = TwapTrigger {
                    side: side.into(),
                    duration_ms,
                    slice_interval_ms,
                    ..Default::default()
                };
                twap.execution = Some(match params.order_type {
                    CreateOrderType::Market => twap_trigger::Execution::MarketIoc(Box::default()),
                    CreateOrderType::Limit => {
                        let price = params.limit_price.as_ref().ok_or_else(|| {
                            Error::validation("twap limit slices require limit_price")
                        })?;
                        twap_trigger::Execution::LimitGtc(Box::new(TwapLimitGtc {
                            price_ticks: self.resolve_catalog_price(price, &params.symbol)?,
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
                let price_min_ticks = params
                    .ladder_price_min
                    .as_ref()
                    .ok_or_else(|| Error::validation("ladder requires ladder_price_min"))
                    .and_then(|price| self.resolve_catalog_price(price, &params.symbol))?;
                let price_max_ticks = params
                    .ladder_price_max
                    .as_ref()
                    .ok_or_else(|| Error::validation("ladder requires ladder_price_max"))
                    .and_then(|price| self.resolve_catalog_price(price, &params.symbol))?;
                let levels = params
                    .ladder_levels
                    .filter(|value| *value > 0)
                    .ok_or_else(|| Error::validation("ladder requires positive ladder_levels"))?;
                if price_min_ticks <= 0 || price_max_ticks <= price_min_ticks {
                    return Err(Error::validation(
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
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            ..Default::default()
        };
        *req.trigger.get_or_insert_default() = intent;
        Ok(req)
    }

    fn resolve_catalog_price(&self, price: &crate::Price, symbol: &str) -> Result<i64> {
        resolve_price_ticks(price, Some(symbol))
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
                conditional_child_execution::Execution::MarketIoc(Box::default())
            }
            CreateOrderType::Limit => {
                let price = params
                    .limit_price
                    .as_ref()
                    .ok_or_else(|| Error::validation("limit trigger requires limit_price"))?;
                let price_ticks = self.resolve_catalog_price(price, &params.symbol)?;
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
                    // gtc or unspecified
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

    fn encode_modify_params(&self, params: &ModifyTriggerParams) -> Result<ModifyTriggerRequest> {
        if params.trigger_price.is_none()
            && params.limit_price.is_none()
            && params.activation_price.is_none()
            && params.trailing_distance_ticks.is_none()
            && params.trailing_distance_bps.is_none()
            && params.max_slippage_ticks.is_none()
            && params.max_slippage_bps.is_none()
        {
            return Err(Error::validation(
                "modify requires at least one of trigger_price, limit_price, trailing_distance_ticks, trailing_distance_bps, activation_price, max_slippage_ticks, or max_slippage_bps",
            ));
        }
        let mut req = ModifyTriggerRequest {
            trigger_id: id_to_u64(&params.trigger_id, "trigger_id")?,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            symbol_id: self.ctx.catalogs.required_symbol_id(
                params.symbol.as_deref(),
                params.symbol_id,
                "modify",
            )?,
            ..Default::default()
        };
        if let Some(price) = params.trigger_price.as_ref() {
            req.trigger_price_ticks = Some(resolve_price_ticks(price, None)?);
        }
        if let Some(price) = params.limit_price.as_ref() {
            req.limit_price_ticks = Some(resolve_price_ticks(price, None)?);
        }
        if let Some(price) = params.activation_price.as_ref() {
            req.activation_price_ticks = Some(resolve_price_ticks(price, None)?);
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
                return Err(Error::validation(
                    "trailing_distance_ticks must be positive",
                ));
            }
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceTicks(ticks));
        } else if let Some(bps) = params.trailing_distance_bps {
            validate_bps("trailing_distance_bps", bps, false)?;
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceBps(bps));
        }
        if let Some(ticks) = params.max_slippage_ticks {
            if ticks < 0 {
                return Err(Error::validation("max_slippage_ticks must be non-negative"));
            }
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageTicks(ticks));
        } else if let Some(bps) = params.max_slippage_bps {
            validate_bps("max_slippage_bps", bps, true)?;
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageBps(bps));
        }
        Ok(req)
    }

    fn fee_asset(asset: FeeAsset, side: CreateSide) -> Result<ProtoFeeAsset> {
        match (asset, side) {
            (FeeAsset::Quote, _) => Ok(ProtoFeeAsset::Quote),
            (FeeAsset::Base, CreateSide::Buy) => Ok(ProtoFeeAsset::Base),
            (FeeAsset::Base, CreateSide::Sell) => Err(Error::validation(
                "fee_asset=base is only valid for BUY triggers",
            )),
        }
    }

    fn stp_mode(label: &str) -> Result<SelfTradePreventionMode> {
        match label.to_ascii_lowercase().as_str() {
            "expire_taker" => Ok(SelfTradePreventionMode::ExpireTaker),
            "expire_maker" => Ok(SelfTradePreventionMode::ExpireMaker),
            "expire_both" => Ok(SelfTradePreventionMode::ExpireBoth),
            _ => Err(Error::validation(
                "self_trade_prevention_mode must be expire_taker, expire_maker, or expire_both",
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

    pub async fn cancel(&self, req: CancelTriggerRequest) -> Result<TriggerMutationResult> {
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
    pub async fn cancel_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: Option<u64>,
    ) -> Result<TriggerMutationResult> {
        self.cancel(CancelTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn pause(&self, req: PauseTriggerRequest) -> Result<TriggerMutationResult> {
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
    pub async fn pause_by_id(
        &self,
        trigger_id: &str,
        subaccount_id: Option<u64>,
    ) -> Result<TriggerMutationResult> {
        self.pause(PauseTriggerRequest {
            trigger_id: id_to_u64(trigger_id, "trigger_id")?,
            subaccount_id,
            ..Default::default()
        })
        .await
    }

    pub async fn resume(&self, req: ResumeTriggerRequest) -> Result<TriggerMutationResult> {
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
    pub async fn resume_by_id(
        &self,
        trigger_id: &str,
        symbol: Option<&str>,
        subaccount_id: Option<u64>,
    ) -> Result<TriggerMutationResult> {
        self.resume_with(ResumeTriggerParams {
            trigger_id: trigger_id.to_owned(),
            symbol: symbol.map(str::to_owned),
            symbol_id: None,
            subaccount_id,
        })
        .await
    }

    /// Resume a trigger. Connect requires `symbol` or `symbol_id`.
    pub async fn resume_with(&self, params: ResumeTriggerParams) -> Result<TriggerMutationResult> {
        if params.symbol.is_some() {
            self.ctx.wait_for_catalogs().await?;
        }
        self.resume(ResumeTriggerRequest {
            trigger_id: id_to_u64(&params.trigger_id, "trigger_id")?,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?,
            symbol_id: self.ctx.catalogs.required_symbol_id(
                params.symbol.as_deref(),
                params.symbol_id,
                "resume",
            )?,
            ..Default::default()
        })
        .await
    }

    /// Modify a trigger. Price fields must be `Price` wrappers.
    pub async fn modify(&self, params: ModifyTriggerParams) -> Result<TriggerMutationResult> {
        if params.symbol.is_some() {
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

    pub async fn list_events(&self, req: ListTriggerEventsRequest) -> Result<TriggerEventsList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggerEvents",
            req,
            |req, opts| client.list_trigger_events_with_options(req, opts),
        )
        .await?
        .into_owned();
        trigger_events_list_from_proto(&resp)
    }

    /// Subscribe to private trigger updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<Trigger>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:triggers:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::trigger_from_bytes)
            .await
    }

    /// Subscribe to private trigger events (requires `realtime` feature).
    pub async fn subscribe_events(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<TriggerEvent>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:spot:triggers:events:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::trigger_event_from_bytes)
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
            time_in_force: Some(CreateTimeInForce::Gtc),
            subaccount_id: None,
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
            crate::Price::from_ticks(49_000_000_000, Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_ticks(48_950_000_000, Some("BTC-USDT".into())).unwrap(),
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

    #[test]
    fn trailing_stop_rejects_buy() {
        let client = client();
        let mut params = create_params(
            crate::Quantity::from_decimal_str("0.1", 8, Some("BTC-USDT".into()), Some(7)).unwrap(),
            crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap(),
            crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap(),
        );
        params.trigger_type = CreateTriggerType::TrailingStop;
        params.side = CreateSide::Buy;
        params.trailing_distance_bps = Some(100);
        let err = client.triggers.encode_create_params(&params).unwrap_err();
        assert!(err.to_string().contains("only supports side=sell"), "{err}");
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
        missing_id.client_trigger_id = " ".into();
        assert!(client.triggers.encode_create_params(&missing_id).is_err());

        let mut missing_trigger_price = base.clone();
        missing_trigger_price.trigger_price = None;
        assert!(
            client
                .triggers
                .encode_create_params(&missing_trigger_price)
                .is_err()
        );

        let mut unsupported_source = base.clone();
        unsupported_source.trigger_price_source = Some("mark".into());
        assert!(
            client
                .triggers
                .encode_create_params(&unsupported_source)
                .is_err()
        );

        let mut trailing = base.clone();
        trailing.trigger_type = CreateTriggerType::TrailingStop;
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
            ..modify_params(None, None, None)
        };
        let wire = client.triggers.encode_modify_params(&params).unwrap();
        assert_eq!(wire.trigger_id, 42);
    }

    #[test]
    fn trigger_modify_requires_a_patch() {
        let client = client();
        let params = ModifyTriggerParams {
            trigger_id: "1".into(),
            subaccount_id: None,
            symbol: Some("BTC-USDT".into()),
            symbol_id: None,
            trigger_price: None,
            limit_price: None,
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
        params.side = CreateSide::Sell;
        params.trailing_distance_bps = Some(100);
        params.trigger_price = None;
        params.limit_price = None;
        params.max_slippage_bps = max_slippage_bps;
        params
    }

    #[test]
    fn trigger_slippage_bps_cap() {
        let client = client();
        client
            .triggers
            .encode_create_params(&trailing_create(Some(10_000)))
            .unwrap();
        assert!(
            client
                .triggers
                .encode_create_params(&trailing_create(Some(10_001)))
                .is_err()
        );

        let modify_too_high = ModifyTriggerParams {
            trigger_id: "1".into(),
            symbol: Some("BTC-USDT".into()),
            max_slippage_bps: Some(10_001),
            ..modify_params(None, None, None)
        };
        assert!(
            client
                .triggers
                .encode_modify_params(&modify_too_high)
                .is_err()
        );
    }

    #[test]
    fn poly_4684_standalone_trailing_slippage_bps_boundaries_preexisting() {
        let client = client();
        for bps in [1, 10_000] {
            client
                .triggers
                .encode_create_params(&trailing_create(Some(bps)))
                .unwrap();
        }
        for bps in [0, 10_001] {
            assert!(
                client
                    .triggers
                    .encode_create_params(&trailing_create(Some(bps)))
                    .is_err(),
                "standalone max_slippage_bps={bps} must be rejected"
            );
        }

        let base = ModifyTriggerParams {
            trigger_id: "1".into(),
            symbol: Some("BTC-USDT".into()),
            ..modify_params(None, None, None)
        };
        for bps in [1, 10_000] {
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
            trigger_id: "1".into(),
            symbol: Some("BTC-USDT".into()),
            ..modify_params(None, None, None)
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
        limit_price: Option<crate::Price>,
        activation_price: Option<crate::Price>,
    ) -> ModifyTriggerParams {
        ModifyTriggerParams {
            trigger_id: "1".into(),
            subaccount_id: None,
            symbol: Some("BTC-USDT".into()),
            symbol_id: None,
            trigger_price,
            limit_price,
            activation_price,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
        }
    }

    #[test]
    fn decimal_and_scaled_trigger_modify_encode_identically() {
        let client = client();
        let decimal = modify_params(
            Some(crate::Price::from_decimal_str("49000", Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_decimal_str("48950", Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_decimal_str("49500", Some("BTC-USDT".into())).unwrap()),
        );
        let scaled = modify_params(
            Some(crate::Price::from_ticks(49_000_000_000, Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_ticks(48_950_000_000, Some("BTC-USDT".into())).unwrap()),
            Some(crate::Price::from_ticks(49_500_000_000, Some("BTC-USDT".into())).unwrap()),
        );
        let decimal_wire = client.triggers.encode_modify_params(&decimal).unwrap();
        let scaled_wire = client.triggers.encode_modify_params(&scaled).unwrap();
        assert_eq!(decimal_wire.encode_to_vec(), scaled_wire.encode_to_vec());
    }
}
