use super::ServiceContext;
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
    ListTriggersOpts, ModifyTriggerParams, Trigger, TriggerEvent, TriggerEventsList,
    TriggerMutationResult, TriggersList,
};
use crate::proto::orders::v1::{FeeSource, SelfTradePreventionMode, Side};
use crate::proto::triggers::v1::{
    CancelTriggerRequest, ConditionalChildExecution, ConditionalTrigger, CreateTriggerRequest,
    GetTriggerRequest, LadderTrigger, ListTriggerEventsRequest, ListTriggersRequest,
    ModifyTriggerRequest, PauseTriggerRequest, ResumeTriggerRequest, TrailingStopTrigger,
    TriggerIntent, TriggerLimitFok, TriggerLimitGtc, TriggerLimitIoc, TwapLimitGtc, TwapTrigger,
    conditional_child_execution, modify_trigger_request, trailing_stop_trigger, trigger_intent,
    twap_trigger,
};
use crate::types::{resolve_price_ticks, resolve_qty_scaled};

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
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
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
        Ok(triggers_list_from_proto(&resp))
    }

    pub async fn list_with(&self, opts: ListTriggersOpts) -> Result<TriggersList> {
        use crate::codecs::decode::trigger_status_from_label;
        let mut req = ListTriggersRequest {
            limit: if opts.limit == 0 { 50 } else { opts.limit },
            ..Default::default()
        };
        if let Some(symbol) = opts.symbol {
            req.symbol = symbol;
        }
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
        Ok(get_trigger_from_proto(&resp))
    }

    fn encode_create_params(&self, params: &CreateTriggerParams) -> Result<CreateTriggerRequest> {
        let scale = self
            .ctx
            .catalogs
            .base_quantity_scale_for_symbol(&params.symbol);
        let qty = resolve_qty_scaled(
            &params.qty,
            scale,
            Some(&params.symbol),
            self.ctx.catalogs.symbol_id_for_symbol(&params.symbol),
        )?;
        let mut intent = TriggerIntent {
            symbol: params.symbol.clone(),
            qty_scaled: qty,
            ..Default::default()
        };
        intent.client_trigger_id = params
            .client_trigger_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(Self::new_client_trigger_id);
        if let Some(src) = params.fee_source.as_deref() {
            intent.fee_source = Self::fee_source(src)?.into();
        }
        if let Some(mode) = params.self_trade_prevention_mode.as_deref() {
            intent.self_trade_prevention_mode = Self::stp_mode(mode)?.into();
        }
        // trigger_price_source is no longer part of the create wire contract; it is
        // evaluated server-side. Accept and ignore for API compatibility.
        let _ = params.trigger_price_source.as_ref();

        let side = match params.side {
            CreateSide::Buy => Side::Buy,
            CreateSide::Sell => Side::Sell,
        };

        intent.strategy = Some(match params.trigger_type {
            CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit => {
                let trigger_price_ticks = match params.trigger_price.as_ref() {
                    Some(price) => resolve_price_ticks(price, Some(&params.symbol))?,
                    None => 0,
                };
                let mut cond = ConditionalTrigger {
                    trigger_price_ticks,
                    side: side.into(),
                    ..Default::default()
                };
                *cond.child.get_or_insert_default() = Self::encode_conditional_child(params)?;
                if matches!(params.trigger_type, CreateTriggerType::StopLoss) {
                    trigger_intent::Strategy::StopLoss(Box::new(cond))
                } else {
                    trigger_intent::Strategy::TakeProfit(Box::new(cond))
                }
            }
            CreateTriggerType::TrailingStop => {
                // Trailing stop is an implicit SELL market-IOC strategy; side,
                // order_type, tif, and post_only are ignored.
                let mut trailing = TrailingStopTrigger::default();
                if let Some(ticks) = params.trailing_distance_ticks {
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceTicks(ticks));
                } else if let Some(bps) = params.trailing_distance_bps {
                    trailing.trailing_distance =
                        Some(trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(bps));
                } else {
                    return Err(Error::validation(
                        "trailing_stop requires trailing_distance_ticks or trailing_distance_bps",
                    ));
                }
                if let Some(price) = params.activation_price.as_ref() {
                    trailing.activation_price_ticks =
                        resolve_price_ticks(price, Some(&params.symbol))?;
                }
                if let Some(ticks) = params.max_slippage_ticks {
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(ticks));
                } else if let Some(bps) = params.max_slippage_bps {
                    trailing.max_slippage =
                        Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(bps));
                }
                trigger_intent::Strategy::TrailingStop(Box::new(trailing))
            }
            CreateTriggerType::Twap => {
                let mut twap = TwapTrigger {
                    side: side.into(),
                    ..Default::default()
                };
                if let Some(ms) = params.twap_duration_ms {
                    twap.duration_ms = ms;
                }
                if let Some(ms) = params.twap_slice_interval_ms {
                    twap.slice_interval_ms = ms;
                }
                twap.execution = Some(match params.order_type {
                    CreateOrderType::Market => twap_trigger::Execution::MarketIoc(Box::default()),
                    CreateOrderType::Limit => {
                        let price = params.limit_price.as_ref().ok_or_else(|| {
                            Error::validation("twap limit slices require limit_price")
                        })?;
                        twap_trigger::Execution::LimitGtc(Box::new(TwapLimitGtc {
                            price_ticks: resolve_price_ticks(price, Some(&params.symbol))?,
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
                let mut ladder = LadderTrigger {
                    side: side.into(),
                    post_only: params.post_only,
                    ..Default::default()
                };
                if let Some(price) = params.ladder_price_min.as_ref() {
                    ladder.price_min_ticks = resolve_price_ticks(price, Some(&params.symbol))?;
                }
                if let Some(price) = params.ladder_price_max.as_ref() {
                    ladder.price_max_ticks = resolve_price_ticks(price, Some(&params.symbol))?;
                }
                if let Some(levels) = params.ladder_levels {
                    ladder.levels = levels;
                }
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

    /// Map flat (`order_type`, `time_in_force`, `limit_price`, `post_only`) params
    /// onto a stop-loss / take-profit child execution variant.
    fn encode_conditional_child(params: &CreateTriggerParams) -> Result<ConditionalChildExecution> {
        let execution = match params.order_type {
            CreateOrderType::Market => {
                conditional_child_execution::Execution::MarketIoc(Box::default())
            }
            CreateOrderType::Limit => {
                let price = params
                    .limit_price
                    .as_ref()
                    .ok_or_else(|| Error::validation("limit trigger requires limit_price"))?;
                let price_ticks = resolve_price_ticks(price, Some(&params.symbol))?;
                match params.time_in_force {
                    Some(CreateTimeInForce::Ioc) => {
                        conditional_child_execution::Execution::LimitIoc(Box::new(
                            TriggerLimitIoc {
                                price_ticks,
                                ..Default::default()
                            },
                        ))
                    }
                    Some(CreateTimeInForce::Fok) => {
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
        if let Some(ticks) = params.trailing_distance_ticks {
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceTicks(ticks));
        } else if let Some(bps) = params.trailing_distance_bps {
            req.trailing_distance =
                Some(modify_trigger_request::TrailingDistance::TrailingDistanceBps(bps));
        }
        if let Some(ticks) = params.max_slippage_ticks {
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageTicks(ticks));
        } else if let Some(bps) = params.max_slippage_bps {
            req.max_slippage = Some(modify_trigger_request::MaxSlippage::MaxSlippageBps(bps));
        }
        Ok(req)
    }

    fn new_client_trigger_id() -> String {
        format!(
            "trg-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }

    fn fee_source(label: &str) -> Result<FeeSource> {
        match label.to_ascii_lowercase().as_str() {
            "quote" => Ok(FeeSource::Quote),
            "received" => Ok(FeeSource::Received),
            _ => Err(Error::validation("fee_source must be quote or received")),
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
        Ok(trigger_mutation_from_create(&resp))
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
        Ok(trigger_mutation_from_cancel(&resp))
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
        Ok(trigger_mutation_from_pause(&resp))
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
        Ok(trigger_mutation_from_resume(&resp))
    }

    /// Modify a trigger. Price fields must be `Price` wrappers.
    pub async fn modify(&self, params: ModifyTriggerParams) -> Result<TriggerMutationResult> {
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
        Ok(trigger_mutation_from_modify(&resp))
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
        Ok(trigger_events_list_from_proto(&resp))
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
        client.catalogs.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 7,
                "base_quantity_scale": 8
            }]
        }));
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
            trigger_price_source: Some("last".into()),
            time_in_force: Some(CreateTimeInForce::Gtc),
            subaccount_id: None,
            client_trigger_id: Some("trigger-equivalence".into()),
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
            fee_source: None,
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
    fn trigger_modify_requires_a_patch() {
        let client = client();
        let params = ModifyTriggerParams {
            trigger_id: "1".into(),
            subaccount_id: None,
            trigger_price: None,
            limit_price: None,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: None,
        };
        assert!(client.triggers.encode_modify_params(&params).is_err());
    }

    fn modify_params(
        trigger_price: Option<crate::Price>,
        limit_price: Option<crate::Price>,
        activation_price: Option<crate::Price>,
    ) -> ModifyTriggerParams {
        ModifyTriggerParams {
            trigger_id: "1".into(),
            subaccount_id: None,
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
