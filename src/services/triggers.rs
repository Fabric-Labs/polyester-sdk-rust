use super::ServiceContext;
use super::unary;
use crate::codecs::decode::{
    get_trigger_from_proto, trigger_events_list_from_proto, trigger_mutation_from_cancel,
    trigger_mutation_from_create, trigger_mutation_from_pause, trigger_mutation_from_resume,
    triggers_list_from_proto,
};
use crate::connect::triggers::v1::TriggersServiceClient;
use crate::errors::Result;
use crate::models::{Trigger, TriggerEventsList, TriggerMutationResult, TriggersList};
use crate::proto::triggers::v1::{
    CancelTriggerRequest, CreateTriggerRequest, GetTriggerRequest, ListTriggerEventsRequest,
    ListTriggersRequest, PauseTriggerRequest, ResumeTriggerRequest,
};

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

    pub async fn create(&self, req: CreateTriggerRequest) -> Result<TriggerMutationResult> {
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
}
