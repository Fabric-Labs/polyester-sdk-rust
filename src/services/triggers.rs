use super::ServiceContext;
use super::unary;
use crate::connect::triggers::v1::TriggersServiceClient;
use crate::errors::Result;
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

    pub async fn list(
        &self,
        req: ListTriggersRequest,
    ) -> Result<crate::proto::triggers::v1::ListTriggersResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggers",
            req,
            |req, opts| client.list_triggers_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn get(
        &self,
        req: GetTriggerRequest,
    ) -> Result<crate::proto::triggers::v1::GetTriggerResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/GetTrigger",
            req,
            |req, opts| client.get_trigger_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn create(
        &self,
        req: CreateTriggerRequest,
    ) -> Result<crate::proto::triggers::v1::CreateTriggerResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CreateTrigger",
            req,
            |req, opts| client.create_trigger_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn cancel(
        &self,
        req: CancelTriggerRequest,
    ) -> Result<crate::proto::triggers::v1::CancelTriggerResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/CancelTrigger",
            req,
            |req, opts| client.cancel_trigger_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn pause(
        &self,
        req: PauseTriggerRequest,
    ) -> Result<crate::proto::triggers::v1::PauseTriggerResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/PauseTrigger",
            req,
            |req, opts| client.pause_trigger_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn resume(
        &self,
        req: ResumeTriggerRequest,
    ) -> Result<crate::proto::triggers::v1::ResumeTriggerResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ResumeTrigger",
            req,
            |req, opts| client.resume_trigger_with_options(req, opts),
        )
        .await?
        .into_owned())
    }

    pub async fn list_events(
        &self,
        req: ListTriggerEventsRequest,
    ) -> Result<crate::proto::triggers::v1::ListTriggerEventsResponse> {
        let client = self.client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/triggers.v1.TriggersService/ListTriggerEvents",
            req,
            |req, opts| client.list_trigger_events_with_options(req, opts),
        )
        .await?
        .into_owned())
    }
}
