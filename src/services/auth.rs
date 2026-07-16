use super::ServiceContext;
use super::unary;
use crate::codecs::decode::me_from_proto;
use crate::connect::auth::v1::AuthServiceClient;
use crate::errors::Result;
use crate::models::MeResult;
use crate::proto::auth::v1::MeRequest;

#[derive(Clone)]
pub struct AuthService {
    ctx: ServiceContext,
}

impl AuthService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn me(&self) -> Result<MeResult> {
        let client = AuthServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let req = MeRequest::default();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.AuthService/Me",
            req,
            |req, opts| client.me_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(me_from_proto(&resp))
    }
}
