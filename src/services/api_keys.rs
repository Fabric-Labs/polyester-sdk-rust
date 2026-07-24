//! API keys service (Go `services/api_keys.go` parity).

use super::ServiceContext;
use super::scope;
use super::unary;
use crate::auth;
use crate::codecs::decode::{api_key_from_get_proto, api_keys_list_from_proto};
use crate::connect::auth::v1::ApiKeyServiceClient;
use crate::errors::Result;
use crate::models::{ApiKeySummary, ApiKeysList, Ed25519Keypair};
use crate::proto::auth::v1::{GetApiKeyRequest, ListApiKeysRequest};

#[derive(Clone)]
pub struct ApiKeysService {
    ctx: ServiceContext,
}

impl ApiKeysService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub fn connect_client(&self) -> ApiKeyServiceClient<crate::transport::SharedTransport> {
        ApiKeyServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    pub async fn list(&self, subaccount_id: Option<u64>) -> Result<ApiKeysList> {
        let req = ListApiKeysRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ApiKeyService/ListApiKeys",
            req,
            |req, opts| client.list_api_keys_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(api_keys_list_from_proto(&resp))
    }

    pub async fn get(&self, key_id: &str) -> Result<Option<ApiKeySummary>> {
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ApiKeyService/GetApiKey",
            GetApiKeyRequest {
                key_id: key_id.to_owned(),
                ..Default::default()
            },
            |req, opts| client.get_api_key_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(api_key_from_get_proto(&resp))
    }

    /// Generate a local Ed25519 keypair for API key creation (secret never sent to API).
    pub fn generate_keypair(&self) -> Ed25519Keypair {
        let (secret_key_hex, public_key_hex) = auth::generate_ed25519_keypair();
        let public_key = hex::decode(&public_key_hex).unwrap_or_default();
        let secret_key = hex::decode(&secret_key_hex).unwrap_or_default();
        Ed25519Keypair {
            public_key_hex,
            secret_key_hex,
            public_key,
            secret_key,
        }
    }

    /// Subscribe to private API key updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<ApiKeySummary>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:api-keys:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::api_key_from_bytes)
            .await
    }
}
