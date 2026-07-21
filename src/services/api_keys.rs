//! API keys service (Go `services/api_keys.go` parity).

use super::ServiceContext;
use super::scope;
use super::unary;
use crate::auth;
use crate::codecs::decode::{
    api_key_from_create_proto, api_key_from_get_proto, api_key_from_update_proto,
    api_keys_list_from_proto,
};
use crate::connect::auth::v1::ApiKeyServiceClient;
use crate::errors::{Error, Result};
use crate::models::{ApiKeySummary, ApiKeysList, Ed25519Keypair};
use crate::proto::auth::v1::{
    ApiKeyStatus, CreateApiKeyRequest, DeleteApiKeyRequest, GetApiKeyRequest, IpWhitelist,
    ListApiKeysRequest, UpdateApiKeyRequest,
};
use buffa::Enumeration;
use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, Default)]
pub struct UpdateApiKeyParams {
    pub label: String,
    pub icon: String,
    pub color: String,
    pub status: Option<String>,
    pub ip_whitelist: Option<Vec<String>>,
    pub expires_at_unix_secs: Option<i64>,
}

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

    pub async fn create(
        &self,
        label: &str,
        subaccount_id: Option<u64>,
        icon: &str,
        color: &str,
        ip_whitelist: Option<Vec<String>>,
        public_key_ed25519: Option<Vec<u8>>,
    ) -> Result<Option<ApiKeySummary>> {
        let req = CreateApiKeyRequest {
            label: label.to_owned(),
            icon: icon.to_owned(),
            color: color.to_owned(),
            subaccount_id: scope::optional_subaccount(&self.ctx, subaccount_id)?,
            ip_whitelist: ip_whitelist.unwrap_or_default(),
            public_key_ed25519: public_key_ed25519.unwrap_or_default(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ApiKeyService/CreateApiKey",
            req,
            |req, opts| client.create_api_key_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(api_key_from_create_proto(&resp))
    }

    pub async fn update(
        &self,
        key_id: &str,
        params: UpdateApiKeyParams,
    ) -> Result<Option<ApiKeySummary>> {
        let mut req = UpdateApiKeyRequest {
            key_id: key_id.to_owned(),
            label: params.label,
            icon: params.icon,
            color: params.color,
            ..Default::default()
        };
        if let Some(status) = params.status.filter(|s| !s.is_empty()) {
            req.status = api_key_status_from_label(&status)?.into();
        }
        if let Some(cidrs) = params.ip_whitelist {
            req.ip_whitelist = IpWhitelist {
                cidrs,
                ..Default::default()
            }
            .into();
        }
        if let Some(secs) = params.expires_at_unix_secs {
            req.expires_at = Timestamp {
                seconds: secs,
                nanos: 0,
                ..Default::default()
            }
            .into();
        }
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ApiKeyService/UpdateApiKey",
            req,
            |req, opts| client.update_api_key_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(api_key_from_update_proto(&resp))
    }

    pub async fn delete(&self, key_id: &str) -> Result<()> {
        let client = self.connect_client();
        let _ = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ApiKeyService/DeleteApiKey",
            DeleteApiKeyRequest {
                key_id: key_id.to_owned(),
                ..Default::default()
            },
            |req, opts| client.delete_api_key_with_options(req, opts),
        )
        .await?;
        Ok(())
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

fn api_key_status_from_label(status: &str) -> Result<ApiKeyStatus> {
    let key = status.trim().to_ascii_lowercase();
    match key.as_str() {
        "active" => Ok(ApiKeyStatus::ACTIVE),
        "revoked" => Ok(ApiKeyStatus::REVOKED),
        "disabled" => Ok(ApiKeyStatus::DISABLED),
        _ => ApiKeyStatus::from_proto_name(&status.trim().to_ascii_uppercase())
            .filter(|s| *s != ApiKeyStatus::API_KEY_STATUS_UNSPECIFIED)
            .ok_or_else(|| Error::validation(format!("unknown API key status: {status}"))),
    }
}
