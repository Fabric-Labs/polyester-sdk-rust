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
    ApiKeyStatus, ApiKeyUpdateSpec, CreateApiKeyRequest, DeleteApiKeyRequest, GetApiKeyRequest,
    ListApiKeysRequest, UpdateApiKeyRequest,
};
use buffa::Enumeration;
use buffa_types::google::protobuf::{FieldMask, Timestamp};

/// Presence-aware patch fields for [`ApiKeysService::update`].
///
/// - `None` omits the field from the update mask.
/// - `Some(value)` selects the field; empty string / empty list / `false` / `0` are sent as-is.
/// - `expires_at`: `None` omits; `Some(None)` clears expiry; `Some(Some(ts))` sets expiry.
#[derive(Debug, Clone, Default)]
pub struct UpdateApiKeyParams {
    pub expected_revision: u64,
    pub label: Option<String>,
    pub icon: Option<String>,
    pub color: Option<String>,
    pub status: Option<String>,
    pub ip_whitelist: Option<Vec<String>>,
    pub expires_at: Option<Option<Timestamp>>,
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
        let req = build_update_api_key_request(key_id, params)?;
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

/// Build a durable-auth `UpdateApiKeyRequest` from presence-aware params.
pub fn build_update_api_key_request(
    key_id: &str,
    params: UpdateApiKeyParams,
) -> Result<UpdateApiKeyRequest> {
    if params.expected_revision == 0 {
        return Err(Error::validation(
            "expected_revision must be a positive revision from a prior read",
        ));
    }

    let mut spec = ApiKeyUpdateSpec::default();
    let mut paths = Vec::new();

    if let Some(label) = params.label {
        paths.push("label".to_owned());
        spec.label = label;
    }
    if let Some(icon) = params.icon {
        paths.push("icon".to_owned());
        spec.icon = icon;
    }
    if let Some(color) = params.color {
        paths.push("color".to_owned());
        spec.color = color;
    }
    if let Some(status) = params.status {
        paths.push("status".to_owned());
        spec.status = api_key_status_from_label(&status)?.into();
    }
    if let Some(cidrs) = params.ip_whitelist {
        paths.push("ip_whitelist".to_owned());
        spec.ip_whitelist = cidrs;
    }
    if let Some(expires) = params.expires_at {
        paths.push("expires_at".to_owned());
        match expires {
            Some(ts) => spec.expires_at = ts.into(),
            // Selected clear: leave the message field unset (null / omission).
            None => spec.expires_at = buffa::MessageField::none(),
        }
    }

    if paths.is_empty() {
        return Err(Error::validation(
            "update_mask must be non-empty; set at least one field on UpdateApiKeyParams",
        ));
    }

    Ok(UpdateApiKeyRequest {
        key_id: key_id.to_owned(),
        api_key: spec.into(),
        update_mask: FieldMask {
            paths,
            ..Default::default()
        }
        .into(),
        expected_revision: params.expected_revision,
        ..Default::default()
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_request_builds_nested_spec_and_mask() {
        let req = build_update_api_key_request(
            "ak_abc",
            UpdateApiKeyParams {
                expected_revision: 7,
                label: Some(String::new()),
                icon: Some("⚡".into()),
                color: None,
                status: Some("disabled".into()),
                ip_whitelist: Some(vec![]),
                expires_at: Some(None),
            },
        )
        .unwrap();

        assert_eq!(req.key_id, "ak_abc");
        assert_eq!(req.expected_revision, 7);
        let mask = req.update_mask.as_option().unwrap();
        assert_eq!(
            mask.paths,
            vec!["label", "icon", "status", "ip_whitelist", "expires_at"]
        );
        let spec = req.api_key.as_option().unwrap();
        assert_eq!(spec.label, "");
        assert_eq!(spec.icon, "⚡");
        assert_eq!(spec.status.as_known(), Some(ApiKeyStatus::DISABLED));
        assert!(spec.ip_whitelist.is_empty());
        assert!(!spec.expires_at.is_set());
    }

    #[test]
    fn update_request_sets_expires_at_when_provided() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
            ..Default::default()
        };
        let req = build_update_api_key_request(
            "ak_abc",
            UpdateApiKeyParams {
                expected_revision: 1,
                expires_at: Some(Some(ts.clone())),
                ..Default::default()
            },
        )
        .unwrap();
        let spec = req.api_key.as_option().unwrap();
        assert_eq!(spec.expires_at.as_option().unwrap().seconds, ts.seconds);
        assert_eq!(
            req.update_mask.as_option().unwrap().paths,
            vec!["expires_at"]
        );
    }

    #[test]
    fn update_request_rejects_zero_revision_and_empty_mask() {
        let err = build_update_api_key_request(
            "ak_abc",
            UpdateApiKeyParams {
                expected_revision: 0,
                label: Some("x".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected_revision"));

        let err = build_update_api_key_request(
            "ak_abc",
            UpdateApiKeyParams {
                expected_revision: 1,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("update_mask"));
    }
}
