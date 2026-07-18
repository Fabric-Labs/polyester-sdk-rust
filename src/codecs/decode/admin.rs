//! API key and resolve-account decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::{ApiKeySummary, ApiKeysList, ResolvedAccount, ResolvedAccountsList};
use crate::proto::auth::v1::{
    ApiKey as ProtoApiKey, CreateApiKeyResponse, GetApiKeyResponse, ListApiKeysResponse,
    ResolveAccountResponse, ResolvedAccount as ProtoResolvedAccount, UpdateApiKeyResponse,
};
use buffa::Enumeration;

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> i64 {
    match ts {
        Some(t) => t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000,
        None => 0,
    }
}

pub fn api_key_from_proto(msg: &ProtoApiKey) -> ApiKeySummary {
    let status = msg
        .status
        .as_known()
        .map(|s| s.proto_name().to_owned())
        .unwrap_or_default();
    ApiKeySummary {
        key_id: msg.key_id.clone(),
        label: msg.label.clone(),
        status,
        public_key_ed25519: hex::encode(&msg.public_key_ed25519),
        // Proto has created_at (no updated_at); surface create time as updated_at_ms for Go parity.
        updated_at_ms: timestamp_ms(msg.created_at.as_option()),
    }
}

pub fn api_keys_list_from_proto(msg: &ListApiKeysResponse) -> ApiKeysList {
    ApiKeysList {
        keys: msg.api_keys.iter().map(api_key_from_proto).collect(),
    }
}

pub fn api_key_from_get_proto(msg: &GetApiKeyResponse) -> Option<ApiKeySummary> {
    msg.api_key.as_option().map(api_key_from_proto)
}

pub fn api_key_from_create_proto(msg: &CreateApiKeyResponse) -> Option<ApiKeySummary> {
    msg.api_key.as_option().map(api_key_from_proto)
}

pub fn api_key_from_update_proto(msg: &UpdateApiKeyResponse) -> Option<ApiKeySummary> {
    msg.api_key.as_option().map(api_key_from_proto)
}

pub fn resolved_account_from_proto(msg: &ProtoResolvedAccount) -> ResolvedAccount {
    let username = if !msg.subaccount_label.is_empty() {
        msg.subaccount_label.clone()
    } else {
        msg.root_username.clone()
    };
    ResolvedAccount {
        account_id: format_uint64_id(msg.account_id),
        username,
        smart_account_address: msg.smart_account_address.clone(),
    }
}

pub fn resolved_accounts_from_proto(msg: &ResolveAccountResponse) -> ResolvedAccountsList {
    ResolvedAccountsList {
        accounts: msg
            .matches
            .iter()
            .map(resolved_account_from_proto)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::ApiKeyStatus;

    #[test]
    fn api_keys_list_maps_status() {
        let msg = ListApiKeysResponse {
            api_keys: vec![ProtoApiKey {
                key_id: "key-1".into(),
                label: "bot".into(),
                status: ApiKeyStatus::Active.into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = api_keys_list_from_proto(&msg);
        assert_eq!(result.keys.len(), 1);
        assert_eq!(result.keys[0].key_id, "key-1");
        assert_eq!(result.keys[0].status, "ACTIVE");
    }

    #[test]
    fn resolved_accounts_formats_id() {
        let msg = ResolveAccountResponse {
            matches: vec![ProtoResolvedAccount {
                smart_account_address: "0x123".into(),
                kind: "subaccount".into(),
                account_id: 99,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = resolved_accounts_from_proto(&msg);
        assert_eq!(result.accounts.len(), 1);
        assert_eq!(result.accounts[0].account_id, format_uint64_id(99));
        assert_eq!(result.accounts[0].smart_account_address, "0x123");
    }
}
