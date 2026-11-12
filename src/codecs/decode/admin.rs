//! API key and resolve-account decoders.

use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::{format_optional_uint64_id, format_uint64_id};
use crate::models::{ApiKeySummary, ApiKeysList, ResolvedAccount, ResolvedAccountsList};
use crate::proto::auth::v1::{
    ApiKey as ProtoApiKey, CreateApiKeyResponse, GetApiKeyResponse, ListApiKeysResponse,
    ResolveAccountResponse, ResolvedAccount as ProtoResolvedAccount, UpdateApiKeyResponse,
};

pub fn api_key_from_proto(msg: &ProtoApiKey) -> ApiKeySummary {
    let status = enum_proto_name(&msg.status);
    ApiKeySummary {
        key_id: msg.key_id.clone(),
        label: msg.label.clone(),
        icon: msg.icon.clone(),
        color: msg.color.clone(),
        created_by_actor: msg.created_by_actor.clone(),
        status,
        public_key_ed25519: hex::encode(&msg.public_key_ed25519),
        subaccount_id: format_optional_uint64_id(msg.subaccount_id.unwrap_or_default()),
        policy_id: format_optional_uint64_id(msg.policy_id.unwrap_or_default()),
        ip_whitelist: msg.ip_whitelist.clone(),
        created_at: msg.created_at.as_option().cloned(),
        last_used_at: msg.last_used_at.as_option().cloned(),
        updated_at: msg.updated_at.as_option().cloned(),
        expires_at: msg.expires_at.as_option().cloned(),
        revision: msg.revision,
        purpose: if msg.purpose.to_i32() == 0 {
            String::new()
        } else {
            enum_proto_name(&msg.purpose)
        },
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
    use buffa_types::google::protobuf::Timestamp;

    /// A key reads its purpose: `"STANDARD"` for the user's own keys and
    /// `"ASSISTANT"` for the assistant's; empty when the server names none,
    /// and `"UNKNOWN(n)"` for a purpose newer than this SDK.
    #[test]
    fn api_key_reads_its_purpose() {
        use crate::proto::auth::v1::ApiKeyPurpose;
        let purpose = |value: buffa::EnumValue<ApiKeyPurpose>| {
            api_key_from_proto(&ProtoApiKey {
                key_id: "key-1".into(),
                purpose: value,
                ..Default::default()
            })
            .purpose
        };
        assert_eq!(purpose(ApiKeyPurpose::STANDARD.into()), "STANDARD");
        assert_eq!(purpose(ApiKeyPurpose::ASSISTANT.into()), "ASSISTANT");
        assert_eq!(purpose(ApiKeyPurpose::PURPOSE_UNSPECIFIED.into()), "");
        assert_eq!(purpose(buffa::EnumValue::from(9)), "UNKNOWN(9)");
    }

    #[test]
    fn api_keys_list_maps_status() {
        let msg = ListApiKeysResponse {
            api_keys: vec![ProtoApiKey {
                key_id: "key-1".into(),
                label: "bot".into(),
                status: ApiKeyStatus::Active.into(),
                created_at: Timestamp {
                    seconds: 1,
                    ..Default::default()
                }
                .into(),
                last_used_at: Timestamp {
                    seconds: 2,
                    ..Default::default()
                }
                .into(),
                updated_at: Timestamp {
                    seconds: 3,
                    nanos: 123_456_000,
                    ..Default::default()
                }
                .into(),
                revision: 9,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = api_keys_list_from_proto(&msg);
        assert_eq!(result.keys.len(), 1);
        assert_eq!(result.keys[0].key_id, "key-1");
        assert_eq!(result.keys[0].status, "ACTIVE");
        assert_eq!(result.keys[0].created_at.as_ref().unwrap().seconds, 1);
        assert_eq!(result.keys[0].last_used_at.as_ref().unwrap().seconds, 2);
        assert_eq!(
            result.keys[0].updated_at.as_ref().unwrap().nanos,
            123_456_000
        );
        assert_eq!(result.keys[0].revision, 9);
    }

    #[test]
    fn resolved_accounts_formats_id() {
        let msg = ResolveAccountResponse {
            matches: vec![ProtoResolvedAccount {
                smart_account_address: "0x123".into(),
                kind: crate::proto::auth::v1::resolved_account::Kind::Sub.into(),
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
