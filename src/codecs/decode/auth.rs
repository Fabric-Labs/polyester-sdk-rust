//! Auth response decoders.

use crate::codecs::decode::common::{require_id, require_str_id};
use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::{format_id, format_uint64_id};
use crate::errors::{Error, Result};
use crate::models::{
    AccountIdentity, MeResult, MeSession, UserProfile, UsernameHistoryEntry, UsernameHistoryList,
};
use crate::proto::auth::v1::{
    AccountIdentity as ProtoAccountIdentity, AccountScopeType, GetUsernameHistoryResponse,
    MeResponse, UserProfile as ProtoUserProfile,
};

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> Option<i64> {
    ts.map(|t| t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000)
}

/// Decode a `Me` answer, refused with `Error::ResponseContract` without its
/// account ID, with a session without its ID, with a sub-account key without
/// its sub-account ID, or with a sub-account ID for any other caller.
pub fn me_from_proto(msg: &MeResponse) -> Result<MeResult> {
    require_id("Me", "account_id", msg.account_id)?;
    if let Some(session) = msg.session.as_option() {
        require_str_id("Me", "session.session_id", &session.session_id)?;
    }
    let subaccount_key = msg.api_key_scope.as_known() == Some(AccountScopeType::SCOPE_SUBACCOUNT);
    match msg.subaccount_id {
        Some(id) if subaccount_key => require_id("Me", "subaccount_id", id)?,
        None if subaccount_key => require_id("Me", "subaccount_id", 0)?,
        Some(_) => {
            return Err(Error::response_contract(
                "Me",
                "subaccount_id sent for a caller that is not a sub-account key",
            ));
        }
        None => {}
    }
    Ok(MeResult {
        account_id: format_uint64_id(msg.account_id),
        api_key_id: msg
            .api_key_id
            .as_ref()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(str::to_owned),
        username: if msg.username.is_empty() {
            None
        } else {
            Some(msg.username.clone())
        },
        root_smart_account_address: if msg.root_smart_account_address.is_empty() {
            None
        } else {
            Some(msg.root_smart_account_address.clone())
        },
        session: msg.session.as_option().map(|session| MeSession {
            session_id: session.session_id.clone(),
            session_level: enum_proto_name(&session.session_level),
            authentication_methods: session.authentication_methods.clone(),
            auth_time_ms: timestamp_ms(session.auth_time.as_option()),
        }),
        api_key_scope: (msg.api_key_scope.as_known() != Some(AccountScopeType::SCOPE_UNSPECIFIED))
            .then(|| enum_proto_name(&msg.api_key_scope)),
        subaccount_id: msg.subaccount_id.map(format_uint64_id),
    })
}

pub fn profile_from_proto(msg: &ProtoUserProfile) -> UserProfile {
    UserProfile {
        username: msg.username.clone(),
        bio: msg.bio.clone(),
        website: msg.website.clone(),
        twitter: msg.twitter.clone(),
        twitter_verified: msg.twitter_verified,
        discord: msg.discord.clone(),
        discord_verified: msg.discord_verified,
        avatar_url: msg.avatar_url.clone(),
        created_at_ms: timestamp_ms(msg.created_at.as_option()),
        next_username_change_at_ms: timestamp_ms(msg.next_username_change_at.as_option()),
        vip_tier: msg.vip_tier,
        username_unlocked: msg.username_unlocked,
    }
}

pub fn username_history_from_proto(msg: &GetUsernameHistoryResponse) -> UsernameHistoryList {
    UsernameHistoryList {
        entries: msg
            .history
            .iter()
            .map(|e| UsernameHistoryEntry {
                username: e.username.clone(),
                changed_at_ms: timestamp_ms(e.set_at.as_option()),
            })
            .collect(),
    }
}

pub fn account_identity_from_proto(msg: &ProtoAccountIdentity) -> AccountIdentity {
    AccountIdentity {
        account_id: format_id(msg.account_id),
        username: msg.username.clone(),
        avatar_url: msg.avatar_url.clone(),
        root_smart_account_address: msg.root_smart_account_address.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::scalars::format_uint64_id;
    use crate::proto::auth::v1::MeResponse;

    #[test]
    fn me_from_proto_formats_ids() {
        let msg = MeResponse {
            account_id: 42,
            api_key_id: Some("ak_0123456789abcdef0123456789abcdef".into()),
            username: "alice".into(),
            root_smart_account_address: "0xabc".into(),
            ..Default::default()
        };
        let me = me_from_proto(&msg).unwrap();
        assert_eq!(me.account_id, format_uint64_id(42));
        assert_eq!(
            me.api_key_id.as_deref(),
            Some("ak_0123456789abcdef0123456789abcdef")
        );
        assert_eq!(me.username.as_deref(), Some("alice"));
        assert_eq!(me.root_smart_account_address.as_deref(), Some("0xabc"));
    }

    /// A key's scope and bound sub-account; the channel target is the
    /// sub-account for a sub-account key and the account otherwise.
    #[test]
    fn me_names_the_scope_and_sub_account_of_the_key() {
        use crate::proto::auth::v1::AccountScopeType;
        let sub = me_from_proto(&MeResponse {
            account_id: 42,
            api_key_scope: AccountScopeType::SCOPE_SUBACCOUNT.into(),
            subaccount_id: Some(60),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(sub.api_key_scope.as_deref(), Some("SCOPE_SUBACCOUNT"));
        assert_eq!(sub.subaccount_id.as_deref(), Some("23"));
        assert_eq!(sub.account_channel_target(), "23");

        let root = me_from_proto(&MeResponse {
            account_id: 42,
            api_key_scope: AccountScopeType::SCOPE_ROOT.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(root.api_key_scope.as_deref(), Some("SCOPE_ROOT"));
        assert_eq!(root.subaccount_id, None);
        assert_eq!(root.account_channel_target(), format_uint64_id(42));

        let session = me_from_proto(&MeResponse {
            account_id: 42,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(session.api_key_scope, None);
        assert_eq!(session.account_channel_target(), format_uint64_id(42));

        let future = me_from_proto(&MeResponse {
            account_id: 42,
            api_key_scope: buffa::EnumValue::from(9),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(future.api_key_scope.as_deref(), Some("UNKNOWN(9)"));
    }

    /// A sub-account key without its sub-account, a zero sub-account, or a
    /// sub-account for any other caller breaks the contract.
    #[test]
    fn me_refuses_an_inconsistent_key_scope() {
        use crate::proto::auth::v1::AccountScopeType;
        for (scope, subaccount_id) in [
            (AccountScopeType::SCOPE_SUBACCOUNT, None),
            (AccountScopeType::SCOPE_SUBACCOUNT, Some(0)),
            (AccountScopeType::SCOPE_ROOT, Some(60)),
            (AccountScopeType::SCOPE_UNSPECIFIED, Some(60)),
        ] {
            let err = me_from_proto(&MeResponse {
                account_id: 42,
                api_key_scope: scope.into(),
                subaccount_id,
                ..Default::default()
            })
            .unwrap_err();
            assert!(
                matches!(err, crate::Error::ResponseContract { .. }),
                "{scope:?} {subaccount_id:?}: {err:?}"
            );
        }
    }

    #[test]
    fn profile_from_proto_maps_fields() {
        let msg = ProtoUserProfile {
            username: "alice".into(),
            bio: "hi".into(),
            twitter_verified: true,
            vip_tier: 2,
            username_unlocked: true,
            ..Default::default()
        };
        let profile = profile_from_proto(&msg);
        assert_eq!(profile.username, "alice");
        assert_eq!(profile.bio, "hi");
        assert!(profile.twitter_verified);
        assert_eq!(profile.vip_tier, 2);
        assert!(profile.username_unlocked);
    }

    /// `MeResponse.session`: the presented access token's session, read with
    /// its ID, level, methods and time; `None` when the answer has none.
    #[test]
    fn me_keeps_the_session_when_the_server_sends_one() {
        use crate::models::MeSession;
        use crate::proto::auth::v1::{SessionInfo, SessionLevel};
        let msg = MeResponse {
            account_id: 18,
            username: "alice".into(),
            session: SessionInfo {
                session_id: "sess_1".into(),
                session_level: SessionLevel::PRIMARY_AUTHENTICATED.into(),
                authentication_methods: vec!["wallet".into()],
                auth_time: buffa_types::google::protobuf::Timestamp {
                    seconds: 3,
                    nanos: 4_000_000,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let me = me_from_proto(&msg).unwrap();
        assert_eq!(
            me.session,
            Some(MeSession {
                session_id: "sess_1".into(),
                session_level: "PRIMARY_AUTHENTICATED".into(),
                authentication_methods: vec!["wallet".into()],
                auth_time_ms: Some(3_004),
            })
        );
        assert_eq!(me.username.as_deref(), Some("alice"));

        let without = MeResponse {
            account_id: 18,
            ..Default::default()
        };
        assert_eq!(me_from_proto(&without).unwrap().session, None);
    }
}
