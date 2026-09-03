//! Auth response decoders.

use crate::codecs::scalars::{format_id, format_uint64_id};
use crate::models::{
    AccountIdentity, MeResult, UserProfile, UsernameHistoryEntry, UsernameHistoryList,
};
use crate::proto::auth::v1::{
    AccountIdentity as ProtoAccountIdentity, GetUsernameHistoryResponse, MeResponse,
    UserProfile as ProtoUserProfile,
};

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> Option<i64> {
    ts.map(|t| t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000)
}

pub fn me_from_proto(msg: &MeResponse) -> MeResult {
    MeResult {
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
    }
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
        let me = me_from_proto(&msg);
        assert_eq!(me.account_id, format_uint64_id(42));
        assert_eq!(
            me.api_key_id.as_deref(),
            Some("ak_0123456789abcdef0123456789abcdef")
        );
        assert_eq!(me.username.as_deref(), Some("alice"));
        assert_eq!(me.root_smart_account_address.as_deref(), Some("0xabc"));
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
}
