//! Sub-account decoders.

use crate::codecs::decode::admin::api_key_from_proto;
use crate::codecs::decode::balances::balances_list_from_proto;
use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    CreateSubaccountResult, GetSubaccountResult, SubAccount, SubAccountActivityEvent,
    SubAccountActivityList, SubAccountInvite, SubAccountInvitesList, SubAccountMember,
    SubAccountMembersList, SubAccountsList,
};
use crate::proto::auth::v1::{
    CreateSubaccountResponse, GetSubaccountResponse, InviteSubaccountMemberResponse,
    ListSubaccountEventsResponse, ListSubaccountInvitesResponse, ListSubaccountMembersResponse,
    ListSubaccountsResponse, RespondSubaccountInviteResponse, Subaccount, SubaccountInvite,
    SubaccountInviteDirection, SubaccountMemberView, SubaccountRole, SubaccountStatus,
};

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> i64 {
    match ts {
        Some(t) => t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000,
        None => 0,
    }
}

fn subaccount_status_name(status: &buffa::EnumValue<SubaccountStatus>) -> String {
    match status.as_known() {
        Some(SubaccountStatus::Active) => "active".to_owned(),
        Some(SubaccountStatus::Disabled) => "disabled".to_owned(),
        Some(SubaccountStatus::Unspecified) => String::new(),
        None => format!("UNKNOWN({})", status.to_i32()),
    }
}

pub fn invite_direction_from_label(direction: &str) -> Result<SubaccountInviteDirection, String> {
    match direction.trim().to_ascii_lowercase().as_str() {
        "" | "unspecified" => Ok(SubaccountInviteDirection::DirectionUnspecified),
        "incoming" => Ok(SubaccountInviteDirection::Incoming),
        "outgoing" => Ok(SubaccountInviteDirection::Outgoing),
        other => Err(format!(
            "invites_direction must be incoming, outgoing, or empty; got {other:?}"
        )),
    }
}

fn activity_label<T: buffa::Enumeration>(value: &buffa::EnumValue<T>, prefix: &str) -> String {
    let label = enum_proto_name(value);
    match label.strip_prefix(prefix) {
        Some(known) => known.to_ascii_lowercase(),
        None => label,
    }
}

/// The caller's role as its proto name (`"VIEWER"`, ...), as member and
/// invite roles read. `None` when unspecified, as on a removal: no role is
/// invented.
fn subaccount_role(role: &buffa::EnumValue<SubaccountRole>) -> Option<String> {
    (role.to_i32() != SubaccountRole::SUBACCOUNT_ROLE_UNSPECIFIED as i32)
        .then(|| enum_proto_name(role))
}

pub fn subaccount_from_proto(msg: &Subaccount) -> SubAccount {
    SubAccount {
        subaccount_id: format_uint64_id(msg.id),
        label: msg.label.clone(),
        smart_account_address: msg.smart_account_address.clone(),
        status: subaccount_status_name(&msg.status),
        updated_at: msg.updated_at.as_option().cloned(),
        revision: msg.revision,
        role: subaccount_role(&msg.role),
        removed: msg.removed,
    }
}

pub fn subaccounts_list_from_proto(msg: &ListSubaccountsResponse) -> SubAccountsList {
    SubAccountsList {
        subaccounts: msg.subaccounts.iter().map(subaccount_from_proto).collect(),
    }
}

/// Decode a `GetSubaccount` answer. `balances` is the balances list when the
/// answer carries it, `None` otherwise.
pub fn get_subaccount_from_proto(msg: &GetSubaccountResponse) -> GetSubaccountResult {
    GetSubaccountResult {
        subaccount: msg.subaccount.as_option().map(subaccount_from_proto),
        api_keys: msg.api_keys.iter().map(api_key_from_proto).collect(),
        balances: msg.balances.as_option().map(balances_list_from_proto),
    }
}

pub fn create_subaccount_from_proto(msg: &CreateSubaccountResponse) -> CreateSubaccountResult {
    CreateSubaccountResult {
        subaccount_id: format_uint64_id(msg.subaccount_id),
        revision: msg.revision,
    }
}

fn member_from_proto(msg: &SubaccountMemberView) -> SubAccountMember {
    SubAccountMember {
        grantee_account_id: format_uint64_id(msg.account_id),
        role: enum_proto_name(&msg.role),
    }
}

pub fn subaccount_members_list_from_proto(
    msg: &ListSubaccountMembersResponse,
) -> SubAccountMembersList {
    SubAccountMembersList {
        members: msg.members.iter().map(member_from_proto).collect(),
    }
}

fn invite_from_proto(msg: &SubaccountInvite) -> SubAccountInvite {
    SubAccountInvite {
        invite_id: format_uint64_id(msg.id),
        grantee_account_id: format_uint64_id(msg.grantee_account_id),
        role: enum_proto_name(&msg.role),
        status: enum_proto_name(&msg.status),
    }
}

pub fn invite_subaccount_member_from_proto(
    msg: &InviteSubaccountMemberResponse,
) -> Option<SubAccountInvite> {
    msg.invite.as_option().map(invite_from_proto)
}

pub fn respond_subaccount_invite_from_proto(
    msg: &RespondSubaccountInviteResponse,
) -> Option<SubAccountInvite> {
    msg.invite.as_option().map(invite_from_proto)
}

pub fn subaccount_invites_list_from_proto(
    msg: &ListSubaccountInvitesResponse,
) -> SubAccountInvitesList {
    SubAccountInvitesList {
        invites: msg.invites.iter().map(invite_from_proto).collect(),
    }
}

pub fn subaccount_activity_list_from_proto(
    msg: &ListSubaccountEventsResponse,
) -> SubAccountActivityList {
    SubAccountActivityList {
        events: msg
            .events
            .iter()
            .map(|e| SubAccountActivityEvent {
                event_type: format!(
                    "{}:{}",
                    activity_label(&e.entity_kind, "ACTIVITY_ENTITY_"),
                    activity_label(&e.event_action, "ACTIVITY_ACTION_")
                ),
                ts_ms: timestamp_ms(e.created_at.as_option()),
            })
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::{ActivityEntityKind, ActivityEvent, ActivityEventAction};
    use buffa_types::google::protobuf::Timestamp;

    #[test]
    fn subaccounts_list_maps_fields() {
        let msg = ListSubaccountsResponse {
            subaccounts: vec![Subaccount {
                id: 12,
                label: "trading".into(),
                smart_account_address: "0xabc".into(),
                status: SubaccountStatus::Active.into(),
                updated_at: Timestamp {
                    seconds: 3,
                    nanos: 123_456_000,
                    ..Default::default()
                }
                .into(),
                revision: 4,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccounts_list_from_proto(&msg);
        assert_eq!(result.subaccounts.len(), 1);
        assert_eq!(result.subaccounts[0].subaccount_id, format_uint64_id(12));
        assert_eq!(result.subaccounts[0].smart_account_address, "0xabc");
        assert_eq!(result.subaccounts[0].status, "active");
        assert_eq!(
            result.subaccounts[0].updated_at.as_ref().unwrap().nanos,
            123_456_000
        );
        assert_eq!(result.subaccounts[0].revision, 4);
    }

    /// POLY-9397: a sub-account update keeps `removed` and the caller's role.
    /// A removal (`removed`, role unspecified) never reads as an active viewer,
    /// even with the same revision and `updated_at`, through the stream
    /// decoder and in proto JSON.
    #[test]
    fn poly_9397_subaccount_update_keeps_removed_and_role() {
        let update = |removed: bool, role: SubaccountRole, revision: u64| Subaccount {
            id: 42,
            role: role.into(),
            status: SubaccountStatus::Active.into(),
            revision,
            removed,
            updated_at: Timestamp {
                seconds: 1_700_000_000,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let mut failures = Vec::new();
        for revision in [0, 3, 9_007_199_254_740_993] {
            let removal = update(true, SubaccountRole::SUBACCOUNT_ROLE_UNSPECIFIED, revision);
            let viewer = update(false, SubaccountRole::VIEWER, revision);
            for (label, msg, want_removed, want_role) in [
                ("removal", &removal, true, None),
                ("viewer", &viewer, false, Some("VIEWER")),
            ] {
                let bytes = buffa::Message::encode_to_vec(msg);
                let json: Subaccount =
                    serde_json::from_slice(&serde_json::to_vec(msg).unwrap()).unwrap();
                assert_eq!(&json, msg, "proto JSON round trip");
                for (path, got) in [
                    (
                        "from_bytes",
                        crate::codecs::decode::subaccount_from_bytes(&bytes).unwrap(),
                    ),
                    ("from_json", subaccount_from_proto(&json)),
                ] {
                    if got.removed != want_removed || got.role.as_deref() != want_role {
                        failures.push(format!(
                            "{label} rev {revision} {path}: removed={} role={:?}",
                            got.removed, got.role
                        ));
                    }
                    assert_eq!(got.revision, revision);
                }
            }
            let removal = crate::codecs::decode::subaccount_from_bytes(
                &buffa::Message::encode_to_vec(&removal),
            )
            .unwrap();
            let viewer = crate::codecs::decode::subaccount_from_bytes(
                &buffa::Message::encode_to_vec(&viewer),
            )
            .unwrap();
            if removal == viewer {
                failures.push(format!("rev {revision}: removal decodes as the viewer"));
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
        // A role change applies as it arrives; each role is kept.
        let trader = subaccount_from_proto(&update(false, SubaccountRole::TRADER, 3));
        assert_eq!(trader.role.as_deref(), Some("TRADER"));
    }

    #[test]
    fn members_list_maps_grantee_and_role() {
        let msg = ListSubaccountMembersResponse {
            members: vec![SubaccountMemberView {
                account_id: 99,
                role: SubaccountRole::TRADER.into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccount_members_list_from_proto(&msg);
        assert_eq!(result.members.len(), 1);
        assert_eq!(result.members[0].grantee_account_id, format_uint64_id(99));
        assert!(!result.members[0].role.is_empty());
    }

    #[test]
    fn activity_list_joins_entity_and_action() {
        let msg = ListSubaccountEventsResponse {
            events: vec![ActivityEvent {
                entity_kind: ActivityEntityKind::ACTIVITY_ENTITY_INVITE.into(),
                event_action: ActivityEventAction::ACTIVITY_ACTION_CREATED.into(),
                ..Default::default()
            }],
            next_page_token: "n".into(),
            ..Default::default()
        };
        let result = subaccount_activity_list_from_proto(&msg);
        assert_eq!(result.events[0].event_type, "invite:created");
        assert_eq!(result.next_page_token, "n");
    }

    #[test]
    fn create_subaccount_maps_revision() {
        let result = create_subaccount_from_proto(&CreateSubaccountResponse {
            subaccount_id: 42,
            revision: 1,
            ..Default::default()
        });
        assert_eq!(result.subaccount_id, format_uint64_id(42));
        assert_eq!(result.revision, 1);
    }

    #[test]
    fn invite_direction_from_label_validates() {
        assert_eq!(
            invite_direction_from_label("").unwrap(),
            SubaccountInviteDirection::DirectionUnspecified
        );
        assert_eq!(
            invite_direction_from_label("incoming").unwrap(),
            SubaccountInviteDirection::Incoming
        );
        assert_eq!(
            invite_direction_from_label("OUTGOING").unwrap(),
            SubaccountInviteDirection::Outgoing
        );
        assert!(invite_direction_from_label("sideways").is_err());
    }
}
