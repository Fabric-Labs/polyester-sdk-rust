//! Sub-account decoders.

use buffa::Enumeration;

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
    SubaccountMemberView,
};

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> i64 {
    match ts {
        Some(t) => t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000,
        None => 0,
    }
}

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>) -> String {
    value
        .as_known()
        .map(|e| e.proto_name().to_owned())
        .unwrap_or_default()
}

pub fn subaccount_from_proto(msg: &Subaccount) -> SubAccount {
    SubAccount {
        subaccount_id: format_uint64_id(msg.id),
        label: msg.label.clone(),
        smart_account_address: msg.smart_account_address.clone(),
        status: msg.status.clone(),
        updated_at_ms: timestamp_ms(msg.updated_at.as_option()),
    }
}

pub fn subaccounts_list_from_proto(msg: &ListSubaccountsResponse) -> SubAccountsList {
    SubAccountsList {
        subaccounts: msg.subaccounts.iter().map(subaccount_from_proto).collect(),
    }
}

pub fn get_subaccount_from_proto(msg: &GetSubaccountResponse) -> GetSubaccountResult {
    GetSubaccountResult {
        subaccount: msg.subaccount.as_option().map(subaccount_from_proto),
    }
}

pub fn create_subaccount_from_proto(msg: &CreateSubaccountResponse) -> CreateSubaccountResult {
    CreateSubaccountResult {
        subaccount_id: format_uint64_id(msg.subaccount_id),
    }
}

fn member_from_proto(msg: &SubaccountMemberView) -> SubAccountMember {
    SubAccountMember {
        grantee_account_id: format_uint64_id(msg.account_id),
        role: enum_label(&msg.role),
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
        role: enum_label(&msg.role),
        status: enum_label(&msg.status),
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
                event_type: format!("{}:{}", e.entity_kind, e.event_action),
                ts_ms: timestamp_ms(e.created_at.as_option()),
            })
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::{ActivityEvent, SubaccountRole};

    #[test]
    fn subaccounts_list_maps_fields() {
        let msg = ListSubaccountsResponse {
            subaccounts: vec![Subaccount {
                id: 12,
                label: "trading".into(),
                smart_account_address: "0xabc".into(),
                status: "active".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccounts_list_from_proto(&msg);
        assert_eq!(result.subaccounts.len(), 1);
        assert_eq!(result.subaccounts[0].subaccount_id, format_uint64_id(12));
        assert_eq!(result.subaccounts[0].smart_account_address, "0xabc");
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
                entity_kind: "invite".into(),
                event_action: "created".into(),
                ..Default::default()
            }],
            next_page_token: "n".into(),
            ..Default::default()
        };
        let result = subaccount_activity_list_from_proto(&msg);
        assert_eq!(result.events[0].event_type, "invite:created");
        assert_eq!(result.next_page_token, "n");
    }
}
