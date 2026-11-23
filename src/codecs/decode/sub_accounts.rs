//! Sub-account decoders.

use crate::codecs::decode::admin::required_api_key;
use crate::codecs::decode::balances::balances_list_from_proto;
use crate::codecs::decode::common::require_id;
use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::decode::policies::subaccount_policy_from_proto;
use crate::codecs::scalars::format_uint64_id;
use crate::errors::Result as SdkResult;
use crate::models::{
    AccountActivityEvent, AccountActivityList, ActivityAccountDetails, ActivityActor,
    ActivityActorKind, ActivityAllowlistAction, ActivityApiKeyDetails, ActivityApiKeyKind,
    ActivityBrokerAllowlistDetails, ActivityBrokerRole, ActivityChange, ActivityDestinationDetails,
    ActivityDetails, ActivityElevatedAction, ActivityInviteDetails, ActivityItem, ActivityItemId,
    ActivityListChange, ActivityMemberDetails, ActivityPolicyDetails, ActivitySecurityDetails,
    ActivitySessionDetails, ActivitySignInFailure, ActivitySignInMethod, ActivitySubaccountDetails,
    ActivitySystemReason, ActivityTime, CreateSubaccountResult, GetSubaccountResult, SubAccount,
    SubAccountActivityEvent, SubAccountActivityList, SubAccountInvite, SubAccountInvitesList,
    SubAccountMember, SubAccountMembersList, SubAccountsList,
};
use crate::proto::auth::v1 as pb;
use crate::proto::auth::v1::{
    ActivityEvent as ProtoActivityEvent, CreateSubaccountResponse, GetSubaccountResponse,
    InviteSubaccountMemberResponse, ListAccountActivityResponse, ListSubaccountEventsResponse,
    ListSubaccountInvitesResponse, ListSubaccountMembersResponse, ListSubaccountsResponse,
    RespondSubaccountInviteResponse, Subaccount, SubaccountInvite, SubaccountInviteDirection,
    SubaccountMemberView, SubaccountRole, SubaccountStatus,
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
        subaccount_policy_id: (msg.subaccount_policy_id != 0)
            .then(|| format_uint64_id(msg.subaccount_policy_id)),
        require_member_mfa: msg.require_member_mfa,
        smart_account_salt_nonce: msg.smart_account_salt_nonce,
        icon: msg.icon.clone(),
        color: msg.color.clone(),
        owner_username: msg.owner_username.clone(),
        owner_avatar_url: msg.owner_avatar_url.clone(),
        owner_root_smart_account_address: msg.owner_root_smart_account_address.clone(),
    }
}

/// A read sub-account, refused with `Error::ResponseContract` without its ID.
fn required_subaccount(operation: &str, msg: &Subaccount) -> SdkResult<SubAccount> {
    require_id(operation, "id", msg.id)?;
    Ok(subaccount_from_proto(msg))
}

pub fn subaccounts_list_from_proto(msg: &ListSubaccountsResponse) -> SdkResult<SubAccountsList> {
    Ok(SubAccountsList {
        subaccounts: msg
            .subaccounts
            .iter()
            .map(|subaccount| required_subaccount("ListSubaccounts", subaccount))
            .collect::<SdkResult<_>>()?,
    })
}

/// Decode a `GetSubaccount` answer. `members`, `invites` and `policy` read as
/// the members list, the invites list and the sub-account policy stream read
/// them; `balances` is the balances list when the answer carries it, `None`
/// otherwise.
pub fn get_subaccount_from_proto(msg: &GetSubaccountResponse) -> SdkResult<GetSubaccountResult> {
    const OPERATION: &str = "GetSubaccount";
    Ok(GetSubaccountResult {
        // A sub-account that does not exist is refused by the API with its own
        // error, never answered with an empty success.
        subaccount: Some(required_subaccount(
            OPERATION,
            msg.subaccount
                .as_option()
                .ok_or_else(|| crate::Error::response_contract(OPERATION, "missing subaccount"))?,
        )?),
        api_keys: msg
            .api_keys
            .iter()
            .map(|key| required_api_key(OPERATION, key))
            .collect::<SdkResult<_>>()?,
        members: msg
            .members
            .iter()
            .map(|member| member_from_proto(OPERATION, member))
            .collect::<SdkResult<_>>()?,
        invites: msg
            .invites
            .iter()
            .map(|invite| required_invite(OPERATION, invite))
            .collect::<SdkResult<_>>()?,
        // `None` when no policy is attached: the server leaves it unset.
        policy: msg
            .policy
            .as_option()
            .map(|policy| {
                require_id(OPERATION, "policy.id", policy.id)?;
                subaccount_policy_from_proto(policy)
            })
            .transpose()?,
        balances: msg.balances.as_option().map(balances_list_from_proto),
    })
}

/// Decode a `CreateSubaccount` answer, refused with `Error::ResponseContract`
/// without the new sub-account's ID.
pub fn create_subaccount_from_proto(
    msg: &CreateSubaccountResponse,
) -> SdkResult<CreateSubaccountResult> {
    require_id("CreateSubaccount", "subaccount_id", msg.subaccount_id)?;
    Ok(CreateSubaccountResult {
        subaccount_id: format_uint64_id(msg.subaccount_id),
        revision: msg.revision,
    })
}

/// A sub-account member, refused with `Error::ResponseContract` without its
/// account ID.
fn member_from_proto(operation: &str, msg: &SubaccountMemberView) -> SdkResult<SubAccountMember> {
    require_id(operation, "member account_id", msg.account_id)?;
    Ok(SubAccountMember {
        grantee_account_id: format_uint64_id(msg.account_id),
        role: enum_proto_name(&msg.role),
        mfa_enrolled: msg.mfa_enrolled,
        username: msg.username.clone(),
        smart_account_address: msg.smart_account_address.clone(),
        avatar_url: msg.avatar_url.clone(),
    })
}

pub fn subaccount_members_list_from_proto(
    msg: &ListSubaccountMembersResponse,
) -> SdkResult<SubAccountMembersList> {
    Ok(SubAccountMembersList {
        members: msg
            .members
            .iter()
            .map(|member| member_from_proto("ListSubaccountMembers", member))
            .collect::<SdkResult<_>>()?,
    })
}

fn invite_from_proto(msg: &SubaccountInvite) -> SubAccountInvite {
    SubAccountInvite {
        invite_id: format_uint64_id(msg.id),
        subaccount_id: format_uint64_id(msg.subaccount_id),
        grantee_account_id: format_uint64_id(msg.grantee_account_id),
        inviter_account_id: (msg.inviter_account_id != 0)
            .then(|| format_uint64_id(msg.inviter_account_id)),
        role: enum_proto_name(&msg.role),
        status: enum_proto_name(&msg.status),
        created_at_ms: msg.created_at.as_option().map(|t| timestamp_ms(Some(t))),
        responded_at_ms: msg.responded_at.as_option().map(|t| timestamp_ms(Some(t))),
        grantee_username: msg.grantee_username.clone(),
        inviter_username: msg.inviter_username.clone(),
        subaccount_label: msg.subaccount_label.clone(),
        inviter_root_smart_account_address: msg.inviter_root_smart_account_address.clone(),
        grantee_root_smart_account_address: msg.grantee_root_smart_account_address.clone(),
        require_member_mfa: msg.require_member_mfa,
    }
}

/// A read invite, refused with `Error::ResponseContract` without its ID or
/// its sub-account ID.
fn required_invite(operation: &str, msg: &SubaccountInvite) -> SdkResult<SubAccountInvite> {
    require_id(operation, "id", msg.id)?;
    require_id(operation, "subaccount_id", msg.subaccount_id)?;
    Ok(invite_from_proto(msg))
}

/// Decode an `InviteSubaccountMember` answer, refused with
/// `Error::ResponseContract` without its invite or the invite's IDs.
pub fn invite_subaccount_member_from_proto(
    msg: &InviteSubaccountMemberResponse,
) -> SdkResult<SubAccountInvite> {
    let invite = msg.invite.as_option().ok_or_else(|| {
        crate::Error::response_contract("InviteSubaccountMember", "missing invite")
    })?;
    required_invite("InviteSubaccountMember", invite)
}

/// Decode a `RespondSubaccountInvite` answer, refused with
/// `Error::ResponseContract` without its invite or the invite's IDs.
pub fn respond_subaccount_invite_from_proto(
    msg: &RespondSubaccountInviteResponse,
) -> SdkResult<SubAccountInvite> {
    let invite = msg.invite.as_option().ok_or_else(|| {
        crate::Error::response_contract("RespondSubaccountInvite", "missing invite")
    })?;
    required_invite("RespondSubaccountInvite", invite)
}

pub fn subaccount_invites_list_from_proto(
    msg: &ListSubaccountInvitesResponse,
) -> SdkResult<SubAccountInvitesList> {
    Ok(SubAccountInvitesList {
        invites: msg
            .invites
            .iter()
            .map(|invite| required_invite("ListSubaccountInvites", invite))
            .collect::<SdkResult<_>>()?,
    })
}

pub fn subaccount_activity_list_from_proto(
    msg: &ListSubaccountEventsResponse,
) -> SubAccountActivityList {
    SubAccountActivityList {
        events: msg
            .events
            .iter()
            .map(|e| {
                let (actor, item, details) = activity_actor_item_details(e);
                SubAccountActivityEvent {
                    event_type: format!(
                        "{}:{}",
                        activity_label(&e.entity_kind, "ACTIVITY_ENTITY_"),
                        activity_label(&e.event_action, "ACTIVITY_ACTION_")
                    ),
                    ts_ms: timestamp_ms(e.created_at.as_option()),
                    actor,
                    item,
                    details,
                }
            })
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

/// A nonzero opaque ID as base58; `None` for 0, which the contract uses for
/// an absent reference.
fn optional_id(id: u64) -> Option<String> {
    (id != 0).then(|| format_uint64_id(id))
}

/// A detail time as sent, at full precision; `None` when unset.
fn optional_time(
    ts: &buffa::MessageField<buffa_types::google::protobuf::Timestamp>,
) -> Option<ActivityTime> {
    ts.as_option().map(|t| ActivityTime {
        seconds: t.seconds,
        nanos: t.nanos,
    })
}

fn change<T, U>(from: T, to: T, read: impl Fn(T) -> U) -> ActivityChange<U> {
    ActivityChange {
        from: read(from),
        to: read(to),
    }
}

fn text_change(
    msg: &buffa::MessageField<pb::ActivityTextChange>,
) -> Option<ActivityChange<String>> {
    msg.as_option()
        .map(|c| change(&c.from, &c.to, String::clone))
}

fn bool_change(msg: &buffa::MessageField<pb::ActivityBoolChange>) -> Option<ActivityChange<bool>> {
    msg.as_option().map(|c| change(c.from, c.to, |v| v))
}

fn uint_change(msg: &buffa::MessageField<pb::ActivityUintChange>) -> Option<ActivityChange<u64>> {
    msg.as_option().map(|c| change(c.from, c.to, |v| v))
}

fn time_change(
    msg: &buffa::MessageField<pb::ActivityTimeChange>,
) -> Option<ActivityChange<Option<ActivityTime>>> {
    msg.as_option()
        .map(|c| change(&c.from, &c.to, optional_time))
}

fn id_change(
    msg: &buffa::MessageField<pb::ActivityIdChange>,
) -> Option<ActivityChange<Option<String>>> {
    msg.as_option()
        .map(|c| change(c.from_id, c.to_id, optional_id))
}

/// A change of an enum shared with other APIs, kept as the contract's own
/// type: unset and unknown values read as sent.
fn enum_change<T: buffa::Enumeration + Copy>(
    from: buffa::EnumValue<T>,
    to: buffa::EnumValue<T>,
) -> ActivityChange<buffa::EnumValue<T>> {
    ActivityChange { from, to }
}

fn activity_actor_kind(
    value: &buffa::EnumValue<pb::ActivityActorKind>,
) -> Option<ActivityActorKind> {
    use pb::ActivityActorKind as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_ACTOR_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_ACTOR_USER) => ActivityActorKind::User,
        Some(P::ACTIVITY_ACTOR_API_KEY) => ActivityActorKind::ApiKey,
        Some(P::ACTIVITY_ACTOR_SYSTEM) => ActivityActorKind::System,
        Some(P::ACTIVITY_ACTOR_UNAUTHENTICATED) => ActivityActorKind::Unauthenticated,
        None => ActivityActorKind::Unknown(value.to_i32()),
    })
}

fn activity_api_key_kind(
    value: &buffa::EnumValue<pb::ActivityApiKeyKind>,
) -> Option<ActivityApiKeyKind> {
    use pb::ActivityApiKeyKind as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_KEY_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_KEY_STANDARD) => ActivityApiKeyKind::Standard,
        Some(P::ACTIVITY_KEY_ASSISTANT) => ActivityApiKeyKind::Assistant,
        Some(P::ACTIVITY_KEY_ADMIN) => ActivityApiKeyKind::Admin,
        Some(P::ACTIVITY_KEY_STRATEGY_BOT) => ActivityApiKeyKind::StrategyBot,
        Some(P::ACTIVITY_KEY_BROKER) => ActivityApiKeyKind::Broker,
        None => ActivityApiKeyKind::Unknown(value.to_i32()),
    })
}

fn activity_system_reason(
    value: &buffa::EnumValue<pb::ActivitySystemReason>,
) -> Option<ActivitySystemReason> {
    use pb::ActivitySystemReason as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_SYSTEM_REASON_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_SYSTEM_ASSISTANT_PRESET_UPDATED) => {
            ActivitySystemReason::AssistantPresetUpdated
        }
        Some(P::ACTIVITY_SYSTEM_BROKER_ALLOWLIST_CHANGE_SETTLED) => {
            ActivitySystemReason::BrokerAllowlistChangeSettled
        }
        Some(P::ACTIVITY_SYSTEM_QUICKSWAP_POOL_PROVISIONED) => {
            ActivitySystemReason::QuickswapPoolProvisioned
        }
        Some(P::ACTIVITY_SYSTEM_QUICKSWAP_BROKER_PROVISIONED) => {
            ActivitySystemReason::QuickswapBrokerProvisioned
        }
        Some(P::ACTIVITY_SYSTEM_BROKER_ALLOWLIST_CHANGE_RECONCILING) => {
            ActivitySystemReason::BrokerAllowlistChangeReconciling
        }
        None => ActivitySystemReason::Unknown(value.to_i32()),
    })
}

fn activity_broker_role(
    value: &buffa::EnumValue<pb::ActivityBrokerRole>,
) -> Option<ActivityBrokerRole> {
    use pb::ActivityBrokerRole as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_BROKER_ROLE_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_BROKER_OWNER) => ActivityBrokerRole::Owner,
        Some(P::ACTIVITY_BROKER_ADMIN) => ActivityBrokerRole::Admin,
        Some(P::ACTIVITY_BROKER_FINANCE) => ActivityBrokerRole::Finance,
        Some(P::ACTIVITY_BROKER_AFFILIATE_MANAGER) => ActivityBrokerRole::AffiliateManager,
        Some(P::ACTIVITY_BROKER_VIEWER) => ActivityBrokerRole::Viewer,
        None => ActivityBrokerRole::Unknown(value.to_i32()),
    })
}

fn activity_sign_in_method(
    value: &buffa::EnumValue<pb::ActivitySignInMethod>,
) -> Option<ActivitySignInMethod> {
    use pb::ActivitySignInMethod as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_SIGN_IN_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_SIGN_IN_WALLET) => ActivitySignInMethod::Wallet,
        Some(P::ACTIVITY_SIGN_IN_CONNECTED_APP) => ActivitySignInMethod::ConnectedApp,
        None => ActivitySignInMethod::Unknown(value.to_i32()),
    })
}

fn activity_sign_in_failure(
    value: &buffa::EnumValue<pb::ActivitySignInFailure>,
) -> Option<ActivitySignInFailure> {
    use pb::ActivitySignInFailure as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_SIGN_IN_FAILURE_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_SIGN_IN_FAILURE_INVALID_CHALLENGE) => {
            ActivitySignInFailure::InvalidChallenge
        }
        Some(P::ACTIVITY_SIGN_IN_FAILURE_CHALLENGE_EXPIRED) => {
            ActivitySignInFailure::ChallengeExpired
        }
        Some(P::ACTIVITY_SIGN_IN_FAILURE_CHALLENGE_USED) => ActivitySignInFailure::ChallengeUsed,
        Some(P::ACTIVITY_SIGN_IN_FAILURE_INVALID_SIGNATURE) => {
            ActivitySignInFailure::InvalidSignature
        }
        Some(P::ACTIVITY_SIGN_IN_FAILURE_NO_CHALLENGE) => ActivitySignInFailure::NoChallenge,
        None => ActivitySignInFailure::Unknown(value.to_i32()),
    })
}

fn activity_elevated_action(
    value: &buffa::EnumValue<pb::ActivityElevatedAction>,
) -> Option<ActivityElevatedAction> {
    use pb::ActivityElevatedAction as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_ELEVATED_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_ELEVATED_ASSISTANT_ACCESS) => ActivityElevatedAction::AssistantAccess,
        Some(P::ACTIVITY_ELEVATED_STRATEGY_BOT_ACCESS) => ActivityElevatedAction::StrategyBotAccess,
        Some(P::ACTIVITY_ELEVATED_TELEGRAM_LINK) => ActivityElevatedAction::TelegramLink,
        None => ActivityElevatedAction::Unknown(value.to_i32()),
    })
}

fn activity_allowlist_action(
    value: &buffa::EnumValue<pb::ActivityAllowlistAction>,
) -> Option<ActivityAllowlistAction> {
    use pb::ActivityAllowlistAction as P;
    Some(match value.as_known() {
        Some(P::ACTIVITY_ALLOWLIST_UNSPECIFIED) => return None,
        Some(P::ACTIVITY_ALLOWLIST_ADD) => ActivityAllowlistAction::Add,
        Some(P::ACTIVITY_ALLOWLIST_REMOVE) => ActivityAllowlistAction::Remove,
        None => ActivityAllowlistAction::Unknown(value.to_i32()),
    })
}

fn activity_actor(msg: &pb::ActivityActor) -> ActivityActor {
    ActivityActor {
        kind: activity_actor_kind(&msg.kind),
        account_id: optional_id(msg.account_id),
        username: msg.username.clone(),
        api_key_id: msg.api_key_id.clone(),
        api_key_kind: activity_api_key_kind(&msg.api_key_kind),
        api_key_label: msg.api_key_label.clone(),
        system_reason: activity_system_reason(&msg.system_reason),
    }
}

fn activity_item(msg: &pb::ActivityItem) -> ActivityItem {
    use pb::activity_item::Id;
    ActivityItem {
        id: msg.id.as_ref().map(|id| match id {
            Id::AccountId(v) => ActivityItemId::Account(format_uint64_id(*v)),
            Id::SubaccountId(v) => ActivityItemId::Subaccount(format_uint64_id(*v)),
            Id::InviteId(v) => ActivityItemId::Invite(format_uint64_id(*v)),
            Id::SubaccountPolicyId(v) => ActivityItemId::SubaccountPolicy(format_uint64_id(*v)),
            Id::ApiPolicyId(v) => ActivityItemId::ApiPolicy(format_uint64_id(*v)),
            Id::ApiKeyId(v) => ActivityItemId::ApiKey(v.clone()),
            Id::MfaFactorId(v) => ActivityItemId::MfaFactor(v.clone()),
            Id::AddressBookEntryId(v) => ActivityItemId::AddressBookEntry(format_uint64_id(*v)),
            Id::BrokerAllowlistChangeId(v) => {
                ActivityItemId::BrokerAllowlistChange(format_uint64_id(*v))
            }
            Id::ConnectedAppId(v) => ActivityItemId::ConnectedApp(format_uint64_id(*v)),
        }),
        name: msg.name.clone(),
    }
}

/// What an event changed; `None` when the server sends no details or a kind
/// this SDK does not know.
fn activity_details(msg: &pb::ActivityDetails) -> Option<ActivityDetails> {
    use pb::activity_details::Kind;
    Some(match msg.kind.as_ref()? {
        Kind::Account(d) => ActivityDetails::Account(ActivityAccountDetails {
            username: text_change(&d.username),
            generated_username: d.generated_username,
        }),
        Kind::Session(d) => ActivityDetails::Session(ActivitySessionDetails {
            method: activity_sign_in_method(&d.method),
            wallet_address: d.wallet_address.clone(),
            failure: activity_sign_in_failure(&d.failure),
            scopes: d.scopes.clone(),
            elevated_action: activity_elevated_action(&d.elevated_action),
        }),
        Kind::ApiKey(d) => ActivityDetails::ApiKey(ActivityApiKeyDetails {
            label: text_change(&d.label),
            status: d.status.as_option().map(|c| enum_change(c.from, c.to)),
            purpose: d.purpose.as_option().map(|c| enum_change(c.from, c.to)),
            icon: text_change(&d.icon),
            color: text_change(&d.color),
            ip_allowlist: d.ip_allowlist.as_option().map(|c| ActivityListChange {
                added: c.added.clone(),
                removed: c.removed.clone(),
            }),
            expires_at: time_change(&d.expires_at),
            api_policy_id: id_change(&d.api_policy_id),
            strategy_bot_id: optional_id(d.strategy_bot_id),
        }),
        Kind::Policy(d) => ActivityDetails::Policy(ActivityPolicyDetails {
            name: text_change(&d.name),
            description: text_change(&d.description),
            actions: d.actions.as_option().map(|c| ActivityListChange {
                added: c.added.clone(),
                removed: c.removed.clone(),
            }),
            all_spot_markets: bool_change(&d.all_spot_markets),
            max_order_notional: uint_change(&d.max_order_notional),
            max_open_orders: uint_change(&d.max_open_orders),
            trading_halted: bool_change(&d.trading_halted),
            locked: bool_change(&d.locked),
            review_at: time_change(&d.review_at),
            expires_at: time_change(&d.expires_at),
            assistant_preset: d.assistant_preset,
            source_template_id: optional_id(d.source_template_id),
            source_template_revision: d.source_template_revision,
            subaccount_policy_id: id_change(&d.subaccount_policy_id),
            strategy_bot_id: optional_id(d.strategy_bot_id),
        }),
        Kind::Subaccount(d) => ActivityDetails::Subaccount(ActivitySubaccountDetails {
            label: text_change(&d.label),
            status: d.status.as_option().map(|c| enum_change(c.from, c.to)),
            icon: text_change(&d.icon),
            color: text_change(&d.color),
            require_member_mfa: bool_change(&d.require_member_mfa),
        }),
        Kind::Member(d) => ActivityDetails::Member(ActivityMemberDetails {
            role: d.role.as_option().map(|c| enum_change(c.from, c.to)),
            broker_role: d
                .broker_role
                .as_option()
                .map(|c| change(&c.from, &c.to, activity_broker_role)),
            ownership_transfer: d.ownership_transfer,
        }),
        Kind::Invite(d) => ActivityDetails::Invite(ActivityInviteDetails {
            grantee_account_id: optional_id(d.grantee_account_id),
            role: d.role,
            broker_role: activity_broker_role(&d.broker_role),
            status: d.status.as_option().map(|c| enum_change(c.from, c.to)),
        }),
        Kind::Destination(d) => ActivityDetails::Destination(ActivityDestinationDetails {
            label: text_change(&d.label),
            note_changed: d.note_changed,
            tag_ids: d.tag_ids.as_option().map(|c| ActivityListChange {
                added: c.added_ids.iter().copied().map(format_uint64_id).collect(),
                removed: c
                    .removed_ids
                    .iter()
                    .copied()
                    .map(format_uint64_id)
                    .collect(),
            }),
        }),
        Kind::BrokerAllowlist(d) => {
            ActivityDetails::BrokerAllowlist(ActivityBrokerAllowlistDetails {
                action: activity_allowlist_action(&d.action),
                destination: d.destination.clone(),
                destination_chain_id: d.destination_chain_id,
                label: d.label.clone(),
            })
        }
        Kind::Security(d) => ActivityDetails::Security(ActivitySecurityDetails {
            factor_type: d.factor_type,
            label: text_change(&d.label),
            challenge_purpose: d.challenge_purpose,
            recovery_codes_rotated: d.recovery_codes_rotated,
        }),
    })
}

/// The actor, item and details of an event, as both activity reads decode
/// them.
fn activity_actor_item_details(
    e: &ProtoActivityEvent,
) -> (
    Option<ActivityActor>,
    Option<ActivityItem>,
    Option<ActivityDetails>,
) {
    (
        e.actor.as_option().map(activity_actor),
        e.item.as_option().map(activity_item),
        e.details.as_option().and_then(activity_details),
    )
}

fn account_activity_event_from_proto(e: &ProtoActivityEvent) -> SdkResult<AccountActivityEvent> {
    let created_at = e.created_at.as_option().ok_or_else(|| {
        crate::errors::Error::response_contract("ListAccountActivity", "event has no created_at")
    })?;
    let (actor, item, details) = activity_actor_item_details(e);
    Ok(AccountActivityEvent {
        event_type: format!(
            "{}:{}",
            activity_label(&e.entity_kind, "ACTIVITY_ENTITY_"),
            activity_label(&e.event_action, "ACTIVITY_ACTION_")
        ),
        ts_ms: timestamp_ms(Some(created_at)),
        source: if e.source.to_i32() == 0 {
            String::new()
        } else {
            activity_label(&e.source, "ACTIVITY_SOURCE_")
        },
        subaccount_id: optional_id(e.subaccount_id),
        ip: e.ip.clone(),
        user_agent: e.user_agent.clone(),
        actor,
        item,
        details,
    })
}

/// One page of `ListAccountActivity`, newest first. An event without its
/// creation time is an [`crate::errors::Error::ResponseContract`].
pub fn account_activity_list_from_proto(
    msg: &ListAccountActivityResponse,
) -> SdkResult<AccountActivityList> {
    Ok(AccountActivityList {
        events: msg
            .events
            .iter()
            .map(account_activity_event_from_proto)
            .collect::<SdkResult<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::{ActivityEntityKind, ActivityEvent, ActivityEventAction};
    use buffa_types::google::protobuf::Timestamp;

    /// The policy a sub-account read expands keeps its template lineage, and
    /// one that names only half of it is refused.
    #[test]
    fn expanded_policy_keeps_and_checks_template_lineage() {
        use crate::proto::auth::v1::{Subaccount, SubaccountPolicyView};
        let read = |source_template_id, source_template_revision| {
            get_subaccount_from_proto(&GetSubaccountResponse {
                subaccount: Subaccount {
                    id: 5,
                    ..Default::default()
                }
                .into(),
                policy: SubaccountPolicyView {
                    id: 7,
                    source_template_id,
                    source_template_revision,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            })
        };
        let copy = read(60, 2).unwrap().policy.unwrap();
        let source = copy.source_template.unwrap();
        assert_eq!(source.template_id, format_uint64_id(60));
        assert_eq!(source.template_revision, 2);
        assert!(
            read(0, 0)
                .unwrap()
                .policy
                .unwrap()
                .source_template
                .is_none()
        );
        for (id, revision) in [(60, 0), (0, 2)] {
            assert!(
                matches!(
                    read(id, revision),
                    Err(crate::Error::ResponseContract { .. })
                ),
                "{id}/{revision}"
            );
        }
    }

    /// The account feed names each event's sub-account as a base58 ID, and
    /// reads 0 as absent: the main account's own event. A system event reads
    /// its `system` source.
    #[test]
    fn account_activity_names_subaccount_and_source() {
        use crate::proto::auth::v1::ActivityEventSource;

        let list = account_activity_list_from_proto(&ListAccountActivityResponse {
            events: vec![
                ActivityEvent {
                    created_at: Timestamp {
                        seconds: 1_794_700_000,
                        nanos: 250_000_000,
                        ..Default::default()
                    }
                    .into(),
                    entity_kind: ActivityEntityKind::ACTIVITY_ENTITY_API_KEY.into(),
                    event_action: ActivityEventAction::ACTIVITY_ACTION_CREATED.into(),
                    source: ActivityEventSource::ACTIVITY_SOURCE_API.into(),
                    subaccount_id: 42,
                    ..Default::default()
                },
                ActivityEvent {
                    created_at: Timestamp {
                        seconds: 1_794_700_001,
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
                ActivityEvent {
                    created_at: Timestamp {
                        seconds: 1_794_700_002,
                        ..Default::default()
                    }
                    .into(),
                    source: ActivityEventSource::ACTIVITY_SOURCE_SYSTEM.into(),
                    ..Default::default()
                },
            ],
            next_page_token: "next".into(),
            ..Default::default()
        })
        .unwrap();
        let first = &list.events[0];
        assert_eq!(first.event_type, "api_key:created");
        assert_eq!(first.ts_ms, 1_794_700_000_250);
        assert_eq!(first.source, "api");
        assert_eq!(first.subaccount_id, Some(format_uint64_id(42)));
        let main = &list.events[1];
        assert_eq!((&main.subaccount_id, main.source.as_str()), (&None, ""));
        assert_eq!(list.events[2].source, "system");
        assert_eq!(list.next_page_token, "next");
    }

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
        let result = subaccounts_list_from_proto(&msg).unwrap();
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
        let result = subaccount_members_list_from_proto(&msg).unwrap();
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
        })
        .unwrap();
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

    /// Reads keep every field of `SubaccountInvite`, `SubaccountMemberView`
    /// and `Subaccount`: an invite names its sub-account, inviter and times, a
    /// member whether it enrolled MFA, a sub-account its policy and MFA rule.
    #[test]
    fn reads_keep_every_invite_member_and_subaccount_field() {
        use crate::proto::auth::v1::SubaccountInviteStatus;
        let ts = |seconds| -> buffa::MessageField<Timestamp> {
            Timestamp {
                seconds,
                ..Default::default()
            }
            .into()
        };
        let invites = subaccount_invites_list_from_proto(&ListSubaccountInvitesResponse {
            invites: vec![SubaccountInvite {
                id: 1,
                subaccount_id: 2,
                grantee_account_id: 3,
                inviter_account_id: 4,
                role: SubaccountRole::TRADER.into(),
                status: SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_PENDING.into(),
                created_at: ts(10),
                responded_at: ts(20),
                grantee_username: "bob".into(),
                inviter_username: "alice".into(),
                subaccount_label: "desk".into(),
                inviter_root_smart_account_address: "0xaaaa".into(),
                grantee_root_smart_account_address: "0xbbbb".into(),
                require_member_mfa: true,
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        let invite = &invites.invites[0];
        assert_eq!(invite.subaccount_id, format_uint64_id(2));
        assert_eq!(invite.inviter_account_id, Some(format_uint64_id(4)));
        assert_eq!(invite.created_at_ms, Some(10_000));
        assert_eq!(invite.responded_at_ms, Some(20_000));
        assert_eq!(invite.grantee_username, "bob");
        assert_eq!(invite.inviter_username, "alice");
        assert_eq!(invite.subaccount_label, "desk");
        assert_eq!(invite.inviter_root_smart_account_address, "0xaaaa");
        assert_eq!(invite.grantee_root_smart_account_address, "0xbbbb");
        assert!(invite.require_member_mfa);

        // A pending invite has no answer time, and an unset inviter is absent.
        let pending = subaccount_invites_list_from_proto(&ListSubaccountInvitesResponse {
            invites: vec![SubaccountInvite {
                id: 1,
                subaccount_id: 2,
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(pending.invites[0].inviter_account_id, None);
        assert_eq!(pending.invites[0].created_at_ms, None);
        assert_eq!(pending.invites[0].responded_at_ms, None);

        let members = subaccount_members_list_from_proto(&ListSubaccountMembersResponse {
            members: vec![SubaccountMemberView {
                account_id: 9,
                role: SubaccountRole::OWNER.into(),
                username: "carol".into(),
                smart_account_address: "0xcccc".into(),
                avatar_url: "https://example.com/a.png".into(),
                mfa_enrolled: true,
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        let member = &members.members[0];
        assert!(member.mfa_enrolled);
        assert_eq!(member.username, "carol");
        assert_eq!(member.smart_account_address, "0xcccc");
        assert_eq!(member.avatar_url, "https://example.com/a.png");

        let subaccount = subaccount_from_proto(&Subaccount {
            id: 3,
            icon: "rocket".into(),
            color: "#ff0000".into(),
            owner_username: "alice".into(),
            owner_avatar_url: "https://example.com/o.png".into(),
            owner_root_smart_account_address: "0xdddd".into(),
            subaccount_policy_id: 5,
            require_member_mfa: true,
            smart_account_salt_nonce: Some(0),
            ..Default::default()
        });
        assert_eq!(subaccount.subaccount_policy_id, Some(format_uint64_id(5)));
        assert!(subaccount.require_member_mfa);
        assert_eq!(subaccount.smart_account_salt_nonce, Some(0));
        assert_eq!(subaccount.icon, "rocket");
        assert_eq!(subaccount.color, "#ff0000");
        assert_eq!(subaccount.owner_username, "alice");
        assert_eq!(subaccount.owner_avatar_url, "https://example.com/o.png");
        assert_eq!(subaccount.owner_root_smart_account_address, "0xdddd");

        let bare = subaccount_from_proto(&Subaccount {
            id: 3,
            ..Default::default()
        });
        assert_eq!(bare.subaccount_policy_id, None);
        assert_eq!(bare.smart_account_salt_nonce, None);
    }
}

/// Who acted, on what, and what changed: every field of an activity event's
/// `actor`, `item` and `details` survives decoding, through both activity
/// reads.
#[cfg(test)]
mod activity_tests {
    use super::*;
    use crate::models::{
        ActivityAccountDetails, ActivityActor, ActivityActorKind, ActivityAllowlistAction,
        ActivityApiKeyDetails, ActivityApiKeyKind, ActivityBrokerAllowlistDetails,
        ActivityBrokerRole, ActivityChange, ActivityDestinationDetails, ActivityDetails,
        ActivityElevatedAction, ActivityInviteDetails, ActivityItem, ActivityItemId,
        ActivityListChange, ActivityMemberDetails, ActivityPolicyDetails, ActivitySecurityDetails,
        ActivitySessionDetails, ActivitySignInFailure, ActivitySignInMethod,
        ActivitySubaccountDetails, ActivitySystemReason, ActivityTime,
    };
    use crate::proto::auth::v1 as pb;
    use buffa::EnumValue;
    use buffa_types::google::protobuf::Timestamp;

    fn event(
        actor: Option<pb::ActivityActor>,
        item: Option<pb::ActivityItem>,
        details: Option<pb::activity_details::Kind>,
    ) -> pb::ActivityEvent {
        pb::ActivityEvent {
            created_at: Timestamp {
                seconds: 1_794_700_000,
                ..Default::default()
            }
            .into(),
            entity_kind: pb::ActivityEntityKind::ACTIVITY_ENTITY_API_KEY.into(),
            event_action: pb::ActivityEventAction::ACTIVITY_ACTION_CREATED.into(),
            source: pb::ActivityEventSource::ACTIVITY_SOURCE_API.into(),
            actor: actor.into(),
            item: item.into(),
            details: details
                .map(|kind| pb::ActivityDetails {
                    kind: Some(kind),
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        }
    }

    /// The event as `list_account_activity` reads it, checked against the
    /// sub-account read, which must read the same actor, item and details.
    fn decode(
        e: pb::ActivityEvent,
    ) -> (
        Option<ActivityActor>,
        Option<ActivityItem>,
        Option<ActivityDetails>,
    ) {
        let account = account_activity_list_from_proto(&ListAccountActivityResponse {
            events: vec![e.clone()],
            ..Default::default()
        })
        .unwrap()
        .events
        .remove(0);
        let sub = subaccount_activity_list_from_proto(&ListSubaccountEventsResponse {
            events: vec![e],
            ..Default::default()
        })
        .events
        .remove(0);
        assert_eq!(
            (&sub.actor, &sub.item, &sub.details),
            (&account.actor, &account.item, &account.details),
            "sub-account and account reads differ"
        );
        (account.actor, account.item, account.details)
    }

    fn details(kind: pb::activity_details::Kind) -> ActivityDetails {
        decode(event(None, None, Some(kind))).2.expect("details")
    }

    fn text(from: &str, to: &str) -> buffa::MessageField<pb::ActivityTextChange> {
        pb::ActivityTextChange {
            from: from.into(),
            to: to.into(),
            ..Default::default()
        }
        .into()
    }

    fn text_change(from: &str, to: &str) -> Option<ActivityChange<String>> {
        Some(ActivityChange {
            from: from.into(),
            to: to.into(),
        })
    }

    fn bool_change(from: bool, to: bool) -> buffa::MessageField<pb::ActivityBoolChange> {
        pb::ActivityBoolChange {
            from,
            to,
            ..Default::default()
        }
        .into()
    }

    fn uint_change(from: u64, to: u64) -> buffa::MessageField<pb::ActivityUintChange> {
        pb::ActivityUintChange {
            from,
            to,
            ..Default::default()
        }
        .into()
    }

    fn ts(seconds: i64, nanos: i32) -> buffa::MessageField<Timestamp> {
        Timestamp {
            seconds,
            nanos,
            ..Default::default()
        }
        .into()
    }

    fn stamp(seconds: i64, nanos: i32) -> ActivityTime {
        ActivityTime { seconds, nanos }
    }

    fn id_change(from_id: u64, to_id: u64) -> buffa::MessageField<pb::ActivityIdChange> {
        pb::ActivityIdChange {
            from_id,
            to_id,
            ..Default::default()
        }
        .into()
    }

    fn id(value: u64) -> String {
        format_uint64_id(value)
    }

    /// The ticket's case: an API key created through the API names the key
    /// that acted (`ak_…`), its kind and label, and its account.
    #[test]
    fn api_key_actor_names_the_key() {
        let (actor, _, _) = decode(event(
            Some(pb::ActivityActor {
                kind: pb::ActivityActorKind::ACTIVITY_ACTOR_API_KEY.into(),
                account_id: 9_000_000_003,
                username: "qa".into(),
                api_key_id: "ak_QAPROBE".into(),
                api_key_kind: pb::ActivityApiKeyKind::ACTIVITY_KEY_ASSISTANT.into(),
                api_key_label: "desk bot".into(),
                ..Default::default()
            }),
            None,
            None,
        ));
        assert_eq!(
            actor,
            Some(ActivityActor {
                kind: Some(ActivityActorKind::ApiKey),
                account_id: Some(id(9_000_000_003)),
                username: "qa".into(),
                api_key_id: "ak_QAPROBE".into(),
                api_key_kind: Some(ActivityApiKeyKind::Assistant),
                api_key_label: "desk bot".into(),
                system_reason: None,
            })
        );
    }

    /// A user, the system with its reason, an unauthenticated request and a
    /// kind or key kind newer than this SDK each read as sent; an actor's
    /// unset fields read as none.
    #[test]
    fn every_actor_kind_reads_as_sent() {
        let actor = |a: pb::ActivityActor| decode(event(Some(a), None, None)).0.unwrap();
        assert_eq!(
            actor(pb::ActivityActor {
                kind: pb::ActivityActorKind::ACTIVITY_ACTOR_USER.into(),
                account_id: 77,
                username: "alice".into(),
                ..Default::default()
            }),
            ActivityActor {
                kind: Some(ActivityActorKind::User),
                account_id: Some(id(77)),
                username: "alice".into(),
                ..Default::default()
            }
        );
        assert_eq!(
            actor(pb::ActivityActor {
                kind: pb::ActivityActorKind::ACTIVITY_ACTOR_UNAUTHENTICATED.into(),
                ..Default::default()
            }),
            ActivityActor {
                kind: Some(ActivityActorKind::Unauthenticated),
                ..Default::default()
            }
        );
        for (reason, want) in [
            (
                pb::ActivitySystemReason::ACTIVITY_SYSTEM_ASSISTANT_PRESET_UPDATED,
                ActivitySystemReason::AssistantPresetUpdated,
            ),
            (
                pb::ActivitySystemReason::ACTIVITY_SYSTEM_BROKER_ALLOWLIST_CHANGE_SETTLED,
                ActivitySystemReason::BrokerAllowlistChangeSettled,
            ),
            (
                pb::ActivitySystemReason::ACTIVITY_SYSTEM_QUICKSWAP_POOL_PROVISIONED,
                ActivitySystemReason::QuickswapPoolProvisioned,
            ),
            (
                pb::ActivitySystemReason::ACTIVITY_SYSTEM_QUICKSWAP_BROKER_PROVISIONED,
                ActivitySystemReason::QuickswapBrokerProvisioned,
            ),
            (
                pb::ActivitySystemReason::ACTIVITY_SYSTEM_BROKER_ALLOWLIST_CHANGE_RECONCILING,
                ActivitySystemReason::BrokerAllowlistChangeReconciling,
            ),
        ] {
            assert_eq!(
                actor(pb::ActivityActor {
                    kind: pb::ActivityActorKind::ACTIVITY_ACTOR_SYSTEM.into(),
                    system_reason: reason.into(),
                    ..Default::default()
                }),
                ActivityActor {
                    kind: Some(ActivityActorKind::System),
                    system_reason: Some(want),
                    ..Default::default()
                },
                "{reason:?}"
            );
        }
        for (kind, want) in [
            (
                pb::ActivityApiKeyKind::ACTIVITY_KEY_STANDARD,
                ActivityApiKeyKind::Standard,
            ),
            (
                pb::ActivityApiKeyKind::ACTIVITY_KEY_ADMIN,
                ActivityApiKeyKind::Admin,
            ),
            (
                pb::ActivityApiKeyKind::ACTIVITY_KEY_STRATEGY_BOT,
                ActivityApiKeyKind::StrategyBot,
            ),
            (
                pb::ActivityApiKeyKind::ACTIVITY_KEY_BROKER,
                ActivityApiKeyKind::Broker,
            ),
        ] {
            assert_eq!(
                actor(pb::ActivityActor {
                    api_key_kind: kind.into(),
                    ..Default::default()
                })
                .api_key_kind,
                Some(want)
            );
        }
        assert_eq!(
            actor(pb::ActivityActor {
                kind: EnumValue::from(9),
                api_key_kind: EnumValue::from(19),
                system_reason: EnumValue::from(29),
                ..Default::default()
            }),
            ActivityActor {
                kind: Some(ActivityActorKind::Unknown(9)),
                api_key_kind: Some(ActivityApiKeyKind::Unknown(19)),
                system_reason: Some(ActivitySystemReason::Unknown(29)),
                ..Default::default()
            }
        );
    }

    /// An event without an actor, item or details still decodes, with each
    /// absent.
    #[test]
    fn absent_actor_item_and_details_read_none() {
        assert_eq!(decode(event(None, None, None)), (None, None, None));
        // Details sent without a kind this SDK knows read as none.
        let mut empty = event(None, None, None);
        empty.details = pb::ActivityDetails::default().into();
        assert_eq!(decode(empty).2, None);
    }

    /// Each item ID reads as the variant naming it, opaque IDs as base58;
    /// an item without one keeps its name.
    #[test]
    fn every_item_id_reads_as_sent() {
        use pb::activity_item::Id;
        let item = |id: Option<Id>| {
            decode(event(
                None,
                Some(pb::ActivityItem {
                    id,
                    name: "shown".into(),
                    ..Default::default()
                }),
                None,
            ))
            .1
            .unwrap()
        };
        for (sent, want) in [
            (Id::AccountId(11), ActivityItemId::Account(id(11))),
            (Id::SubaccountId(12), ActivityItemId::Subaccount(id(12))),
            (Id::InviteId(13), ActivityItemId::Invite(id(13))),
            (
                Id::SubaccountPolicyId(14),
                ActivityItemId::SubaccountPolicy(id(14)),
            ),
            (Id::ApiPolicyId(15), ActivityItemId::ApiPolicy(id(15))),
            (
                Id::ApiKeyId("ak_0123".into()),
                ActivityItemId::ApiKey("ak_0123".into()),
            ),
            (
                Id::MfaFactorId("mfa_1".into()),
                ActivityItemId::MfaFactor("mfa_1".into()),
            ),
            (
                Id::AddressBookEntryId(18),
                ActivityItemId::AddressBookEntry(id(18)),
            ),
            (
                Id::BrokerAllowlistChangeId(19),
                ActivityItemId::BrokerAllowlistChange(id(19)),
            ),
            (Id::ConnectedAppId(20), ActivityItemId::ConnectedApp(id(20))),
        ] {
            assert_eq!(
                item(Some(sent)),
                ActivityItem {
                    id: Some(want),
                    name: "shown".into(),
                }
            );
        }
        assert_eq!(
            item(None),
            ActivityItem {
                id: None,
                name: "shown".into(),
            }
        );
    }

    #[test]
    fn account_details_read_username_change() {
        assert_eq!(
            details(pb::activity_details::Kind::Account(Box::new(
                pb::ActivityAccountDetails {
                    username: text("old", "new"),
                    generated_username: true,
                    ..Default::default()
                }
            ))),
            ActivityDetails::Account(ActivityAccountDetails {
                username: text_change("old", "new"),
                generated_username: true,
            })
        );
    }

    #[test]
    fn session_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Session(Box::new(
                pb::ActivitySessionDetails {
                    method: pb::ActivitySignInMethod::ACTIVITY_SIGN_IN_WALLET.into(),
                    wallet_address: "0xabc".into(),
                    failure: pb::ActivitySignInFailure::ACTIVITY_SIGN_IN_FAILURE_CHALLENGE_USED
                        .into(),
                    scopes: vec!["read".into(), "trade".into()],
                    elevated_action: pb::ActivityElevatedAction::ACTIVITY_ELEVATED_TELEGRAM_LINK
                        .into(),
                    ..Default::default()
                }
            ))),
            ActivityDetails::Session(ActivitySessionDetails {
                method: Some(ActivitySignInMethod::Wallet),
                wallet_address: "0xabc".into(),
                failure: Some(ActivitySignInFailure::ChallengeUsed),
                scopes: vec!["read".into(), "trade".into()],
                elevated_action: Some(ActivityElevatedAction::TelegramLink),
            })
        );
        // The other sign-in methods, failures and elevated actions.
        let session = |d: pb::ActivitySessionDetails| match details(
            pb::activity_details::Kind::Session(Box::new(d)),
        ) {
            ActivityDetails::Session(s) => s,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            session(pb::ActivitySessionDetails::default()),
            ActivitySessionDetails::default()
        );
        let read = session(pb::ActivitySessionDetails {
            method: pb::ActivitySignInMethod::ACTIVITY_SIGN_IN_CONNECTED_APP.into(),
            failure: EnumValue::from(99),
            elevated_action: pb::ActivityElevatedAction::ACTIVITY_ELEVATED_ASSISTANT_ACCESS.into(),
            ..Default::default()
        });
        assert_eq!(read.method, Some(ActivitySignInMethod::ConnectedApp));
        assert_eq!(read.failure, Some(ActivitySignInFailure::Unknown(99)));
        assert_eq!(
            read.elevated_action,
            Some(ActivityElevatedAction::AssistantAccess)
        );
        for (failure, want) in [
            (
                pb::ActivitySignInFailure::ACTIVITY_SIGN_IN_FAILURE_INVALID_CHALLENGE,
                ActivitySignInFailure::InvalidChallenge,
            ),
            (
                pb::ActivitySignInFailure::ACTIVITY_SIGN_IN_FAILURE_CHALLENGE_EXPIRED,
                ActivitySignInFailure::ChallengeExpired,
            ),
            (
                pb::ActivitySignInFailure::ACTIVITY_SIGN_IN_FAILURE_INVALID_SIGNATURE,
                ActivitySignInFailure::InvalidSignature,
            ),
            (
                pb::ActivitySignInFailure::ACTIVITY_SIGN_IN_FAILURE_NO_CHALLENGE,
                ActivitySignInFailure::NoChallenge,
            ),
        ] {
            let read = session(pb::ActivitySessionDetails {
                failure: failure.into(),
                elevated_action: pb::ActivityElevatedAction::ACTIVITY_ELEVATED_STRATEGY_BOT_ACCESS
                    .into(),
                ..Default::default()
            });
            assert_eq!(read.failure, Some(want));
            assert_eq!(
                read.elevated_action,
                Some(ActivityElevatedAction::StrategyBotAccess)
            );
        }
    }

    /// Text, enum, list, time and ID changes and a plain ID: every field of
    /// an API key change.
    #[test]
    fn api_key_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::ApiKey(Box::new(
                pb::ActivityApiKeyDetails {
                    label: text("old", "new"),
                    status: pb::ActivityApiKeyStatusChange {
                        from: pb::ApiKeyStatus::ACTIVE.into(),
                        to: pb::ApiKeyStatus::REVOKED.into(),
                        ..Default::default()
                    }
                    .into(),
                    purpose: pb::ActivityApiKeyPurposeChange {
                        from: pb::ApiKeyPurpose::PURPOSE_UNSPECIFIED.into(),
                        to: pb::ApiKeyPurpose::STRATEGY_BOT.into(),
                        ..Default::default()
                    }
                    .into(),
                    icon: text("", "🤖"),
                    color: text("red", ""),
                    ip_allowlist: pb::ActivityListChange {
                        added: vec!["10.0.0.0/8".into()],
                        removed: vec!["192.168.*.*".into(), "1.2.3.4".into()],
                        ..Default::default()
                    }
                    .into(),
                    expires_at: pb::ActivityTimeChange {
                        from: buffa::MessageField::none(),
                        to: ts(1_794_700_000, 250_000_001),
                        ..Default::default()
                    }
                    .into(),
                    api_policy_id: id_change(0, 31),
                    strategy_bot_id: 32,
                    ..Default::default()
                }
            ))),
            ActivityDetails::ApiKey(ActivityApiKeyDetails {
                label: text_change("old", "new"),
                status: Some(ActivityChange {
                    from: pb::ApiKeyStatus::ACTIVE.into(),
                    to: pb::ApiKeyStatus::REVOKED.into(),
                }),
                purpose: Some(ActivityChange {
                    from: pb::ApiKeyPurpose::PURPOSE_UNSPECIFIED.into(),
                    to: pb::ApiKeyPurpose::STRATEGY_BOT.into(),
                }),
                icon: text_change("", "🤖"),
                color: text_change("red", ""),
                ip_allowlist: Some(ActivityListChange {
                    added: vec!["10.0.0.0/8".into()],
                    removed: vec!["192.168.*.*".into(), "1.2.3.4".into()],
                }),
                expires_at: Some(ActivityChange {
                    from: None,
                    to: Some(stamp(1_794_700_000, 250_000_001)),
                }),
                api_policy_id: Some(ActivityChange {
                    from: None,
                    to: Some(id(31)),
                }),
                strategy_bot_id: Some(id(32)),
            })
        );
        assert_eq!(
            details(pb::activity_details::Kind::ApiKey(Box::default())),
            ActivityDetails::ApiKey(ActivityApiKeyDetails::default())
        );
    }

    /// Bool and number changes, added and removed actions, times, a
    /// fromId/toId pair and plain values: every field of a policy change.
    #[test]
    fn policy_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Policy(Box::new(
                pb::ActivityPolicyDetails {
                    name: text("a", "b"),
                    description: text("c", "d"),
                    actions: pb::ActivityActionsChange {
                        added: vec![
                            pb::PolicyAction::TRADE_SPOT.into(),
                            pb::PolicyAction::UNSPECIFIED.into(),
                            EnumValue::from(999),
                        ],
                        removed: vec![pb::PolicyAction::EXTERNAL_WITHDRAW.into()],
                        ..Default::default()
                    }
                    .into(),
                    all_spot_markets: bool_change(false, true),
                    max_order_notional: uint_change(1_000_000, 0),
                    max_open_orders: uint_change(5, 50),
                    trading_halted: bool_change(true, false),
                    locked: bool_change(false, true),
                    review_at: pb::ActivityTimeChange {
                        from: ts(1_794_700_000, 1),
                        to: ts(1_794_800_000, 999_999_999),
                        ..Default::default()
                    }
                    .into(),
                    expires_at: pb::ActivityTimeChange {
                        from: ts(1_794_900_000, 123_456_789),
                        to: buffa::MessageField::none(),
                        ..Default::default()
                    }
                    .into(),
                    assistant_preset: pb::AssistantKeyPreset::TRADE_WITH_APPROVAL.into(),
                    source_template_id: 41,
                    source_template_revision: 3,
                    subaccount_policy_id: id_change(42, 43),
                    strategy_bot_id: 44,
                    ..Default::default()
                }
            ))),
            ActivityDetails::Policy(ActivityPolicyDetails {
                name: text_change("a", "b"),
                description: text_change("c", "d"),
                actions: Some(ActivityListChange {
                    added: vec![
                        pb::PolicyAction::TRADE_SPOT.into(),
                        pb::PolicyAction::UNSPECIFIED.into(),
                        EnumValue::Unknown(999),
                    ],
                    removed: vec![pb::PolicyAction::EXTERNAL_WITHDRAW.into()],
                }),
                all_spot_markets: Some(ActivityChange {
                    from: false,
                    to: true,
                }),
                max_order_notional: Some(ActivityChange {
                    from: 1_000_000,
                    to: 0,
                }),
                max_open_orders: Some(ActivityChange { from: 5, to: 50 }),
                trading_halted: Some(ActivityChange {
                    from: true,
                    to: false,
                }),
                locked: Some(ActivityChange {
                    from: false,
                    to: true,
                }),
                review_at: Some(ActivityChange {
                    from: Some(stamp(1_794_700_000, 1)),
                    to: Some(stamp(1_794_800_000, 999_999_999)),
                }),
                expires_at: Some(ActivityChange {
                    from: Some(stamp(1_794_900_000, 123_456_789)),
                    to: None,
                }),
                assistant_preset: pb::AssistantKeyPreset::TRADE_WITH_APPROVAL.into(),
                source_template_id: Some(id(41)),
                source_template_revision: 3,
                subaccount_policy_id: Some(ActivityChange {
                    from: Some(id(42)),
                    to: Some(id(43)),
                }),
                strategy_bot_id: Some(id(44)),
            })
        );
    }

    #[test]
    fn subaccount_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Subaccount(Box::new(
                pb::ActivitySubaccountDetails {
                    label: text("desk", "desk 2"),
                    status: pb::ActivitySubaccountStatusChange {
                        from: pb::SubaccountStatus::SUBACCOUNT_STATUS_ACTIVE.into(),
                        to: EnumValue::from(9),
                        ..Default::default()
                    }
                    .into(),
                    icon: text("a", "b"),
                    color: text("c", "d"),
                    require_member_mfa: bool_change(false, true),
                    ..Default::default()
                }
            ))),
            ActivityDetails::Subaccount(ActivitySubaccountDetails {
                label: text_change("desk", "desk 2"),
                status: Some(ActivityChange {
                    from: pb::SubaccountStatus::SUBACCOUNT_STATUS_ACTIVE.into(),
                    to: EnumValue::Unknown(9),
                }),
                icon: text_change("a", "b"),
                color: text_change("c", "d"),
                require_member_mfa: Some(ActivityChange {
                    from: false,
                    to: true,
                }),
            })
        );
    }

    #[test]
    fn member_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Member(Box::new(
                pb::ActivityMemberDetails {
                    role: pb::ActivitySubaccountRoleChange {
                        from: pb::SubaccountRole::SUBACCOUNT_ROLE_UNSPECIFIED.into(),
                        to: pb::SubaccountRole::TRADER.into(),
                        ..Default::default()
                    }
                    .into(),
                    broker_role: pb::ActivityBrokerRoleChange {
                        from: pb::ActivityBrokerRole::ACTIVITY_BROKER_ADMIN.into(),
                        to: pb::ActivityBrokerRole::ACTIVITY_BROKER_OWNER.into(),
                        ..Default::default()
                    }
                    .into(),
                    ownership_transfer: true,
                    ..Default::default()
                }
            ))),
            ActivityDetails::Member(ActivityMemberDetails {
                role: Some(ActivityChange {
                    from: pb::SubaccountRole::SUBACCOUNT_ROLE_UNSPECIFIED.into(),
                    to: pb::SubaccountRole::TRADER.into(),
                }),
                broker_role: Some(ActivityChange {
                    from: Some(ActivityBrokerRole::Admin),
                    to: Some(ActivityBrokerRole::Owner),
                }),
                ownership_transfer: true,
            })
        );
        for (role, want) in [
            (
                pb::ActivityBrokerRole::ACTIVITY_BROKER_FINANCE,
                Some(ActivityBrokerRole::Finance),
            ),
            (
                pb::ActivityBrokerRole::ACTIVITY_BROKER_AFFILIATE_MANAGER,
                Some(ActivityBrokerRole::AffiliateManager),
            ),
            (
                pb::ActivityBrokerRole::ACTIVITY_BROKER_VIEWER,
                Some(ActivityBrokerRole::Viewer),
            ),
            (
                pb::ActivityBrokerRole::ACTIVITY_BROKER_ROLE_UNSPECIFIED,
                None,
            ),
        ] {
            let ActivityDetails::Member(member) = details(pb::activity_details::Kind::Member(
                Box::new(pb::ActivityMemberDetails {
                    broker_role: pb::ActivityBrokerRoleChange {
                        from: role.into(),
                        to: EnumValue::from(77),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }),
            )) else {
                panic!("not a member change");
            };
            assert_eq!(
                member.broker_role,
                Some(ActivityChange {
                    from: want,
                    to: Some(ActivityBrokerRole::Unknown(77)),
                })
            );
        }
    }

    #[test]
    fn invite_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Invite(Box::new(
                pb::ActivityInviteDetails {
                    grantee_account_id: 51,
                    role: pb::SubaccountRole::VIEWER.into(),
                    broker_role: pb::ActivityBrokerRole::ACTIVITY_BROKER_FINANCE.into(),
                    status: pb::ActivityInviteStatusChange {
                        from: pb::SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_PENDING.into(),
                        to: pb::SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_ACCEPTED.into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
            ))),
            ActivityDetails::Invite(ActivityInviteDetails {
                grantee_account_id: Some(id(51)),
                role: pb::SubaccountRole::VIEWER.into(),
                broker_role: Some(ActivityBrokerRole::Finance),
                status: Some(ActivityChange {
                    from: pb::SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_PENDING.into(),
                    to: pb::SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_ACCEPTED.into(),
                }),
            })
        );
    }

    /// Added and removed IDs read as base58.
    #[test]
    fn destination_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Destination(Box::new(
                pb::ActivityDestinationDetails {
                    label: text("", "cold wallet"),
                    note_changed: true,
                    tag_ids: pb::ActivityIdListChange {
                        added_ids: vec![61, 62],
                        removed_ids: vec![63],
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
            ))),
            ActivityDetails::Destination(ActivityDestinationDetails {
                label: text_change("", "cold wallet"),
                note_changed: true,
                tag_ids: Some(ActivityListChange {
                    added: vec![id(61), id(62)],
                    removed: vec![id(63)],
                }),
            })
        );
    }

    #[test]
    fn broker_allowlist_details_read_every_field() {
        for (action, want) in [
            (
                pb::ActivityAllowlistAction::ACTIVITY_ALLOWLIST_ADD,
                ActivityAllowlistAction::Add,
            ),
            (
                pb::ActivityAllowlistAction::ACTIVITY_ALLOWLIST_REMOVE,
                ActivityAllowlistAction::Remove,
            ),
        ] {
            assert_eq!(
                details(pb::activity_details::Kind::BrokerAllowlist(Box::new(
                    pb::ActivityBrokerAllowlistDetails {
                        action: action.into(),
                        destination: "0xdef".into(),
                        destination_chain_id: 8453,
                        label: "treasury".into(),
                        ..Default::default()
                    }
                ))),
                ActivityDetails::BrokerAllowlist(ActivityBrokerAllowlistDetails {
                    action: Some(want),
                    destination: "0xdef".into(),
                    destination_chain_id: 8453,
                    label: "treasury".into(),
                })
            );
        }
    }

    /// Times keep the contract's full precision: two expiries within the
    /// same millisecond stay apart, through both reads.
    #[test]
    fn detail_times_keep_full_precision() {
        let expiry = |nanos| {
            let ActivityDetails::ApiKey(key) = details(pb::activity_details::Kind::ApiKey(
                Box::new(pb::ActivityApiKeyDetails {
                    expires_at: pb::ActivityTimeChange {
                        from: ts(1_794_700_000, 250_000_000),
                        to: ts(1_794_700_000, nanos),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }),
            )) else {
                panic!("not an API key change");
            };
            key.expires_at.unwrap()
        };
        let change = expiry(250_000_001);
        assert_eq!(change.from, Some(stamp(1_794_700_000, 250_000_000)));
        assert_eq!(change.to, Some(stamp(1_794_700_000, 250_000_001)));
        assert_ne!(change.from, change.to);
        assert_eq!(
            Timestamp::from(change.to.unwrap()),
            Timestamp {
                seconds: 1_794_700_000,
                nanos: 250_000_001,
                ..Default::default()
            }
        );
        assert_eq!(
            expiry(999_999_999).to,
            Some(stamp(1_794_700_000, 999_999_999))
        );
    }

    /// The activity events and pages keep the `Eq` they had before they
    /// carried actor, item and details.
    #[test]
    fn activity_events_and_pages_stay_eq() {
        fn eq<T: Eq>() {}
        eq::<AccountActivityEvent>();
        eq::<AccountActivityList>();
        eq::<SubAccountActivityEvent>();
        eq::<SubAccountActivityList>();
        eq::<ActivityDetails>();
    }

    /// Unset and unknown values of the shared enums read as sent.
    #[test]
    fn shared_enums_keep_unset_and_unknown_values() {
        assert_eq!(
            details(pb::activity_details::Kind::Security(Box::default())),
            ActivityDetails::Security(ActivitySecurityDetails {
                factor_type: pb::MFAFactorType::MFA_FACTOR_TYPE_UNSPECIFIED.into(),
                challenge_purpose: pb::MFAChallengePurpose::MFA_CHALLENGE_PURPOSE_UNSPECIFIED
                    .into(),
                ..Default::default()
            })
        );
        let ActivityDetails::Security(security) = details(pb::activity_details::Kind::Security(
            Box::new(pb::ActivitySecurityDetails {
                factor_type: EnumValue::from(41),
                challenge_purpose: EnumValue::from(42),
                ..Default::default()
            }),
        )) else {
            panic!("not a security change");
        };
        assert_eq!(
            (security.factor_type, security.challenge_purpose),
            (EnumValue::Unknown(41), EnumValue::Unknown(42))
        );
        let ActivityDetails::Invite(invite) = details(pb::activity_details::Kind::Invite(
            Box::new(pb::ActivityInviteDetails {
                role: EnumValue::from(43),
                ..Default::default()
            }),
        )) else {
            panic!("not an invite");
        };
        assert_eq!(invite.role, EnumValue::Unknown(43));
        assert_eq!(invite.status, None);
        let ActivityDetails::Policy(policy) = details(pb::activity_details::Kind::Policy(
            Box::new(pb::ActivityPolicyDetails {
                assistant_preset: EnumValue::from(44),
                ..Default::default()
            }),
        )) else {
            panic!("not a policy change");
        };
        assert_eq!(policy.assistant_preset, EnumValue::Unknown(44));
    }

    #[test]
    fn security_details_read_every_field() {
        assert_eq!(
            details(pb::activity_details::Kind::Security(Box::new(
                pb::ActivitySecurityDetails {
                    factor_type: pb::MFAFactorType::MFA_FACTOR_TYPE_PASSKEY.into(),
                    label: text("laptop", "yubikey"),
                    challenge_purpose: pb::MFAChallengePurpose::MFA_CHALLENGE_PURPOSE_FRESH_STEP_UP
                        .into(),
                    recovery_codes_rotated: true,
                    ..Default::default()
                }
            ))),
            ActivityDetails::Security(ActivitySecurityDetails {
                factor_type: pb::MFAFactorType::MFA_FACTOR_TYPE_PASSKEY.into(),
                label: text_change("laptop", "yubikey"),
                challenge_purpose: pb::MFAChallengePurpose::MFA_CHALLENGE_PURPOSE_FRESH_STEP_UP
                    .into(),
                recovery_codes_rotated: true,
            })
        );
    }

    /// The other event fields still read as before beside the new ones.
    #[test]
    fn actor_item_and_details_sit_beside_the_event_fields() {
        let list = account_activity_list_from_proto(&ListAccountActivityResponse {
            events: vec![event(
                Some(pb::ActivityActor {
                    kind: pb::ActivityActorKind::ACTIVITY_ACTOR_API_KEY.into(),
                    api_key_id: "ak_QAPROBE".into(),
                    username: "qa".into(),
                    ..Default::default()
                }),
                Some(pb::ActivityItem {
                    id: Some(pb::activity_item::Id::ApiKeyId("ak_NEW".into())),
                    name: "new key".into(),
                    ..Default::default()
                }),
                Some(pb::activity_details::Kind::ApiKey(Box::new(
                    pb::ActivityApiKeyDetails {
                        label: text("", "new key"),
                        ..Default::default()
                    },
                ))),
            )],
            ..Default::default()
        })
        .unwrap();
        let e = &list.events[0];
        assert_eq!(
            (e.event_type.as_str(), e.ts_ms, e.source.as_str()),
            ("api_key:created", 1_794_700_000_000, "api")
        );
        let actor = e.actor.as_ref().unwrap();
        assert_eq!(actor.kind, Some(ActivityActorKind::ApiKey));
        assert_eq!(actor.api_key_id, "ak_QAPROBE");
        assert_eq!(
            e.item,
            Some(ActivityItem {
                id: Some(ActivityItemId::ApiKey("ak_NEW".into())),
                name: "new key".into(),
            })
        );
        assert_eq!(
            e.details,
            Some(ActivityDetails::ApiKey(ActivityApiKeyDetails {
                label: text_change("", "new key"),
                ..Default::default()
            }))
        );
    }
}
