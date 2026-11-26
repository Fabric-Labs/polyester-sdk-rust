//! Sub-account models (Go `models/sub_accounts.go` thin parity).

use crate::proto::auth::v1 as pb;
use buffa::EnumValue;
use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccount {
    pub subaccount_id: String,
    pub label: String,
    pub smart_account_address: String,
    pub status: String,
    pub updated_at: Option<Timestamp>,
    /// Monotonic resource revision for conditional updates.
    pub revision: u64,
    /// The caller's role on this sub-account as its proto name (`"VIEWER"`,
    /// `"TRADER"`, `"LEVERAGED_TRADER"`, `"TREASURY"`, `"ADMIN"`, `"OWNER"`),
    /// as member and invite roles read; `"UNKNOWN(n)"` for a role this
    /// version does not know. `None` when the server sends none, as on a
    /// removal: no role is assumed.
    pub role: Option<String>,
    /// The caller no longer has access to this sub-account, for example after
    /// its owner removed the caller as a member: drop it from the caller's
    /// list. Only real-time updates (`sub_accounts.subscribe`) set it;
    /// reads list only accessible sub-accounts. A role or access change does
    /// not change `revision` or `updated_at`, so apply each update as it
    /// arrives.
    pub removed: bool,
    /// The sub-account policy attached to it, as public-ID text; `None` when
    /// none is attached.
    pub subaccount_policy_id: Option<String>,
    /// Members must enroll MFA to act on this sub-account.
    pub require_member_mfa: bool,
    /// The salt nonce its smart account was derived with; `None` when the
    /// server sends none.
    pub smart_account_salt_nonce: Option<u32>,
    pub icon: String,
    pub color: String,
    pub owner_username: String,
    pub owner_avatar_url: String,
    pub owner_root_smart_account_address: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAccountsList {
    pub subaccounts: Vec<SubAccount>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GetSubaccountResult {
    pub subaccount: Option<SubAccount>,
    /// The API keys scoped to the sub-account, read as
    /// [`crate::services::ApiKeysService::list`] reads them. Empty unless the
    /// read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_api_keys`] and the
    /// server sent some: it sends them to an owner or admin of the
    /// sub-account only.
    pub api_keys: Vec<crate::models::ApiKeySummary>,
    /// The sub-account's current members, read as
    /// [`crate::services::SubAccountsService::list_members`] reads them.
    /// Empty unless the read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_members`] and the
    /// server sent some: it sends them to a current member, and leaves them
    /// out for an API key without `READ_SUBACCOUNT_MEMBERS`.
    pub members: Vec<SubAccountMember>,
    /// The invitations visible to the caller, read as
    /// [`crate::services::SubAccountsService::list_invites`] reads them and
    /// filtered by [`crate::services::GetSubaccountOpts::invites_direction`].
    /// Empty unless the read asked for them with
    /// [`crate::services::GetSubaccountOpts::include_invites`] and the
    /// server sent some: it leaves them out for an API key without
    /// `READ_SUBACCOUNT_INVITES`.
    pub invites: Vec<SubAccountInvite>,
    /// The sub-account policy attached to the sub-account, read as
    /// [`crate::services::PoliciesService::subscribe_subaccount_policies`]
    /// reads it. `Some` only when the read asked for it with
    /// [`crate::services::GetSubaccountOpts::include_policy`], the caller is
    /// a current member and a policy is attached.
    pub policy: Option<crate::models::SubaccountPolicy>,
    /// The sub-account's current balances, one row per asset as the
    /// balances list returns them. `Some` only when the read asked for them
    /// with [`crate::services::GetSubaccountOpts::include_balances`].
    pub balances: Option<crate::models::BalancesList>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountMember {
    pub grantee_account_id: String,
    pub role: String,
    /// The member has enrolled MFA.
    pub mfa_enrolled: bool,
    pub username: String,
    pub smart_account_address: String,
    pub avatar_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountMembersList {
    pub members: Vec<SubAccountMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountInvite {
    pub invite_id: String,
    /// The sub-account the invite is for, as public-ID text.
    pub subaccount_id: String,
    pub grantee_account_id: String,
    /// The account that sent the invite, as public-ID text; `None` when the
    /// server sends none.
    pub inviter_account_id: Option<String>,
    pub role: String,
    pub status: String,
    /// When the invite was sent, in Unix milliseconds; `None` when the server
    /// sends no time.
    pub created_at_ms: Option<i64>,
    /// When the grantee accepted or declined, in Unix milliseconds; `None`
    /// while pending.
    pub responded_at_ms: Option<i64>,
    pub grantee_username: String,
    pub inviter_username: String,
    pub subaccount_label: String,
    pub inviter_root_smart_account_address: String,
    pub grantee_root_smart_account_address: String,
    /// The sub-account requires members to enroll MFA.
    pub require_member_mfa: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountInvitesList {
    pub invites: Vec<SubAccountInvite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountActivityEvent {
    pub event_type: String,
    pub ts_ms: i64,
    /// Who performed the event; `None` only when the server sends none.
    pub actor: Option<ActivityActor>,
    /// The item the event affected; `None` for an event recorded before
    /// items were.
    pub item: Option<ActivityItem>,
    /// What the event changed; `None` when it records no change, such as a
    /// deletion, or was recorded before details were.
    pub details: Option<ActivityDetails>,
}

/// Kind of [`ActivityActor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityActorKind {
    /// A signed-in user.
    User,
    /// A request signed with an API key.
    ApiKey,
    /// Polyester acting by itself; see [`ActivityActor::system_reason`].
    System,
    /// A request nobody authenticated, such as a failed sign-in.
    Unauthenticated,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// Kind of the API key an [`ActivityActor`] signed with, when the event was
/// recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityApiKeyKind {
    Standard,
    /// An AI assistant key.
    Assistant,
    Admin,
    /// A strategy bot key.
    StrategyBot,
    /// A QuickSwap broker key.
    Broker,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// Why Polyester acted, for the system actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivitySystemReason {
    /// An AI assistant key preset changed, and the policies following it
    /// were updated.
    AssistantPresetUpdated,
    /// A QuickSwap broker allowlist change settled.
    BrokerAllowlistChangeSettled,
    /// A QuickSwap pool was provisioned.
    QuickswapPoolProvisioned,
    /// A QuickSwap broker was provisioned.
    QuickswapBrokerProvisioned,
    /// A QuickSwap broker allowlist change is being reconciled; a later event
    /// records how it ended.
    BrokerAllowlistChangeReconciling,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// Who performed an activity event: a user, an API key, Polyester itself or
/// an unauthenticated request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityActor {
    /// `None` when the server sends no kind.
    pub kind: Option<ActivityActorKind>,
    /// The account that acted, as a base58 ID, for a user or an API key: a
    /// sub-account member is named by their own account, and a QuickSwap
    /// broker's key by the broker's account. `None` for the system and an
    /// unauthenticated request.
    pub account_id: Option<String>,
    /// That account's username, when it has one.
    pub username: String,
    /// The API key the actor signed with, as
    /// [`crate::models::ApiKeySummary::key_id`] names it (`ak_…`), or a
    /// QuickSwap broker key's ID. Set for an API key actor only.
    pub api_key_id: String,
    /// That key's kind when the event was recorded; `None` for an actor
    /// other than an API key.
    pub api_key_kind: Option<ActivityApiKeyKind>,
    /// That key's label, when it has one.
    pub api_key_label: String,
    /// Why Polyester acted; set for the system actor only.
    pub system_reason: Option<ActivitySystemReason>,
}

/// ID of the item an activity event affected: which variant is set says
/// what the item is. Opaque IDs are base58, as each item's own APIs return
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityItemId {
    /// A main account, or the member account of a sub-account membership.
    Account(String),
    Subaccount(String),
    /// A sub-account invitation.
    Invite(String),
    SubaccountPolicy(String),
    /// An API key policy.
    ApiPolicy(String),
    /// An API key, as [`crate::models::ApiKeySummary::key_id`] names it.
    ApiKey(String),
    /// An MFA factor, by its factor ID.
    MfaFactor(String),
    /// A saved address-book destination.
    AddressBookEntry(String),
    /// A QuickSwap broker allowlist change request.
    BrokerAllowlistChange(String),
    ConnectedApp(String),
}

/// The item an activity event affected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityItem {
    /// `None` for an item with no ID: a sign-in session, a failed sign-in or
    /// the recovery codes.
    pub id: Option<ActivityItemId>,
    /// The item's display name when the event was recorded: a policy name, an
    /// API key or address-book label, a sub-account label, a member's
    /// username, an MFA factor label, a connected app's name or a session's
    /// device.
    pub name: String,
}

/// A value before and after an activity event. What an unset value reads as
/// is documented on each field that holds one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityChange<T> {
    pub from: T,
    pub to: T,
}

/// Entries an activity event added to and removed from a list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityListChange<T> {
    pub added: Vec<T>,
    pub removed: Vec<T>,
}

/// A QuickSwap broker role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityBrokerRole {
    Owner,
    Admin,
    Finance,
    AffiliateManager,
    Viewer,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// How a sign-in was attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivitySignInMethod {
    /// A wallet signature.
    Wallet,
    /// A connected app the user approved.
    ConnectedApp,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// Why a sign-in failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivitySignInFailure {
    /// The signed message is not a sign-in challenge.
    InvalidChallenge,
    /// The challenge expired before it was signed.
    ChallengeExpired,
    /// The challenge was already used.
    ChallengeUsed,
    /// The signature does not match the wallet.
    InvalidSignature,
    /// No challenge was issued for the wallet.
    NoChallenge,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// An action that needs a recent MFA check in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityElevatedAction {
    /// Setting up the AI assistant's API keys.
    AssistantAccess,
    /// Giving a strategy bot its trade key.
    StrategyBotAccess,
    /// Linking a Telegram account.
    TelegramLink,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// Whether a QuickSwap broker allowlist change adds or removes a
/// destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ActivityAllowlistAction {
    Add,
    Remove,
    /// A value this SDK does not know, kept as its number.
    Unknown(i32),
}

/// What an activity event changed, by the kind of item it affected.
///
/// Each `Option` field is `None` when the event did not set it. A text
/// change reads an unset side as `""`, an ID change and a time change as
/// `None`. Times keep the contract's full precision. Enums shared with
/// other APIs keep the contract's own types as sent: an unset value reads as
/// its `..._UNSPECIFIED` variant, and a value newer than this SDK as
/// [`EnumValue::Unknown`] with its number.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ActivityDetails {
    /// An account change.
    Account(ActivityAccountDetails),
    /// A sign-in, a failed sign-in, a connected app or an MFA-elevated
    /// action.
    Session(ActivitySessionDetails),
    /// An API key change.
    ApiKey(ActivityApiKeyDetails),
    /// A sub-account or API key policy change.
    Policy(ActivityPolicyDetails),
    /// A sub-account change.
    Subaccount(ActivitySubaccountDetails),
    /// A sub-account or QuickSwap broker membership change.
    Member(ActivityMemberDetails),
    /// A sub-account or QuickSwap broker invitation.
    Invite(ActivityInviteDetails),
    /// An address-book change.
    Destination(ActivityDestinationDetails),
    /// A QuickSwap broker allowlist change; the change request is the
    /// event's item.
    BrokerAllowlist(ActivityBrokerAllowlistDetails),
    /// An MFA or recovery-code change.
    Security(ActivitySecurityDetails),
}

/// What an account event changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityAccountDetails {
    pub username: Option<ActivityChange<String>>,
    /// Polyester generated the new username rather than the user choosing
    /// it.
    pub generated_username: bool,
}

/// What a session event recorded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivitySessionDetails {
    /// How the user signed in, for a sign-in or a failed one.
    pub method: Option<ActivitySignInMethod>,
    /// Wallet address that signed in, lowercase.
    pub wallet_address: String,
    /// Why the sign-in failed, for a failed one.
    pub failure: Option<ActivitySignInFailure>,
    /// Scopes the user granted a connected app.
    pub scopes: Vec<String>,
    /// Action the session's recent MFA check was used for.
    pub elevated_action: Option<ActivityElevatedAction>,
}

/// What an API key event changed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActivityApiKeyDetails {
    pub label: Option<ActivityChange<String>>,
    pub status: Option<ActivityChange<EnumValue<pb::ApiKeyStatus>>>,
    pub purpose: Option<ActivityChange<EnumValue<pb::ApiKeyPurpose>>>,
    pub icon: Option<ActivityChange<String>>,
    pub color: Option<ActivityChange<String>>,
    /// IP addresses or ranges added to and removed from the key's allowlist,
    /// masked like the event's IP for callers who may not see IP addresses.
    pub ip_allowlist: Option<ActivityListChange<String>>,
    /// The key's expiry.
    pub expires_at: Option<ActivityChange<Option<Timestamp>>>,
    /// The API key policy bound to the key, as base58 IDs.
    pub api_policy_id: Option<ActivityChange<Option<String>>>,
    /// The strategy bot the key belongs to, as a base58 ID, for a strategy
    /// bot key.
    pub strategy_bot_id: Option<String>,
}

// Every field compares reflexively (a `Timestamp` is two integers), so the
// equality is total and the activity events keep `Eq`.
impl Eq for ActivityApiKeyDetails {}

/// What a policy event changed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActivityPolicyDetails {
    pub name: Option<ActivityChange<String>>,
    pub description: Option<ActivityChange<String>>,
    /// Actions the policy allowed and stopped allowing.
    pub actions: Option<ActivityListChange<EnumValue<pb::PolicyAction>>>,
    /// Whether the policy allows every spot market.
    pub all_spot_markets: Option<ActivityChange<bool>>,
    /// Maximum notional of one order, in canonical quote microunits (one unit
    /// is 0.000001 USDT); 0 is no cap.
    pub max_order_notional: Option<ActivityChange<u64>>,
    /// Maximum number of resting orders; 0 is no cap.
    pub max_open_orders: Option<ActivityChange<u64>>,
    pub trading_halted: Option<ActivityChange<bool>>,
    pub locked: Option<ActivityChange<bool>>,
    /// The policy's review time.
    pub review_at: Option<ActivityChange<Option<Timestamp>>>,
    /// The policy's expiry.
    pub expires_at: Option<ActivityChange<Option<Timestamp>>>,
    /// The AI assistant key preset the policy follows.
    pub assistant_preset: EnumValue<pb::AssistantKeyPreset>,
    /// The template the policy was copied from, as a base58 ID.
    pub source_template_id: Option<String>,
    /// That template's revision when it was copied; 0 when none.
    pub source_template_revision: u64,
    /// The sub-account policy bound to the event's sub-account, as base58
    /// IDs.
    pub subaccount_policy_id: Option<ActivityChange<Option<String>>>,
    /// The strategy bot the policy was made for, as a base58 ID.
    pub strategy_bot_id: Option<String>,
}

// Every field compares reflexively (a `Timestamp` is two integers), so the
// equality is total and the activity events keep `Eq`.
impl Eq for ActivityPolicyDetails {}

/// What a sub-account event changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivitySubaccountDetails {
    pub label: Option<ActivityChange<String>>,
    pub status: Option<ActivityChange<EnumValue<pb::SubaccountStatus>>>,
    pub icon: Option<ActivityChange<String>>,
    pub color: Option<ActivityChange<String>>,
    /// Whether the sub-account's members must use MFA.
    pub require_member_mfa: Option<ActivityChange<bool>>,
}

/// What a membership event changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityMemberDetails {
    /// The member's sub-account role; `SUBACCOUNT_ROLE_UNSPECIFIED` on the
    /// side where the member had none.
    pub role: Option<ActivityChange<EnumValue<pb::SubaccountRole>>>,
    /// The member's QuickSwap broker role; `None` on the side where the
    /// member had none.
    pub broker_role: Option<ActivityChange<Option<ActivityBrokerRole>>>,
    /// The event moved a QuickSwap broker's ownership to the member.
    pub ownership_transfer: bool,
}

/// What an invitation event recorded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityInviteDetails {
    /// The account invited, as a base58 ID.
    pub grantee_account_id: Option<String>,
    /// The sub-account role offered.
    pub role: EnumValue<pb::SubaccountRole>,
    /// The QuickSwap broker role offered.
    pub broker_role: Option<ActivityBrokerRole>,
    pub status: Option<ActivityChange<EnumValue<pb::SubaccountInviteStatus>>>,
}

/// What an address-book event changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityDestinationDetails {
    pub label: Option<ActivityChange<String>>,
    /// The entry's note changed. The note itself is not shown.
    pub note_changed: bool,
    /// Tags added to and removed from the entry, as base58 IDs.
    pub tag_ids: Option<ActivityListChange<String>>,
}

/// What a QuickSwap broker allowlist event recorded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityBrokerAllowlistDetails {
    pub action: Option<ActivityAllowlistAction>,
    /// Destination address.
    pub destination: String,
    pub destination_chain_id: u64,
    pub label: String,
}

/// What an account security event changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivitySecurityDetails {
    /// Type of the MFA factor added, changed, removed or used.
    pub factor_type: EnumValue<pb::MFAFactorType>,
    pub label: Option<ActivityChange<String>>,
    /// What a passed MFA check was for.
    pub challenge_purpose: EnumValue<pb::MFAChallengePurpose>,
    /// The recovery codes were replaced by new ones.
    pub recovery_codes_rotated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubAccountActivityList {
    pub events: Vec<SubAccountActivityEvent>,
    pub next_page_token: String,
}

/// One event of the whole account's activity feed, from
/// [`crate::services::SubAccountsService::list_account_activity`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AccountActivityEvent {
    /// `<entity>:<action>` in lowercase, as [`SubAccountActivityEvent`]
    /// names it, such as `"api_key:created"`.
    pub event_type: String,
    /// When the event occurred, in Unix milliseconds (UTC).
    pub ts_ms: i64,
    /// Client channel the event came from, such as `"web"`, `"mobile"` or
    /// `"api"`, or `"system"` for an event Polyester recorded by itself;
    /// empty when unspecified.
    pub source: String,
    /// Sub-account the event belongs to, as a base58 ID; `None` for an event
    /// of the main account itself.
    pub subaccount_id: Option<String>,
    /// Origin IP address, masked for callers other than the owner and admins.
    pub ip: String,
    /// User agent of the event, when available.
    pub user_agent: String,
    /// Who performed the event; `None` only when the server sends none.
    pub actor: Option<ActivityActor>,
    /// The item the event affected; `None` for an event recorded before
    /// items were.
    pub item: Option<ActivityItem>,
    /// What the event changed; `None` when it records no change, such as a
    /// deletion, or was recorded before details were.
    pub details: Option<ActivityDetails>,
}

/// One page of the whole account's activity feed, newest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AccountActivityList {
    pub events: Vec<AccountActivityEvent>,
    /// Cursor for the next page; empty when there is none.
    pub next_page_token: String,
}
