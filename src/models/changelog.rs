//! Public product changelog models (Go `models/changelog.go` parity).
//!
//! Enum values are lowercase labels. A value newer than this SDK reads as
//! `unknown(<n>)` instead of failing the call.

use buffa_types::google::protobuf::Timestamp;

/// Change category labels.
pub const CHANGELOG_CATEGORY_BREAKING_CHANGE: &str = "breaking_change";
pub const CHANGELOG_CATEGORY_NEW_FEATURE: &str = "new_feature";
pub const CHANGELOG_CATEGORY_IMPROVEMENT: &str = "improvement";
pub const CHANGELOG_CATEGORY_BUG_FIX: &str = "bug_fix";

/// Audience labels.
pub const CHANGELOG_AUDIENCE_ALL_USERS: &str = "all_users";
pub const CHANGELOG_AUDIENCE_TRADERS: &str = "traders";
pub const CHANGELOG_AUDIENCE_MARKET_MAKERS: &str = "market_makers";
pub const CHANGELOG_AUDIENCE_DEVELOPERS: &str = "developers";

/// Surface labels.
pub const CHANGELOG_SURFACE_TRADING: &str = "trading";
pub const CHANGELOG_SURFACE_FUNDING: &str = "funding";
pub const CHANGELOG_SURFACE_ACCOUNTS: &str = "accounts";
pub const CHANGELOG_SURFACE_API: &str = "api";
pub const CHANGELOG_SURFACE_EXPLORER: &str = "explorer";
pub const CHANGELOG_SURFACE_DOCUMENTATION: &str = "documentation";
pub const CHANGELOG_SURFACE_PLATFORM: &str = "platform";
/// Prices, candles, order book, heatmaps and market overviews.
pub const CHANGELOG_SURFACE_MARKET_DATA: &str = "market_data";
/// The QuickSwap swap app.
pub const CHANGELOG_SURFACE_QUICKSWAP: &str = "quickswap";
/// Daily claims, campaigns, VIP tiers and fee discounts.
pub const CHANGELOG_SURFACE_REWARDS: &str = "rewards";
/// The AI assistant: chats, approvals, memories and preferences.
pub const CHANGELOG_SURFACE_ASSISTANT: &str = "assistant";

/// Environment labels.
pub const CHANGELOG_ENVIRONMENT_DEVNET: &str = "devnet";
pub const CHANGELOG_ENVIRONMENT_TESTNET: &str = "testnet";
pub const CHANGELOG_ENVIRONMENT_MAINNET: &str = "mainnet";

/// Change availability labels: how one change of a release differs from its
/// release in one environment.
///
/// Available in the environment while its release is still pending there.
pub const CHANGELOG_AVAILABILITY_AHEAD: &str = "ahead";
/// Not yet available in the environment although its release is live there.
pub const CHANGELOG_AVAILABILITY_PENDING: &str = "pending";
/// Not available in the environment for now, whatever its release's status.
pub const CHANGELOG_AVAILABILITY_UNAVAILABLE: &str = "unavailable";

/// Whether a release is live in one environment.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogEnvironmentStatus {
    /// A `CHANGELOG_ENVIRONMENT_*` label.
    pub environment: String,
    /// When the release went live in the environment; `None` while it is
    /// pending there.
    pub live_at: Option<Timestamp>,
}

/// The status of one change in one environment where it differs from its
/// release's status.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogChangeException {
    /// A `CHANGELOG_ENVIRONMENT_*` label.
    pub environment: String,
    /// A `CHANGELOG_AVAILABILITY_*` label.
    pub availability: String,
    /// When the change became available in the environment; set for
    /// [`CHANGELOG_AVAILABILITY_AHEAD`] only, `None` otherwise.
    pub live_at: Option<Timestamp>,
}

// Every field compares reflexively (a `Timestamp` is two integers), so the
// equality is total and `ChangelogChange` keeps `Eq`.
impl Eq for ChangelogChangeException {}

/// One product change in a changelog post.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangelogChange {
    /// Public identifier in base58.
    pub id: String,
    pub title: String,
    pub summary: String,
    pub details: Option<String>,
    /// A `CHANGELOG_CATEGORY_*` label.
    pub category: String,
    pub action_required: bool,
    pub migration_guidance: Option<String>,
    /// `CHANGELOG_AUDIENCE_*` labels.
    pub audiences: Vec<String>,
    /// `CHANGELOG_SURFACE_*` labels.
    pub surfaces: Vec<String>,
    /// The environments, any of them and not only the one the client points
    /// at, where this change differs from its release's status, for example
    /// a change already available on testnet before its release reaches
    /// testnet. Empty outside release posts and when the change follows its
    /// release everywhere.
    pub environment_exceptions: Vec<ChangelogChangeException>,
}

/// A post's changes grouped under one heading.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangelogSection {
    pub heading: String,
    pub summary: Option<String>,
    pub changes: Vec<ChangelogChange>,
    /// The product area the section covers, a `CHANGELOG_SURFACE_*` label
    /// (`unknown(<n>)` for an area newer than this SDK). Empty for a section
    /// not tied to one area, such as the changes that need your attention.
    /// It does not change which changes `list_changes` matches: a change
    /// keeps its own `surfaces`.
    pub surface: String,
}

/// A changelog post without its sections, with change counts by kind.
///
/// A published post can be republished, for example to correct its copy or
/// withdraw a change. Its `slug`, `published_at` and its changes' `id`
/// values stay the same; `revision` and `updated_at` tell the versions
/// apart. To follow edits, read a post again when its `revision` differs
/// from the one you hold.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogPostSummary {
    /// The post's coverage cutoff date, `YYYY-MM-DD`, with a `-2` or `-3`
    /// suffix for a later post on the same day. It never changes when the
    /// post is republished.
    pub slug: String,
    pub title: String,
    pub introduction: String,
    /// Time of first publication. A republication does not move it.
    pub published_at: Option<Timestamp>,
    /// When the current revision was published; equal to `published_at` for
    /// revision 1. `None` from a server that predates revisions.
    pub updated_at: Option<Timestamp>,
    /// Published revision, starting at 1 and increasing with each
    /// republication. 0 from a server that predates revisions.
    pub revision: u32,
    pub coverage_cutoff: Option<Timestamp>,
    pub change_count: u32,
    pub breaking_change_count: u32,
    pub action_required_count: u32,
    pub new_feature_count: u32,
    pub improvement_count: u32,
    pub bug_fix_count: u32,
    /// The release the post announces, for example `0.92.9`; `None` for a
    /// post that does not announce one release. A release post has the same
    /// slug (`v0.92.9`) and content in every environment, and is listed even
    /// where its release is still pending.
    pub release_version: Option<String>,
    /// The status of the post's release in each environment, devnet, testnet
    /// and mainnet in that order; empty for a post without
    /// `release_version`. It changes when the release goes live somewhere,
    /// without a new `revision`: compare the whole summary to notice it.
    pub environments: Vec<ChangelogEnvironmentStatus>,
    /// The slug of the deployment group that brought the release to this
    /// environment, for `ChangelogService::get_deployment_group`; `None`
    /// while the release is pending here and for a post without
    /// `release_version`.
    pub deployment_group_slug: Option<String>,
}

/// One full changelog post at its current revision.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogPost {
    pub summary: ChangelogPostSummary,
    pub sections: Vec<ChangelogSection>,
}

/// One change with the post it was published in.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogChangeEntry {
    pub change: ChangelogChange,
    pub post_slug: String,
    pub published_at: Option<Timestamp>,
    pub section_heading: String,
}

/// One `list_posts` page, newest post first. An empty `next_page_token` means
/// the last page.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogPostsPage {
    pub posts: Vec<ChangelogPostSummary>,
    pub next_page_token: String,
}

/// One `list_changes` page. An empty `next_page_token` means the last page.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogChangesPage {
    pub changes: Vec<ChangelogChangeEntry>,
    pub next_page_token: String,
}

/// One published release post a deployment group brought.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogDeploymentGroupPost {
    /// The post's slug, for example `v0.92.9`, for `ChangelogService::get_post`.
    pub slug: String,
    /// The release the post announces, for example `0.92.9`.
    pub release_version: String,
    pub title: String,
    /// Time of first publication of the post.
    pub published_at: Option<Timestamp>,
    /// The post's published revision, starting at 1.
    pub revision: u32,
    /// When the post's current revision was published.
    pub updated_at: Option<Timestamp>,
}

/// One deployment group without its details: one deployment to the
/// environment that raised its release version, bringing every release
/// above the previous group's version up to and including its own.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogDeploymentGroupSummary {
    /// Stable, shareable identifier: the environment and the group's own
    /// release, for example `testnet-v0.92.9`.
    pub slug: String,
    /// The `CHANGELOG_ENVIRONMENT_*` label the group was deployed to.
    pub environment: String,
    /// When the deployment ran.
    pub occurred_at: Option<Timestamp>,
    /// The group's own, highest release, for example `0.92.9`.
    pub release_version: String,
    /// The tag of the group's own release, for example `v0.92.9`; `None`
    /// when unknown.
    pub release_tag: Option<String>,
    /// The release of the previous deployment group; `None` for the first
    /// group.
    pub previous_release_version: Option<String>,
    /// Every release the group brought, ascending, for example
    /// `["0.92.8", "0.92.9"]`.
    pub versions: Vec<String>,
    /// The published release posts of those releases, ascending by release.
    /// A release without a published post has none.
    pub posts: Vec<ChangelogDeploymentGroupPost>,
}

/// One change of a deployment group that needs attention: breaking, or with
/// `action_required`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogDeploymentGroupAttention {
    /// The slug of the release post that announces the change.
    pub post_slug: String,
    /// The release of that post.
    pub release_version: String,
    /// The change as in its post.
    pub change: ChangelogChange,
}

/// One highlight of a release post in a deployment group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangelogDeploymentGroupHighlight {
    /// The slug of the release post.
    pub post_slug: String,
    /// The release of that post.
    pub release_version: String,
    /// Highlight text, inline Markdown.
    pub text: String,
}

/// One complete deployment group.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogDeploymentGroup {
    pub summary: ChangelogDeploymentGroupSummary,
    /// The breaking changes and the changes that require action across the
    /// group's posts, in post order (ascending release), then section and
    /// change order.
    pub attention: Vec<ChangelogDeploymentGroupAttention>,
    /// The highlights of the group's posts, in post order.
    pub highlights: Vec<ChangelogDeploymentGroupHighlight>,
}

/// One `list_deployment_groups` page, newest deployment first. An empty
/// `next_page_token` means the last page.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogDeploymentGroupsPage {
    pub groups: Vec<ChangelogDeploymentGroupSummary>,
    pub next_page_token: String,
}
