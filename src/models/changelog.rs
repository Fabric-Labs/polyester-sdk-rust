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
}

/// A post's changes grouped under one heading.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangelogSection {
    pub heading: String,
    pub summary: Option<String>,
    pub changes: Vec<ChangelogChange>,
}

/// A changelog post without its sections, with change counts by kind.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangelogPostSummary {
    /// The post's coverage cutoff date, `YYYY-MM-DD`, with a `-2` or `-3`
    /// suffix for a later post on the same day.
    pub slug: String,
    pub title: String,
    pub introduction: String,
    pub published_at: Option<Timestamp>,
    pub coverage_cutoff: Option<Timestamp>,
    pub change_count: u32,
    pub breaking_change_count: u32,
    pub action_required_count: u32,
    pub new_feature_count: u32,
    pub improvement_count: u32,
    pub bug_fix_count: u32,
}

/// One full changelog post.
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
