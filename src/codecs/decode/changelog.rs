//! Decode changelog.v1 responses into public SDK models.

use crate::codecs::scalars::format_id;
use crate::errors::{Error, Result};
use crate::models::{
    ChangelogChange, ChangelogChangeEntry, ChangelogChangesPage, ChangelogPost,
    ChangelogPostSummary, ChangelogPostsPage, ChangelogSection,
};
use crate::proto::changelog::v1::{
    Change, ChangeEntry, GetPostResponse, ListChangesResponse, ListPostsResponse, PostSummary,
    Section,
};
use buffa::Enumeration;
use buffa_types::google::protobuf::Timestamp;

fn clone_timestamp(ts: Option<&Timestamp>) -> Option<Timestamp> {
    ts.map(|t| Timestamp {
        seconds: t.seconds,
        nanos: t.nanos,
        ..Default::default()
    })
}

/// Lowercase label of a changelog enum: `""` when unspecified, `unknown(<n>)`
/// for a value newer than this SDK.
pub fn changelog_enum_label<T: Enumeration>(value: &buffa::EnumValue<T>) -> String {
    match value.as_known() {
        Some(_) if value.to_i32() == 0 => String::new(),
        Some(known) => known.proto_name().to_ascii_lowercase(),
        None => format!("unknown({})", value.to_i32()),
    }
}

fn change_from_proto(msg: &Change) -> ChangelogChange {
    ChangelogChange {
        id: format_id(msg.id),
        title: msg.title.clone(),
        summary: msg.summary.clone(),
        details: msg.details.clone(),
        category: changelog_enum_label(&msg.category),
        action_required: msg.action_required,
        migration_guidance: msg.migration_guidance.clone(),
        audiences: msg.audiences.iter().map(changelog_enum_label).collect(),
        surfaces: msg.surfaces.iter().map(changelog_enum_label).collect(),
    }
}

fn section_from_proto(msg: &Section) -> ChangelogSection {
    ChangelogSection {
        heading: msg.heading.clone(),
        summary: msg.summary.clone(),
        changes: msg.changes.iter().map(change_from_proto).collect(),
        surface: changelog_enum_label(&msg.surface),
    }
}

fn post_summary_from_proto(msg: &PostSummary) -> ChangelogPostSummary {
    ChangelogPostSummary {
        slug: msg.slug.clone(),
        title: msg.title.clone(),
        introduction: msg.introduction.clone(),
        published_at: clone_timestamp(msg.published_at.as_option()),
        updated_at: clone_timestamp(msg.updated_at.as_option()),
        revision: msg.revision,
        coverage_cutoff: clone_timestamp(msg.coverage_cutoff.as_option()),
        change_count: msg.change_count,
        breaking_change_count: msg.breaking_change_count,
        action_required_count: msg.action_required_count,
        new_feature_count: msg.new_feature_count,
        improvement_count: msg.improvement_count,
        bug_fix_count: msg.bug_fix_count,
    }
}

fn change_entry_from_proto(msg: &ChangeEntry) -> ChangelogChangeEntry {
    ChangelogChangeEntry {
        change: msg
            .change
            .as_option()
            .map(change_from_proto)
            .unwrap_or_default(),
        post_slug: msg.post_slug.clone(),
        published_at: clone_timestamp(msg.published_at.as_option()),
        section_heading: msg.section_heading.clone(),
    }
}

pub fn changelog_posts_page_from_proto(msg: &ListPostsResponse) -> ChangelogPostsPage {
    ChangelogPostsPage {
        posts: msg.posts.iter().map(post_summary_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

/// Decode `GetPostResponse`; a response without a post is a contract
/// violation.
pub fn changelog_post_from_proto(msg: &GetPostResponse) -> Result<ChangelogPost> {
    let post = msg
        .post
        .as_option()
        .ok_or_else(|| Error::transport("invalid GetPost response: missing post"))?;
    Ok(ChangelogPost {
        summary: post
            .summary
            .as_option()
            .map(post_summary_from_proto)
            .unwrap_or_default(),
        sections: post.sections.iter().map(section_from_proto).collect(),
    })
}

pub fn changelog_changes_page_from_proto(msg: &ListChangesResponse) -> ChangelogChangesPage {
    ChangelogChangesPage {
        changes: msg.changes.iter().map(change_entry_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::changelog::v1::{Audience, ChangeCategory, Post, Surface};
    use buffa::EnumValue;

    #[test]
    fn change_labels_known_and_unknown_enums() {
        let change = Change {
            id: 3_914_982_961_873_321_678,
            title: "Request IDs".into(),
            category: ChangeCategory::BREAKING_CHANGE.into(),
            action_required: true,
            audiences: vec![Audience::DEVELOPERS.into(), EnumValue::Unknown(42)],
            surfaces: vec![Surface::API.into(), EnumValue::Unknown(99)],
            ..Default::default()
        };
        let decoded = change_from_proto(&change);
        assert_eq!(decoded.id, format_id(3_914_982_961_873_321_678));
        assert_eq!(decoded.category, "breaking_change");
        assert_eq!(decoded.audiences, vec!["developers", "unknown(42)"]);
        assert_eq!(decoded.surfaces, vec!["api", "unknown(99)"]);

        let unknown = change_from_proto(&Change {
            category: EnumValue::Unknown(7),
            ..Default::default()
        });
        assert_eq!(unknown.category, "unknown(7)");
        let unspecified = change_from_proto(&Change::default());
        assert_eq!(unspecified.category, "");
    }

    #[test]
    fn sections_say_which_product_area_they_cover() {
        let section = |surface: EnumValue<Surface>| {
            section_from_proto(&Section {
                heading: "Section".into(),
                surface,
                ..Default::default()
            })
        };
        assert_eq!(section(Surface::MARKET_DATA.into()).surface, "market_data");
        assert_eq!(section(Surface::QUICKSWAP.into()).surface, "quickswap");
        assert_eq!(section(Surface::REWARDS.into()).surface, "rewards");
        // A section not tied to one area, and an area newer than this SDK.
        assert_eq!(section(Surface::SURFACE_UNSPECIFIED.into()).surface, "");
        assert_eq!(section(EnumValue::Unknown(77)).surface, "unknown(77)");

        let change = change_from_proto(&Change {
            surfaces: vec![Surface::MARKET_DATA.into(), Surface::REWARDS.into()],
            ..Default::default()
        });
        assert_eq!(change.surfaces, vec!["market_data", "rewards"]);
    }

    /// Changes to the AI assistant, and a post section covering it, read
    /// `assistant`.
    #[test]
    fn assistant_surface_reads_its_label() {
        let change = change_from_proto(&Change {
            surfaces: vec![Surface::ASSISTANT.into(), Surface::API.into()],
            ..Default::default()
        });
        assert_eq!(
            change.surfaces,
            vec![crate::models::CHANGELOG_SURFACE_ASSISTANT, "api"]
        );
        let section = section_from_proto(&Section {
            heading: "AI assistant".into(),
            surface: Surface::ASSISTANT.into(),
            ..Default::default()
        });
        assert_eq!(section.surface, "assistant");
    }

    /// A republished post keeps its slug and first publication time, and
    /// reports its current revision and when that revision was published.
    #[test]
    fn post_summary_reads_revision_and_updated_at() {
        use buffa_types::google::protobuf::Timestamp;
        let published = Timestamp {
            seconds: 1_793_786_400,
            ..Default::default()
        };
        let updated = Timestamp {
            seconds: 1_793_957_400,
            ..Default::default()
        };
        let page = changelog_posts_page_from_proto(&ListPostsResponse {
            posts: vec![PostSummary {
                slug: "2026-11-04".into(),
                published_at: published.clone().into(),
                updated_at: updated.clone().into(),
                revision: 2,
                ..Default::default()
            }],
            ..Default::default()
        });
        let summary = &page.posts[0];
        assert_eq!(summary.revision, 2);
        assert_eq!(
            summary.updated_at.as_ref().map(|t| t.seconds),
            Some(updated.seconds)
        );
        assert_eq!(
            summary.published_at.as_ref().map(|t| t.seconds),
            Some(published.seconds)
        );

        // A server before revisions sends neither.
        let older = changelog_posts_page_from_proto(&ListPostsResponse {
            posts: vec![PostSummary {
                slug: "2026-10-01".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(older.posts[0].revision, 0);
        assert!(older.posts[0].updated_at.is_none());
    }

    #[test]
    fn get_post_requires_a_post() {
        let err = changelog_post_from_proto(&GetPostResponse::default()).unwrap_err();
        assert!(err.to_string().contains("missing post"), "{err}");

        let post = changelog_post_from_proto(&GetPostResponse {
            post: Post {
                summary: PostSummary {
                    slug: "2026-09-24".into(),
                    change_count: 1,
                    ..Default::default()
                }
                .into(),
                sections: vec![Section {
                    heading: "Trading".into(),
                    changes: vec![Change {
                        id: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(post.summary.slug, "2026-09-24");
        assert_eq!(post.sections[0].changes[0].id, "2");
    }
}
