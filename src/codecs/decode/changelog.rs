//! Decode changelog.v1 responses into public SDK models.

use crate::codecs::scalars::format_id;
use crate::errors::{Error, Result};
use crate::models::{
    ChangelogChange, ChangelogChangeEntry, ChangelogChangeException, ChangelogChangesPage,
    ChangelogDeploymentGroup, ChangelogDeploymentGroupAttention, ChangelogDeploymentGroupHighlight,
    ChangelogDeploymentGroupPost, ChangelogDeploymentGroupSummary, ChangelogDeploymentGroupsPage,
    ChangelogEnvironmentStatus, ChangelogPost, ChangelogPostSummary, ChangelogPostsPage,
    ChangelogSection,
};
use crate::proto::changelog::v1::{
    Change, ChangeEntry, ChangeException, DeploymentGroupPost, DeploymentGroupSummary,
    EnvironmentStatus, GetDeploymentGroupResponse, GetPostResponse, ListChangesResponse,
    ListDeploymentGroupsResponse, ListPostsResponse, PostSummary, Section,
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
        environment_exceptions: msg
            .environment_exceptions
            .iter()
            .map(change_exception_from_proto)
            .collect(),
    }
}

fn change_exception_from_proto(msg: &ChangeException) -> ChangelogChangeException {
    ChangelogChangeException {
        environment: changelog_enum_label(&msg.environment),
        availability: changelog_enum_label(&msg.availability),
        live_at: clone_timestamp(msg.live_at.as_option()),
    }
}

fn environment_status_from_proto(msg: &EnvironmentStatus) -> ChangelogEnvironmentStatus {
    ChangelogEnvironmentStatus {
        environment: changelog_enum_label(&msg.environment),
        live_at: clone_timestamp(msg.live_at.as_option()),
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
        release_version: msg.release_version.clone(),
        environments: msg
            .environments
            .iter()
            .map(environment_status_from_proto)
            .collect(),
        deployment_group_slug: msg.deployment_group_slug.clone(),
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

fn deployment_group_post_from_proto(msg: &DeploymentGroupPost) -> ChangelogDeploymentGroupPost {
    ChangelogDeploymentGroupPost {
        slug: msg.slug.clone(),
        release_version: msg.release_version.clone(),
        title: msg.title.clone(),
        published_at: clone_timestamp(msg.published_at.as_option()),
        revision: msg.revision,
        updated_at: clone_timestamp(msg.updated_at.as_option()),
    }
}

fn deployment_group_summary_from_proto(
    msg: &DeploymentGroupSummary,
) -> ChangelogDeploymentGroupSummary {
    ChangelogDeploymentGroupSummary {
        slug: msg.slug.clone(),
        environment: changelog_enum_label(&msg.environment),
        occurred_at: clone_timestamp(msg.occurred_at.as_option()),
        release_version: msg.release_version.clone(),
        release_tag: msg.release_tag.clone(),
        previous_release_version: msg.previous_release_version.clone(),
        versions: msg.versions.clone(),
        posts: msg
            .posts
            .iter()
            .map(deployment_group_post_from_proto)
            .collect(),
    }
}

pub fn changelog_deployment_groups_page_from_proto(
    msg: &ListDeploymentGroupsResponse,
) -> ChangelogDeploymentGroupsPage {
    ChangelogDeploymentGroupsPage {
        groups: msg
            .groups
            .iter()
            .map(deployment_group_summary_from_proto)
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

/// Decode `GetDeploymentGroupResponse`; a response without a group, or a
/// group without its summary, is a contract violation.
pub fn changelog_deployment_group_from_proto(
    msg: &GetDeploymentGroupResponse,
) -> Result<ChangelogDeploymentGroup> {
    let group = msg.group.as_option().ok_or_else(|| {
        Error::transport("invalid GetDeploymentGroup response: missing deployment group")
    })?;
    let summary = group.summary.as_option().ok_or_else(|| {
        Error::transport("invalid GetDeploymentGroup response: missing deployment group summary")
    })?;
    Ok(ChangelogDeploymentGroup {
        summary: deployment_group_summary_from_proto(summary),
        attention: group
            .attention
            .iter()
            .map(|item| ChangelogDeploymentGroupAttention {
                post_slug: item.post_slug.clone(),
                release_version: item.release_version.clone(),
                change: item
                    .change
                    .as_option()
                    .map(change_from_proto)
                    .unwrap_or_default(),
            })
            .collect(),
        highlights: group
            .highlights
            .iter()
            .map(|item| ChangelogDeploymentGroupHighlight {
                post_slug: item.post_slug.clone(),
                release_version: item.release_version.clone(),
                text: item.text.clone(),
            })
            .collect(),
    })
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

    fn ts(seconds: i64) -> Timestamp {
        Timestamp {
            seconds,
            ..Default::default()
        }
    }

    /// A release post names its release, its status in every environment and
    /// the deployment group that brought it here; a pending environment has
    /// no `live_at`, and a post that announces no release has none of them.
    #[test]
    fn release_post_fields_keep_their_presence() {
        use crate::proto::changelog::v1::Environment;
        let page = changelog_posts_page_from_proto(&ListPostsResponse {
            posts: vec![
                PostSummary {
                    slug: "v0.92.9".into(),
                    release_version: Some("0.92.9".into()),
                    environments: vec![
                        EnvironmentStatus {
                            environment: Environment::DEVNET.into(),
                            live_at: ts(1_793_000_000).into(),
                            ..Default::default()
                        },
                        EnvironmentStatus {
                            environment: Environment::TESTNET.into(),
                            ..Default::default()
                        },
                        EnvironmentStatus {
                            environment: EnumValue::Unknown(9),
                            ..Default::default()
                        },
                    ],
                    deployment_group_slug: Some("devnet-v0.92.9".into()),
                    ..Default::default()
                },
                PostSummary {
                    slug: "2026-11-04".into(),
                    release_version: Some(String::new()),
                    ..Default::default()
                },
                PostSummary {
                    slug: "2026-11-03".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        let release = &page.posts[0];
        assert_eq!(release.release_version.as_deref(), Some("0.92.9"));
        assert_eq!(
            release.deployment_group_slug.as_deref(),
            Some("devnet-v0.92.9")
        );
        let environments: Vec<_> = release
            .environments
            .iter()
            .map(|e| {
                (
                    e.environment.as_str(),
                    e.live_at.as_ref().map(|t| t.seconds),
                )
            })
            .collect();
        assert_eq!(
            environments,
            [
                (
                    crate::models::CHANGELOG_ENVIRONMENT_DEVNET,
                    Some(1_793_000_000)
                ),
                (crate::models::CHANGELOG_ENVIRONMENT_TESTNET, None),
                ("unknown(9)", None),
            ]
        );
        // An empty optional string is present, not absent.
        assert_eq!(page.posts[1].release_version.as_deref(), Some(""));
        let plain = &page.posts[2];
        assert!(plain.release_version.is_none());
        assert!(plain.deployment_group_slug.is_none());
        assert!(plain.environments.is_empty());
    }

    #[test]
    fn change_exceptions_read_environment_and_availability() {
        use crate::proto::changelog::v1::{ChangeAvailability, Environment};
        let change = change_from_proto(&Change {
            id: 1,
            environment_exceptions: vec![
                ChangeException {
                    environment: Environment::TESTNET.into(),
                    availability: ChangeAvailability::AHEAD.into(),
                    live_at: ts(1_792_000_000).into(),
                    ..Default::default()
                },
                ChangeException {
                    environment: Environment::MAINNET.into(),
                    availability: ChangeAvailability::UNAVAILABLE.into(),
                    ..Default::default()
                },
                ChangeException {
                    environment: Environment::DEVNET.into(),
                    availability: EnumValue::Unknown(8),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        let got: Vec<_> = change
            .environment_exceptions
            .iter()
            .map(|e| {
                (
                    e.environment.as_str(),
                    e.availability.as_str(),
                    e.live_at.as_ref().map(|t| t.seconds),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                (
                    "testnet",
                    crate::models::CHANGELOG_AVAILABILITY_AHEAD,
                    Some(1_792_000_000)
                ),
                (
                    "mainnet",
                    crate::models::CHANGELOG_AVAILABILITY_UNAVAILABLE,
                    None
                ),
                ("devnet", "unknown(8)", None),
            ]
        );
        assert!(
            change_from_proto(&Change::default())
                .environment_exceptions
                .is_empty()
        );
    }

    fn group_summary() -> DeploymentGroupSummary {
        use crate::proto::changelog::v1::Environment;
        DeploymentGroupSummary {
            slug: "testnet-v0.92.9".into(),
            environment: Environment::TESTNET.into(),
            occurred_at: ts(1_793_100_000).into(),
            release_version: "0.92.9".into(),
            release_tag: Some("v0.92.9".into()),
            previous_release_version: None,
            versions: vec!["0.92.8".into(), "0.92.9".into()],
            posts: vec![DeploymentGroupPost {
                slug: "v0.92.9".into(),
                release_version: "0.92.9".into(),
                title: "Release 0.92.9".into(),
                published_at: ts(1_793_000_000).into(),
                revision: 2,
                updated_at: ts(1_793_050_000).into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn deployment_groups_page_decodes_every_field() {
        let page = changelog_deployment_groups_page_from_proto(&ListDeploymentGroupsResponse {
            groups: vec![group_summary()],
            next_page_token: "next".into(),
            ..Default::default()
        });
        assert_eq!(page.next_page_token, "next");
        let group = &page.groups[0];
        assert_eq!(group.slug, "testnet-v0.92.9");
        assert_eq!(group.environment, "testnet");
        assert_eq!(
            group.occurred_at.as_ref().map(|t| t.seconds),
            Some(1_793_100_000)
        );
        assert_eq!(group.release_version, "0.92.9");
        assert_eq!(group.release_tag.as_deref(), Some("v0.92.9"));
        assert!(group.previous_release_version.is_none());
        assert_eq!(group.versions, ["0.92.8", "0.92.9"]);
        let post = &group.posts[0];
        assert_eq!(
            (
                post.slug.as_str(),
                post.release_version.as_str(),
                post.title.as_str()
            ),
            ("v0.92.9", "0.92.9", "Release 0.92.9")
        );
        assert_eq!(post.revision, 2);
        assert_eq!(
            post.published_at.as_ref().map(|t| t.seconds),
            Some(1_793_000_000)
        );
        assert_eq!(
            post.updated_at.as_ref().map(|t| t.seconds),
            Some(1_793_050_000)
        );
    }

    #[test]
    fn get_deployment_group_requires_a_group_and_its_summary() {
        use crate::proto::changelog::v1::{
            DeploymentGroup, DeploymentGroupAttention, DeploymentGroupHighlight,
        };
        let err = changelog_deployment_group_from_proto(&GetDeploymentGroupResponse::default())
            .unwrap_err();
        assert!(
            err.to_string().contains("missing deployment group"),
            "{err}"
        );
        let err = changelog_deployment_group_from_proto(&GetDeploymentGroupResponse {
            group: DeploymentGroup::default().into(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("missing deployment group summary"),
            "{err}"
        );

        let group = changelog_deployment_group_from_proto(&GetDeploymentGroupResponse {
            group: DeploymentGroup {
                summary: group_summary().into(),
                attention: vec![DeploymentGroupAttention {
                    post_slug: "v0.92.9".into(),
                    release_version: "0.92.9".into(),
                    change: Change {
                        id: 7,
                        category: ChangeCategory::BREAKING_CHANGE.into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }],
                highlights: vec![DeploymentGroupHighlight {
                    post_slug: "v0.92.9".into(),
                    release_version: "0.92.9".into(),
                    text: "**OCO** orders".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(group.summary.slug, "testnet-v0.92.9");
        assert_eq!(group.attention[0].post_slug, "v0.92.9");
        assert_eq!(group.attention[0].change.id, format_id(7));
        assert_eq!(group.attention[0].change.category, "breaking_change");
        assert_eq!(group.highlights[0].text, "**OCO** orders");
    }
}
