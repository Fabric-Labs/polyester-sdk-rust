//! Public changelog without credentials: newest posts, the newest post in
//! full, breaking changes since a date, and deployment groups.
//!
//!   cargo run --example changelog
//!   POLYESTER_API_URL=https://api.testnet.polyester.com cargo run --example changelog

use futures_util::TryStreamExt;
use polyester::services::ListChangelogPostsOptions;
use polyester::{Client, Config};
use std::time::{Duration, UNIX_EPOCH};

#[tokio::main]
async fn main() -> polyester::Result<()> {
    let mut config = Config {
        hydrate_catalogs: false,
        ..Default::default()
    };
    if let Ok(url) = std::env::var("POLYESTER_API_URL") {
        config.api_url = url;
    }
    let client = Client::new(config)?;

    let posts: Vec<_> = client
        .changelog
        .posts(ListChangelogPostsOptions::default())
        .try_collect()
        .await?;
    println!("posts: {}", posts.len());
    for p in &posts {
        println!(
            "  {} changes={} breaking={} action_required={} {}",
            p.slug, p.change_count, p.breaking_change_count, p.action_required_count, p.title
        );
    }

    let Some(newest) = posts.first() else {
        return Ok(());
    };
    let post = client.changelog.get_post(&newest.slug).await?;
    println!(
        "post {}: {} sections",
        post.summary.slug,
        post.sections.len()
    );
    for s in &post.sections {
        println!("  {} ({} changes)", s.heading, s.changes.len());
    }

    // 2026-01-01T00:00:00Z
    let since = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
    let breaking = client.changelog.breaking_changes_since(since).await?;
    println!("breaking changes since 2026-01-01: {}", breaking.len());
    for e in &breaking {
        println!(
            "  {} [{}] {} action_required={} surfaces={:?}",
            e.change.id, e.post_slug, e.change.title, e.change.action_required, e.change.surfaces
        );
    }

    // Empty until the environment publishes release posts.
    let groups = client
        .changelog
        .list_deployment_groups(Default::default())
        .await?;
    println!("deployment groups: {}", groups.groups.len());
    for g in &groups.groups {
        println!("  {} {} {:?}", g.slug, g.environment, g.versions);
    }
    if let Some(first) = groups.groups.first() {
        let group = client.changelog.get_deployment_group(&first.slug).await?;
        println!(
            "group {}: {} attention, {} highlights",
            group.summary.slug,
            group.attention.len(),
            group.highlights.len()
        );
    }
    Ok(())
}
