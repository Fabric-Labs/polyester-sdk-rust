//! Changes met again while a walk pages through a republished changelog post
//! (contract 1bf038e).

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use futures_util::TryStreamExt;
use polyester::proto::changelog::v1::{
    Change, ChangeCategory, ChangeEntry, ListChangesRequest, ListChangesResponse,
};
use polyester::services::ListChangelogChangesOptions;
use polyester::{Client, Config};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

const LIST_CHANGES_PATH: &str = "/changelog.v1.ChangelogService/ListChanges";

fn entry(id: u64, title: &str, slug: &str) -> ChangeEntry {
    ChangeEntry {
        change: Change {
            id,
            title: title.into(),
            category: ChangeCategory::BREAKING_CHANGE.into(),
            action_required: true,
            ..Default::default()
        }
        .into(),
        post_slug: slug.into(),
        ..Default::default()
    }
}

/// Two ListChanges pages. Between them the post the first page's token points
/// into is republished, so the second page restarts at that post's first
/// change and repeats change 1002 with its new wording, as the server does.
async fn republishing_server() -> (MockHttpServer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path != LIST_CHANGES_PATH {
            return HttpScript::NotFound;
        }
        counter.fetch_add(1, Ordering::SeqCst);
        let req = ListChangesRequest::decode_from_slice(&request.body).unwrap();
        let (changes, next) = if req.page_token.is_empty() {
            (
                vec![
                    entry(1001, "newer post change", "2026-11-06"),
                    entry(1002, "first wording", "2026-11-04"),
                ],
                "t1",
            )
        } else {
            (
                vec![
                    entry(1002, "corrected wording", "2026-11-04"),
                    entry(1003, "third", "2026-11-04"),
                ],
                "",
            )
        };
        connect_proto_ok(&ListChangesResponse {
            changes,
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    (server, calls)
}

fn client(server: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap()
}

fn distinct(ids: &[String]) -> bool {
    let mut sorted = ids.to_vec();
    sorted.sort();
    sorted.dedup();
    sorted.len() == ids.len()
}

#[tokio::test]
async fn changes_stream_yields_a_republished_change_once() {
    let (server, calls) = republishing_server().await;
    let entries: Vec<_> = client(&server)
        .changelog
        .changes(ListChangelogChangesOptions::default())
        .try_collect()
        .await
        .unwrap();
    let ids: Vec<String> = entries.iter().map(|e| e.change.id.clone()).collect();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(ids.len() == 3 && distinct(&ids), "{ids:?}");
}

#[tokio::test]
async fn since_helpers_keep_one_current_copy_of_a_republished_change() {
    let since = UNIX_EPOCH + Duration::from_secs(1_793_491_200);
    for helper in ["breaking", "action_required"] {
        let (server, _) = republishing_server().await;
        let client = client(&server);
        let entries = if helper == "breaking" {
            client.changelog.breaking_changes_since(since).await
        } else {
            client.changelog.action_required_since(since).await
        }
        .unwrap();
        let ids: Vec<String> = entries.iter().map(|e| e.change.id.clone()).collect();
        assert!(ids.len() == 3 && distinct(&ids), "{helper}: {ids:?}");
        // The repeat keeps its place with the current revision's wording.
        assert_eq!(entries[1].change.title, "corrected wording", "{helper}");
    }
}
