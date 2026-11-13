//! Public contract 74af7f3 (exchange release 0.93.1), through the public
//! services against a local server: changelog deployment groups and the
//! explicit presence of a transfer history upper bound.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use futures_util::TryStreamExt;
use polyester::proto::changelog::v1::{
    DeploymentGroup, DeploymentGroupSummary, Environment, GetDeploymentGroupRequest,
    GetDeploymentGroupResponse, ListDeploymentGroupsRequest, ListDeploymentGroupsResponse,
};
use polyester::proto::ledger::read::v1::{ListTransfersRequest, ListTransfersResponse};
use polyester::services::ListChangelogDeploymentGroupsOptions;
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error};
use serde_json::Value;
use std::sync::{Arc, Mutex};

const LIST_GROUPS: &str = "/changelog.v1.ChangelogService/ListDeploymentGroups";
const GET_GROUP: &str = "/changelog.v1.ChangelogService/GetDeploymentGroup";
const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

fn summary(slug: &str) -> DeploymentGroupSummary {
    DeploymentGroupSummary {
        slug: slug.into(),
        environment: Environment::TESTNET.into(),
        release_version: slug.trim_start_matches("testnet-v").into(),
        ..Default::default()
    }
}

/// Two pages of deployment groups, and one group by slug; any other slug is
/// not found.
async fn changelog_server() -> (MockHttpServer, Arc<Mutex<Vec<String>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| match request.path.as_str() {
        LIST_GROUPS => {
            let req = ListDeploymentGroupsRequest::decode_from_slice(&request.body).unwrap();
            log.lock()
                .unwrap()
                .push(format!("list limit={} token={}", req.limit, req.page_token));
            let (groups, next) = if req.page_token.is_empty() {
                (
                    vec![summary("testnet-v0.92.9"), summary("testnet-v0.92.7")],
                    "g2",
                )
            } else {
                (vec![summary("testnet-v0.92.5")], "")
            };
            connect_proto_ok(&ListDeploymentGroupsResponse {
                groups,
                next_page_token: next.into(),
                ..Default::default()
            })
        }
        GET_GROUP => {
            let req = GetDeploymentGroupRequest::decode_from_slice(&request.body).unwrap();
            log.lock().unwrap().push(format!("get {}", req.slug));
            if req.slug == "testnet-v0.92.9" {
                connect_proto_ok(&GetDeploymentGroupResponse {
                    group: DeploymentGroup {
                        summary: summary(&req.slug).into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                })
            } else {
                HttpScript::Json {
                    status: 404,
                    body: br#"{"code":"not_found","message":"deployment group not found"}"#
                        .to_vec(),
                }
            }
        }
        _ => HttpScript::NotFound,
    })
    .await;
    (server, seen)
}

/// Deployment groups are public: a client without an API key reads them,
/// one page, every page, and one group by slug.
#[tokio::test]
async fn deployment_groups_are_read_without_credentials() {
    let (server, seen) = changelog_server().await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();

    let page = client
        .changelog
        .list_deployment_groups(ListChangelogDeploymentGroupsOptions {
            limit: Some(2),
            page_token: None,
        })
        .await
        .unwrap();
    assert_eq!(page.groups.len(), 2);
    assert_eq!(page.groups[0].slug, "testnet-v0.92.9");
    assert_eq!(page.groups[0].environment, "testnet");
    assert_eq!(page.next_page_token, "g2");

    let all: Vec<_> = client
        .changelog
        .deployment_groups(ListChangelogDeploymentGroupsOptions {
            limit: Some(2),
            page_token: None,
        })
        .try_collect()
        .await
        .unwrap();
    let slugs: Vec<_> = all.iter().map(|g| g.release_version.as_str()).collect();
    assert_eq!(slugs, ["0.92.9", "0.92.7", "0.92.5"]);

    let group = client
        .changelog
        .get_deployment_group("testnet-v0.92.9")
        .await
        .unwrap();
    assert_eq!(group.summary.slug, "testnet-v0.92.9");
    assert!(group.attention.is_empty() && group.highlights.is_empty());

    let missing = client
        .changelog
        .get_deployment_group("testnet-v9.9.9")
        .await
        .unwrap_err();
    assert!(
        matches!(&missing, Error::Api { code, .. } if code == "NotFound"),
        "{missing:?}"
    );

    assert_eq!(
        *seen.lock().unwrap(),
        [
            "list limit=2 token=",
            "list limit=2 token=",
            "list limit=2 token=g2",
            "get testnet-v0.92.9",
            "get testnet-v9.9.9",
        ]
    );
}

/// `ts_max_us` has explicit presence: `None` asks for no upper bound and is
/// absent on the wire, while `Some(0)` is an explicit epoch bound, which the
/// API answers with an empty final page, and is sent as zero. Both wire
/// formats keep the difference.
#[tokio::test]
async fn transfer_upper_bound_keeps_its_presence_on_both_wires() {
    for wire in [WireFormat::Binary, WireFormat::Json] {
        for bound in [None, Some(0_u64), Some(1_793_000_000_000_000)] {
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let log = bodies.clone();
            let server = MockHttpServer::spawn(move |request| {
                assert!(request.path.ends_with("/ListTransfers"));
                log.lock().unwrap().push(request.body.clone());
                let response = ListTransfersResponse::default();
                if wire == WireFormat::Json {
                    HttpScript::Json {
                        status: 200,
                        body: serde_json::to_vec(&response).unwrap(),
                    }
                } else {
                    connect_proto_ok(&response)
                }
            })
            .await;
            let client = Client::new(Config {
                api_url: server.base_url(),
                api_key_id: Some("ak_test".into()),
                api_private_key: Some(TEST_KEY.into()),
                hydrate_catalogs: false,
                wire_format: wire,
                ..Default::default()
            })
            .unwrap();
            client
                .transfers
                .list(ListTransfersRequest {
                    limit: 10,
                    ts_max_us: bound,
                    ..Default::default()
                })
                .await
                .unwrap();
            let body = bodies.lock().unwrap().pop().expect("one request");
            let case = format!("{wire:?} {bound:?}");
            let sent = if wire == WireFormat::Json {
                let json: Value = serde_json::from_slice(&body).unwrap();
                let field = json.get("tsMaxUs").or_else(|| json.get("ts_max_us"));
                assert_eq!(
                    field.is_some(),
                    bound.is_some(),
                    "{case}: presence in {json}"
                );
                serde_json::from_slice::<ListTransfersRequest>(&body)
                    .unwrap()
                    .ts_max_us
            } else {
                // limit (field 2) is 10; ts_max_us is field 5, tag 0x28.
                match bound {
                    None => assert_eq!(body, [0x10, 10], "{case}"),
                    Some(0) => assert_eq!(body, [0x10, 10, 0x28, 0], "{case}"),
                    Some(_) => assert_eq!(body[..3], [0x10, 10, 0x28], "{case}"),
                }
                ListTransfersRequest::decode_from_slice(&body)
                    .unwrap()
                    .ts_max_us
            };
            assert_eq!(sent, bound, "{case}");
        }
    }
}
