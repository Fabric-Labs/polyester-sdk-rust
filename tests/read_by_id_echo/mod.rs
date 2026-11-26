//! A read by ID must answer the object it asked for: a reply that names
//! another one breaks the contract, however well formed it is, and is refused
//! with `Error::ResponseContract`; the reply that names the requested object
//! decodes. Each read goes through its public service against a local server.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use polyester::codecs::scalars::format_id;
use polyester::models::{GetOrderOpts, OrderKey, SubAccountScope};
use polyester::proto::auth::v1::{ApiKey, GetApiKeyResponse, GetSubaccountResponse, Subaccount};
use polyester::proto::chain::lifecycle::v1::{
    FlowDetailView, FlowParticipation, FlowSummaryView, GetAccountFlowResponse, GetFlowResponse,
};
use polyester::proto::changelog::v1::{
    DeploymentGroup, DeploymentGroupSummary, GetDeploymentGroupResponse, GetPostResponse, Post,
    PostSummary,
};
use polyester::proto::ledger::read::v1::{
    CancelStatementExportResponse, GetStatementExportResponse, StatementExport,
    StatementExportStatus,
};
use polyester::proto::orders::v1::{
    BatchReplaceAdmissionStatus, GetBatchReplaceStatusResponse, GetOrderResponse, Order,
};
use polyester::proto::transfer::v1::{DustConversion, GetDustConversionResponse};
use polyester::proto::triggers::v1::{GetTriggerResponse, Trigger};
use polyester::services::GetSubaccountOpts;
use polyester::{Client, Config, Error};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

/// A client against a server that answers each request with the next of
/// `scripts`, and not found once they are spent.
async fn server(scripts: Vec<HttpScript>) -> Client {
    let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
    let http = MockHttpServer::spawn(move |_| {
        scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(HttpScript::NotFound)
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    // The server lives as long as the test: leak it with the client.
    std::mem::forget(http);
    client
}

/// A client whose server answers the wrong reply, then the right one.
async fn wrong_then_right<M: buffa::Message>(wrong: &M, right: &M) -> Client {
    server(vec![connect_proto_ok(wrong), connect_proto_ok(right)]).await
}

#[track_caller]
fn assert_contract<T: std::fmt::Debug>(result: polyester::Result<T>, rpc: &str) {
    let err = result.unwrap_err();
    assert!(
        matches!(&err, Error::ResponseContract { context, .. } if context == rpc),
        "{err:?}"
    );
}

#[tokio::test]
async fn get_batch_replace_status_refuses_another_batch() {
    let reply = |id| GetBatchReplaceStatusResponse {
        batch_request_id: id,
        admission_status: BatchReplaceAdmissionStatus::BATCH_REPLACE_ADMISSION_STATUS_ADMITTED
            .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply(8), &reply(7)).await;
    let id = format_id(7);
    assert_contract(
        client
            .orders
            .get_batch_replace_status(&id, SubAccountScope::Default)
            .await,
        "GetBatchReplaceStatus",
    );
    let read = client
        .orders
        .get_batch_replace_status(&id, SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(read.batch_request_id, id);
}

fn order_reply(order_id: u64, client_order_id: &str) -> GetOrderResponse {
    GetOrderResponse {
        order: Order {
            order_id,
            client_order_id: client_order_id.into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn order_opts(key: OrderKey) -> GetOrderOpts {
    GetOrderOpts {
        key,
        subaccount_id: SubAccountScope::Default,
        include_attached_risk: false,
        include_attached_risk_state: false,
        include_execution_history: None,
        limit: None,
        page_token: None,
    }
}

#[tokio::test]
async fn get_order_by_order_id_refuses_another_order() {
    let client = wrong_then_right(&order_reply(43, "cid-1"), &order_reply(42, "cid-1")).await;
    let key = OrderKey::OrderId(format_id(42));
    assert_contract(
        client.orders.get_with(order_opts(key.clone())).await,
        "GetOrder",
    );
    let read = client.orders.get_with(order_opts(key)).await.unwrap();
    assert_eq!(read.order.unwrap().order_id, format_id(42));
}

#[tokio::test]
async fn get_order_by_client_order_id_refuses_another_order() {
    let client = wrong_then_right(&order_reply(42, "cid-2"), &order_reply(42, "cid-1")).await;
    // The client order ID is sent trimmed, and the reply echoes what was sent.
    let key = OrderKey::ClientOrderId(" cid-1 ".into());
    assert_contract(
        client.orders.get_with(order_opts(key.clone())).await,
        "GetOrder",
    );
    let read = client.orders.get_with(order_opts(key)).await.unwrap();
    assert_eq!(read.order.unwrap().client_order_id, "cid-1");
}

#[tokio::test]
async fn get_trigger_refuses_another_trigger() {
    let reply = |trigger_id| GetTriggerResponse {
        trigger: Trigger {
            trigger_id,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply(6), &reply(5)).await;
    let id = format_id(5);
    assert_contract(
        client
            .triggers
            .get_by_id(&id, SubAccountScope::Default)
            .await,
        "GetTrigger",
    );
    let read = client
        .triggers
        .get_by_id(&id, SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(read.unwrap().trigger_id, id);
}

#[tokio::test]
async fn get_dust_conversion_refuses_another_conversion() {
    let reply = |conversion_id: &str| GetDustConversionResponse {
        conversion: DustConversion {
            conversion_id: conversion_id.into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply("124"), &reply("123")).await;
    assert_contract(
        client
            .dust
            .get_conversion("123", SubAccountScope::Default)
            .await,
        "GetDustConversion",
    );
    let read = client
        .dust
        .get_conversion("123", SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(read.conversion_id, "123");
}

fn flow(flow_id: &str) -> FlowDetailView {
    FlowDetailView {
        summary: FlowSummaryView {
            flow_id: flow_id.into(),
            participation: FlowParticipation::PARTICIPATION_OWNER.into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn get_flow_refuses_another_flow() {
    let reply = |flow_id| GetFlowResponse {
        flow: flow(flow_id).into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply("flow_b"), &reply("flow_a")).await;
    assert_contract(client.lifecycle.get_flow("flow_a").await, "GetFlowById");
    let read = client.lifecycle.get_flow("flow_a").await.unwrap();
    assert_eq!(read.intent_id, "flow_a");
}

#[tokio::test]
async fn get_account_flow_refuses_another_flow() {
    let reply = |flow_id| GetAccountFlowResponse {
        flow: flow(flow_id).into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply("flow_b"), &reply("flow_a")).await;
    assert_contract(
        client
            .lifecycle
            .get_account_flow("flow_a", SubAccountScope::Default)
            .await,
        "GetAccountFlow",
    );
    let read = client
        .lifecycle
        .get_account_flow("flow_a", SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(read.intent_id, "flow_a");
}

#[tokio::test]
async fn get_subaccount_refuses_another_subaccount() {
    let reply = |id| GetSubaccountResponse {
        subaccount: Subaccount {
            id,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply(10), &reply(9)).await;
    let id = format_id(9);
    assert_contract(
        client
            .sub_accounts
            .get(&id, GetSubaccountOpts::default())
            .await,
        "GetSubaccount",
    );
    let read = client
        .sub_accounts
        .get(&id, GetSubaccountOpts::default())
        .await
        .unwrap();
    assert_eq!(read.subaccount.unwrap().subaccount_id, id);
}

#[tokio::test]
async fn get_api_key_refuses_another_key() {
    let reply = |key_id: &str| GetApiKeyResponse {
        api_key: ApiKey {
            key_id: key_id.into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let asked = "ak_0123456789abcdef0123456789abcdef";
    let other = "ak_fedcba9876543210fedcba9876543210";
    let client = wrong_then_right(&reply(other), &reply(asked)).await;
    assert_contract(client.api_keys.get(asked).await, "GetApiKey");
    // The server reads the key ID with surrounding whitespace trimmed.
    let read = client.api_keys.get(&format!(" {asked}\n")).await.unwrap();
    assert_eq!(read.unwrap().key_id, asked);
}

#[tokio::test]
async fn get_post_refuses_another_post() {
    let reply = |slug: &str| GetPostResponse {
        post: Post {
            summary: PostSummary {
                slug: slug.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply("2026-09-25"), &reply("2026-09-24")).await;
    assert_contract(client.changelog.get_post("2026-09-24").await, "GetPost");
    let read = client.changelog.get_post("2026-09-24").await.unwrap();
    assert_eq!(read.summary.slug, "2026-09-24");
}

#[tokio::test]
async fn get_deployment_group_refuses_another_group() {
    let reply = |slug: &str| GetDeploymentGroupResponse {
        group: DeploymentGroup {
            summary: DeploymentGroupSummary {
                slug: slug.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply("testnet-v0.92.8"), &reply("testnet-v0.92.9")).await;
    assert_contract(
        client
            .changelog
            .get_deployment_group("testnet-v0.92.9")
            .await,
        "GetDeploymentGroup",
    );
    let read = client
        .changelog
        .get_deployment_group("testnet-v0.92.9")
        .await
        .unwrap();
    assert_eq!(read.summary.slug, "testnet-v0.92.9");
}

fn statement_export(export_id: u64) -> StatementExport {
    StatementExport {
        export_id,
        status: StatementExportStatus::STATEMENT_EXPORT_STATUS_QUEUED.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn get_statement_export_refuses_another_export() {
    let reply = |id| GetStatementExportResponse {
        export: statement_export(id).into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply(78), &reply(77)).await;
    let id = format_id(77);
    assert_contract(
        client.statements.get_export(&id).await,
        "GetStatementExport",
    );
    let read = client.statements.get_export(&id).await.unwrap();
    assert_eq!(read.export_id, id);
}

#[tokio::test]
async fn cancel_statement_export_refuses_another_export() {
    let reply = |id| CancelStatementExportResponse {
        export: statement_export(id).into(),
        ..Default::default()
    };
    let client = wrong_then_right(&reply(78), &reply(77)).await;
    let id = format_id(77);
    assert_contract(
        client.statements.cancel_export(&id).await,
        "CancelStatementExport",
    );
    let read = client.statements.cancel_export(&id).await.unwrap();
    assert_eq!(read.export_id, id);
}
