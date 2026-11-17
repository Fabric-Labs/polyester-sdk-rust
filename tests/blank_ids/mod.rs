//! A supplied ID is an ID. `None` (or an unset field) is the only way to
//! leave one out: an empty or blank string, or one with whitespace around it,
//! is refused with `Error::Validation` and nothing is sent. It never falls
//! back to the public feed, the main account, no filter or the configured
//! default.

use super::hardening_support::{HttpScript, MockHttpServer, MockWsServer};
use polyester::codecs::scalars::format_id;
use polyester::models::{
    BatchCreateInternalTransfersParams, CreateInternalTransferParams, InternalTransferBatchLeg,
    ListOpenOrdersOpts, ListOrderHistoryOpts, SubAccountScope,
};
use polyester::types::{AssetAmount, QuantityDomain};
use polyester::{Client, Config, Error};
use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::Duration;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// Supplied IDs that are no ID: empty, blank, and padded.
const BLANK: [&str; 6] = ["", " ", "\t", "\u{00A0}", " 23", "23 "];

async fn server() -> MockHttpServer {
    MockHttpServer::spawn(|_| HttpScript::NotFound).await
}

/// A WebSocket server that accepts and never answers: a subscription that
/// connects to it waits, and its `connects` counts the attempt.
async fn ws_server() -> MockWsServer {
    MockWsServer::spawn_hang_after_accept().await
}

fn client(http: &MockHttpServer, config: Config) -> Client {
    Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..config
    })
    .expect("client")
}

/// The call must be refused with `Error::Validation` at once: a call that
/// connects, waits or answers anything else fell back to something else.
async fn assert_refused<T, F>(label: &str, call: F)
where
    F: Future<Output = polyester::Result<T>>,
{
    match tokio::time::timeout(Duration::from_secs(2), call).await {
        Ok(Err(Error::Validation(_))) => {}
        Ok(Err(other)) => panic!("{label}: want Error::Validation, got {other:?}"),
        Ok(Ok(_)) => panic!("{label}: want Error::Validation, the call went ahead"),
        Err(_) => panic!("{label}: want Error::Validation, the call went ahead and waited"),
    }
}

/// `None` is no ID: the call goes ahead (here it fails or waits on the local
/// server, but it is no validation error).
async fn assert_not_refused<T, F>(label: &str, call: F)
where
    F: Future<Output = polyester::Result<T>>,
{
    if let Ok(Err(Error::Validation(message))) =
        tokio::time::timeout(Duration::from_millis(300), call).await
    {
        panic!("{label}: None was refused: {message}");
    }
}

/// `lifecycle.subscribe_open_flows` with a supplied account subscribes to
/// that account's channel; a blank one is refused, never the public feed.
#[tokio::test]
async fn open_flow_subscription_refuses_a_blank_account() {
    let http = server().await;
    let ws = ws_server().await;
    let c = client(
        &http,
        Config {
            ws_url: ws.ws_url(),
            ..Default::default()
        },
    );
    for id in BLANK {
        assert_refused(
            &format!("subscribe_open_flows({id:?})"),
            c.lifecycle.subscribe_open_flows(Some(id)),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    assert_eq!(ws.connects.load(Ordering::SeqCst), 0, "nothing connected");
    assert_not_refused(
        "subscribe_open_flows(None)",
        c.lifecycle.subscribe_open_flows(None),
    )
    .await;
}

/// An explicit account on the private subscriptions is used as given; a
/// blank one is refused, never replaced by the configured default account.
#[tokio::test]
async fn private_subscriptions_refuse_a_blank_account_instead_of_the_default() {
    let http = server().await;
    let ws = ws_server().await;
    let c = client(
        &http,
        Config {
            ws_url: ws.ws_url(),
            default_account_id: Some(format_id(5)),
            ..Default::default()
        },
    );
    for id in BLANK {
        assert_refused(
            &format!("orders.subscribe({id:?})"),
            c.orders.subscribe(Some(id)),
        )
        .await;
        assert_refused(
            &format!("trades.subscribe({id:?})"),
            c.trades.subscribe(Some(id)),
        )
        .await;
        assert_refused(
            &format!("transfers.subscribe({id:?})"),
            c.transfers.subscribe(Some(id)),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    assert_eq!(ws.connects.load(Ordering::SeqCst), 0, "nothing connected");
    assert_not_refused("orders.subscribe(None)", c.orders.subscribe(None)).await;
}

/// A configured default account or sub-account that is blank is refused
/// when it is used, never read as no default.
#[tokio::test]
async fn a_blank_configured_default_is_refused_not_read_as_absent() {
    let http = server().await;
    let ws = ws_server().await;
    for id in BLANK {
        let c = client(
            &http,
            Config {
                ws_url: ws.ws_url(),
                default_sub_account_id: Some(id.into()),
                default_account_id: Some(id.into()),
                ..Default::default()
            },
        );
        assert_refused(
            &format!("default sub-account {id:?}: orders.list_open"),
            c.orders.list_open(SubAccountScope::Default),
        )
        .await;
        assert_refused(
            &format!("default account {id:?}: orders.subscribe"),
            c.orders.subscribe(None),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    assert_eq!(ws.connects.load(Ordering::SeqCst), 0, "nothing connected");
    // Unset defaults: the main account is read.
    let c = client(&http, Config::default());
    let _ = c.orders.list_open(SubAccountScope::Default).await;
    assert_eq!(
        http.requests.load(Ordering::SeqCst),
        1,
        "None is no default"
    );
}

/// The order reads' trigger filter: a blank trigger ID is refused, never read
/// as no filter.
#[tokio::test]
async fn order_reads_refuse_a_blank_trigger_filter() {
    let http = server().await;
    let c = client(&http, Config::default());
    for id in BLANK {
        assert_refused(
            &format!("list_open_with trigger_id {id:?}"),
            c.orders.list_open_with(ListOpenOrdersOpts {
                trigger_id: Some(id.into()),
                ..Default::default()
            }),
        )
        .await;
        assert_refused(
            &format!("list_history_with trigger_id {id:?}"),
            c.orders.list_history_with(ListOrderHistoryOpts {
                trigger_id: Some(id.into()),
                ..Default::default()
            }),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    let _ = c.orders.list_open_with(ListOpenOrdersOpts::default()).await;
    assert_eq!(http.requests.load(Ordering::SeqCst), 1, "None is no filter");
}

/// An internal transfer's destination account or sub-account, once
/// supplied, is that destination: a blank one is refused, never skipped for
/// the other destination given with it.
#[tokio::test]
async fn internal_transfers_refuse_a_blank_destination_instead_of_another() {
    let http = server().await;
    let c = client(&http, Config::default());
    let amount =
        || AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
    let valid = Some(format_id(9));
    for id in BLANK {
        for (account, sub) in [
            (Some(id.to_owned()), valid.clone()),
            (valid.clone(), Some(id.to_owned())),
            (Some(id.to_owned()), None),
            (None, Some(id.to_owned())),
        ] {
            let label = format!("account {account:?} sub {sub:?}");
            assert_refused(
                &format!("internal_transfers.create {label}"),
                c.internal_transfers.create(CreateInternalTransferParams {
                    asset_id: 7,
                    quantity: amount(),
                    idempotency_key: "blank-destination".into(),
                    subaccount_id: SubAccountScope::Main,
                    destination_account_id: account.clone(),
                    destination_subaccount_id: sub.clone(),
                    destination_smart_account_address: None,
                    quantity_scale: Some(18),
                }),
            )
            .await;
            assert_refused(
                &format!("internal_transfers.batch_create {label}"),
                c.internal_transfers
                    .batch_create(BatchCreateInternalTransfersParams {
                        legs: vec![InternalTransferBatchLeg {
                            asset_id: 7,
                            quantity: amount(),
                            quantity_scale: Some(18),
                            subaccount_id: SubAccountScope::Main,
                            destination_account_id: account,
                            destination_subaccount_id: sub,
                            destination_smart_account_address: None,
                        }],
                        idempotency_key: "blank-destination".into(),
                    }),
            )
            .await;
        }
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
}

/// A flow ID is `flow_` and base58, as the contract's pattern has it: a
/// blank or padded one is refused before sending.
#[tokio::test]
async fn flow_lookups_refuse_a_blank_or_padded_flow_id() {
    let http = server().await;
    let c = client(&http, Config::default());
    for id in BLANK
        .into_iter()
        .chain([" flow_2j", "flow_2j ", "flow_2 j"])
    {
        assert_refused(
            &format!("lifecycle.get_flow({id:?})"),
            c.lifecycle.get_flow(id),
        )
        .await;
        assert_refused(
            &format!("lifecycle.get_account_flow({id:?})"),
            c.lifecycle.get_account_flow(id, SubAccountScope::Main),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    let _ = c.lifecycle.get_flow("flow_2j").await;
    assert_eq!(http.requests.load(Ordering::SeqCst), 1, "a flow ID is sent");
}

/// The lookups that take a public ID refuse a blank one before sending.
#[tokio::test]
async fn id_lookups_refuse_a_blank_id() {
    let http = server().await;
    let c = client(&http, Config::default());
    for id in BLANK {
        assert_refused(
            &format!("orders.get({id:?})"),
            c.orders.get(
                polyester::models::OrderKey::OrderId(id.into()),
                SubAccountScope::Main,
            ),
        )
        .await;
        assert_refused(
            &format!("triggers.get_by_id({id:?})"),
            c.triggers.get_by_id(id, SubAccountScope::Main),
        )
        .await;
        assert_refused(
            &format!("triggers.cancel_oco_by_id({id:?})"),
            c.triggers.cancel_oco_by_id(id, SubAccountScope::Main),
        )
        .await;
        assert_refused(&format!("alerts.cancel({id:?})"), c.alerts.cancel(id)).await;
        assert_refused(
            &format!("sub_accounts.get({id:?})"),
            c.sub_accounts.get(id, Default::default()),
        )
        .await;
        assert_refused(
            &format!("statements.get_export({id:?})"),
            c.statements.get_export(id),
        )
        .await;
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0, "nothing was sent");
}

/// `lifecycle.subscribe_flow_detail` reads its flow ID by the contract's
/// rule, as the flow reads do: a blank or padded one is refused and never
/// subscribes.
#[tokio::test]
async fn flow_detail_subscription_refuses_a_blank_or_padded_flow_id() {
    let http = server().await;
    let ws = ws_server().await;
    let c = client(
        &http,
        Config {
            ws_url: ws.ws_url(),
            ..Default::default()
        },
    );
    for id in BLANK
        .into_iter()
        .chain([" flow_2j", "flow_2j ", "flow_2 j"])
    {
        assert_refused(
            &format!("subscribe_flow_detail({id:?})"),
            c.lifecycle.subscribe_flow_detail(id),
        )
        .await;
    }
    assert_eq!(ws.connects.load(Ordering::SeqCst), 0, "nothing connected");
    assert_not_refused(
        "subscribe_flow_detail(\"flow_2j\")",
        c.lifecycle.subscribe_flow_detail("flow_2j"),
    )
    .await;
}
