//! Page sizes against the contract's `limit` rules, on a local server.
//!
//! A page size the contract refuses (above its `lte`, or an explicit 0 where
//! the optional `limit` is `gte: 1`) is an `Error::Validation` before
//! anything is sent; the bounds themselves are sent unchanged, and an unset
//! page size is left out for the server's default.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{
    GetCandlesOpts, GetOrderOpts, GetTradesOpts, ListOpenOrdersOpts, ListOrderHistoryOpts,
    ListTriggerEventsOpts, ListTriggersOpts, ListUserTradesOpts, OrderKey, SubAccountScope,
};
use polyester::proto::auth::v1::{
    GetAddressBookViewRequest, ListAddressBookEntriesRequest,
    ListInternalTransferWhitelistEntriesRequest, ListSubaccountEventsRequest,
    ListTransferCounterpartiesRequest, ListTransferDestinationsRequest,
};
use polyester::proto::chain::lifecycle::v1::{ListFlowsByTxRequest, ListFlowsRequest};
use polyester::proto::ledger::read::v1::{ListTransfersRequest, ListTransfersResponse};
use polyester::proto::orders::v1::{
    GetOpenOrdersRequest, GetOpenOrdersResponse, GetOrderHistoryRequest, GetOrderHistoryResponse,
    GetOrderRequest, GetOrderResponse, GetUserTradesRequest, GetUserTradesResponse,
};
use polyester::proto::triggers::v1::{
    ListTriggerEventsRequest, ListTriggerEventsResponse, ListTriggersRequest, ListTriggersResponse,
};
use polyester::services::{
    ListChangelogChangesOptions, ListChangelogDeploymentGroupsOptions, ListChangelogPostsOptions,
    ListMarketOverviewOptions, ListStatusEventsOptions,
};
use polyester::{Client, Config, Error};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The `limit` of each request the server received, by method path.
type Sent = Arc<Mutex<Vec<(String, Option<u32>)>>>;

async fn server() -> (Client, MockHttpServer, Sent) {
    let sent: Sent = Arc::new(Mutex::new(Vec::new()));
    let log = sent.clone();
    let http = MockHttpServer::spawn(move |req| {
        let path = req.path.as_str();
        let (limit, answer) = match path.rsplit('/').next().unwrap_or_default() {
            "GetOpenOrders" => (
                GetOpenOrdersRequest::decode_from_slice(&req.body)
                    .unwrap()
                    .limit,
                connect_proto_ok(&GetOpenOrdersResponse::default()),
            ),
            "GetOrderHistory" => (
                GetOrderHistoryRequest::decode_from_slice(&req.body)
                    .unwrap()
                    .limit,
                connect_proto_ok(&GetOrderHistoryResponse::default()),
            ),
            "GetUserTrades" => (
                GetUserTradesRequest::decode_from_slice(&req.body)
                    .unwrap()
                    .limit,
                connect_proto_ok(&GetUserTradesResponse::default()),
            ),
            "GetOrder" => (
                GetOrderRequest::decode_from_slice(&req.body).unwrap().limit,
                connect_proto_ok(&GetOrderResponse::default()),
            ),
            "ListTransfers" => (
                Some(
                    ListTransfersRequest::decode_from_slice(&req.body)
                        .unwrap()
                        .limit,
                ),
                connect_proto_ok(&ListTransfersResponse::default()),
            ),
            "ListTriggers" => (
                Some(
                    ListTriggersRequest::decode_from_slice(&req.body)
                        .unwrap()
                        .limit,
                ),
                connect_proto_ok(&ListTriggersResponse::default()),
            ),
            "ListTriggerEvents" => (
                Some(
                    ListTriggerEventsRequest::decode_from_slice(&req.body)
                        .unwrap()
                        .limit,
                ),
                connect_proto_ok(&ListTriggerEventsResponse::default()),
            ),
            _ => (None, HttpScript::NotFound),
        };
        log.lock().unwrap().push((path.to_owned(), limit));
        answer
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    (client, http, sent)
}

fn get_order(limit: Option<u32>) -> GetOrderOpts {
    GetOrderOpts {
        key: OrderKey::OrderId("2".into()),
        subaccount_id: SubAccountScope::Default,
        include_attached_risk: false,
        include_attached_risk_state: false,
        include_execution_history: None,
        limit,
        page_token: None,
    }
}

fn assert_refused(label: &str, result: polyester::Result<impl std::fmt::Debug>) {
    match result {
        Err(Error::Validation(message)) => {
            assert!(message.contains("limit"), "{label}: {message}")
        }
        other => panic!("{label}: expected a limit validation error, got {other:?}"),
    }
}

/// Every list call refuses a page size above its contract maximum, and the
/// order reads an explicit 0, before anything is sent.
#[tokio::test]
async fn page_sizes_the_contract_refuses_are_never_sent() {
    let (c, http, _) = server().await;
    for limit in [0, 1001, 5000] {
        assert_refused(
            &format!("list_open {limit}"),
            c.orders
                .list_open_with(ListOpenOrdersOpts {
                    limit: Some(limit),
                    ..Default::default()
                })
                .await,
        );
        assert_refused(
            &format!("list_history {limit}"),
            c.orders
                .list_history_with(ListOrderHistoryOpts {
                    limit: Some(limit),
                    ..Default::default()
                })
                .await,
        );
        assert_refused(
            &format!("orders.get {limit}"),
            c.orders.get_with(get_order(Some(limit))).await,
        );
        assert_refused(
            &format!("trades.list {limit}"),
            c.trades
                .list_with(ListUserTradesOpts {
                    limit: Some(limit),
                    ..Default::default()
                })
                .await,
        );
    }
    for limit in [1001, 5000] {
        let transfers = || ListTransfersRequest {
            limit,
            ..Default::default()
        };
        assert_refused("transfers.list", c.transfers.list(transfers()).await);
        assert_refused(
            "balances.list_transfers",
            c.balances.list_transfers(transfers()).await,
        );
        assert_refused(
            "triggers.list",
            c.triggers
                .list(ListTriggersRequest {
                    limit,
                    ..Default::default()
                })
                .await,
        );
        assert_refused(
            "triggers.list_with",
            c.triggers
                .list_with(ListTriggersOpts {
                    limit: Some(limit),
                    ..Default::default()
                })
                .await,
        );
        assert_refused(
            "triggers.list_events",
            c.triggers
                .list_events(ListTriggerEventsRequest {
                    trigger_id: 1,
                    limit,
                    ..Default::default()
                })
                .await,
        );
        assert_refused(
            "triggers.list_events_by_id",
            c.triggers
                .list_events_by_id(
                    "2",
                    ListTriggerEventsOpts {
                        limit: Some(limit),
                        ..Default::default()
                    },
                )
                .await,
        );
        assert_refused(
            "market_data.get_trades_with",
            c.market_data
                .get_trades_with(GetTradesOpts {
                    symbol_id: Some(1),
                    limit: Some(limit),
                    ..Default::default()
                })
                .await,
        );
    }
    let candles = || GetCandlesOpts {
        symbol_id: Some(1),
        timeframe: "1m".into(),
        limit: Some(10_001),
        ..Default::default()
    };
    assert_refused(
        "market_data.get_candles_with",
        c.market_data.get_candles_with(candles()).await,
    );
    assert_refused(
        "market_data.get_candles_columns",
        c.market_data.get_candles_columns(candles()).await,
    );
    assert_refused(
        "market_overview.list",
        c.market_overview
            .list(ListMarketOverviewOptions {
                limit: Some(2001),
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "heatmap.get",
        c.heatmap.get("BTC-USDT", "1s", 50, 20_001, "close").await,
    );
    assert_refused(
        "sub_accounts.list_activity",
        c.sub_accounts
            .list_activity(ListSubaccountEventsRequest {
                subaccount_id: 2,
                limit: 201,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "address_book.list_entries",
        c.address_book
            .list_entries(ListAddressBookEntriesRequest {
                limit: 501,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "address_book.list_transfer_counterparties",
        c.address_book
            .list_transfer_counterparties(ListTransferCounterpartiesRequest {
                limit: 201,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "address_book.list_transfer_destinations",
        c.address_book
            .list_transfer_destinations(ListTransferDestinationsRequest {
                limit: 501,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "address_book.list_internal_transfer_whitelist_entries",
        c.address_book
            .list_internal_transfer_whitelist_entries(ListInternalTransferWhitelistEntriesRequest {
                limit: 501,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "address_book.get_view",
        c.address_book
            .get_view(GetAddressBookViewRequest {
                limit: 201,
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "lifecycle.list_flows",
        c.lifecycle
            .list_flows(ListFlowsRequest {
                limit: 501,
                ..Default::default()
            })
            .await,
    );
    let by_tx = || ListFlowsByTxRequest {
        tx_hash: format!("0x{}", "ab".repeat(32)),
        limit: 501,
        ..Default::default()
    };
    assert_refused(
        "lifecycle.list_flows_by_tx",
        c.lifecycle.list_flows_by_tx(by_tx()).await,
    );
    assert_refused(
        "lifecycle.get_flow_by_tx",
        c.lifecycle.get_flow_by_tx(by_tx()).await,
    );
    assert_refused(
        "list_status_events",
        c.list_status_events(ListStatusEventsOptions {
            limit: Some(201),
            ..Default::default()
        })
        .await,
    );
    assert_refused(
        "changelog.list_posts",
        c.changelog
            .list_posts(ListChangelogPostsOptions {
                limit: Some(101),
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "changelog.list_deployment_groups",
        c.changelog
            .list_deployment_groups(ListChangelogDeploymentGroupsOptions {
                limit: Some(101),
                ..Default::default()
            })
            .await,
    );
    assert_refused(
        "changelog.list_changes",
        c.changelog
            .list_changes(ListChangelogChangesOptions {
                limit: Some(501),
                ..Default::default()
            })
            .await,
    );
    assert_eq!(
        http.requests.load(Ordering::SeqCst),
        0,
        "a refused page size must send nothing"
    );
}

/// 1 and the contract maximum are sent unchanged; an unset page size is left
/// out (the order reads) or sent as 0 (the plain `uint32` limits), and the
/// server applies its default.
#[tokio::test]
async fn page_sizes_within_the_contract_are_sent_unchanged() {
    let (c, _http, sent) = server().await;
    for limit in [None, Some(1), Some(1000)] {
        c.orders
            .list_open_with(ListOpenOrdersOpts {
                limit,
                ..Default::default()
            })
            .await
            .unwrap();
        c.orders
            .list_history_with(ListOrderHistoryOpts {
                limit,
                ..Default::default()
            })
            .await
            .unwrap();
        let _ = c.orders.get_with(get_order(limit)).await;
        c.trades
            .list_with(ListUserTradesOpts {
                limit,
                ..Default::default()
            })
            .await
            .unwrap();
        let plain = limit.unwrap_or(0);
        c.transfers
            .list(ListTransfersRequest {
                limit: plain,
                ..Default::default()
            })
            .await
            .unwrap();
        c.triggers
            .list_with(ListTriggersOpts {
                limit,
                ..Default::default()
            })
            .await
            .unwrap();
        c.triggers
            .list_events_by_id(
                "2",
                ListTriggerEventsOpts {
                    limit,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut sent = std::mem::take(&mut *sent.lock().unwrap());
        sent.sort();
        let want_plain = Some(plain);
        let mut want = vec![
            (
                "/ledger.read.v1.LedgerReadService/ListTransfers".to_owned(),
                want_plain,
            ),
            (
                "/orders.v1.OrdersReadService/GetOpenOrders".to_owned(),
                limit,
            ),
            ("/orders.v1.OrdersReadService/GetOrder".to_owned(), limit),
            (
                "/orders.v1.OrdersReadService/GetOrderHistory".to_owned(),
                limit,
            ),
            (
                "/orders.v1.OrdersReadService/GetUserTrades".to_owned(),
                limit,
            ),
            (
                "/triggers.v1.TriggersService/ListTriggerEvents".to_owned(),
                want_plain,
            ),
            (
                "/triggers.v1.TriggersService/ListTriggers".to_owned(),
                want_plain,
            ),
        ];
        want.sort();
        assert_eq!(sent, want, "limit {limit:?}");
    }
}
