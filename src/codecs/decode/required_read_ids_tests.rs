//! Every read reply that carries an object's required ID refuses 0 (or an
//! empty string ID) with `Error::ResponseContract`; the same reply with a real
//! ID decodes. A nested message the reply may leave out (an order's lineage,
//! the caller's session, a sub-account's policy, a destination's saved entry)
//! is checked only when present.

use super::*;
use crate::errors::{Error, Result};
use crate::proto::alerts::v1::{ListAlertsResponse, PriceAlert};
use crate::proto::auth::v1::{
    AddressBookEntriesView, AddressBookEntry, AddressBookTag, ApiKey,
    CreateAddressBookEntryResponse, ExternalAddressBookEntry, GetAddressBookViewResponse,
    GetApiKeyResponse, GetSubaccountResponse, InternalAddressBookEntry,
    InternalTransferWhitelistEntry, ListAddressBookEntriesResponse, ListApiKeysResponse,
    ListInternalTransferWhitelistEntriesResponse, ListSubaccountInvitesResponse,
    ListSubaccountsResponse, ListTransferCounterpartiesResponse, ListTransferDestinationsResponse,
    MeResponse, SessionInfo, Subaccount, SubaccountInvite, SubaccountPolicyView,
    TransferCounterparty, TransferDestination,
};
use crate::proto::chain::analytics::v1::GetZippedAssetSupplyGroupResponse;
use crate::proto::chain::lifecycle::v1::{
    FlowDetailView, FlowSummaryView, FlowTxMatchView, GetFlowResponse, ListFlowsByTxResponse,
    ListFlowsResponse,
};
use crate::proto::changelog::v1::{
    Change, ChangeEntry, DeploymentGroup, DeploymentGroupAttention, DeploymentGroupSummary,
    GetDeploymentGroupResponse, GetPostResponse, ListChangesResponse, Post, PostSummary, Section,
};
use crate::proto::ledger::read::v1::{ListTransfersResponse, TransferRow};
use crate::proto::orders::v1::{
    GetOpenOrdersResponse, GetOrderHistoryResponse, GetOrderResponse, GetUserTradesResponse, Order,
    OrderLineage, UserTrade,
};
use crate::proto::system::v1::{ListStatusEventsResponse, StatusEvent};
use crate::proto::triggers::v1::{
    GetTriggerResponse, ListTriggerEventsResponse, ListTriggersResponse, Trigger, TriggerEvent,
};

fn check<T: std::fmt::Debug>(name: &str, zero: Result<T>, valid: Option<Result<T>>) {
    match zero {
        Err(Error::ResponseContract { .. }) => {}
        other => panic!("{name} with a zero ID: want Error::ResponseContract, got {other:?}"),
    }
    if let Some(Err(err)) = valid {
        panic!("{name} with a real ID: {err:?}");
    }
}

fn lineage(id: u64) -> buffa::MessageField<OrderLineage> {
    OrderLineage {
        id,
        generation: 1,
        ..Default::default()
    }
    .into()
}

fn order(order_id: u64, lineage_id: u64) -> Order {
    Order {
        order_id,
        lineage: lineage(lineage_id),
        ..Default::default()
    }
}

fn trade(lineage_id: u64) -> UserTrade {
    UserTrade {
        order_id: 9,
        lineage: lineage(lineage_id),
        ..Default::default()
    }
}

fn entry(address_book_entry_id: u64, tag_id: u64) -> AddressBookEntry {
    AddressBookEntry {
        address_book_entry_id,
        tags: vec![AddressBookTag {
            tag_id,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn invite(id: u64, subaccount_id: u64) -> SubaccountInvite {
    SubaccountInvite {
        id,
        subaccount_id,
        ..Default::default()
    }
}

fn subaccount(
    id: u64,
    key_id: &str,
    inv: SubaccountInvite,
    policy_id: u64,
) -> GetSubaccountResponse {
    GetSubaccountResponse {
        subaccount: Subaccount {
            id,
            ..Default::default()
        }
        .into(),
        api_keys: vec![ApiKey {
            key_id: key_id.into(),
            ..Default::default()
        }],
        invites: vec![inv],
        policy: SubaccountPolicyView {
            id: policy_id,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn post(change_id: u64) -> GetPostResponse {
    GetPostResponse {
        post: Post {
            summary: PostSummary::default().into(),
            sections: vec![Section {
                changes: vec![Change {
                    id: change_id,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn group(change_id: u64) -> GetDeploymentGroupResponse {
    GetDeploymentGroupResponse {
        group: DeploymentGroup {
            summary: DeploymentGroupSummary::default().into(),
            attention: vec![DeploymentGroupAttention {
                change: Change {
                    id: change_id,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn flow(flow_id: &str) -> GetFlowResponse {
    GetFlowResponse {
        flow: FlowDetailView {
            summary: FlowSummaryView {
                flow_id: flow_id.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn no_symbol(_: u32) -> String {
    String::new()
}

#[test]
fn trigger_reads_refuse_a_zero_trigger_id() {
    let get = |trigger_id| {
        get_trigger_from_proto(&GetTriggerResponse {
            trigger: Trigger {
                trigger_id,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    };
    check("GetTrigger", get(0), Some(get(7)));
    let list = |trigger_id| {
        triggers_list_from_proto(&ListTriggersResponse {
            triggers: vec![Trigger {
                trigger_id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListTriggers", list(0), Some(list(7)));
    let events = |trigger_id| {
        trigger_events_list_from_proto(&ListTriggerEventsResponse {
            events: vec![TriggerEvent {
                trigger_id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListTriggerEvents", events(0), Some(events(7)));
    // A get that finds nothing still reads as None.
    assert!(
        get_trigger_from_proto(&GetTriggerResponse::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn order_reads_refuse_a_zero_order_or_lineage_id() {
    let get = |order_id, lineage_id, trade_lineage_id| {
        get_order_from_proto(&GetOrderResponse {
            order: order(order_id, lineage_id).into(),
            trades: vec![trade(trade_lineage_id)],
            ..Default::default()
        })
    };
    check("GetOrder order_id", get(0, 3, 3), Some(get(9, 3, 3)));
    check("GetOrder lineage.id", get(9, 0, 3), None);
    check("GetOrder trade lineage.id", get(9, 3, 0), None);
    let open = |order_id, lineage_id| {
        orders_list_from_open(&GetOpenOrdersResponse {
            orders: vec![order(order_id, lineage_id)],
            ..Default::default()
        })
    };
    check("GetOpenOrders order_id", open(0, 3), Some(open(9, 3)));
    check("GetOpenOrders lineage.id", open(9, 0), None);
    let history = |order_id, lineage_id| {
        orders_list_from_history(&GetOrderHistoryResponse {
            orders: vec![order(order_id, lineage_id)],
            ..Default::default()
        })
    };
    check(
        "GetOrderHistory order_id",
        history(0, 3),
        Some(history(9, 3)),
    );
    check("GetOrderHistory lineage.id", history(9, 0), None);
    let trades = |lineage_id| {
        user_trades_list_from_proto(&GetUserTradesResponse {
            trades: vec![trade(lineage_id), UserTrade::default()],
            ..Default::default()
        })
    };
    check("GetUserTrades lineage.id", trades(0), Some(trades(3)));
    assert!(
        get_order_from_proto(&GetOrderResponse::default())
            .unwrap()
            .order
            .is_none()
    );
}

#[test]
fn alert_and_address_book_reads_refuse_a_zero_id() {
    let alerts = |alert_id| {
        price_alerts_page_from_proto(
            &ListAlertsResponse {
                alerts: vec![PriceAlert {
                    alert_id,
                    ..Default::default()
                }],
                ..Default::default()
            },
            no_symbol,
        )
    };
    check("ListAlerts", alerts(0), Some(alerts(7)));
    let entries = |entry_id, tag_id| {
        list_entries_from_proto(&ListAddressBookEntriesResponse {
            entries: vec![entry(entry_id, tag_id)],
            ..Default::default()
        })
    };
    check(
        "ListAddressBookEntries entry",
        entries(0, 5),
        Some(entries(4, 5)),
    );
    check("ListAddressBookEntries tag", entries(4, 0), None);
    let created = |tag_id| {
        entry_from_create_proto(&CreateAddressBookEntryResponse {
            entry: entry(4, tag_id).into(),
            ..Default::default()
        })
    };
    check("CreateAddressBookEntry tag", created(0), Some(created(5)));
    let destinations = |entry_id, tag_id| {
        list_destinations_from_proto(&ListTransferDestinationsResponse {
            destinations: vec![
                TransferDestination {
                    address_book_entry: entry(entry_id, tag_id).into(),
                    ..Default::default()
                },
                TransferDestination::default(),
            ],
            ..Default::default()
        })
    };
    check(
        "ListTransferDestinations entry",
        destinations(0, 5),
        Some(destinations(4, 5)),
    );
    check("ListTransferDestinations tag", destinations(4, 0), None);
    let view = |external_id, internal_id| {
        address_book_view_from_proto(&GetAddressBookViewResponse {
            entries: AddressBookEntriesView {
                external: vec![ExternalAddressBookEntry {
                    address_book_entry_id: external_id,
                    ..Default::default()
                }],
                internal: vec![InternalAddressBookEntry {
                    address_book_entry_id: internal_id,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    };
    check("GetAddressBookView external", view(0, 5), Some(view(4, 5)));
    check("GetAddressBookView internal", view(4, 0), None);
    let whitelist = |entry_id| {
        list_internal_whitelist_from_proto(&ListInternalTransferWhitelistEntriesResponse {
            entries: vec![InternalTransferWhitelistEntry {
                entry_id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check(
        "ListInternalTransferWhitelistEntries",
        whitelist(0),
        Some(whitelist(4)),
    );
    let counterparties = |counterparty_id| {
        list_counterparties_from_proto(&ListTransferCounterpartiesResponse {
            counterparties: vec![TransferCounterparty {
                counterparty_id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check(
        "ListTransferCounterparties",
        counterparties(0),
        Some(counterparties(4)),
    );
}

#[test]
fn account_reads_refuse_a_zero_id() {
    let key = |key_id: &str| {
        api_key_from_get_proto(&GetApiKeyResponse {
            api_key: ApiKey {
                key_id: key_id.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    };
    check("GetApiKey", key(""), Some(key("ak_1")));
    assert!(
        api_key_from_get_proto(&GetApiKeyResponse::default())
            .unwrap()
            .is_none()
    );
    let keys = |key_id: &str| {
        api_keys_list_from_proto(&ListApiKeysResponse {
            api_keys: vec![ApiKey {
                key_id: key_id.into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListApiKeys", keys(""), Some(keys("ak_1")));
    let me = |account_id, session: Option<&str>| {
        me_from_proto(&MeResponse {
            account_id,
            session: session
                .map(|id| {
                    SessionInfo {
                        session_id: id.into(),
                        ..Default::default()
                    }
                    .into()
                })
                .unwrap_or_default(),
            ..Default::default()
        })
    };
    check("Me account_id", me(0, None), Some(me(4, None)));
    check(
        "Me session.session_id",
        me(4, Some("")),
        Some(me(4, Some("s1"))),
    );
    check(
        "GetSubaccount id",
        get_subaccount_from_proto(&subaccount(0, "ak_1", invite(5, 6), 7)),
        Some(get_subaccount_from_proto(&subaccount(
            4,
            "ak_1",
            invite(5, 6),
            7,
        ))),
    );
    check(
        "GetSubaccount api key",
        get_subaccount_from_proto(&subaccount(4, "", invite(5, 6), 7)),
        None,
    );
    check(
        "GetSubaccount invite id",
        get_subaccount_from_proto(&subaccount(4, "ak_1", invite(0, 6), 7)),
        None,
    );
    check(
        "GetSubaccount invite subaccount_id",
        get_subaccount_from_proto(&subaccount(4, "ak_1", invite(5, 0), 7)),
        None,
    );
    check(
        "GetSubaccount policy",
        get_subaccount_from_proto(&subaccount(4, "ak_1", invite(5, 6), 0)),
        None,
    );
    let subaccounts = |id| {
        subaccounts_list_from_proto(&ListSubaccountsResponse {
            subaccounts: vec![Subaccount {
                id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListSubaccounts", subaccounts(0), Some(subaccounts(4)));
    let invites = |inv| {
        subaccount_invites_list_from_proto(&ListSubaccountInvitesResponse {
            invites: vec![inv],
            ..Default::default()
        })
    };
    check(
        "ListSubaccountInvites id",
        invites(invite(0, 6)),
        Some(invites(invite(5, 6))),
    );
    check(
        "ListSubaccountInvites subaccount_id",
        invites(invite(5, 0)),
        None,
    );
}

#[test]
fn flow_changelog_transfer_and_status_reads_refuse_a_zero_id() {
    let group_reply = |group_id: &str| {
        zipped_asset_supply_group_from_proto(&GetZippedAssetSupplyGroupResponse {
            group_id: group_id.into(),
            ..Default::default()
        })
    };
    check(
        "GetZippedAssetSupplyGroup",
        group_reply(""),
        Some(group_reply("g1")),
    );
    check(
        "GetFlowById",
        flow_from_get_response(&flow("")),
        Some(flow_from_get_response(&flow("f1"))),
    );
    let flows = |flow_id: &str| {
        flows_list_from_proto(&ListFlowsResponse {
            flows: vec![FlowSummaryView {
                flow_id: flow_id.into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListFlows", flows(""), Some(flows("f1")));
    let by_tx = |flow_id: &str| {
        flows_by_tx_list_from_proto(&ListFlowsByTxResponse {
            matches: vec![FlowTxMatchView {
                flow_id: flow_id.into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListFlowsByTx", by_tx(""), Some(by_tx("f1")));
    let changes = |id| {
        changelog_changes_page_from_proto(&ListChangesResponse {
            changes: vec![ChangeEntry {
                change: Change {
                    id,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListChanges", changes(0), Some(changes(7)));
    check(
        "GetPost",
        changelog_post_from_proto(&post(0)),
        Some(changelog_post_from_proto(&post(7))),
    );
    check(
        "GetDeploymentGroup",
        changelog_deployment_group_from_proto(&group(0)),
        Some(changelog_deployment_group_from_proto(&group(7))),
    );
    let transfers = |transfer_id: &str| {
        transfers_list_from_proto(&ListTransfersResponse {
            transfers: vec![TransferRow {
                transfer_id: transfer_id.into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListTransfers", transfers(""), Some(transfers("7")));
    let events = |id| {
        status_events_page_from_proto(&ListStatusEventsResponse {
            events: vec![StatusEvent {
                id,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    check("ListStatusEvents", events(0), Some(events(7)));
}
