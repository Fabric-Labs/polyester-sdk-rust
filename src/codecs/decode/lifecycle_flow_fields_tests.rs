//! Every lifecycle flow read and flow update keeps what the contract's
//! `chain.lifecycle.v1.FlowSummaryView` says moved and when: the amount, the
//! asset, the transactions, the route, the times and the progress. A
//! transaction lookup keeps the `FlowTxMatchView` subset.

use super::*;
use crate::errors::Error;
use crate::models::{LifecycleFlowSummary, LifecycleFlowTimelineItem};
use crate::proto::chain::lifecycle::v1::{
    AssetIds, FlowDetailView, FlowDomain, FlowKind, FlowSummaryView, FlowTimelineItemView,
    FlowTimelineStatus, FlowTxMatchView, GetAccountFlowResponse, GetFlowResponse,
    ListAccountFlowsResponse, ListFlowsByTxResponse, ListFlowsResponse, RequestFee,
    RequestFeeStatus,
};
use crate::proto::polyester::r#type::v1::U128;
use crate::types::{AssetAmount, QuantityDomain};
use buffa::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A flow summary in the shape the API's JSON answer has: an open withdrawal
/// with its amount, fee, transactions, route, times, current progress and
/// timeline.
const OPEN_WITHDRAW_JSON: &str = r#"{
 "smartAccountAddress": "0x00000000000000000000000000000000000000a1",
 "flowId": "flow_UZBpp6Kx7uX",
 "flowKind": "KIND_WITHDRAW",
 "currentStep": "FLOW_STEP_VALIDATION",
 "assetIds": {"zippedAssetId": 3, "unifiedAssetId": 1},
 "polyesterChainId": 2,
 "amountE18": {"hi": "1", "lo": "5000000000000000000"},
 "requestFee": {
  "assetIds": {"zippedAssetId": 3, "unifiedAssetId": 1},
  "amountE18": {"lo": "720000000000000000"},
  "recipientAddress": "0x00000000000000000000000000000000000000f1",
  "status": "REQUEST_FEE_STATUS_LOCKED"
 },
 "sourceTxHash": "0x6bb813ce477bf3aa09eb66315f10b2ae365a4d78285eba5e5cb6e09067f3a9b0",
 "txOccurrenceIndex": "8",
 "sourceAddress": "0x00000000000000000000000000000000000000a1",
 "destinationAddress": "0x00000000000000000000000000000000000000b2",
 "latestTxRef": "11455311618482410803791580314813518967",
 "sourceDomain": "DOMAIN_FUNDING",
 "destinationDomain": "DOMAIN_EXTERNAL_CHAIN",
 "latestLifecycleSource": "SOURCE_LEDGER",
 "startedAtUnixMs": "1795113306000",
 "updatedAtUnixMs": "1795113325000",
 "lastActivityAtUnixMs": "1795113325500",
 "isOpen": true,
 "currentStepSequence": 2,
 "currentProgress": {
  "currentStepStartedAtUnixMs": "1795113310000",
  "currentStepExpectedDurationMs": "11000",
  "currentConfirmations": 1,
  "requiredConfirmations": 12,
  "approveCount": 2,
  "rejectCount": 1,
  "validatorCount": 5,
  "requiredApprovals": 3,
  "requiredRejections": 3
 },
 "progressTimeline": [
  {"sequence": 1, "step": "FLOW_STEP_REQUEST", "status": "TIMELINE_STATUS_COMPLETED", "expectedDurationMs": "3000"},
  {"sequence": 2, "step": "FLOW_STEP_VALIDATION", "status": "TIMELINE_STATUS_CURRENT"},
  {"sequence": 3, "step": "FLOW_STEP_EXECUTION", "status": "TIMELINE_STATUS_PLANNED", "expectedDurationMs": "2000"}
 ],
 "estimatedCompletionUnixMs": "1795113347903"
}"#;

fn open_withdraw_view() -> FlowSummaryView {
    serde_json::from_str(OPEN_WITHDRAW_JSON).expect("flow summary JSON")
}

fn detail(summary: FlowSummaryView) -> FlowDetailView {
    FlowDetailView {
        summary: summary.into(),
        ..Default::default()
    }
}

/// The time `ms` milliseconds after the Unix epoch.
fn at(ms: u64) -> Option<SystemTime> {
    Some(UNIX_EPOCH + Duration::from_millis(ms))
}

fn assert_ledger_amount(name: &str, got: Option<&AssetAmount>, want: i128, asset_id: u32) {
    let got = got.unwrap_or_else(|| panic!("{name} is None"));
    assert_eq!(got.as_scaled(), want, "{name}");
    assert_eq!(got.scale(), Some(18), "{name} scale");
    assert_eq!(got.domain(), QuantityDomain::LedgerE18, "{name} domain");
    assert_eq!(got.asset_id(), Some(asset_id), "{name} asset");
}

/// Checks every field `OPEN_WITHDRAW_JSON` sets.
fn assert_open_withdraw(flow: &LifecycleFlowSummary) {
    assert_eq!(
        (
            flow.intent_id.as_str(),
            flow.flow_kind.as_str(),
            flow.latest_step.as_str(),
            flow.is_open
        ),
        ("flow_UZBpp6Kx7uX", "withdraw", "validation", true)
    );
    assert_eq!(
        (
            flow.zipped_asset_id,
            flow.unified_asset_id,
            flow.polyester_chain_id
        ),
        (3, 1, 2)
    );
    // hi = 1, lo = 5e18: 2^64 + 5e18, past u64, decoded exactly.
    assert_ledger_amount(
        "amount",
        flow.amount.as_ref(),
        23_446_744_073_709_551_616,
        1,
    );
    assert_eq!(
        flow.source_tx_hash,
        "0x6bb813ce477bf3aa09eb66315f10b2ae365a4d78285eba5e5cb6e09067f3a9b0"
    );
    assert_eq!(flow.tx_occurrence_index, 8);
    assert_eq!(flow.latest_tx_ref, "11455311618482410803791580314813518967");
    assert_eq!(
        (
            flow.source_domain.as_str(),
            flow.destination_domain.as_str()
        ),
        ("funding", "external_chain")
    );
    assert_eq!(flow.started_at, at(1_795_113_306_000));
    assert_eq!(flow.updated_at, at(1_795_113_325_000));
    assert_eq!(flow.last_activity_at, at(1_795_113_325_500));
    assert_eq!(flow.estimated_completion_at, at(1_795_113_347_903));
    assert_eq!(flow.terminal_at, None, "an open flow has no terminal time");

    let fee = flow.request_fee.as_ref().expect("request_fee");
    assert_eq!((fee.zipped_asset_id, fee.unified_asset_id), (3, 1));
    assert_eq!(
        fee.recipient_address,
        "0x00000000000000000000000000000000000000f1"
    );
    assert_eq!(fee.status, "locked");
    assert_ledger_amount(
        "request_fee.amount",
        fee.amount.as_ref(),
        720_000_000_000_000_000,
        1,
    );

    assert_eq!(flow.current_step_sequence, 2);
    let progress = flow.current_progress.as_ref().expect("current_progress");
    assert_eq!(progress.current_step_started_at, at(1_795_113_310_000));
    assert_eq!(progress.current_step_expected_duration_ms, 11_000);
    assert_eq!(
        (
            progress.current_confirmations,
            progress.required_confirmations
        ),
        (1, 12)
    );
    assert_eq!(
        (
            progress.approve_count,
            progress.reject_count,
            progress.validator_count,
            progress.required_approvals,
            progress.required_rejections
        ),
        (2, 1, 5, 3, 3)
    );
    let item =
        |sequence, step: &str, status: &str, expected_duration_ms| LifecycleFlowTimelineItem {
            sequence,
            step: step.to_owned(),
            status: status.to_owned(),
            expected_duration_ms,
        };
    assert_eq!(
        flow.progress_timeline,
        vec![
            item(1, "request", "completed", 3_000),
            item(2, "validation", "current", 0),
            item(3, "execution", "planned", 2_000),
        ]
    );
}

#[test]
fn list_flows_keeps_amount_transactions_and_times() {
    let page = flows_list_from_proto(&ListFlowsResponse {
        flows: vec![open_withdraw_view()],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(page.flows.len(), 1);
    assert_open_withdraw(&page.flows[0]);
}

#[test]
fn get_flow_keeps_amount_transactions_and_times() {
    let flow = flow_from_get_response(&GetFlowResponse {
        flow: detail(open_withdraw_view()).into(),
        ..Default::default()
    })
    .unwrap();
    assert_open_withdraw(&flow);
}

#[test]
fn account_flows_keep_amount_transactions_and_times() {
    let page = account_flows_list_from_proto(&ListAccountFlowsResponse {
        flows: vec![open_withdraw_view()],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(page.flows.len(), 1);
    assert_open_withdraw(&page.flows[0]);
}

#[test]
fn get_account_flow_keeps_amount_transactions_and_times() {
    let flow = account_flow_from_get_response(&GetAccountFlowResponse {
        flow: detail(open_withdraw_view()).into(),
        ..Default::default()
    })
    .unwrap();
    assert_open_withdraw(&flow);
}

#[test]
fn realtime_flow_update_keeps_amount_transactions_and_times() {
    let flow = flow_summary_from_bytes(&open_withdraw_view().encode_to_vec()).unwrap();
    assert_open_withdraw(&flow);
    assert_open_withdraw(&flow_summary_message_from_proto(&open_withdraw_view()).unwrap());
}

#[test]
fn realtime_flow_detail_keeps_amount_transactions_and_times() {
    let flow = flow_detail_from_bytes(&detail(open_withdraw_view()).encode_to_vec()).unwrap();
    assert_open_withdraw(&flow);
}

/// A transaction lookup carries the amount, asset, chain, transactions,
/// route and last activity, and nothing else of the summary.
#[test]
fn flows_by_tx_keep_their_amount_transactions_and_activity() {
    let reply = ListFlowsByTxResponse {
        matches: vec![FlowTxMatchView {
            flow_id: "flow_m".into(),
            flow_kind: FlowKind::KIND_DEPOSIT.into(),
            source_tx_hash: "0xabc".into(),
            latest_tx_ref: "0xdef".into(),
            tx_occurrence_index: 4,
            source_domain: FlowDomain::DOMAIN_EXTERNAL_CHAIN.into(),
            destination_domain: FlowDomain::DOMAIN_TRADING.into(),
            asset_ids: AssetIds {
                zipped_asset_id: 11,
                unified_asset_id: 5,
                ..Default::default()
            }
            .into(),
            polyester_chain_id: 2,
            amount_e18: U128 {
                lo: 110_000_000_000_000,
                ..Default::default()
            }
            .into(),
            last_activity_at_unix_ms: 1_795_113_314_313,
            ..Default::default()
        }],
        ..Default::default()
    };
    for page in [
        flows_by_tx_list_from_proto(&reply).unwrap(),
        flow_from_get_by_tx_response(&reply).unwrap(),
    ] {
        assert_eq!(page.flows.len(), 1);
        let flow = &page.flows[0];
        assert_eq!(
            (flow.source_tx_hash.as_str(), flow.latest_tx_ref.as_str()),
            ("0xabc", "0xdef")
        );
        assert_eq!(flow.tx_occurrence_index, 4);
        assert_eq!(
            (
                flow.source_domain.as_str(),
                flow.destination_domain.as_str()
            ),
            ("external_chain", "trading")
        );
        assert_eq!(
            (
                flow.zipped_asset_id,
                flow.unified_asset_id,
                flow.polyester_chain_id
            ),
            (11, 5, 2)
        );
        assert_ledger_amount("amount", flow.amount.as_ref(), 110_000_000_000_000, 5);
        assert_eq!(flow.last_activity_at, at(1_795_113_314_313));
        assert_eq!(
            (
                &flow.started_at,
                &flow.updated_at,
                &flow.terminal_at,
                &flow.estimated_completion_at
            ),
            (&None, &None, &None, &None)
        );
        assert!(flow.request_fee.is_none() && flow.current_progress.is_none());
        assert!(flow.progress_timeline.is_empty());
        assert_eq!(flow.current_step_sequence, 0);
    }
}

/// A terminal flow has its terminal time; a delayed one no estimate. A
/// summary sent without them leaves every optional field unset, not zero.
#[test]
fn flow_times_and_optional_fields_when_unset() {
    let terminal = flow_summary_message_from_proto(&FlowSummaryView {
        flow_id: "flow_t".into(),
        is_terminal: true,
        terminal_at_unix_ms: 1_795_113_340_254,
        request_fee: RequestFee {
            status: RequestFeeStatus::REQUEST_FEE_STATUS_KEPT.into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(terminal.terminal_at, at(1_795_113_340_254));
    let fee = terminal.request_fee.expect("request_fee");
    assert_eq!(fee.status, "kept");
    assert_eq!(fee.amount, None);
    assert_eq!((fee.zipped_asset_id, fee.unified_asset_id), (0, 0));

    let bare = flow_summary_message_from_proto(&FlowSummaryView {
        flow_id: "flow_d".into(),
        is_open: true,
        is_delayed: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(bare.amount, None);
    assert_eq!(bare.request_fee, None);
    assert_eq!(bare.current_progress, None);
    assert!(bare.progress_timeline.is_empty());
    for (name, time) in [
        ("started_at", &bare.started_at),
        ("updated_at", &bare.updated_at),
        ("terminal_at", &bare.terminal_at),
        ("last_activity_at", &bare.last_activity_at),
        ("estimated_completion_at", &bare.estimated_completion_at),
    ] {
        assert_eq!(time, &None, "{name}");
    }
    assert_eq!(
        (
            bare.source_domain.as_str(),
            bare.destination_domain.as_str()
        ),
        ("unspecified", "unspecified")
    );
    assert_eq!(
        (
            bare.zipped_asset_id,
            bare.unified_asset_id,
            bare.polyester_chain_id
        ),
        (0, 0, 0)
    );

    // An amount sent without asset IDs names no asset.
    let unnamed = flow_summary_message_from_proto(&FlowSummaryView {
        flow_id: "flow_u".into(),
        amount_e18: U128 {
            lo: 7,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    })
    .unwrap();
    let amount = unnamed.amount.expect("amount");
    assert_eq!((amount.as_scaled(), amount.asset_id()), (7, None));
}

#[test]
fn flow_enums_newer_than_the_sdk_keep_their_number() {
    let flow = flow_summary_message_from_proto(&FlowSummaryView {
        flow_id: "flow_n".into(),
        source_domain: buffa::EnumValue::from(42),
        request_fee: RequestFee {
            status: buffa::EnumValue::from(9),
            ..Default::default()
        }
        .into(),
        progress_timeline: vec![FlowTimelineItemView {
            sequence: 1,
            status: buffa::EnumValue::from(7),
            ..Default::default()
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(flow.source_domain, "unknown_domain_42");
    assert_eq!(flow.request_fee.unwrap().status, "unknown_fee_status_9");
    assert_eq!(
        flow.progress_timeline[0].status,
        "unknown_timeline_status_7"
    );
    let known = flow_summary_message_from_proto(&FlowSummaryView {
        progress_timeline: vec![FlowTimelineItemView {
            status: FlowTimelineStatus::TIMELINE_STATUS_UNSPECIFIED.into(),
            ..Default::default()
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(known.progress_timeline[0].status, "unspecified");
}

/// An amount past what `AssetAmount` holds (2^127 - 1) is refused with
/// `Error::ResponseContract` on every path, never read as another amount.
#[test]
fn flow_amount_out_of_range_fails_closed() {
    let huge = || -> buffa::MessageField<U128> {
        U128 {
            hi: 1 << 63,
            ..Default::default()
        }
        .into()
    };
    let summary = FlowSummaryView {
        flow_id: "flow_x".into(),
        amount_e18: huge(),
        ..Default::default()
    };
    let fee_only = FlowSummaryView {
        flow_id: "flow_y".into(),
        request_fee: RequestFee {
            amount_e18: huge(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    for msg in [summary, fee_only] {
        let contract = |result: crate::errors::Result<LifecycleFlowSummary>| {
            assert!(
                matches!(result, Err(Error::ResponseContract { .. })),
                "{result:?}"
            );
        };
        contract(flow_summary_message_from_proto(&msg));
        contract(flow_summary_from_bytes(&msg.encode_to_vec()));
        contract(flow_detail_from_bytes(&detail(msg.clone()).encode_to_vec()));
        contract(flow_from_get_response(&GetFlowResponse {
            flow: detail(msg.clone()).into(),
            ..Default::default()
        }));
        assert!(matches!(
            flows_list_from_proto(&ListFlowsResponse {
                flows: vec![msg.clone()],
                ..Default::default()
            }),
            Err(Error::ResponseContract { .. })
        ));
    }
    assert!(matches!(
        flows_by_tx_list_from_proto(&ListFlowsByTxResponse {
            matches: vec![FlowTxMatchView {
                flow_id: "flow_z".into(),
                amount_e18: huge(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        Err(Error::ResponseContract { .. })
    ));
}

/// A flow summary stays `Eq`, as it was before it carried its times: callers
/// compare flows and pages with `==` and require `Eq` of them.
#[test]
fn flow_models_stay_eq() {
    fn eq<T: Eq>() {}
    eq::<LifecycleFlowSummary>();
    eq::<crate::models::LifecycleFlowsList>();
    eq::<crate::models::LifecycleRequestFee>();
    eq::<crate::models::LifecycleFlowProgress>();
    eq::<LifecycleFlowTimelineItem>();
    let flow = flow_summary_message_from_proto(&open_withdraw_view()).unwrap();
    assert_eq!(flow, flow.clone());
    assert_ne!(flow, LifecycleFlowSummary::default());
}
