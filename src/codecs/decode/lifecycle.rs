//! Lifecycle flow decoders.

use crate::codecs::decode::enums::enum_proto_name;
use crate::codecs::scalars::format_uint64_id;
use crate::errors::{Error, Result};
use crate::models::{LifecycleFlowSummary, LifecycleFlowsList, ZipperReasonDetails};
use crate::proto::chain::lifecycle::v1::{
    FlowKind as FlowKindEnum, FlowStep, FlowSummaryView, FlowTxMatchView, GetFlowResponse,
    LifecycleReason, ListFlowsByTxResponse, ListFlowsResponse,
};
use crate::proto::chain::zipper::v1::ZipperReasonDetails as ProtoZipperReasonDetails;

fn lifecycle_reason_label(value: &buffa::EnumValue<LifecycleReason>) -> String {
    match value.as_known() {
        Some(LifecycleReason::REASON_UNSPECIFIED) => "unspecified".to_owned(),
        Some(LifecycleReason::ZIPPER_VALIDATION_REJECTED) => {
            "zipper_validation_rejected".to_owned()
        }
        Some(LifecycleReason::ZIPPER_EXECUTION_REJECTED) => "zipper_execution_rejected".to_owned(),
        Some(LifecycleReason::ZIPPER_WITHDRAW_EXECUTION_FAILED) => {
            "zipper_withdraw_execution_failed".to_owned()
        }
        Some(LifecycleReason::ZIPPER_DEPOSIT_REFUND_FAILED) => {
            "zipper_deposit_refund_failed".to_owned()
        }
        Some(LifecycleReason::LEDGER_MIRROR_REJECTED) => "ledger_mirror_rejected".to_owned(),
        Some(LifecycleReason::LEDGER_MIRROR_TRANSFER_EXCEEDS_CREDITS) => {
            "ledger_mirror_transfer_exceeds_credits".to_owned()
        }
        Some(LifecycleReason::LEDGER_MIRROR_TRANSFER_EXISTS) => {
            "ledger_mirror_transfer_exists".to_owned()
        }
        Some(LifecycleReason::LEDGER_MIRROR_PENDING_TRANSFER_NOT_FOUND) => {
            "ledger_mirror_pending_transfer_not_found".to_owned()
        }
        Some(LifecycleReason::LEDGER_MIRROR_TRANSFER_ID_ALREADY_FAILED) => {
            "ledger_mirror_transfer_id_already_failed".to_owned()
        }
        Some(LifecycleReason::TRADING_WITHDRAW_POLICY_DENIED) => {
            "trading_withdraw_policy_denied".to_owned()
        }
        Some(LifecycleReason::TRADING_WITHDRAW_CONTRACT_REVERTED) => {
            "trading_withdraw_contract_reverted".to_owned()
        }
        Some(LifecycleReason::TRADING_WITHDRAW_EXECUTION_FAILED) => {
            "trading_withdraw_execution_failed".to_owned()
        }
        None => format!("unknown_reason_{}", value.to_i32()),
    }
}

fn zipper_reason_from_proto(msg: &ProtoZipperReasonDetails) -> ZipperReasonDetails {
    ZipperReasonDetails {
        code: msg.code.to_i32(),
        reason_id: msg.reason_id.clone(),
        message: msg.message.clone(),
    }
}

fn flow_kind_label(value: &buffa::EnumValue<FlowKindEnum>) -> String {
    match value.as_known() {
        Some(FlowKindEnum::KIND_DEPOSIT) => "deposit".to_owned(),
        Some(FlowKindEnum::KIND_WITHDRAW) => "withdraw".to_owned(),
        Some(FlowKindEnum::KIND_TRANSFER) => "transfer".to_owned(),
        Some(FlowKindEnum::KIND_UNSPECIFIED) => "unspecified".to_owned(),
        None => {
            let raw = enum_proto_name(value);
            if raw.is_empty() { String::new() } else { raw }
        }
    }
}

fn flow_step_label(value: &buffa::EnumValue<FlowStep>) -> String {
    match value.as_known() {
        Some(FlowStep::FLOW_STEP_SOURCE) => "source".to_owned(),
        Some(FlowStep::FLOW_STEP_TRANSFER) => "transfer".to_owned(),
        Some(FlowStep::FLOW_STEP_REQUEST) => "request".to_owned(),
        Some(FlowStep::FLOW_STEP_VALIDATION) => "validation".to_owned(),
        Some(FlowStep::FLOW_STEP_EXECUTION) => "execution".to_owned(),
        Some(FlowStep::FLOW_STEP_BRIDGE_FULFILLMENT) => "bridge_fulfillment".to_owned(),
        Some(FlowStep::FLOW_STEP_DROPPED) => "dropped".to_owned(),
        Some(FlowStep::FLOW_STEP_FAILED) => "failed".to_owned(),
        Some(FlowStep::FLOW_STEP_REFUNDED) => "refunded".to_owned(),
        Some(FlowStep::FLOW_STEP_FULFILLING) => "fulfilling".to_owned(),
        Some(FlowStep::FLOW_STEP_SETTLEMENT) => "settlement".to_owned(),
        Some(FlowStep::FLOW_STEP_UNSPECIFIED) => "unspecified".to_owned(),
        None => {
            let raw = enum_proto_name(value);
            if raw.is_empty() { String::new() } else { raw }
        }
    }
}

fn flow_summary_from_proto(msg: &FlowSummaryView) -> LifecycleFlowSummary {
    LifecycleFlowSummary {
        intent_id: msg.flow_id.clone(),
        flow_kind: flow_kind_label(&msg.flow_kind),
        latest_step: flow_step_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        owner_account_id: format_uint64_id(msg.owner_account_id),
        smart_account_address: msg.smart_account_address.clone(),
        lifecycle_reason: lifecycle_reason_label(&msg.lifecycle_reason),
        zipper_reason: msg.zipper_reason.as_option().map(zipper_reason_from_proto),
    }
}

pub fn flow_summary_message_from_proto(msg: &FlowSummaryView) -> LifecycleFlowSummary {
    flow_summary_from_proto(msg)
}

pub fn flows_list_from_proto(msg: &ListFlowsResponse) -> LifecycleFlowsList {
    LifecycleFlowsList {
        flows: msg.flows.iter().map(flow_summary_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

fn flow_tx_match_from_proto(msg: &FlowTxMatchView) -> LifecycleFlowSummary {
    LifecycleFlowSummary {
        intent_id: msg.flow_id.clone(),
        flow_kind: flow_kind_label(&msg.flow_kind),
        latest_step: flow_step_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        owner_account_id: format_uint64_id(msg.owner_account_id),
        smart_account_address: msg.smart_account_address.clone(),
        lifecycle_reason: lifecycle_reason_label(&msg.lifecycle_reason),
        zipper_reason: msg.zipper_reason.as_option().map(zipper_reason_from_proto),
    }
}

pub fn flows_by_tx_list_from_proto(msg: &ListFlowsByTxResponse) -> LifecycleFlowsList {
    LifecycleFlowsList {
        flows: msg.matches.iter().map(flow_tx_match_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn flow_from_get_response(msg: &GetFlowResponse) -> Result<LifecycleFlowSummary> {
    let detail = msg
        .flow
        .as_option()
        .ok_or_else(|| Error::transport("invalid GetFlow response: missing flow"))?;
    detail
        .summary
        .as_option()
        .map(flow_summary_from_proto)
        .ok_or_else(|| Error::transport("invalid GetFlow response: missing flow summary"))
}

/// Decode every match from a transaction lookup response.
///
/// The legacy helper name is retained for compatibility, but transaction
/// lookups are one-to-many and must not silently discard bundled flows.
pub fn flow_from_get_by_tx_response(msg: &ListFlowsByTxResponse) -> LifecycleFlowsList {
    flows_by_tx_list_from_proto(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::chain::lifecycle::v1::{FlowKind as FlowKindEnum, FlowStep};
    use crate::proto::chain::zipper::v1::ZipperReasonCode;

    #[test]
    fn flow_summary_maps_fields() {
        let msg = FlowSummaryView {
            flow_id: "flow-abc".into(),
            flow_kind: FlowKindEnum::KindDeposit.into(),
            current_step: FlowStep::Settlement.into(),
            is_open: false,
            is_terminal: true,
            owner_account_id: 99,
            smart_account_address: "0xabc".into(),
            source_address: "0xsource".into(),
            lifecycle_reason: LifecycleReason::LedgerMirrorTransferExceedsCredits.into(),
            zipper_reason: ProtoZipperReasonDetails {
                code: ZipperReasonCode::DepositAmountBelowMinimum.into(),
                reason_id: "deposit_amount_below_minimum".into(),
                message: "Deposit amount is below the minimum".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert_eq!(flow.intent_id, "flow-abc");
        assert!(flow.is_terminal);
        assert_eq!(flow.flow_kind, "deposit");
        assert_eq!(flow.latest_step, "settlement");
        assert_eq!(flow.smart_account_address, "0xabc");
        assert_eq!(flow.owner_account_id, format_uint64_id(99));
        assert_eq!(
            flow.lifecycle_reason,
            "ledger_mirror_transfer_exceeds_credits"
        );
        let zipper = flow.zipper_reason.expect("zipper_reason");
        assert_eq!(zipper.code, 1003);
        assert_eq!(zipper.reason_id, "deposit_amount_below_minimum");
        assert_eq!(zipper.message, "Deposit amount is below the minimum");
    }

    #[test]
    fn lifecycle_reason_preserves_unknown_codes() {
        let msg = FlowSummaryView {
            flow_id: "flow-unknown".into(),
            lifecycle_reason: buffa::EnumValue::from(2001),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert_eq!(flow.flow_kind, "unspecified");
        assert_eq!(flow.latest_step, "unspecified");
        assert_eq!(flow.lifecycle_reason, "unknown_reason_2001");
        assert!(flow.zipper_reason.is_none());
    }

    #[test]
    fn trading_withdraw_lifecycle_reasons() {
        let cases = [
            (
                LifecycleReason::TRADING_WITHDRAW_POLICY_DENIED,
                "trading_withdraw_policy_denied",
            ),
            (
                LifecycleReason::TRADING_WITHDRAW_CONTRACT_REVERTED,
                "trading_withdraw_contract_reverted",
            ),
            (
                LifecycleReason::TRADING_WITHDRAW_EXECUTION_FAILED,
                "trading_withdraw_execution_failed",
            ),
        ];
        for (reason, expected) in cases {
            let msg = FlowSummaryView {
                flow_id: "flow-trading-withdraw".into(),
                flow_kind: FlowKindEnum::KIND_WITHDRAW.into(),
                current_step: FlowStep::FLOW_STEP_FAILED.into(),
                is_terminal: true,
                lifecycle_reason: reason.into(),
                ..Default::default()
            };
            let flow = flow_summary_message_from_proto(&msg);
            assert_eq!(flow.lifecycle_reason, expected);
        }
    }

    #[test]
    fn flows_list_maps_pagination() {
        let msg = ListFlowsResponse {
            flows: vec![
                FlowSummaryView {
                    flow_id: "a".into(),
                    ..Default::default()
                },
                FlowSummaryView {
                    flow_id: "b".into(),
                    ..Default::default()
                },
            ],
            next_page_token: "next".into(),
            ..Default::default()
        };
        let result = flows_list_from_proto(&msg);
        assert_eq!(result.flows.len(), 2);
        assert_eq!(result.next_page_token, "next");
        assert_eq!(result.flows[0].lifecycle_reason, "unspecified");
    }

    #[test]
    fn flow_by_tx_response_preserves_all_matches_and_owner_identity() {
        let msg = ListFlowsByTxResponse {
            matches: vec![
                FlowTxMatchView {
                    flow_id: "flow-a".into(),
                    owner_account_id: 99,
                    smart_account_address: "0xsmart".into(),
                    ..Default::default()
                },
                FlowTxMatchView {
                    flow_id: "flow-b".into(),
                    ..Default::default()
                },
            ],
            next_page_token: "next".into(),
            ..Default::default()
        };
        let result = flow_from_get_by_tx_response(&msg);
        assert_eq!(result.flows.len(), 2);
        assert_eq!(result.flows[0].owner_account_id, format_uint64_id(99));
        assert_eq!(result.flows[0].smart_account_address, "0xsmart");
        assert_eq!(result.flows[1].intent_id, "flow-b");
        assert_eq!(result.next_page_token, "next");
    }

    #[test]
    fn singular_flow_responses_reject_missing_required_entities() {
        assert!(flow_from_get_response(&GetFlowResponse::default()).is_err());
    }
}
