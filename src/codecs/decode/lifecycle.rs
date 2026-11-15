//! Lifecycle flow decoders.

use crate::codecs::decode::common::require_str_id;
use crate::codecs::decode::enums::enum_proto_name;
use crate::errors::{Error, Result};
use crate::models::{LifecycleFlowSummary, LifecycleFlowsList, ZipperReasonDetails};
use crate::proto::chain::lifecycle::v1::{
    FlowKind as FlowKindEnum, FlowParticipation, FlowStep, FlowSummaryView, FlowTxMatchView,
    GetAccountFlowResponse, GetFlowResponse, LifecycleReason, ListAccountFlowsResponse,
    ListFlowsByTxResponse, ListFlowsResponse,
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
        Some(LifecycleReason::DEPOSIT_SOURCE_NOT_CONFIRMED) => {
            "deposit_source_not_confirmed".to_owned()
        }
        Some(LifecycleReason::ZIPPER_COMPLIANCE_TRANSFERRED) => {
            "zipper_compliance_transferred".to_owned()
        }
        Some(LifecycleReason::ZIPPER_DEPOSIT_DELIVERED_TO_WALLET) => {
            "zipper_deposit_delivered_to_wallet".to_owned()
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

/// Names a `FlowParticipation` the way REST spells it, lowercased. A value
/// newer than this SDK reads `unknown_participation_<n>`, never unspecified.
fn flow_participation_label(value: &buffa::EnumValue<FlowParticipation>) -> String {
    match value.as_known() {
        Some(FlowParticipation::PARTICIPATION_UNSPECIFIED) => "unspecified".to_owned(),
        Some(FlowParticipation::PARTICIPATION_OWNER) => "owner".to_owned(),
        Some(FlowParticipation::PARTICIPATION_RECIPIENT) => "recipient".to_owned(),
        None => format!("unknown_participation_{}", value.to_i32()),
    }
}

fn flow_summary_from_proto(msg: &FlowSummaryView) -> LifecycleFlowSummary {
    LifecycleFlowSummary {
        intent_id: msg.flow_id.clone(),
        flow_kind: flow_kind_label(&msg.flow_kind),
        latest_step: flow_step_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        is_delayed: msg.is_delayed,
        smart_account_address: msg.smart_account_address.clone(),
        lifecycle_reason: lifecycle_reason_label(&msg.lifecycle_reason),
        zipper_reason: msg.zipper_reason.as_option().map(zipper_reason_from_proto),
        participation: flow_participation_label(&msg.participation),
        source_address: msg.source_address.clone(),
        destination_address: msg.destination_address.clone(),
    }
}

pub fn flow_summary_message_from_proto(msg: &FlowSummaryView) -> LifecycleFlowSummary {
    flow_summary_from_proto(msg)
}

/// A read flow summary, refused with `Error::ResponseContract` without its
/// flow ID.
fn required_flow_summary(operation: &str, msg: &FlowSummaryView) -> Result<LifecycleFlowSummary> {
    require_str_id(operation, "flow_id", &msg.flow_id)?;
    Ok(flow_summary_from_proto(msg))
}

pub fn flows_list_from_proto(msg: &ListFlowsResponse) -> Result<LifecycleFlowsList> {
    Ok(LifecycleFlowsList {
        flows: msg
            .flows
            .iter()
            .map(|flow| required_flow_summary("ListFlows", flow))
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

fn flow_tx_match_from_proto(msg: &FlowTxMatchView) -> LifecycleFlowSummary {
    LifecycleFlowSummary {
        intent_id: msg.flow_id.clone(),
        flow_kind: flow_kind_label(&msg.flow_kind),
        latest_step: flow_step_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        // Transaction lookups do not report delay.
        is_delayed: false,
        smart_account_address: msg.smart_account_address.clone(),
        lifecycle_reason: lifecycle_reason_label(&msg.lifecycle_reason),
        zipper_reason: msg.zipper_reason.as_option().map(zipper_reason_from_proto),
        // Transaction lookups do not report participation.
        participation: String::new(),
        source_address: msg.source_address.clone(),
        destination_address: msg.destination_address.clone(),
    }
}

pub fn flows_by_tx_list_from_proto(msg: &ListFlowsByTxResponse) -> Result<LifecycleFlowsList> {
    Ok(LifecycleFlowsList {
        flows: msg
            .matches
            .iter()
            .map(|flow| {
                require_str_id("ListFlowsByTx", "flow_id", &flow.flow_id)?;
                Ok(flow_tx_match_from_proto(flow))
            })
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

/// One page of the account's own flows, from `ListAccountFlows`: every row
/// needs its flow ID, as on `ListFlows`.
pub fn account_flows_list_from_proto(msg: &ListAccountFlowsResponse) -> Result<LifecycleFlowsList> {
    Ok(LifecycleFlowsList {
        flows: msg
            .flows
            .iter()
            .map(|flow| required_flow_summary("ListAccountFlows", flow))
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
}

/// One of the account's flows, from `GetAccountFlow`. A reply without the
/// flow, its summary or the summary's flow ID is an
/// [`Error::ResponseContract`].
pub fn account_flow_from_get_response(
    msg: &GetAccountFlowResponse,
) -> Result<LifecycleFlowSummary> {
    let detail = msg
        .flow
        .as_option()
        .ok_or_else(|| Error::response_contract("GetAccountFlow", "missing flow"))?;
    let summary = detail
        .summary
        .as_option()
        .ok_or_else(|| Error::response_contract("GetAccountFlow", "missing flow summary"))?;
    required_flow_summary("GetAccountFlow", summary)
}

pub fn flow_from_get_response(msg: &GetFlowResponse) -> Result<LifecycleFlowSummary> {
    let detail = msg
        .flow
        .as_option()
        .ok_or_else(|| Error::response_contract("GetFlowById", "missing flow"))?;
    let summary = detail
        .summary
        .as_option()
        .ok_or_else(|| Error::response_contract("GetFlowById", "missing flow summary"))?;
    required_flow_summary("GetFlowById", summary)
}

/// Decode every match from a transaction lookup response.
///
/// The legacy helper name is retained for compatibility, but transaction
/// lookups are one-to-many and must not silently discard bundled flows.
pub fn flow_from_get_by_tx_response(msg: &ListFlowsByTxResponse) -> Result<LifecycleFlowsList> {
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
        assert_eq!(
            flow.lifecycle_reason,
            "ledger_mirror_transfer_exceeds_credits"
        );
        let zipper = flow.zipper_reason.expect("zipper_reason");
        assert_eq!(zipper.code, 1003);
        assert_eq!(zipper.reason_id, "deposit_amount_below_minimum");
        assert_eq!(zipper.message, "Deposit amount is below the minimum");
    }

    /// A withdrawal whose execution failed after its destination transaction
    /// keeps the server's reason id and message, which say it is under review,
    /// whatever the code, including when the bridge gave none.
    #[test]
    fn withdraw_execution_failed_keeps_the_review_message() {
        for code in [None, Some(ZipperReasonCode::DepositAmountBelowMinimum)] {
            let msg = FlowSummaryView {
                flow_id: "flow-w".into(),
                lifecycle_reason: LifecycleReason::ZIPPER_WITHDRAW_EXECUTION_FAILED.into(),
                zipper_reason: ProtoZipperReasonDetails {
                    code: code.map(Into::into).unwrap_or_default(),
                    reason_id: "withdrawal.execution_failed.under_review".into(),
                    message: "Failed, under review by the bridge operator. The funds were not returned to your account.".into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            };
            let flow = flow_summary_message_from_proto(&msg);
            assert_eq!(flow.lifecycle_reason, "zipper_withdraw_execution_failed");
            let zipper = flow.zipper_reason.expect("zipper_reason");
            assert_eq!(zipper.code, code.map_or(0, |c| c as i32));
            assert_eq!(zipper.reason_id, "withdrawal.execution_failed.under_review");
            assert!(zipper.message.starts_with("Failed, under review"));
        }
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
    fn zipper_compliance_transfer_reason_has_its_label() {
        let msg = FlowSummaryView {
            flow_id: "flow-compliance".into(),
            lifecycle_reason: LifecycleReason::ZIPPER_COMPLIANCE_TRANSFERRED.into(),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert_eq!(flow.lifecycle_reason, "zipper_compliance_transferred");
    }

    /// A deposit delivered to the account's wallet ends completed with
    /// reason 106, read by name.
    #[test]
    fn wallet_delivered_deposit_reason_has_its_label() {
        let msg = FlowSummaryView {
            flow_id: "flow-wallet".into(),
            flow_kind: FlowKindEnum::KIND_DEPOSIT.into(),
            current_step: FlowStep::FLOW_STEP_TRANSFER.into(),
            is_terminal: true,
            lifecycle_reason: LifecycleReason::ZIPPER_DEPOSIT_DELIVERED_TO_WALLET.into(),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert_eq!(flow.lifecycle_reason, "zipper_deposit_delivered_to_wallet");
        assert!(flow.is_terminal && !flow.is_open);
        assert_eq!(flow.latest_step, "transfer");
        let by_number = flow_summary_message_from_proto(&FlowSummaryView {
            flow_id: "flow-wallet".into(),
            lifecycle_reason: buffa::EnumValue::from(106),
            ..Default::default()
        });
        assert_eq!(
            by_number.lifecycle_reason,
            "zipper_deposit_delivered_to_wallet"
        );
    }

    #[test]
    fn account_flow_replies_require_the_flow_and_its_id() {
        use crate::proto::chain::lifecycle::v1::FlowDetailView;
        let list = account_flows_list_from_proto(&ListAccountFlowsResponse {
            flows: vec![FlowSummaryView {
                flow_id: "flow_a".into(),
                participation: FlowParticipation::PARTICIPATION_RECIPIENT.into(),
                ..Default::default()
            }],
            next_page_token: "next".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(list.flows[0].participation, "recipient");
        assert_eq!(list.next_page_token, "next");
        assert!(matches!(
            account_flows_list_from_proto(&ListAccountFlowsResponse {
                flows: vec![FlowSummaryView::default()],
                ..Default::default()
            }),
            Err(Error::ResponseContract { .. })
        ));
        let get = |summary: Option<FlowSummaryView>| GetAccountFlowResponse {
            flow: FlowDetailView {
                summary: summary.map_or_else(Default::default, Into::into),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let flow = account_flow_from_get_response(&get(Some(FlowSummaryView {
            flow_id: "flow_b".into(),
            participation: FlowParticipation::PARTICIPATION_OWNER.into(),
            ..Default::default()
        })))
        .unwrap();
        assert_eq!(
            (flow.intent_id.as_str(), flow.participation.as_str()),
            ("flow_b", "owner")
        );
        for reply in [
            GetAccountFlowResponse::default(),
            get(None),
            get(Some(FlowSummaryView::default())),
        ] {
            assert!(matches!(
                account_flow_from_get_response(&reply),
                Err(Error::ResponseContract { .. })
            ));
        }
    }

    #[test]
    fn open_deposit_reports_delay_and_unconfirmed_source() {
        let msg = FlowSummaryView {
            flow_id: "flow-deposit".into(),
            flow_kind: FlowKindEnum::KIND_DEPOSIT.into(),
            current_step: FlowStep::FLOW_STEP_SOURCE.into(),
            is_open: true,
            is_delayed: true,
            lifecycle_reason: LifecycleReason::DEPOSIT_SOURCE_NOT_CONFIRMED.into(),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert!(flow.is_delayed);
        assert_eq!(flow.lifecycle_reason, "deposit_source_not_confirmed");

        let on_time = flow_summary_message_from_proto(&FlowSummaryView {
            is_open: true,
            ..Default::default()
        });
        assert!(!on_time.is_delayed);
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
        let result = flows_list_from_proto(&msg).unwrap();
        assert_eq!(result.flows.len(), 2);
        assert_eq!(result.next_page_token, "next");
        assert_eq!(result.flows[0].lifecycle_reason, "unspecified");
    }

    #[test]
    fn flow_by_tx_response_preserves_all_matches_and_smart_account() {
        let msg = ListFlowsByTxResponse {
            matches: vec![
                FlowTxMatchView {
                    flow_id: "flow-a".into(),
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
        let result = flow_from_get_by_tx_response(&msg).unwrap();
        assert_eq!(result.flows.len(), 2);
        assert_eq!(result.flows[0].smart_account_address, "0xsmart");
        assert_eq!(result.flows[1].intent_id, "flow-b");
        assert_eq!(result.next_page_token, "next");
    }

    #[test]
    fn singular_flow_responses_reject_missing_required_entities() {
        assert!(flow_from_get_response(&GetFlowResponse::default()).is_err());
    }
}
