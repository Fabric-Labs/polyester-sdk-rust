//! Lifecycle flow decoders.

use crate::codecs::decode::common::require_str_id;
use crate::codecs::decode::dust::e18_amount;
use crate::codecs::decode::enums::enum_proto_name;
use crate::errors::{Error, Result};
use crate::models::{
    LifecycleFlowProgress, LifecycleFlowSummary, LifecycleFlowTimelineItem, LifecycleFlowsList,
    LifecycleRequestFee, ZipperReasonDetails,
};
use crate::proto::chain::lifecycle::v1::{
    AssetIds, FlowDomain, FlowKind as FlowKindEnum, FlowParticipation, FlowStep,
    FlowSummaryProgressView, FlowSummaryView, FlowTimelineItemView, FlowTimelineStatus,
    FlowTxMatchView, GetAccountFlowResponse, GetFlowResponse, LifecycleReason,
    ListAccountFlowsResponse, ListFlowsByTxResponse, ListFlowsResponse, RequestFee,
    RequestFeeStatus,
};
use crate::proto::chain::zipper::v1::ZipperReasonDetails as ProtoZipperReasonDetails;
use crate::proto::polyester::r#type::v1::U128;
use crate::types::AssetAmount;
use buffa_types::google::protobuf::Timestamp;

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

/// Names a `FlowDomain` lowercased without its `DOMAIN_` prefix. A value
/// newer than this SDK reads `unknown_domain_<n>`, never unspecified.
fn flow_domain_label(value: &buffa::EnumValue<FlowDomain>) -> String {
    match value.as_known() {
        Some(FlowDomain::DOMAIN_UNSPECIFIED) => "unspecified".to_owned(),
        Some(FlowDomain::DOMAIN_EXTERNAL_CHAIN) => "external_chain".to_owned(),
        Some(FlowDomain::DOMAIN_ZIPPER) => "zipper".to_owned(),
        Some(FlowDomain::DOMAIN_FUNDING) => "funding".to_owned(),
        Some(FlowDomain::DOMAIN_TRADING) => "trading".to_owned(),
        Some(FlowDomain::DOMAIN_LENDING) => "lending".to_owned(),
        None => format!("unknown_domain_{}", value.to_i32()),
    }
}

fn request_fee_status_label(value: &buffa::EnumValue<RequestFeeStatus>) -> String {
    match value.as_known() {
        Some(RequestFeeStatus::REQUEST_FEE_STATUS_UNSPECIFIED) => "unspecified".to_owned(),
        Some(RequestFeeStatus::REQUEST_FEE_STATUS_LOCKED) => "locked".to_owned(),
        Some(RequestFeeStatus::REQUEST_FEE_STATUS_SETTLED) => "settled".to_owned(),
        Some(RequestFeeStatus::REQUEST_FEE_STATUS_KEPT) => "kept".to_owned(),
        None => format!("unknown_fee_status_{}", value.to_i32()),
    }
}

fn flow_timeline_status_label(value: &buffa::EnumValue<FlowTimelineStatus>) -> String {
    match value.as_known() {
        Some(FlowTimelineStatus::TIMELINE_STATUS_UNSPECIFIED) => "unspecified".to_owned(),
        Some(FlowTimelineStatus::TIMELINE_STATUS_COMPLETED) => "completed".to_owned(),
        Some(FlowTimelineStatus::TIMELINE_STATUS_CURRENT) => "current".to_owned(),
        Some(FlowTimelineStatus::TIMELINE_STATUS_PLANNED) => "planned".to_owned(),
        None => format!("unknown_timeline_status_{}", value.to_i32()),
    }
}

/// A time the contract sends in milliseconds since the epoch (UTC). Zero is
/// the contract's "no time" and reads `None`, as does a value past `i64`
/// milliseconds.
fn unix_ms_time(ms: u64) -> Option<Timestamp> {
    let ms = i64::try_from(ms).ok().filter(|ms| *ms != 0)?;
    Some(Timestamp {
        seconds: ms / 1000,
        nanos: i32::try_from(ms % 1000).ok()? * 1_000_000,
        ..Default::default()
    })
}

/// The `(zipped, unified)` asset IDs, 0 when the server sends none.
fn asset_ids(msg: &buffa::MessageField<AssetIds>) -> (u32, u32) {
    msg.as_option()
        .map_or((0, 0), |ids| (ids.zipped_asset_id, ids.unified_asset_id))
}

/// An exact 18-decimal ledger amount in units of `unified_asset_id` (none
/// when it is 0); `None` when the server sends none.
fn ledger_amount(
    operation: &str,
    field: &str,
    value: &buffa::MessageField<U128>,
    unified_asset_id: u32,
) -> Result<Option<AssetAmount>> {
    value
        .as_option()
        .map(|v| {
            e18_amount(
                operation,
                field,
                Some(v),
                (unified_asset_id != 0).then_some(unified_asset_id),
            )
        })
        .transpose()
}

fn request_fee_from_proto(operation: &str, msg: &RequestFee) -> Result<LifecycleRequestFee> {
    let (zipped_asset_id, unified_asset_id) = asset_ids(&msg.asset_ids);
    Ok(LifecycleRequestFee {
        zipped_asset_id,
        unified_asset_id,
        amount: ledger_amount(
            operation,
            "request_fee.amount_e18",
            &msg.amount_e18,
            unified_asset_id,
        )?,
        recipient_address: msg.recipient_address.clone(),
        status: request_fee_status_label(&msg.status),
    })
}

fn flow_progress_from_proto(msg: &FlowSummaryProgressView) -> LifecycleFlowProgress {
    LifecycleFlowProgress {
        current_step_started_at: unix_ms_time(msg.current_step_started_at_unix_ms),
        current_step_expected_duration_ms: msg.current_step_expected_duration_ms,
        current_confirmations: msg.current_confirmations,
        required_confirmations: msg.required_confirmations,
        approve_count: msg.approve_count,
        reject_count: msg.reject_count,
        validator_count: msg.validator_count,
        required_approvals: msg.required_approvals,
        required_rejections: msg.required_rejections,
    }
}

fn flow_timeline_item_from_proto(msg: &FlowTimelineItemView) -> LifecycleFlowTimelineItem {
    LifecycleFlowTimelineItem {
        sequence: msg.sequence,
        step: flow_step_label(&msg.step),
        status: flow_timeline_status_label(&msg.status),
        expected_duration_ms: msg.expected_duration_ms,
    }
}

/// A flow summary, refused with `Error::ResponseContract` under `operation`
/// when its amount or fee is past what [`AssetAmount`] holds.
fn flow_summary_from_proto(operation: &str, msg: &FlowSummaryView) -> Result<LifecycleFlowSummary> {
    let (zipped_asset_id, unified_asset_id) = asset_ids(&msg.asset_ids);
    Ok(LifecycleFlowSummary {
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
        zipped_asset_id,
        unified_asset_id,
        polyester_chain_id: msg.polyester_chain_id,
        amount: ledger_amount(operation, "amount_e18", &msg.amount_e18, unified_asset_id)?,
        request_fee: msg
            .request_fee
            .as_option()
            .map(|fee| request_fee_from_proto(operation, fee))
            .transpose()?,
        source_tx_hash: msg.source_tx_hash.clone(),
        tx_occurrence_index: msg.tx_occurrence_index,
        latest_tx_ref: msg.latest_tx_ref.clone(),
        source_domain: flow_domain_label(&msg.source_domain),
        destination_domain: flow_domain_label(&msg.destination_domain),
        started_at: unix_ms_time(msg.started_at_unix_ms),
        updated_at: unix_ms_time(msg.updated_at_unix_ms),
        terminal_at: unix_ms_time(msg.terminal_at_unix_ms),
        last_activity_at: unix_ms_time(msg.last_activity_at_unix_ms),
        estimated_completion_at: unix_ms_time(msg.estimated_completion_unix_ms),
        current_step_sequence: msg.current_step_sequence,
        current_progress: msg
            .current_progress
            .as_option()
            .map(flow_progress_from_proto),
        progress_timeline: msg
            .progress_timeline
            .iter()
            .map(flow_timeline_item_from_proto)
            .collect(),
    })
}

/// A flow summary as a realtime flow update carries it. An amount or fee
/// past what [`AssetAmount`] holds is an [`Error::ResponseContract`].
pub fn flow_summary_message_from_proto(msg: &FlowSummaryView) -> Result<LifecycleFlowSummary> {
    flow_summary_from_proto("FlowSummary", msg)
}

/// A read flow summary, refused with `Error::ResponseContract` without its
/// flow ID.
fn required_flow_summary(operation: &str, msg: &FlowSummaryView) -> Result<LifecycleFlowSummary> {
    require_str_id(operation, "flow_id", &msg.flow_id)?;
    flow_summary_from_proto(operation, msg)
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

/// A transaction match, which carries the amount, asset, chain,
/// transactions, route and last activity but none of the summary's fee,
/// progress or other times.
fn flow_tx_match_from_proto(msg: &FlowTxMatchView) -> Result<LifecycleFlowSummary> {
    let (zipped_asset_id, unified_asset_id) = asset_ids(&msg.asset_ids);
    Ok(LifecycleFlowSummary {
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
        zipped_asset_id,
        unified_asset_id,
        polyester_chain_id: msg.polyester_chain_id,
        amount: ledger_amount(
            "ListFlowsByTx",
            "amount_e18",
            &msg.amount_e18,
            unified_asset_id,
        )?,
        source_tx_hash: msg.source_tx_hash.clone(),
        tx_occurrence_index: msg.tx_occurrence_index,
        latest_tx_ref: msg.latest_tx_ref.clone(),
        source_domain: flow_domain_label(&msg.source_domain),
        destination_domain: flow_domain_label(&msg.destination_domain),
        last_activity_at: unix_ms_time(msg.last_activity_at_unix_ms),
        ..Default::default()
    })
}

pub fn flows_by_tx_list_from_proto(msg: &ListFlowsByTxResponse) -> Result<LifecycleFlowsList> {
    Ok(LifecycleFlowsList {
        flows: msg
            .matches
            .iter()
            .map(|flow| {
                require_str_id("ListFlowsByTx", "flow_id", &flow.flow_id)?;
                flow_tx_match_from_proto(flow)
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
        let flow = flow_summary_message_from_proto(&msg).unwrap();
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
            let flow = flow_summary_message_from_proto(&msg).unwrap();
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
        let flow = flow_summary_message_from_proto(&msg).unwrap();
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
            let flow = flow_summary_message_from_proto(&msg).unwrap();
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
        let flow = flow_summary_message_from_proto(&msg).unwrap();
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
        let flow = flow_summary_message_from_proto(&msg).unwrap();
        assert_eq!(flow.lifecycle_reason, "zipper_deposit_delivered_to_wallet");
        assert!(flow.is_terminal && !flow.is_open);
        assert_eq!(flow.latest_step, "transfer");
        let by_number = flow_summary_message_from_proto(&FlowSummaryView {
            flow_id: "flow-wallet".into(),
            lifecycle_reason: buffa::EnumValue::from(106),
            ..Default::default()
        })
        .unwrap();
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
        let flow = flow_summary_message_from_proto(&msg).unwrap();
        assert!(flow.is_delayed);
        assert_eq!(flow.lifecycle_reason, "deposit_source_not_confirmed");

        let on_time = flow_summary_message_from_proto(&FlowSummaryView {
            is_open: true,
            ..Default::default()
        })
        .unwrap();
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
