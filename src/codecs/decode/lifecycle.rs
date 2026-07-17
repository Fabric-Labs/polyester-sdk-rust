//! Lifecycle flow decoders.

use buffa::Enumeration;

use crate::codecs::scalars::format_uint64_id;
use crate::models::{LifecycleFlowSummary, LifecycleFlowsList};
use crate::proto::chain::lifecycle::v1::{
    FlowSummaryView, FlowTxMatchView, ListFlowsByTxResponse, ListFlowsResponse,
};

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>) -> String {
    value
        .as_known()
        .map(|e| e.proto_name().to_owned())
        .unwrap_or_default()
}

fn flow_summary_from_proto(msg: &FlowSummaryView) -> LifecycleFlowSummary {
    LifecycleFlowSummary {
        intent_id: msg.flow_id.clone(),
        flow_kind: enum_label(&msg.flow_kind),
        latest_step: enum_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        owner_account_id: format_uint64_id(msg.owner_account_id),
        smart_account_address: msg.source_address.clone(),
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
        flow_kind: enum_label(&msg.flow_kind),
        latest_step: enum_label(&msg.current_step),
        is_open: msg.is_open,
        is_terminal: msg.is_terminal,
        owner_account_id: String::new(),
        smart_account_address: msg.source_address.clone(),
    }
}

pub fn flows_by_tx_list_from_proto(msg: &ListFlowsByTxResponse) -> LifecycleFlowsList {
    LifecycleFlowsList {
        flows: msg.matches.iter().map(flow_tx_match_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::chain::lifecycle::v1::{FlowKind as FlowKindEnum, FlowStep};

    #[test]
    fn flow_summary_maps_fields() {
        let msg = FlowSummaryView {
            flow_id: "flow-abc".into(),
            flow_kind: FlowKindEnum::KindDeposit.into(),
            current_step: FlowStep::Settlement.into(),
            is_open: false,
            is_terminal: true,
            owner_account_id: 99,
            source_address: "0xabc".into(),
            ..Default::default()
        };
        let flow = flow_summary_message_from_proto(&msg);
        assert_eq!(flow.intent_id, "flow-abc");
        assert!(flow.is_terminal);
        assert_eq!(flow.smart_account_address, "0xabc");
        assert_eq!(flow.owner_account_id, format_uint64_id(99));
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
    }
}
