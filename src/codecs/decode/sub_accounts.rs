//! Sub-account decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::{SubAccount, SubAccountsList};
use crate::proto::auth::v1::{ListSubaccountsResponse, Subaccount};

fn timestamp_ms(ts: Option<&buffa_types::google::protobuf::Timestamp>) -> i64 {
    match ts {
        Some(t) => t.seconds.saturating_mul(1000) + (t.nanos as i64) / 1_000_000,
        None => 0,
    }
}

pub fn subaccount_from_proto(msg: &Subaccount) -> SubAccount {
    SubAccount {
        subaccount_id: format_uint64_id(msg.id),
        label: msg.label.clone(),
        smart_account_address: msg.smart_account_address.clone(),
        status: msg.status.clone(),
        updated_at_ms: timestamp_ms(msg.updated_at.as_option()),
    }
}

pub fn subaccounts_list_from_proto(msg: &ListSubaccountsResponse) -> SubAccountsList {
    SubAccountsList {
        subaccounts: msg.subaccounts.iter().map(subaccount_from_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subaccounts_list_maps_fields() {
        let msg = ListSubaccountsResponse {
            subaccounts: vec![Subaccount {
                id: 12,
                label: "trading".into(),
                smart_account_address: "0xabc".into(),
                status: "active".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccounts_list_from_proto(&msg);
        assert_eq!(result.subaccounts.len(), 1);
        assert_eq!(result.subaccounts[0].subaccount_id, format_uint64_id(12));
        assert_eq!(result.subaccounts[0].smart_account_address, "0xabc");
    }
}
