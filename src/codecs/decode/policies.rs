//! Policy decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::{ApiPoliciesList, ApiPolicy, SubaccountPoliciesList, SubaccountPolicy};
use crate::proto::auth::v1::{
    ApiPolicyView, ListApiPoliciesResponse, ListSubaccountPoliciesResponse, SubaccountPolicyView,
};

fn subaccount_policy_from_proto(msg: &SubaccountPolicyView) -> SubaccountPolicy {
    SubaccountPolicy {
        policy_id: format_uint64_id(msg.id),
        name: msg.name.clone(),
        description: msg.description.clone(),
    }
}

pub fn subaccount_policies_list_from_proto(
    msg: &ListSubaccountPoliciesResponse,
) -> SubaccountPoliciesList {
    SubaccountPoliciesList {
        policies: msg
            .policies
            .iter()
            .map(subaccount_policy_from_proto)
            .collect(),
    }
}

fn api_policy_from_proto(msg: &ApiPolicyView) -> ApiPolicy {
    ApiPolicy {
        policy_id: format_uint64_id(msg.id),
        name: msg.name.clone(),
        description: msg.description.clone(),
    }
}

pub fn api_policies_list_from_proto(msg: &ListApiPoliciesResponse) -> ApiPoliciesList {
    ApiPoliciesList {
        policies: msg.policies.iter().map(api_policy_from_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subaccount_policies_list_maps_ids() {
        let msg = ListSubaccountPoliciesResponse {
            policies: vec![SubaccountPolicyView {
                id: 42,
                name: "default".into(),
                description: "desc".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccount_policies_list_from_proto(&msg);
        assert_eq!(result.policies.len(), 1);
        assert_eq!(result.policies[0].policy_id, format_uint64_id(42));
        assert_eq!(result.policies[0].name, "default");
    }

    #[test]
    fn api_policies_list_maps_ids() {
        let msg = ListApiPoliciesResponse {
            policies: vec![ApiPolicyView {
                id: 7,
                name: "read-only".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = api_policies_list_from_proto(&msg);
        assert_eq!(result.policies.len(), 1);
        assert_eq!(result.policies[0].policy_id, format_uint64_id(7));
    }
}
