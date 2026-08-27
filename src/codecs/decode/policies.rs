//! Policy decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    ApiPoliciesList, ApiPolicy, SpotMarketRule, SubaccountPoliciesList, SubaccountPolicy,
};
use crate::proto::auth::v1::{
    ApiPolicyView, CreateApiPolicyResponse, CreateSubaccountPolicyResponse, GetApiPolicyResponse,
    GetSubaccountPolicyResponse, ListApiPoliciesResponse, ListSubaccountPoliciesResponse,
    SpotMarketRule as ProtoSpotMarketRule, SubaccountPolicyView, UpdateApiPolicyResponse,
    UpdateSubaccountPolicyResponse,
};

fn spot_market_rule_from_proto(msg: &ProtoSpotMarketRule) -> SpotMarketRule {
    SpotMarketRule {
        symbol_id: msg.symbol_id,
        symbol: String::new(),
    }
}

pub fn subaccount_policy_from_proto(msg: &SubaccountPolicyView) -> SubaccountPolicy {
    SubaccountPolicy {
        policy_id: format_uint64_id(msg.id),
        name: msg.name.clone(),
        description: msg.description.clone(),
        spot_markets: msg
            .spot_markets
            .iter()
            .map(spot_market_rule_from_proto)
            .collect(),
        revision: msg.revision,
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

pub fn get_subaccount_policy_from_proto(
    msg: &GetSubaccountPolicyResponse,
) -> Option<SubaccountPolicy> {
    msg.policy.as_option().map(subaccount_policy_from_proto)
}

pub fn create_subaccount_policy_from_proto(
    msg: &CreateSubaccountPolicyResponse,
) -> Option<SubaccountPolicy> {
    msg.policy.as_option().map(subaccount_policy_from_proto)
}

pub fn update_subaccount_policy_from_proto(
    msg: &UpdateSubaccountPolicyResponse,
) -> Option<SubaccountPolicy> {
    msg.policy.as_option().map(subaccount_policy_from_proto)
}

pub fn api_policy_from_proto(msg: &ApiPolicyView) -> ApiPolicy {
    ApiPolicy {
        policy_id: format_uint64_id(msg.id),
        name: msg.name.clone(),
        description: msg.description.clone(),
        spot_markets: msg
            .spot_markets
            .iter()
            .map(spot_market_rule_from_proto)
            .collect(),
        revision: msg.revision,
    }
}

pub fn api_policies_list_from_proto(msg: &ListApiPoliciesResponse) -> ApiPoliciesList {
    ApiPoliciesList {
        policies: msg.policies.iter().map(api_policy_from_proto).collect(),
    }
}

pub fn get_api_policy_from_proto(msg: &GetApiPolicyResponse) -> Option<ApiPolicy> {
    msg.policy.as_option().map(api_policy_from_proto)
}

pub fn create_api_policy_from_proto(msg: &CreateApiPolicyResponse) -> Option<ApiPolicy> {
    msg.policy.as_option().map(api_policy_from_proto)
}

pub fn update_api_policy_from_proto(msg: &UpdateApiPolicyResponse) -> Option<ApiPolicy> {
    msg.policy.as_option().map(api_policy_from_proto)
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
                revision: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = subaccount_policies_list_from_proto(&msg);
        assert_eq!(result.policies.len(), 1);
        assert_eq!(result.policies[0].policy_id, format_uint64_id(42));
        assert_eq!(result.policies[0].name, "default");
        assert_eq!(result.policies[0].revision, 3);
        assert!(result.policies[0].spot_markets.is_empty());
    }

    #[test]
    fn api_policies_list_maps_ids() {
        let msg = ListApiPoliciesResponse {
            policies: vec![ApiPolicyView {
                id: 7,
                name: "read-only".into(),
                revision: 2,
                spot_markets: vec![crate::proto::auth::v1::SpotMarketRule {
                    symbol_id: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = api_policies_list_from_proto(&msg);
        assert_eq!(result.policies.len(), 1);
        assert_eq!(result.policies[0].policy_id, format_uint64_id(7));
        assert_eq!(result.policies[0].revision, 2);
        assert_eq!(result.policies[0].spot_markets.len(), 1);
        assert_eq!(result.policies[0].spot_markets[0].symbol_id, 3);
        assert_eq!(result.policies[0].spot_markets[0].symbol, "");
    }
}
