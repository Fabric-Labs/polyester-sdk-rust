//! Policy decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::{
    ApiPoliciesList, ApiPolicy, SpotMarketRule, SubaccountPoliciesList, SubaccountPolicy,
};
use crate::proto::auth::v1::{
    ApiPolicyView, CreateApiPolicyResponse, CreateSubaccountPolicyResponse, GetApiPolicyResponse,
    GetSubaccountPolicyResponse, ListApiPoliciesResponse, ListSubaccountPoliciesResponse,
    PolicyAction, SpotMarketRule as ProtoSpotMarketRule, SubaccountPolicyView,
    UpdateApiPolicyResponse, UpdateSubaccountPolicyResponse, market_scope,
};

/// Contract label for a market scope: `"ALL"` or `"ALLOWLIST"`, empty when
/// unspecified, and `UNKNOWN(n)` for a value newer than this SDK.
fn market_scope_label(scope: &buffa::EnumValue<market_scope::Value>) -> String {
    match scope.as_known() {
        Some(market_scope::Value::ALL) => "ALL".to_owned(),
        Some(market_scope::Value::ALLOWLIST) => "ALLOWLIST".to_owned(),
        Some(market_scope::Value::UNSPECIFIED) => String::new(),
        None => format!("UNKNOWN({})", scope.to_i32()),
    }
}

/// Granted policy actions by proto name, skipping unset entries;
/// `UNKNOWN(n)` for an action newer than this SDK.
fn policy_actions(actions: &[buffa::EnumValue<PolicyAction>]) -> Vec<String> {
    actions
        .iter()
        .filter(|action| action.to_i32() != 0)
        .map(crate::codecs::decode::enum_proto_name)
        .collect()
}

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
        spot_market_scope: market_scope_label(&msg.spot_market_scope),
        max_order_notional: msg.max_order_notional,
        max_open_orders: msg.max_open_orders,
        trading_halted: msg.trading_halted,
        revision: msg.revision,
        actions: policy_actions(&msg.actions),
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
        spot_market_scope: market_scope_label(&msg.spot_market_scope),
        revision: msg.revision,
        actions: policy_actions(&msg.actions),
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

    /// The account permissions that open deposit-address, sub-account and
    /// Guard Signer calls to API keys read by name and keep their numbers;
    /// `CREATE_DEPOSIT_ADDRESS` sits among the product actions' numbers, so
    /// account permissions are told apart by name, not by number range.
    #[test]
    fn account_permissions_for_deposit_subaccount_and_guard_signer_reads() {
        use crate::proto::auth::v1::PolicyAction;
        use buffa::Enumeration;

        let added = [
            (
                PolicyAction::CREATE_DEPOSIT_ADDRESS,
                "CREATE_DEPOSIT_ADDRESS",
                10,
            ),
            (
                PolicyAction::READ_DEPOSIT_ADDRESSES,
                "READ_DEPOSIT_ADDRESSES",
                0,
            ),
            (PolicyAction::READ_SUBACCOUNTS, "READ_SUBACCOUNTS", 0),
            (
                PolicyAction::READ_SUBACCOUNT_ACTIVITY,
                "READ_SUBACCOUNT_ACTIVITY",
                0,
            ),
            (
                PolicyAction::READ_SUBACCOUNT_MEMBERS,
                "READ_SUBACCOUNT_MEMBERS",
                0,
            ),
            (
                PolicyAction::READ_SUBACCOUNT_INVITES,
                "READ_SUBACCOUNT_INVITES",
                0,
            ),
            (
                PolicyAction::READ_GUARD_SIGNER_STATUS,
                "READ_GUARD_SIGNER_STATUS",
                0,
            ),
        ];
        let api = api_policy_from_proto(&ApiPolicyView {
            actions: added
                .iter()
                .map(|(action, _, _)| (*action).into())
                .collect(),
            ..Default::default()
        });
        let names: Vec<&str> = added.iter().map(|(_, name, _)| *name).collect();
        assert_eq!(api.actions, names);
        for (action, name, number) in added {
            assert_eq!(PolicyAction::from_proto_name(name), Some(action));
            if number != 0 {
                assert_eq!(action.to_i32(), number, "{name}");
            }
        }
    }

    /// The price-alert permissions read by name on both policy kinds and
    /// keep their numbers.
    #[test]
    fn price_alert_policy_actions_read_by_name() {
        use crate::proto::auth::v1::PolicyAction;
        use buffa::Enumeration;

        let actions = vec![
            PolicyAction::READ_PRICE_ALERTS.into(),
            PolicyAction::MANAGE_PRICE_ALERTS.into(),
        ];
        let api = api_policy_from_proto(&ApiPolicyView {
            actions: actions.clone(),
            ..Default::default()
        });
        let sub = subaccount_policy_from_proto(&SubaccountPolicyView {
            actions,
            ..Default::default()
        });
        for decoded in [&api.actions, &sub.actions] {
            assert_eq!(decoded, &["READ_PRICE_ALERTS", "MANAGE_PRICE_ALERTS"]);
        }
        assert_eq!(PolicyAction::READ_PRICE_ALERTS.to_i32(), 36);
        assert_eq!(PolicyAction::MANAGE_PRICE_ALERTS.to_i32(), 37);
    }

    /// Every `PolicyAction`, the POLY-9102 account permissions included,
    /// reads as its proto name and parses back to the same value. Unset
    /// entries are skipped and a value newer than this SDK keeps its number.
    #[test]
    fn poly_9102_policy_actions_round_trip_by_name() {
        use crate::proto::auth::v1::PolicyAction;
        use buffa::Enumeration;

        let known: Vec<PolicyAction> = PolicyAction::values()
            .iter()
            .copied()
            .filter(|action| *action != PolicyAction::UNSPECIFIED)
            .collect();
        for added in [
            PolicyAction::READ_API_KEYS,
            PolicyAction::READ_API_POLICIES,
            PolicyAction::READ_SUBACCOUNT_POLICIES,
            PolicyAction::READ_SUBACCOUNT_PERMISSIONS,
            PolicyAction::READ_VIP_STATUS,
            PolicyAction::READ_SOCIAL_VERIFICATION,
            PolicyAction::READ_PROFILE,
            PolicyAction::READ_ACCOUNT_DIRECTORY,
            PolicyAction::READ_WITHDRAW_VALIDATION,
            PolicyAction::READ_DAILY_CLAIMS,
            PolicyAction::CLAIM_DAILY_REWARD,
            PolicyAction::READ_CHECKPOINTS,
            PolicyAction::READ_REWARDS,
        ] {
            assert!(known.contains(&added), "{added:?}");
        }
        let mut actions: Vec<buffa::EnumValue<PolicyAction>> =
            known.iter().map(|action| (*action).into()).collect();
        actions.push(PolicyAction::UNSPECIFIED.into());
        actions.push(buffa::EnumValue::Unknown(99));

        let api = api_policy_from_proto(&ApiPolicyView {
            actions: actions.clone(),
            ..Default::default()
        });
        let sub = subaccount_policy_from_proto(&SubaccountPolicyView {
            actions,
            ..Default::default()
        });
        for decoded in [&api.actions, &sub.actions] {
            assert_eq!(decoded.len(), known.len() + 1, "{decoded:?}");
            for (name, action) in decoded.iter().zip(&known) {
                assert_eq!(name, action.proto_name());
                assert_eq!(PolicyAction::from_proto_name(name), Some(*action));
            }
            assert_eq!(decoded.last().map(String::as_str), Some("UNKNOWN(99)"));
        }
        assert!(
            api.actions
                .iter()
                .any(|name| name == "READ_WITHDRAW_VALIDATION")
        );
        assert!(api.actions.iter().any(|name| name == "CLAIM_DAILY_REWARD"));
    }
}
