//! `sub_accounts.get` returns the sub-account's members, invites and
//! attached policy when they are asked for and the server sends them, read
//! as `sub_accounts.list_members`, `sub_accounts.list_invites` and the
//! sub-account policy stream read them. Every read goes through the public
//! service against a local server, in binary and JSON.

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa::Message as _;
use polyester::codecs::decode::{
    subaccount_invites_list_from_proto, subaccount_members_list_from_proto,
    subaccount_policy_from_bytes,
};
use polyester::codecs::format_id;
use polyester::models::{SubAccountInvite, SubAccountMember};
use polyester::proto::auth::v1::{
    GetSubaccountRequest, GetSubaccountResponse, ListSubaccountInvitesResponse,
    ListSubaccountMembersResponse, PolicyAction, SpotMarketRule, Subaccount, SubaccountInvite,
    SubaccountInviteDirection, SubaccountInviteStatus, SubaccountMemberView, SubaccountPolicyView,
    SubaccountRole, market_scope,
};
use polyester::services::GetSubaccountOpts;
use polyester::transport::WireFormat;
use polyester::{Client, Config};
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];
const SUBACCOUNT: u64 = 42;

fn members() -> Vec<SubaccountMemberView> {
    vec![
        SubaccountMemberView {
            account_id: 7,
            role: SubaccountRole::OWNER.into(),
            username: "alice".into(),
            ..Default::default()
        },
        SubaccountMemberView {
            account_id: 9,
            role: SubaccountRole::TRADER.into(),
            username: "bob".into(),
            ..Default::default()
        },
    ]
}

fn invites() -> Vec<SubaccountInvite> {
    vec![SubaccountInvite {
        id: 11,
        subaccount_id: SUBACCOUNT,
        grantee_account_id: 12,
        inviter_account_id: 7,
        role: SubaccountRole::VIEWER.into(),
        status: SubaccountInviteStatus::SUBACCOUNT_INVITE_STATUS_PENDING.into(),
        ..Default::default()
    }]
}

fn policy() -> SubaccountPolicyView {
    SubaccountPolicyView {
        id: 5,
        name: "desk".into(),
        description: "desk limits".into(),
        spot_markets: vec![SpotMarketRule {
            symbol_id: 1,
            ..Default::default()
        }],
        spot_market_scope: market_scope::Value::ALLOWLIST.into(),
        actions: vec![
            PolicyAction::TRADE_SPOT.into(),
            PolicyAction::READ_BALANCES.into(),
        ],
        max_order_notional: 1_000_000,
        max_open_orders: 20,
        trading_halted: true,
        revision: 4,
        ..Default::default()
    }
}

fn answer<M: buffa::Message + serde::Serialize>(wire: WireFormat, msg: &M) -> HttpScript {
    let (content_type, body) = match wire {
        WireFormat::Binary => ("application/proto", msg.encode_to_vec()),
        WireFormat::Json => ("application/json", serde_json::to_vec(msg).unwrap()),
    };
    HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), content_type.into())],
        body,
    }
}

fn decode_request(wire: WireFormat, body: &[u8]) -> GetSubaccountRequest {
    match wire {
        WireFormat::Binary => {
            GetSubaccountRequest::decode_from_slice(body).expect("binary request")
        }
        WireFormat::Json => serde_json::from_slice(body).expect("json request"),
    }
}

type Requests = Arc<Mutex<Vec<GetSubaccountRequest>>>;

/// Each expansion is answered only when it was asked for, as the server
/// does. `with_policy` is false for a sub-account with no attached policy.
async fn server(wire: WireFormat, with_policy: bool) -> (Client, MockHttpServer, Requests) {
    let requests: Requests = Arc::default();
    let seen = Arc::clone(&requests);
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        "/auth.v1.SubaccountViewService/GetSubaccount" => {
            let msg = decode_request(wire, &req.body);
            let resp = GetSubaccountResponse {
                subaccount: Subaccount {
                    id: msg.subaccount_id,
                    ..Default::default()
                }
                .into(),
                members: if msg.include_members {
                    members()
                } else {
                    Vec::new()
                },
                invites: if msg.include_invites {
                    invites()
                } else {
                    Vec::new()
                },
                policy: if msg.include_policy && with_policy {
                    policy().into()
                } else {
                    Default::default()
                },
                ..Default::default()
            };
            seen.lock().unwrap().push(msg);
            answer(wire, &resp)
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: wire,
        ..Default::default()
    })
    .expect("client");
    (client, http, requests)
}

#[tokio::test]
async fn sub_accounts_get_returns_members_invites_and_policy_when_asked() {
    for wire in WIRES {
        let (client, _http, requests) = server(wire, true).await;
        let plain = client
            .sub_accounts
            .get(SUBACCOUNT, GetSubaccountOpts::default())
            .await
            .expect("get");
        assert!(plain.members.is_empty(), "{wire:?}: members without asking");
        assert!(plain.invites.is_empty(), "{wire:?}: invites without asking");
        assert_eq!(plain.policy, None, "{wire:?}: policy without asking");

        let got = client
            .sub_accounts
            .get(
                SUBACCOUNT,
                GetSubaccountOpts {
                    include_members: true,
                    include_invites: true,
                    include_policy: true,
                    invites_direction: "incoming".into(),
                    ..Default::default()
                },
            )
            .await
            .expect("get with expansions");
        assert_eq!(
            got.subaccount.as_ref().map(|s| s.subaccount_id.as_str()),
            Some(format_id(SUBACCOUNT).as_str()),
            "{wire:?}: sub-account"
        );

        let want_members = vec![
            SubAccountMember {
                grantee_account_id: format_id(7),
                role: "OWNER".into(),
            },
            SubAccountMember {
                grantee_account_id: format_id(9),
                role: "TRADER".into(),
            },
        ];
        assert_eq!(got.members, want_members, "{wire:?}: members");
        // The same decoders as list_members and list_invites.
        let listed_members = subaccount_members_list_from_proto(&ListSubaccountMembersResponse {
            members: members(),
            ..Default::default()
        });
        assert_eq!(
            got.members, listed_members.members,
            "{wire:?}: as list_members"
        );

        let want_invites = vec![SubAccountInvite {
            invite_id: format_id(11),
            grantee_account_id: format_id(12),
            role: "VIEWER".into(),
            status: "SUBACCOUNT_INVITE_STATUS_PENDING".into(),
        }];
        assert_eq!(got.invites, want_invites, "{wire:?}: invites");
        let listed_invites = subaccount_invites_list_from_proto(&ListSubaccountInvitesResponse {
            invites: invites(),
            ..Default::default()
        });
        assert_eq!(
            got.invites, listed_invites.invites,
            "{wire:?}: as list_invites"
        );

        // The same decoder as the sub-account policy stream.
        let p = got.policy.as_ref().expect("policy");
        let streamed = subaccount_policy_from_bytes(&policy().encode_to_vec()).expect("stream");
        assert_eq!(p, &streamed, "{wire:?}: as the policy stream");
        assert_eq!(p.policy_id, format_id(5), "{wire:?}");
        assert_eq!(p.name, "desk", "{wire:?}");
        assert_eq!(p.description, "desk limits", "{wire:?}");
        assert_eq!(p.spot_market_scope, "ALLOWLIST", "{wire:?}");
        assert_eq!(p.spot_markets.len(), 1, "{wire:?}");
        assert_eq!(p.spot_markets[0].symbol_id, 1, "{wire:?}");
        assert_eq!(p.actions, ["TRADE_SPOT", "READ_BALANCES"], "{wire:?}");
        assert_eq!(p.max_order_notional, 1_000_000, "{wire:?}");
        assert_eq!(p.max_open_orders, 20, "{wire:?}");
        assert!(p.trading_halted, "{wire:?}");
        assert_eq!(p.revision, 4, "{wire:?}");

        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), 2, "{wire:?}");
        let (first, second) = (&seen[0], &seen[1]);
        assert!(
            !first.include_members && !first.include_invites && !first.include_policy,
            "{wire:?}: first request asked for expansions"
        );
        assert!(
            second.include_members && second.include_invites && second.include_policy,
            "{wire:?}: second request"
        );
        assert!(
            !second.include_api_keys && !second.include_balances,
            "{wire:?}: second request"
        );
        assert_eq!(second.subaccount_id, SUBACCOUNT, "{wire:?}");
        assert_eq!(
            second.invites_direction.to_i32(),
            SubaccountInviteDirection::INCOMING as i32,
            "{wire:?}: invites direction"
        );
    }
}

/// A sub-account with no attached policy reads `policy` as `None` even when
/// it was asked for.
#[tokio::test]
async fn sub_accounts_get_without_attached_policy_reads_none() {
    for wire in WIRES {
        let (client, _http, _requests) = server(wire, false).await;
        let got = client
            .sub_accounts
            .get(
                SUBACCOUNT,
                GetSubaccountOpts {
                    include_policy: true,
                    ..Default::default()
                },
            )
            .await
            .expect("get");
        assert_eq!(got.policy, None, "{wire:?}");
        assert!(got.members.is_empty() && got.invites.is_empty(), "{wire:?}");
    }
}
