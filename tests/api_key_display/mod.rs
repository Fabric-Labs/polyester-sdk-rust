//! API-key reads keep the contract's `icon`, `color` and `created_by_actor`,
//! read a main-account key's `subaccount_id` as empty (unset or an explicit
//! 0), and `sub_accounts.get` returns the sub-account's API keys when asked
//! for them. Every read goes through the public services against a local
//! server, in binary and JSON; the streams decode through the same decoder.

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa::Message as _;
use polyester::codecs::decode::{api_key_from_bytes, api_keys_list_from_proto};
use polyester::models::ApiKeySummary;
use polyester::proto::auth::v1::{
    ApiKey as ProtoApiKey, ApiKeyStatus, GetApiKeyRequest, GetApiKeyResponse, GetSubaccountRequest,
    GetSubaccountResponse, ListApiKeysResponse, Subaccount,
};
use polyester::services::GetSubaccountOpts;
use polyester::transport::WireFormat;
use polyester::{Client, Config};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];
const SUBACCOUNT: u64 = 42;

fn display_keys() -> Vec<ProtoApiKey> {
    vec![
        ProtoApiKey {
            key_id: "ak_11111111111111111111111111111111".into(),
            label: "bot".into(),
            icon: "🤖".into(),
            color: "violet-500".into(),
            created_by_actor: "alice".into(),
            status: ApiKeyStatus::Active.into(),
            revision: 3,
            ..Default::default()
        },
        // subaccount_id has presence: a server may send an explicit 0.
        ProtoApiKey {
            key_id: "ak_22222222222222222222222222222222".into(),
            label: "zero".into(),
            subaccount_id: Some(0),
            ..Default::default()
        },
        ProtoApiKey {
            key_id: "ak_33333333333333333333333333333333".into(),
            label: "desk".into(),
            subaccount_id: Some(SUBACCOUNT),
            icon: "#".into(),
            created_by_actor: "desk-admin".into(),
            ..Default::default()
        },
    ]
}

/// key_id, subaccount_id, icon, color, created_by_actor.
const WANTS: [(&str, &str, &str, &str, &str); 3] = [
    (
        "ak_11111111111111111111111111111111",
        "",
        "🤖",
        "violet-500",
        "alice",
    ),
    ("ak_22222222222222222222222222222222", "", "", "", ""),
    (
        "ak_33333333333333333333333333333333",
        "j",
        "#",
        "",
        "desk-admin",
    ),
];

fn assert_keys(label: &str, keys: &[ApiKeySummary]) {
    let got: Vec<_> = keys
        .iter()
        .map(|k| {
            (
                k.key_id.as_str(),
                k.subaccount_id.as_str(),
                k.icon.as_str(),
                k.color.as_str(),
                k.created_by_actor.as_str(),
            )
        })
        .collect();
    assert_eq!(got, WANTS.to_vec(), "{label}");
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

fn decode_request<M: buffa::Message + Default + serde::de::DeserializeOwned>(
    wire: WireFormat,
    body: &[u8],
) -> M {
    match wire {
        WireFormat::Binary => M::decode_from_slice(body).expect("binary request"),
        WireFormat::Json => serde_json::from_slice(body).expect("json request"),
    }
}

async fn server(wire: WireFormat) -> (Client, MockHttpServer) {
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        "/auth.v1.ApiKeyService/ListApiKeys" => answer(
            wire,
            &ListApiKeysResponse {
                api_keys: display_keys(),
                ..Default::default()
            },
        ),
        "/auth.v1.ApiKeyService/GetApiKey" => {
            let msg: GetApiKeyRequest = decode_request(wire, &req.body);
            match display_keys().into_iter().find(|k| k.key_id == msg.key_id) {
                Some(key) => answer(
                    wire,
                    &GetApiKeyResponse {
                        api_key: key.into(),
                        ..Default::default()
                    },
                ),
                None => HttpScript::NotFound,
            }
        }
        // API keys only when they were asked for, as the server does.
        "/auth.v1.SubaccountViewService/GetSubaccount" => {
            let msg: GetSubaccountRequest = decode_request(wire, &req.body);
            answer(
                wire,
                &GetSubaccountResponse {
                    subaccount: Subaccount {
                        id: msg.subaccount_id,
                        ..Default::default()
                    }
                    .into(),
                    api_keys: if msg.include_api_keys {
                        display_keys()
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                },
            )
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
    (client, http)
}

#[tokio::test]
async fn api_key_reads_keep_display_fields_and_main_account() {
    for wire in WIRES {
        let (client, _http) = server(wire).await;
        let listed = client.api_keys.list(None).await.expect("list");
        assert_keys(&format!("{wire:?} list"), &listed.keys);

        let mut got = Vec::new();
        for (key_id, ..) in WANTS {
            got.push(
                client
                    .api_keys
                    .get(key_id)
                    .await
                    .expect("get")
                    .expect("key"),
            );
        }
        assert_keys(&format!("{wire:?} get"), &got);
    }
}

#[tokio::test]
async fn sub_accounts_get_returns_api_keys_when_asked() {
    for wire in WIRES {
        let (client, _http) = server(wire).await;
        let plain = client
            .sub_accounts
            .get(SUBACCOUNT, GetSubaccountOpts::default())
            .await
            .expect("get");
        assert!(plain.api_keys.is_empty(), "{wire:?}: keys without asking");

        let with_keys = client
            .sub_accounts
            .get(
                SUBACCOUNT,
                GetSubaccountOpts {
                    include_api_keys: true,
                    ..Default::default()
                },
            )
            .await
            .expect("get with keys");
        assert_eq!(
            with_keys
                .subaccount
                .as_ref()
                .map(|s| s.subaccount_id.as_str()),
            Some("j"),
            "{wire:?}: sub-account"
        );
        assert_keys(&format!("{wire:?} sub_accounts.get"), &with_keys.api_keys);
        // The same decoder as api_keys.list: every contract field is kept.
        let listed = api_keys_list_from_proto(&ListApiKeysResponse {
            api_keys: display_keys(),
            ..Default::default()
        });
        assert_eq!(with_keys.api_keys, listed.keys, "{wire:?}: same as list");
    }
}

#[test]
fn api_key_stream_keeps_display_fields_and_main_account() {
    let got: Vec<_> = display_keys()
        .iter()
        .map(|k| api_key_from_bytes(&k.encode_to_vec()).expect("stream key"))
        .collect();
    assert_keys("stream", &got);
}
