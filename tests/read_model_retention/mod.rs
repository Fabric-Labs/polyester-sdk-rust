//! Typed reads keep contract fields the server sends, through the public
//! services against a local server, in binary and JSON, and through the
//! stream decoders: an API key's policy, sub-account, IP allowlist and expiry
//! (POLY-9894), an order's batch replace ID (POLY-9896), and each
//! address-book view entry's own revision (POLY-9893).

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa::Message as _;
use buffa_types::google::protobuf::Timestamp;
use polyester::codecs::decode::{api_key_from_bytes, orders_unit_from_proto};
use polyester::models::{ApiKeySummary, Order, OrderKey};
use polyester::proto::auth::v1::{
    AddressBookEntriesView, ApiKey as ProtoApiKey, ApiKeyStatus, ExternalAddressBookEntry,
    GetAddressBookViewRequest, GetAddressBookViewResponse, GetApiKeyResponse,
    InternalAddressBookEntry, ListApiKeysResponse,
};
use polyester::proto::orders::v1::{
    GetOpenOrdersResponse, GetOrderHistoryResponse, GetOrderResponse, Order as ProtoOrder,
    OrderStatus,
};
use polyester::proto::privatews::v1::Unit as ProtoUnit;
use polyester::transport::WireFormat;
use polyester::{Client, Config};
use serde_json::{Value, json};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];
const POPULATED_KEY_ID: &str = "ak_11111111111111111111111111111111";
const ABSENT_KEY_ID: &str = "ak_22222222222222222222222222222222";

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

struct Fixture {
    key: ProtoApiKey,
    order: ProtoOrder,
}

async fn server(wire: WireFormat, fixture: Fixture) -> (Client, MockHttpServer) {
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        "/auth.v1.ApiKeyService/GetApiKey" => answer(
            wire,
            &GetApiKeyResponse {
                api_key: fixture.key.clone().into(),
                ..Default::default()
            },
        ),
        "/auth.v1.ApiKeyService/ListApiKeys" => answer(
            wire,
            &ListApiKeysResponse {
                api_keys: vec![fixture.key.clone(), absent_key()],
                ..Default::default()
            },
        ),
        "/orders.v1.OrdersReadService/GetOrder" => answer(
            wire,
            &GetOrderResponse {
                order: fixture.order.clone().into(),
                ..Default::default()
            },
        ),
        "/orders.v1.OrdersReadService/GetOpenOrders" => answer(
            wire,
            &GetOpenOrdersResponse {
                orders: vec![fixture.order.clone()],
                ..Default::default()
            },
        ),
        "/orders.v1.OrdersReadService/GetOrderHistory" => answer(
            wire,
            &GetOrderHistoryResponse {
                orders: vec![fixture.order.clone()],
                ..Default::default()
            },
        ),
        "/auth.v1.AddressBookService/GetAddressBookView" => answer(wire, &address_book_view()),
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

// --- POLY-9894: API-key policy, sub-account, IP allowlist and expiry -------

const PUBLIC_KEY: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

fn expiry(seconds: i64, nanos: i32) -> Timestamp {
    Timestamp {
        seconds,
        nanos,
        ..Default::default()
    }
}

fn populated_key(policy_id: u64, expires: Timestamp) -> ProtoApiKey {
    ProtoApiKey {
        key_id: POPULATED_KEY_ID.into(),
        label: "bot".into(),
        status: ApiKeyStatus::Active.into(),
        subaccount_id: Some(42),
        policy_id: Some(policy_id),
        ip_whitelist: vec!["192.0.2.1".into(), "198.51.100.0/24".into()],
        public_key_ed25519: PUBLIC_KEY.to_vec(),
        expires_at: expires.into(),
        revision: 3,
        ..Default::default()
    }
}

fn absent_key() -> ProtoApiKey {
    ProtoApiKey {
        key_id: ABSENT_KEY_ID.into(),
        label: "plain".into(),
        revision: 1,
        ..Default::default()
    }
}

/// Policy ID, its public ID (as `ApiPolicy::policy_id`), and the expiry.
fn key_cases() -> Vec<(u64, &'static str, Timestamp)> {
    vec![
        (7, "8", expiry(4_102_444_800, 0)),
        (19, "L", expiry(4_133_980_799, 123_456_789)),
    ]
}

fn assert_populated(label: &str, key: &ApiKeySummary, policy: &str, expires: &Timestamp) {
    assert_eq!(
        (key.key_id.as_str(), key.label.as_str(), key.revision),
        (POPULATED_KEY_ID, "bot", 3),
        "{label}: identity"
    );
    assert_eq!(key.policy_id, policy, "{label}: policy_id");
    assert_eq!(key.subaccount_id, "j", "{label}: subaccount_id");
    assert_eq!(
        key.ip_whitelist,
        vec!["192.0.2.1".to_owned(), "198.51.100.0/24".to_owned()],
        "{label}: ip_whitelist"
    );
    assert_eq!(
        key.expires_at.as_ref(),
        Some(expires),
        "{label}: expires_at"
    );
    assert_eq!(
        key.public_key_ed25519,
        hex::encode(PUBLIC_KEY),
        "{label}: public key"
    );
}

fn assert_absent(label: &str, key: &ApiKeySummary) {
    assert_eq!(key.key_id, ABSENT_KEY_ID, "{label}: identity");
    assert_eq!(key.policy_id, "", "{label}: policy_id");
    assert_eq!(key.subaccount_id, "", "{label}: subaccount_id");
    assert!(key.ip_whitelist.is_empty(), "{label}: ip_whitelist");
    assert_eq!(key.expires_at, None, "{label}: expires_at");
    assert_eq!(key.public_key_ed25519, "", "{label}: public key");
}

#[tokio::test]
async fn api_key_reads_keep_policy_subaccount_allowlist_and_expiry() {
    for wire in WIRES {
        for (policy_id, policy, expires) in key_cases() {
            let label = format!("{wire:?}/policy {policy_id}");
            let (client, _http) = server(
                wire,
                Fixture {
                    key: populated_key(policy_id, expires.clone()),
                    order: batch_order(0),
                },
            )
            .await;
            let got = client
                .api_keys
                .get(POPULATED_KEY_ID)
                .await
                .expect("get")
                .expect("key");
            assert_populated(&format!("{label} get"), &got, policy, &expires);

            let listed = client.api_keys.list(None).await.expect("list");
            assert_eq!(listed.keys.len(), 2, "{label}: listed keys");
            assert_populated(&format!("{label} list"), &listed.keys[0], policy, &expires);
            assert_absent(&format!("{label} list"), &listed.keys[1]);
        }
    }
}

#[test]
fn api_key_stream_keeps_policy_subaccount_allowlist_and_expiry() {
    for (policy_id, policy, expires) in key_cases() {
        let payload = populated_key(policy_id, expires.clone()).encode_to_vec();
        let got = api_key_from_bytes(&payload).expect("stream key");
        assert_populated("stream", &got, policy, &expires);
    }
    let got = api_key_from_bytes(&absent_key().encode_to_vec()).expect("stream key");
    assert_absent("stream", &got);
}

// --- POLY-9896: order batch_request_id -------------------------------------

const BATCH_CASES: [(u64, &str); 3] = [
    (7, "8"),
    (18_446_744_073_709_551_614, "jpXCZedGfVP"),
    (0, ""),
];

fn batch_order(batch_request_id: u64) -> ProtoOrder {
    ProtoOrder {
        order_id: 901,
        symbol_id: 1,
        client_order_id: "c-901".into(),
        status: OrderStatus::Working.into(),
        version: 4,
        batch_request_id,
        ..Default::default()
    }
}

fn assert_batch(label: &str, order: &Order, want: &str) {
    assert_eq!(
        (order.client_order_id.as_str(), order.version),
        ("c-901", 4),
        "{label}: identity"
    );
    assert_eq!(order.batch_request_id, want, "{label}: batch_request_id");
}

#[tokio::test]
async fn order_reads_keep_batch_request_id() {
    for wire in WIRES {
        for (id, want) in BATCH_CASES {
            let label = format!("{wire:?}/{id}");
            let (client, _http) = server(
                wire,
                Fixture {
                    key: absent_key(),
                    order: batch_order(id),
                },
            )
            .await;
            let got = client
                .orders
                .get(OrderKey::OrderId("901".into()), None)
                .await
                .expect("get")
                .order
                .expect("order");
            assert_batch(&format!("{label} get"), &got, want);
            let open = client.orders.list_open(None).await.expect("list_open");
            assert_batch(&format!("{label} list_open"), &open.orders[0], want);
            let history = client
                .orders
                .list_history(None, None)
                .await
                .expect("list_history");
            assert_batch(&format!("{label} list_history"), &history.orders[0], want);
        }
    }
}

#[test]
fn order_stream_keeps_batch_request_id() {
    for (id, want) in BATCH_CASES {
        let unit = ProtoUnit {
            orders: vec![batch_order(id)],
            ..Default::default()
        };
        let decoded = orders_unit_from_proto(&unit).expect("unit");
        assert_batch("stream", &decoded.orders[0], want);
    }
}

// --- POLY-9893: address-book view entry revisions --------------------------

fn address_book_view() -> GetAddressBookViewResponse {
    GetAddressBookViewResponse {
        entries: AddressBookEntriesView {
            external: vec![
                ExternalAddressBookEntry {
                    address_book_entry_id: 11,
                    label: "ext-11".into(),
                    revision: 7,
                    ..Default::default()
                },
                ExternalAddressBookEntry {
                    address_book_entry_id: 12,
                    label: "ext-12".into(),
                    ..Default::default()
                },
            ],
            internal: vec![
                InternalAddressBookEntry {
                    address_book_entry_id: 21,
                    label: "int-21".into(),
                    revision: 19,
                    ..Default::default()
                },
                InternalAddressBookEntry {
                    address_book_entry_id: 22,
                    label: "int-22".into(),
                    revision: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
        .into(),
        view_revision: 99,
        ..Default::default()
    }
}

/// The Rust view is the response as protobuf JSON in `ApiData::raw`, where
/// each entry keeps its own revision; protobuf JSON omits a 0.
#[tokio::test]
async fn address_book_view_entries_keep_their_own_revision() {
    for wire in WIRES {
        let (client, _http) = server(
            wire,
            Fixture {
                key: absent_key(),
                order: batch_order(0),
            },
        )
        .await;
        let view = client
            .address_book
            .get_view(GetAddressBookViewRequest::default())
            .await
            .expect("get_view");
        let entries = &view.raw["entries"];
        let revisions = |kind: &str| -> Vec<Value> {
            entries[kind]
                .as_array()
                .unwrap_or_else(|| panic!("{wire:?}: {kind} entries in {}", view.raw))
                .iter()
                .map(|row| row["revision"].clone())
                .collect()
        };
        assert_eq!(
            revisions("external"),
            vec![json!("7"), Value::Null],
            "{wire:?}"
        );
        assert_eq!(
            revisions("internal"),
            vec![json!("19"), json!("2")],
            "{wire:?}"
        );
        assert_eq!(view.raw["viewRevision"], json!("99"), "{wire:?}");
    }
}
