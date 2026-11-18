//! A string public ID is base58 only, read as the server reads it, and is
//! never trimmed: a digit-only string such as `"23"` is base58 (60), a
//! decimal-looking one with a `0` such as `"100"` is refused, and so is any
//! whitespace or a spelling other than the canonical one (`"123"`, a leading
//! base58 zero). A typed `u64` ID is sent as given.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{ListTriggerEventsOpts, OrderKey, SubAccountScope};
use polyester::proto::orders::v1::{GetOrderRequest, GetOrderResponse, Order, get_order_request};
use polyester::proto::triggers::v1::{ListTriggerEventsRequest, ListTriggerEventsResponse};
use polyester::{Client, Config, Error};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The IDs each request named: `(order or trigger ID, sub-account ID)`.
type Sent = Arc<Mutex<Vec<(u64, Option<u64>)>>>;

async fn server() -> (Client, MockHttpServer, Sent) {
    let sent: Sent = Arc::new(Mutex::new(Vec::new()));
    let log = sent.clone();
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        "/orders.v1.OrdersReadService/GetOrder" => {
            let request = GetOrderRequest::decode_from_slice(&req.body).unwrap();
            let id = match request.key {
                Some(get_order_request::Key::OrderId(id)) => id,
                other => panic!("GetOrder without an order ID: {other:?}"),
            };
            log.lock().unwrap().push((id, request.subaccount_id));
            connect_proto_ok(&GetOrderResponse {
                order: Some(Order {
                    order_id: id,
                    symbol_id: 1,
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            })
        }
        "/triggers.v1.TriggersService/ListTriggerEvents" => {
            let request = ListTriggerEventsRequest::decode_from_slice(&req.body).unwrap();
            log.lock()
                .unwrap()
                .push((request.trigger_id, request.subaccount_id));
            connect_proto_ok(&ListTriggerEventsResponse::default())
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    (client, http, sent)
}

/// Decimal-looking IDs, IDs with whitespace and non-canonical spellings are
/// refused, naming the field, and nothing is sent.
#[tokio::test]
async fn string_ids_that_are_not_bare_base58_are_refused_before_sending() {
    let (client, http, _) = server().await;
    for id in [
        "100", "00023", "0", " 23", "23 ", "\t23", "2 3", "123", "1123",
    ] {
        match client
            .orders
            .get(OrderKey::OrderId(id.into()), SubAccountScope::Default)
            .await
        {
            Err(Error::Validation(message)) => {
                assert!(message.contains("order_id"), "{id:?}: {message}")
            }
            other => panic!("orders.get({id:?}): want Error::Validation, got {other:?}"),
        }
        match client
            .triggers
            .list_events_by_id(id, ListTriggerEventsOpts::default())
            .await
        {
            Err(Error::Validation(message)) => {
                assert!(message.contains("trigger_id"), "{id:?}: {message}")
            }
            other => panic!("list_events_by_id({id:?}): want Error::Validation, got {other:?}"),
        }
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 0);
}

/// A digit-only string is base58: `"23"` names 60. A typed `u64` ID, 100 or
/// 23, is sent as that number.
#[tokio::test]
async fn digit_only_strings_are_base58_and_typed_ids_are_sent_as_given() {
    let (client, _http, sent) = server().await;
    for id in ["23"] {
        client
            .orders
            .get(OrderKey::OrderId(id.into()), SubAccountScope::Default)
            .await
            .unwrap();
        client
            .triggers
            .list_events_by_id(id, ListTriggerEventsOpts::default())
            .await
            .unwrap();
    }
    assert_eq!(
        std::mem::take(&mut *sent.lock().unwrap()),
        vec![(60, None), (60, None)]
    );
    for id in [100u64, 23] {
        client
            .triggers
            .list_events(ListTriggerEventsRequest {
                trigger_id: id,
                subaccount_id: Some(id),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .orders
            .get(OrderKey::OrderId("2".into()), SubAccountScope::Id(id))
            .await
            .unwrap();
    }
    assert_eq!(
        std::mem::take(&mut *sent.lock().unwrap()),
        vec![
            (100, Some(100)),
            (1, Some(100)),
            (23, Some(23)),
            (1, Some(23))
        ]
    );
}
