//! A public ID that decodes to 0 names nothing: `"1"` is the base58 form of
//! 0, `"0"` and `"00"` the decimal. Every call that takes one refuses it with
//! `Error::Validation` and sends no request.

use super::hardening_support::{HttpScript, MockHttpServer};
use polyester::models::{
    CancelOrderParams, ListTriggerEventsOpts, ModifyTriggerParams, OrderKey, ResumeTriggerParams,
    SubAccountScope,
};
use polyester::{Client, Config, Error};
use std::sync::atomic::Ordering;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ZERO_IDS: [&str; 3] = ["1", "0", "00"];

fn assert_refused<T: std::fmt::Debug>(call: &str, id: &str, result: polyester::Result<T>) {
    match result {
        Err(Error::Validation(message)) => assert!(
            message.contains("non-zero"),
            "{call}({id:?}): unexpected message {message}"
        ),
        other => panic!("{call}({id:?}): want Error::Validation, got {other:?}"),
    }
}

#[tokio::test]
async fn calls_refuse_a_public_id_that_decodes_to_zero_before_sending() {
    let http = MockHttpServer::spawn(|_| HttpScript::NotFound).await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    let triggers = &client.triggers;
    let orders = &client.orders;
    let scope = || SubAccountScope::Default;
    for id in ZERO_IDS {
        assert_refused(
            "triggers.get_by_id",
            id,
            triggers.get_by_id(id, scope()).await,
        );
        assert_refused(
            "triggers.cancel_by_id",
            id,
            triggers.cancel_by_id(id, scope()).await,
        );
        assert_refused(
            "triggers.pause_by_id",
            id,
            triggers.pause_by_id(id, scope()).await,
        );
        assert_refused(
            "triggers.resume_with",
            id,
            triggers
                .resume_with(ResumeTriggerParams {
                    trigger_id: id.into(),
                    subaccount_id: scope(),
                    symbol: None,
                    symbol_id: Some(1),
                })
                .await,
        );
        assert_refused(
            "triggers.modify",
            id,
            triggers
                .modify(ModifyTriggerParams {
                    trigger_id: id.into(),
                    subaccount_id: scope(),
                    symbol: None,
                    symbol_id: Some(1),
                    trigger_price: None,
                    activation_price: None,
                    trailing_distance_ticks: None,
                    trailing_distance_bps: None,
                    max_slippage_ticks: None,
                    max_slippage_bps: Some(10),
                })
                .await,
        );
        assert_refused(
            "triggers.list_events_by_id",
            id,
            triggers
                .list_events_by_id(id, ListTriggerEventsOpts::default())
                .await,
        );
        assert_refused(
            "triggers.cancel_oco_by_id",
            id,
            triggers.cancel_oco_by_id(id, scope()).await,
        );
        assert_refused(
            "orders.get",
            id,
            orders.get(OrderKey::OrderId(id.into()), scope()).await,
        );
        assert_refused(
            "orders.cancel_with",
            id,
            orders
                .cancel_with(CancelOrderParams {
                    key: OrderKey::OrderId(id.into()),
                    symbol: None,
                    symbol_id: Some(1),
                    subaccount_id: scope(),
                })
                .await,
        );
        assert_refused(
            "orders.get_batch_replace_status",
            id,
            orders.get_batch_replace_status(id, scope()).await,
        );
    }
    assert_eq!(
        http.requests.load(Ordering::SeqCst),
        0,
        "a request was sent for a zero ID"
    );
}
