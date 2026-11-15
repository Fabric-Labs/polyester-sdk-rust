//! Clearing an order's attached risk on modify and batch replace.
//!
//! `clear_attached_risk` sends an empty `new_attached_risk`, which the API
//! reads as "remove the take-profit, stop-loss and trailing stop", as the
//! TypeScript SDK's `clearRisk` does. Setting it with a new attached risk, or
//! sending an empty `AttachedRisk`, is refused before anything is sent.

use super::*;
use polyester::Price;
use polyester::models::{AttachedRisk, BatchReplaceItem, ModifyOrderParams, OrderKey, RiskLeg};
use polyester::proto::orders::v1::{BatchReplaceOrdersRequest, ModifyOrderRequest, RiskPolicy};

type Captured = Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>;

async fn risk_capture_client() -> (MockHttpServer, Client, Captured) {
    let seen = Captured::default();
    let captured = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        let rpc = request.path.rsplit('/').next().unwrap().to_owned();
        captured.lock().unwrap().push((rpc, request.body.to_vec()));
        // The capture is the assertion; a refusal stops response decoding.
        json_body(
            400,
            "application/json",
            r#"{"code":"failed_precondition","message":"capture complete"}"#,
        )
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("risk-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [{
            "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8
        }]}))
        .unwrap();
    (server, client, seen)
}

fn price(ticks: i64) -> Price {
    Price::from_ticks(ticks, Some("BTC-USDT".into())).unwrap()
}

fn take_profit() -> AttachedRisk {
    AttachedRisk {
        take_profit: Some(RiskLeg::market(price(70_000_000_000_000))),
        ..Default::default()
    }
}

fn modify(new_price: Option<Price>) -> ModifyOrderParams {
    ModifyOrderParams {
        symbol: "BTC-USDT".into(),
        key: OrderKey::OrderId("9".into()),
        subaccount_id: None,
        request_id: None,
        new_price,
        new_qty: None,
        new_attached_risk: None,
        clear_attached_risk: false,
        behavior: None,
        new_client_order_id: None,
    }
}

fn replace_item(new_price: Option<Price>) -> BatchReplaceItem {
    BatchReplaceItem {
        key: OrderKey::OrderId("9".into()),
        new_price,
        new_qty: None,
        new_attached_risk: None,
        clear_attached_risk: false,
        new_client_order_id: None,
    }
}

#[tokio::test]
async fn modify_and_batch_replace_clear_attached_risk_send_an_empty_risk_policy() {
    for new_price in [None, Some(price(64_672_970_000_000))] {
        let (_server, client, seen) = risk_capture_client().await;
        let mut params = modify(new_price.clone());
        params.clear_attached_risk = true;
        let _ = client.orders.modify(params).await;
        let mut item = replace_item(new_price.clone());
        item.clear_attached_risk = true;
        let _ = client
            .orders
            .batch_replace(vec![item], "BTC-USDT", None, None)
            .await;

        let got = std::mem::take(&mut *seen.lock().unwrap());
        assert_eq!(got.len(), 2, "{got:?}");
        let modify = ModifyOrderRequest::decode_from_slice(&got[0].1).unwrap();
        assert_eq!(
            modify.new_attached_risk.as_option(),
            Some(&RiskPolicy::default())
        );
        assert_eq!(modify.new_price_ticks.is_some(), new_price.is_some());
        let batch = BatchReplaceOrdersRequest::decode_from_slice(&got[1].1).unwrap();
        assert_eq!(
            batch.items[0].new_attached_risk.as_option(),
            Some(&RiskPolicy::default())
        );
    }
}

#[tokio::test]
async fn clear_with_new_risk_and_an_empty_risk_are_refused_before_sending() {
    let (_server, client, seen) = risk_capture_client().await;

    let mut both = modify(None);
    both.new_attached_risk = Some(take_profit());
    both.clear_attached_risk = true;
    let mut empty = modify(Some(price(64_672_970_000_000)));
    empty.new_attached_risk = Some(AttachedRisk::default());
    for params in [both, empty] {
        let err = client.orders.modify(params).await.unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
    }

    let mut both = replace_item(None);
    both.new_attached_risk = Some(take_profit());
    both.clear_attached_risk = true;
    let err = client
        .orders
        .batch_replace(vec![both], "BTC-USDT", None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validation(_)), "{err:?}");

    assert!(
        seen.lock().unwrap().is_empty(),
        "a refused call must send nothing"
    );
}
