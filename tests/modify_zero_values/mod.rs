//! A zero new price or quantity on modify and batch replace is refused before
//! sending, in every form: the API reads 0 as no change.

use super::*;
use polyester::Price;
use polyester::models::{BatchReplaceItem, ModifyOrderParams, OrderKey};
use rust_decimal::Decimal;

type Captured = Arc<std::sync::Mutex<Vec<String>>>;

async fn capture_client() -> (MockHttpServer, Client, Captured) {
    let seen = Captured::default();
    let captured = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        captured.lock().unwrap().push(request.path.clone());
        json_body(
            400,
            "application/json",
            r#"{"code":"failed_precondition","message":"capture complete"}"#,
        )
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("zero-test".into()),
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

fn zero_prices() -> Vec<Price> {
    vec![
        Price::from_ticks(0, None).unwrap(),
        Price::from_decimal_str("0", None).unwrap(),
        Price::from_decimal_str("0.000", None).unwrap(),
        Price::from_decimal(Decimal::ZERO, None).unwrap(),
    ]
}

/// A decimal zero quantity is refused when it is built; a scaled one reaches
/// the call.
fn zero_quantities() -> Vec<Quantity> {
    assert!(Quantity::from_decimal_str("0", 8, None, None).is_err());
    assert!(Quantity::from_decimal(Decimal::ZERO, 8, None, None).is_err());
    vec![Quantity::from_scaled(0, Some(8), QuantityDomain::OrderBase, None, None).unwrap()]
}

fn modify(new_price: Option<Price>, new_qty: Option<Quantity>) -> ModifyOrderParams {
    ModifyOrderParams {
        symbol: "BTC-USDT".into(),
        key: OrderKey::OrderId("9".into()),
        subaccount_id: SubAccountScope::Default,
        request_id: None,
        new_price,
        new_qty,
        new_attached_risk: None,
        clear_attached_risk: false,
        behavior: None,
        new_client_order_id: None,
    }
}

fn replace_item(new_price: Option<Price>, new_qty: Option<Quantity>) -> BatchReplaceItem {
    BatchReplaceItem {
        key: OrderKey::OrderId("9".into()),
        new_price,
        new_qty,
        new_attached_risk: None,
        clear_attached_risk: false,
        new_client_order_id: None,
    }
}

fn assert_refused(err: Error, field: &str) {
    match err {
        Error::Validation(message) => assert!(
            message.contains(&format!("{field} must be positive")),
            "{message}"
        ),
        other => panic!("{field}: {other:?}"),
    }
}

#[tokio::test]
async fn modify_and_batch_replace_refuse_zero_price_and_quantity_before_sending() {
    let (_server, client, seen) = capture_client().await;
    let one = Price::from_decimal_str("1", None).unwrap();
    for price in zero_prices() {
        let err = client
            .orders
            .modify(modify(Some(price.clone()), None))
            .await
            .unwrap_err();
        assert_refused(err, "new_price");
        let err = client
            .orders
            .batch_replace(
                vec![replace_item(Some(price), None)],
                "BTC-USDT",
                SubAccountScope::Default,
                None,
            )
            .await
            .unwrap_err();
        assert_refused(err, "new_price");
    }
    for qty in zero_quantities() {
        let err = client
            .orders
            .modify(modify(Some(one.clone()), Some(qty.clone())))
            .await
            .unwrap_err();
        assert_refused(err, "new_qty");
        let err = client
            .orders
            .batch_replace(
                vec![replace_item(None, Some(qty))],
                "BTC-USDT",
                SubAccountScope::Default,
                None,
            )
            .await
            .unwrap_err();
        assert_refused(err, "new_qty");
    }
    assert!(
        seen.lock().unwrap().is_empty(),
        "a refused call must send nothing: {:?}",
        seen.lock().unwrap()
    );

    // The smallest positive values are still sent.
    let tick = Price::from_ticks(1, None).unwrap();
    let step = Quantity::from_scaled(1, Some(8), QuantityDomain::OrderBase, None, None).unwrap();
    let _ = client
        .orders
        .modify(modify(Some(tick.clone()), Some(step.clone())))
        .await;
    let _ = client
        .orders
        .batch_replace(
            vec![replace_item(Some(tick), Some(step))],
            "BTC-USDT",
            SubAccountScope::Default,
            None,
        )
        .await;
    assert_eq!(seen.lock().unwrap().len(), 2);
}
