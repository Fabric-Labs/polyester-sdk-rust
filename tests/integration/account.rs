use crate::support::{call_optional, require_live_client};

#[tokio::test]
async fn deposit_list_addresses_optional() {
    let Some(client) = require_live_client() else {
        return;
    };
    let _ = call_optional("deposit.list_addresses", || client.deposit.list_addresses()).await;
}

#[tokio::test]
async fn hydrate_catalogs_best_effort() {
    let Some(client) = require_live_client() else {
        return;
    };
    client
        .hydrate_catalogs()
        .await
        .expect("hydrate_catalogs should not fail hard");
    let _ = client.catalogs.symbol_id_for_symbol("BTC-USDT");
    let scale = client.catalogs.base_quantity_scale_for_symbol("BTC-USDT").expect("catalog scale");
    assert!(scale > 0);
}
