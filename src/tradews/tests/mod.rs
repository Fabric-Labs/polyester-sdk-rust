mod codec;
mod pin;
mod reconnect_limit;
mod session;

use std::sync::Arc;

use crate::marketstream::Pairs;

/// The catalog the tests trade with: pair 7, BTC-USDT, whose base quantity
/// scale is 6.
pub(super) fn pairs() -> Arc<dyn Pairs> {
    let catalogs = crate::catalogs::Manager::new();
    catalogs
        .hydrate_spot_config_json(serde_json::json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 7,
                "base_quantity_scale": 6,
                "quote_quantity_scale": 6
            }]
        }))
        .expect("hydrates");
    Arc::new(catalogs)
}
