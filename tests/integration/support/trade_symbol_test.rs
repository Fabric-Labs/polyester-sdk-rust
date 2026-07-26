//! F-23 / B5: trade symbol picker never silently selects ETH via smoke fallback.

use super::trade::trade_symbol_with_override;
use polyester::models::SpotConfig;
use serde_json::json;

fn spot_with_eth_first() -> SpotConfig {
    SpotConfig {
        raw: json!({
            "pairs": [
                {"symbol": "ETH-USDT", "symbol_id": 1, "base_quantity_scale": 8},
                {"symbol": "BTC-USDT", "symbol_id": 2, "base_quantity_scale": 8}
            ]
        }),
    }
}

#[test]
fn trade_symbol_honors_polyester_test_trade_symbol_over_smoke() {
    let symbol = trade_symbol_with_override(&spot_with_eth_first(), Some("BTC-USDT"));
    assert_eq!(symbol, "BTC-USDT");
}

#[test]
fn trade_symbol_defaults_to_btc_not_eth_when_unset() {
    let symbol = trade_symbol_with_override(&spot_with_eth_first(), None);
    assert_eq!(symbol, "BTC-USDT");
}
