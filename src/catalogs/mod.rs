//! Spot / zipper catalog cache for scale lookups.
//!
//! Zipper live supply is stored in [`Manager::zipped_asset_supply`] (keyed by
//! `zipped_asset_id`). Full enriched zipper config rows are not mutated; use
//! [`Manager::patch_zipper_supply`] from `subscribe_zipped_asset_supply(true)`.

use crate::models::ZippedAssetSupplyUpdate;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

const DEFAULT_BASE_QTY_SCALE: u32 = 8;
const DEFAULT_ZIPPED_ASSET_SCALE: u32 = 18;

#[derive(Debug, Default)]
pub struct Manager {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    symbol_to_id: HashMap<String, u32>,
    id_to_base_scale: HashMap<u32, u32>,
    symbol_to_base_scale: HashMap<String, u32>,
    asset_to_ledger_id: HashMap<String, u32>,
    asset_to_qty_scale: HashMap<String, u32>,
    zipped_id_to_scale: HashMap<u32, u32>,
    /// Live supply strings by `zipped_asset_id` (updated via [`Manager::patch_zipper_supply`]).
    zipped_id_to_supply: HashMap<u32, String>,
    orderbook_buckets: HashMap<String, Vec<String>>,
    spot_config: Option<Value>,
    zipper_config: Option<Value>,
}

impl Manager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hydrate_spot_config_json(&self, value: Value) {
        let mut inner = self.inner.write().expect("catalog lock");
        inner.spot_config = Some(value.clone());
        // Wire/proto uses `pairs`; some helpers historically used `markets`.
        let markets = value
            .get("pairs")
            .or_else(|| value.get("markets"))
            .and_then(|m| m.as_array());
        let Some(markets) = markets else {
            return;
        };
        for m in markets {
            let symbol = m.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            let symbol_id = m
                .get("symbol_id")
                .or_else(|| m.get("symbolId"))
                .and_then(|s| s.as_u64())
                .unwrap_or(0) as u32;
            let scale = m
                .get("base_quantity_scale")
                .or_else(|| m.get("baseQuantityScale"))
                .and_then(|s| s.as_u64())
                .unwrap_or(DEFAULT_BASE_QTY_SCALE as u64) as u32;
            if !symbol.is_empty() && symbol_id != 0 {
                inner.symbol_to_id.insert(symbol.to_owned(), symbol_id);
                inner.symbol_to_base_scale.insert(symbol.to_owned(), scale);
                inner.id_to_base_scale.insert(symbol_id, scale);
            }
            let buckets = m
                .get("orderbook_price_buckets")
                .or_else(|| m.get("orderbookPriceBuckets"))
                .or_else(|| {
                    m.get("marketdata")
                        .and_then(|md| md.get("orderbook_price_buckets"))
                })
                .and_then(|b| b.as_array());
            if let Some(buckets) = buckets {
                let list: Vec<String> = buckets
                    .iter()
                    .filter_map(|b| match b {
                        Value::String(s) => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    })
                    .collect();
                if !list.is_empty() {
                    inner.orderbook_buckets.insert(symbol.to_owned(), list);
                }
            }
        }
    }

    pub fn hydrate_zipper_config_json(&self, value: Value) {
        let mut inner = self.inner.write().expect("catalog lock");
        inner.zipper_config = Some(value.clone());
        // Best-effort extract asset scales from common shapes.
        if let Some(assets) = value.get("assets").and_then(|a| a.as_array()) {
            for a in assets {
                let sym = a
                    .get("asset")
                    .or_else(|| a.get("symbol"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let ledger_id = a
                    .get("ledger_id")
                    .or_else(|| a.get("ledgerId"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                let scale =
                    a.get("quantity_scale")
                        .or_else(|| a.get("quantityScale"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(DEFAULT_ZIPPED_ASSET_SCALE as u64) as u32;
                if !sym.is_empty() {
                    if ledger_id != 0 {
                        inner.asset_to_ledger_id.insert(sym.to_owned(), ledger_id);
                        inner.zipped_id_to_scale.insert(ledger_id, scale);
                    }
                    inner.asset_to_qty_scale.insert(sym.to_owned(), scale);
                }
            }
        }
    }

    pub fn symbol_id_for_symbol(&self, symbol: &str) -> Option<u32> {
        self.inner.read().ok()?.symbol_to_id.get(symbol).copied()
    }

    pub fn base_quantity_scale_for_symbol(&self, symbol: &str) -> u32 {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.symbol_to_base_scale.get(symbol).copied())
            .unwrap_or(DEFAULT_BASE_QTY_SCALE)
    }

    pub fn base_quantity_scale_for_symbol_id(&self, id: u32) -> u32 {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.id_to_base_scale.get(&id).copied())
            .unwrap_or(DEFAULT_BASE_QTY_SCALE)
    }

    pub fn quantity_scale_for_zipped_asset_id(&self, id: u32) -> u32 {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.zipped_id_to_scale.get(&id).copied())
            .unwrap_or(DEFAULT_ZIPPED_ASSET_SCALE)
    }

    pub fn orderbook_price_buckets_for_symbol(&self, symbol: &str) -> Vec<String> {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.orderbook_buckets.get(symbol).cloned())
            .unwrap_or_default()
    }

    pub fn ledger_id_for_asset(&self, symbol: &str) -> Option<u32> {
        self.inner
            .read()
            .ok()?
            .asset_to_ledger_id
            .get(symbol)
            .copied()
    }

    /// Latest supply string for a zipped asset id, if patched from realtime updates.
    pub fn supply_for_zipped_asset_id(&self, id: u32) -> Option<String> {
        self.inner
            .read()
            .ok()?
            .zipped_id_to_supply
            .get(&id)
            .cloned()
    }

    /// Apply supply updates to the in-memory `zipped_asset_id -> supply` map.
    ///
    /// Returns `true` when at least one entry changed. Unlike Go/Python, Rust
    /// catalogs do not store full enriched zipper chain rows with a `supply`
    /// field; this map is the live-supply source of truth.
    pub fn patch_zipper_supply(&self, updates: &[ZippedAssetSupplyUpdate]) -> bool {
        if updates.is_empty() {
            return false;
        }
        let mut inner = self.inner.write().expect("catalog lock");
        let mut changed = false;
        for update in updates {
            let prev = inner.zipped_id_to_supply.get(&update.zipped_asset_id);
            if prev.map(String::as_str) != Some(update.supply.as_str()) {
                inner
                    .zipped_id_to_supply
                    .insert(update.zipped_asset_id, update.supply.clone());
                changed = true;
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hydrate_spot_pairs_sets_symbol_scale_and_buckets() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8,
                "orderbook_price_buckets": [0.01, 0.1, 1.0]
            }]
        }));
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), 8);
        assert_eq!(mgr.base_quantity_scale_for_symbol_id(1), 8);
        assert_eq!(
            mgr.orderbook_price_buckets_for_symbol("BTC-USDT"),
            vec!["0.01".to_owned(), "0.1".to_owned(), "1.0".to_owned()]
        );
    }

    #[test]
    fn hydrate_zipper_assets_sets_ledger_id() {
        let mgr = Manager::new();
        mgr.hydrate_zipper_config_json(json!({
            "assets": [{
                "asset": "USDT",
                "ledger_id": 99,
                "quantity_scale": 6
            }]
        }));
        assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(99), 6);
    }

    #[test]
    fn unknown_symbol_falls_back_to_default_scale() {
        let mgr = Manager::new();
        assert_eq!(
            mgr.base_quantity_scale_for_symbol("NOPE"),
            DEFAULT_BASE_QTY_SCALE
        );
    }

    #[test]
    fn patch_zipper_supply_updates_map() {
        let mgr = Manager::new();
        assert!(mgr.patch_zipper_supply(&[ZippedAssetSupplyUpdate {
            zipped_asset_id: 42,
            supply: "100.5".to_owned(),
        }]));
        assert_eq!(mgr.supply_for_zipped_asset_id(42).as_deref(), Some("100.5"));
        assert!(!mgr.patch_zipper_supply(&[ZippedAssetSupplyUpdate {
            zipped_asset_id: 42,
            supply: "100.5".to_owned(),
        }]));
        assert!(mgr.patch_zipper_supply(&[ZippedAssetSupplyUpdate {
            zipped_asset_id: 42,
            supply: "200".to_owned(),
        }]));
        assert_eq!(mgr.supply_for_zipped_asset_id(42).as_deref(), Some("200"));
    }
}
