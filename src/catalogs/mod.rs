//! Spot / zipper catalog cache for scale lookups.

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
        if let Some(markets) = value.get("markets").and_then(|m| m.as_array()) {
            for m in markets {
                let symbol = m.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
                let symbol_id = m.get("symbol_id").and_then(|s| s.as_u64()).unwrap_or(0) as u32;
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
                if let Some(buckets) = m
                    .get("orderbook_price_buckets")
                    .or_else(|| m.get("orderbookPriceBuckets"))
                    .and_then(|b| b.as_array())
                {
                    let list: Vec<String> = buckets
                        .iter()
                        .filter_map(|b| b.as_str().map(str::to_owned))
                        .collect();
                    if !list.is_empty() {
                        inner.orderbook_buckets.insert(symbol.to_owned(), list);
                    }
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
                let sym = a.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
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
}
