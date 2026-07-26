//! Spot / zipper catalog cache for scale lookups.
//!
//! Zipper live supply is read through [`Manager::supply_for_zipped_asset_id`]
//! (keyed by `zipped_asset_id`). Full enriched zipper config rows are not
//! mutated; use [`Manager::patch_zipper_supply`] from
//! `subscribe_zipped_asset_supply(true)`.

use crate::codecs::scalars::{MAX_PROTOCOL_SCALE, validate_protocol_scale};
use crate::errors::{Error, Result};
use crate::models::{DepositWithdrawConfig, ZippedAssetSupplyUpdate};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

const DEFAULT_BASE_QTY_SCALE: u32 = 8;
const DEFAULT_ZIPPED_ASSET_SCALE: u32 = 18;

fn parse_u32_id(value: &Value, field: &str) -> Result<Option<u32>> {
    let Some(raw) = value.as_u64() else {
        if value.is_null() {
            return Ok(None);
        }
        return Err(Error::validation(format!(
            "catalog {field} must be a non-negative integer"
        )));
    };
    u32::try_from(raw)
        .map(Some)
        .map_err(|_| Error::validation(format!("catalog {field} {raw} exceeds u32 range")))
}

fn parse_scale(value: Option<&Value>, field: &str, default: u32) -> Result<u32> {
    let Some(value) = value else {
        return Ok(default);
    };
    let Some(raw) = value.as_u64() else {
        return Err(Error::validation(format!(
            "catalog {field} must be a non-negative integer"
        )));
    };
    let scale = u32::try_from(raw).map_err(|_| {
        Error::validation(format!(
            "catalog {field} {raw} exceeds u32 range (max protocol scale {MAX_PROTOCOL_SCALE})"
        ))
    })?;
    validate_protocol_scale(scale)?;
    Ok(scale)
}

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

#[derive(Default)]
struct SpotSnapshot {
    symbol_to_id: HashMap<String, u32>,
    id_to_base_scale: HashMap<u32, u32>,
    symbol_to_base_scale: HashMap<String, u32>,
    orderbook_buckets: HashMap<String, Vec<String>>,
    spot_config: Value,
}

#[derive(Default)]
struct ZipperSnapshot {
    asset_to_ledger_id: HashMap<String, u32>,
    asset_to_qty_scale: HashMap<String, u32>,
    zipped_id_to_scale: HashMap<u32, u32>,
    zipper_config: Value,
}

fn build_spot_snapshot(value: Value) -> Result<SpotSnapshot> {
    let mut snap = SpotSnapshot {
        spot_config: value.clone(),
        ..Default::default()
    };
    let markets = value
        .get("pairs")
        .or_else(|| value.get("markets"))
        .and_then(|m| m.as_array());
    let Some(markets) = markets else {
        return Ok(snap);
    };
    for m in markets {
        let symbol = m.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
        let symbol_id = parse_u32_id(
            m.get("symbol_id")
                .or_else(|| m.get("symbolId"))
                .unwrap_or(&Value::Null),
            "symbol_id",
        )?
        .unwrap_or(0);
        let scale = parse_scale(
            m.get("base_quantity_scale")
                .or_else(|| m.get("baseQuantityScale")),
            "base_quantity_scale",
            DEFAULT_BASE_QTY_SCALE,
        )?;
        if !symbol.is_empty() && symbol_id != 0 {
            snap.symbol_to_id.insert(symbol.to_owned(), symbol_id);
            snap.symbol_to_base_scale.insert(symbol.to_owned(), scale);
            snap.id_to_base_scale.insert(symbol_id, scale);
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
                snap.orderbook_buckets.insert(symbol.to_owned(), list);
            }
        }
    }
    if snap.symbol_to_base_scale.is_empty() {
        return Err(Error::validation(
            "catalog spot config contains no usable markets",
        ));
    }
    Ok(snap)
}

fn build_zipper_snapshot(value: Value) -> Result<ZipperSnapshot> {
    let mut snap = ZipperSnapshot {
        zipper_config: value.clone(),
        ..Default::default()
    };
    if let Some(assets) = value.get("assets").and_then(|a| a.as_array()) {
        for a in assets {
            let sym = a
                .get("asset")
                .or_else(|| a.get("symbol"))
                .and_then(|s| s.as_str())
                .unwrap_or("");
            let ledger_id = parse_u32_id(
                a.get("ledger_id")
                    .or_else(|| a.get("ledgerId"))
                    .unwrap_or(&Value::Null),
                "ledger_id",
            )?
            .unwrap_or(0);
            let scale = parse_scale(
                a.get("quantity_scale").or_else(|| a.get("quantityScale")),
                "quantity_scale",
                DEFAULT_ZIPPED_ASSET_SCALE,
            )?;
            if !sym.is_empty() {
                if ledger_id != 0 {
                    snap.asset_to_ledger_id.insert(sym.to_owned(), ledger_id);
                    snap.zipped_id_to_scale.insert(ledger_id, scale);
                }
                snap.asset_to_qty_scale.insert(sym.to_owned(), scale);
            }
        }
    }
    if snap.asset_to_qty_scale.is_empty() {
        return Err(Error::validation(
            "catalog zipper config contains no usable assets",
        ));
    }
    Ok(snap)
}

impl Manager {
    pub fn new() -> Self {
        Self::default()
    }

    /// True only after both spot and zipper snapshots were validated and
    /// installed atomically.
    pub fn is_ready(&self) -> bool {
        self.inner
            .read()
            .map(|inner| inner.spot_config.is_some() && inner.zipper_config.is_some())
            .unwrap_or(false)
    }

    /// Validate and install spot config atomically (no partial row mutation).
    pub fn hydrate_spot_config_json(&self, value: Value) -> Result<()> {
        let snap = build_spot_snapshot(value)?;
        let mut inner = self.inner.write().expect("catalog lock");
        apply_spot(&mut inner, snap);
        Ok(())
    }

    /// Typed Zipper hydrate — consumers do not need a direct `serde_json` dependency.
    pub fn hydrate_zipper_config(&self, config: &DepositWithdrawConfig) -> Result<()> {
        let value = serde_json::to_value(config)
            .map_err(|e| Error::validation(format!("catalog zipper encode failed: {e}")))?;
        self.hydrate_zipper_config_json(value)
    }

    /// Validate and install zipper config atomically (no partial row mutation).
    pub fn hydrate_zipper_config_json(&self, value: Value) -> Result<()> {
        let snap = build_zipper_snapshot(value)?;
        let mut inner = self.inner.write().expect("catalog lock");
        apply_zipper(&mut inner, snap);
        Ok(())
    }

    /// Validate spot + zipper, then commit both under one write lock.
    ///
    /// On any validation error neither catalog is mutated.
    pub fn hydrate_spot_and_zipper_json(&self, spot: Value, zipper: Value) -> Result<()> {
        let spot_snap = build_spot_snapshot(spot)?;
        let zipper_snap = build_zipper_snapshot(zipper)?;
        let mut inner = self.inner.write().expect("catalog lock");
        apply_spot(&mut inner, spot_snap);
        apply_zipper(&mut inner, zipper_snap);
        Ok(())
    }

    pub fn symbol_id_for_symbol(&self, symbol: &str) -> Option<u32> {
        self.inner.read().ok()?.symbol_to_id.get(symbol).copied()
    }

    /// Returns the pair base quantity scale, or `None` when unknown/unhydrated.
    ///
    /// Never invents scale 8 for missing symbols — callers that need a decode
    /// fallback must choose it explicitly.
    pub fn base_quantity_scale_for_symbol(&self, symbol: &str) -> Option<u32> {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.symbol_to_base_scale.get(symbol).copied())
    }

    pub fn base_quantity_scale_for_symbol_id(&self, id: u32) -> Option<u32> {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.id_to_base_scale.get(&id).copied())
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

fn apply_spot(inner: &mut Inner, snap: SpotSnapshot) {
    // Replace spot maps wholesale so a refresh cannot leave stale symbols.
    inner.symbol_to_id = snap.symbol_to_id;
    inner.id_to_base_scale = snap.id_to_base_scale;
    inner.symbol_to_base_scale = snap.symbol_to_base_scale;
    inner.orderbook_buckets = snap.orderbook_buckets;
    inner.spot_config = Some(snap.spot_config);
}

fn apply_zipper(inner: &mut Inner, snap: ZipperSnapshot) {
    inner.asset_to_ledger_id = snap.asset_to_ledger_id;
    inner.asset_to_qty_scale = snap.asset_to_qty_scale;
    inner.zipped_id_to_scale = snap.zipped_id_to_scale;
    inner.zipper_config = Some(snap.zipper_config);
    // Preserve live supply patches across zipper catalog replacement.
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
        }))
        .expect("hydrate");
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), Some(8));
        assert_eq!(mgr.base_quantity_scale_for_symbol_id(1), Some(8));
        assert_eq!(
            mgr.orderbook_price_buckets_for_symbol("BTC-USDT"),
            vec!["0.01".to_owned(), "0.1".to_owned(), "1.0".to_owned()]
        );
    }

    #[test]
    fn readiness_requires_usable_spot_and_zipper_snapshots() {
        let mgr = Manager::new();
        assert!(!mgr.is_ready());
        assert!(mgr.hydrate_spot_config_json(json!({"pairs": []})).is_err());
        assert!(
            mgr.hydrate_zipper_config_json(json!({"assets": []}))
                .is_err()
        );
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .expect("spot");
        assert!(!mgr.is_ready());
        mgr.hydrate_zipper_config_json(json!({
            "assets": [{
                "asset": "USDT",
                "ledger_id": 99,
                "quantity_scale": 6
            }]
        }))
        .expect("zipper");
        assert!(mgr.is_ready());
    }

    #[test]
    fn hydrate_rejects_oversized_scale_without_truncating() {
        let mgr = Manager::new();
        let err = mgr
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": "BTC-USDT",
                    "symbol_id": 1,
                    "base_quantity_scale": 65535
                }]
            }))
            .expect_err("scale 65535 must fail");
        assert!(err.to_string().contains("scale"));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), None);
    }

    #[test]
    fn hydrate_spot_invalid_later_row_does_not_mutate_existing_catalog() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .expect("seed");
        let err = mgr
            .hydrate_spot_config_json(json!({
                "pairs": [
                    {
                        "symbol": "ETH-USDT",
                        "symbol_id": 2,
                        "base_quantity_scale": 6
                    },
                    {
                        "symbol": "BAD-USDT",
                        "symbol_id": 3,
                        "base_quantity_scale": 65535
                    }
                ]
            }))
            .expect_err("later invalid row must fail");
        assert!(err.to_string().contains("scale"));
        // Prior catalog untouched; partial new rows must not install.
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), Some(8));
        assert_eq!(mgr.base_quantity_scale_for_symbol("ETH-USDT"), None);
        assert_eq!(mgr.symbol_id_for_symbol("ETH-USDT"), None);
    }

    #[test]
    fn hydrate_zipper_invalid_later_row_does_not_mutate_existing_catalog() {
        let mgr = Manager::new();
        mgr.hydrate_zipper_config_json(json!({
            "assets": [{
                "asset": "USDT",
                "ledger_id": 99,
                "quantity_scale": 6
            }]
        }))
        .expect("seed");
        let err = mgr
            .hydrate_zipper_config_json(json!({
                "assets": [
                    {
                        "asset": "BTC",
                        "ledger_id": 1,
                        "quantity_scale": 8
                    },
                    {
                        "asset": "BAD",
                        "ledger_id": 2,
                        "quantity_scale": 65535
                    }
                ]
            }))
            .expect_err("later invalid row must fail");
        assert!(err.to_string().contains("scale"));
        assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
        assert_eq!(mgr.ledger_id_for_asset("BTC"), None);
    }

    #[test]
    fn catalog_refresh_replaces_stale_entries() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "OLD-USDT",
                "symbol_id": 9,
                "base_quantity_scale": 8
            }]
        }))
        .expect("seed");
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .expect("refresh");
        assert_eq!(mgr.symbol_id_for_symbol("OLD-USDT"), None);
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
    }

    #[test]
    fn hydrate_spot_and_zipper_commits_neither_on_zipper_failure() {
        let mgr = Manager::new();
        let err = mgr
            .hydrate_spot_and_zipper_json(
                json!({
                    "pairs": [{
                        "symbol": "BTC-USDT",
                        "symbol_id": 1,
                        "base_quantity_scale": 8
                    }]
                }),
                json!({
                    "assets": [{
                        "asset": "USDT",
                        "ledger_id": 99,
                        "quantity_scale": 65535
                    }]
                }),
            )
            .expect_err("zipper invalid must fail");
        assert!(err.to_string().contains("scale"));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), None);
        assert_eq!(mgr.ledger_id_for_asset("USDT"), None);
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
        }))
        .expect("hydrate");
        assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(99), 6);
    }

    #[test]
    fn unknown_symbol_returns_none_not_default_scale() {
        let mgr = Manager::new();
        assert_eq!(mgr.base_quantity_scale_for_symbol("NOPE"), None);
        assert_eq!(mgr.base_quantity_scale_for_symbol("ETH-USDT"), None);
    }

    #[test]
    fn hydrated_eth_usdt_uses_scale_6() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "ETH-USDT",
                "symbol_id": 2,
                "base_quantity_scale": 6
            }]
        }))
        .expect("hydrate");
        assert_eq!(mgr.base_quantity_scale_for_symbol("ETH-USDT"), Some(6));
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
