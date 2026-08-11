//! Spot / zipper catalog cache for scale lookups.
//!
//! Zipper live supply is read through [`Manager::supply_for_zipped_asset_id`]
//! (keyed by `zipped_asset_id`). Full enriched zipper config rows are not
//! mutated; use [`Manager::patch_zipper_supply`] from
//! `subscribe_zipped_asset_supply(true)`.

use crate::codecs::scalars::{
    MAX_PROTOCOL_SCALE, PRICE_TICK_SCALE, decimal_to_scaled_str, parse_price_ticks_str,
    parse_qty_scaled_str, validate_protocol_scale,
};
use crate::errors::{Error, Result};
use crate::models::{DepositWithdrawConfig, ZippedAssetSupplyUpdate};
use crate::realtime::{read_unpoisoned, write_unpoisoned};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

fn parse_u32_id(value: Option<&Value>, field: &str) -> Result<u32> {
    let Some(value) = value else {
        return Err(Error::validation(format!("catalog {field} is required")));
    };
    let Some(raw) = value.as_u64() else {
        return Err(Error::validation(format!(
            "catalog {field} must be a positive integer"
        )));
    };
    let id = u32::try_from(raw)
        .map_err(|_| Error::validation(format!("catalog {field} {raw} exceeds u32 range")))?;
    if id == 0 {
        return Err(Error::validation(format!(
            "catalog {field} must be non-zero"
        )));
    }
    Ok(id)
}

fn parse_scale(value: Option<&Value>, field: &str) -> Result<u32> {
    let Some(value) = value else {
        return Err(Error::validation(format!("catalog {field} is required")));
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

fn parse_optional_scale(value: Option<&Value>, field: &str) -> Result<Option<u32>> {
    value
        .map(|value| parse_scale(Some(value), field))
        .transpose()
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
    id_to_quote_scale: HashMap<u32, u32>,
    symbol_to_quote_scale: HashMap<String, u32>,
    symbol_to_constraints: HashMap<String, PairConstraints>,
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
    id_to_quote_scale: HashMap<u32, u32>,
    symbol_to_quote_scale: HashMap<String, u32>,
    symbol_to_constraints: HashMap<String, PairConstraints>,
    orderbook_buckets: HashMap<String, Vec<String>>,
    spot_config: Value,
}

/// Deterministic trading constraints parsed from one validated spot-catalog row.
///
/// These checks complement server-side preview/admission; they do not replace
/// balance, risk, permission, or live-market validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairConstraints {
    pub symbol_id: u32,
    pub base_quantity_scale: u32,
    pub quote_quantity_scale: Option<u32>,
    pub tick_size_ticks: Option<i64>,
    pub step_size_scaled: Option<i64>,
    pub min_qty_scaled: Option<i64>,
    pub min_notional_quote_scaled: Option<i128>,
}

fn nonempty_string<'a>(market: &'a Value, snake: &str, camel: &str) -> Option<&'a str> {
    market
        .get(snake)
        .or_else(|| market.get(camel))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_pair_constraints(
    market: &Value,
    symbol: &str,
    symbol_id: u32,
    base_scale: u32,
    quote_scale: Option<u32>,
) -> Result<PairConstraints> {
    let tick_size_ticks = nonempty_string(market, "tick_size", "tickSize")
        .map(|raw| {
            let value = parse_price_ticks_str(raw, "catalog tick_size")?;
            if value == 0 {
                return Err(Error::validation("catalog tick_size must be positive"));
            }
            Ok(value)
        })
        .transpose()?;
    let step_size_scaled = nonempty_string(market, "step_size", "stepSize")
        .map(|raw| parse_qty_scaled_str(raw, base_scale, "catalog step_size"))
        .transpose()?;
    let min_qty_scaled = nonempty_string(market, "min_qty_base", "minQtyBase")
        .map(|raw| {
            if decimal_to_scaled_str(raw, base_scale, "catalog min_qty_base")? == 0 {
                return Ok(None);
            }
            parse_qty_scaled_str(raw, base_scale, "catalog min_qty_base").map(Some)
        })
        .transpose()?
        .flatten();
    let min_notional_quote_scaled =
        nonempty_string(market, "min_notional_quote", "minNotionalQuote")
            .map(|raw| {
                let scale = quote_scale.ok_or_else(|| {
                    Error::validation(format!(
                        "catalog {symbol} has min_notional_quote but no quote_quantity_scale"
                    ))
                })?;
                let value = decimal_to_scaled_str(raw, scale, "catalog min_notional_quote")?;
                if value == 0 {
                    return Ok(None);
                }
                if value < 0 {
                    return Err(Error::validation(
                        "catalog min_notional_quote must be positive",
                    ));
                }
                Ok(Some(value))
            })
            .transpose()?
            .flatten();
    if let (Some(min_qty), Some(step)) = (min_qty_scaled, step_size_scaled)
        && min_qty % step != 0
    {
        return Err(Error::validation(format!(
            "catalog {symbol} min_qty_base must be aligned to step_size"
        )));
    }
    Ok(PairConstraints {
        symbol_id,
        base_quantity_scale: base_scale,
        quote_quantity_scale: quote_scale,
        tick_size_ticks,
        step_size_scaled,
        min_qty_scaled,
        min_notional_quote_scaled,
    })
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
        return Err(Error::validation(
            "catalog spot config must contain a pairs or markets array",
        ));
    };
    let mut id_to_symbol = HashMap::<u32, String>::new();
    for m in markets {
        let symbol = m
            .get("symbol")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|symbol| !symbol.is_empty())
            .ok_or_else(|| Error::validation("catalog symbol must be non-empty"))?;
        let symbol_id = parse_u32_id(
            m.get("symbol_id").or_else(|| m.get("symbolId")),
            "symbol_id",
        )?;
        let scale = parse_scale(
            m.get("base_quantity_scale")
                .or_else(|| m.get("baseQuantityScale")),
            "base_quantity_scale",
        )?;
        let quote_scale = parse_optional_scale(
            m.get("quote_quantity_scale")
                .or_else(|| m.get("quoteQuantityScale")),
            "quote_quantity_scale",
        )?;
        if snap.symbol_to_id.contains_key(symbol) {
            return Err(Error::validation(format!(
                "catalog contains duplicate symbol {symbol}"
            )));
        }
        if let Some(existing) = id_to_symbol.get(&symbol_id) {
            return Err(Error::validation(format!(
                "catalog symbol_id {symbol_id} is shared by {existing} and {symbol}"
            )));
        }
        snap.symbol_to_id.insert(symbol.to_owned(), symbol_id);
        snap.symbol_to_base_scale.insert(symbol.to_owned(), scale);
        snap.id_to_base_scale.insert(symbol_id, scale);
        if let Some(quote_scale) = quote_scale {
            snap.symbol_to_quote_scale
                .insert(symbol.to_owned(), quote_scale);
            snap.id_to_quote_scale.insert(symbol_id, quote_scale);
        }
        let constraints = parse_pair_constraints(m, symbol, symbol_id, scale, quote_scale)?;
        snap.symbol_to_constraints
            .insert(symbol.to_owned(), constraints);
        id_to_symbol.insert(symbol_id, symbol.to_owned());
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
    let assets = value
        .get("assets")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::validation("catalog zipper config must contain an assets array"))?;
    let mut ledger_id_to_asset = HashMap::<u32, String>::new();
    let mut zipped_id_to_asset = HashMap::<u32, String>::new();
    for a in assets {
        let sym = a
            .get("asset")
            .or_else(|| a.get("symbol"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|asset| !asset.is_empty())
            .ok_or_else(|| Error::validation("catalog asset must be non-empty"))?;
        let ledger_id = parse_u32_id(
            a.get("ledger_id").or_else(|| a.get("ledgerId")),
            "ledger_id",
        )?;
        let scale = parse_scale(
            a.get("quantity_scale").or_else(|| a.get("quantityScale")),
            "quantity_scale",
        )?;
        if snap.asset_to_ledger_id.contains_key(sym) {
            return Err(Error::validation(format!(
                "catalog contains duplicate asset {sym}"
            )));
        }
        if let Some(existing) = ledger_id_to_asset.get(&ledger_id) {
            return Err(Error::validation(format!(
                "catalog ledger_id {ledger_id} is shared by {existing} and {sym}"
            )));
        }
        snap.asset_to_ledger_id.insert(sym.to_owned(), ledger_id);
        snap.asset_to_qty_scale.insert(sym.to_owned(), scale);
        ledger_id_to_asset.insert(ledger_id, sym.to_owned());

        if let Some(variants) = a.get("variants").and_then(Value::as_array) {
            for variant in variants {
                let zipped_id = parse_u32_id(
                    variant
                        .get("zipped_asset_id")
                        .or_else(|| variant.get("zippedAssetId")),
                    "zipped_asset_id",
                )?;
                if let Some(existing) = zipped_id_to_asset.get(&zipped_id) {
                    return Err(Error::validation(format!(
                        "catalog zipped_asset_id {zipped_id} is shared by {existing} and {sym}"
                    )));
                }
                if snap.zipped_id_to_scale.insert(zipped_id, scale).is_some() {
                    return Err(Error::validation(format!(
                        "catalog contains duplicate zipped_asset_id {zipped_id}"
                    )));
                }
                zipped_id_to_asset.insert(zipped_id, sym.to_owned());
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
        let inner = read_unpoisoned(&self.inner);
        inner.spot_config.is_some() && inner.zipper_config.is_some()
    }

    /// Validate and install spot config atomically (no partial row mutation).
    pub fn hydrate_spot_config_json(&self, value: Value) -> Result<()> {
        let snap = build_spot_snapshot(value)?;
        let mut inner = write_unpoisoned(&self.inner);
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
        let mut inner = write_unpoisoned(&self.inner);
        apply_zipper(&mut inner, snap);
        Ok(())
    }

    /// Validate spot + zipper, then commit both under one write lock.
    ///
    /// On any validation error neither catalog is mutated.
    pub fn hydrate_spot_and_zipper_json(&self, spot: Value, zipper: Value) -> Result<()> {
        let spot_snap = build_spot_snapshot(spot)?;
        let zipper_snap = build_zipper_snapshot(zipper)?;
        let mut inner = write_unpoisoned(&self.inner);
        apply_spot(&mut inner, spot_snap);
        apply_zipper(&mut inner, zipper_snap);
        Ok(())
    }

    pub fn symbol_id_for_symbol(&self, symbol: &str) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .symbol_to_id
            .get(symbol)
            .copied()
    }

    /// Resolve an optional raw symbol filter against the hydrated catalog.
    ///
    /// Empty/whitespace filters remain omitted. Any unknown non-empty symbol
    /// fails closed so it cannot accidentally become an unfiltered request.
    pub fn resolve_symbol_filter(&self, symbol: Option<&str>) -> Result<Option<String>> {
        let Some(symbol) = symbol.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        if self.symbol_id_for_symbol(symbol).is_none() {
            return Err(Error::validation(format!(
                "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
            )));
        }
        Ok(Some(symbol.to_owned()))
    }

    /// Validate and normalize a list of raw symbol filters.
    pub fn resolve_symbol_filters(&self, symbols: Option<&[String]>) -> Result<Vec<String>> {
        let mut resolved = Vec::new();
        for symbol in symbols.unwrap_or_default() {
            if let Some(symbol) = self.resolve_symbol_filter(Some(symbol))? {
                resolved.push(symbol);
            }
        }
        Ok(resolved)
    }

    /// Return validated deterministic constraints for `symbol`.
    pub fn pair_constraints_for_symbol(&self, symbol: &str) -> Option<PairConstraints> {
        read_unpoisoned(&self.inner)
            .symbol_to_constraints
            .get(symbol)
            .cloned()
    }

    /// Preflight deterministic price/quantity/minimum constraints.
    ///
    /// Minimum notional is checked only when both base quantity and price are
    /// available. Stateful venue admission remains authoritative.
    pub fn preflight_order_values(
        &self,
        symbol: &str,
        qty_scaled: Option<i64>,
        price_ticks: Option<i64>,
    ) -> Result<()> {
        let constraints = self.pair_constraints_for_symbol(symbol).ok_or_else(|| {
            Error::validation(format!(
                "constraints for {symbol:?} are unavailable; await client.wait_for_catalogs()"
            ))
        })?;
        if let Some(price) = price_ticks {
            if price <= 0 {
                return Err(Error::validation("price must be positive"));
            }
            if let Some(tick) = constraints.tick_size_ticks
                && price % tick != 0
            {
                return Err(Error::validation(format!(
                    "price for {symbol} must be aligned to catalog tick_size"
                )));
            }
        }
        if let Some(qty) = qty_scaled {
            if qty <= 0 {
                return Err(Error::validation("quantity must be positive"));
            }
            if let Some(step) = constraints.step_size_scaled
                && qty % step != 0
            {
                return Err(Error::validation(format!(
                    "quantity for {symbol} must be aligned to catalog step_size"
                )));
            }
            if let Some(min_qty) = constraints.min_qty_scaled
                && qty < min_qty
            {
                return Err(Error::validation(format!(
                    "quantity for {symbol} is below catalog min_qty_base"
                )));
            }
        }
        if let (Some(qty), Some(price), Some(min_notional), Some(quote_scale)) = (
            qty_scaled,
            price_ticks,
            constraints.min_notional_quote_scaled,
            constraints.quote_quantity_scale,
        ) {
            let quote_factor = 10_i128
                .checked_pow(quote_scale)
                .ok_or_else(|| Error::validation("quote scale factor overflow"))?;
            let base_factor = 10_i128
                .checked_pow(constraints.base_quantity_scale)
                .ok_or_else(|| Error::validation("base scale factor overflow"))?;
            let price_factor = 10_i128
                .checked_pow(PRICE_TICK_SCALE)
                .ok_or_else(|| Error::validation("price scale factor overflow"))?;
            let actual = i128::from(price)
                .checked_mul(i128::from(qty))
                .and_then(|value| value.checked_mul(quote_factor))
                .ok_or_else(|| Error::validation("order notional preflight overflow"))?;
            let minimum = min_notional
                .checked_mul(price_factor)
                .and_then(|value| value.checked_mul(base_factor))
                .ok_or_else(|| Error::validation("minimum notional preflight overflow"))?;
            if actual < minimum {
                return Err(Error::validation(format!(
                    "order for {symbol} is below catalog min_notional_quote"
                )));
            }
        }
        Ok(())
    }

    /// Check a quote-debit budget against a computable catalog minimum.
    pub fn preflight_quote_budget(&self, symbol: &str, scaled: i64, scale: u32) -> Result<()> {
        let constraints = self.pair_constraints_for_symbol(symbol).ok_or_else(|| {
            Error::validation(format!(
                "constraints for {symbol:?} are unavailable; await client.wait_for_catalogs()"
            ))
        })?;
        if scaled <= 0 {
            return Err(Error::validation("quote budget must be positive"));
        }
        if let Some(expected) = constraints.quote_quantity_scale
            && scale != expected
        {
            return Err(Error::validation(format!(
                "quote budget scale mismatch for {symbol}: got {scale}, expected {expected}"
            )));
        }
        if let Some(minimum) = constraints.min_notional_quote_scaled
            && i128::from(scaled) < minimum
        {
            return Err(Error::validation(format!(
                "quote budget for {symbol} is below catalog min_notional_quote"
            )));
        }
        Ok(())
    }

    /// Returns the pair base quantity scale, or `None` when unknown/unhydrated.
    ///
    /// Never invents scale 8 for missing symbols — callers that need a decode
    /// fallback must choose it explicitly.
    pub fn base_quantity_scale_for_symbol(&self, symbol: &str) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .symbol_to_base_scale
            .get(symbol)
            .copied()
    }

    pub fn base_quantity_scale_for_symbol_id(&self, id: u32) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .id_to_base_scale
            .get(&id)
            .copied()
    }

    /// Returns the pair quote quantity scale, or `None` when unknown/unhydrated.
    ///
    /// Quote-debit budgets must use this scale. Callers must not infer it from
    /// the base quantity scale or from the quote asset's display decimals.
    pub fn quote_quantity_scale_for_symbol(&self, symbol: &str) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .symbol_to_quote_scale
            .get(symbol)
            .copied()
    }

    pub fn quote_quantity_scale_for_symbol_id(&self, id: u32) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .id_to_quote_scale
            .get(&id)
            .copied()
    }

    pub fn quantity_scale_for_zipped_asset_id(&self, id: u32) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .zipped_id_to_scale
            .get(&id)
            .copied()
    }

    pub fn orderbook_price_buckets_for_symbol(&self, symbol: &str) -> Vec<String> {
        read_unpoisoned(&self.inner)
            .orderbook_buckets
            .get(symbol)
            .cloned()
            .unwrap_or_default()
    }

    pub fn ledger_id_for_asset(&self, symbol: &str) -> Option<u32> {
        read_unpoisoned(&self.inner)
            .asset_to_ledger_id
            .get(symbol)
            .copied()
    }

    /// Latest supply string for a zipped asset id, if patched from realtime updates.
    pub fn supply_for_zipped_asset_id(&self, id: u32) -> Option<String> {
        read_unpoisoned(&self.inner)
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
        let mut inner = write_unpoisoned(&self.inner);
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

    #[cfg(test)]
    fn poison_for_test(&self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.inner.write().expect("catalog lock");
            panic!("poison catalog");
        }));
        assert!(result.is_err(), "poison panic must unwind");
        assert!(
            self.inner.read().is_err(),
            "catalog RwLock must be poisoned for this test"
        );
    }
}

fn apply_spot(inner: &mut Inner, snap: SpotSnapshot) {
    // Replace spot maps wholesale so a refresh cannot leave stale symbols.
    inner.symbol_to_id = snap.symbol_to_id;
    inner.id_to_base_scale = snap.id_to_base_scale;
    inner.symbol_to_base_scale = snap.symbol_to_base_scale;
    inner.id_to_quote_scale = snap.id_to_quote_scale;
    inner.symbol_to_quote_scale = snap.symbol_to_quote_scale;
    inner.symbol_to_constraints = snap.symbol_to_constraints;
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
                "quote_quantity_scale": 6,
                "orderbook_price_buckets": [0.01, 0.1, 1.0]
            }]
        }))
        .expect("hydrate");
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), Some(8));
        assert_eq!(mgr.base_quantity_scale_for_symbol_id(1), Some(8));
        assert_eq!(mgr.quote_quantity_scale_for_symbol("BTC-USDT"), Some(6));
        assert_eq!(mgr.quote_quantity_scale_for_symbol_id(1), Some(6));
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
                "quantity_scale": 6,
                "variants": [{
                    "zipped_asset_id": 42
                }]
            }]
        }))
        .expect("hydrate");
        assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(42), Some(6));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(99), None);
    }

    #[test]
    fn unknown_symbol_returns_none_not_default_scale() {
        let mgr = Manager::new();
        assert_eq!(mgr.base_quantity_scale_for_symbol("NOPE"), None);
        assert_eq!(mgr.base_quantity_scale_for_symbol("ETH-USDT"), None);
        assert_eq!(mgr.quote_quantity_scale_for_symbol("NOPE"), None);
        assert_eq!(mgr.quote_quantity_scale_for_symbol_id(999), None);
    }

    #[test]
    fn symbol_filters_fail_closed_and_preserve_omission() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
        assert_eq!(mgr.resolve_symbol_filter(None).unwrap(), None);
        assert_eq!(mgr.resolve_symbol_filter(Some("  ")).unwrap(), None);
        assert_eq!(
            mgr.resolve_symbol_filter(Some(" BTC-USDT ")).unwrap(),
            Some("BTC-USDT".into())
        );
        let err = mgr.resolve_symbol_filter(Some("NOPE-USDT")).unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("unknown symbol"));
    }

    #[test]
    fn pair_constraints_are_exposed_and_preflight_is_deterministic() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 3,
                "quote_quantity_scale": 2,
                "tick_size": "0.01",
                "step_size": "0.01",
                "min_qty_base": "0.02",
                "min_notional_quote": "10"
            }]
        }))
        .unwrap();
        let constraints = mgr.pair_constraints_for_symbol("BTC-USDT").unwrap();
        assert_eq!(constraints.tick_size_ticks, Some(10_000));
        assert_eq!(constraints.step_size_scaled, Some(10));
        assert_eq!(constraints.min_qty_scaled, Some(20));
        assert_eq!(constraints.min_notional_quote_scaled, Some(1_000));

        mgr.preflight_order_values("BTC-USDT", Some(2_000), Some(5_000_000))
            .unwrap();
        assert!(
            mgr.preflight_order_values("BTC-USDT", Some(2_001), Some(5_000_000))
                .unwrap_err()
                .to_string()
                .contains("step_size")
        );
        assert!(
            mgr.preflight_order_values("BTC-USDT", Some(10), Some(5_000_000))
                .unwrap_err()
                .to_string()
                .contains("min_qty")
        );
        assert!(
            mgr.preflight_order_values("BTC-USDT", Some(2_000), Some(5_000_001))
                .unwrap_err()
                .to_string()
                .contains("tick_size")
        );
        assert!(
            mgr.preflight_order_values("BTC-USDT", Some(20), Some(5_000_000))
                .unwrap_err()
                .to_string()
                .contains("min_notional")
        );
    }

    #[test]
    fn zero_optional_pair_constraints_are_treated_as_unset() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 3,
                "quote_quantity_scale": 2,
                "tick_size": "0.01",
                "step_size": "0.01",
                "min_qty_base": "0",
                "min_notional_quote": "0"
            }]
        }))
        .unwrap();
        let constraints = mgr.pair_constraints_for_symbol("BTC-USDT").unwrap();
        assert_eq!(constraints.tick_size_ticks, Some(10_000));
        assert_eq!(constraints.step_size_scaled, Some(10));
        assert_eq!(constraints.min_qty_scaled, None);
        assert_eq!(constraints.min_notional_quote_scaled, None);
    }

    #[test]
    fn malformed_constraints_do_not_replace_existing_snapshot() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
        let err = mgr
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": "ETH-USDT",
                    "symbol_id": 2,
                    "base_quantity_scale": 6,
                    "tick_size": "-0.01"
                }]
            }))
            .unwrap_err();
        assert!(err.to_string().contains("tick_size"));
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
        assert_eq!(mgr.symbol_id_for_symbol("ETH-USDT"), None);
    }

    #[test]
    fn malformed_quote_scale_rejects_catalog_atomically() {
        let mgr = Manager::new();
        let err = mgr
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": "BTC-USDT",
                    "symbol_id": 1,
                    "base_quantity_scale": 8,
                    "quote_quantity_scale": 65535
                }]
            }))
            .expect_err("invalid quote scale must fail");
        assert!(err.to_string().contains("scale"));
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), None);
        assert_eq!(mgr.quote_quantity_scale_for_symbol("BTC-USDT"), None);
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

    #[test]
    fn contradictory_spot_identities_fail_without_replacing_previous_catalog() {
        let mgr = Manager::new();
        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();

        for malformed in [
            json!({"pairs": [
                {"symbol": "ETH-USDT", "symbol_id": 2, "base_quantity_scale": 6},
                {"symbol": "SOL-USDT", "symbol_id": 2, "base_quantity_scale": 8}
            ]}),
            json!({"pairs": [
                {"symbol": "ETH-USDT", "symbol_id": 2, "base_quantity_scale": 6},
                {"symbol": "ETH-USDT", "symbol_id": 3, "base_quantity_scale": 8}
            ]}),
            json!({"pairs": [
                {"symbol": "", "symbol_id": 2, "base_quantity_scale": 6}
            ]}),
            json!({"pairs": [
                {"symbol": "ETH-USDT", "symbol_id": 2}
            ]}),
        ] {
            assert!(mgr.hydrate_spot_config_json(malformed).is_err());
            assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
            assert_eq!(mgr.symbol_id_for_symbol("ETH-USDT"), None);
        }
    }

    #[test]
    fn contradictory_zipper_identities_fail_without_replacing_previous_catalog() {
        let mgr = Manager::new();
        mgr.hydrate_zipper_config_json(json!({
            "assets": [{
                "asset": "USDT",
                "ledger_id": 99,
                "quantity_scale": 6,
                "variants": [{"zipped_asset_id": 42}]
            }]
        }))
        .unwrap();

        for malformed in [
            json!({"assets": [
                {"asset": "BTC", "ledger_id": 1, "quantity_scale": 8},
                {"asset": "ETH", "ledger_id": 1, "quantity_scale": 6}
            ]}),
            json!({"assets": [
                {"asset": "BTC", "ledger_id": 1, "quantity_scale": 8},
                {"asset": "BTC", "ledger_id": 2, "quantity_scale": 6}
            ]}),
            json!({"assets": [
                {"asset": "", "ledger_id": 1, "quantity_scale": 8}
            ]}),
            json!({"assets": [
                {"asset": "BTC", "ledger_id": 1}
            ]}),
            json!({"assets": [
                {"asset": "BTC", "ledger_id": 1, "quantity_scale": 8,
                 "variants": [{"zipped_asset_id": 7}]},
                {"asset": "ETH", "ledger_id": 2, "quantity_scale": 6,
                 "variants": [{"zipped_asset_id": 7}]}
            ]}),
        ] {
            assert!(mgr.hydrate_zipper_config_json(malformed).is_err());
            assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
            assert_eq!(mgr.ledger_id_for_asset("BTC"), None);
            assert_eq!(mgr.quantity_scale_for_zipped_asset_id(42), Some(6));
        }
    }

    fn seed_ready_catalog(mgr: &Manager) {
        mgr.hydrate_spot_and_zipper_json(
            json!({
                "pairs": [{
                    "symbol": "BTC-USDT",
                    "symbol_id": 1,
                    "base_quantity_scale": 8,
                    "orderbook_price_buckets": [0.01, 0.1]
                }]
            }),
            json!({
                "assets": [{
                    "asset": "USDT",
                    "ledger_id": 99,
                    "quantity_scale": 6,
                    "variants": [{"zipped_asset_id": 42}]
                }]
            }),
        )
        .expect("hydrate");
        assert!(mgr.patch_zipper_supply(&[ZippedAssetSupplyUpdate {
            zipped_asset_id: 42,
            supply: "100.5".to_owned(),
        }]));
    }

    #[test]
    fn poisoned_catalog_reads_still_return_hydrated_scale_data() {
        let mgr = Manager::new();
        seed_ready_catalog(&mgr);
        mgr.poison_for_test();

        // Poison must not masquerade as "symbol/asset not found" — that would
        // route around fail-closed scale lookups by reporting absence.
        assert!(mgr.is_ready());
        assert_eq!(mgr.symbol_id_for_symbol("BTC-USDT"), Some(1));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), Some(8));
        assert_eq!(mgr.base_quantity_scale_for_symbol_id(1), Some(8));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(42), Some(6));
        assert_eq!(mgr.ledger_id_for_asset("USDT"), Some(99));
        assert_eq!(
            mgr.orderbook_price_buckets_for_symbol("BTC-USDT"),
            vec!["0.01".to_owned(), "0.1".to_owned()]
        );
        assert_eq!(mgr.supply_for_zipped_asset_id(42).as_deref(), Some("100.5"));
        assert_eq!(mgr.base_quantity_scale_for_symbol("NOPE"), None);
    }

    #[test]
    fn poisoned_catalog_writes_still_hydrate_and_patch() {
        let mgr = Manager::new();
        seed_ready_catalog(&mgr);
        mgr.poison_for_test();

        mgr.hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "ETH-USDT",
                "symbol_id": 2,
                "base_quantity_scale": 6
            }]
        }))
        .expect("spot write after poison");
        assert_eq!(mgr.base_quantity_scale_for_symbol("ETH-USDT"), Some(6));
        assert_eq!(mgr.base_quantity_scale_for_symbol("BTC-USDT"), None);

        assert!(mgr.patch_zipper_supply(&[ZippedAssetSupplyUpdate {
            zipped_asset_id: 42,
            supply: "200".to_owned(),
        }]));
        assert_eq!(mgr.supply_for_zipped_asset_id(42).as_deref(), Some("200"));

        mgr.hydrate_zipper_config_json(json!({
            "assets": [{
                "asset": "BTC",
                "ledger_id": 1,
                "quantity_scale": 8,
                "variants": [{"zipped_asset_id": 7}]
            }]
        }))
        .expect("zipper write after poison");
        assert_eq!(mgr.ledger_id_for_asset("BTC"), Some(1));
        assert_eq!(mgr.quantity_scale_for_zipped_asset_id(7), Some(8));
        // Live supply map is preserved across zipper replacement.
        assert_eq!(mgr.supply_for_zipped_asset_id(42).as_deref(), Some("200"));
    }
}
