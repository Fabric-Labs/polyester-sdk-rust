//! Trade sizing, pricing, balances, and symbol helpers.

use super::call::{call_optional, call_required};
use super::env::{env_trade_symbol, load_dotenv, min_trading_quote, skip_funding_check};
use polyester::codecs::scalars::{format_price_ticks, parse_price_ticks_str};
use polyester::models::AssetBalance;
use polyester::models::OrderbookData;
use polyester::proto::chain::zipper::v1::GetDepositWithdrawConfigResponse;
use polyester::proto::ledger::read::v1::GetBalancesRequest;
use polyester::proto::marketdata::v1::{GetSpotConfigResponse, PairConfig};
use polyester::{Client, Result};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

const SMOKE_CANDIDATES: &[&str] = &["ETH-USDT", "BTC-USDT", "SOL-USDT", "BNB-USDT"];

const FAR_BELOW_BUY_HINTS: &[(&str, &str)] = &[
    ("ETH-USDT", "100"),
    ("BTC-USDT", "1000"),
    ("SOL-USDT", "10"),
    ("BNB-USDT", "10"),
];

const FAR_ABOVE_BUY_STOP_HINTS: &[(&str, &str)] = &[
    ("ETH-USDT", "50000"),
    ("BTC-USDT", "500000"),
    ("SOL-USDT", "5000"),
    ("BNB-USDT", "5000"),
];

pub const LEDGER_SCALE: u32 = 18;

/// Prefer `POLYESTER_TEST_SMOKE_SYMBOL`, else first liquid candidate present, else first pair.
pub fn smoke_symbol(spot: &GetSpotConfigResponse) -> String {
    load_dotenv();
    if let Ok(sym) = std::env::var("POLYESTER_TEST_SMOKE_SYMBOL") {
        let trimmed = sym.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    if let Ok(sym) = std::env::var("POLYESTER_SMOKE_SYMBOL") {
        let trimmed = sym.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    let symbols: Vec<&str> = spot
        .pairs
        .iter()
        .map(|p| p.symbol.trim())
        .filter(|s| !s.is_empty())
        .collect();
    for candidate in SMOKE_CANDIDATES {
        if symbols.iter().any(|s| s == candidate) {
            return (*candidate).to_owned();
        }
    }
    symbols
        .first()
        .map(|s| (*s).to_owned())
        .unwrap_or_else(|| "BTC-USDT".to_owned())
}

pub fn trade_symbol(spot: &GetSpotConfigResponse) -> String {
    if let Some(override_sym) = env_trade_symbol() {
        return override_sym;
    }
    smoke_symbol(spot)
}

pub fn pair_for_symbol<'a>(
    spot: &'a GetSpotConfigResponse,
    symbol: &str,
) -> Option<&'a PairConfig> {
    spot.pairs.iter().find(|p| p.symbol == symbol)
}

/// Hydrate catalogs from spot + zipper; returns spot config.
pub async fn hydrate_spot_and_zipper(client: &Client) -> Result<GetSpotConfigResponse> {
    let spot = client.market_data.get_spot_config().await?;
    if let Ok(json) = serde_json::to_value(&spot) {
        client.catalogs.hydrate_spot_config_json(json);
    }
    if let Ok(zipper) = client.zipper.get_deposit_withdraw_config().await
        && let Ok(json) = serde_json::to_value(&zipper)
    {
        client.catalogs.hydrate_zipper_config_json(json);
    }
    Ok(spot)
}

pub fn unique_client_order_id(prefix: &str) -> String {
    let p = if prefix.is_empty() { "test" } else { prefix };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{p}-{nanos}")
}

pub fn u128_raw_from_str(s: &str) -> u128 {
    s.parse().unwrap_or(0)
}

pub fn ledger_amount_to_decimal(raw_str: &str) -> Decimal {
    let raw = u128_raw_from_str(raw_str);
    let s = raw.to_string();
    if s.len() <= LEDGER_SCALE as usize {
        let frac = format!("{s:0>18}");
        format!("0.{frac}").parse().unwrap_or(Decimal::ZERO)
    } else {
        let split = s.len() - LEDGER_SCALE as usize;
        let (whole, frac) = s.split_at(split);
        format!("{whole}.{frac}").parse().unwrap_or(Decimal::ZERO)
    }
}

pub fn trading_balance_human(balances: &[AssetBalance], asset_id: u32) -> Decimal {
    for row in balances {
        if row.asset_id == asset_id && !row.trading.is_empty() && row.trading != "0" {
            return ledger_amount_to_decimal(&row.trading);
        }
    }
    Decimal::ZERO
}

pub fn trading_balance_raw(balances: &[AssetBalance], asset_id: u32) -> u128 {
    for row in balances {
        if row.asset_id == asset_id {
            return u128_raw_from_str(&row.trading);
        }
    }
    0
}

pub fn quote_asset_id(
    spot: &GetSpotConfigResponse,
    symbol: &str,
    zipper: Option<&GetDepositWithdrawConfigResponse>,
) -> Option<u32> {
    let pair = pair_for_symbol(spot, symbol)?;
    if !pair.quote_asset.is_empty()
        && let Some(z) = zipper
    {
        for a in &z.assets {
            if a.asset == pair.quote_asset && a.ledger_id > 0 {
                return Some(a.ledger_id);
            }
        }
    }
    None
}

pub fn base_asset_id(
    spot: &GetSpotConfigResponse,
    symbol: &str,
    zipper: Option<&GetDepositWithdrawConfigResponse>,
) -> Option<u32> {
    let pair = pair_for_symbol(spot, symbol)?;
    if !pair.base_asset.is_empty()
        && let Some(z) = zipper
    {
        for a in &z.assets {
            if a.asset == pair.base_asset && a.ledger_id > 0 {
                return Some(a.ledger_id);
            }
        }
    }
    None
}

pub fn far_above_buy_stop_price(symbol: &str) -> String {
    load_dotenv();
    if let Ok(v) = std::env::var("POLYESTER_TEST_TRIGGER_PRICE") {
        let t = v.trim();
        if !t.is_empty() {
            return t.to_owned();
        }
    }
    FAR_ABOVE_BUY_STOP_HINTS
        .iter()
        .find(|(s, _)| *s == symbol)
        .map(|(_, p)| (*p).to_owned())
        .unwrap_or_else(|| "50000".to_owned())
}

fn far_below_hint(symbol: &str) -> String {
    FAR_BELOW_BUY_HINTS
        .iter()
        .find(|(s, _)| *s == symbol)
        .map(|(_, p)| (*p).to_owned())
        .unwrap_or_else(|| "100".to_owned())
}

fn post_only_buy_price_from_book(book: &OrderbookData, tick_size: &str) -> Option<String> {
    if book.bids.is_empty() {
        return None;
    }
    let tick_ticks = parse_price_ticks_str(tick_size, "tick_size").ok()?;
    if tick_ticks == 0 {
        return None;
    }
    let bid_ticks = book.bids[0].price.as_ref()?.as_ticks();
    let mut target = bid_ticks - tick_ticks;
    if target < tick_ticks {
        target = tick_ticks;
    }
    if let Some(ask) = book.asks.first()
        && let Some(ask_px) = ask.price.as_ref()
        && ask_px.as_ticks() > 0
    {
        let max_post_only = ask_px.as_ticks() - tick_ticks;
        if target > max_post_only {
            target = max_post_only;
        }
    }
    if target < tick_ticks {
        return None;
    }
    Some(format_price_ticks(target))
}

fn post_only_buy_price_from_last(last_ticks: i64, tick_size: &str, symbol: &str) -> Option<String> {
    if last_ticks <= 0 {
        return None;
    }
    let tick_ticks = parse_price_ticks_str(tick_size, "tick_size").ok()?;
    if tick_ticks == 0 {
        return None;
    }
    let target = last_ticks * 995 / 1000;
    let mut aligned = (target / tick_ticks) * tick_ticks;
    if aligned < tick_ticks {
        return Some(far_below_hint(symbol));
    }
    if aligned < tick_ticks {
        aligned = tick_ticks;
    }
    Some(format_price_ticks(aligned))
}

pub async fn resolve_post_only_buy_limit_price(
    client: &Client,
    symbol: &str,
    pair: Option<&PairConfig>,
) -> String {
    load_dotenv();
    for key in ["POLYESTER_TEST_PRICE", "POLYESTER_SMOKE_PRICE"] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if !t.is_empty() {
                return t.to_owned();
            }
        }
    }
    let tick_size = pair
        .map(|p| {
            if p.tick_size.trim().is_empty() {
                "0.01"
            } else {
                p.tick_size.trim()
            }
        })
        .unwrap_or("0.01");

    if let Ok(book) = client.orderbook.get(symbol, None).await
        && let Some(price) = post_only_buy_price_from_book(&book, tick_size)
    {
        return price;
    }
    if let Ok(overview) = client.market_overview.list(Some(5)).await {
        for row in &overview.markets {
            if row.symbol == symbol
                && let Some(last) = row.last_price.as_ref()
                && last.as_ticks() > 0
                && let Some(price) =
                    post_only_buy_price_from_last(last.as_ticks(), tick_size, symbol)
            {
                return price;
            }
        }
    }
    far_below_hint(symbol)
}

pub fn min_base_qty_for_pair(pair: Option<&PairConfig>, price: &str) -> String {
    load_dotenv();
    for key in ["POLYESTER_TEST_QTY", "POLYESTER_SMOKE_QTY"] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if !t.is_empty() {
                return t.to_owned();
            }
        }
    }
    let step_str = pair
        .map(|p| {
            if p.step_size.trim().is_empty() {
                "0.001".to_owned()
            } else {
                p.step_size.trim().to_owned()
            }
        })
        .unwrap_or_else(|| "0.001".to_owned());
    let min_notional_str = pair
        .map(|p| {
            if p.min_notional_quote.trim().is_empty() {
                "10".to_owned()
            } else {
                p.min_notional_quote.trim().to_owned()
            }
        })
        .unwrap_or_else(|| "10".to_owned());
    let min_qty_str = pair
        .map(|p| {
            if p.min_qty_base.trim().is_empty() {
                step_str.clone()
            } else {
                p.min_qty_base.trim().to_owned()
            }
        })
        .unwrap_or_else(|| step_str.clone());

    let step: f64 = step_str.parse().unwrap_or(0.001);
    let price_f: f64 = price.parse().unwrap_or(0.0);
    if step <= 0.0 || price_f <= 0.0 {
        return step_str;
    }
    let min_notional: f64 = min_notional_str.parse::<f64>().unwrap_or(10.0).max(10.0);
    let min_qty: f64 = min_qty_str.parse::<f64>().unwrap_or(step).max(step);
    let mut units = (min_notional / price_f / step).ceil();
    let min_units = (min_qty / step).ceil();
    if units < min_units {
        units = min_units;
    }
    if units < 1.0 {
        units = 1.0;
    }
    let qty = units * step;
    // Prefer shortest float formatting without scientific notation.
    let s = format!("{qty:.10}");
    s.trim_end_matches('0').trim_end_matches('.').to_owned()
}

pub async fn usdt_funded_buy_limit_params(
    client: &Client,
    symbol: &str,
) -> Result<(String, String)> {
    let spot = hydrate_spot_and_zipper(client).await?;
    let pair = pair_for_symbol(&spot, symbol);
    let price = resolve_post_only_buy_limit_price(client, symbol, pair).await;
    let qty = min_base_qty_for_pair(pair, &price);
    Ok((price, qty))
}

pub async fn market_ref_price(
    client: &Client,
    symbol: &str,
    side: &str,
    pair: Option<&PairConfig>,
) -> String {
    load_dotenv();
    if let Ok(v) = std::env::var("POLYESTER_TEST_TRADE_PRICE") {
        let t = v.trim();
        if !t.is_empty() {
            return t.to_owned();
        }
    }
    if let Ok(book) = client.orderbook.get(symbol, None).await {
        let side_l = side.trim().to_ascii_lowercase();
        if side_l == "sell" {
            if let Some(bid) = book.bids.first()
                && let Some(px) = bid.price.as_ref()
                && px.as_ticks() > 0
            {
                return format_price_ticks(px.as_ticks());
            }
        } else if let Some(ask) = book.asks.first()
            && let Some(px) = ask.price.as_ref()
            && px.as_ticks() > 0
        {
            return format_price_ticks(px.as_ticks());
        }
        if let Some(ask) = book.asks.first()
            && let Some(px) = ask.price.as_ref()
            && px.as_ticks() > 0
        {
            return format_price_ticks(px.as_ticks());
        }
        if let Some(bid) = book.bids.first()
            && let Some(px) = bid.price.as_ref()
            && px.as_ticks() > 0
        {
            return format_price_ticks(px.as_ticks());
        }
    }
    if let Ok(overview) = client.market_overview.list(Some(5)).await {
        for row in &overview.markets {
            if row.symbol == symbol
                && let Some(last) = row.last_price.as_ref()
                && last.as_ticks() > 0
            {
                return format_price_ticks(last.as_ticks());
            }
        }
    }
    let _ = pair;
    far_above_buy_stop_price(symbol)
}

/// Soft-skip when quote trading balance is below minimum. Returns false when skipped.
pub async fn require_trading_quote_balance(client: &Client, symbol: &str) -> bool {
    if skip_funding_check() {
        return true;
    }
    let Ok(spot) = hydrate_spot_and_zipper(client).await else {
        eprintln!("skip: cannot hydrate spot for funding check");
        return false;
    };
    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset_id) = quote_asset_id(&spot, symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve quote asset for {symbol}");
        return false;
    };
    let balances = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let balance = trading_balance_human(&balances.balances, asset_id);
    let minimum = min_trading_quote();
    if balance < minimum {
        eprintln!(
            "skip: trading balance {balance} below minimum {minimum} for asset {asset_id}; \
             fund or set POLYESTER_TEST_SKIP_FUNDING_CHECK=1"
        );
        return false;
    }
    true
}

/// Soft-skip when base trading balance is below qty.
pub async fn require_trading_base_balance(client: &Client, symbol: &str, qty: &str) -> bool {
    if skip_funding_check() {
        return true;
    }
    let Ok(spot) = hydrate_spot_and_zipper(client).await else {
        eprintln!("skip: cannot hydrate spot for funding check");
        return false;
    };
    let zipper = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await;
    let Some(asset_id) = base_asset_id(&spot, symbol, zipper.as_ref()) else {
        eprintln!("skip: cannot resolve base asset for {symbol}");
        return false;
    };
    let Ok(qty_dec) = qty.trim().parse::<Decimal>() else {
        eprintln!("skip: invalid qty {qty}");
        return false;
    };
    let balances = call_required("balances.list", || {
        client.balances.list(GetBalancesRequest::default())
    })
    .await;
    let balance = trading_balance_human(&balances.balances, asset_id);
    if balance < qty_dec {
        eprintln!(
            "skip: trading base balance {balance} below required {qty_dec} for asset {asset_id}"
        );
        return false;
    }
    true
}

pub async fn usdt_funded_buy_stop_params(
    client: &Client,
    symbol: &str,
) -> Result<(String, String, String)> {
    let spot = hydrate_spot_and_zipper(client).await?;
    let pair = pair_for_symbol(&spot, symbol);
    let trigger_price = far_above_buy_stop_price(symbol);
    load_dotenv();
    let limit_price = std::env::var("POLYESTER_TEST_TRIGGER_LIMIT_PRICE")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| trigger_price.clone());
    let qty = min_base_qty_for_pair(pair, &limit_price);
    Ok((trigger_price, limit_price, qty))
}

/// Scale a human decimal quantity to ledger units (string of integer).
pub fn scaled_quantity_string(quantity: &str, scale: u32) -> Result<String> {
    use polyester::Error;
    let qty: Decimal = quantity
        .trim()
        .parse()
        .map_err(|_| Error::validation(format!("invalid quantity {quantity:?}")))?;
    let mult = Decimal::from(10u64.pow(scale));
    let scaled = qty * mult;
    if scaled != scaled.trunc() {
        return Err(Error::validation(format!(
            "quantity {quantity:?} does not scale cleanly to {scale} decimals"
        )));
    }
    Ok(scaled
        .to_u128()
        .ok_or_else(|| Error::validation("scaled quantity out of range"))?
        .to_string())
}
