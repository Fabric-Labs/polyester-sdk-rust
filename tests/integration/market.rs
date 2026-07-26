use crate::support::{
    call_optional, call_required, hydrate_spot_and_zipper, require_live_client, smoke_symbol,
};
use polyester::services::{CreateSubscriptionOptions, MarketOverviewCreateSubscriptionOptions};
use std::time::Duration;

#[tokio::test]
async fn spot_config_has_pairs() {
    let Some(client) = require_live_client() else {
        return;
    };
    let cfg = call_required("market_data.get_spot_config", || {
        client.market_data.get_spot_config()
    })
    .await;
    let pairs = cfg
        .raw
        .get("pairs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(!pairs.is_empty(), "expected spot pairs");
    for pair in &pairs {
        let symbol = pair
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        assert!(!symbol.is_empty(), "pair missing symbol: {pair:?}");
    }
}

#[tokio::test]
async fn market_overview_list_returns_markets() {
    let Some(client) = require_live_client() else {
        return;
    };
    let overview = call_required("market_overview.list", || {
        client.market_overview.list(Some(10))
    })
    .await;
    assert!(
        !overview.markets.is_empty(),
        "expected market overview entries"
    );
    for market in &overview.markets {
        assert!(
            !market.symbol.trim().is_empty(),
            "market missing symbol: {market:?}"
        );
    }
}

#[tokio::test]
async fn orderbook_get_for_smoke_symbol() {
    let Some(client) = require_live_client() else {
        return;
    };
    let cfg = hydrate_spot_and_zipper(&client)
        .await
        .expect("hydrate catalogs for orderbook");
    let symbol = smoke_symbol(&cfg);
    let Some(book) = call_optional("orderbook.get", || client.orderbook.get(&symbol, None)).await
    else {
        return;
    };
    assert!(
        !book.book_seq.is_empty() || !book.bids.is_empty() || !book.asks.is_empty(),
        "expected orderbook payload for {symbol}: {book:?}"
    );
}

#[tokio::test]
async fn zipper_deposit_withdraw_config_optional() {
    let Some(client) = require_live_client() else {
        return;
    };
    let Some(cfg) = call_optional("zipper.get_deposit_withdraw_config", || {
        client.zipper.get_deposit_withdraw_config()
    })
    .await
    else {
        return;
    };
    let _ = (cfg.assets.len(), cfg.chains.len());
}

#[tokio::test]
async fn market_data_get_trades() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let Some(trades) = call_optional("market_data.get_trades", || {
        client.market_data.get_trades(&symbol, Some(5))
    })
    .await
    else {
        return;
    };
    for trade in &trades.trades {
        assert!(
            trade.symbol_id != 0 || !trade.match_id.is_empty(),
            "trade missing ids: {trade:?}"
        );
    }
}

#[tokio::test]
async fn market_data_get_candles() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = match hydrate_spot_and_zipper(&client).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("skip: hydrate failed: {err}");
            return;
        }
    };
    let symbol = smoke_symbol(&spot);
    let Some(candles) = call_optional("market_data.get_candles", || {
        client.market_data.get_candles(&symbol, "1m", Some(5))
    })
    .await
    else {
        return;
    };
    for candle in &candles.candles {
        let _ = candle;
    }
}

#[tokio::test]
async fn managed_market_overview_starts_with_snapshot() {
    let Some(client) = require_live_client() else {
        return;
    };
    let mut sub = client
        .market_overview
        .create_subscription(MarketOverviewCreateSubscriptionOptions {
            limit: Some(10),
            ..Default::default()
        })
        .await
        .expect("market overview managed subscribe");
    let rows = tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
        .await
        .expect("market overview initial snapshot timed out")
        .expect("market overview subscription closed");
    assert!(!rows.is_empty(), "expected initial market overview rows");
    assert!(
        sub.err().is_none(),
        "market overview error: {:?}",
        sub.err()
    );
    sub.close();
}

#[tokio::test]
async fn managed_orderbook_starts_with_snapshot() {
    let Some(client) = require_live_client() else {
        return;
    };
    let spot = hydrate_spot_and_zipper(&client)
        .await
        .expect("hydrate spot for managed orderbook");
    let symbol = smoke_symbol(&spot);
    let mut sub = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: symbol.clone(),
            depth: Some(50),
            ..Default::default()
        })
        .await
        .expect("orderbook managed subscribe");
    let book = tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
        .await
        .expect("orderbook initial snapshot timed out")
        .expect("orderbook subscription closed");
    assert_eq!(book.symbol, symbol);
    assert!(
        !book.book_seq.is_empty() || !book.bids.is_empty() || !book.asks.is_empty(),
        "expected initial orderbook state"
    );
    assert!(sub.err().is_none(), "orderbook error: {:?}", sub.err());
    sub.close();
}
