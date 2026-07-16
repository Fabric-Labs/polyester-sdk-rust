use crate::support::{
    call_optional, call_required, hydrate_spot_and_zipper, require_live_client, smoke_symbol,
};

#[tokio::test]
async fn spot_config_has_pairs() {
    let Some(client) = require_live_client() else {
        return;
    };
    let cfg = call_required("market_data.get_spot_config", || {
        client.market_data.get_spot_config()
    })
    .await;
    assert!(!cfg.pairs.is_empty(), "expected spot pairs");
    for pair in &cfg.pairs {
        assert!(
            !pair.symbol.trim().is_empty(),
            "pair missing symbol: {pair:?}"
        );
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
    let cfg = call_required("market_data.get_spot_config", || {
        client.market_data.get_spot_config()
    })
    .await;
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
