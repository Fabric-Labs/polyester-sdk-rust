//! Public market-data decoders (trades / candles).

use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::{format_price_ticks, format_qty_scaled};
use crate::models::{Candle, CandlesResult, MarketTrade, MarketTradesResult};
use crate::proto::marketdata::v1::{
    CandlePoint, GetCandlesResponse, GetTradesResponse, MarketTrade as ProtoMarketTrade, Timeframe,
};

pub fn timeframe_label(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::Sec1 => "1s",
        Timeframe::Min1 => "1m",
        Timeframe::Min5 => "5m",
        Timeframe::Min15 => "15m",
        Timeframe::Min30 => "30m",
        Timeframe::Hour1 => "1h",
        Timeframe::Hour4 => "4h",
        Timeframe::Hour12 => "12h",
        Timeframe::Day1 => "1d",
        Timeframe::Week1 => "1w",
        Timeframe::Month1 => "1mo",
        Timeframe::TimeframeUnspecified => "",
    }
}

fn enum_value_timeframe(value: buffa::EnumValue<Timeframe>) -> String {
    value
        .as_known()
        .map(timeframe_label)
        .unwrap_or("")
        .to_owned()
}

pub fn market_trade_from_proto(msg: &ProtoMarketTrade) -> MarketTrade {
    let symbol_id = msg.symbol_id;
    let symbol_id_opt = if symbol_id == 0 {
        None
    } else {
        Some(symbol_id)
    };
    MarketTrade {
        symbol_id,
        match_id: if msg.match_id == 0 {
            String::new()
        } else {
            msg.match_id.to_string()
        },
        price: decode_price_ticks(msg.price_ticks, None),
        qty: decode_qty_scaled(msg.qty_scaled, None, None, symbol_id_opt),
        ts_ns: if msg.ts_ns == 0 {
            String::new()
        } else {
            msg.ts_ns.to_string()
        },
        side: if msg.is_buy {
            "buy".to_owned()
        } else {
            "sell".to_owned()
        },
    }
}

pub fn market_trades_from_proto(msg: &GetTradesResponse) -> MarketTradesResult {
    MarketTradesResult {
        trades: msg.trades.iter().map(market_trade_from_proto).collect(),
    }
}

pub fn candle_point_from_proto(
    msg: &CandlePoint,
    volume_scale: u32,
    symbol_id: u32,
    timeframe: &str,
) -> Candle {
    Candle {
        ts_sec: msg.ts_sec as i64,
        open: format_price_ticks(msg.open),
        high: format_price_ticks(msg.high),
        low: format_price_ticks(msg.low),
        close: format_price_ticks(msg.close),
        volume: format_qty_scaled(msg.volume, volume_scale),
        symbol_id,
        timeframe: timeframe.to_owned(),
    }
}

pub fn candles_from_proto(msg: &GetCandlesResponse, volume_scale: u32) -> CandlesResult {
    let timeframe = enum_value_timeframe(msg.timeframe);
    let symbol_id = msg.symbol_id;
    CandlesResult {
        symbol_id,
        timeframe: timeframe.clone(),
        candles: msg
            .candles
            .iter()
            .map(|c| candle_point_from_proto(c, volume_scale, symbol_id, &timeframe))
            .collect(),
        next_page_token: msg.next_page_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_trades_maps_side_and_ids() {
        let msg = GetTradesResponse {
            trades: vec![ProtoMarketTrade {
                symbol_id: 3,
                match_id: 99,
                is_buy: true,
                price_ticks: 1_500_000,
                qty_scaled: 100,
                ts_ns: 42,
                ..Default::default()
            }],
            ..Default::default()
        };
        let list = market_trades_from_proto(&msg);
        assert_eq!(list.trades.len(), 1);
        let t = &list.trades[0];
        assert_eq!(t.symbol_id, 3);
        assert_eq!(t.match_id, "99");
        assert_eq!(t.side, "buy");
        assert_eq!(t.price.as_ref().unwrap().as_ticks(), 1_500_000);
        assert_eq!(t.qty.as_ref().unwrap().as_scaled(), 100);
        assert_eq!(t.ts_ns, "42");
    }

    #[test]
    fn candles_format_ohlcv() {
        let msg = GetCandlesResponse {
            symbol_id: 1,
            timeframe: Timeframe::Min1.into(),
            candles: vec![CandlePoint {
                ts_sec: 10,
                open: 1_000_000,
                high: 2_000_000,
                low: 500_000,
                close: 1_500_000,
                volume: 100_000_000,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = candles_from_proto(&msg, 8);
        assert_eq!(result.symbol_id, 1);
        assert_eq!(result.timeframe, "1m");
        assert_eq!(result.candles.len(), 1);
        let c = &result.candles[0];
        assert_eq!(c.ts_sec, 10);
        assert_eq!(c.open, "1");
        assert_eq!(c.high, "2");
        assert_eq!(c.low, "0.5");
        assert_eq!(c.close, "1.5");
        assert_eq!(c.volume, "1");
    }
}
