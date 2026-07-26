//! Public market-data decoders (trades / candles).

use super::common::api_data_from_proto;
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::{format_price_ticks, format_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{Candle, CandlesResult, MarketTrade, MarketTradesResult, SpotConfig};
use crate::proto::marketdata::v1::{
    CandlePoint, GetCandlesColumnsResponse, GetCandlesResponse, GetSpotConfigResponse,
    GetTradesResponse, MarketTrade as ProtoMarketTrade, Timeframe,
};

pub fn spot_config_from_proto(msg: &GetSpotConfigResponse) -> SpotConfig {
    SpotConfig {
        raw: api_data_from_proto(msg).raw,
    }
}

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
        next_page_token: msg.next_page_token.clone(),
    }
}

pub fn candle_point_from_proto(
    msg: &CandlePoint,
    volume_scale: u32,
    symbol_id: u32,
    timeframe: &str,
) -> Result<Candle> {
    Ok(Candle {
        ts_sec: msg.ts_sec as i64,
        open: format_price_ticks(msg.open),
        high: format_price_ticks(msg.high),
        low: format_price_ticks(msg.low),
        close: format_price_ticks(msg.close),
        volume: format_qty_scaled(msg.volume, volume_scale)
            .map_err(|e| Error::validation(format!("candle volume scale invalid: {e}")))?,
        symbol_id,
        timeframe: timeframe.to_owned(),
    })
}

pub fn candles_from_proto(msg: &GetCandlesResponse, volume_scale: u32) -> Result<CandlesResult> {
    let timeframe = enum_value_timeframe(msg.timeframe);
    let symbol_id = msg.symbol_id;
    let mut candles = Vec::with_capacity(msg.candles.len());
    for c in &msg.candles {
        candles.push(candle_point_from_proto(
            c,
            volume_scale,
            symbol_id,
            &timeframe,
        )?);
    }
    Ok(CandlesResult {
        symbol_id,
        timeframe,
        candles,
        next_page_token: msg.next_page_token.clone(),
    })
}

/// Decode columnar OHLCV into row-oriented [`CandlesResult`] (Go `CandlesColumnsFromProto`).
pub fn candles_columns_from_proto(
    msg: &GetCandlesColumnsResponse,
    volume_scale: u32,
) -> Result<CandlesResult> {
    let timeframe = enum_value_timeframe(msg.timeframe);
    let symbol_id = msg.symbol_id;
    let mut candles = Vec::with_capacity(msg.ts_sec.len());
    for (i, &ts) in msg.ts_sec.iter().enumerate() {
        let volume = match msg.volume.get(i).copied() {
            Some(v) => format_qty_scaled(v, volume_scale)
                .map_err(|e| Error::validation(format!("candle volume scale invalid: {e}")))?,
            None => String::new(),
        };
        candles.push(Candle {
            ts_sec: ts as i64,
            open: msg
                .open
                .get(i)
                .copied()
                .map(format_price_ticks)
                .unwrap_or_default(),
            high: msg
                .high
                .get(i)
                .copied()
                .map(format_price_ticks)
                .unwrap_or_default(),
            low: msg
                .low
                .get(i)
                .copied()
                .map(format_price_ticks)
                .unwrap_or_default(),
            close: msg
                .close
                .get(i)
                .copied()
                .map(format_price_ticks)
                .unwrap_or_default(),
            volume,
            symbol_id,
            timeframe: timeframe.clone(),
        });
    }
    Ok(CandlesResult {
        symbol_id,
        timeframe,
        candles,
        next_page_token: msg.next_page_token.clone(),
    })
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
            next_page_token: "page-2".into(),
            ..Default::default()
        };
        let list = market_trades_from_proto(&msg);
        assert_eq!(list.trades.len(), 1);
        assert_eq!(list.next_page_token, "page-2");
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
        let result = candles_from_proto(&msg, 8).expect("candles");
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

    #[test]
    fn candle_decode_rejects_invalid_volume_scale() {
        let msg = GetCandlesResponse {
            symbol_id: 1,
            timeframe: Timeframe::Min1.into(),
            candles: vec![CandlePoint {
                ts_sec: 10,
                volume: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = candles_from_proto(&msg, 65535).expect_err("invalid scale");
        assert!(err.to_string().to_ascii_lowercase().contains("scale"));
    }

    #[test]
    fn candles_columns_decode_rejects_invalid_volume_scale() {
        let msg = GetCandlesColumnsResponse {
            symbol_id: 1,
            timeframe: Timeframe::Min1.into(),
            ts_sec: vec![10],
            volume: vec![1],
            ..Default::default()
        };
        let err = candles_columns_from_proto(&msg, 65535).expect_err("invalid scale");
        assert!(err.to_string().to_ascii_lowercase().contains("scale"));
    }
}
