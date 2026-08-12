//! Public market-data decoders (trades / candles).

use serde_json::Value;

use super::common::api_data_from_proto;
use super::money::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::{TsNs, format_price_ticks, format_qty_scaled};
use crate::errors::{Error, Result};
use crate::models::{Candle, CandlesResult, MarketTrade, MarketTradesResult, SpotConfig};
use crate::proto::marketdata::v1::{
    CandlePoint, GetCandlesColumnsResponse, GetCandlesResponse, GetSpotConfigResponse,
    GetTradesResponse, MarketTrade as ProtoMarketTrade, Timeframe,
};

pub fn spot_config_from_proto(msg: &GetSpotConfigResponse) -> SpotConfig {
    let mut raw = api_data_from_proto(msg).raw;
    // proto3 omits scalar zeroes during proto-JSON conversion. Quantity scale
    // zero is nevertheless valid, so restore the typed wire value rather than
    // treating an omitted JSON key as an unknown scale.
    if let Some(pairs) = raw.get_mut("pairs").and_then(Value::as_array_mut) {
        for (pair, typed) in pairs.iter_mut().zip(&msg.pairs) {
            if let Some(object) = pair.as_object_mut() {
                // Proto-JSON uses camelCase. Writing the snake_case alias as a
                // second key makes serde reject the object as a duplicate field
                // when consumers re-deserialize into generated PairConfig types.
                object.remove("base_quantity_scale");
                object.insert(
                    "baseQuantityScale".to_owned(),
                    Value::from(typed.base_quantity_scale),
                );
                object.remove("quote_quantity_scale");
                object.insert(
                    "quoteQuantityScale".to_owned(),
                    Value::from(typed.quote_quantity_scale),
                );
            }
        }
    }
    SpotConfig { raw }
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
        .map(|known| timeframe_label(known).to_owned())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

pub fn market_trade_from_proto(msg: &ProtoMarketTrade, quantity_scale: u32) -> Result<MarketTrade> {
    let symbol_id = msg.symbol_id;
    let symbol_id_opt = if symbol_id == 0 {
        None
    } else {
        Some(symbol_id)
    };
    Ok(MarketTrade {
        symbol_id,
        match_id: if msg.match_id == 0 {
            String::new()
        } else {
            msg.match_id.to_string()
        },
        price: decode_price_ticks(msg.price_ticks, None),
        qty: decode_qty_scaled(msg.qty_scaled, Some(quantity_scale), None, symbol_id_opt),
        ts_ns: TsNs::from_wire(msg.ts_ns, "MarketTrade.ts_ns")?.optional_string(),
        side: if msg.is_buy {
            "buy".to_owned()
        } else {
            "sell".to_owned()
        },
    })
}

pub fn market_trades_from_proto(
    msg: &GetTradesResponse,
    quantity_scale: u32,
) -> Result<MarketTradesResult> {
    Ok(MarketTradesResult {
        trades: msg
            .trades
            .iter()
            .map(|trade| market_trade_from_proto(trade, quantity_scale))
            .collect::<Result<Vec<_>>>()?,
        next_page_token: msg.next_page_token.clone(),
    })
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
    let rows = msg.ts_sec.len();
    let lengths = [
        ("open", msg.open.len()),
        ("high", msg.high.len()),
        ("low", msg.low.len()),
        ("close", msg.close.len()),
        ("volume", msg.volume.len()),
    ];
    if lengths.iter().any(|(_, len)| *len != rows) {
        return Err(Error::transport(format!(
            "invalid GetCandlesColumns response lengths: ts_sec={rows}, open={}, high={}, low={}, close={}, volume={}",
            msg.open.len(),
            msg.high.len(),
            msg.low.len(),
            msg.close.len(),
            msg.volume.len()
        )));
    }

    let timeframe = enum_value_timeframe(msg.timeframe);
    let symbol_id = msg.symbol_id;
    let mut candles = Vec::with_capacity(rows);
    for (i, &ts) in msg.ts_sec.iter().enumerate() {
        let volume = format_qty_scaled(msg.volume[i], volume_scale)
            .map_err(|e| Error::validation(format!("candle volume scale invalid: {e}")))?;
        candles.push(Candle {
            ts_sec: ts as i64,
            open: format_price_ticks(msg.open[i]),
            high: format_price_ticks(msg.high[i]),
            low: format_price_ticks(msg.low[i]),
            close: format_price_ticks(msg.close[i]),
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
                ts_ns: 1_700_000_000_000_000_000,
                ..Default::default()
            }],
            next_page_token: "page-2".into(),
            ..Default::default()
        };
        let list = market_trades_from_proto(&msg, 6).unwrap();
        assert_eq!(list.trades.len(), 1);
        assert_eq!(list.next_page_token, "page-2");
        let t = &list.trades[0];
        assert_eq!(t.symbol_id, 3);
        assert_eq!(t.match_id, "99");
        assert_eq!(t.side, "buy");
        assert_eq!(t.price.as_ref().unwrap().as_ticks(), 1_500_000);
        assert_eq!(t.qty.as_ref().unwrap().as_scaled(), 100);
        assert_eq!(t.qty.as_ref().unwrap().format(None).unwrap(), "0.0001");
        assert_eq!(t.ts_ns, "1700000000000000000");
    }

    #[test]
    fn market_trade_accepts_millisecond_shaped_ts_ns() {
        let msg = ProtoMarketTrade {
            symbol_id: 1,
            ts_ns: 1_700_000_000_000,
            ..Default::default()
        };
        let trade = market_trade_from_proto(&msg, 8).unwrap();
        assert_eq!(trade.ts_ns, "1700000000000");
    }

    #[test]
    fn spot_config_preserves_valid_zero_quantity_scale() {
        let msg = GetSpotConfigResponse {
            pairs: vec![crate::proto::marketdata::v1::PairConfig {
                symbol: "WHOLE-USDT".into(),
                symbol_id: 9,
                base_quantity_scale: 0,
                quote_quantity_scale: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        let spot = spot_config_from_proto(&msg);
        assert_eq!(spot.raw["pairs"][0]["baseQuantityScale"], 0);
        assert_eq!(spot.raw["pairs"][0]["quoteQuantityScale"], 0);
        assert!(spot.raw["pairs"][0].get("base_quantity_scale").is_none());
        assert!(spot.raw["pairs"][0].get("quote_quantity_scale").is_none());
        let round_trip: GetSpotConfigResponse =
            serde_json::from_value(spot.raw.clone()).expect("spot config round-trip");
        assert_eq!(round_trip.pairs[0].base_quantity_scale, 0);
        assert_eq!(round_trip.pairs[0].quote_quantity_scale, 0);
    }

    #[test]
    fn unknown_timeframe_preserves_numeric_value() {
        let msg = GetCandlesResponse {
            timeframe: buffa::EnumValue::from(77),
            ..Default::default()
        };
        assert_eq!(
            candles_from_proto(&msg, 8).unwrap().timeframe,
            "UNKNOWN(77)"
        );
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
            open: vec![1],
            high: vec![1],
            low: vec![1],
            close: vec![1],
            volume: vec![1],
            ..Default::default()
        };
        let err = candles_columns_from_proto(&msg, 65535).expect_err("invalid scale");
        assert!(err.to_string().to_ascii_lowercase().contains("scale"));
    }

    #[test]
    fn candles_columns_rejects_short_parallel_arrays() {
        let msg = GetCandlesColumnsResponse {
            symbol_id: 1,
            timeframe: Timeframe::Min1.into(),
            ts_sec: vec![10, 20],
            open: vec![1, 2],
            high: vec![1],
            low: vec![1, 2],
            close: vec![1, 2],
            volume: vec![1, 2],
            ..Default::default()
        };
        let err = candles_columns_from_proto(&msg, 8)
            .expect_err("misaligned columnar response must fail closed");
        assert!(err.to_string().contains("response lengths"));
        assert!(err.to_string().contains("high=1"));
    }
}
