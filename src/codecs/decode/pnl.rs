//! Profit and loss decoders (`pnl.v1`).

use crate::models::{
    DailyProfitAndLoss, DailyProfitAndLossPage, HoldingProfitAndLoss, MarketProfitAndLoss,
    ProductProfitAndLoss, ProfitAndLoss, TradingStatistics,
};
use crate::proto::pnl::v1::{
    DailyProfitAndLoss as ProtoDailyProfitAndLoss, GetProfitAndLossResponse,
    GetTradingStatisticsResponse, HoldingProfitAndLoss as ProtoHoldingProfitAndLoss,
    ListDailyProfitAndLossResponse, MarketProfitAndLoss as ProtoMarketProfitAndLoss,
    ProductProfitAndLoss as ProtoProductProfitAndLoss, ProfitAndLossNotice, ProfitAndLossProduct,
};

fn product_label(product: buffa::EnumValue<ProfitAndLossProduct>) -> String {
    match product.as_known() {
        Some(ProfitAndLossProduct::PROFIT_AND_LOSS_PRODUCT_SPOT) => "spot".to_owned(),
        Some(ProfitAndLossProduct::PROFIT_AND_LOSS_PRODUCT_UNSPECIFIED) => String::new(),
        None => format!("unknown({})", product.to_i32()),
    }
}

fn notice_label(notice: &buffa::EnumValue<ProfitAndLossNotice>) -> String {
    use ProfitAndLossNotice as N;
    let label = match notice.as_known() {
        Some(N::PROFIT_AND_LOSS_NOTICE_PRICE_UNAVAILABLE) => "price_unavailable",
        Some(N::PROFIT_AND_LOSS_NOTICE_MARK_UNAVAILABLE) => "mark_unavailable",
        Some(N::PROFIT_AND_LOSS_NOTICE_UNCLASSIFIED_TRANSFER) => "unclassified_transfer",
        Some(N::PROFIT_AND_LOSS_NOTICE_QUANTITY_SHORTFALL) => "quantity_shortfall",
        Some(N::PROFIT_AND_LOSS_NOTICE_EQUITY_UNAVAILABLE) => "equity_unavailable",
        Some(N::PROFIT_AND_LOSS_NOTICE_FLOW_UNVALUED) => "flow_unvalued",
        Some(N::PROFIT_AND_LOSS_NOTICE_SHARPE_INSUFFICIENT_DAYS) => "sharpe_insufficient_days",
        Some(N::PROFIT_AND_LOSS_NOTICE_UNSPECIFIED) | None => {
            return format!("unknown({})", notice.to_i32());
        }
    };
    label.to_owned()
}

fn notices(values: &[buffa::EnumValue<ProfitAndLossNotice>]) -> Vec<String> {
    values.iter().map(notice_label).collect()
}

fn product_from_proto(msg: &ProtoProductProfitAndLoss) -> ProductProfitAndLoss {
    ProductProfitAndLoss {
        product: product_label(msg.product),
        realized_pnl_usd: msg.realized_pnl_usd.clone(),
        fees_usd: msg.fees_usd.clone(),
        volume_usd: msg.volume_usd.clone(),
    }
}

fn market_from_proto(msg: &ProtoMarketProfitAndLoss) -> MarketProfitAndLoss {
    MarketProfitAndLoss {
        symbol_id: msg.symbol_id,
        product: product_label(msg.product),
        realized_pnl_usd: msg.realized_pnl_usd.clone(),
        fees_usd: msg.fees_usd.clone(),
        volume_usd: msg.volume_usd.clone(),
        trade_count: msg.trade_count,
    }
}

fn holding_from_proto(msg: &ProtoHoldingProfitAndLoss) -> HoldingProfitAndLoss {
    HoldingProfitAndLoss {
        asset_id: msg.asset_id,
        position_quantity: msg.position_quantity.clone(),
        untracked_quantity: msg.untracked_quantity.clone(),
        cost_usd: msg.cost_usd.clone(),
        entry_price_usd: msg.entry_price_usd.clone(),
        mark_price_usd: msg.mark_price_usd.clone(),
        unrealized_pnl_usd: msg.unrealized_pnl_usd.clone(),
    }
}

fn day_from_proto(msg: &ProtoDailyProfitAndLoss) -> DailyProfitAndLoss {
    DailyProfitAndLoss {
        day: msg.day.as_option().cloned(),
        pnl_usd: msg.pnl_usd.clone(),
        realized_pnl_usd: msg.realized_pnl_usd.clone(),
        unrealized_pnl_at_close_usd: msg.unrealized_pnl_at_close_usd.clone(),
        fees_usd: msg.fees_usd.clone(),
        volume_usd: msg.volume_usd.clone(),
        trade_count: msg.trade_count,
        notices: notices(&msg.notices),
        start_equity_usd: msg.start_equity_usd.clone(),
        end_equity_usd: msg.end_equity_usd.clone(),
        net_flow_usd: msg.net_flow_usd.clone(),
    }
}

/// Decode a `GetProfitAndLoss` answer. An optional figure the server omits
/// stays `None`, never `"0"`.
pub fn profit_and_loss_from_proto(msg: &GetProfitAndLossResponse) -> ProfitAndLoss {
    ProfitAndLoss {
        window_start: msg.window_start.as_option().cloned(),
        as_of: msg.as_of.as_option().cloned(),
        mark_time: msg.mark_time.as_option().cloned(),
        pnl_usd: msg.pnl_usd.clone(),
        realized_pnl_usd: msg.realized_pnl_usd.clone(),
        unrealized_pnl_change_usd: msg.unrealized_pnl_change_usd.clone(),
        unrealized_pnl_usd: msg.unrealized_pnl_usd.clone(),
        fees_usd: msg.fees_usd.clone(),
        volume_usd: msg.volume_usd.clone(),
        products: msg.products.iter().map(product_from_proto).collect(),
        markets: msg.markets.iter().map(market_from_proto).collect(),
        holdings: msg.holdings.iter().map(holding_from_proto).collect(),
        notices: notices(&msg.notices),
    }
}

/// Decode a `ListDailyProfitAndLoss` page.
pub fn daily_profit_and_loss_page_from_proto(
    msg: &ListDailyProfitAndLossResponse,
) -> DailyProfitAndLossPage {
    DailyProfitAndLossPage {
        days: msg.days.iter().map(day_from_proto).collect(),
        next_page_token: msg.next_page_token.clone(),
        as_of: msg.as_of.as_option().cloned(),
    }
}

/// Decode a `GetTradingStatistics` answer.
pub fn trading_statistics_from_proto(msg: &GetTradingStatisticsResponse) -> TradingStatistics {
    TradingStatistics {
        window_start: msg.window_start.as_option().cloned(),
        as_of: msg.as_of.as_option().cloned(),
        trade_count: msg.trade_count,
        volume_usd: msg.volume_usd.clone(),
        fees_usd: msg.fees_usd.clone(),
        realized_pnl_usd: msg.realized_pnl_usd.clone(),
        notices: notices(&msg.notices),
        return_pct: msg.return_pct.clone(),
        max_drawdown_pct: msg.max_drawdown_pct.clone(),
        sharpe: msg.sharpe.clone(),
        return_days: msg.return_days,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa_types::google::protobuf::Timestamp;

    fn ts(seconds: i64) -> buffa::MessageField<Timestamp> {
        Timestamp {
            seconds,
            ..Default::default()
        }
        .into()
    }

    #[test]
    fn profit_and_loss_keeps_exact_strings_and_absent_figures() {
        let msg = GetProfitAndLossResponse {
            window_start: ts(1_795_046_400),
            as_of: ts(1_795_111_380),
            mark_time: ts(1_795_111_320),
            pnl_usd: Some("-12.000000000000000001".into()),
            realized_pnl_usd: "3.5".into(),
            unrealized_pnl_change_usd: None,
            unrealized_pnl_usd: Some("0".into()),
            fees_usd: "-0.25".into(),
            volume_usd: "1000".into(),
            products: vec![ProtoProductProfitAndLoss {
                product: ProfitAndLossProduct::PROFIT_AND_LOSS_PRODUCT_SPOT.into(),
                realized_pnl_usd: "3.5".into(),
                fees_usd: "-0.25".into(),
                volume_usd: "1000".into(),
                ..Default::default()
            }],
            markets: vec![ProtoMarketProfitAndLoss {
                symbol_id: 1,
                product: buffa::EnumValue::from(7),
                realized_pnl_usd: "3.5".into(),
                fees_usd: "0.1".into(),
                volume_usd: "1000".into(),
                trade_count: u64::MAX,
                ..Default::default()
            }],
            holdings: vec![ProtoHoldingProfitAndLoss {
                asset_id: 2,
                position_quantity: "1.5".into(),
                untracked_quantity: "0.5".into(),
                cost_usd: "90000".into(),
                entry_price_usd: Some("90000".into()),
                mark_price_usd: None,
                unrealized_pnl_usd: None,
                ..Default::default()
            }],
            notices: vec![
                ProfitAndLossNotice::PROFIT_AND_LOSS_NOTICE_MARK_UNAVAILABLE.into(),
                buffa::EnumValue::from(42),
            ],
            ..Default::default()
        };
        let pnl = profit_and_loss_from_proto(&msg);
        assert_eq!(pnl.window_start.unwrap().seconds, 1_795_046_400);
        assert_eq!(pnl.as_of.unwrap().seconds, 1_795_111_380);
        assert_eq!(pnl.mark_time.unwrap().seconds, 1_795_111_320);
        assert_eq!(pnl.pnl_usd.as_deref(), Some("-12.000000000000000001"));
        assert_eq!(pnl.unrealized_pnl_change_usd, None);
        // Zero is a figure, distinct from an absent one.
        assert_eq!(pnl.unrealized_pnl_usd.as_deref(), Some("0"));
        assert_eq!(pnl.fees_usd, "-0.25");
        assert_eq!(pnl.products[0].product, "spot");
        assert_eq!(pnl.markets[0].product, "unknown(7)");
        assert_eq!(pnl.markets[0].trade_count, u64::MAX);
        let holding = &pnl.holdings[0];
        assert_eq!(
            (
                holding.position_quantity.as_str(),
                holding.untracked_quantity.as_str()
            ),
            ("1.5", "0.5")
        );
        assert_eq!(holding.entry_price_usd.as_deref(), Some("90000"));
        assert_eq!(
            (&holding.mark_price_usd, &holding.unrealized_pnl_usd),
            (&None, &None)
        );
        assert_eq!(pnl.notices, vec!["mark_unavailable", "unknown(42)"]);

        let empty = profit_and_loss_from_proto(&GetProfitAndLossResponse::default());
        assert_eq!(empty.window_start, None);
        assert_eq!(empty.pnl_usd, None);
        assert!(empty.products.is_empty() && empty.notices.is_empty());
    }

    #[test]
    fn daily_page_keeps_every_day_and_its_equity() {
        let msg = ListDailyProfitAndLossResponse {
            days: vec![
                ProtoDailyProfitAndLoss {
                    day: ts(1_795_046_400),
                    pnl_usd: Some("1".into()),
                    realized_pnl_usd: "1".into(),
                    unrealized_pnl_at_close_usd: Some("0".into()),
                    fees_usd: "0".into(),
                    volume_usd: "10".into(),
                    trade_count: 2,
                    start_equity_usd: Some("100".into()),
                    end_equity_usd: Some("101".into()),
                    net_flow_usd: Some("-5".into()),
                    ..Default::default()
                },
                // Today, before it closes, and with no activity.
                ProtoDailyProfitAndLoss {
                    day: ts(1_795_132_800),
                    realized_pnl_usd: "0".into(),
                    fees_usd: "0".into(),
                    volume_usd: "0".into(),
                    start_equity_usd: Some("101".into()),
                    notices: vec![
                        ProfitAndLossNotice::PROFIT_AND_LOSS_NOTICE_EQUITY_UNAVAILABLE.into(),
                    ],
                    ..Default::default()
                },
            ],
            next_page_token: "next".into(),
            as_of: ts(1_795_140_000),
            ..Default::default()
        };
        let page = daily_profit_and_loss_page_from_proto(&msg);
        assert_eq!(page.days.len(), 2);
        assert_eq!(page.next_page_token, "next");
        assert_eq!(page.as_of.unwrap().seconds, 1_795_140_000);
        let first = &page.days[0];
        assert_eq!(first.day.as_ref().unwrap().seconds, 1_795_046_400);
        assert_eq!(first.start_equity_usd.as_deref(), Some("100"));
        assert_eq!(first.end_equity_usd.as_deref(), Some("101"));
        assert_eq!(first.net_flow_usd.as_deref(), Some("-5"));
        assert_eq!(first.trade_count, 2);
        let today = &page.days[1];
        assert_eq!(today.end_equity_usd, None);
        assert_eq!(today.pnl_usd, None);
        assert_eq!(today.unrealized_pnl_at_close_usd, None);
        assert_eq!(today.notices, vec!["equity_unavailable"]);
    }

    #[test]
    fn trading_statistics_keep_return_figures() {
        let stats = trading_statistics_from_proto(&GetTradingStatisticsResponse {
            window_start: ts(1),
            trade_count: 12,
            volume_usd: "1500.5".into(),
            fees_usd: "1.5".into(),
            realized_pnl_usd: "-3".into(),
            return_pct: Some("9.95".into()),
            max_drawdown_pct: Some("0".into()),
            sharpe: None,
            return_days: 2,
            notices: vec![
                ProfitAndLossNotice::PROFIT_AND_LOSS_NOTICE_SHARPE_INSUFFICIENT_DAYS.into(),
            ],
            ..Default::default()
        });
        assert_eq!(stats.trade_count, 12);
        assert_eq!(stats.realized_pnl_usd, "-3");
        assert_eq!(stats.return_pct.as_deref(), Some("9.95"));
        assert_eq!(stats.max_drawdown_pct.as_deref(), Some("0"));
        assert_eq!(stats.sharpe, None);
        assert_eq!(stats.return_days, 2);
        assert_eq!(stats.notices, vec!["sharpe_insufficient_days"]);
    }
}
