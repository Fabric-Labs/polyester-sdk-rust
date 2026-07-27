//! Ledger balance / hold / transfer decoders.

use crate::codecs::scalars::{format_uint64_id, u128_to_str};
use crate::models::{
    AssetBalance, BalanceHistory, BalanceHistorySeries, BalancesList, Hold, HoldsList,
    LedgerTransfer, TransfersList,
};
use crate::proto::ledger::read::v1::{
    AssetBalance as ProtoAssetBalance, BalanceRange, GetBalanceHistoryResponse,
    GetBalancesResponse, HoldRow, ListHoldsResponse, ListTransfersResponse, TransferRow,
};
use crate::proto::polyester::r#type::v1::U128;

fn u128_field(msg: Option<&U128>) -> String {
    match msg {
        Some(u) => u128_to_str(u.hi, u.lo),
        None => "0".to_owned(),
    }
}

fn balance_range_label(range: buffa::EnumValue<BalanceRange>) -> String {
    match range.as_known() {
        Some(BalanceRange::Day1) => "1d".to_owned(),
        Some(BalanceRange::Day7) => "7d".to_owned(),
        Some(BalanceRange::Day30) => "30d".to_owned(),
        Some(BalanceRange::Day90) => "90d".to_owned(),
        Some(BalanceRange::Day180) => "180d".to_owned(),
        Some(BalanceRange::Day365) => "365d".to_owned(),
        Some(_) => String::new(),
        None => format!("UNKNOWN({})", range.to_i32()),
    }
}

pub fn asset_balance_from_proto(msg: &ProtoAssetBalance) -> AssetBalance {
    AssetBalance {
        asset_id: msg.asset_id,
        trading: u128_field(msg.trading.as_option()),
        funding: u128_field(msg.funding.as_option()),
        reserved: u128_field(msg.reserved.as_option()),
        available: u128_field(msg.available.as_option()),
        trading_revision: msg.trading_revision,
        funding_revision: msg.funding_revision,
    }
}

pub fn balances_list_from_proto(msg: &GetBalancesResponse) -> BalancesList {
    BalancesList {
        balances: msg.balances.iter().map(asset_balance_from_proto).collect(),
    }
}

pub fn balance_history_from_proto(msg: &GetBalanceHistoryResponse) -> BalanceHistory {
    BalanceHistory {
        range: balance_range_label(msg.range),
        bucket: msg.bucket.clone(),
        start_ts_sec: msg.start_ts_sec as i64,
        end_ts_sec: msg.end_ts_sec as i64,
        points: msg.points,
        series: msg
            .series
            .iter()
            .map(|s| BalanceHistorySeries {
                asset_id: s.asset_id,
                account_code: s.account_code.to_i32(),
                balance_q: s.balance_q.clone(),
            })
            .collect(),
    }
}

pub fn equity_history_from_proto(
    msg: &crate::proto::ledger::read::v1::GetEquityHistorySeriesResponse,
) -> crate::models::EquityHistory {
    use crate::models::{EquityHistory, EquityHistorySeries};
    use crate::proto::ledger::read::v1::__buffa::oneof::equity_series::Grouping;
    EquityHistory {
        range: balance_range_label(msg.range),
        bucket: msg.bucket.clone(),
        start_ts_sec: msg.start_ts_sec as i64,
        end_ts_sec: msg.end_ts_sec as i64,
        quote_asset: msg.quote_asset.clone(),
        points: msg.points,
        series: msg
            .series
            .iter()
            .map(|s| {
                let mut row = EquityHistorySeries {
                    account_code: 0,
                    account_name: String::new(),
                    asset_id: 0,
                    asset_symbol: String::new(),
                    equity_q: s.equity_q.clone(),
                };
                match s.grouping.as_ref() {
                    Some(Grouping::Account(a)) => {
                        row.account_code = a.account_code;
                        row.account_name = a.name.clone();
                    }
                    Some(Grouping::Asset(a)) => {
                        row.asset_id = a.id;
                        row.asset_symbol = a.symbol.clone();
                    }
                    None => {}
                }
                row
            })
            .collect(),
    }
}

pub fn hold_from_proto(msg: &HoldRow) -> Hold {
    Hold {
        hold_id: format_uint64_id(msg.hold_id),
        asset_id: msg.asset_id,
        amount_reserved: u128_field(msg.amount_reserved_e18.as_option()),
        expires_at_ns: if msg.expires_at_ns == 0 {
            String::new()
        } else {
            msg.expires_at_ns.to_string()
        },
    }
}

pub fn holds_list_from_proto(msg: &ListHoldsResponse) -> HoldsList {
    HoldsList {
        holds: msg.holds.iter().map(hold_from_proto).collect(),
    }
}

pub fn transfer_row_from_proto(msg: &TransferRow) -> LedgerTransfer {
    LedgerTransfer {
        asset_id: msg.asset_id,
        amount: u128_field(msg.amount_e18.as_option()),
        transfer_type: msg.transfer_code.to_i32(),
        account_code: msg.account_code.to_i32(),
        timestamp: msg.ts_us as i64,
        tx_id: msg.flow_id.clone(),
        is_debit: msg.is_debit,
    }
}

pub fn transfers_list_from_proto(msg: &ListTransfersResponse) -> TransfersList {
    let next_cursor = if msg.next_page_token.is_empty() {
        None
    } else {
        msg.next_page_token.parse::<i64>().ok().filter(|v| *v != 0)
    };
    TransfersList {
        transfers: msg.transfers.iter().map(transfer_row_from_proto).collect(),
        next_cursor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ledger::read::v1::__buffa::oneof::equity_series::Grouping;
    use crate::proto::ledger::read::v1::{
        AccountGrouping, BalanceSeries, GetBalanceHistoryResponse, GetEquityHistorySeriesResponse,
        ListHoldsResponse, ListTransfersResponse,
    };

    #[test]
    fn balances_list_maps_u128() {
        let msg = GetBalancesResponse {
            balances: vec![ProtoAssetBalance {
                asset_id: 7,
                trading: U128 {
                    hi: 0,
                    lo: 500,
                    ..Default::default()
                }
                .into(),
                trading_revision: u64::MAX - 2,
                funding_revision: u64::MAX - 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let list = balances_list_from_proto(&msg);
        assert_eq!(list.balances.len(), 1);
        assert_eq!(list.balances[0].asset_id, 7);
        assert_eq!(list.balances[0].trading, "500");
        assert_eq!(list.balances[0].trading_revision, u64::MAX - 2);
        assert_eq!(list.balances[0].funding_revision, u64::MAX - 1);

        // M5: 1e18 ledger integer must stay as the scaled string (not re-scaled to "1").
        let one_e18 = GetBalancesResponse {
            balances: vec![ProtoAssetBalance {
                asset_id: 1,
                trading: U128 {
                    hi: 0,
                    lo: 1_000_000_000_000_000_000,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let list = balances_list_from_proto(&one_e18);
        assert_eq!(list.balances[0].trading, "1000000000000000000");
    }

    #[test]
    fn balance_history_maps_range_and_series() {
        let msg = GetBalanceHistoryResponse {
            range: BalanceRange::Day7.into(),
            bucket: "1h".into(),
            start_ts_sec: 100,
            end_ts_sec: 200,
            points: 2,
            series: vec![BalanceSeries {
                asset_id: 1,
                balance_q: vec![100, 200],
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = balance_history_from_proto(&msg);
        assert_eq!(result.range, "7d");
        assert_eq!(result.bucket, "1h");
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].balance_q, vec![100, 200]);
    }

    #[test]
    fn balance_history_preserves_full_u64_range() {
        use crate::proto::ledger::read::v1::BalanceSeries;

        let msg = GetBalanceHistoryResponse {
            series: vec![BalanceSeries {
                asset_id: 1,
                account_code: buffa::EnumValue::from(-7),
                balance_q: vec![i64::MAX as u64 + 1, u64::MAX],
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = balance_history_from_proto(&msg);
        assert_eq!(
            result.series[0].balance_q,
            vec![i64::MAX as u64 + 1, u64::MAX]
        );
        assert_eq!(result.series[0].account_code, -7);
    }

    #[test]
    fn equity_history_maps_account_grouping() {
        let msg = GetEquityHistorySeriesResponse {
            range: BalanceRange::Day30.into(),
            bucket: "1d".into(),
            start_ts_sec: 1,
            end_ts_sec: 2,
            quote_asset: "USD".into(),
            points: 1,
            series: vec![crate::proto::ledger::read::v1::EquitySeries {
                equity_q: vec![999],
                grouping: Some(Grouping::Account(Box::new(AccountGrouping {
                    account_code: 5,
                    name: "Trading".into(),
                    ..Default::default()
                }))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = equity_history_from_proto(&msg);
        assert_eq!(result.range, "30d");
        assert_eq!(result.quote_asset, "USD");
        assert_eq!(result.series[0].account_code, 5);
        assert_eq!(result.series[0].account_name, "Trading");
        assert_eq!(result.series[0].equity_q, vec![999]);
    }

    #[test]
    fn history_point_counts_preserve_full_u32_range() {
        let balance = balance_history_from_proto(&GetBalanceHistoryResponse {
            points: u32::MAX,
            ..Default::default()
        });
        let equity = equity_history_from_proto(&GetEquityHistorySeriesResponse {
            points: u32::MAX,
            ..Default::default()
        });
        assert_eq!(balance.points, u32::MAX);
        assert_eq!(equity.points, u32::MAX);
    }

    #[test]
    fn hold_formats_id_and_amount() {
        let msg = HoldRow {
            hold_id: 42,
            asset_id: 1,
            amount_reserved_e18: U128 {
                hi: 0,
                lo: 99,
                ..Default::default()
            }
            .into(),
            expires_at_ns: 123,
            ..Default::default()
        };
        let hold = hold_from_proto(&msg);
        assert_eq!(hold.hold_id, format_uint64_id(42));
        assert_eq!(hold.amount_reserved, "99");
        assert_eq!(hold.expires_at_ns, "123");
    }

    #[test]
    fn holds_list_maps_rows() {
        let msg = ListHoldsResponse {
            holds: vec![HoldRow {
                hold_id: 42,
                asset_id: 1,
                amount_reserved_e18: U128 {
                    hi: 0,
                    lo: 500,
                    ..Default::default()
                }
                .into(),
                expires_at_ns: 1_700_000_000_000,
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = holds_list_from_proto(&msg);
        assert_eq!(result.holds.len(), 1);
        assert_eq!(result.holds[0].hold_id, format_uint64_id(42));
        assert_eq!(result.holds[0].amount_reserved, "500");
    }

    #[test]
    fn transfer_row_and_list_cursor() {
        let row = TransferRow {
            asset_id: 2,
            amount_e18: U128 {
                hi: 0,
                lo: 1000,
                ..Default::default()
            }
            .into(),
            transfer_code: buffa::EnumValue::from(5),
            account_code: buffa::EnumValue::from(1),
            ts_us: 999,
            is_debit: true,
            flow_id: "flow-abc".into(),
            ..Default::default()
        };
        let transfer = transfer_row_from_proto(&row);
        assert_eq!(transfer.amount, "1000");
        assert_eq!(transfer.transfer_type, 5);
        assert_eq!(transfer.tx_id, "flow-abc");
        assert!(transfer.is_debit);

        let list = transfers_list_from_proto(&ListTransfersResponse {
            transfers: vec![TransferRow {
                asset_id: 1,
                ..Default::default()
            }],
            next_page_token: "12345".into(),
            ..Default::default()
        });
        assert_eq!(list.transfers.len(), 1);
        assert_eq!(list.next_cursor, Some(12345));
    }
}
