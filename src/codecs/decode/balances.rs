//! Ledger balance / hold / transfer decoders.

use crate::codecs::scalars::{format_uint64_id, u128_to_str};
use crate::models::{
    AssetBalance, BalanceHistory, BalanceHistorySeries, BalancesList, Hold, HoldsList,
    LedgerTransfer, TransfersList,
};
use crate::proto::ledger::read::v1::{
    AssetBalance as ProtoAssetBalance, BalanceRange, GetBalanceHistoryResponse, GetBalancesResponse,
    HoldRow, ListHoldsResponse, ListTransfersResponse, TransferRow,
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
        _ => String::new(),
    }
}

pub fn asset_balance_from_proto(msg: &ProtoAssetBalance) -> AssetBalance {
    AssetBalance {
        asset_id: msg.asset_id,
        trading: u128_field(msg.trading.as_option()),
        funding: u128_field(msg.funding.as_option()),
        reserved: u128_field(msg.reserved.as_option()),
        available: u128_field(msg.available.as_option()),
        trading_version: msg.trading_version,
        funding_version: msg.funding_version,
        reserved_version: msg.reserved_version,
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
        points: msg.points as i32,
        series: msg
            .series
            .iter()
            .map(|s| BalanceHistorySeries {
                asset_id: s.asset_id,
                account_code: s.account_code.to_i32() as u32,
                balance_q: s.balance_q.iter().map(|v| *v as i64).collect(),
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
    use crate::proto::ledger::read::v1::GetBalancesResponse;

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
                ..Default::default()
            }],
            ..Default::default()
        };
        let list = balances_list_from_proto(&msg);
        assert_eq!(list.balances.len(), 1);
        assert_eq!(list.balances[0].asset_id, 7);
        assert_eq!(list.balances[0].trading, "500");
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
}
