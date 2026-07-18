//! Ledger / balance SDK models (Go `models` parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetBalance {
    pub asset_id: u32,
    pub trading: String,
    pub funding: String,
    pub reserved: String,
    pub available: String,
    pub trading_updated_at_ns: u64,
    pub funding_updated_at_ns: u64,
    pub reserved_updated_at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancesList {
    pub balances: Vec<AssetBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceHistorySeries {
    pub asset_id: u32,
    pub account_code: u32,
    pub balance_q: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceHistory {
    pub range: String,
    pub bucket: String,
    pub start_ts_sec: i64,
    pub end_ts_sec: i64,
    pub points: i32,
    pub series: Vec<BalanceHistorySeries>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityHistorySeries {
    pub account_code: u32,
    pub account_name: String,
    pub asset_id: u32,
    pub asset_symbol: String,
    pub equity_q: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityHistory {
    pub range: String,
    pub bucket: String,
    pub start_ts_sec: i64,
    pub end_ts_sec: i64,
    pub quote_asset: String,
    pub points: i32,
    pub series: Vec<EquityHistorySeries>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    pub hold_id: String,
    pub asset_id: u32,
    pub amount_reserved: String,
    pub expires_at_ns: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldsList {
    pub holds: Vec<Hold>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerTransfer {
    pub asset_id: u32,
    pub amount: String,
    pub transfer_type: i32,
    pub account_code: i32,
    pub timestamp: i64,
    pub tx_id: String,
    pub is_debit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransfersList {
    pub transfers: Vec<LedgerTransfer>,
    pub next_cursor: Option<i64>,
}
