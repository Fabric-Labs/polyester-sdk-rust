//! Ledger / balance SDK models (Go `models` parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetBalance {
    pub asset_id: u32,
    pub trading: String,
    pub funding: String,
    pub reserved: String,
    pub available: String,
    /// Orders the atomic trading/reserved/available state.
    pub trading_revision: u64,
    /// Orders funding state independently of trading.
    pub funding_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancesList {
    pub balances: Vec<AssetBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceHistorySeries {
    pub asset_id: u32,
    /// Raw protobuf enum value; unknown/negative future values are preserved.
    pub account_code: i32,
    /// Unsigned scaled balance values from the ledger protocol.
    pub balance_q: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceHistory {
    pub range: String,
    pub bucket: String,
    pub start_ts_sec: i64,
    pub end_ts_sec: i64,
    pub points: u32,
    pub series: Vec<BalanceHistorySeries>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityHistorySeries {
    pub account_code: u32,
    pub account_name: String,
    pub asset_id: u32,
    pub asset_symbol: String,
    /// Public master/subaccount ID when `grouping` is `portfolio_account`.
    /// Empty for the Remaining aggregate, which omits `account_id`.
    pub portfolio_account_id: String,
    /// True when this series is the Remaining owned-subaccount aggregate.
    pub portfolio_remaining: bool,
    pub equity_q: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityHistory {
    pub range: String,
    pub bucket: String,
    pub start_ts_sec: i64,
    pub end_ts_sec: i64,
    pub quote_asset: String,
    pub points: u32,
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

/// One display side of a ledger transfer.
///
/// `kind` is a snake_case label (`funding_account`, `trading_account`,
/// `external_address`, `private_counterparty`, `fee_account`,
/// `system_account`). `chain_id` is the Zipper `ChainConfig.chain_id` for
/// external-address sides, not an EIP-155 or Polyester chain id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferSide {
    pub kind: String,
    pub account_id: String,
    pub address: String,
    pub chain_id: Option<u32>,
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
    pub source: Option<TransferSide>,
    pub destination: Option<TransferSide>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransfersList {
    pub transfers: Vec<LedgerTransfer>,
    pub next_cursor: Option<i64>,
}
