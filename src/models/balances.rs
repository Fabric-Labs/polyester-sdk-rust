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
    /// Raw BTC-USDT closes scaled by 1e9, aligned to the implicit timestamps.
    pub btc_prices_q: Vec<i64>,
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
    /// The `ledger.v1.TransferCode` number. `TRADING_WITHDRAW_REQUEST_FEE`
    /// (1065) is not a fee: it is the debit of a withdrawal from the Trading
    /// account that was rejected or failed to execute, for the whole amount
    /// sent for it, its request fee included; `WITHDRAW_FAILED` (1075) is the
    /// same debit for a withdrawal from the Funding account. A rejected
    /// withdrawal refunds that amount less the request fee to the account's
    /// own wallet; one that failed to execute is not refunded.
    pub transfer_type: i32,
    pub account_code: i32,
    pub timestamp: i64,
    /// The transfer's lifecycle flow ID (the contract's `flow_id`), the ID
    /// the lifecycle reads take; empty when it has none.
    ///
    /// New transfers from the Trading to the Funding account carry it from
    /// their first realtime update. The `withdraw_failed` (1075) debit of a
    /// rejected or failed withdrawal carries its withdrawal's flow ID in
    /// `balances.list_transfers`, joined when the history is read, so a read made
    /// before the withdrawal's outcome was recorded can return it empty and a
    /// later read returns it; the realtime transfer stream sends that debit
    /// once, without it. Withdrawals that ended before this link existed keep
    /// it empty. Reload recent transfers with `balances.list_transfers` to see it.
    pub tx_id: String,
    pub is_debit: bool,
    pub source: Option<TransferSide>,
    pub destination: Option<TransferSide>,
    /// Transfer ID, an unsigned 128-bit integer as a decimal string. Both
    /// legs of a transfer share it; with `account_code` and `is_debit` it
    /// identifies this leg. Empty when the server did not send one.
    pub transfer_id: String,
    /// The account bucket's balance after this transfer, an exact decimal
    /// integer at the fixed 18-decimal ledger scale, like `amount`. `None`
    /// when the server sent none; a zero balance is `Some("0")`.
    pub balance_after: Option<String>,
    /// Decimal correlation ID that groups related transfer legs. `None` when
    /// the server has none.
    pub link_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransfersList {
    pub transfers: Vec<LedgerTransfer>,
    /// Opaque continuation token; empty means the terminal page.
    pub next_page_token: String,
}
