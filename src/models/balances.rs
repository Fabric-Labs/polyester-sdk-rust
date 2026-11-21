//! Ledger / balance SDK models (Go `models` parity).

/// One asset's balances, as [`crate::services::BalancesService::list`]
/// returns them.
///
/// Amounts are exact decimal integers at the fixed 18-decimal ledger scale.
/// The Trading values (`trading`, `reserved`, `available`) have no revision
/// of their own: they are at the position of the list that holds them
/// ([`BalancesList::trading_epoch`], [`BalancesList::trading_sequence`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetBalance {
    pub asset_id: u32,
    pub trading: String,
    pub funding: String,
    pub reserved: String,
    pub available: String,
    /// Orders the funding value: higher is newer; 0 when no funding state has
    /// been observed. Apply a FUNDING [`BalanceUpdate`] only when its
    /// revision is higher than the one held for the asset.
    pub funding_revision: u64,
}

/// The Trading numbering a balance position belongs to.
///
/// It changes when the numbering restarts, and a different epoch means
/// reload. It has no order: compare epochs only for equality, never as
/// "newer" or "older". [`Self::get`] returns the wire value, a plain
/// unsigned 64-bit number, for logging or storage. Zero is the epoch of an
/// account with no Trading balances yet, and the unset epoch of a FUNDING
/// [`BalanceUpdate`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TradingEpoch(u64);

impl TradingEpoch {
    /// The epoch with this wire value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The wire value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TradingEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The balances of an account (`GetBalances`) and the position of their
/// Trading values.
///
/// The Trading values of every asset are one atomic cut, all at the single
/// position (`trading_epoch`, `trading_sequence`), never older than a
/// position the balance stream already published. The Funding values carry
/// their own [`AssetBalance::funding_revision`] and are not part of that
/// position. See [`crate::services::BalancesService::subscribe`] for how to
/// keep the balances exact from this list and the balance stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BalancesList {
    pub balances: Vec<AssetBalance>,
    /// The Trading numbering the Trading values are in. Zero, with
    /// `trading_sequence` zero, when the account has no Trading balances yet.
    pub trading_epoch: TradingEpoch,
    /// The Trading sequence the Trading values are at, within
    /// `trading_epoch`: apply the stream's TRADING updates of this epoch from
    /// `trading_sequence + 1` on.
    pub trading_sequence: u64,
    /// Every Trading transfer of the account whose
    /// [`LedgerTransfer::ts_ns`] is at or below this value is applied in these
    /// balances: a receipt within `trading_epoch`. While one of the account's
    /// transfers could not be applied, it stays below that transfer until a
    /// later balance of the asset includes it. Nanoseconds since the Unix
    /// epoch (UTC); 0 when not provided.
    pub trading_applied_through_ts_ns: u64,
}

/// Which balances a [`BalanceUpdate`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BalanceUpdateAccount {
    /// One step of the account's Trading balances, at a position.
    Trading,
    /// One asset's Funding balance, ordered by its funding revision.
    Funding,
    /// A value this SDK does not know, kept as its number. Ignore the update.
    Unknown(i32),
}

/// One asset of a [`BalanceUpdate`]: the fields its kind carries, the other
/// kind's left `None`.
///
/// Amounts are exact decimal integers at the fixed 18-decimal ledger scale.
/// A TRADING update sets `trading`, `reserved` and `available`; a FUNDING
/// update sets `funding` and `funding_revision`. Keep the fields an update
/// leaves `None` from your earlier state: `None` is "not in this update",
/// never zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceUpdateAsset {
    pub asset_id: u32,
    pub trading: Option<String>,
    pub reserved: Option<String>,
    pub available: Option<String>,
    pub funding: Option<String>,
    /// The funding value's revision (FUNDING updates): higher is newer. 0 on
    /// a TRADING update.
    pub funding_revision: u64,
}

/// One publication of an account's balance stream
/// ([`crate::services::BalancesService::subscribe`]), decoded from
/// `ledger.read.v1.BalanceUpdate` as it arrives.
///
/// A TRADING update is one step of the account's Trading balances at the
/// position (`trading_epoch`, `trading_sequence`). Within an epoch the
/// sequence runs 1, 2, 3 with no gap; a new epoch starts again at 1, and its
/// updates up to its first `complete` one carry every asset the account holds
/// in Trading. A FUNDING update carries one asset's Funding balance, not
/// sequenced: its position fields are unset. Tell the two kinds apart by
/// `account`, never by a zero epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceUpdate {
    pub account: BalanceUpdateAccount,
    /// TRADING only: the Trading numbering this update is in, as
    /// [`BalancesList::trading_epoch`] reports it.
    pub trading_epoch: TradingEpoch,
    /// TRADING only: this update's place in its epoch, from 1, with no gap.
    pub trading_sequence: u64,
    /// TRADING only: true when the account's Trading values are a consistent
    /// snapshot after this update; false while the same step continues in the
    /// next update. Show Trading values only after a complete update.
    pub complete: bool,
    /// The assets this update changed, each at most once (at most 64).
    pub balances: Vec<BalanceUpdateAsset>,
    /// TRADING only: the Trading transfer legs this update applied, oldest
    /// first (at most 50). Each carries its asset, amount, transfer code,
    /// account code, timestamps, balance after, direction and transfer ID;
    /// `tx_id`, `link_id`, `source` and `destination` are left empty here and
    /// come with the transfer stream and `balances.list_transfers`. After an
    /// ownership move a transfer's legs can arrive again in the new epoch:
    /// (`transfer_id`, `is_debit`) identifies a leg.
    pub transfers: Vec<LedgerTransfer>,
    /// TRADING only: every Trading transfer of the account whose
    /// [`LedgerTransfer::ts_ns`] is at or below this value is applied in this
    /// update's balances, whether or not its leg was received; a receipt
    /// within `trading_epoch`. Nanoseconds since the Unix epoch (UTC).
    pub trading_applied_through_ts_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceHistorySeries {
    pub asset_id: u32,
    /// Raw protobuf enum value; unknown/negative future values are preserved.
    pub account_code: i32,
    /// The exact balance at each point, in whole units of the asset at the
    /// fixed 18-decimal ledger scale ([`crate::types::AssetAmount`] with
    /// scale 18, domain `LedgerE18` and this series' asset ID), aligned 1:1
    /// with the implicit timestamps: one value per point, `points` in all.
    /// Decoded exactly from the contract's `polyester.type.v1.U128`:
    /// [`crate::types::AssetAmount::as_scaled`] is the balance times 10^18.
    /// Never convert it through a float.
    pub balance: Vec<crate::types::AssetAmount>,
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
    /// When the transfer happened, in microseconds since the Unix epoch.
    pub timestamp: i64,
    /// The exact transfer time, in nanoseconds since the Unix epoch; 0 when
    /// the server did not send it. Compare it with a
    /// `trading_applied_through_ts_ns` of the same Trading epoch
    /// ([`BalancesList`], [`BalanceUpdate`]) to tell whether the transfer is
    /// already in those Trading balances.
    pub ts_ns: u64,
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
