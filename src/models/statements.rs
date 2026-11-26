//! Account statements (`ledger.read.v1.StatementExportService`).
//!
//! A statement file is RFC 4180 CSV text in UTF-8 with CRLF line endings:
//! an account statement, a ledger or balances, by
//! [`StatementExport::export_type`]. Read its columns by header name, with
//! [`crate::codecs::decode::parse_statement_csv`],
//! [`crate::codecs::decode::parse_ledger_csv`] or
//! [`crate::codecs::decode::parse_balances_csv`]: later releases may append
//! columns.

use buffa_types::google::protobuf::Timestamp;
use std::fmt;

/// The kind of file a statement export produces, which
/// [`crate::services::CreateStatementExportOptions::export_type`] requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementExportType {
    /// Account statement: one row per entry, account bucket and asset, with
    /// the entry's net amount, the fees paid and the balance after it. Read
    /// it with [`crate::codecs::decode::parse_statement_csv`].
    AccountStatement,
    /// Ledger: one row per ledger movement, each transfer leg on its own,
    /// with its transfer reason, side, counterparty and the balance after
    /// it. Read it with [`crate::codecs::decode::parse_ledger_csv`].
    Ledger,
    /// Balances: one row per account bucket and asset, with its balance at
    /// the start and at the end of the period and the period's credits and
    /// debits. Read it with [`crate::codecs::decode::parse_balances_csv`].
    /// Its period must start inside the account history, or the create is
    /// refused ([`crate::Error::is_statement_history_unavailable`]).
    Balances,
}

impl StatementExportType {
    /// `"account_statement"`, `"ledger"` or `"balances"`, as
    /// [`StatementExport::export_type`] reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccountStatement => "account_statement",
            Self::Ledger => "ledger",
            Self::Balances => "balances",
        }
    }
}

/// One account statement row ([`StatementExportType::AccountStatement`]): the
/// effect of one ledger entry on one account bucket in one asset. Amounts are exact decimal strings in whole units of the
/// asset, with up to 18 fractional digits; never parse them as floats. For
/// consecutive rows of the same bucket and asset, `balance` equals the
/// previous `balance` plus `amount` minus `fee`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementRow {
    /// Entry time (the `time` column), in UTC to the microsecond.
    pub time: Timestamp,
    /// Entry identifier, an unsigned 128-bit integer as a decimal string: the
    /// `transfer_id` of the entry's first leg on the account within the
    /// export's period, as the transfer history returns it. It is the same
    /// whichever asset the export keeps. A period boundary can fall between
    /// the legs of one entry: each export then holds its own part of it,
    /// under the `transfer_id` of that part's first leg, so do not join rows
    /// of different periods on `entry_id` alone.
    pub entry_id: String,
    /// The `type` column: `trade` for a trade's settlement and its fees,
    /// otherwise the entry's transfer reason, such as `deposit`, `withdraw`,
    /// `internal_transfer`, `funding_to_trading` or `system_adjustment`.
    pub kind: String,
    /// Account bucket: `funding` or `trading`.
    pub account: String,
    /// Asset symbol.
    pub asset: String,
    /// Signed balance change of the entry, excluding `fee`; fees the account
    /// receives are included here.
    pub amount: String,
    /// Fees the account paid in the asset for the entry; `0` when none.
    pub fee: String,
    /// Bucket balance in the asset after the entry.
    pub balance: String,
    /// Correlation ID shared by related transfer legs (a trade's match ID);
    /// empty when unavailable.
    pub link_id: String,
    /// Public lifecycle flow ID of a deposit, withdrawal or internal
    /// transfer linked to the entry before the export was created; empty for
    /// other entries and in a statement without the column. A flow linked
    /// later, which the transfer history then shows, is not in the file: a
    /// new export shows it.
    pub flow_id: String,
}

/// One ledger row ([`StatementExportType::Ledger`]): one ledger movement, one
/// transfer leg on the account, as the transfer history returns it. A
/// trade's settlement is several rows, such as its base and quote legs and
/// each fee. Amounts are exact decimal strings in whole units of the asset,
/// with up to 18 fractional digits; never parse them as floats. For
/// consecutive rows of the same bucket and asset, `balance` equals the
/// previous `balance` plus `amount`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LedgerRow {
    /// Transfer time (the `time` column), in UTC to the microsecond.
    pub time: Timestamp,
    /// Transfer identifier, an unsigned 128-bit integer as a decimal string,
    /// as the transfer history returns it. With `account` and `side` it
    /// identifies the row.
    pub transfer_id: String,
    /// Account bucket: `funding` or `trading`.
    pub account: String,
    /// Asset symbol.
    pub asset: String,
    /// The `type` column: the transfer reason, such as `trade_base`,
    /// `trade_quote`, `maker_fee`, `taker_fee`, `rebate`, `deposit`,
    /// `withdraw`, `internal_transfer` or `funding_to_trading`.
    pub kind: String,
    /// `debit` when the movement takes from the account bucket, `credit`
    /// when it adds to it.
    pub side: String,
    /// Signed balance change: negative for a debit, positive for a credit.
    pub amount: String,
    /// Kind of the transfer's other side: `funding_account`,
    /// `trading_account`, `external_address`, `private_counterparty`,
    /// `fee_account` or `system_account`; empty when unknown.
    pub counterparty: String,
    /// Bucket balance in the asset after the movement.
    pub balance: String,
    /// Correlation ID shared by related transfer legs (a trade's match ID);
    /// empty when unavailable.
    pub link_id: String,
    /// Public lifecycle flow ID of a transfer linked to a chain lifecycle
    /// flow before the export was created; empty otherwise, as for
    /// [`StatementRow::flow_id`].
    pub flow_id: String,
}

/// One balances row ([`StatementExportType::Balances`]): one account bucket
/// in one asset over the export's period. Amounts are exact decimal strings
/// in whole units of the asset, with up to 18 fractional digits; never parse
/// them as floats. Every row reconciles: `closing_balance` equals
/// `opening_balance` plus `credits` minus `debits`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BalancesRow {
    /// Account bucket: `funding` or `trading`.
    pub account: String,
    /// Asset symbol.
    pub asset: String,
    /// Bucket balance in the asset at the export's `start_time`.
    pub opening_balance: String,
    /// Total the period's movements added to the bucket, the sum of the
    /// ledger file's credit rows of that bucket and asset; `0` when none.
    pub credits: String,
    /// Total the period's movements took from the bucket, as a positive
    /// amount: the sum of the ledger file's debit rows of that bucket and
    /// asset; `0` when none.
    pub debits: String,
    /// Bucket balance in the asset at the export's `complete_through`.
    pub closing_balance: String,
    /// Number of the period's ledger movements on the bucket in the asset.
    pub movements: u64,
}

/// A whole-period account statement, ledger or balances file produced in
/// the background, from
/// [`crate::services::StatementsService::create_export`],
/// [`crate::services::StatementsService::get_export`] or
/// [`crate::services::StatementsService::list_exports`].
///
/// A `ready` file holds the header row, then every row of the period up to
/// `complete_through` (oldest first for an account statement or a ledger, by
/// bucket and asset for balances); read it with
/// [`crate::codecs::decode::parse_statement_csv`],
/// [`crate::codecs::decode::parse_ledger_csv`] or
/// [`crate::codecs::decode::parse_balances_csv`], by `export_type`. It can be
/// downloaded for
/// 7 days, from the short-lived URL
/// [`crate::services::StatementsService::get_export_download_url`] returns,
/// which needs no credentials, or with an authenticated REST
/// `GET /v1/statements/exports/{export_id}/file` (`text/csv`, with HTTP
/// `Range` requests). This SDK does not download it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementExport {
    /// Public ID of the export. Never empty.
    pub export_id: String,
    /// Public ID of the sub-account the statement covers; `None` for a main
    /// account.
    pub subaccount_id: Option<String>,
    /// Start of the period (UTC), inclusive.
    pub start_time: Option<Timestamp>,
    /// End of the requested period (UTC), exclusive.
    pub end_time: Option<Timestamp>,
    /// Numeric asset ID the statement is limited to; 0 for every asset.
    pub asset_id: u32,
    /// Idempotency key of the request that created the export.
    pub idempotency_key: String,
    /// The kind of file the export produces, as requested:
    /// `account_statement`, `ledger` or `balances`
    /// ([`StatementExportType::as_str`]);
    /// `UNKNOWN(<n>)` for a type this SDK does not know, which never fails
    /// the read. Exports created before types existed are account
    /// statements.
    pub export_type: String,
    /// Where the export is: `queued`, `processing`, `ready`, `failed`,
    /// `expired` (create a new export) or `canceled` (stopped with
    /// [`crate::services::StatementsService::cancel_export`]; it has no
    /// file); `UNKNOWN(<n>)` for a status this SDK does not know.
    pub status: String,
    /// End of what the file covers (UTC), exclusive: the earlier of
    /// `end_time` and the time the export was created.
    pub complete_through: Option<Timestamp>,
    /// Ledger movements written so far; the file's total when `ready`.
    pub movements: u64,
    /// Bytes written so far; the file's size when `ready`.
    pub bytes: u64,
    /// When the export was created.
    pub created_at: Option<Timestamp>,
    /// When the file became `ready`; `None` before.
    pub ready_at: Option<Timestamp>,
    /// When a `ready` file expires, 7 days after `ready_at`; `None` before.
    pub expires_at: Option<Timestamp>,
    /// Why a `failed` export failed: `too_large` (the period holds more than
    /// 4,000,000 ledger movements of the account, counting every asset's even
    /// when the export keeps one; split the period) or `unavailable` (create
    /// a new export), or `history_unavailable` (a balances export whose
    /// period starts before the account history, which moved later after
    /// the export was created: create a new export from
    /// `history_available_from` on);
    /// `None` otherwise, `UNKNOWN(<n>)` for a reason this SDK does not know.
    pub failure_reason: Option<String>,
    /// The earliest instant whose balances the account history holds; set
    /// only when `failure_reason` is `history_unavailable`.
    pub history_available_from: Option<Timestamp>,
}

/// One page of an account's statement exports, newest first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementExportsList {
    pub exports: Vec<StatementExport>,
    /// Cursor for the next page; empty on the last page.
    pub next_page_token: String,
    /// Exports `queued` or `processing` across the whole main account, its
    /// sub-accounts included, whichever account the list selects.
    pub in_progress_exports: u32,
    /// Most exports the main account may have `queued` or `processing` at a
    /// time. While `in_progress_exports` is at it, a create is refused with
    /// [`crate::errors::ledger_codes::STATEMENT_EXPORT_LIMIT_REACHED`].
    pub max_in_progress_exports: u32,
}

/// A short-lived URL of a `ready` export's file, from
/// [`crate::services::StatementsService::get_export_download_url`].
///
/// The URL carries its own authorization: anyone who holds it can download
/// the file until `expires_at`. [`Debug`] redacts `url`, so logging the
/// result cannot leak it; read the field explicitly to hand it to a
/// browser, and never log, store or share it.
#[derive(Clone, PartialEq)]
pub struct StatementExportDownloadUrl {
    /// URL of the file, valid until `expires_at`. Never empty.
    pub url: String,
    /// When the URL stops working: 60 seconds after it was signed for this
    /// call, or when the file expires if that is sooner. Request a new URL
    /// after it. Always set: a reply without it is an
    /// [`crate::Error::ResponseContract`].
    pub expires_at: Timestamp,
    /// Name the download saves the file as, by the export's type:
    /// `account-statement-<export_id>.csv`, `ledger-<export_id>.csv` or
    /// `balances-<export_id>.csv`. Never empty.
    pub filename: String,
}

impl fmt::Debug for StatementExportDownloadUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatementExportDownloadUrl")
            .field("url", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("filename", &self.filename)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Debug` never prints the download URL, which is a bearer secret.
    #[test]
    fn download_url_debug_redacts_the_url() {
        let url = StatementExportDownloadUrl {
            url: "https://files.example/statement?signature=fake-signature-marker".into(),
            expires_at: Timestamp {
                seconds: 1_763_337_660,
                ..Default::default()
            },
            filename: "account-statement-7.csv".into(),
        };
        for rendered in [format!("{url:?}"), format!("{url:#?}")] {
            assert!(!rendered.contains("fake-signature-marker"), "{rendered}");
            assert!(!rendered.contains("files.example"), "{rendered}");
            assert!(rendered.contains("[REDACTED]"), "{rendered}");
            assert!(rendered.contains("account-statement-7.csv"), "{rendered}");
        }
    }
}
