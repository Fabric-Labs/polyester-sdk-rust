//! Account statements (`ledger.read.v1.LedgerReadService/ExportStatement`).
//!
//! A statement is RFC 4180 CSV text in UTF-8 with CRLF line endings, kept as
//! sent. Read its columns by header name: later releases may append columns.

use crate::errors::Result;
use buffa_types::google::protobuf::Timestamp;

/// One page of a statement, from
/// [`crate::services::StatementsService::export_page`]. Only the first page
/// starts with the header row; later pages carry rows only, so the pages
/// concatenated in order are the whole statement.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementPage {
    /// This page's CSV text, as sent.
    pub csv: String,
    /// Cursor for the next page; empty when no more rows follow. Send it
    /// back with the same request fields.
    pub next_page_token: String,
    /// How far the statement is complete: it covers movements from
    /// `start_time` up to this time, exclusive. It is the earliest of
    /// `end_time`, the time of the first page's request and the time through
    /// which account history was complete then, never before `start_time`,
    /// and the same on every page of one statement. When it is before
    /// `end_time`, request the rest, from it to `end_time`, later. `None`
    /// when the server sent none.
    pub complete_through: Option<Timestamp>,
}

/// A whole statement, from [`crate::services::StatementsService::export`]:
/// every page's CSV text concatenated in order, the header row first. A
/// period without movements is the header row alone.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Statement {
    /// The statement's CSV text, as sent; save it as a `.csv` file as is.
    pub csv: String,
    /// How far the statement is complete, as on every
    /// [`StatementPage::complete_through`]: the statement covers movements
    /// from `start_time` up to this time, exclusive; request the rest, up to
    /// `end_time`, later. `None` when the server sent none.
    pub complete_through: Option<Timestamp>,
}

impl Statement {
    /// The statement's rows, read by header name; see
    /// [`crate::codecs::decode::parse_statement_csv`].
    pub fn rows(&self) -> Result<Vec<StatementRow>> {
        crate::codecs::decode::parse_statement_csv(&self.csv)
    }
}

/// One statement row: the effect of one ledger entry on one account bucket
/// in one asset. Amounts are exact decimal strings in whole units of the
/// asset, with up to 18 fractional digits; never parse them as floats. For
/// consecutive rows of the same bucket and asset, `balance` equals the
/// previous `balance` plus `amount` minus `fee`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementRow {
    /// Entry time (the `time` column), in UTC to the microsecond.
    pub time: Timestamp,
    /// Stable entry identifier, an unsigned 128-bit integer as a decimal
    /// string: the `transfer_id` of the entry's first leg on the account, as
    /// the transfer history returns it.
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
    /// transfer, as the transfer history returns it; empty for other entries
    /// and in a statement without the column.
    pub flow_id: String,
}

/// A whole-period statement file produced in the background, from
/// [`crate::services::StatementsService::create_export`],
/// [`crate::services::StatementsService::get_export`] or
/// [`crate::services::StatementsService::list_exports`].
///
/// A `ready` file is byte for byte the [`Statement`] of the same account,
/// asset and period up to `complete_through`. It is downloaded over REST
/// only, with an authenticated `GET /v1/statements/exports/{export_id}/file`
/// (`text/csv`, single-range `Range` requests with `If-Range`), for 7 days;
/// this SDK does not download it.
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
    /// Where the export is: `queued`, `processing`, `ready`, `failed` or
    /// `expired` (create a new export); `UNKNOWN(<n>)` for a status this SDK
    /// does not know.
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
    /// Why a `failed` export failed: `too_large` (more than 4,000,000
    /// movements; split the period) or `unavailable` (create a new export);
    /// `None` otherwise, `UNKNOWN(<n>)` for a reason this SDK does not know.
    pub failure_reason: Option<String>,
}

/// One page of an account's statement exports, newest first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementExportsList {
    pub exports: Vec<StatementExport>,
    /// Cursor for the next page; empty on the last page.
    pub next_page_token: String,
}
