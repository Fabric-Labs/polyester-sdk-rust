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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatementPage {
    /// This page's CSV text, as sent.
    pub csv: String,
    /// Cursor for the next page; empty when no more rows follow. Send it
    /// back with the same request fields.
    pub next_page_token: String,
}

/// A whole statement, from [`crate::services::StatementsService::export`]:
/// every page's CSV text concatenated in order, the header row first. A
/// period without movements is the header row alone.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Statement {
    /// The statement's CSV text, as sent; save it as a `.csv` file as is.
    pub csv: String,
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
