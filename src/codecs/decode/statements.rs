//! Statement export decoders and the account statement and ledger CSV
//! readers.

use crate::codecs::decode::common::require_id;
use crate::codecs::decode::dust::prefixed_label;
use crate::codecs::scalars::format_uint64_id;
use crate::errors::{Error, Result};
use crate::models::{
    BalancesRow, LedgerRow, StatementExport, StatementExportDownloadUrl, StatementExportsList,
    StatementRow,
};
use crate::proto::ledger::read::v1::{
    CancelStatementExportResponse, CreateStatementExportResponse,
    GetStatementExportDownloadUrlResponse, GetStatementExportResponse,
    ListStatementExportsResponse, StatementExport as ProtoStatementExport,
    StatementExportFailureReason, StatementExportStatus, StatementExportType,
};
use buffa_types::google::protobuf::Timestamp;
use serde::Deserialize;

/// Columns every statement row carries.
const REQUIRED_COLUMNS: [&str; 9] = [
    "time", "entry_id", "type", "account", "asset", "amount", "fee", "balance", "link_id",
];

/// Columns every ledger row carries.
const LEDGER_COLUMNS: [&str; 11] = [
    "time",
    "transfer_id",
    "account",
    "asset",
    "type",
    "side",
    "amount",
    "counterparty",
    "balance",
    "link_id",
    "flow_id",
];

/// Columns every balances row carries.
const BALANCES_COLUMNS: [&str; 7] = [
    "account",
    "asset",
    "opening_balance",
    "credits",
    "debits",
    "closing_balance",
    "movements",
];

/// The file a reader parses, as its errors name it.
const STATEMENT: &str = "statement";
const LEDGER: &str = "ledger";
const BALANCES: &str = "balances";

fn invalid(file: &str, message: impl std::fmt::Display) -> Error {
    Error::response_contract("statement file", format!("{file} csv: {message}"))
}

/// Split RFC 4180 text into records: fields separated by commas, records by
/// CRLF (a bare LF is accepted too), and quoted fields that may hold commas,
/// line breaks and doubled quotes. Blank lines are skipped. A closing quote
/// must end its field: anything but a comma or a line break after it, or a
/// quoted field still open at the end, is refused.
fn csv_records(file: &str, text: &str) -> Result<Vec<Vec<String>>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    // A quoted field just closed: only a delimiter may follow.
    let mut closed = false;
    let mut chars = text.chars().peekable();
    let mut end_record = |record: &mut Vec<String>, field: &mut String| {
        record.push(std::mem::take(field));
        let done = std::mem::take(record);
        if !(done.len() == 1 && done[0].is_empty()) {
            records.push(done);
        }
    };
    while let Some(c) = chars.next() {
        if quoted {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                    closed = true;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        if closed {
            closed = false;
            let delimiter = match c {
                ',' | '\n' => true,
                '\r' => chars.peek() == Some(&'\n'),
                _ => false,
            };
            if !delimiter {
                return Err(invalid(
                    file,
                    format!("{c:?} after the closing quote of a field"),
                ));
            }
        }
        match c {
            '"' if field.is_empty() => quoted = true,
            ',' => record.push(std::mem::take(&mut field)),
            '\r' if chars.peek() == Some(&'\n') => {
                chars.next();
                end_record(&mut record, &mut field);
            }
            '\n' => end_record(&mut record, &mut field),
            other => field.push(other),
        }
    }
    if quoted {
        return Err(invalid(file, "unterminated quoted field"));
    }
    if !field.is_empty() || !record.is_empty() {
        end_record(&mut record, &mut field);
    }
    Ok(records)
}

fn parse_time(file: &str, value: &str, line: usize) -> Result<Timestamp> {
    Timestamp::deserialize(serde::de::value::StrDeserializer::<serde::de::value::Error>::new(value))
        .map_err(|_| {
            invalid(
                file,
                format!("row {line}: time {value:?} is not an RFC 3339 time"),
            )
        })
}

/// A statement export, refused with [`Error::ResponseContract`] without its
/// export ID.
fn statement_export(operation: &str, msg: &ProtoStatementExport) -> Result<StatementExport> {
    require_id(operation, "export_id", msg.export_id)?;
    let failure_reason = prefixed_label::<StatementExportFailureReason>(
        &msg.failure_reason,
        "STATEMENT_EXPORT_FAILURE_REASON_",
    );
    Ok(StatementExport {
        export_id: format_uint64_id(msg.export_id),
        subaccount_id: msg
            .subaccount_id
            .filter(|id| *id != 0)
            .map(format_uint64_id),
        start_time: msg.start_time.as_option().cloned(),
        end_time: msg.end_time.as_option().cloned(),
        asset_id: msg.asset_id,
        idempotency_key: msg.idempotency_key.clone(),
        export_type: prefixed_label::<StatementExportType>(&msg.r#type, ""),
        status: prefixed_label::<StatementExportStatus>(&msg.status, "STATEMENT_EXPORT_STATUS_"),
        complete_through: msg.complete_through.as_option().cloned(),
        movements: msg.movements,
        bytes: msg.bytes,
        created_at: msg.created_at.as_option().cloned(),
        ready_at: msg.ready_at.as_option().cloned(),
        expires_at: msg.expires_at.as_option().cloned(),
        failure_reason: (!failure_reason.is_empty()).then_some(failure_reason),
        history_available_from: msg.history_available_from.as_option().cloned(),
    })
}

/// The export a `CreateStatementExport` reply returns; a reply without it,
/// or without its export ID, is an [`Error::ResponseContract`].
pub fn statement_export_from_create_response(
    msg: &CreateStatementExportResponse,
) -> Result<StatementExport> {
    const OP: &str = "CreateStatementExport";
    let export = msg
        .export
        .as_option()
        .ok_or_else(|| Error::response_contract(OP, "missing export"))?;
    statement_export(OP, export)
}

/// The export a `GetStatementExport` reply returns; a reply without it, or
/// without its export ID, is an [`Error::ResponseContract`].
pub fn statement_export_from_get_response(
    msg: &GetStatementExportResponse,
) -> Result<StatementExport> {
    const OP: &str = "GetStatementExport";
    let export = msg
        .export
        .as_option()
        .ok_or_else(|| Error::response_contract(OP, "missing export"))?;
    statement_export(OP, export)
}

/// The export a `CancelStatementExport` reply returns; a reply without it,
/// or without its export ID, is an [`Error::ResponseContract`].
pub fn statement_export_from_cancel_response(
    msg: &CancelStatementExportResponse,
) -> Result<StatementExport> {
    const OP: &str = "CancelStatementExport";
    let export = msg
        .export
        .as_option()
        .ok_or_else(|| Error::response_contract(OP, "missing export"))?;
    statement_export(OP, export)
}

/// The download URL a `GetStatementExportDownloadUrl` reply returns; a reply
/// without a URL is an [`Error::ResponseContract`], since the URL is all the
/// call is for.
pub fn statement_export_download_url_from_proto(
    msg: &GetStatementExportDownloadUrlResponse,
) -> Result<StatementExportDownloadUrl> {
    if msg.url.is_empty() {
        return Err(Error::response_contract(
            "GetStatementExportDownloadUrl",
            "missing url",
        ));
    }
    let expires_at = msg.expires_at.as_option().cloned().ok_or_else(|| {
        Error::response_contract("GetStatementExportDownloadUrl", "missing expires_at")
    })?;
    if msg.filename.is_empty() {
        return Err(Error::response_contract(
            "GetStatementExportDownloadUrl",
            "missing filename",
        ));
    }
    Ok(StatementExportDownloadUrl {
        url: msg.url.clone(),
        expires_at,
        filename: msg.filename.clone(),
    })
}

pub fn statement_exports_list_from_proto(
    msg: &ListStatementExportsResponse,
) -> Result<StatementExportsList> {
    Ok(StatementExportsList {
        exports: msg
            .exports
            .iter()
            .map(|e| statement_export("ListStatementExports", e))
            .collect::<Result<_>>()?,
        next_page_token: msg.next_page_token.clone(),
        in_progress_exports: msg.in_progress_exports,
        max_in_progress_exports: msg.max_in_progress_exports,
    })
}

/// Read a statement file's rows by header name: the CSV text of a `ready`
/// [`crate::models::StatementExport`]'s file, header row first.
///
/// Columns are found by name, so a statement with columns in another order,
/// or with columns appended by a later release, reads the same; columns this
/// SDK does not know are ignored. `flow_id` is empty when the statement has
/// no such column. Text without a header row, a header missing any other
/// column, a row with another number of fields than the header, content after
/// a closing quote, an unterminated quoted field or a `time` that is not
/// RFC 3339 is an [`Error::ResponseContract`]. Amounts stay exact decimal
/// strings.
pub fn parse_statement_csv(csv: &str) -> Result<Vec<StatementRow>> {
    let table = csv_table(STATEMENT, csv, REQUIRED_COLUMNS)?;
    let [
        time,
        entry_id,
        kind,
        account,
        asset,
        amount,
        fee,
        balance,
        link_id,
    ] = table.columns;
    let flow_id = table.column("flow_id");
    table
        .rows
        .iter()
        .map(|(line, record)| {
            Ok(StatementRow {
                time: parse_time(STATEMENT, &record[time], *line)?,
                entry_id: record[entry_id].clone(),
                kind: record[kind].clone(),
                account: record[account].clone(),
                asset: record[asset].clone(),
                amount: record[amount].clone(),
                fee: record[fee].clone(),
                balance: record[balance].clone(),
                link_id: record[link_id].clone(),
                flow_id: flow_id.map(|i| record[i].clone()).unwrap_or_default(),
            })
        })
        .collect()
}

/// Read a ledger file's rows by header name: the CSV text of a `ready`
/// [`crate::models::StatementExport`] of type `ledger`, header row first.
///
/// Read as [`parse_statement_csv`] reads an account statement: columns are
/// found by name and columns this SDK does not know are ignored. Text
/// without a header row, a header missing a ledger column (an account
/// statement lacks `transfer_id` and `side`), a row with another number of
/// fields than the header, content after a closing quote, an unterminated
/// quoted field or a `time` that is not RFC 3339 is an
/// [`Error::ResponseContract`]. Amounts stay exact decimal strings.
pub fn parse_ledger_csv(csv: &str) -> Result<Vec<LedgerRow>> {
    let table = csv_table(LEDGER, csv, LEDGER_COLUMNS)?;
    let [
        time,
        transfer_id,
        account,
        asset,
        kind,
        side,
        amount,
        counterparty,
        balance,
        link_id,
        flow_id,
    ] = table.columns;
    table
        .rows
        .iter()
        .map(|(line, record)| {
            Ok(LedgerRow {
                time: parse_time(LEDGER, &record[time], *line)?,
                transfer_id: record[transfer_id].clone(),
                account: record[account].clone(),
                asset: record[asset].clone(),
                kind: record[kind].clone(),
                side: record[side].clone(),
                amount: record[amount].clone(),
                counterparty: record[counterparty].clone(),
                balance: record[balance].clone(),
                link_id: record[link_id].clone(),
                flow_id: record[flow_id].clone(),
            })
        })
        .collect()
}

/// Read a balances file's rows by header name: the CSV text of a `ready`
/// [`crate::models::StatementExport`] of type `balances`, header row first.
///
/// Read as [`parse_statement_csv`] reads an account statement: columns are
/// found by name and columns this SDK does not know are ignored. Rows come
/// sorted by `account`, then `asset`; a bucket and asset with a zero balance
/// at both ends and no movement has no row. Text without a header row, a
/// header missing a balances column, a row with another number of fields
/// than the header, content after a closing quote, an unterminated quoted
/// field or a `movements` that is not a count is an
/// [`Error::ResponseContract`]. Amounts stay exact decimal strings.
pub fn parse_balances_csv(csv: &str) -> Result<Vec<BalancesRow>> {
    let table = csv_table(BALANCES, csv, BALANCES_COLUMNS)?;
    let [
        account,
        asset,
        opening_balance,
        credits,
        debits,
        closing_balance,
        movements,
    ] = table.columns;
    table
        .rows
        .iter()
        .map(|(line, record)| {
            let count = &record[movements];
            if count.is_empty() || !count.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid(
                    BALANCES,
                    format!("row {line}: movements {count:?} is not a count"),
                ));
            }
            Ok(BalancesRow {
                account: record[account].clone(),
                asset: record[asset].clone(),
                opening_balance: record[opening_balance].clone(),
                credits: record[credits].clone(),
                debits: record[debits].clone(),
                closing_balance: record[closing_balance].clone(),
                movements: count.parse().map_err(|_| {
                    invalid(
                        BALANCES,
                        format!("row {line}: movements {count:?} is not a count"),
                    )
                })?,
            })
        })
        .collect()
}

/// A CSV file's header, the positions of its required columns, and its rows
/// with their line numbers, each as long as the header.
struct CsvTable<const N: usize> {
    header: Vec<String>,
    columns: [usize; N],
    rows: Vec<(usize, Vec<String>)>,
}

impl<const N: usize> CsvTable<N> {
    fn column(&self, name: &str) -> Option<usize> {
        self.header.iter().position(|column| column == name)
    }
}

fn csv_table<const N: usize>(file: &str, csv: &str, required: [&str; N]) -> Result<CsvTable<N>> {
    let mut records = csv_records(file, csv)?.into_iter();
    let header = records
        .next()
        .ok_or_else(|| invalid(file, "no header row"))?;
    let mut columns = [0usize; N];
    for (slot, name) in columns.iter_mut().zip(required) {
        *slot = header
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| invalid(file, format!("header has no {name} column")))?;
    }
    let rows = records
        .enumerate()
        .map(|(index, record)| {
            let line = index + 2;
            if record.len() != header.len() {
                return Err(invalid(
                    file,
                    format!(
                        "row {line} has {} fields; the header has {}",
                        record.len(),
                        header.len()
                    ),
                ));
            }
            Ok((line, record))
        })
        .collect::<Result<_>>()?;
    Ok(CsvTable {
        header,
        columns,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATEMENT: &str = "time,entry_id,type,account,asset,amount,fee,balance,link_id,flow_id\r\n\
        2026-10-02T14:03:11.000000Z,27419385013475392777,deposit,funding,USDT,100,0,100,,flow_8d1f2c\r\n\
        2026-10-03T08:12:45.123456Z,27419385013475392811,trade,trading,USDT,-5000,2.5,4997.5,77,\r\n";

    #[test]
    fn statement_rows_read_every_column_as_text() {
        let rows = parse_statement_csv(STATEMENT).unwrap();
        assert_eq!(rows.len(), 2);
        let deposit = &rows[0];
        assert_eq!(
            (deposit.time.seconds, deposit.time.nanos),
            (1_790_949_791, 0)
        );
        assert_eq!(deposit.entry_id, "27419385013475392777");
        assert_eq!(deposit.kind, "deposit");
        assert_eq!(deposit.account, "funding");
        assert_eq!(deposit.link_id, "");
        assert_eq!(deposit.flow_id, "flow_8d1f2c");
        let trade = &rows[1];
        assert_eq!(trade.time.nanos, 123_456_000);
        assert_eq!(
            (
                trade.amount.as_str(),
                trade.fee.as_str(),
                trade.balance.as_str()
            ),
            ("-5000", "2.5", "4997.5")
        );
        assert_eq!(trade.link_id, "77");
        assert_eq!(trade.flow_id, "");
    }

    /// Columns are read by name: reordered and appended columns read the
    /// same, and a statement without `flow_id` reads it as empty.
    #[test]
    fn statement_rows_read_columns_by_header_name() {
        let reordered = "asset,flow_id,future_column,amount,time,type,account,fee,balance,link_id,entry_id\r\n\
            BTC,flow_1,x,0.1,2026-10-03T08:12:45.123456Z,trade,trading,0,1.1,77,42\r\n";
        let row = &parse_statement_csv(reordered).unwrap()[0];
        assert_eq!(
            (
                row.asset.as_str(),
                row.amount.as_str(),
                row.entry_id.as_str()
            ),
            ("BTC", "0.1", "42")
        );
        assert_eq!(
            (row.kind.as_str(), row.flow_id.as_str()),
            ("trade", "flow_1")
        );

        let nine = "time,entry_id,type,account,asset,amount,fee,balance,link_id\r\n\
            2026-10-03T08:12:45.123456Z,42,trade,trading,BTC,0.1,0,1.1,77\r\n";
        let row = &parse_statement_csv(nine).unwrap()[0];
        assert_eq!(row.flow_id, "");
        assert_eq!(row.link_id, "77");
    }

    /// Quoted fields keep commas, line breaks and doubled quotes; a final
    /// line without CRLF and bare LF line endings read too.
    #[test]
    fn statement_rows_read_rfc_4180_quoting() {
        let csv = "time,entry_id,type,account,asset,amount,fee,balance,link_id\n\
            2026-10-03T08:12:45Z,42,\"system_adjustment\",trading,\"A,\"\"B\"\"\r\nC\",-1,0,0,\"\"";
        let row = &parse_statement_csv(csv).unwrap()[0];
        assert_eq!(row.kind, "system_adjustment");
        assert_eq!(row.asset, "A,\"B\"\r\nC");
        assert_eq!(row.link_id, "");
    }

    /// A closing quote ends its field: `"100"0` is refused, never read as
    /// 1000, and so is a quoted field left open. A doubled quote still reads
    /// as one quote.
    #[test]
    fn statement_csv_quoting_is_strict() {
        const HEADER: &str = "time,entry_id,type,account,asset,amount,fee,balance,link_id\r\n";
        for (row, want) in [
            (
                "2026-10-03T08:12:45Z,1,trade,trading,BTC,\"100\"0,0,1.1,77\r\n",
                "after the closing quote",
            ),
            (
                "2026-10-03T08:12:45Z,1,trade,trading,BTC,\"100\" ,0,1.1,77\r\n",
                "after the closing quote",
            ),
            (
                "2026-10-03T08:12:45Z,1,trade,trading,BTC,0.1,0,1.1,\"77\"\rx\r\n",
                "after the closing quote",
            ),
            (
                "2026-10-03T08:12:45Z,1,trade,trading,BTC,\"0.1,0,1.1,77\r\n",
                "unterminated quoted field",
            ),
        ] {
            let err = parse_statement_csv(&format!("{HEADER}{row}")).unwrap_err();
            assert!(
                matches!(err, Error::ResponseContract { .. }),
                "{row:?}: {err:?}"
            );
            assert!(err.to_string().contains(want), "{row:?}: {err}");
        }
        let rows = parse_statement_csv(&format!(
            "{HEADER}2026-10-03T08:12:45Z,1,trade,trading,\"B\"\"TC\",0.1,0,\"1,1\",\"77\"\r\n\
             2026-10-03T08:12:46Z,2,trade,trading,BTC,0.1,0,1.2,\"\""
        ))
        .unwrap();
        assert_eq!(rows[0].asset, "B\"TC");
        assert_eq!(rows[0].balance, "1,1");
        assert_eq!(rows[0].link_id, "77");
        assert_eq!(rows[1].link_id, "");
    }

    #[test]
    fn statement_header_alone_is_an_empty_statement() {
        let rows = parse_statement_csv(
            "time,entry_id,type,account,asset,amount,fee,balance,link_id,flow_id\r\n",
        )
        .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn statement_csv_refuses_what_it_cannot_read() {
        for (csv, want) in [
            ("", "no header row"),
            (
                "time,entry_id,type,account,asset,amount,fee,balance\r\n",
                "no link_id column",
            ),
            (
                "time,entry_id,type,account,asset,amount,fee,balance,link_id\r\n1,2,3\r\n",
                "row 2 has 3 fields",
            ),
            (
                "time,entry_id,type,account,asset,amount,fee,balance,link_id\r\n\"open,2,3",
                "unterminated",
            ),
            (
                "time,entry_id,type,account,asset,amount,fee,balance,link_id\r\nyesterday,1,t,a,b,0,0,0,\r\n",
                "not an RFC 3339 time",
            ),
        ] {
            let err = parse_statement_csv(csv).unwrap_err();
            assert!(matches!(err, Error::ResponseContract { .. }), "{err:?}");
            assert!(err.to_string().contains(want), "{csv:?}: {err}");
        }
    }

    fn export(id: u64) -> ProtoStatementExport {
        ProtoStatementExport {
            export_id: id,
            subaccount_id: Some(9),
            asset_id: 3,
            idempotency_key: "statement-2026".into(),
            status: StatementExportStatus::STATEMENT_EXPORT_STATUS_FAILED.into(),
            failure_reason: StatementExportFailureReason::STATEMENT_EXPORT_FAILURE_REASON_TOO_LARGE
                .into(),
            movements: u64::MAX,
            bytes: 12,
            ..Default::default()
        }
    }

    #[test]
    fn statement_export_decodes_labels_ids_and_counters() {
        let got = statement_export_from_get_response(&GetStatementExportResponse {
            export: export(7).into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(got.export_id, format_uint64_id(7));
        assert_eq!(got.subaccount_id, Some(format_uint64_id(9)));
        assert_eq!(got.status, "failed");
        assert_eq!(got.failure_reason.as_deref(), Some("too_large"));
        assert_eq!((got.movements, got.bytes), (u64::MAX, 12));
        let mut ready = export(8);
        ready.subaccount_id = None;
        ready.status = StatementExportStatus::STATEMENT_EXPORT_STATUS_READY.into();
        ready.failure_reason = buffa::EnumValue::from(0);
        let got = statement_export_from_create_response(&CreateStatementExportResponse {
            export: ready.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            (got.status.as_str(), got.subaccount_id, got.failure_reason),
            ("ready", None, None)
        );
        let mut future = export(9);
        future.status = buffa::EnumValue::from(42);
        let list = statement_exports_list_from_proto(&ListStatementExportsResponse {
            exports: vec![future],
            next_page_token: "n".into(),
            in_progress_exports: 5,
            max_in_progress_exports: 5,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(list.exports[0].status, "UNKNOWN(42)");
        assert_eq!(list.next_page_token, "n");
        assert_eq!(
            (list.in_progress_exports, list.max_in_progress_exports),
            (5, 5)
        );
        let mut canceled = export(10);
        canceled.status = StatementExportStatus::STATEMENT_EXPORT_STATUS_CANCELED.into();
        canceled.failure_reason = buffa::EnumValue::from(0);
        let got = statement_export_from_cancel_response(&CancelStatementExportResponse {
            export: canceled.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            (got.export_id, got.status.as_str()),
            (format_uint64_id(10), "canceled")
        );
    }

    /// A download URL reply keeps its URL, expiry and file name as sent, and
    /// a reply without a URL is refused.
    #[test]
    fn statement_export_download_url_requires_its_url() {
        let expires_at = Timestamp {
            seconds: 1_763_337_660,
            nanos: 5,
            ..Default::default()
        };
        let got =
            statement_export_download_url_from_proto(&GetStatementExportDownloadUrlResponse {
                url: "https://files.example/s?sig=x".into(),
                expires_at: expires_at.clone().into(),
                filename: "statement-7.csv".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(got.url, "https://files.example/s?sig=x");
        assert_eq!(got.expires_at, expires_at);
        assert_eq!(got.filename, "statement-7.csv");
        let err =
            statement_export_download_url_from_proto(&GetStatementExportDownloadUrlResponse {
                filename: "statement-7.csv".into(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, Error::ResponseContract { .. }), "{err:?}");
        // The expiry and the file name are part of every URL the contract
        // returns: a reply without either is refused, not read as unknown.
        for reply in [
            GetStatementExportDownloadUrlResponse {
                url: "https://files.example/s?sig=x".into(),
                filename: "statement-7.csv".into(),
                ..Default::default()
            },
            GetStatementExportDownloadUrlResponse {
                url: "https://files.example/s?sig=x".into(),
                expires_at: expires_at.clone().into(),
                ..Default::default()
            },
        ] {
            let err = statement_export_download_url_from_proto(&reply).unwrap_err();
            assert!(matches!(err, Error::ResponseContract { .. }), "{err:?}");
        }
    }

    /// An export reply must carry the export and its ID.
    #[test]
    fn statement_export_replies_require_the_export_and_its_id() {
        for err in [
            statement_export_from_create_response(&CreateStatementExportResponse::default()),
            statement_export_from_get_response(&GetStatementExportResponse::default()),
            statement_export_from_get_response(&GetStatementExportResponse {
                export: export(0).into(),
                ..Default::default()
            }),
            statement_export_from_cancel_response(&CancelStatementExportResponse::default()),
            statement_export_from_cancel_response(&CancelStatementExportResponse {
                export: export(0).into(),
                ..Default::default()
            }),
        ] {
            assert!(
                matches!(err, Err(Error::ResponseContract { .. })),
                "{err:?}"
            );
        }
        assert!(matches!(
            statement_exports_list_from_proto(&ListStatementExportsResponse {
                exports: vec![export(0)],
                ..Default::default()
            }),
            Err(Error::ResponseContract { .. })
        ));
    }

    /// An export's type reads as its label, and a type newer than this SDK
    /// reads as `UNKNOWN(<n>)` instead of failing the read, since more types
    /// will come.
    #[test]
    fn statement_export_reads_known_and_unknown_types() {
        for (value, want) in [
            (
                StatementExportType::ACCOUNT_STATEMENT as i32,
                "account_statement",
            ),
            (StatementExportType::LEDGER as i32, "ledger"),
            (StatementExportType::BALANCES as i32, "balances"),
            (0, ""),
            (7, "UNKNOWN(7)"),
        ] {
            let mut msg = export(7);
            msg.r#type = buffa::EnumValue::from(value);
            let got = statement_export_from_get_response(&GetStatementExportResponse {
                export: msg.into(),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(got.export_type, want, "type {value}");
        }
        assert_eq!(
            crate::models::StatementExportType::AccountStatement.as_str(),
            "account_statement"
        );
        assert_eq!(
            crate::models::StatementExportType::Ledger.as_str(),
            "ledger"
        );
    }

    const LEDGER_HEADER: &str =
        "time,transfer_id,account,asset,type,side,amount,counterparty,balance,link_id,flow_id\r\n";

    /// A trade's quote debit and its taker fee, then a deposit credit, with
    /// reordered columns and a column a later release appends.
    #[test]
    fn ledger_rows_read_columns_by_header_name() {
        let csv = "side,flow_id,time,transfer_id,account,asset,type,amount,counterparty,balance,link_id,future\r\n\
            debit,,2026-10-03T08:12:45.123456Z,27419385013475392811,trading,USDT,trade_quote,-100.5,trading_account,899.5,77,\"x,y\"\r\n\
            debit,,2026-10-03T08:12:45.123456Z,27419385013475392812,trading,USDT,taker_fee,-0.1,fee_account,899.4,77,y\r\n\
            credit,flow_1,2026-10-04T00:00:00.000001Z,27419385013475392900,funding,BTC,deposit,0.25,,0.25,,z\r\n";
        let rows = parse_ledger_csv(csv).unwrap();
        assert_eq!(rows.len(), 3);
        let quote = &rows[0];
        assert_eq!(
            (quote.time.seconds, quote.time.nanos),
            (1_791_015_165, 123_456_000)
        );
        assert_eq!(
            (
                quote.transfer_id.as_str(),
                quote.account.as_str(),
                quote.asset.as_str(),
                quote.kind.as_str(),
                quote.side.as_str(),
                quote.amount.as_str(),
                quote.counterparty.as_str(),
                quote.balance.as_str(),
                quote.link_id.as_str(),
                quote.flow_id.as_str(),
            ),
            (
                "27419385013475392811",
                "trading",
                "USDT",
                "trade_quote",
                "debit",
                "-100.5",
                "trading_account",
                "899.5",
                "77",
                ""
            )
        );
        assert_eq!(
            (rows[1].kind.as_str(), rows[1].counterparty.as_str()),
            ("taker_fee", "fee_account")
        );
        let deposit = &rows[2];
        assert_eq!(
            (
                deposit.side.as_str(),
                deposit.counterparty.as_str(),
                deposit.link_id.as_str(),
                deposit.flow_id.as_str(),
                deposit.time.nanos
            ),
            ("credit", "", "", "flow_1", 1_000)
        );
    }

    #[test]
    fn ledger_header_alone_is_an_empty_ledger() {
        assert!(parse_ledger_csv(LEDGER_HEADER).unwrap().is_empty());
    }

    #[test]
    fn ledger_csv_refuses_what_it_cannot_read() {
        for (csv, want) in [
            ("", "no header row"),
            (
                "time,transfer_id,account,asset,type,side,amount,counterparty,balance,link_id\r\n",
                "no flow_id column",
            ),
            (
                "time,entry_id,type,account,asset,amount,fee,balance,link_id,flow_id\r\n",
                "no transfer_id column",
            ),
            (
                &format!("{LEDGER_HEADER}2026-10-03T08:12:45Z,1,trading\r\n"),
                "row 2 has 3 fields",
            ),
            (
                &format!("{LEDGER_HEADER}yesterday,1,trading,BTC,deposit,credit,1,,1,,\r\n"),
                "not an RFC 3339 time",
            ),
        ] {
            let err = parse_ledger_csv(csv).unwrap_err();
            assert!(
                matches!(err, Error::ResponseContract { .. }),
                "{csv:?}: {err:?}"
            );
            let message = err.to_string();
            assert!(message.contains("ledger csv"), "{csv:?}: {message}");
            assert!(message.contains(want), "{csv:?}: {message}");
        }
        // An account statement reader refuses a ledger.
        assert!(parse_statement_csv(LEDGER_HEADER).is_err());
    }

    /// A balances export that failed because the account history moved later
    /// names where the history starts; another export leaves it `None`.
    #[test]
    fn statement_export_reads_history_available_from() {
        let mut msg = export(7);
        msg.r#type = StatementExportType::BALANCES.into();
        msg.failure_reason =
            StatementExportFailureReason::STATEMENT_EXPORT_FAILURE_REASON_HISTORY_UNAVAILABLE
                .into();
        msg.history_available_from = Timestamp {
            seconds: 1_795_964_102,
            nanos: 5,
            ..Default::default()
        }
        .into();
        let got = statement_export_from_get_response(&GetStatementExportResponse {
            export: msg.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(got.failure_reason.as_deref(), Some("history_unavailable"));
        let from = got.history_available_from.unwrap();
        assert_eq!((from.seconds, from.nanos), (1_795_964_102, 5));
        let got = statement_export_from_get_response(&GetStatementExportResponse {
            export: export(8).into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(got.history_available_from, None);
    }

    const BALANCES_HEADER: &str =
        "account,asset,opening_balance,credits,debits,closing_balance,movements\r\n";

    #[test]
    fn balances_rows_read_columns_by_header_name() {
        let csv = "movements,asset,account,closing_balance,debits,credits,opening_balance,future\r\n\
            3,BTC,funding,0.75,0.25,1,0,x\r\n\
            12,USDT,trading,899.4,100.6,0,1000,y\r\n";
        assert_eq!(
            parse_balances_csv(csv).unwrap(),
            vec![
                BalancesRow {
                    account: "funding".into(),
                    asset: "BTC".into(),
                    opening_balance: "0".into(),
                    credits: "1".into(),
                    debits: "0.25".into(),
                    closing_balance: "0.75".into(),
                    movements: 3,
                },
                BalancesRow {
                    account: "trading".into(),
                    asset: "USDT".into(),
                    opening_balance: "1000".into(),
                    credits: "0".into(),
                    debits: "100.6".into(),
                    closing_balance: "899.4".into(),
                    movements: 12,
                },
            ]
        );
        assert!(parse_balances_csv(BALANCES_HEADER).unwrap().is_empty());
    }

    #[test]
    fn balances_csv_refuses_what_it_cannot_read() {
        for (csv, want) in [
            ("", "no header row"),
            (
                "account,asset,opening_balance,credits,debits,closing_balance\r\n",
                "no movements column",
            ),
            (LEDGER_HEADER, "no opening_balance column"),
            (
                &format!("{BALANCES_HEADER}funding,BTC\r\n"),
                "row 2 has 2 fields",
            ),
            (
                &format!("{BALANCES_HEADER}funding,BTC,0,1,0,1,three\r\n"),
                "is not a count",
            ),
            (
                &format!("{BALANCES_HEADER}funding,BTC,0,1,0,1,-1\r\n"),
                "is not a count",
            ),
            // Digits only, but past u64.
            (
                &format!(
                    "{BALANCES_HEADER}funding,BTC,0,1,0,1,{}\r\n",
                    "9".repeat(30)
                ),
                "is not a count",
            ),
        ] {
            let err = parse_balances_csv(csv).unwrap_err();
            let message = err.to_string();
            assert!(message.contains("balances csv"), "{csv:?}: {message}");
            assert!(message.contains(want), "{csv:?}: {message}");
        }
    }
}
