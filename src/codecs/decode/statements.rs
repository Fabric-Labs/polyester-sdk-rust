//! Account statement CSV reader.

use crate::errors::{Error, Result};
use crate::models::StatementRow;
use buffa_types::google::protobuf::Timestamp;
use serde::Deserialize;

/// Columns every statement row carries.
const REQUIRED_COLUMNS: [&str; 9] = [
    "time", "entry_id", "type", "account", "asset", "amount", "fee", "balance", "link_id",
];

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::response_contract("ExportStatement", format!("statement csv: {message}"))
}

/// Split RFC 4180 text into records: fields separated by commas, records by
/// CRLF (a bare LF is accepted too), and quoted fields that may hold commas,
/// line breaks and doubled quotes. Blank lines are skipped. A closing quote
/// must end its field: anything but a comma or a line break after it, or a
/// quoted field still open at the end, is refused.
fn csv_records(text: &str) -> Result<Vec<Vec<String>>> {
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
                return Err(invalid(format!("{c:?} after the closing quote of a field")));
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
        return Err(invalid("unterminated quoted field"));
    }
    if !field.is_empty() || !record.is_empty() {
        end_record(&mut record, &mut field);
    }
    Ok(records)
}

fn parse_time(value: &str, line: usize) -> Result<Timestamp> {
    Timestamp::deserialize(serde::de::value::StrDeserializer::<serde::de::value::Error>::new(value))
        .map_err(|_| {
            invalid(format!(
                "row {line}: time {value:?} is not an RFC 3339 time"
            ))
        })
}

/// Read a complete statement's rows by header name: the CSV text of
/// [`crate::models::Statement`], or a first page, header row first.
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
    let mut records = csv_records(csv)?.into_iter();
    let header = records.next().ok_or_else(|| invalid("no header row"))?;
    let column = |name: &str| header.iter().position(|column| column == name);
    let mut required = [0usize; REQUIRED_COLUMNS.len()];
    for (slot, name) in required.iter_mut().zip(REQUIRED_COLUMNS) {
        *slot = column(name).ok_or_else(|| invalid(format!("header has no {name} column")))?;
    }
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
    ] = required;
    let flow_id = column("flow_id");
    records
        .enumerate()
        .map(|(index, record)| {
            let line = index + 2;
            if record.len() != header.len() {
                return Err(invalid(format!(
                    "row {line} has {} fields; the header has {}",
                    record.len(),
                    header.len()
                )));
            }
            Ok(StatementRow {
                time: parse_time(&record[time], line)?,
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
}
