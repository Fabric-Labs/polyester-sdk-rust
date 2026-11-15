//! Request limits the published contract defines for the account-flow,
//! batch-transfer, dust-conversion and statement-export calls.
//!
//! Each value is a `buf.validate` rule of the contract. Where the generated
//! field docs state it, a test pins the constant to that text, so a contract
//! change that regenerates the docs breaks the build; where they do not, the
//! test pins the rule's value and names the rule.

use crate::errors::{Error, Result};

/// Most flows one `ListAccountFlows` page returns: the `uint32.lte` rule on
/// `ListAccountFlowsRequest.limit`.
pub(crate) const LIST_ACCOUNT_FLOWS_LIMIT_MAX: u32 = 500;
/// Most IDs each `ListAccountFlowsRequest` ID filter takes
/// (`polyester_chain_ids`, `zipped_asset_ids`, `unified_asset_ids`): the
/// `repeated.max_items` rule, with `unique: true` and `items.uint32.gt: 0`.
pub(crate) const ACCOUNT_FLOWS_ID_FILTER_MAX: usize = 100;
/// Most legs one `BatchCreateInternalTransfers` request takes: the
/// `repeated.max_items` rule on `BatchCreateInternalTransfersRequest.legs`.
pub(crate) const BATCH_TRANSFER_LEGS_MAX: usize = 10;
/// Fewest legs a batch takes: the `repeated.min_items` rule on the same field.
pub(crate) const BATCH_TRANSFER_LEGS_MIN: usize = 1;
/// Longest idempotency key of a batch transfer or a dust conversion, in
/// characters: the `string.max_len` rule on
/// `BatchCreateInternalTransfersRequest.idempotency_key` and
/// `CreateDustConversionRequest.idempotency_key` (both also `min_len: 1`).
pub(crate) const IDEMPOTENCY_KEY_MAX_LEN: usize = 128;
/// Longest dust quote ID, in characters: the `string.max_len` rule on
/// `CreateDustConversionRequest.quote_id` (also `min_len: 1`).
pub(crate) const DUST_QUOTE_ID_MAX_LEN: usize = 64;
/// Longest dust conversion ID, in characters: the `string.max_len` rule on
/// `GetDustConversionRequest.conversion_id`, whose `pattern` is
/// `^[1-9][0-9]*$`.
pub(crate) const DUST_CONVERSION_ID_MAX_LEN: usize = 39;
/// Most conversions one `ListDustConversions` page returns: the `uint32.lte`
/// rule on `ListDustConversionsRequest.limit`.
pub(crate) const LIST_DUST_CONVERSIONS_LIMIT_MAX: u32 = 100;
/// Longest `ListDustConversions` page token: the `string.max_len` rule on
/// `ListDustConversionsRequest.page_token`.
pub(crate) const DUST_PAGE_TOKEN_MAX_LEN: usize = 256;
/// Longest statement export idempotency key, in characters: the
/// `string.max_len` rule on `CreateStatementExportRequest.idempotency_key`
/// (also `min_len: 1` and `pattern: "^[A-Za-z0-9._:-]+$"`).
pub(crate) const STATEMENT_EXPORT_IDEMPOTENCY_KEY_MAX_LEN: usize = 64;
/// Longest statement export period: the CEL rule
/// `create_statement_export.period_valid` on `CreateStatementExportRequest`,
/// `end_time - start_time <= duration('8784h')`, that is 366 days.
pub(crate) const STATEMENT_EXPORT_PERIOD_MAX_SECS: u64 = 8_784 * 3_600;
/// Most exports one `ListStatementExports` page returns: the `uint32.lte`
/// rule on `ListStatementExportsRequest.limit`.
pub(crate) const LIST_STATEMENT_EXPORTS_LIMIT_MAX: u32 = 100;
/// Longest `ListStatementExports` page token: the `string.max_len` rule on
/// `ListStatementExportsRequest.page_token`.
pub(crate) const STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN: usize = 512;

/// Refuse an ID filter that breaks its `repeated` rule: more than `max`
/// IDs, an ID of 0, or an ID listed twice.
pub(crate) fn check_id_filter(field: &str, ids: &[u32], max: usize) -> Result<()> {
    if ids.len() > max {
        return Err(Error::validation(format!(
            "{field} holds at most {max} IDs, not {}",
            ids.len()
        )));
    }
    if ids.contains(&0) {
        return Err(Error::validation(format!("{field} IDs must be positive")));
    }
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    if let Some(repeated) = ids.iter().find(|id| !seen.insert(**id)) {
        return Err(Error::validation(format!("{field} repeats {repeated}")));
    }
    Ok(())
}

/// Refuse a page size above the contract's maximum; 0 asks for the server's
/// default.
pub(crate) fn check_limit(operation: &str, limit: u32, max: u32) -> Result<()> {
    if limit > max {
        return Err(Error::validation(format!(
            "{operation} limit {limit} is above the maximum {max}"
        )));
    }
    Ok(())
}

/// Refuse an empty string, or one longer than `max` characters.
pub(crate) fn check_len(operation: &str, field: &str, value: &str, max: usize) -> Result<()> {
    let len = value.chars().count();
    if len == 0 {
        return Err(Error::validation(format!(
            "{operation} {field} is required"
        )));
    }
    if len > max {
        return Err(Error::validation(format!(
            "{operation} {field} is {len} characters; at most {max} are allowed"
        )));
    }
    Ok(())
}

/// Refuse a page token longer than `max` characters; an empty one starts at
/// the first page.
pub(crate) fn check_page_token(operation: &str, token: &str, max: usize) -> Result<()> {
    let len = token.chars().count();
    if len > max {
        return Err(Error::validation(format!(
            "{operation} page_token is {len} characters; at most {max} are allowed"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_docs(docs: &str, text: &str) {
        assert!(docs.contains(text), "generated docs changed: {text:?}");
    }

    /// The limits the generated field docs state are pinned to that text.
    #[test]
    fn limits_match_the_generated_contract_docs() {
        let lifecycle = include_str!("../gen/buffa/chain.lifecycle.v1.rs");
        assert_docs(
            lifecycle,
            &format!(
                "/// Maximum number of flows to return. Defaults to 100 when omitted; maximum is\n    /// {LIST_ACCOUNT_FLOWS_LIMIT_MAX}.\n    ///\n    /// Field 2: `limit`\n"
            ),
        );
        for (line, number, filter) in [
            ("Optional Polyester Chain ids", 8, "polyester_chain_ids"),
            ("Optional chain asset mapping ids", 9, "zipped_asset_ids"),
            ("Optional unified asset ids", 10, "unified_asset_ids"),
        ] {
            assert_docs(
                lifecycle,
                &format!(
                    "/// {line} to include. Maximum {ACCOUNT_FLOWS_ID_FILTER_MAX} unique ids.\n    ///\n    /// Field {number}: `{filter}`\n"
                ),
            );
        }
        let transfer = include_str!("../gen/buffa/transfer.v1.rs");
        assert_docs(
            transfer,
            &format!(
                "/// Transfers to run, in order. At most {BATCH_TRANSFER_LEGS_MAX} legs; each leg counts as one\n"
            ),
        );
        assert_docs(
            transfer,
            &format!(
                "/// Maximum number of conversions to return: 0 means 20, at most {LIST_DUST_CONVERSIONS_LIMIT_MAX}.\n"
            ),
        );
        let ledger = include_str!("../gen/buffa/ledger.read.v1.rs");
        assert_docs(
            ledger,
            &format!(
                "/// Client-chosen key, 1 to {STATEMENT_EXPORT_IDEMPOTENCY_KEY_MAX_LEN} characters of letters, digits and `.`, `_`,\n    /// `:` or `-`, unique per root account."
            ),
        );
        assert_docs(
            ledger,
            &format!(
                "/// Maximum exports to return; 0 means 20. At most {LIST_STATEMENT_EXPORTS_LIMIT_MAX}.\n"
            ),
        );
    }

    /// The generated docs do not state these rules, so their values are
    /// pinned here: legs `min_items: 1`; idempotency keys `max_len: 128`;
    /// `quote_id` `max_len: 64`; `conversion_id` `max_len: 39`; page tokens
    /// `max_len: 256` (dust) and `512` (statement exports); the statement
    /// export period `duration('8784h')`.
    #[test]
    fn limits_the_generated_docs_do_not_state_are_the_contract_rules() {
        assert_eq!(BATCH_TRANSFER_LEGS_MIN, 1);
        assert_eq!(IDEMPOTENCY_KEY_MAX_LEN, 128);
        assert_eq!(DUST_QUOTE_ID_MAX_LEN, 64);
        assert_eq!(DUST_CONVERSION_ID_MAX_LEN, 39);
        assert_eq!(DUST_PAGE_TOKEN_MAX_LEN, 256);
        assert_eq!(STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN, 512);
        assert_eq!(STATEMENT_EXPORT_PERIOD_MAX_SECS, 366 * 24 * 3_600);
    }

    #[test]
    fn checks_refuse_only_outside_the_rule() {
        assert!(check_limit("op", 0, 100).is_ok());
        assert!(check_limit("op", 100, 100).is_ok());
        assert!(matches!(
            check_limit("op", 101, 100),
            Err(Error::Validation(_))
        ));
        assert!(check_len("op", "key", "a", 2).is_ok());
        assert!(check_len("op", "key", "éé", 2).is_ok());
        assert!(matches!(
            check_len("op", "key", "", 2),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            check_len("op", "key", "abc", 2),
            Err(Error::Validation(_))
        ));
        assert!(check_id_filter("ids", &[1, 2], 2).is_ok());
        for (ids, want) in [
            (&[1, 2, 3][..], "ids holds at most 2 IDs, not 3"),
            (&[0][..], "ids IDs must be positive"),
            (&[4, 4][..], "ids repeats 4"),
        ] {
            let err = check_id_filter("ids", ids, 2).unwrap_err();
            assert!(err.to_string().contains(want), "{err}");
        }
        assert!(check_page_token("op", "", 2).is_ok());
        assert!(matches!(
            check_page_token("op", "abc", 2),
            Err(Error::Validation(_))
        ));
    }
}
