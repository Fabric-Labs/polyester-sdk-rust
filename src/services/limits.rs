//! Request limits the published contract defines: page sizes of the list
//! calls, and the account-flow, batch-transfer, dust-conversion and
//! statement-export limits.
//!
//! Each value is a `buf.validate` rule of the contract, or a limit its
//! generated field docs state and the API enforces. Where the generated
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
/// Most legs one `BatchCreateInternalTransfers` request takes, as the
/// generated docs of `BatchCreateInternalTransfersRequest.legs` state it
/// ("A batch holds at most 500 legs"); the API refuses more with
/// `INVALID_REQUEST`.
pub(crate) const BATCH_TRANSFER_LEGS_MAX: usize = 500;
/// Fewest legs a batch takes: the `repeated.min_items` rule on the same field.
pub(crate) const BATCH_TRANSFER_LEGS_MIN: usize = 1;
/// Longest batch request ID, in characters: the `string.max_len` rule on
/// `GetInternalTransferBatchRequest.request_id`, whose `pattern` is
/// `^[1-9][0-9]*$` (also `min_len: 1`).
pub(crate) const BATCH_TRANSFER_REQUEST_ID_MAX_LEN: usize = 39;
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

/// Largest page of the order reads: the `uint32 {gte: 1, lte: 1000}` rule on
/// the optional `limit` of `GetOpenOrdersRequest`, `GetOrderHistoryRequest`,
/// `GetUserTradesRequest` and `GetOrderRequest` (its executions).
pub(crate) const ORDER_READ_LIMIT_MAX: u32 = 1_000;
/// Largest `GetTrades` page: the `uint32.lte` rule on `GetTradesRequest.limit`.
pub(crate) const MARKET_TRADES_LIMIT_MAX: u32 = 1_000;
/// Most candles one `GetCandles` or `GetCandlesColumns` page returns: the
/// `uint32.lte` rule on their `limit`.
pub(crate) const CANDLES_LIMIT_MAX: u32 = 10_000;
/// Most delta buckets one heatmap page returns: the `uint32 {gte: 1, lte:
/// 20000}` rule on `GetOrderbookHeatmapRequest.limit`.
pub(crate) const HEATMAP_LIMIT_MAX: u32 = 20_000;
/// Fewest delta buckets one heatmap page asks for: the `gte: 1` of the same
/// rule. The field has no presence, so 0 is on the wire and refused there.
pub(crate) const HEATMAP_LIMIT_MIN: u32 = 1;
/// Largest `ListTransfers` page: the `uint32.lte` rule on
/// `ListTransfersRequest.limit`.
pub(crate) const LIST_TRANSFERS_LIMIT_MAX: u32 = 1_000;
/// Largest `ListMarketOverview` page: the `uint32.lte` rule on
/// `ListMarketOverviewRequest.limit`.
pub(crate) const MARKET_OVERVIEW_LIMIT_MAX: u32 = 2_000;
/// Largest sub-account activity page: the `uint32.lte` rule on
/// `ListSubaccountEventsRequest.limit`.
pub(crate) const SUBACCOUNT_EVENTS_LIMIT_MAX: u32 = 200;
/// Largest `ListTriggers` and `ListTriggerEvents` page: the `uint32.lte` rule
/// on their `limit`.
pub(crate) const TRIGGER_LIST_LIMIT_MAX: u32 = 1_000;
/// Largest address-book entries, transfer destinations and internal-transfer
/// whitelist page: the `uint32.lte` rule on the `limit` of
/// `ListAddressBookEntriesRequest`, `ListTransferDestinationsRequest` and
/// `ListInternalTransferWhitelistEntriesRequest`.
pub(crate) const ADDRESS_BOOK_PAGE_LIMIT_MAX: u32 = 500;
/// Most counterparties `ListTransferCounterparties` returns, and most recent
/// destinations `GetAddressBookView` returns: the `uint32.lte` rule on
/// `ListTransferCounterpartiesRequest.limit` and
/// `GetAddressBookViewRequest.limit`.
pub(crate) const ADDRESS_BOOK_RECENT_LIMIT_MAX: u32 = 200;
/// Largest `ListFlows` and `ListFlowsByTx` page: the `uint32.lte` rule on
/// their `limit`.
pub(crate) const LIST_FLOWS_LIMIT_MAX: u32 = 500;
/// Largest `ListStatusEvents` page: the `uint32.lte` rule on
/// `ListStatusEventsRequest.limit`.
pub(crate) const STATUS_EVENTS_LIMIT_MAX: u32 = 200;
/// Largest changelog `ListPosts` and `ListDeploymentGroups` page: the
/// `uint32.lte` rule on their `limit`.
pub(crate) const CHANGELOG_PAGE_LIMIT_MAX: u32 = 100;
/// Largest changelog `ListChanges` page: the `uint32.lte` rule on
/// `ListChangesRequest.limit`.
pub(crate) const CHANGELOG_CHANGES_LIMIT_MAX: u32 = 500;

/// Most IDs one `ListAssetInfo` request takes: the `repeated.max_items` rule
/// on `ListAssetInfoRequest.asset_ids`.
pub(crate) const ASSET_INFO_IDS_MAX: usize = 1_000;
/// Largest asset ID `ListAssetInfo` takes: the `items.uint32.lte` rule on
/// `ListAssetInfoRequest.asset_ids` (with `gt: 0`).
pub(crate) const ASSET_INFO_ID_MAX: u32 = 2_147_483_647;

/// Refuse `ListAssetInfoRequest.asset_ids` outside its `repeated` rule: more
/// than [`ASSET_INFO_IDS_MAX`] IDs, or an ID of 0 or above
/// [`ASSET_INFO_ID_MAX`]. A repeated ID is the server's to ignore.
pub(crate) fn check_asset_info_ids(ids: &[u32]) -> Result<()> {
    if ids.len() > ASSET_INFO_IDS_MAX {
        return Err(Error::validation(format!(
            "asset_ids holds at most {ASSET_INFO_IDS_MAX} IDs, not {}",
            ids.len()
        )));
    }
    if let Some(id) = ids.iter().find(|id| **id == 0 || **id > ASSET_INFO_ID_MAX) {
        return Err(Error::validation(format!(
            "asset_ids must be from 1 to {ASSET_INFO_ID_MAX}, not {id}"
        )));
    }
    Ok(())
}

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

/// Refuse a page size outside `min..=max`, the contract's `gte: min, lte:
/// max` rule on a plain `uint32` `limit`, which is on the wire even at 0.
pub(crate) fn check_limit_range(operation: &str, limit: u32, min: u32, max: u32) -> Result<()> {
    if limit < min {
        return Err(Error::validation(format!(
            "{operation} limit {limit} is below the minimum {min}"
        )));
    }
    check_limit(operation, limit, max)
}

/// Refuse an explicit page size outside `1..=max`, the contract's
/// `gte: 1, lte: max` rule on an optional `limit`. `None` is left out, and
/// the server applies its default; `Some(0)` is never sent.
pub(crate) fn check_optional_limit(
    operation: &str,
    limit: Option<u32>,
    max: u32,
) -> Result<Option<u32>> {
    match limit {
        Some(0) => Err(Error::validation(format!(
            "{operation} limit must be positive when explicitly supplied"
        ))),
        Some(limit) => check_limit(operation, limit, max).map(|()| Some(limit)),
        None => Ok(None),
    }
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
        assert_docs(
            include_str!("../gen/buffa/marketdata.v1.rs"),
            &format!(
                "/// At most {ASSET_INFO_IDS_MAX} IDs. Duplicate and unknown IDs are ignored.\n    ///\n    /// Field 1: `asset_ids`\n"
            ),
        );
        let transfer = include_str!("../gen/buffa/transfer.v1.rs");
        assert_docs(
            transfer,
            &format!(
                "/// Transfers to move, in order. A batch holds at most {BATCH_TRANSFER_LEGS_MAX} legs, debiting at\n"
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

    /// The doc comment of `message`'s `limit` field in the generated
    /// `source`, one line.
    fn limit_doc(source: &str, message: &str) -> String {
        let start = source
            .find(&format!("pub struct {message} {{\n"))
            .unwrap_or_else(|| panic!("{message} is not generated"));
        let body = &source[start..];
        let field = body
            .find("    pub limit:")
            .unwrap_or_else(|| panic!("{message} has no limit"));
        let mut doc = Vec::new();
        for line in body[..field].lines().rev() {
            let line = line.trim();
            if let Some(text) = line.strip_prefix("///") {
                doc.push(text.trim());
            } else if line.starts_with("pub ") {
                break;
            }
        }
        doc.reverse();
        doc.retain(|line| !line.is_empty());
        doc.join(" ")
    }

    /// Every list call's page-size maximum is the one its request's
    /// generated field docs state, so a contract change that regenerates
    /// them breaks the build.
    #[test]
    fn page_size_limits_match_the_generated_contract_docs() {
        let orders = include_str!("../gen/buffa/orders.v1.rs");
        let marketdata = include_str!("../gen/buffa/marketdata.v1.rs");
        let ledger = include_str!("../gen/buffa/ledger.read.v1.rs");
        let overview = include_str!("../gen/buffa/marketoverview.v1.rs");
        let auth = include_str!("../gen/buffa/auth.v1.rs");
        let triggers = include_str!("../gen/buffa/triggers.v1.rs");
        let lifecycle = include_str!("../gen/buffa/chain.lifecycle.v1.rs");
        let system = include_str!("../gen/buffa/system.v1.rs");
        let changelog = include_str!("../gen/buffa/changelog.v1.rs");
        let order_read = format!("(1-{ORDER_READ_LIMIT_MAX}, default 100)");
        let maximum_is = |max: u32| format!("maximum is {max}.");
        for (source, message, text) in [
            (orders, "GetOpenOrdersRequest", order_read.clone()),
            (orders, "GetOrderHistoryRequest", order_read.clone()),
            (orders, "GetUserTradesRequest", order_read.clone()),
            (orders, "GetOrderRequest", order_read),
            (
                marketdata,
                "GetTradesRequest",
                format!("max {MARKET_TRADES_LIMIT_MAX}."),
            ),
            (
                marketdata,
                "GetCandlesRequest",
                format!("max {CANDLES_LIMIT_MAX}."),
            ),
            (
                marketdata,
                "GetCandlesColumnsRequest",
                format!("max {CANDLES_LIMIT_MAX}."),
            ),
            (
                marketdata,
                "GetOrderbookHeatmapRequest",
                format!("(min {HEATMAP_LIMIT_MIN}, max {HEATMAP_LIMIT_MAX})"),
            ),
            (
                ledger,
                "ListTransfersRequest",
                maximum_is(LIST_TRANSFERS_LIMIT_MAX),
            ),
            (
                overview,
                "ListMarketOverviewRequest",
                maximum_is(MARKET_OVERVIEW_LIMIT_MAX),
            ),
            (
                auth,
                "ListSubaccountEventsRequest",
                maximum_is(SUBACCOUNT_EVENTS_LIMIT_MAX),
            ),
            (
                triggers,
                "ListTriggersRequest",
                maximum_is(TRIGGER_LIST_LIMIT_MAX),
            ),
            (
                triggers,
                "ListTriggerEventsRequest",
                maximum_is(TRIGGER_LIST_LIMIT_MAX),
            ),
            (
                auth,
                "ListAddressBookEntriesRequest",
                maximum_is(ADDRESS_BOOK_PAGE_LIMIT_MAX),
            ),
            (
                auth,
                "ListTransferDestinationsRequest",
                maximum_is(ADDRESS_BOOK_PAGE_LIMIT_MAX),
            ),
            (
                auth,
                "ListInternalTransferWhitelistEntriesRequest",
                maximum_is(ADDRESS_BOOK_PAGE_LIMIT_MAX),
            ),
            (
                auth,
                "ListTransferCounterpartiesRequest",
                maximum_is(ADDRESS_BOOK_RECENT_LIMIT_MAX),
            ),
            (
                auth,
                "GetAddressBookViewRequest",
                maximum_is(ADDRESS_BOOK_RECENT_LIMIT_MAX),
            ),
            (
                lifecycle,
                "ListFlowsRequest",
                maximum_is(LIST_FLOWS_LIMIT_MAX),
            ),
            (
                lifecycle,
                "ListFlowsByTxRequest",
                maximum_is(LIST_FLOWS_LIMIT_MAX),
            ),
            (
                system,
                "ListStatusEventsRequest",
                format!("maximum {STATUS_EVENTS_LIMIT_MAX}."),
            ),
            (
                changelog,
                "ListPostsRequest",
                format!("maximum {CHANGELOG_PAGE_LIMIT_MAX}."),
            ),
            (
                changelog,
                "ListDeploymentGroupsRequest",
                format!("maximum {CHANGELOG_PAGE_LIMIT_MAX}."),
            ),
            (
                changelog,
                "ListChangesRequest",
                format!("maximum {CHANGELOG_CHANGES_LIMIT_MAX}."),
            ),
        ] {
            let doc = limit_doc(source, message);
            assert!(doc.contains(&text), "{message}: {doc:?} lacks {text:?}");
        }
    }

    /// The generated docs do not state these rules, so their values are
    /// pinned here: legs `min_items: 1`; idempotency keys `max_len: 128`;
    /// `quote_id` `max_len: 64`; `conversion_id` and the batch `request_id`
    /// `max_len: 39`; page tokens
    /// `max_len: 256` (dust) and `512` (statement exports); the statement
    /// export period `duration('8784h')`.
    #[test]
    fn limits_the_generated_docs_do_not_state_are_the_contract_rules() {
        assert_eq!(BATCH_TRANSFER_LEGS_MIN, 1);
        assert_eq!(IDEMPOTENCY_KEY_MAX_LEN, 128);
        assert_eq!(DUST_QUOTE_ID_MAX_LEN, 64);
        assert_eq!(DUST_CONVERSION_ID_MAX_LEN, 39);
        assert_eq!(BATCH_TRANSFER_REQUEST_ID_MAX_LEN, 39);
        assert_eq!(DUST_PAGE_TOKEN_MAX_LEN, 256);
        assert_eq!(STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN, 512);
        assert_eq!(STATEMENT_EXPORT_PERIOD_MAX_SECS, 366 * 24 * 3_600);
    }

    #[test]
    fn checks_refuse_only_outside_the_rule() {
        assert_eq!(check_optional_limit("op", None, 1000).unwrap(), None);
        assert_eq!(check_optional_limit("op", Some(1), 1000).unwrap(), Some(1));
        assert_eq!(
            check_optional_limit("op", Some(1000), 1000).unwrap(),
            Some(1000)
        );
        for limit in [0, 1001, u32::MAX] {
            assert!(matches!(
                check_optional_limit("op", Some(limit), 1000),
                Err(Error::Validation(_))
            ));
        }
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
