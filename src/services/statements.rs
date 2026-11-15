//! Account statements (`ledger.read.v1.LedgerReadService/ExportStatement`)
//! and whole-period statement files (`ledger.read.v1.StatementExportService`).

use super::changelog::timestamp_field;
use super::limits::{
    LIST_STATEMENT_EXPORTS_LIMIT_MAX, STATEMENT_EXPORT_IDEMPOTENCY_KEY_MAX_LEN,
    STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN, STATEMENT_EXPORT_PERIOD_MAX_SECS, check_len, check_limit,
    check_page_token,
};
use super::{ServiceContext, scope, unary};
use crate::codecs::decode::{
    statement_export_from_create_response, statement_export_from_get_response,
    statement_exports_list_from_proto,
};
use crate::codecs::scalars::parse_account_id;
use crate::connect::ledger::read::v1::{LedgerReadServiceClient, StatementExportServiceClient};
use crate::errors::{Error, Result};
use crate::models::SubAccountScope;
use crate::models::{Statement, StatementExport, StatementExportsList, StatementPage};
use crate::proto::ledger::read::v1::{
    CreateStatementExportRequest, ExportStatementRequest, GetStatementExportRequest,
    ListStatementExportsRequest,
};
use crate::transport::SharedTransport;
use std::collections::HashSet;
use std::time::SystemTime;

const EXPORT_STATEMENT: &str = "/ledger.read.v1.LedgerReadService/ExportStatement";
const CREATE_STATEMENT_EXPORT: &str =
    "/ledger.read.v1.StatementExportService/CreateStatementExport";
const GET_STATEMENT_EXPORT: &str = "/ledger.read.v1.StatementExportService/GetStatementExport";
const LIST_STATEMENT_EXPORTS: &str = "/ledger.read.v1.StatementExportService/ListStatementExports";

/// What [`StatementsService::create_export`] asks for: a whole-period
/// statement file of one account, optionally one asset.
#[derive(Debug, Clone, Default)]
pub struct CreateStatementExportOptions {
    /// The account (see [`SubAccountScope`]), chosen as for
    /// [`ExportStatementOptions::subaccount_id`].
    pub subaccount_id: SubAccountScope,
    /// Start of the period (UTC), inclusive. Required.
    pub start_time: Option<SystemTime>,
    /// End of the period (UTC), exclusive. Required: after `start_time` and
    /// at most 366 days after it. A period still open when the export is
    /// created ends then; see [`StatementExport::complete_through`].
    pub end_time: Option<SystemTime>,
    /// Numeric asset ID; `None` or `Some(0)` covers every asset.
    pub asset_id: Option<u32>,
    /// Required key, 1 to 64 letters, digits, `.`, `_`, `:` or `-`, unique per
    /// main account. Repeating a create with the same key returns the same
    /// export; the same key with other fields is refused
    /// (`ERROR_CODE_IDEMPOTENCY_KEY_REUSED`).
    pub idempotency_key: String,
}

/// What [`StatementsService::export_page`] and [`StatementsService::export`]
/// read: one account, optionally one asset, over a period of at most 366
/// days. The statement is CSV; there is no other format.
#[derive(Debug, Clone, Default)]
pub struct ExportStatementOptions {
    /// The account to export (see [`SubAccountScope`]). With
    /// [`SubAccountScope::Default`] and no default configured, a key scoped to a
    /// sub-account reads that sub-account and other callers their main
    /// account. [`SubAccountScope::Id`] 0 is refused.
    pub subaccount_id: SubAccountScope,
    /// Start of the period (UTC), inclusive. Required.
    pub start_time: Option<SystemTime>,
    /// End of the period (UTC), exclusive. Required. The statement covers
    /// the period up to its `complete_through`, which is at most `end_time`.
    pub end_time: Option<SystemTime>,
    /// Numeric asset ID (the spot config's `ledger_id`); `None` or `Some(0)`
    /// covers every asset.
    pub asset_id: Option<u32>,
    /// Continue from a previous page's `next_page_token`. It is bound to the
    /// account, sub-account, asset, period and `complete_through`: reuse it
    /// only with the same fields. A token issued before the statement carried
    /// `complete_through` is refused as invalid: restart from the first page.
    pub page_token: Option<String>,
}

#[derive(Clone)]
pub struct StatementsService {
    ctx: ServiceContext,
}

impl StatementsService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> LedgerReadServiceClient<SharedTransport> {
        LedgerReadServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn export_client(&self) -> StatementExportServiceClient<SharedTransport> {
        StatementExportServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn request(&self, opts: ExportStatementOptions) -> Result<ExportStatementRequest> {
        let required = |time: Option<SystemTime>, field: &str| {
            time.map(Some)
                .ok_or_else(|| Error::validation(format!("export_statement {field} is required")))
        };
        Ok(ExportStatementRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?,
            start_time: timestamp_field(required(opts.start_time, "start_time")?),
            end_time: timestamp_field(required(opts.end_time, "end_time")?),
            asset_id: opts.asset_id.unwrap_or(0),
            page_token: opts.page_token.unwrap_or_default(),
            ..Default::default()
        })
    }

    /// One page of an account statement as CSV text. A page holds at most
    /// 2000 ledger movements and never splits an entry; only the first page
    /// starts with the header row. Signed: an API key needs
    /// `READ_STATEMENTS` on its policy (`READ_BALANCES` does not grant it).
    pub async fn export_page(&self, opts: ExportStatementOptions) -> Result<StatementPage> {
        let req = self.request(opts)?;
        let client = self.client();
        let resp = unary::await_auth(&self.ctx.factory, EXPORT_STATEMENT, req, |req, opts| {
            client.export_statement_with_options(req, opts)
        })
        .await?
        .into_owned();
        Ok(StatementPage {
            csv: resp.csv,
            next_page_token: resp.next_page_token,
            complete_through: resp.complete_through.as_option().cloned(),
        })
    }

    /// The whole statement: every page from `opts.page_token` on, requested
    /// with the same fields and concatenated in order, so that from the first
    /// page the result starts with the header row. Read its rows with
    /// [`Statement::rows`]; its `complete_through` says how far it is
    /// complete. A next page token that repeats one this call already sent,
    /// or pages that disagree on `complete_through`, fail with a
    /// response-contract error.
    pub async fn export(&self, opts: ExportStatementOptions) -> Result<Statement> {
        let mut csv = String::new();
        let mut complete_through = None;
        let mut first = true;
        let mut requested = HashSet::new();
        let mut token = opts.page_token.clone().unwrap_or_default();
        loop {
            if !token.is_empty() {
                requested.insert(token.clone());
            }
            let page = self
                .export_page(ExportStatementOptions {
                    page_token: Some(token),
                    ..opts.clone()
                })
                .await?;
            if first {
                complete_through = page.complete_through;
                first = false;
            } else if page.complete_through != complete_through {
                return Err(Error::response_contract(
                    "statement pagination",
                    "pages of one statement disagree on complete_through",
                ));
            }
            csv.push_str(&page.csv);
            if page.next_page_token.is_empty() {
                return Ok(Statement {
                    csv,
                    complete_through,
                });
            }
            if requested.contains(&page.next_page_token) {
                return Err(Error::response_contract(
                    "statement pagination",
                    "next_page_token repeats an earlier page_token",
                ));
            }
            token = page.next_page_token;
        }
    }

    /// Create a whole-period statement file of one account, produced in the
    /// background, or return the export already created under the same
    /// idempotency key. Poll [`Self::get_export`] until its status is `ready`
    /// (or `failed`); download the file over REST (see [`StatementExport`]).
    ///
    /// Signed like [`Self::export_page`]: an API key needs `READ_STATEMENTS`.
    /// A main account has at most 5 exports `queued` or `processing`;
    /// another create is refused with `ERROR_CODE_STATEMENT_EXPORT_LIMIT_REACHED`
    /// (not retryable: wait for one to finish). Refused before sending: a
    /// missing start or end, an end not after the start or more than 366
    /// days after it, and an idempotency key that is empty, over 64
    /// characters or holds a character other than letters, digits, `.`, `_`,
    /// `:` or `-`.
    pub async fn create_export(
        &self,
        opts: CreateStatementExportOptions,
    ) -> Result<StatementExport> {
        const OP: &str = "create_statement_export";
        let start = opts
            .start_time
            .ok_or_else(|| Error::validation(format!("{OP} start_time is required")))?;
        let end = opts
            .end_time
            .ok_or_else(|| Error::validation(format!("{OP} end_time is required")))?;
        match end.duration_since(start) {
            Ok(period) if !period.is_zero() => {
                if period.as_secs() > STATEMENT_EXPORT_PERIOD_MAX_SECS
                    || (period.as_secs() == STATEMENT_EXPORT_PERIOD_MAX_SECS
                        && period.subsec_nanos() > 0)
                {
                    return Err(Error::validation(format!(
                        "{OP} end_time must be at most 366 days after start_time"
                    )));
                }
            }
            _ => {
                return Err(Error::validation(format!(
                    "{OP} end_time must be after start_time"
                )));
            }
        }
        check_len(
            OP,
            "idempotency_key",
            &opts.idempotency_key,
            STATEMENT_EXPORT_IDEMPOTENCY_KEY_MAX_LEN,
        )?;
        if !opts
            .idempotency_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
        {
            return Err(Error::validation(format!(
                "{OP} idempotency_key may hold only letters, digits, '.', '_', ':' and '-'"
            )));
        }
        let req = CreateStatementExportRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?,
            start_time: timestamp_field(Some(start)),
            end_time: timestamp_field(Some(end)),
            asset_id: opts.asset_id.unwrap_or(0),
            idempotency_key: opts.idempotency_key,
            ..Default::default()
        };
        let client = self.export_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            CREATE_STATEMENT_EXPORT,
            req,
            |req, opts| client.create_statement_export_with_options(req, opts),
        )
        .await?
        .into_owned();
        statement_export_from_create_response(&resp)
    }

    /// One statement export by its public ID: its status, progress and
    /// expiry.
    pub async fn get_export(&self, export_id: &str) -> Result<StatementExport> {
        let req = GetStatementExportRequest {
            export_id: parse_account_id(export_id, "export_id")?,
            ..Default::default()
        };
        let client = self.export_client();
        let resp = unary::await_auth(&self.ctx.factory, GET_STATEMENT_EXPORT, req, |req, opts| {
            client.get_statement_export_with_options(req, opts)
        })
        .await?
        .into_owned();
        statement_export_from_get_response(&resp)
    }

    /// One page of an account's statement exports, newest first. `limit` 0
    /// asks for the server's default (20); above 100 is refused. Pass the
    /// previous page's `next_page_token` to continue; it is bound to the
    /// account.
    pub async fn list_exports(
        &self,
        subaccount_id: SubAccountScope,
        limit: u32,
        page_token: Option<&str>,
    ) -> Result<StatementExportsList> {
        const OP: &str = "list_statement_exports";
        check_limit(OP, limit, LIST_STATEMENT_EXPORTS_LIMIT_MAX)?;
        let page_token = page_token.unwrap_or_default();
        check_page_token(OP, page_token, STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN)?;
        let req = ListStatementExportsRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, subaccount_id)?,
            limit,
            page_token: page_token.to_owned(),
            ..Default::default()
        };
        let client = self.export_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            LIST_STATEMENT_EXPORTS,
            req,
            |req, opts| client.list_statement_exports_with_options(req, opts),
        )
        .await?
        .into_owned();
        statement_exports_list_from_proto(&resp)
    }
}
