//! Account statements (`ledger.read.v1.StatementExportService`): a
//! whole-period statement of one account, produced in the background as one
//! CSV file.

use super::changelog::timestamp_field;
use super::limits::{
    LIST_STATEMENT_EXPORTS_LIMIT_MAX, STATEMENT_EXPORT_IDEMPOTENCY_KEY_MAX_LEN,
    STATEMENT_EXPORT_PAGE_TOKEN_MAX_LEN, STATEMENT_EXPORT_PERIOD_MAX_SECS, check_len, check_limit,
    check_page_token,
};
use super::{ServiceContext, scope, unary};
use crate::codecs::decode::{
    statement_export_download_url_from_proto, statement_export_from_cancel_response,
    statement_export_from_create_response, statement_export_from_get_response,
    statement_exports_list_from_proto,
};
use crate::codecs::scalars::parse_account_id;
use crate::connect::ledger::read::v1::StatementExportServiceClient;
use crate::errors::{Error, Result};
use crate::models::SubAccountScope;
use crate::models::{StatementExport, StatementExportDownloadUrl, StatementExportsList};
use crate::proto::ledger::read::v1::{
    CancelStatementExportRequest, CreateStatementExportRequest,
    GetStatementExportDownloadUrlRequest, GetStatementExportRequest, ListStatementExportsRequest,
};
use crate::transport::SharedTransport;
use std::time::SystemTime;

const CREATE_STATEMENT_EXPORT: &str =
    "/ledger.read.v1.StatementExportService/CreateStatementExport";
const GET_STATEMENT_EXPORT: &str = "/ledger.read.v1.StatementExportService/GetStatementExport";
const GET_STATEMENT_EXPORT_DOWNLOAD_URL: &str =
    "/ledger.read.v1.StatementExportService/GetStatementExportDownloadUrl";
const CANCEL_STATEMENT_EXPORT: &str =
    "/ledger.read.v1.StatementExportService/CancelStatementExport";
const LIST_STATEMENT_EXPORTS: &str = "/ledger.read.v1.StatementExportService/ListStatementExports";

/// What [`StatementsService::create_export`] asks for: a whole-period
/// statement file of one account, optionally one asset.
#[derive(Debug, Clone, Default)]
pub struct CreateStatementExportOptions {
    /// The account the statement covers (see [`SubAccountScope`]). With
    /// [`SubAccountScope::Default`] and no default configured, a key scoped
    /// to a sub-account reads that sub-account and other callers their main
    /// account. [`SubAccountScope::Id`] 0 is refused.
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

#[derive(Clone)]
pub struct StatementsService {
    ctx: ServiceContext,
}

impl StatementsService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn export_client(&self) -> StatementExportServiceClient<SharedTransport> {
        StatementExportServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    /// Create a whole-period statement file of one account, produced in the
    /// background, or return the export already created under the same
    /// idempotency key. Poll [`Self::get_export`] until its status is
    /// `ready`, or ends `failed`, `expired` or `canceled`; then download the
    /// file (see [`StatementExport`] and [`Self::get_export_download_url`]).
    ///
    /// Signed: an API key needs `READ_STATEMENTS` on its policy
    /// (`READ_BALANCES` does not grant it).
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

    /// A URL that downloads a `ready` export's file without credentials, for
    /// a browser to save it as `filename` (`statement-<export_id>.csv`). It
    /// works until `expires_at`, at most 60 seconds after the call; each call
    /// returns a new URL. Anyone holding it can download the file until
    /// then: never log, store or share it (the result's `Debug` redacts it).
    ///
    /// Refused by the API with `failed_precondition` and an
    /// [`crate::errors::ledger_codes`] code: `STATEMENT_EXPORT_NOT_READY`
    /// before the file is ready, `STATEMENT_EXPORT_FAILED` and
    /// `STATEMENT_EXPORT_CANCELED` for an export without a file, and
    /// `STATEMENT_EXPORT_EXPIRED` once the file expired or has less than 5
    /// seconds left. A reply without a URL is an
    /// [`Error::ResponseContract`].
    pub async fn get_export_download_url(
        &self,
        export_id: &str,
    ) -> Result<StatementExportDownloadUrl> {
        let req = GetStatementExportDownloadUrlRequest {
            export_id: parse_account_id(export_id, "export_id")?,
            ..Default::default()
        };
        let client = self.export_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            GET_STATEMENT_EXPORT_DOWNLOAD_URL,
            req,
            |req, opts| client.get_statement_export_download_url_with_options(req, opts),
        )
        .await?
        .into_owned();
        statement_export_download_url_from_proto(&resp)
    }

    /// Cancel a `queued` or `processing` export and return it, `canceled`.
    /// It stops at once: it no longer counts toward the main account's 5
    /// exports in progress, the account's next export can start, and any
    /// partial file is deleted. Canceling a `canceled` export returns it as
    /// it is; a finished one (`ready`, `failed` or `expired`) is refused with
    /// `failed_precondition` and
    /// [`crate::errors::ledger_codes::STATEMENT_EXPORT_NOT_CANCELABLE`].
    pub async fn cancel_export(&self, export_id: &str) -> Result<StatementExport> {
        let req = CancelStatementExportRequest {
            export_id: parse_account_id(export_id, "export_id")?,
            ..Default::default()
        };
        let client = self.export_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            CANCEL_STATEMENT_EXPORT,
            req,
            |req, opts| client.cancel_statement_export_with_options(req, opts),
        )
        .await?
        .into_owned();
        statement_export_from_cancel_response(&resp)
    }

    /// One page of an account's statement exports, newest first, with the
    /// main account's exports in progress and its limit. `limit` 0 asks for
    /// the server's default (20); above 100 is refused. Pass the previous
    /// page's `next_page_token` to continue; it is bound to the account.
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
