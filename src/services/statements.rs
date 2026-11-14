//! Account statement exports (`ledger.read.v1.LedgerReadService/ExportStatement`).

use super::changelog::timestamp_field;
use super::{ServiceContext, scope, unary};
use crate::connect::ledger::read::v1::LedgerReadServiceClient;
use crate::errors::{Error, Result};
use crate::models::{Statement, StatementPage};
use crate::proto::ledger::read::v1::ExportStatementRequest;
use crate::transport::SharedTransport;
use std::collections::HashSet;
use std::time::SystemTime;

const EXPORT_STATEMENT: &str = "/ledger.read.v1.LedgerReadService/ExportStatement";

/// What [`StatementsService::export_page`] and [`StatementsService::export`]
/// read: one account, optionally one asset, over a period of at most 366
/// days. The statement is CSV; there is no other format.
#[derive(Debug, Clone, Default)]
pub struct ExportStatementOptions {
    /// Public sub-account ID, decoded. `None` takes the configured default
    /// sub-account, as the other ledger reads do; with no default, a key
    /// scoped to a sub-account reads that sub-account and other callers
    /// their main account. `Some(0)` is refused.
    pub subaccount_id: Option<u64>,
    /// Start of the period (UTC), inclusive. Required.
    pub start_time: Option<SystemTime>,
    /// End of the period (UTC), exclusive. Required. A period that has not
    /// ended yet covers the movements up to the time of the request.
    pub end_time: Option<SystemTime>,
    /// Numeric asset ID (the spot config's `ledger_id`); `None` or `Some(0)`
    /// covers every asset.
    pub asset_id: Option<u32>,
    /// Continue from a previous page's `next_page_token`. It is bound to the
    /// account, sub-account, asset and period: reuse it only with the same
    /// fields.
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

    fn request(&self, opts: ExportStatementOptions) -> Result<ExportStatementRequest> {
        let required = |time: Option<SystemTime>, field: &str| {
            time.map(Some)
                .ok_or_else(|| Error::validation(format!("export_statement {field} is required")))
        };
        Ok(ExportStatementRequest {
            subaccount_id: scope::optional_subaccount(&self.ctx, opts.subaccount_id)?,
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
        })
    }

    /// The whole statement: every page from `opts.page_token` on, requested
    /// with the same fields and concatenated in order, so that from the first
    /// page the result starts with the header row. Read its rows with
    /// [`Statement::rows`]. A next page token that repeats one this call
    /// already sent fails with a response-contract error instead of looping.
    pub async fn export(&self, opts: ExportStatementOptions) -> Result<Statement> {
        let mut csv = String::new();
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
            csv.push_str(&page.csv);
            if page.next_page_token.is_empty() {
                return Ok(Statement { csv });
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
}
