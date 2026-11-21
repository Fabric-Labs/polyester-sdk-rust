//! Profit and loss (`pnl.v1.ProfitAndLossService`): the profit and loss of
//! the main account or one sub-account, in USD.

use super::changelog::{paginate, timestamp_field};
use super::limits::{
    PNL_DAILY_LIMIT_MAX, PNL_DAILY_PERIOD_MAX_SECS, PNL_PAGE_TOKEN_MAX_LEN, check_limit,
    check_page_token,
};
use super::{ServiceContext, scope, unary};
use crate::codecs::decode::{
    daily_profit_and_loss_page_from_proto, profit_and_loss_from_proto,
    trading_statistics_from_proto,
};
use crate::connect::pnl::v1::ProfitAndLossServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    DailyProfitAndLoss, DailyProfitAndLossPage, ProfitAndLoss, ProfitAndLossProduct,
    ProfitAndLossWindow, SubAccountScope, TradingStatistics,
};
use crate::proto::pnl::v1::{
    GetProfitAndLossRequest, GetTradingStatisticsRequest, ListDailyProfitAndLossRequest,
    ProfitAndLossProduct as ProtoProduct, ProfitAndLossWindow as ProtoWindow,
};
use crate::transport::SharedTransport;
use futures_util::Stream;
use std::time::SystemTime;

const GET_PROFIT_AND_LOSS: &str = "/pnl.v1.ProfitAndLossService/GetProfitAndLoss";
const GET_TRADING_STATISTICS: &str = "/pnl.v1.ProfitAndLossService/GetTradingStatistics";
const LIST_DAILY_PROFIT_AND_LOSS: &str = "/pnl.v1.ProfitAndLossService/ListDailyProfitAndLoss";

/// What [`ProfitAndLossService::get`] and
/// [`ProfitAndLossService::get_trading_statistics`] read: the account, the
/// window and optional product and market filters.
#[derive(Debug, Clone)]
pub struct ProfitAndLossOptions {
    /// The period to report.
    pub window: ProfitAndLossWindow,
    /// The account to read. Profit and loss has no main-account-only read:
    /// a read either names one sub-account or names none, and one that names
    /// none returns the caller's whole account.
    ///
    /// - [`SubAccountScope::Id`] reads that sub-account; a main-account key
    ///   may name a sub-account its main account owns. 0 is refused before
    ///   sending.
    /// - [`SubAccountScope::Main`] names no sub-account, so it does **not**
    ///   read the main account alone: a session or main-account key reads
    ///   the main account together with every sub-account it owns, and a
    ///   sub-account key reads its own sub-account.
    /// - [`SubAccountScope::Default`] reads the configured default
    ///   sub-account, or names none as [`SubAccountScope::Main`] does when no
    ///   default is configured.
    pub subaccount_id: SubAccountScope,
    /// Only this product; `None` reads every product.
    pub product: Option<ProfitAndLossProduct>,
    /// Only this market, such as `BTC-USDT`. Set at most one of `symbol` and
    /// `symbol_id`; neither reads every market. A blank name is refused,
    /// never read as every market.
    /// [`ProfitAndLossService::get`] limits only its `markets` to it, not
    /// its totals or holdings.
    pub symbol: Option<String>,
    /// Only this market, by symbol ID; non-zero.
    pub symbol_id: Option<u32>,
}

impl ProfitAndLossOptions {
    /// Read `window` for the default account, every product and market.
    pub fn new(window: ProfitAndLossWindow) -> Self {
        Self {
            window,
            subaccount_id: SubAccountScope::Default,
            product: None,
            symbol: None,
            symbol_id: None,
        }
    }
}

/// What [`ProfitAndLossService::list_daily`] reads: a range of UTC days of
/// one account.
#[derive(Debug, Clone, Default)]
pub struct ListDailyProfitAndLossOptions {
    /// The account to read, as for [`ProfitAndLossOptions::subaccount_id`]:
    /// [`SubAccountScope::Main`] names no sub-account and reads the caller's
    /// whole account, never the main account alone.
    pub subaccount_id: SubAccountScope,
    /// The first day, inclusive: 00:00 UTC of a day. Required.
    pub start_time: Option<SystemTime>,
    /// The day after the last one, exclusive: 00:00 UTC of a day. Required:
    /// after `start_time` and at most 366 days after it.
    pub end_time: Option<SystemTime>,
    /// Days per page; 0 asks for the server's default (31). At most 366.
    pub limit: u32,
    /// Continues from an earlier page's `next_page_token`, with the same
    /// account and days.
    pub page_token: Option<String>,
}

/// Profit and loss of the caller's whole account or one sub-account, in USD.
///
/// A read names one sub-account or none. Named, it reads that sub-account.
/// Unnamed, a session or main-account key reads the main account together
/// with every sub-account it owns, and a sub-account key reads its own
/// sub-account; there is no main-account-only read (see
/// [`ProfitAndLossOptions::subaccount_id`]).
///
/// Spot PnL uses the weighted average cost of each asset: every trade is
/// valued at its execution time, every fee is a realized expense, and
/// deposits, withdrawals and transfers are never PnL (deposited coins have
/// no known cost; a transfer between the main account and its sub-accounts
/// carries its cost). Figures follow settled trades and transfers within
/// seconds; `as_of` says when they were last published.
///
/// Every read is signed and needs `READ_PNL` on the API key's policy. The
/// server refuses an invalid window or sub-account with `invalid_argument`,
/// an account the key cannot read with `permission_denied` or `not_found`,
/// and answers `unavailable` when the same request can be retried.
#[derive(Clone)]
pub struct ProfitAndLossService {
    ctx: ServiceContext,
}

impl ProfitAndLossService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> ProfitAndLossServiceClient<SharedTransport> {
        ProfitAndLossServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    /// The account, window, product and market of a read, as the contract
    /// spells them.
    async fn selection(
        &self,
        opts: &ProfitAndLossOptions,
    ) -> Result<(Option<u64>, ProtoWindow, ProtoProduct, u32)> {
        // A blank name would resolve to no market and so read every market,
        // or slip past the one-selector rule beside a symbol ID: refuse it.
        if let Some(symbol) = opts.symbol.as_deref() {
            if symbol.trim().is_empty() {
                return Err(Error::validation(
                    "pnl symbol must not be blank; leave it None to read every market",
                ));
            }
            if opts.symbol_id.is_some() {
                return Err(Error::validation(
                    "pnl accepts symbol or symbol_id, not both",
                ));
            }
        }
        let subaccount_id = scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?;
        if opts.symbol.is_some() {
            self.ctx.wait_for_catalogs().await?;
        }
        let symbol_id = self
            .ctx
            .catalogs
            .optional_symbol_id(opts.symbol.as_deref(), opts.symbol_id)?;
        Ok((
            subaccount_id,
            window_to_proto(opts.window),
            product_to_proto(opts.product),
            symbol_id,
        ))
    }

    /// The account's PnL over a window (`GetProfitAndLoss`): realized PnL
    /// after fees, the change in unrealized PnL, fees and volume, by product
    /// and by market, and every asset held now with its cost, entry price,
    /// mark and unrealized PnL. A figure that needs an unavailable price is
    /// `None`, never `"0"`, and `notices` says why.
    pub async fn get(&self, opts: ProfitAndLossOptions) -> Result<ProfitAndLoss> {
        let (subaccount_id, window, product, symbol_id) = self.selection(&opts).await?;
        let req = GetProfitAndLossRequest {
            subaccount_id,
            window: window.into(),
            product: product.into(),
            symbol_id,
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(&self.ctx.factory, GET_PROFIT_AND_LOSS, req, |req, opts| {
            client.get_profit_and_loss_with_options(req, opts)
        })
        .await?
        .into_owned();
        Ok(profit_and_loss_from_proto(&resp))
    }

    /// The account's trade count, volume, fees and realized PnL over a
    /// window, with the time-weighted return, maximum drawdown and Sharpe
    /// ratio of its closed UTC days (`GetTradingStatistics`). The return
    /// figures cover the whole account, whatever the product and market
    /// filters select.
    pub async fn get_trading_statistics(
        &self,
        opts: ProfitAndLossOptions,
    ) -> Result<TradingStatistics> {
        let (subaccount_id, window, product, symbol_id) = self.selection(&opts).await?;
        let req = GetTradingStatisticsRequest {
            subaccount_id,
            window: window.into(),
            product: product.into(),
            symbol_id,
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            GET_TRADING_STATISTICS,
            req,
            |req, opts| client.get_trading_statistics_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(trading_statistics_from_proto(&resp))
    }

    /// One page of the account's PnL per UTC day, oldest first
    /// (`ListDailyProfitAndLoss`). Every day of the range is listed, days
    /// without activity included. Pass the page's `next_page_token` to
    /// continue, or read every day with [`Self::daily`].
    ///
    /// Refused before sending: a missing `start_time` or `end_time`, a bound
    /// that is not 00:00 UTC of a day, an end not after the start or more
    /// than 366 days after it, a `limit` above 366 and a page token over 512
    /// characters.
    pub async fn list_daily(
        &self,
        opts: ListDailyProfitAndLossOptions,
    ) -> Result<DailyProfitAndLossPage> {
        let req = self.encode_list_daily(opts)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            LIST_DAILY_PROFIT_AND_LOSS,
            req,
            |req, opts| client.list_daily_profit_and_loss_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(daily_profit_and_loss_page_from_proto(&resp))
    }

    /// Every day of `opts` from `opts.page_token` (or the first day) on,
    /// oldest first, fetching pages as the stream is polled. The stream ends
    /// at the first error.
    pub fn daily(
        &self,
        opts: ListDailyProfitAndLossOptions,
    ) -> impl Stream<Item = Result<DailyProfitAndLoss>> + Send + 'static {
        let svc = self.clone();
        let first_token = opts.page_token.clone();
        paginate("pnl daily pagination", first_token, move |page_token| {
            let svc = svc.clone();
            let opts = ListDailyProfitAndLossOptions {
                page_token: Some(page_token),
                ..opts.clone()
            };
            async move {
                let page = svc.list_daily(opts).await?;
                Ok((page.days, page.next_page_token))
            }
        })
    }

    fn encode_list_daily(
        &self,
        opts: ListDailyProfitAndLossOptions,
    ) -> Result<ListDailyProfitAndLossRequest> {
        const OP: &str = "pnl.list_daily";
        let start = opts
            .start_time
            .ok_or_else(|| Error::validation(format!("{OP} start_time is required")))?;
        let end = opts
            .end_time
            .ok_or_else(|| Error::validation(format!("{OP} end_time is required")))?;
        for (name, bound) in [("start_time", start), ("end_time", end)] {
            check_utc_midnight(OP, name, bound)?;
        }
        match end.duration_since(start) {
            Ok(period) if !period.is_zero() => {
                if period.as_secs() > PNL_DAILY_PERIOD_MAX_SECS
                    || (period.as_secs() == PNL_DAILY_PERIOD_MAX_SECS && period.subsec_nanos() > 0)
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
        check_limit(OP, opts.limit, PNL_DAILY_LIMIT_MAX)?;
        let page_token = opts.page_token.unwrap_or_default();
        check_page_token(OP, &page_token, PNL_PAGE_TOKEN_MAX_LEN)?;
        Ok(ListDailyProfitAndLossRequest {
            subaccount_id: scope::resolve_subaccount(&self.ctx, opts.subaccount_id)?,
            start_time: timestamp_field(Some(start)),
            end_time: timestamp_field(Some(end)),
            limit: opts.limit,
            page_token,
            ..Default::default()
        })
    }
}

/// Earliest and latest seconds a `google.protobuf.Timestamp` holds:
/// 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z.
const TIMESTAMP_MIN_SECS: i64 = -62_135_596_800;
const TIMESTAMP_MAX_SECS: i64 = 253_402_300_799;

/// Refuse a day bound that is not 00:00 UTC of a day, or that a protobuf
/// timestamp cannot hold.
fn check_utc_midnight(op: &str, name: &str, time: SystemTime) -> Result<()> {
    let (seconds, nanos) = match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => (
            i64::try_from(after.as_secs()).unwrap_or(i64::MAX),
            after.subsec_nanos(),
        ),
        Err(before) => {
            let before = before.duration();
            let seconds = i64::try_from(before.as_secs()).map_or(i64::MIN, |s| -s);
            match before.subsec_nanos() {
                0 => (seconds, 0),
                nanos => (seconds.saturating_sub(1), 1_000_000_000 - nanos),
            }
        }
    };
    if !(TIMESTAMP_MIN_SECS..=TIMESTAMP_MAX_SECS).contains(&seconds) {
        return Err(Error::validation(format!(
            "{op} {name} is outside the timestamp range"
        )));
    }
    if nanos != 0 || seconds.rem_euclid(86_400) != 0 {
        return Err(Error::validation(format!(
            "{op} {name} must be 00:00 UTC of a day"
        )));
    }
    Ok(())
}

fn window_to_proto(window: ProfitAndLossWindow) -> ProtoWindow {
    match window {
        ProfitAndLossWindow::Today => ProtoWindow::PROFIT_AND_LOSS_WINDOW_TODAY,
        ProfitAndLossWindow::Days7 => ProtoWindow::PROFIT_AND_LOSS_WINDOW_DAYS_7,
        ProfitAndLossWindow::Days30 => ProtoWindow::PROFIT_AND_LOSS_WINDOW_DAYS_30,
        ProfitAndLossWindow::Lifetime => ProtoWindow::PROFIT_AND_LOSS_WINDOW_LIFETIME,
    }
}

fn product_to_proto(product: Option<ProfitAndLossProduct>) -> ProtoProduct {
    match product {
        None => ProtoProduct::PROFIT_AND_LOSS_PRODUCT_UNSPECIFIED,
        Some(ProfitAndLossProduct::Spot) => ProtoProduct::PROFIT_AND_LOSS_PRODUCT_SPOT,
    }
}
