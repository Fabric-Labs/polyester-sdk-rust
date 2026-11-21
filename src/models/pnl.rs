//! Profit and loss models (`pnl.v1.ProfitAndLossService`).
//!
//! Figures are in USD. Every amount and quantity is an exact base-10 decimal
//! string, signed where its field says so: parse it with a decimal type such
//! as `rust_decimal::Decimal`, never a float. An optional figure is `None`
//! when a price it needs is not available, which is distinct from `"0"`;
//! `notices` then says what the figures leave out.
//!
//! Notices are the contract's `ProfitAndLossNotice` values without their
//! prefix, in lower case: `price_unavailable`, `mark_unavailable`,
//! `unclassified_transfer`, `quantity_shortfall`, `equity_unavailable`,
//! `flow_unvalued` and `sharpe_insufficient_days`; a value newer than this
//! SDK reads `unknown(<n>)`. Products read `spot`.

use buffa_types::google::protobuf::Timestamp;

/// The period a profit and loss read covers. Every window ends now and
/// starts at 00:00 UTC of a day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProfitAndLossWindow {
    /// Since 00:00 UTC today.
    Today,
    /// Today and the 6 days before it.
    Days7,
    /// Today and the 29 days before it.
    Days30,
    /// Since 00:00 UTC of the day of the account's (or the selected
    /// sub-account's) first trade or transfer.
    Lifetime,
}

/// A product a profit and loss read can be limited to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProfitAndLossProduct {
    Spot,
}

/// The account's profit and loss over a window
/// ([`crate::services::ProfitAndLossService::get`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ProfitAndLoss {
    /// Start of the window (UTC), inclusive.
    pub window_start: Option<Timestamp>,
    /// When the figures were last published. Trades and transfers settled
    /// shortly before may not be included yet.
    pub as_of: Option<Timestamp>,
    /// Time of the prices that value the holdings now: the close of the
    /// latest complete minute.
    pub mark_time: Option<Timestamp>,
    /// `realized_pnl_usd` plus the change in unrealized PnL over the window,
    /// with deposits, withdrawals and transfers neutral. Signed.
    pub pnl_usd: Option<String>,
    /// Realized PnL of the window's trades, after their fees. Signed.
    pub realized_pnl_usd: String,
    /// Change in unrealized PnL from the window's start to now, net of
    /// transfers in and out. Signed.
    pub unrealized_pnl_change_usd: Option<String>,
    /// Unrealized PnL of the holdings now. Signed.
    pub unrealized_pnl_usd: Option<String>,
    /// Fees paid net of rebates; negative when rebates exceed fees.
    pub fees_usd: String,
    /// Traded volume, non-negative.
    pub volume_usd: String,
    /// PnL by product.
    pub products: Vec<ProductProfitAndLoss>,
    /// Realized PnL by market, ordered by symbol ID. Limited to one market
    /// when the read names one; the totals and holdings never are.
    pub markets: Vec<MarketProfitAndLoss>,
    /// Assets held now, ordered by asset ID.
    pub holdings: Vec<HoldingProfitAndLoss>,
    /// What the figures leave out, if anything (see the module docs).
    pub notices: Vec<String>,
}

/// The profit and loss of one product over the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductProfitAndLoss {
    /// `spot`; `unknown(<n>)` for a product newer than this SDK.
    pub product: String,
    /// Realized PnL after fees. Signed.
    pub realized_pnl_usd: String,
    /// Fees paid net of rebates. Signed.
    pub fees_usd: String,
    /// Traded volume, non-negative.
    pub volume_usd: String,
}

/// The realized side of one spot market over the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketProfitAndLoss {
    pub symbol_id: u32,
    /// `spot`; `unknown(<n>)` for a product newer than this SDK.
    pub product: String,
    /// Realized PnL of the market's trades after their fees: each sale
    /// realizes its proceeds minus the average cost of what it sold, and
    /// each fee is a realized expense. Signed.
    pub realized_pnl_usd: String,
    /// Fees paid net of rebates. Signed.
    pub fees_usd: String,
    /// Traded volume, valued at execution time; non-negative.
    pub volume_usd: String,
    /// The account's fills in the market.
    pub trade_count: u64,
}

/// One asset the account holds now. Quantities are in whole units of the
/// asset, non-negative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldingProfitAndLoss {
    pub asset_id: u32,
    /// Quantity held: the quantity with a known cost plus
    /// `untracked_quantity`.
    pub position_quantity: String,
    /// Quantity whose cost is unknown, such as deposited coins. It never
    /// counts toward PnL.
    pub untracked_quantity: String,
    /// Cost of the quantity with a known cost, fees in the asset included;
    /// non-negative.
    pub cost_usd: String,
    /// Average entry price per unit; `None` when no quantity has a known
    /// cost.
    pub entry_price_usd: Option<String>,
    /// Current price per unit used for unrealized PnL; `None` when not
    /// available.
    pub mark_price_usd: Option<String>,
    /// Unrealized PnL: the quantity with a known cost at `mark_price_usd`,
    /// minus `cost_usd`. Signed; `None` when `mark_price_usd` is.
    pub unrealized_pnl_usd: Option<String>,
}

/// One UTC day of an account.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyProfitAndLoss {
    /// The day, at 00:00 UTC.
    pub day: Option<Timestamp>,
    /// `realized_pnl_usd` plus the change in unrealized PnL from the previous
    /// day's close to this day's close, with transfers neutral. Signed.
    pub pnl_usd: Option<String>,
    /// Realized PnL of the day's trades, after fees. Signed.
    pub realized_pnl_usd: String,
    /// Unrealized PnL of the holdings at the day's close; `None` when a price
    /// it needs is not available, as for today before it closes.
    pub unrealized_pnl_at_close_usd: Option<String>,
    /// Fees paid net of rebates. Signed.
    pub fees_usd: String,
    /// Traded volume, non-negative.
    pub volume_usd: String,
    /// The account's fills in the day.
    pub trade_count: u64,
    /// What the day's figures leave out, if anything (see the module docs).
    pub notices: Vec<String>,
    /// Equity at the 00:00 UTC that starts the day: everything the account
    /// owns, funds reserved by open orders and pending withdrawals included,
    /// valued at that time's prices. It equals the previous day's
    /// `end_equity_usd`; non-negative. `None` with `equity_unavailable` when a
    /// price it needs is not available, or after a day with
    /// `unclassified_transfer` or `quantity_shortfall`.
    pub start_equity_usd: Option<String>,
    /// Equity at the 00:00 UTC that ends the day, valued as
    /// `start_equity_usd`; also `None` for today until the day closes.
    pub end_equity_usd: Option<String>,
    /// Deposits and transfers in minus withdrawals and transfers out, each
    /// valued when it settled. Transfers between the main account and its own
    /// sub-accounts count for a single sub-account and cancel out for the
    /// main account. Signed.
    pub net_flow_usd: Option<String>,
}

/// One page of days ([`crate::services::ProfitAndLossService::list_daily`]),
/// oldest first. Every day of the range is listed, days without activity
/// included.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyProfitAndLossPage {
    pub days: Vec<DailyProfitAndLoss>,
    /// Continues the list with the same account and days; empty on the last
    /// page.
    pub next_page_token: String,
    /// When the figures were last published.
    pub as_of: Option<Timestamp>,
}

/// The account's trading over a window
/// ([`crate::services::ProfitAndLossService::get_trading_statistics`]).
///
/// The trade count, volume, fees and realized PnL cover the window, today
/// included, and follow its market and product filters. The return figures
/// cover closed UTC days only (the last 7 or 30 for `Days7` and `Days30`,
/// every closed day for `Lifetime`, none for `Today`) and the whole account,
/// whatever the filters select.
#[derive(Debug, Clone, PartialEq)]
pub struct TradingStatistics {
    /// Start of the window (UTC), inclusive.
    pub window_start: Option<Timestamp>,
    /// When the figures were last published.
    pub as_of: Option<Timestamp>,
    /// The account's fills in the window.
    pub trade_count: u64,
    /// Traded volume, non-negative.
    pub volume_usd: String,
    /// Fees paid net of rebates. Signed.
    pub fees_usd: String,
    /// Realized PnL of the window's trades, after fees. Signed.
    pub realized_pnl_usd: String,
    /// What the figures leave out, if anything (see the module docs).
    pub notices: Vec<String>,
    /// Time-weighted return in percent (`"1.25"` is 1.25%), its daily returns
    /// compounded so deposits and withdrawals are not gains or losses.
    /// Signed. `None` when a day's equity or net flow is not available, or
    /// the window has no closed day.
    pub return_pct: Option<String>,
    /// Largest fall in percent of the compounded return from a previous peak,
    /// at day closes; non-negative. `None` as `return_pct` is.
    pub max_drawdown_pct: Option<String>,
    /// Annualized Sharpe ratio of the daily returns, a plain ratio. Signed.
    /// `None` below 7 daily returns (notice `sharpe_insufficient_days`), when
    /// the returns do not vary, or as `return_pct` is.
    pub sharpe: Option<String>,
    /// The daily returns the return figures are based on: the window's closed
    /// days from the account's first deposit on. Show the Sharpe ratio as
    /// based on this many days.
    pub return_days: u32,
}
