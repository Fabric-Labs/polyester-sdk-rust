//! Recovering trades gaps from the trade history API by trade ID.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use super::trades::{epoch_base, epoch_of};
use super::{Side, Trade, TradesGap};
use crate::errors::{Error, Result};
use crate::models::GetTradesOpts;
use crate::services::MarketDataService;

/// A boxed future of a [`TradeHistory`] call.
pub type HistoryFuture<'a> = Pin<Box<dyn Future<Output = Result<HistoryPage>> + Send + 'a>>;

/// Reads a pair's public trades gap-free by trade ID, as the trade history
/// API's after-trade-ID walk does. [`MarketDataService`] implements it.
pub trait TradeHistory: Send + Sync {
    /// The pair's trades with an ID above `after_trade_id`, ascending, up to
    /// `limit`.
    fn trades_after(&self, symbol_id: u32, after_trade_id: u64, limit: u32) -> HistoryFuture<'_>;
}

/// One page of a gap-free trade walk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryPage {
    pub trades: Vec<Trade>,
    /// Every trade with an ID at or below it exists and is returned by the
    /// walk.
    pub complete_through_trade_id: u64,
    /// The cursor of the next call; equal to the request's, nothing newer is
    /// available yet.
    pub next_after_trade_id: u64,
    /// Every book epoch that ended between the request's cursor and
    /// `next_after_trade_id`, both inclusive.
    pub epoch_ends: Vec<EpochEnd>,
}

/// A book epoch's last trade ID, or the epoch's base when it ended without
/// trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEnd {
    pub epoch: u64,
    pub last_trade_id: u64,
}

impl TradeHistory for MarketDataService {
    fn trades_after(&self, symbol_id: u32, after_trade_id: u64, limit: u32) -> HistoryFuture<'_> {
        Box::pin(async move {
            let res = self
                .get_trades_with(GetTradesOpts {
                    symbol_id: Some(symbol_id),
                    limit: Some(limit),
                    after_trade_id: Some(after_trade_id),
                    ..Default::default()
                })
                .await?;
            let mut page = HistoryPage {
                complete_through_trade_id: res.complete_through_trade_id,
                next_after_trade_id: res.next_after_trade_id,
                epoch_ends: res
                    .epoch_ends
                    .iter()
                    .map(|e| EpochEnd {
                        epoch: e.epoch,
                        last_trade_id: e.last_trade_id,
                    })
                    .collect(),
                trades: Vec::with_capacity(res.trades.len()),
            };
            for t in res.trades {
                let trade_id = t.match_id.parse::<u64>().map_err(|_| {
                    Error::response_contract(
                        "get_trades",
                        format!("match_id {:?} is not a trade ID", t.match_id),
                    )
                })?;
                let (Some(price), Some(qty)) = (t.price, t.qty) else {
                    return Err(Error::response_contract(
                        "get_trades",
                        format!("trade {trade_id} without a price or quantity"),
                    ));
                };
                let taker_side = match t.side.as_str() {
                    "buy" => Side::Buy,
                    "sell" => Side::Sell,
                    _ => Side::Unknown,
                };
                page.trades.push(Trade {
                    trade_id,
                    price,
                    qty,
                    taker_side,
                });
            }
            Ok(page)
        })
    }
}

const PAGE_LIMIT: u32 = 1000;
const FETCH_ATTEMPTS: u32 = 10;

/// Retry pacing of a walk: a delay doubling from the first to the last.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Pacing {
    pub first: Duration,
    pub max: Duration,
}

impl Default for Pacing {
    fn default() -> Self {
        Self {
            first: Duration::from_millis(100),
            max: Duration::from_secs(2),
        }
    }
}

/// Recovers the trades of a gap from history: it walks the pair's trades by ID
/// from `gap.after_trade_id` until it holds every trade through
/// `gap.through_trade_id`, or, for an epoch tail, through the epoch's last
/// trade. It returns the gap's trades in ascending ID order.
///
/// A page without progress is retried with backoff for as long as it takes:
/// the history lags the live stream. A history call that fails is retried the
/// same way, up to ten times in a row. A page that breaks the walk's contract
/// (a trade that does not continue the cursor, or a cursor that moves
/// backwards or skips trades) is [`Error::ResponseContract`].
pub async fn backfill(history: &dyn TradeHistory, gap: TradesGap) -> Result<Vec<Trade>> {
    backfill_paced(history, gap, Pacing::default()).await
}

pub(crate) async fn backfill_paced(
    history: &dyn TradeHistory,
    gap: TradesGap,
    pacing: Pacing,
) -> Result<Vec<Trade>> {
    if gap.through_trade_id == 0 && gap.tail_epoch == 0 {
        return Err(Error::validation(
            "trades gap has neither a last trade ID nor a tail epoch",
        ));
    }
    let mut walk = Walk {
        cursor: gap.after_trade_id,
        ends: HashMap::new(),
        trades: Vec::new(),
    };
    let mut delay = pacing.first;
    let mut failures = 0;
    while !walk.done(&gap) {
        match history
            .trades_after(gap.symbol_id, walk.cursor, PAGE_LIMIT)
            .await
        {
            Err(err) => {
                failures += 1;
                if failures >= FETCH_ATTEMPTS {
                    return Err(err);
                }
            }
            Ok(page) => {
                failures = 0;
                let before = walk.cursor;
                walk.take(page, &gap)
                    .map_err(|m| Error::response_contract("trade history backfill", m))?;
                if walk.cursor != before {
                    delay = pacing.first;
                    continue;
                }
            }
        }
        if walk.done(&gap) {
            break;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(pacing.max);
    }
    Ok(walk.trades)
}

struct Walk {
    cursor: u64,
    ends: HashMap<u64, u64>,
    trades: Vec<Trade>,
}

impl Walk {
    fn done(&self, gap: &TradesGap) -> bool {
        if gap.tail_epoch != 0 {
            if let Some(last) = self.ends.get(&gap.tail_epoch)
                && *last <= self.cursor
            {
                return true;
            }
            return epoch_of(self.cursor) > gap.tail_epoch;
        }
        self.cursor >= gap.through_trade_id
    }

    /// Whether the epoch ends seen close the cursor's epoch at the cursor and
    /// every epoch before `target_epoch` empty.
    fn closed_through(&self, cursor: u64, target_epoch: u64) -> bool {
        let from = epoch_of(cursor);
        if self.ends.get(&from) != Some(&cursor) {
            return false;
        }
        (from + 1..target_epoch).all(|e| matches!((epoch_base(e), self.ends.get(&e)), (Some(base), Some(last)) if *last == base))
    }

    fn take(&mut self, page: HistoryPage, gap: &TradesGap) -> std::result::Result<(), String> {
        let request = self.cursor;
        for end in &page.epoch_ends {
            self.ends.insert(end.epoch, end.last_trade_id);
        }
        for t in page.trades {
            let id = t.trade_id;
            let continues =
                id == self.cursor.wrapping_add(1) && epoch_of(id) == epoch_of(self.cursor);
            let crosses = epoch_of(id) > epoch_of(self.cursor)
                && epoch_base(epoch_of(id)).map(|b| b + 1) == Some(id)
                && self.closed_through(self.cursor, epoch_of(id));
            if !continues && !crosses {
                return Err(format!(
                    "trade {id} does not continue cursor {}",
                    self.cursor
                ));
            }
            self.cursor = id;
            let wanted = if gap.tail_epoch != 0 {
                epoch_of(id) == gap.tail_epoch
            } else {
                id <= gap.through_trade_id
            };
            if wanted {
                self.trades.push(t);
            }
        }
        let next = page.next_after_trade_id;
        if next < request {
            return Err(format!("next cursor {next} below the request's {request}"));
        }
        if next == self.cursor {
            return Ok(());
        }
        if next < self.cursor {
            return Err(format!(
                "next cursor {next} below the last trade {}",
                self.cursor
            ));
        }
        if epoch_of(next) == epoch_of(self.cursor) {
            return Err(format!(
                "next cursor {next} skips trades after {}",
                self.cursor
            ));
        }
        if epoch_base(epoch_of(next)) != Some(next)
            || !self.closed_through(self.cursor, epoch_of(next))
        {
            return Err(format!(
                "next cursor {next} crosses epochs not closed at {}",
                self.cursor
            ));
        }
        self.cursor = next;
        Ok(())
    }
}
