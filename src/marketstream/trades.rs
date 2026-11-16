//! Per-pair trade accounting: every trade ID up to `last` has been delivered
//! or reported in a gap.

use super::codec::Fill;
use super::{GapCause, TradesGap};

/// Trade IDs encode their book epoch: epoch `e` numbers its trades from
/// `epoch_base(e) + 1`, where `epoch_base(e) = (e - 1) << 40`, so IDs ascend
/// across epochs and `epoch_base(e)` itself means "epoch `e`, before its first
/// trade".
const EPOCH_BITS: u32 = 40;

pub(crate) fn epoch_base(epoch: u64) -> Option<u64> {
    if epoch == 0 || (epoch - 1) >> (64 - EPOCH_BITS) != 0 {
        return None;
    }
    Some((epoch - 1) << EPOCH_BITS)
}

/// The epoch a trade ID or epoch base belongs to.
pub(crate) fn epoch_of(id: u64) -> u64 {
    (id >> EPOCH_BITS) + 1
}

/// Whether `id` is a trade ID of `epoch` (not its base).
pub(crate) fn in_epoch(id: u64, epoch: u64) -> bool {
    matches!(epoch_base(epoch), Some(base) if id > base && epoch_of(id) == epoch)
}

/// One pair's trades accounting. It survives reconnects, so a new
/// subscription's baseline is reconciled against what was accounted for
/// before.
#[derive(Debug, Default)]
pub(crate) struct Tracker {
    pub symbol_id: u32,
    /// Set by the current subscription's baseline.
    started: bool,
    /// The held subscription's baseline arrived before the stream was held,
    /// so its trades and skipped ranges are dropped until the next baseline,
    /// which the stream's resubscription brings.
    pub missed: bool,
    /// Set once any subscription had a baseline.
    known: bool,
    epoch: u64,
    last: u64,
    /// The rest of `epoch` was reported skipped; the next message is of a
    /// later epoch.
    tail_open: bool,
}

pub(crate) type Gaps = Vec<TradesGap>;

impl Tracker {
    pub fn new(symbol_id: u32) -> Self {
        Self {
            symbol_id,
            ..Self::default()
        }
    }

    /// Whether the current subscription's baseline arrived.
    pub fn started(&self) -> bool {
        self.started
    }

    /// Forgets the subscription, keeping what was accounted for.
    pub fn restart(&mut self) {
        self.started = false;
        self.missed = false;
    }

    fn gap(&self, after: u64, through: u64, tail: u64, cause: GapCause) -> TradesGap {
        TradesGap {
            symbol_id: self.symbol_id,
            after_trade_id: after,
            through_trade_id: through,
            tail_epoch: tail,
            cause,
        }
    }

    /// Moves to a later epoch. Trades of the epochs in between that no
    /// message reported are a detected gap, up to the new epoch's base.
    fn advance_epoch(&mut self, epoch: u64, gaps: &mut Gaps) -> Result<(), String> {
        let base = epoch_base(epoch).ok_or_else(|| format!("book epoch {epoch} out of range"))?;
        if !self.tail_open {
            gaps.push(self.gap(self.last, base, 0, GapCause::Detected));
        } else if epoch > self.epoch + 1 {
            let from = epoch_base(self.epoch + 1).unwrap_or(base);
            gaps.push(self.gap(from, base, 0, GapCause::Detected));
        }
        self.epoch = epoch;
        self.tail_open = false;
        self.last = self.last.max(base);
        Ok(())
    }

    /// Applies a TradesThrough: the first of a subscription is its baseline;
    /// later ones confirm the frontier, and one past it is a detected gap.
    pub fn through(&mut self, epoch: u64, last: u64) -> Result<Gaps, String> {
        if epoch_base(epoch).is_none() || epoch_of(last) != epoch {
            return Err(format!("trade frontier {last} outside book epoch {epoch}"));
        }
        let mut gaps = Gaps::new();
        if !self.started {
            self.started = true;
            if !self.known {
                self.known = true;
                self.epoch = epoch;
                self.last = last;
                self.tail_open = false;
                return Ok(gaps);
            }
            // A new subscription: everything between what was accounted for
            // and its baseline is missing, across epochs if they changed.
            if last > self.last {
                gaps.push(self.gap(self.last, last, 0, GapCause::Reconnect));
                self.last = last;
            }
            if epoch > self.epoch {
                self.epoch = epoch;
                self.tail_open = false;
            }
            return Ok(gaps);
        }
        if epoch < self.epoch {
            return Ok(gaps);
        }
        if epoch > self.epoch {
            self.advance_epoch(epoch, &mut gaps)?;
        } else if self.tail_open {
            return Ok(gaps);
        }
        if last > self.last {
            gaps.push(self.gap(self.last, last, 0, GapCause::Detected));
            self.last = last;
        }
        Ok(gaps)
    }

    /// Applies a TradesSkipped; `last` `None` is the rest of the epoch.
    pub fn skipped(&mut self, epoch: u64, first: u64, last: Option<u64>) -> Result<Gaps, String> {
        if !self.started {
            if self.missed {
                return Ok(Vec::new());
            }
            return Err("trades skipped before the subscription's baseline".into());
        }
        if !in_epoch(first, epoch) || last.is_some_and(|l| !in_epoch(l, epoch) || l < first) {
            return Err(format!(
                "skipped trades {first}..{last:?} outside book epoch {epoch}"
            ));
        }
        let mut gaps = Gaps::new();
        if epoch < self.epoch {
            return Ok(gaps);
        }
        if epoch > self.epoch {
            self.advance_epoch(epoch, &mut gaps)?;
        } else if self.tail_open {
            return Ok(gaps);
        }
        if first > self.last + 1 {
            gaps.push(self.gap(self.last, first - 1, 0, GapCause::Detected));
            self.last = first - 1;
        }
        match last {
            None => {
                gaps.push(self.gap(self.last, 0, epoch, GapCause::Skipped));
                self.tail_open = true;
            }
            Some(last) if last > self.last => {
                gaps.push(self.gap(self.last, last, 0, GapCause::Skipped));
                self.last = last;
            }
            Some(_) => {}
        }
        Ok(gaps)
    }

    /// Applies a Trades message: the fills not yet accounted for, and the
    /// gaps before them.
    pub fn trades(&mut self, epoch: u64, fills: &[Fill]) -> Result<(Vec<Fill>, Gaps), String> {
        if !self.started {
            if self.missed {
                return Ok((Vec::new(), Vec::new()));
            }
            return Err("trades before the subscription's baseline".into());
        }
        if fills.is_empty() {
            return Err("trades message without trades".into());
        }
        let mut prev = 0;
        for f in fills {
            if !in_epoch(f.trade_id, epoch) {
                return Err(format!("trade {} outside book epoch {epoch}", f.trade_id));
            }
            if f.trade_id <= prev {
                return Err(format!("trade {} after trade {prev}", f.trade_id));
            }
            prev = f.trade_id;
        }
        let mut gaps = Gaps::new();
        if epoch < self.epoch {
            return Ok((Vec::new(), gaps));
        }
        if epoch > self.epoch {
            self.advance_epoch(epoch, &mut gaps)?;
        } else if self.tail_open {
            // The rest of this epoch was reported skipped, so its recovery
            // covers these trades.
            return Ok((Vec::new(), gaps));
        }
        let mut fresh = Vec::with_capacity(fills.len());
        for f in fills {
            if f.trade_id <= self.last {
                continue; // already delivered or reported
            }
            if f.trade_id > self.last + 1 {
                gaps.push(self.gap(self.last, f.trade_id - 1, 0, GapCause::Detected));
            }
            fresh.push(f.clone());
            self.last = f.trade_id;
        }
        Ok((fresh, gaps))
    }
}
