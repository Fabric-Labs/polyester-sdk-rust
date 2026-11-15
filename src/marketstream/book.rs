//! A pair's book of one depth stream.

use super::codec::WireLevel;
use super::{Level, Stream};

/// A copy of a pair's book of the depth20 or depth stream. Bids are best
/// (highest) first, asks best (lowest) first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBook {
    pub symbol_id: u32,
    pub symbol: Option<String>,
    pub stream: Stream,
    pub book_epoch: u64,
    /// The book update the book shows.
    pub update_id: u64,
    /// The most levels per side the stream holds: 20 for depth20, the live
    /// window for depth.
    pub levels_per_side: u16,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    /// Set while the book may not be current: the pair's feed is stale, or
    /// the book waits for its next snapshot after a reconnect. The levels are
    /// then the last state known.
    pub stale: bool,
}

/// A book in wire units: bids descending by price, asks ascending.
#[derive(Debug)]
pub(crate) struct Book {
    pub stream: Stream,
    /// The book holds a snapshot of the current subscription and has
    /// followed every update since.
    pub synced: bool,
    pub has: bool,
    pub epoch: u64,
    pub update_id: u64,
    /// The snapshot's levels per side: each side is truncated to it after
    /// every update.
    pub window: u16,
    pub bids: Vec<WireLevel>,
    pub asks: Vec<WireLevel>,
}

/// What an update or snapshot the book could not take is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Malformed: the message breaks the protocol.
    Invalid(String),
    /// The book is not at the update's base and stops following the stream.
    Gap(String),
}

fn valid_levels(levels: &[WireLevel], descending: bool) -> Result<(), String> {
    for (i, l) in levels.iter().enumerate() {
        if l.price <= 0 || l.qty <= 0 {
            return Err(format!(
                "level {i} has price {} and quantity {}",
                l.price, l.qty
            ));
        }
        if i > 0 {
            let prev = levels[i - 1].price;
            if (descending && l.price >= prev) || (!descending && l.price <= prev) {
                return Err(format!("level {i} at price {} is out of order", l.price));
            }
        }
    }
    Ok(())
}

/// Sets each level of `changes` on `side`, deleting those with quantity 0,
/// and keeps the side ordered best first.
fn apply_levels(side: &mut Vec<WireLevel>, changes: &[WireLevel], descending: bool) {
    for c in changes {
        let found = side.binary_search_by(|l| {
            if descending {
                c.price.cmp(&l.price)
            } else {
                l.price.cmp(&c.price)
            }
        });
        match (found, c.qty) {
            (Ok(i), 0) => {
                side.remove(i);
            }
            (Ok(i), qty) => side[i].qty = qty,
            (Err(_), 0) => {}
            (Err(i), _) => side.insert(i, *c),
        }
    }
}

impl Book {
    pub fn new(stream: Stream) -> Self {
        Self {
            stream,
            synced: false,
            has: false,
            epoch: 0,
            update_id: 0,
            window: 0,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn newer(&self, epoch: u64, update_id: u64) -> bool {
        !self.has || epoch > self.epoch || (epoch == self.epoch && update_id > self.update_id)
    }

    /// Replaces the book. A depth20 snapshot not newer than the book is a
    /// re-send and changes nothing: `Ok(false)`. A depth snapshot always
    /// replaces the book.
    pub fn snapshot(
        &mut self,
        epoch: u64,
        update_id: u64,
        window: u16,
        bids: &[WireLevel],
        asks: &[WireLevel],
    ) -> Result<bool, String> {
        valid_levels(bids, true).map_err(|e| format!("snapshot bids: {e}"))?;
        valid_levels(asks, false).map_err(|e| format!("snapshot asks: {e}"))?;
        if window == 0 || bids.len() > window as usize || asks.len() > window as usize {
            return Err(format!(
                "snapshot of {} bids and {} asks for {window} levels per side",
                bids.len(),
                asks.len()
            ));
        }
        if self.stream == Stream::Depth20 && self.synced && !self.newer(epoch, update_id) {
            return Ok(false);
        }
        self.synced = true;
        self.has = true;
        self.epoch = epoch;
        self.update_id = update_id;
        self.window = window;
        self.bids = bids.to_vec();
        self.asks = asks.to_vec();
        Ok(true)
    }

    /// Applies a DepthUpdate at exactly its base, then truncates each side to
    /// the snapshot's levels per side.
    pub fn update(
        &mut self,
        epoch: u64,
        first: u64,
        last: u64,
        bids: &[WireLevel],
        asks: &[WireLevel],
    ) -> Result<(), Refusal> {
        if last < first {
            return Err(Refusal::Invalid(format!("update covers {first}..{last}")));
        }
        for (i, l) in bids.iter().chain(asks).enumerate() {
            if l.price <= 0 || l.qty < 0 {
                return Err(Refusal::Invalid(format!(
                    "update level {i} has price {} and quantity {}",
                    l.price, l.qty
                )));
            }
        }
        if !self.synced {
            return Err(Refusal::Gap(format!(
                "depth update {first}..{last} before a snapshot"
            )));
        }
        if epoch != self.epoch {
            return Err(Refusal::Gap(format!(
                "depth update of book epoch {epoch} on a book of epoch {}",
                self.epoch
            )));
        }
        if self.update_id.checked_add(1) != Some(first) {
            return Err(Refusal::Gap(format!(
                "depth update {first}..{last} on a book at {}",
                self.update_id
            )));
        }
        apply_levels(&mut self.bids, bids, true);
        apply_levels(&mut self.asks, asks, false);
        self.bids.truncate(self.window as usize);
        self.asks.truncate(self.window as usize);
        self.update_id = last;
        Ok(())
    }
}
