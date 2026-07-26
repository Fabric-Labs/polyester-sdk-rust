//! Managed market overview subscription (Go `marketoverview.Subscription` parity).

use crate::errors::{Error, Result};
use crate::models::{MarketOverviewEntry, MarketOverviewList};
use crate::realtime::{SnapshotThenStream, lock_unpoisoned};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Managed market-overview stream with snapshot prefetch and live merge.
///
/// Delivery contract: a full consumer queue fails the subscription with
/// [`Error::QueueOverflow`] instead of silently dropping rows.
pub struct Subscription {
    rx: mpsc::Receiver<Vec<MarketOverviewEntry>>,
    stream: SnapshotThenStream<MarketOverviewList, MarketOverviewList>,
    closed: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<Error>>>,
    tx_slot: Arc<Mutex<Option<mpsc::Sender<Vec<MarketOverviewEntry>>>>>,
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<Vec<MarketOverviewEntry>>,
        stream: SnapshotThenStream<MarketOverviewList, MarketOverviewList>,
        closed: Arc<AtomicBool>,
        last_error: Arc<Mutex<Option<Error>>>,
        tx_slot: Arc<Mutex<Option<mpsc::Sender<Vec<MarketOverviewEntry>>>>>,
    ) -> Self {
        Self {
            rx,
            stream,
            closed,
            last_error,
            tx_slot,
        }
    }

    /// Receiver for merged overview rows.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<Vec<MarketOverviewEntry>> {
        &mut self.rx
    }

    /// Terminal subscription error, if the stream failed (e.g. queue overflow).
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.last_error)
            .clone()
            .or_else(|| self.stream.err())
    }

    /// Register a callback for background transport, decode, snapshot, or
    /// terminal buffering errors.
    pub fn set_on_error<F>(&self, callback: F)
    where
        F: Fn(Error) + Send + Sync + 'static,
    {
        self.stream.set_on_error(callback);
    }

    /// Refetch the REST snapshot.
    pub async fn refresh_snapshot(&self) -> Result<()> {
        self.stream.refresh_snapshot().await
    }

    /// Stop the subscription.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = lock_unpoisoned(&self.tx_slot).take();
        self.stream.close();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.close();
    }
}
