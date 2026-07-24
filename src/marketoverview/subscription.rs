//! Managed market overview subscription (Go `marketoverview.Subscription` parity).

use crate::errors::{Error, Result};
use crate::models::{MarketOverviewEntry, MarketOverviewList};
use crate::realtime::SnapshotThenStream;
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
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<Vec<MarketOverviewEntry>>,
        stream: SnapshotThenStream<MarketOverviewList, MarketOverviewList>,
        closed: Arc<AtomicBool>,
        last_error: Arc<Mutex<Option<Error>>>,
    ) -> Self {
        Self {
            rx,
            stream,
            closed,
            last_error,
        }
    }

    /// Receiver for merged overview rows.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<Vec<MarketOverviewEntry>> {
        &mut self.rx
    }

    /// Terminal subscription error, if the stream failed (e.g. queue overflow).
    pub fn err(&self) -> Option<Error> {
        self.last_error
            .lock()
            .expect("error lock")
            .clone()
            .or_else(|| self.stream.err())
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
        self.stream.close();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.close();
    }
}
