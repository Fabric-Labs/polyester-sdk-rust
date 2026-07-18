//! Managed market overview subscription (Go `marketoverview.Subscription` parity).

use crate::errors::Result;
use crate::models::{MarketOverviewEntry, MarketOverviewList};
use crate::realtime::SnapshotThenStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Managed market-overview stream with snapshot prefetch and live merge.
pub struct Subscription {
    rx: mpsc::Receiver<Vec<MarketOverviewEntry>>,
    stream: SnapshotThenStream<MarketOverviewList, MarketOverviewList>,
    closed: Arc<AtomicBool>,
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<Vec<MarketOverviewEntry>>,
        stream: SnapshotThenStream<MarketOverviewList, MarketOverviewList>,
        closed: Arc<AtomicBool>,
    ) -> Self {
        Self { rx, stream, closed }
    }

    /// Receiver for merged overview rows.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<Vec<MarketOverviewEntry>> {
        &mut self.rx
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
