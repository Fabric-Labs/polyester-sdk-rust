//! Managed orderbook subscription (Go `orderbook.Subscription` parity).

use crate::errors::Result;
use crate::models::{OrderBookDeltaUpdate, OrderbookData};
use crate::realtime::SnapshotThenStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Managed orderbook stream with snapshot prefetch and sequence-checked deltas.
pub struct Subscription {
    rx: mpsc::Receiver<OrderbookData>,
    stream: SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>,
    closed: Arc<AtomicBool>,
    bucket_ticks: Arc<Mutex<i64>>,
    emit: Arc<dyn Fn() + Send + Sync>,
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<OrderbookData>,
        stream: SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>,
        closed: Arc<AtomicBool>,
        bucket_ticks: Arc<Mutex<i64>>,
        emit: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            rx,
            stream,
            closed,
            bucket_ticks,
            emit,
        }
    }

    /// Receiver for merged orderbook snapshots.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<OrderbookData> {
        &mut self.rx
    }

    /// Change the active price bucket and re-emit the current book.
    pub fn set_bucket(&self, bucket: &str) {
        let ticks = crate::orderbook::parse_bucket_ticks(bucket);
        *self.bucket_ticks.lock().expect("bucket lock") = ticks;
        if self.stream.is_ready() && !self.closed.load(Ordering::SeqCst) {
            (self.emit)();
        }
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
