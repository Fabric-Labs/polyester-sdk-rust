//! Managed orderbook subscription (Go `orderbook.Subscription` parity).

use crate::errors::{Error, Result};
use crate::models::{OrderBookDeltaUpdate, OrderbookData};
use crate::realtime::{SnapshotThenStream, lock_unpoisoned};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Managed orderbook stream with snapshot prefetch and sequence-checked deltas.
///
/// Delivery contract: a full consumer queue fails the subscription with
/// [`Error::QueueOverflow`] instead of silently dropping books.
pub struct Subscription {
    rx: mpsc::Receiver<OrderbookData>,
    stream: SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>,
    closed: Arc<AtomicBool>,
    bucket_ticks: Arc<Mutex<i64>>,
    emit: Arc<dyn Fn() + Send + Sync>,
    last_error: Arc<Mutex<Option<Error>>>,
    tx_slot: Arc<Mutex<Option<mpsc::Sender<OrderbookData>>>>,
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<OrderbookData>,
        stream: SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>,
        closed: Arc<AtomicBool>,
        bucket_ticks: Arc<Mutex<i64>>,
        emit: Arc<dyn Fn() + Send + Sync>,
        last_error: Arc<Mutex<Option<Error>>>,
        tx_slot: Arc<Mutex<Option<mpsc::Sender<OrderbookData>>>>,
    ) -> Self {
        Self {
            rx,
            stream,
            closed,
            bucket_ticks,
            emit,
            last_error,
            tx_slot,
        }
    }

    /// Receiver for merged orderbook snapshots.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<OrderbookData> {
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

    /// Change the active price bucket and re-emit the current book.
    pub fn set_bucket(&self, bucket: &str) {
        let ticks = crate::orderbook::parse_bucket_ticks(bucket);
        *lock_unpoisoned(&self.bucket_ticks) = ticks;
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
        // Drop the sender so `updates().recv()` unblocks with None.
        let _ = lock_unpoisoned(&self.tx_slot).take();
        self.stream.close();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.close();
    }
}
