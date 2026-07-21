//! Managed orderbook subscription (Go `orderbook.Subscription` parity).

use crate::errors::{Error, Result};
use crate::models::{OrderBookDeltaUpdate, OrderbookData};
use crate::realtime::SnapshotThenStream;
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
}

impl Subscription {
    pub(crate) fn new(
        rx: mpsc::Receiver<OrderbookData>,
        stream: SnapshotThenStream<OrderbookData, OrderBookDeltaUpdate>,
        closed: Arc<AtomicBool>,
        bucket_ticks: Arc<Mutex<i64>>,
        emit: Arc<dyn Fn() + Send + Sync>,
        last_error: Arc<Mutex<Option<Error>>>,
    ) -> Self {
        Self {
            rx,
            stream,
            closed,
            bucket_ticks,
            emit,
            last_error,
        }
    }

    /// Receiver for merged orderbook snapshots.
    pub fn updates(&mut self) -> &mut mpsc::Receiver<OrderbookData> {
        &mut self.rx
    }

    /// Terminal subscription error, if the stream failed (e.g. queue overflow).
    pub fn err(&self) -> Option<Error> {
        self.last_error.lock().expect("error lock").clone()
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
