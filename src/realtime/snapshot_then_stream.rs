//! Snapshot-then-stream coordinator (Go `realtime.SnapshotThenStream` parity).

use crate::errors::{Error, Result};
use crate::realtime::{Client, ReconnectBackoff, TypedSubscription, lock_unpoisoned};
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::Duration;

const MAX_REQUEST_REFRESH_ATTEMPTS: usize = 3;
const REQUEST_REFRESH_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const REQUEST_REFRESH_MAX_BACKOFF: Duration = Duration::from_millis(200);

type FetchSnapshotFn<TSnapshot> =
    Arc<dyn Fn() -> BoxFuture<'static, Result<TSnapshot>> + Send + Sync>;
type ApplySnapshotFn<TSnapshot, TPublication> =
    Arc<dyn Fn(TSnapshot, Vec<TPublication>) + Send + Sync>;
type ApplyLiveFn<TPublication> = Arc<dyn Fn(Vec<TPublication>) + Send + Sync>;
type ReadPublicationFn<TPublication> = Arc<dyn Fn(TPublication) -> Vec<TPublication> + Send + Sync>;
type DecodeFn<TPublication> = Arc<dyn Fn(&[u8]) -> Result<TPublication> + Send + Sync>;
type NotifyFn = Arc<dyn Fn() + Send + Sync>;
/// Callback invoked when the coordinator observes a transport, decode, snapshot,
/// or terminal buffer error.
pub type SnapshotErrorFn = Arc<dyn Fn(Error) + Send + Sync>;

/// Configuration for [`SnapshotThenStream`].
pub struct SnapshotThenStreamConfig<TSnapshot, TPublication> {
    pub client: Client,
    pub channel: String,
    pub decode: DecodeFn<TPublication>,
    pub fetch_snapshot: FetchSnapshotFn<TSnapshot>,
    pub read_publication: ReadPublicationFn<TPublication>,
    pub apply_snapshot: ApplySnapshotFn<TSnapshot, TPublication>,
    pub apply_live_publications: ApplyLiveFn<TPublication>,
    pub max_buffered: usize,
    pub on_reconnect: Option<NotifyFn>,
    pub on_snapshot_refresh: Option<NotifyFn>,
    pub on_error: Option<SnapshotErrorFn>,
}

/// Coordinates REST snapshot hydration with a live protobuf channel.
pub struct SnapshotThenStream<TSnapshot, TPublication> {
    inner: Arc<Inner<TSnapshot, TPublication>>,
    counts_handle: bool,
}

struct Inner<TSnapshot, TPublication> {
    client: Client,
    channel: String,
    decode: DecodeFn<TPublication>,
    fetch_snapshot: FetchSnapshotFn<TSnapshot>,
    read_publication: ReadPublicationFn<TPublication>,
    apply_snapshot: ApplySnapshotFn<TSnapshot, TPublication>,
    apply_live_publications: ApplyLiveFn<TPublication>,
    max_buffered: usize,
    on_reconnect: Option<NotifyFn>,
    on_snapshot_refresh: Option<NotifyFn>,
    on_error: Mutex<Option<SnapshotErrorFn>>,
    ready: AtomicBool,
    disposed: AtomicBool,
    generation: AtomicU64,
    publications: Mutex<PublicationState<TPublication>>,
    refresh_gate: AsyncMutex<()>,
    refresh_worker_running: AtomicBool,
    refresh_requested: AtomicBool,
    last_error: Mutex<Option<Error>>,
    stop_tx: watch::Sender<bool>,
    connection_tx: watch::Sender<Option<Result<()>>>,
    started: AtomicBool,
    connected_once: AtomicBool,
    handles: AtomicUsize,
}

enum PublicationState<TPublication> {
    Buffering(Vec<TPublication>),
    Ready,
}

impl<TSnapshot, TPublication> SnapshotThenStream<TSnapshot, TPublication>
where
    TSnapshot: Send + 'static,
    TPublication: Send + 'static,
{
    pub fn new(cfg: SnapshotThenStreamConfig<TSnapshot, TPublication>) -> Self {
        let max_buffered = if cfg.max_buffered == 0 {
            200
        } else {
            cfg.max_buffered
        };
        let (stop_tx, _) = watch::channel(false);
        let (connection_tx, _) = watch::channel(None);
        Self {
            inner: Arc::new(Inner {
                client: cfg.client,
                channel: cfg.channel,
                decode: cfg.decode,
                fetch_snapshot: cfg.fetch_snapshot,
                read_publication: cfg.read_publication,
                apply_snapshot: cfg.apply_snapshot,
                apply_live_publications: cfg.apply_live_publications,
                max_buffered,
                on_reconnect: cfg.on_reconnect,
                on_snapshot_refresh: cfg.on_snapshot_refresh,
                on_error: Mutex::new(cfg.on_error),
                ready: AtomicBool::new(false),
                disposed: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                publications: Mutex::new(PublicationState::Buffering(Vec::new())),
                refresh_gate: AsyncMutex::new(()),
                refresh_worker_running: AtomicBool::new(false),
                refresh_requested: AtomicBool::new(false),
                last_error: Mutex::new(None),
                stop_tx,
                connection_tx,
                started: AtomicBool::new(false),
                connected_once: AtomicBool::new(false),
                handles: AtomicUsize::new(1),
            }),
            counts_handle: true,
        }
    }

    /// Begin websocket streaming and perform the initial snapshot refresh.
    pub async fn start(&self) -> Result<()> {
        let timeout = self.inner.client.request_timeout();
        match tokio::time::timeout(timeout, self.start_within_deadline()).await {
            Ok(result) => result,
            Err(_) => {
                let err = Error::realtime(format!(
                    "snapshot-then-stream startup timed out after {timeout:?}"
                ));
                self.inner.fail_closed(err.clone());
                Err(err)
            }
        }
    }

    async fn start_within_deadline(&self) -> Result<()> {
        if !self.inner.started.swap(true, Ordering::SeqCst) {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                inner.run().await;
            });
        }
        let mut connection_rx = self.inner.connection_tx.subscribe();
        loop {
            if self.inner.disposed.load(Ordering::SeqCst) {
                return match self.err() {
                    Some(err) => Err(err),
                    None => Ok(()),
                };
            }
            // A connection can complete its handshake and close before this
            // receiver observes the transient watch value. This latch preserves
            // that first successful generation.
            if self.inner.connected_once.load(Ordering::SeqCst) {
                break;
            }
            let status = connection_rx.borrow().clone();
            if let Some(result) = status {
                result?;
                break;
            }
            if connection_rx.changed().await.is_err() {
                return Ok(());
            }
        }
        self.refresh_snapshot().await
    }

    /// Fetch a REST snapshot and merge buffered publications.
    ///
    /// On failure, readiness stays false, [`Self::err`] is set, and the pending
    /// buffer is retained so a successful retry merges each buffered publication
    /// exactly once. Success clears `err`.
    pub async fn refresh_snapshot(&self) -> Result<()> {
        let _refresh_guard = self.inner.refresh_gate.lock().await;
        self.inner.refresh_snapshot_once().await
    }

    /// Request a snapshot refresh from a sync context (e.g. sequence gap handler).
    ///
    /// Requests are coalesced behind one worker. A request arriving during a
    /// fetch schedules a follow-up, while repeated failures or persistent gaps
    /// fail closed after a bounded number of attempts.
    pub fn request_refresh(&self) {
        self.inner.request_refresh();
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::SeqCst)
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::SeqCst)
    }

    /// Terminal stream error, if recovery failed closed.
    pub fn err(&self) -> Option<Error> {
        lock_unpoisoned(&self.inner.last_error).clone()
    }

    /// Register a callback for transport, decode, snapshot, and terminal
    /// buffering errors.
    ///
    /// If an error was already recorded, the callback is invoked immediately.
    /// Callback panics are isolated from the stream worker.
    pub fn set_on_error<F>(&self, callback: F)
    where
        F: Fn(Error) + Send + Sync + 'static,
    {
        let callback: SnapshotErrorFn = Arc::new(callback);
        let current = {
            *lock_unpoisoned(&self.inner.on_error) = Some(callback.clone());
            lock_unpoisoned(&self.inner.last_error).clone()
        };
        if let Some(err) = current {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(err)));
        }
    }

    /// Stop the stream.
    pub fn close(&self) {
        if self.inner.disposed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        {
            let mut publications = lock_unpoisoned(&self.inner.publications);
            *publications = PublicationState::Buffering(Vec::new());
        }
        self.inner.ready.store(false, Ordering::SeqCst);
        self.inner.connection_tx.send_replace(Some(Ok(())));
        let _ = self.inner.stop_tx.send(true);
    }

    /// Terminate the managed stream with an observable error.
    pub(crate) fn fail(&self, err: Error) {
        self.inner.fail_closed(err);
    }
}

impl<TSnapshot, TPublication> Inner<TSnapshot, TPublication>
where
    TSnapshot: Send + 'static,
    TPublication: Send + 'static,
{
    async fn refresh_snapshot_once(&self) -> Result<()> {
        if self.disposed.load(Ordering::SeqCst) {
            return match lock_unpoisoned(&self.last_error).clone() {
                Some(err) => Err(err),
                None => Ok(()),
            };
        }
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.begin_buffering();
        // Do not clear pending here: publications buffered during a failed fetch
        // must survive for the next successful refresh.

        let mut stop_rx = self.stop_tx.subscribe();
        let snapshot = match tokio::select! {
            _ = stop_rx.changed() => return Ok(()),
            snapshot = (self.fetch_snapshot)() => snapshot,
        } {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.record_error(err.clone());
                return Err(err);
            }
        };

        if self.disposed.load(Ordering::SeqCst)
            || self.generation.load(Ordering::SeqCst) != generation
        {
            return Ok(());
        }
        let Some(buffered) = self.take_buffered_if_current(generation) else {
            return Ok(());
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.apply_snapshot)(snapshot, buffered)
        }))
        .is_err()
        {
            let err = Error::realtime("apply_snapshot callback panicked".to_owned());
            self.fail_closed(err.clone());
            return Err(err);
        }

        // Publications can arrive after the initial take and while the user
        // snapshot callback runs. Drain those batches before atomically changing
        // Buffering -> Ready under the same lock used by handle_publication.
        loop {
            let Some(buffered) = self.take_or_mark_ready(generation) else {
                return Ok(());
            };
            if buffered.is_empty() {
                break;
            }
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (self.apply_live_publications)(buffered)
            }))
            .is_err()
            {
                let err = Error::realtime("apply_live_publications callback panicked".to_owned());
                self.fail_closed(err.clone());
                return Err(err);
            }
        }

        self.clear_error();
        if let Some(cb) = &self.on_snapshot_refresh {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb()));
        }
        Ok(())
    }

    fn request_refresh(self: &Arc<Self>) {
        if self.disposed.load(Ordering::SeqCst) {
            return;
        }
        self.refresh_requested.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.begin_buffering();
        if self
            .refresh_worker_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let inner = self.clone();
        tokio::spawn(async move {
            inner.run_requested_refreshes().await;
        });
    }

    async fn run_requested_refreshes(self: Arc<Self>) {
        let mut attempts = 0usize;
        loop {
            self.refresh_requested.store(false, Ordering::SeqCst);
            attempts += 1;
            let last_error = {
                let _refresh_guard = self.refresh_gate.lock().await;
                self.refresh_snapshot_once().await
            }
            .err();

            if self.disposed.load(Ordering::SeqCst) {
                return;
            }
            let follow_up = self.refresh_requested.load(Ordering::SeqCst);
            if last_error.is_none() && !follow_up {
                self.refresh_worker_running.store(false, Ordering::SeqCst);
                // Close the handoff race with a request that observed the worker
                // as running immediately before the store above.
                if !self.refresh_requested.load(Ordering::SeqCst)
                    || self
                        .refresh_worker_running
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    return;
                }
                attempts = 0;
                continue;
            }
            if attempts >= MAX_REQUEST_REFRESH_ATTEMPTS {
                if last_error.is_some() {
                    self.stop_after_recorded_error();
                } else {
                    self.fail_closed(Error::realtime(
                        "snapshot refresh did not converge after repeated publication gaps"
                            .to_owned(),
                    ));
                }
                return;
            }

            let multiplier = 1u32 << (attempts - 1).min(16);
            let delay = REQUEST_REFRESH_INITIAL_BACKOFF
                .saturating_mul(multiplier)
                .min(REQUEST_REFRESH_MAX_BACKOFF);
            let mut stop_rx = self.stop_tx.subscribe();
            let stopped = tokio::select! {
                changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
                _ = tokio::time::sleep(delay) => false,
            };
            if stopped {
                return;
            }
        }
    }

    fn begin_buffering(&self) {
        let mut publications = lock_unpoisoned(&self.publications);
        if matches!(*publications, PublicationState::Ready) {
            *publications = PublicationState::Buffering(Vec::new());
        }
        self.ready.store(false, Ordering::SeqCst);
    }

    fn take_buffered_if_current(&self, generation: u64) -> Option<Vec<TPublication>> {
        let mut publications = lock_unpoisoned(&self.publications);
        if self.disposed.load(Ordering::SeqCst)
            || self.generation.load(Ordering::SeqCst) != generation
        {
            return None;
        }
        match &mut *publications {
            PublicationState::Buffering(pending) => Some(std::mem::take(pending)),
            PublicationState::Ready => None,
        }
    }

    fn take_or_mark_ready(&self, generation: u64) -> Option<Vec<TPublication>> {
        let mut publications = lock_unpoisoned(&self.publications);
        if self.disposed.load(Ordering::SeqCst)
            || self.generation.load(Ordering::SeqCst) != generation
        {
            return None;
        }
        match &mut *publications {
            PublicationState::Buffering(pending) if pending.is_empty() => {
                *publications = PublicationState::Ready;
                self.ready.store(true, Ordering::SeqCst);
                Some(Vec::new())
            }
            PublicationState::Buffering(pending) => Some(std::mem::take(pending)),
            PublicationState::Ready => Some(Vec::new()),
        }
    }

    fn clear_publications(&self) {
        let mut publications = lock_unpoisoned(&self.publications);
        *publications = PublicationState::Buffering(Vec::new());
    }
}

impl<TSnapshot, TPublication> Drop for SnapshotThenStream<TSnapshot, TPublication> {
    fn drop(&mut self) {
        if !self.counts_handle {
            return;
        }
        // Background tasks also hold Arc references, so Arc::strong_count cannot
        // identify the last public handle. Track public clones explicitly.
        if self.inner.handles.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        if self.inner.disposed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        {
            let mut publications = lock_unpoisoned(&self.inner.publications);
            *publications = PublicationState::Buffering(Vec::new());
        }
        self.inner.ready.store(false, Ordering::SeqCst);
        self.inner.connection_tx.send_replace(Some(Ok(())));
        let _ = self.inner.stop_tx.send(true);
    }
}

impl<TSnapshot, TPublication> Clone for SnapshotThenStream<TSnapshot, TPublication> {
    fn clone(&self) -> Self {
        self.inner.handles.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: self.inner.clone(),
            counts_handle: true,
        }
    }
}

impl<TSnapshot, TPublication> Inner<TSnapshot, TPublication>
where
    TSnapshot: Send + 'static,
    TPublication: Send + 'static,
{
    fn fail_closed(&self, err: Error) {
        self.ready.store(false, Ordering::SeqCst);
        self.disposed.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.clear_publications();
        self.record_error(err.clone());
        self.connection_tx.send_replace(Some(Err(err)));
        let _ = self.stop_tx.send(true);
    }

    fn stop_after_recorded_error(&self) {
        self.ready.store(false, Ordering::SeqCst);
        self.disposed.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.clear_publications();
        let _ = self.stop_tx.send(true);
    }

    async fn run(self: Arc<Self>) {
        let mut stop_rx = self.stop_tx.subscribe();
        let mut first = true;
        let mut backoff = ReconnectBackoff::new();
        loop {
            if self.disposed.load(Ordering::SeqCst) || *stop_rx.borrow() {
                break;
            }
            let decode = self.decode.clone();
            let sub = tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                    continue;
                }
                sub = self.client.subscribe_proto_with_options(
                    &self.channel,
                    move |bytes| decode(bytes),
                    false,
                ) => {
                    match sub {
                        Ok(sub) => {
                            backoff.reset();
                            self.connected_once.store(true, Ordering::SeqCst);
                            self.connection_tx.send_replace(Some(Ok(())));
                            sub
                        }
                        Err(err) => {
                            self.connection_tx.send_replace(Some(Err(err)));
                            if self.disposed.load(Ordering::SeqCst) {
                                break;
                            }
                            let delay = backoff.next_delay();
                            let stopped = tokio::select! {
                                changed = stop_rx.changed() => {
                                    changed.is_err() || *stop_rx.borrow()
                                }
                                _ = tokio::time::sleep(delay) => false,
                            };
                            if stopped {
                                break;
                            }
                            self.connection_tx.send_replace(None);
                            continue;
                        }
                    }
                }
            };
            if self.disposed.load(Ordering::SeqCst) || *stop_rx.borrow() {
                sub.close();
                break;
            }
            if !first {
                if let Some(cb) = &self.on_reconnect {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb()));
                }
                let this = SnapshotThenStream {
                    inner: self.clone(),
                    counts_handle: false,
                };
                // One bounded retry, then fail-closed with err() set.
                let mut refresh = this.refresh_snapshot().await;
                if refresh.is_err() {
                    refresh = this.refresh_snapshot().await;
                }
                if refresh.is_err() {
                    // refresh_snapshot already preserved and reported the
                    // underlying error. Stop without invoking the callback a
                    // second time for the same terminal attempt.
                    self.stop_after_recorded_error();
                    self.ready.store(false, Ordering::SeqCst);
                    sub.close();
                    break;
                }
            }
            first = false;
            if let Err(err) = self.pump_subscription(sub, &mut stop_rx).await
                && !self.disposed.load(Ordering::SeqCst)
            {
                self.record_error(err);
            }
            self.connection_tx.send_replace(None);
            if self.disposed.load(Ordering::SeqCst) || *stop_rx.borrow() {
                break;
            }
            let delay = backoff.next_delay();
            let stopped = tokio::select! {
                changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
                _ = tokio::time::sleep(delay) => false,
            };
            if stopped {
                break;
            }
        }
    }

    async fn pump_subscription(
        &self,
        mut sub: TypedSubscription<TPublication>,
        stop_rx: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        sub.close();
                        return Ok(());
                    }
                }
                item = sub.recv_result() => {
                    match item {
                        Ok(Some(msg)) => self.handle_publication(msg)?,
                        Ok(None) => return Ok(()),
                        Err(err) => return Err(err),
                    }
                }
            }
        }
    }

    fn handle_publication(&self, msg: TPublication) -> Result<()> {
        if self.disposed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let items = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.read_publication)(msg)
        })) {
            Ok(items) => items,
            Err(_) => {
                let err = Error::realtime("read_publication callback panicked".to_owned());
                self.fail_closed(err.clone());
                return Err(err);
            }
        };
        if items.is_empty() {
            return Ok(());
        }
        let live_items = {
            let mut publications = lock_unpoisoned(&self.publications);
            match &mut *publications {
                PublicationState::Buffering(pending) => {
                    pending.extend(items);
                    if pending.len() > self.max_buffered {
                        pending.clear();
                        None
                    } else {
                        return Ok(());
                    }
                }
                PublicationState::Ready => Some(items),
            }
        };
        let Some(items) = live_items else {
            self.fail_closed(Error::queue_overflow(
                "snapshot recovery buffer full; recreate the subscription",
            ));
            return Ok(());
        };
        if self.disposed.load(Ordering::SeqCst) {
            return Ok(());
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.apply_live_publications)(items)
        }))
        .is_err()
        {
            let err = Error::realtime("apply_live_publications callback panicked".to_owned());
            self.fail_closed(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn record_error(&self, err: Error) {
        let callback = {
            *lock_unpoisoned(&self.last_error) = Some(err.clone());
            lock_unpoisoned(&self.on_error).clone()
        };
        if let Some(callback) = callback {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(err)));
        }
    }

    fn clear_error(&self) {
        *lock_unpoisoned(&self.last_error) = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::FutureExt;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn refresh_snapshot_fires_on_snapshot_refresh() {
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_cb = fired.clone();
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(|| async { Ok("snap".to_string()) }.boxed()),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_s, _p| {}),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: Some(Arc::new(move || {
                fired_cb.fetch_add(1, Ordering::SeqCst);
            })),
            on_error: None,
        });
        sts.refresh_snapshot().await.expect("refresh");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert!(sts.is_ready());
    }

    #[tokio::test]
    async fn failed_refresh_retains_buffer_and_success_merges_each_item_once() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let fetch_attempts = attempts.clone();
        let merged = Arc::new(Mutex::new(Vec::<u8>::new()));
        let merged_cb = merged.clone();
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(move || {
                let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(Error::transport("snapshot refresh failed"))
                    } else {
                        Ok("recovered".to_owned())
                    }
                }
                .boxed()
            }),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(move |_snapshot, pending| {
                merged_cb.lock().expect("merged lock").extend(pending);
            }),
            apply_live_publications: Arc::new(|_publications| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        assert!(sts.refresh_snapshot().await.is_err());
        assert!(!sts.is_ready());
        assert!(sts.err().is_some());

        sts.inner.handle_publication(7).expect("buffer 7");
        sts.inner.handle_publication(9).expect("buffer 9");
        {
            let publications = lock_unpoisoned(&sts.inner.publications);
            let PublicationState::Buffering(pending) = &*publications else {
                panic!("expected buffering state");
            };
            assert_eq!(pending.as_slice(), &[7, 9]);
        }

        sts.refresh_snapshot().await.expect("retry succeeds");
        assert!(sts.is_ready());
        assert!(sts.err().is_none());
        assert_eq!(merged.lock().expect("merged lock").as_slice(), &[7, 9]);
        assert!(matches!(
            *lock_unpoisoned(&sts.inner.publications),
            PublicationState::Ready
        ));

        sts.refresh_snapshot()
            .await
            .expect("later refresh succeeds");
        assert_eq!(
            merged.lock().expect("merged lock").as_slice(),
            &[7, 9],
            "buffered publications must be applied exactly once"
        );
    }

    #[tokio::test]
    async fn refresh_snapshot_retries_after_initial_failure() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let fetch_attempts = attempts.clone();
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(move || {
                let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(crate::Error::transport("transient snapshot failure"))
                    } else {
                        Ok("snap".to_string())
                    }
                }
                .boxed()
            }),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_s, _p| {}),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        assert!(sts.refresh_snapshot().await.is_err());
        assert!(sts.err().is_some());
        assert!(!sts.is_ready());
        sts.refresh_snapshot().await.expect("snapshot retry");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(sts.is_ready());
        assert!(sts.err().is_none());
        sts.close();
    }

    #[tokio::test]
    async fn refresh_snapshot_failure_retains_buffer_for_successful_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let fetch_attempts = attempts.clone();
        let merged = Arc::new(Mutex::new(Vec::<u8>::new()));
        let merged_cb = merged.clone();
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(move || {
                let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(crate::Error::transport("transient snapshot failure"))
                    } else {
                        Ok("snap".to_string())
                    }
                }
                .boxed()
            }),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(move |_s, pending| {
                merged_cb.lock().expect("merged").extend(pending);
            }),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        // Simulate publications arriving while not ready / during failed fetch.
        sts.inner.handle_publication(10).expect("buffer 10");
        sts.inner.handle_publication(11).expect("buffer 11");
        assert!(sts.refresh_snapshot().await.is_err());
        sts.inner.handle_publication(12).expect("buffer 12");
        sts.refresh_snapshot().await.expect("retry");
        let got = merged.lock().expect("merged").clone();
        assert_eq!(
            got,
            vec![10, 11, 12],
            "each buffered pub merged exactly once"
        );
        assert!(sts.err().is_none());
        sts.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_arriving_after_pending_take_is_drained_exactly_once() {
        let snapshot_entered = Arc::new(Barrier::new(2));
        let release_snapshot = Arc::new(Barrier::new(2));
        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(Mutex::new(Vec::<u8>::new()));
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(|| async { Ok("snap".to_owned()) }.boxed()),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: {
                let entered = snapshot_entered.clone();
                let release = release_snapshot.clone();
                let calls = snapshot_calls.clone();
                Arc::new(move |_snapshot, pending| {
                    assert!(pending.is_empty());
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.wait();
                        release.wait();
                    }
                })
            },
            apply_live_publications: {
                let live = live.clone();
                Arc::new(move |publications| {
                    live.lock().expect("live lock").extend(publications);
                })
            },
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        let refresh = {
            let sts = sts.clone();
            tokio::spawn(async move { sts.refresh_snapshot().await })
        };
        snapshot_entered.wait();
        sts.inner
            .handle_publication(42)
            .expect("publication in vulnerable window");
        release_snapshot.wait();
        refresh.await.expect("refresh task").expect("refresh");

        assert!(sts.is_ready());
        assert_eq!(live.lock().expect("live lock").as_slice(), &[42]);
        sts.refresh_snapshot().await.expect("second refresh");
        assert_eq!(
            live.lock().expect("live lock").as_slice(),
            &[42],
            "drained publication must not be replayed"
        );
    }

    #[tokio::test]
    async fn request_refresh_burst_is_single_flight_and_coalesced() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Semaphore::new(0));
        let release_first = Arc::new(tokio::sync::Semaphore::new(0));
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: {
                let attempts = attempts.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                Arc::new(move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now_active, Ordering::SeqCst);
                    let active = active.clone();
                    let first_started = first_started.clone();
                    let release_first = release_first.clone();
                    async move {
                        if attempt == 0 {
                            first_started.add_permits(1);
                            release_first.acquire().await.expect("release").forget();
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok("snap".to_owned())
                    }
                    .boxed()
                })
            },
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_snapshot: String, _pending: Vec<u8>| {}),
            apply_live_publications: Arc::new(|_publications| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        sts.request_refresh();
        first_started.acquire().await.expect("first fetch").forget();
        for _ in 0..100 {
            sts.request_refresh();
        }
        release_first.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while sts.inner.refresh_worker_running.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh worker completion");

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "the burst should produce one in-flight fetch and one coalesced follow-up"
        );
        assert!(sts.is_ready());
    }

    #[tokio::test]
    async fn request_refresh_persistent_failure_retries_bounded_then_fails_closed() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: {
                let attempts = attempts.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                Arc::new(move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now_active, Ordering::SeqCst);
                    let active = active.clone();
                    async move {
                        active.fetch_sub(1, Ordering::SeqCst);
                        Err(Error::transport("persistent snapshot failure"))
                    }
                    .boxed()
                })
            },
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_snapshot: String, _pending: Vec<u8>| {}),
            apply_live_publications: Arc::new(|_publications| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        sts.request_refresh();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !sts.is_disposed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded failure completion");

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            MAX_REQUEST_REFRESH_ATTEMPTS
        );
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        assert!(!sts.is_ready());
        assert!(matches!(sts.err(), Some(Error::Transport(_))));
    }

    #[tokio::test]
    async fn request_refresh_persistent_gaps_fail_closed_after_bounded_followups() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let stream_slot: Arc<Mutex<Option<SnapshotThenStream<String, u8>>>> =
            Arc::new(Mutex::new(None));
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: {
                let attempts = attempts.clone();
                Arc::new(move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Ok("snap".to_owned()) }.boxed()
                })
            },
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: {
                let stream_slot = stream_slot.clone();
                Arc::new(move |_snapshot, _pending| {
                    lock_unpoisoned(&stream_slot)
                        .as_ref()
                        .expect("stream installed")
                        .request_refresh();
                })
            },
            apply_live_publications: Arc::new(|_publications| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });
        *lock_unpoisoned(&stream_slot) = Some(sts.clone());

        sts.request_refresh();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !sts.is_disposed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded persistent-gap completion");

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            MAX_REQUEST_REFRESH_ATTEMPTS
        );
        assert!(!sts.is_ready());
        assert!(matches!(sts.err(), Some(Error::Realtime(_))));
        lock_unpoisoned(&stream_slot).take();
    }

    #[test]
    fn snapshot_buffer_overflow_fails_closed() {
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(|| async { Ok("snap".to_string()) }.boxed()),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_s, _p| {}),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 1,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        sts.inner.handle_publication(1).expect("first publication");
        assert!(!sts.is_disposed());
        sts.inner
            .handle_publication(2)
            .expect("overflow is persisted on the coordinator");
        assert!(sts.is_disposed());
        assert!(matches!(sts.err(), Some(Error::QueueOverflow(_))));
    }

    #[test]
    fn dropping_last_public_handle_disposes_background_coordinator() {
        let client = Client::new(
            "wss://example.invalid",
            "https://example.invalid",
            None,
            None,
        );
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client,
            channel: "public:test".into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(|| async { Ok("snap".to_string()) }.boxed()),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_s, _p| {}),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 1,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });
        let clone = sts.clone();
        let observer = sts.inner.clone();
        drop(sts);
        assert!(
            !observer.disposed.load(Ordering::SeqCst),
            "one public clone remains"
        );
        drop(clone);
        assert!(
            observer.disposed.load(Ordering::SeqCst),
            "last public handle must stop the coordinator even when tasks hold Arc references"
        );
    }
}
