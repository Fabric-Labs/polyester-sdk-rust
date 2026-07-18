//! Snapshot-then-stream coordinator (Go `realtime.SnapshotThenStream` parity).

use crate::errors::Result;
use crate::realtime::{Client, TypedSubscription};
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

type FetchSnapshotFn<TSnapshot> =
    Arc<dyn Fn() -> BoxFuture<'static, Result<TSnapshot>> + Send + Sync>;
type ApplySnapshotFn<TSnapshot, TPublication> =
    Arc<dyn Fn(TSnapshot, Vec<TPublication>) + Send + Sync>;
type ApplyLiveFn<TPublication> = Arc<dyn Fn(Vec<TPublication>) + Send + Sync>;
type ReadPublicationFn<TPublication> = Arc<dyn Fn(TPublication) -> Vec<TPublication> + Send + Sync>;
type DecodeFn<TPublication> = Arc<dyn Fn(&[u8]) -> Result<TPublication> + Send + Sync>;

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
}

/// Coordinates REST snapshot hydration with a live protobuf channel.
pub struct SnapshotThenStream<TSnapshot, TPublication> {
    inner: Arc<Inner<TSnapshot, TPublication>>,
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
    ready: AtomicBool,
    disposed: AtomicBool,
    generation: AtomicU64,
    pending: Mutex<Vec<TPublication>>,
    stop_tx: watch::Sender<bool>,
    started: AtomicBool,
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
                ready: AtomicBool::new(false),
                disposed: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                pending: Mutex::new(Vec::new()),
                stop_tx,
                started: AtomicBool::new(false),
            }),
        }
    }

    /// Begin websocket streaming and perform the initial snapshot refresh.
    pub async fn start(&self) -> Result<()> {
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.run().await;
        });
        self.refresh_snapshot().await
    }

    /// Fetch a REST snapshot and merge buffered publications.
    pub async fn refresh_snapshot(&self) -> Result<()> {
        if self.inner.disposed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let generation = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.ready.store(false, Ordering::SeqCst);
        {
            let mut pending = self.inner.pending.lock().expect("pending lock");
            pending.clear();
        }

        let snapshot = (self.inner.fetch_snapshot)().await?;

        if self.inner.disposed.load(Ordering::SeqCst)
            || self.inner.generation.load(Ordering::SeqCst) != generation
        {
            return Ok(());
        }
        let buffered = {
            let mut pending = self.inner.pending.lock().expect("pending lock");
            std::mem::take(&mut *pending)
        };
        (self.inner.apply_snapshot)(snapshot, buffered);
        if self.inner.disposed.load(Ordering::SeqCst)
            || self.inner.generation.load(Ordering::SeqCst) != generation
        {
            return Ok(());
        }
        self.inner.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Request a snapshot refresh from a sync context (e.g. sequence gap handler).
    pub fn request_refresh(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            let _ = this.refresh_snapshot().await;
        });
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::SeqCst)
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::SeqCst)
    }

    /// Stop the stream.
    pub fn close(&self) {
        if self.inner.disposed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        {
            let mut pending = self.inner.pending.lock().expect("pending lock");
            pending.clear();
        }
        self.inner.ready.store(false, Ordering::SeqCst);
        let _ = self.inner.stop_tx.send(true);
    }
}

impl<TSnapshot, TPublication> Clone for SnapshotThenStream<TSnapshot, TPublication> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<TSnapshot, TPublication> Inner<TSnapshot, TPublication>
where
    TPublication: Send + 'static,
{
    async fn run(self: Arc<Self>) {
        let mut stop_rx = self.stop_tx.subscribe();
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
                sub = self.client.subscribe_proto(&self.channel, move |bytes| decode(bytes)) => {
                    match sub {
                        Ok(sub) => sub,
                        Err(_) => {
                            if self.disposed.load(Ordering::SeqCst) {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            };
            if self.disposed.load(Ordering::SeqCst) || *stop_rx.borrow() {
                sub.close();
                break;
            }
            self.pump_subscription(sub, &mut stop_rx).await;
            if self.disposed.load(Ordering::SeqCst) || *stop_rx.borrow() {
                break;
            }
        }
    }

    async fn pump_subscription(
        &self,
        mut sub: TypedSubscription<TPublication>,
        stop_rx: &mut watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        sub.close();
                        break;
                    }
                }
                item = sub.recv() => {
                    match item {
                        Some(msg) => self.handle_publication(msg),
                        None => break,
                    }
                }
            }
        }
    }

    fn handle_publication(&self, msg: TPublication) {
        if self.disposed.load(Ordering::SeqCst) {
            return;
        }
        let items = (self.read_publication)(msg);
        if items.is_empty() {
            return;
        }
        if !self.ready.load(Ordering::SeqCst) {
            let mut pending = self.pending.lock().expect("pending lock");
            pending.extend(items);
            let overflow = pending.len().saturating_sub(self.max_buffered);
            if overflow > 0 {
                pending.drain(0..overflow);
            }
            return;
        }
        (self.apply_live_publications)(items);
    }
}
