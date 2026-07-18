//! Centrifugo websocket realtime client.

mod auth;
mod snapshot_then_stream;

pub use snapshot_then_stream::{SnapshotThenStream, SnapshotThenStreamConfig};

use crate::auth::Credentials;
use crate::errors::{Error, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_PATH: &str = "/connection/websocket";
const DEFAULT_QUEUE: usize = 1000;
const CENTRIFUGO_READ_TIMEOUT: Duration = Duration::from_secs(30);
const CENTRIFUGO_RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Realtime websocket client.
#[derive(Clone)]
pub struct Client {
    ws_url: String,
    api_url: String,
    credentials: Option<Credentials>,
    max_queue: usize,
}

impl Client {
    pub fn new(
        ws_url: impl Into<String>,
        api_url: impl Into<String>,
        credentials: Option<Credentials>,
        max_queue: Option<usize>,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            api_url: api_url.into(),
            credentials,
            max_queue: max_queue.unwrap_or(DEFAULT_QUEUE),
        }
    }

    fn ws_endpoint(&self) -> String {
        let base = self.ws_url.trim_end_matches('/');
        if base.contains(WS_PATH) {
            base.to_owned()
        } else {
            format!("{base}{WS_PATH}")
        }
    }

    fn validate_channel(&self, channel: &str) -> Result<()> {
        if is_private_channel(channel) {
            if self.credentials.is_none() {
                return Err(Error::auth(format!(
                    "Cannot subscribe to private channel \"{channel}\" without API-key credentials"
                )));
            }
            if self.api_url.is_empty() {
                return Err(Error::realtime(
                    "Realtime private channels require api_url".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Subscribe to a channel and receive raw Centrifugo JSON frames.
    pub async fn subscribe_raw(&self, channel: &str) -> Result<Subscription> {
        self.validate_channel(channel)?;

        let (stop_tx, stop_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel::<String>(self.max_queue);

        let this = self.clone();
        let channel = channel.to_owned();
        let alive_task = alive.clone();
        let task = tokio::spawn(async move {
            let _guard = AliveGuard(alive_task.clone());
            while !*stop_rx.borrow() {
                match this
                    .run_raw_subscription_once(&channel, &tx, &stop_rx)
                    .await
                {
                    Ok(()) => break,
                    Err(_) if *stop_rx.borrow() => break,
                    Err(_) => {
                        if *stop_rx.borrow() {
                            break;
                        }
                        tokio::time::sleep(CENTRIFUGO_RECONNECT_DELAY).await;
                    }
                }
            }
            alive_task.store(false, Ordering::SeqCst);
            drop(tx);
        });

        Ok(Subscription {
            rx,
            stop: stop_tx,
            alive,
            task,
        })
    }

    /// Subscribe to a protobuf Centrifugo channel and decode publications.
    pub async fn subscribe_proto<T, F>(
        &self,
        channel: &str,
        decode: F,
    ) -> Result<TypedSubscription<T>>
    where
        T: Send + 'static,
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
    {
        self.validate_channel(channel)?;

        let (stop_tx, stop_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel::<T>(self.max_queue);
        let decode = Arc::new(decode);

        let this = self.clone();
        let channel = channel.to_owned();
        let alive_task = alive.clone();
        let task = tokio::spawn(async move {
            let _guard = AliveGuard(alive_task.clone());
            while !*stop_rx.borrow() {
                match this
                    .run_proto_subscription_once(&channel, decode.as_ref(), &tx, &stop_rx)
                    .await
                {
                    Ok(()) => break,
                    Err(_) if *stop_rx.borrow() => break,
                    Err(_) => {
                        if *stop_rx.borrow() {
                            break;
                        }
                        tokio::time::sleep(CENTRIFUGO_RECONNECT_DELAY).await;
                    }
                }
            }
            alive_task.store(false, Ordering::SeqCst);
            drop(tx);
        });

        Ok(TypedSubscription {
            rx,
            stop: stop_tx,
            alive,
            task,
        })
    }

    async fn handshake_channel<W, R>(
        &self,
        write: &mut W,
        read: &mut R,
        channel: &str,
    ) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: std::fmt::Display,
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        if is_private_channel(channel) {
            let creds = self
                .credentials
                .as_ref()
                .ok_or_else(|| Error::auth("private channel requires credentials"))?;
            let connection_token = auth::fetch_connection_token(creds, &self.api_url).await?;
            centrifugo_connect(write, read, Some(&connection_token)).await?;
            let subscription_token =
                auth::fetch_subscription_token(creds, &self.api_url, channel).await?;
            centrifugo_subscribe(write, read, channel, Some(&subscription_token)).await?;
        } else {
            centrifugo_connect(write, read, None).await?;
            centrifugo_subscribe(write, read, channel, None).await?;
        }
        Ok(())
    }

    async fn run_raw_subscription_once(
        &self,
        channel: &str,
        tx: &mpsc::Sender<String>,
        stop: &watch::Receiver<bool>,
    ) -> Result<()> {
        let url = self.ws_endpoint();
        let (ws, _) = connect_async(&url)
            .await
            .map_err(|e| Error::realtime(format!("ws connect: {e}")))?;
        let (mut write, mut read) = ws.split();
        self.handshake_channel(&mut write, &mut read, channel)
            .await?;

        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let msg = match timeout(CENTRIFUGO_READ_TIMEOUT, read.next()).await {
                Ok(Some(Ok(msg))) => msg,
                Ok(Some(Err(e))) => return Err(Error::realtime(e.to_string())),
                Ok(None) => return Err(Error::realtime("websocket closed".to_owned())),
                Err(_) => continue,
            };
            if *stop.borrow() {
                return Ok(());
            }
            match msg {
                Message::Text(text) => {
                    for frame in split_centrifugo_frames(&text) {
                        if let Some(reply) = handle_centrifugo_control(&frame) {
                            write
                                .send(Message::Text(reply.into()))
                                .await
                                .map_err(|e| Error::realtime(format!("ws send: {e}")))?;
                        } else if !frame.trim().is_empty() && tx.send(frame).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Message::Ping(payload) => {
                    write
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|e| Error::realtime(format!("ws pong: {e}")))?;
                }
                Message::Close(_) => {
                    return Err(Error::realtime("websocket closed".to_owned()));
                }
                _ => {}
            }
        }
    }

    async fn run_proto_subscription_once<T, F>(
        &self,
        channel: &str,
        decode: &F,
        tx: &mpsc::Sender<T>,
        stop: &watch::Receiver<bool>,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T>,
    {
        let url = self.ws_endpoint();
        let (ws, _) = connect_async(&url)
            .await
            .map_err(|e| Error::realtime(format!("ws connect: {e}")))?;
        let (mut write, mut read) = ws.split();
        self.handshake_channel(&mut write, &mut read, channel)
            .await?;

        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let msg = match timeout(CENTRIFUGO_READ_TIMEOUT, read.next()).await {
                Ok(Some(Ok(msg))) => msg,
                Ok(Some(Err(e))) => return Err(Error::realtime(e.to_string())),
                Ok(None) => return Err(Error::realtime("websocket closed".to_owned())),
                Err(_) => continue,
            };
            if *stop.borrow() {
                return Ok(());
            }
            match msg {
                Message::Text(text) => {
                    for frame in split_centrifugo_frames(&text) {
                        if let Some(reply) = handle_centrifugo_control(&frame) {
                            write
                                .send(Message::Text(reply.into()))
                                .await
                                .map_err(|e| Error::realtime(format!("ws send: {e}")))?;
                            continue;
                        }
                        if let Some(payload) = publication_payload(&frame) {
                            let bytes = payload?;
                            let item = decode(&bytes)?;
                            if tx.send(item).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                Message::Ping(payload) => {
                    write
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|e| Error::realtime(format!("ws pong: {e}")))?;
                }
                Message::Close(_) => {
                    return Err(Error::realtime("websocket closed".to_owned()));
                }
                _ => {}
            }
        }
    }
}

/// Handle for a raw realtime subscription (Centrifugo JSON frames).
pub struct Subscription {
    rx: mpsc::Receiver<String>,
    stop: watch::Sender<bool>,
    alive: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<String> {
        self.rx.recv().await
    }

    /// True while the background websocket task is still running.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst) && !self.task.is_finished()
    }

    pub fn close(&self) {
        let _ = self.stop.send(true);
    }
}

/// Handle for a typed protobuf realtime subscription.
pub struct TypedSubscription<T> {
    rx: mpsc::Receiver<T>,
    stop: watch::Sender<bool>,
    alive: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> TypedSubscription<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await
    }

    /// True while the background websocket task is still running.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst) && !self.task.is_finished()
    }

    pub fn close(&self) {
        let _ = self.stop.send(true);
    }
}

struct AliveGuard(Arc<AtomicBool>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub fn is_private_channel(channel: &str) -> bool {
    channel.starts_with("private:")
}

async fn centrifugo_connect<W, R>(write: &mut W, read: &mut R, token: Option<&str>) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let mut payload = serde_json::Map::new();
    if let Some(token) = token {
        payload.insert(
            "token".to_owned(),
            serde_json::Value::String(token.to_owned()),
        );
    }
    let msg = serde_json::json!({ "id": 1, "connect": payload });
    write
        .send(Message::Text(msg.to_string().into()))
        .await
        .map_err(|e| Error::realtime(format!("connect send: {e}")))?;
    read_centrifugo_reply(read).await
}

async fn centrifugo_subscribe<W, R>(
    write: &mut W,
    read: &mut R,
    channel: &str,
    token: Option<&str>,
) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let mut payload = serde_json::Map::new();
    payload.insert(
        "channel".to_owned(),
        serde_json::Value::String(channel.to_owned()),
    );
    if let Some(token) = token {
        payload.insert(
            "token".to_owned(),
            serde_json::Value::String(token.to_owned()),
        );
    }
    let msg = serde_json::json!({ "id": 2, "subscribe": payload });
    write
        .send(Message::Text(msg.to_string().into()))
        .await
        .map_err(|e| Error::realtime(format!("subscribe send: {e}")))?;
    read_centrifugo_reply(read).await
}

async fn read_centrifugo_reply<R>(read: &mut R) -> Result<()>
where
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let msg = timeout(Duration::from_secs(10), read.next())
        .await
        .map_err(|_| Error::realtime("centrifugo reply timeout".to_owned()))?
        .ok_or_else(|| Error::realtime("centrifugo closed before reply".to_owned()))?
        .map_err(|e| Error::realtime(format!("centrifugo read: {e}")))?;
    let text = match msg {
        Message::Text(t) => t.to_string(),
        Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => {
            return Err(Error::realtime(
                "unexpected centrifugo reply type".to_owned(),
            ));
        }
    };
    let payload: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| Error::realtime(format!("centrifugo reply json: {e}")))?;
    if payload.get("error").is_some() {
        return Err(Error::realtime(format!("centrifugo error: {payload}")));
    }
    Ok(())
}

fn split_centrifugo_frames(raw: &str) -> Vec<String> {
    raw.split('\n')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Returns pong reply when Centrifugo expects `{}`, otherwise None.
fn handle_centrifugo_control(frame: &str) -> Option<&'static str> {
    let message: serde_json::Value = match serde_json::from_str(frame) {
        Ok(v) => v,
        Err(_) => return None,
    };
    if message.as_object().is_some_and(|m| m.is_empty()) {
        return Some("{}");
    }
    if let Some(push) = message.get("push").and_then(|v| v.as_object())
        && push.contains_key("ping")
    {
        return Some("{}");
    }
    if message.get("ping").is_some() && message.get("id").is_none() {
        return Some("{}");
    }
    None
}

/// Extract protobuf payload bytes from a Centrifugo push publication frame.
fn publication_payload(frame: &str) -> Option<Result<Vec<u8>>> {
    let message: serde_json::Value = match serde_json::from_str(frame) {
        Ok(v) => v,
        Err(_) => return None,
    };
    let data = message.get("push")?.get("pub")?.get("data").cloned()?;
    Some(decode_publication_data(&data))
}

fn decode_publication_data(data: &serde_json::Value) -> Result<Vec<u8>> {
    match data {
        serde_json::Value::String(s) => B64
            .decode(s)
            .map_err(|e| Error::realtime(format!("publication base64: {e}"))),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let n = item
                    .as_u64()
                    .ok_or_else(|| Error::realtime("invalid publication bytes".to_owned()))?;
                if n > u8::MAX as u64 {
                    return Err(Error::realtime("invalid publication bytes".to_owned()));
                }
                out.push(n as u8);
            }
            Ok(out)
        }
        _ => Err(Error::realtime(
            "unsupported publication data type".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_private_channel_detects_prefix() {
        assert!(is_private_channel("private:spot:orders:acct:proto"));
        assert!(!is_private_channel("public:spot:market:trades:1:proto"));
    }

    #[test]
    fn split_frames_handles_newline_batches() {
        let frames = split_centrifugo_frames("{\"ping\":{}}\n{\"push\":{\"ping\":{}}}");
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn handle_frame_replies_to_ping() {
        assert_eq!(handle_centrifugo_control("{\"ping\":{}}"), Some("{}"));
        assert_eq!(
            handle_centrifugo_control("{\"push\":{\"ping\":{}}}"),
            Some("{}")
        );
        assert_eq!(
            handle_centrifugo_control("{\"push\":{\"pub\":{\"data\":\"x\"}}}"),
            None
        );
    }

    #[test]
    fn decode_publication_data_supports_base64_and_byte_array() {
        let b64 = serde_json::json!("AQID");
        assert_eq!(decode_publication_data(&b64).unwrap(), vec![1, 2, 3]);
        let arr = serde_json::json!([1, 2, 3]);
        assert_eq!(decode_publication_data(&arr).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn publication_payload_extracts_push_pub_data() {
        let frame = r#"{"push":{"pub":{"data":"AQID"}}}"#;
        let bytes = publication_payload(frame).unwrap().unwrap();
        assert_eq!(bytes, vec![1, 2, 3]);
        assert!(publication_payload(r#"{"ping":{}}"#).is_none());
    }
}
