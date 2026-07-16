//! Centrifugo websocket realtime client.

mod auth;

use crate::auth::Credentials;
use crate::errors::{Error, Result};
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

    /// Subscribe to a channel and receive raw Centrifugo JSON frames.
    pub async fn subscribe_raw(&self, channel: &str) -> Result<Subscription> {
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

        let (stop_tx, stop_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel::<String>(self.max_queue);

        let this = self.clone();
        let channel = channel.to_owned();
        let alive_task = alive.clone();
        let task = tokio::spawn(async move {
            let _guard = AliveGuard(alive_task.clone());
            while !*stop_rx.borrow() {
                match this.run_subscription_once(&channel, &tx, &stop_rx).await {
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

    async fn run_subscription_once(
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

        if is_private_channel(channel) {
            let creds = self
                .credentials
                .as_ref()
                .ok_or_else(|| Error::auth("private channel requires credentials"))?;
            let connection_token = auth::fetch_connection_token(creds, &self.api_url).await?;
            centrifugo_connect(&mut write, &mut read, Some(&connection_token)).await?;
            let subscription_token =
                auth::fetch_subscription_token(creds, &self.api_url, channel).await?;
            centrifugo_subscribe(&mut write, &mut read, channel, Some(&subscription_token)).await?;
        } else {
            centrifugo_connect(&mut write, &mut read, None).await?;
            centrifugo_subscribe(&mut write, &mut read, channel, None).await?;
        }

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
                        if let Some(reply) = handle_centrifugo_frame(&frame) {
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
}

/// Handle for a realtime subscription.
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
fn handle_centrifugo_frame(frame: &str) -> Option<&'static str> {
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
        assert_eq!(handle_centrifugo_frame("{\"ping\":{}}"), Some("{}"));
        assert_eq!(
            handle_centrifugo_frame("{\"push\":{\"ping\":{}}}"),
            Some("{}")
        );
        assert_eq!(
            handle_centrifugo_frame("{\"push\":{\"pub\":{\"data\":\"x\"}}}"),
            None
        );
    }
}
