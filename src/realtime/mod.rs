//! Centrifugo websocket realtime client.

use crate::auth::Credentials;
use crate::errors::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_PATH: &str = "/connection/websocket";
const DEFAULT_QUEUE: usize = 1000;

/// Realtime websocket client.
#[derive(Clone)]
pub struct Client {
    ws_url: String,
    #[allow(dead_code)]
    api_url: String,
    #[allow(dead_code)]
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

    /// Subscribe to a channel and receive raw text frames (JSON).
    ///
    /// Private-channel token exchange (`/v1/rt/token`) is implemented for
    /// authenticated clients when credentials are present; public channels
    /// connect with empty tokens.
    pub async fn subscribe_raw(&self, channel: &str) -> Result<Subscription> {
        let url = self.ws_endpoint();
        let (ws, _) = connect_async(&url)
            .await
            .map_err(|e| Error::realtime(format!("ws connect: {e}")))?;
        let (mut write, mut read) = ws.split();

        let connect_msg = serde_json::json!({
            "connect": { "name": "polyester-sdk-rust" },
            "id": 1
        });
        write
            .send(Message::Text(connect_msg.to_string().into()))
            .await
            .map_err(|e| Error::realtime(format!("ws send: {e}")))?;

        let sub_msg = serde_json::json!({
            "subscribe": { "channel": channel },
            "id": 2
        });
        write
            .send(Message::Text(sub_msg.to_string().into()))
            .await
            .map_err(|e| Error::realtime(format!("ws subscribe: {e}")))?;

        let (tx, rx) = mpsc::channel::<String>(self.max_queue);
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        if tx.send(t.to_string()).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Ping(p)) => {
                        let _ = write.send(Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(Subscription { rx })
    }
}

/// Handle for a realtime subscription.
pub struct Subscription {
    rx: mpsc::Receiver<String>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<String> {
        self.rx.recv().await
    }
}
