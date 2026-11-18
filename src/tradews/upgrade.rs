//! Opening the trading WebSocket's connection so a refused upgrade is read
//! whole.
//!
//! The upgrade's TCP and TLS connection is opened here and handed to the
//! WebSocket handshake in a [`Recoverable`] wrapper. When the server refuses
//! the upgrade, the handshake keeps only the part of the refusal's body that
//! arrived with its headers and drops the connection; the wrapper sends the
//! connection back on drop, so the rest of the body is read before the
//! refusal is classified. On success the wrapper only forwards.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::http::Response;

use crate::errors::{Error, Result};

/// Largest refusal body read: problem bodies are a few hundred bytes.
const MAX_REFUSAL_BODY: usize = 64 * 1024;
/// How long the rest of a refusal body may take to arrive.
const REFUSAL_BODY_WAIT: Duration = Duration::from_secs(2);

pub(super) type Stream = MaybeTlsStream<TcpStream>;

/// A connection that returns itself through `back` when dropped while
/// `back` is set: by a handshake that failed.
pub(super) struct Recoverable {
    inner: Option<Stream>,
    back: Option<oneshot::Sender<Stream>>,
}

impl Recoverable {
    /// Stops returning the connection on drop, once the upgrade succeeded.
    pub(super) fn keep(&mut self) {
        self.back = None;
    }

    fn stream(&mut self) -> Pin<&mut Stream> {
        Pin::new(
            self.inner
                .as_mut()
                .expect("the connection is held until the wrapper is dropped"),
        )
    }
}

impl Drop for Recoverable {
    fn drop(&mut self) {
        if let (Some(stream), Some(back)) = (self.inner.take(), self.back.take()) {
            let _ = back.send(stream);
        }
    }
}

impl AsyncRead for Recoverable {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.get_mut().stream().poll_read(cx, buf)
    }
}

impl AsyncWrite for Recoverable {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.get_mut().stream().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().stream().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().stream().poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.get_mut().stream().poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(AsyncWrite::is_write_vectored)
    }
}

/// Opens the TCP connection, and TLS for `wss://`, to `url`'s host. Returns
/// the wrapped connection and the receiver that gets it back if the
/// handshake drops it.
pub(super) async fn open(url: &str) -> Result<(Recoverable, oneshot::Receiver<Stream>)> {
    let parsed =
        url::Url::parse(url).map_err(|e| Error::validation(format!("tradews URL: {e}")))?;
    let tls = match parsed.scheme() {
        "wss" => true,
        "ws" => false,
        other => {
            return Err(Error::validation(format!(
                "tradews URL scheme {other} is not ws or wss"
            )));
        }
    };
    let host = match parsed.host() {
        Some(url::Host::Domain(domain)) => domain.to_owned(),
        Some(url::Host::Ipv4(ip)) => ip.to_string(),
        Some(url::Host::Ipv6(ip)) => ip.to_string(),
        None => return Err(Error::validation("tradews URL has no host")),
    };
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| Error::validation("tradews URL has no port"))?;
    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| Error::realtime(format!("tradews connect: {e}")))?;
    let _ = tcp.set_nodelay(true);
    let stream = if tls {
        let name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|e| Error::validation(format!("tradews host {host}: {e}")))?;
        let tls = tokio_rustls::TlsConnector::from(crate::tls::client_config()?)
            .connect(name, tcp)
            .await
            .map_err(|e| Error::realtime(format!("tradews TLS: {e}")))?;
        MaybeTlsStream::Rustls(tls)
    } else {
        MaybeTlsStream::Plain(tcp)
    };
    let (back, returned) = oneshot::channel();
    Ok((
        Recoverable {
            inner: Some(stream),
            back: Some(back),
        },
        returned,
    ))
}

/// The refused upgrade `response` with the rest of its body, read from the
/// connection the handshake returned: up to its `Content-Length`, or to the
/// end of the connection without one, at most [`MAX_REFUSAL_BODY`] bytes and
/// [`REFUSAL_BODY_WAIT`]. Whatever arrived by then is kept.
pub(super) async fn complete_refusal(
    mut response: Response<Option<Vec<u8>>>,
    returned: oneshot::Receiver<Stream>,
) -> Response<Option<Vec<u8>>> {
    let Ok(mut stream) = returned.await else {
        return response;
    };
    let mut body = response.body_mut().take().unwrap_or_default();
    let want = response
        .headers()
        .get(tokio_tungstenite::tungstenite::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.trim().parse::<usize>().ok())
        .map(|length| length.min(MAX_REFUSAL_BODY));
    let _ = tokio::time::timeout(REFUSAL_BODY_WAIT, async {
        let mut buf = [0u8; 4096];
        loop {
            let limit = want.unwrap_or(MAX_REFUSAL_BODY);
            if body.len() >= limit {
                body.truncate(limit);
                break;
            }
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await;
    *response.body_mut() = (!body.is_empty()).then_some(body);
    response
}
