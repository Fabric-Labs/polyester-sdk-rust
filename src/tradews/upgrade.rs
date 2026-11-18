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

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
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

/// The refused upgrade `response` with its whole body, read from the
/// connection the handshake returned. The handshake has parsed the head and
/// kept only the body bytes that arrived with it; the head is written back in
/// front of them and of the connection, and hyper's HTTP/1 client reads the
/// response from that, so a `Content-Length`, a chunked or a
/// connection-delimited body is decoded by the library. At most
/// [`MAX_REFUSAL_BODY`] bytes and [`REFUSAL_BODY_WAIT`]: whatever arrived
/// by then is kept, and a response hyper cannot read keeps what the
/// handshake read.
pub(super) async fn complete_refusal(
    mut response: Response<Option<Vec<u8>>>,
    returned: oneshot::Receiver<Stream>,
) -> Response<Option<Vec<u8>>> {
    let Ok(stream) = returned.await else {
        return response;
    };
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        response.status().as_u16(),
        response.status().canonical_reason().unwrap_or("")
    )
    .into_bytes();
    for (name, value) in response.headers() {
        head.extend_from_slice(name.as_str().as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value.as_bytes());
        head.extend_from_slice(b"\r\n");
    }
    head.extend_from_slice(b"\r\n");
    if let Some(received) = response.body() {
        head.extend_from_slice(received);
    }
    let replay = Replay {
        prefix: head,
        at: 0,
        stream,
        requested: false,
        reader: None,
    };
    let read = tokio::time::timeout(REFUSAL_BODY_WAIT, read_body(replay)).await;
    let body = match read {
        Ok(Some(body)) => body,
        // hyper could not read the response, or nothing arrived in time:
        // keep what the handshake read.
        Ok(None) | Err(_) => return response,
    };
    *response.body_mut() = (!body.is_empty()).then_some(body);
    response
}

/// The response's body as hyper's HTTP/1 client decodes it from `replay`,
/// up to [`MAX_REFUSAL_BODY`] bytes; what arrived before an error is kept.
/// `None` when hyper cannot read a response at all.
async fn read_body(replay: Replay) -> Option<Vec<u8>> {
    use http_body_util::BodyExt;

    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(replay))
            .await
            .ok()?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    // The request is never sent anywhere: `Replay` discards writes, and the
    // response it reads is the refusal already received.
    let request = tokio_tungstenite::tungstenite::http::Request::get("/")
        .body(http_body_util::Empty::<hyper::body::Bytes>::new())
        .ok()?;
    let result = async {
        let mut incoming = sender.send_request(request).await.ok()?.into_body();
        let mut body = Vec::new();
        while body.len() < MAX_REFUSAL_BODY {
            match incoming.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        body.extend_from_slice(&data);
                    }
                }
                Some(Err(_)) | None => break,
            }
        }
        body.truncate(MAX_REFUSAL_BODY);
        Some(body)
    }
    .await;
    driver.abort();
    result
}

/// The refusal's head and the body bytes already received, then the
/// connection; writes go nowhere. Nothing is read before hyper has written
/// its request: hyper refuses a response that comes before it.
struct Replay {
    prefix: Vec<u8>,
    at: usize,
    stream: Stream,
    requested: bool,
    reader: Option<std::task::Waker>,
}

impl AsyncRead for Replay {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.requested {
            this.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if this.at < this.prefix.len() {
            let n = buf.remaining().min(this.prefix.len() - this.at);
            buf.put_slice(&this.prefix[this.at..this.at + n]);
            this.at += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Replay {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.requested = true;
        if let Some(reader) = this.reader.take() {
            reader.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
