//! Local servers for the tests of a failed answer whose body cannot be read
//! and of a redirect: the answer's metadata is in its headers, so every error
//! made from the answer carries it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How the body of an answer arrives.
#[derive(Clone)]
pub(crate) enum AnswerBody {
    /// `Content-Length: 100`, then one byte and the connection closes.
    Truncated,
    /// A `Content-Length` body of this many bytes.
    Sized(usize),
    /// A chunked body of this many bytes.
    Chunked(usize),
    /// `Content-Length: 100`, and the body never arrives.
    Stalled,
    /// `Content-Length: 0`.
    Empty,
}

/// A failed answer: its status, extra headers and body.
#[derive(Clone)]
pub(crate) struct Answer {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: AnswerBody,
}

impl Answer {
    /// A 503 with `Retry-After: 30` and this body.
    pub(crate) fn unavailable(body: AnswerBody) -> Self {
        Self {
            status: 503,
            headers: vec![
                ("Content-Type", "application/json".to_owned()),
                ("Retry-After", "30".to_owned()),
            ],
            body,
        }
    }

    /// A 307 with `Retry-After: 30` to `location`.
    pub(crate) fn redirect(location: String) -> Self {
        Self {
            status: 307,
            headers: vec![("Location", location), ("Retry-After", "30".to_owned())],
            body: AnswerBody::Empty,
        }
    }
}

/// The address of a server that answers every request (but a WebSocket
/// upgrade, which it accepts when `upgrade`) with `answer`, and the number of
/// requests it answered.
pub(crate) async fn answering(answer: Answer, upgrade: bool) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let answered = Arc::new(AtomicUsize::new(0));
    let count = answered.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let answer = answer.clone();
            let count = count.clone();
            tokio::spawn(async move {
                let mut head = [0u8; 64];
                let n = stream.peek(&mut head).await.unwrap_or(0);
                if n == 0 || head[..n].starts_with(b"HEAD / ") {
                    return;
                }
                if upgrade && head[..n].starts_with(b"GET /connection/websocket") {
                    // Echo the requested subprotocol, as the server does.
                    let echo = |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        if let Some(protocol) = request.headers().get("sec-websocket-protocol") {
                            response
                                .headers_mut()
                                .insert("sec-websocket-protocol", protocol.clone());
                        }
                        Ok(response)
                    };
                    if let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, echo).await {
                        use futures_util::StreamExt;
                        while ws.next().await.is_some() {}
                    }
                    return;
                }
                let mut buf = vec![0u8; 16 * 1024];
                let _ = stream.read(&mut buf).await;
                count.fetch_add(1, Ordering::SeqCst);
                let mut head = format!("HTTP/1.1 {} Answer\r\n", answer.status);
                for (name, value) in &answer.headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                match answer.body {
                    AnswerBody::Truncated | AnswerBody::Stalled => {
                        head.push_str("Content-Length: 100\r\n");
                    }
                    AnswerBody::Sized(len) => head.push_str(&format!("Content-Length: {len}\r\n")),
                    AnswerBody::Chunked(_) => head.push_str("Transfer-Encoding: chunked\r\n"),
                    AnswerBody::Empty => head.push_str("Content-Length: 0\r\n"),
                }
                head.push_str("Connection: close\r\n\r\n");
                if stream.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                match answer.body {
                    AnswerBody::Truncated => {
                        let _ = stream.write_all(b"{").await;
                    }
                    AnswerBody::Sized(len) => {
                        let _ = stream.write_all(&vec![b' '; len]).await;
                    }
                    AnswerBody::Chunked(len) => {
                        let chunk = vec![b' '; 16 * 1024];
                        let mut sent = 0;
                        while sent < len {
                            let n = (len - sent).min(chunk.len());
                            let frame = format!("{n:x}\r\n");
                            if stream.write_all(frame.as_bytes()).await.is_err()
                                || stream.write_all(&chunk[..n]).await.is_err()
                                || stream.write_all(b"\r\n").await.is_err()
                            {
                                return;
                            }
                            sent += n;
                        }
                        let _ = stream.write_all(b"0\r\n\r\n").await;
                    }
                    AnswerBody::Stalled => {
                        // Until the client gives up and closes.
                        let mut sink = [0u8; 1];
                        let _ =
                            tokio::time::timeout(Duration::from_secs(30), stream.read(&mut sink))
                                .await;
                    }
                    AnswerBody::Empty => {}
                }
            });
        }
    });
    (addr, answered)
}

/// The address of a server that only counts the connections it gets: a
/// redirect's target, which must get none.
pub(crate) async fn counting_target() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    tokio::spawn(async move {
        while let Ok((_stream, _)) = listener.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    (addr, connections)
}

/// A redirect's `Location` that does not parse as a URL.
pub(crate) const MALFORMED_LOCATION: &str = "http://[::1";
