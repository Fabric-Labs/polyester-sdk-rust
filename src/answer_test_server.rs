//! Local servers for the tests of a failed answer whose body cannot be read
//! and of a redirect: the answer's metadata is in its headers, so every error
//! made from the answer carries it.
//!
//! The answering server counts only the request it expects, by method, path
//! and one header; a redirect's target counts every request for its own
//! path, whatever the method, as a followed redirect may change it. Both
//! read the whole request head first. Other processes probe local ports on a
//! shared machine (with a `HEAD /`, for example): such a request is dropped
//! and counts for nothing.

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

/// The request a server counts: its method, its path and one header it
/// carries, by name (in any case) and, when given, value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Expected {
    pub method: &'static str,
    pub path: &'static str,
    pub header: &'static str,
    pub value: Option<&'static str>,
}

/// Largest request head read; a longer one is not the expected request.
const MAX_HEAD: usize = 16 * 1024;

/// The request head that reached `stream`, read without taking it off the
/// socket: up to its blank line, within [`MAX_HEAD`] bytes and 2 s. `None`
/// when the peer goes away first, or sends no complete head in time.
async fn request_head(stream: &tokio::net::TcpStream) -> Option<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut buf = vec![0u8; MAX_HEAD];
        loop {
            let n = stream.peek(&mut buf).await.ok()?;
            if n == 0 {
                return None;
            }
            if let Some(end) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
                return Some(buf[..end + 4].to_vec());
            }
            if n == MAX_HEAD {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

/// The path of a redirect's target. No request but a followed redirect
/// asks for it: not the request redirected, nor a port probe.
const TARGET_PATH: &str = "/redirect-target";

/// Whether `head` is a request for the redirect's [`TARGET_PATH`], by any
/// method and with any query, parsed as HTTP/1.x.
fn is_for_target(head: &[u8]) -> bool {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    matches!(request.parse(head), Ok(httparse::Status::Complete(_)))
        && request
            .path
            .is_some_and(|path| path.split('?').next() == Some(TARGET_PATH))
}

/// Whether `head` is the `expected` request, parsed as HTTP/1.1.
fn is_expected(head: &[u8], expected: &Expected) -> bool {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    if !matches!(request.parse(head), Ok(httparse::Status::Complete(_))) {
        return false;
    }
    request.method == Some(expected.method)
        && request.path == Some(expected.path)
        && request.headers.iter().any(|header| {
            header.name.eq_ignore_ascii_case(expected.header)
                && expected
                    .value
                    .is_none_or(|value| header.value.eq_ignore_ascii_case(value.as_bytes()))
        })
}

/// The address of a server that answers the `expected` request with
/// `answer` (and accepts a WebSocket upgrade of `/connection/websocket` when
/// `upgrade`), and the number of requests it answered. Any other request is
/// dropped unanswered and uncounted.
pub(crate) async fn answering(
    answer: Answer,
    upgrade: bool,
    expected: Expected,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let answered = Arc::new(AtomicUsize::new(0));
    let count = answered.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let answer = answer.clone();
            let count = count.clone();
            tokio::spawn(async move {
                let Some(head) = request_head(&stream).await else {
                    return;
                };
                if upgrade && head.starts_with(b"GET /connection/websocket") {
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
                if !is_expected(&head, &expected) {
                    return;
                }
                let mut buf = vec![0u8; MAX_HEAD];
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

/// A redirect's target, which must get no request: the URL to redirect to
/// and the number of requests for it. Whatever a followed redirect sends
/// there counts (the request redirected, or a `GET` in its place, with or
/// without its headers); any other request, such as a port probe, does not.
pub(crate) async fn counting_target() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}{TARGET_PATH}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let count = count.clone();
            tokio::spawn(async move {
                if let Some(head) = request_head(&stream).await
                    && is_for_target(&head)
                {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });
    (url, requests)
}

/// A redirect's `Location` that does not parse as a URL.
pub(crate) const MALFORMED_LOCATION: &str = "http://[::1";

#[cfg(test)]
mod tests {
    use super::*;

    /// A port probe, another method or path, or a request without the
    /// expected header is not the expected request.
    #[test]
    fn only_the_expected_request_counts() {
        let expected = Expected {
            method: "POST",
            path: "/",
            header: "content-type",
            value: Some("application/json"),
        };
        assert!(is_expected(
            b"POST / HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n",
            &expected
        ));
        for other in [
            &b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n",
            b"POST /other HTTP/1.1\r\nContent-Type: application/json\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"POST / HTTP/1.1\r\nContent-Type: text/plain\r\n\r\n",
            b"not http\r\n\r\n",
        ] {
            assert!(
                !is_expected(other, &expected),
                "{}",
                String::from_utf8_lossy(other)
            );
        }
    }

    /// Any request for the target's path counts, whatever its method,
    /// query or headers; a port probe or another path does not.
    #[test]
    fn every_request_for_the_target_counts() {
        for followed in [
            &b"POST /redirect-target HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n"
                [..],
            b"GET /redirect-target HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET /redirect-target?q=1 HTTP/1.1\r\n\r\n",
            b"PUT /redirect-target HTTP/1.0\r\n\r\n",
        ] {
            assert!(
                is_for_target(followed),
                "{}",
                String::from_utf8_lossy(followed)
            );
        }
        for other in [
            &b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"POST /redirect-target/more HTTP/1.1\r\n\r\n",
            b"GET /redirect-target HTTP/9.9\r\n\r\n",
            b"not http\r\n\r\n",
        ] {
            assert!(!is_for_target(other), "{}", String::from_utf8_lossy(other));
        }
    }
}
