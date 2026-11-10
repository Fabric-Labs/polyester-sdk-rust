//! Signed requests are never forwarded to another origin by a redirect.
//!
//! The ConnectRPC transport and the realtime token client are hyper clients,
//! and neither hyper nor connectrpc follows redirects. These tests pin that:
//! an origin that answers a signed call with 307 or 308 and a Location on
//! another host or port, keeping the path, gets an error back, and the
//! destination receives nothing, in both wire formats. Each test runs its own
//! loopback servers and signs with a disposable Ed25519 key.

use buffa::Message as _;
use polyester::proto::auth::v1::MeResponse;
use polyester::transport::WireFormat;
use polyester::{Client, Config};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const KEY_ID: &str = "ak_redirect_test";
const SIGNED_HEADERS: [&str; 3] = ["x-api-key-id", "x-api-timestamp", "x-api-signature"];

fn disposable_key() -> String {
    polyester::auth::generate_ed25519_keypair().0
}

/// The lowercased request head of every request a server received.
type Received = Arc<Mutex<Vec<String>>>;

fn is_signed(head: &str) -> bool {
    head.lines().any(|line| {
        SIGNED_HEADERS
            .iter()
            .any(|name| line.starts_with(&format!("{name}:")))
    })
}

fn me_answer(head: &str) -> Vec<u8> {
    let (content_type, body) = if head.contains("content-type: application/json") {
        (
            "application/json",
            format!(r#"{{"accountId":"7","apiKeyId":"{KEY_ID}"}}"#).into_bytes(),
        )
    } else {
        let message = MeResponse {
            account_id: 7,
            api_key_id: Some(KEY_ID.into()),
            ..Default::default()
        };
        ("application/proto", message.encode_to_vec())
    };
    let mut out = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend(body);
    out
}

fn redirect_answer(status: u16, location: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .into_bytes()
}

/// Serves on `listener`, records each request head and answers it with
/// `answer`.
fn serve(
    listener: TcpListener,
    answer: impl Fn(&str) -> Vec<u8> + Send + Sync + 'static,
) -> Received {
    let received: Received = Arc::default();
    let log = received.clone();
    let answer = Arc::new(answer);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = log.clone();
            let answer = answer.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let end = buf
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .unwrap_or(buf.len());
                let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                log.lock().unwrap().push(head.clone());
                let _ = stream.write_all(&answer(&head)).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    received
}

fn path_of(head: &str) -> String {
    head.split_whitespace().nth(1).unwrap_or("/").to_owned()
}

/// The redirecting origin's and the destination's listeners: another port on
/// the same host, or the same port on another loopback host.
async fn listeners(host_only: bool) -> Option<(TcpListener, TcpListener)> {
    let redirecting = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let destination = if host_only {
        let port = redirecting.local_addr().expect("addr").port();
        // Skips where 127.0.0.2 is not a loopback address.
        TcpListener::bind(("127.0.0.2", port)).await.ok()?
    } else {
        TcpListener::bind("127.0.0.1:0").await.expect("bind")
    };
    Some((redirecting, destination))
}

fn client(api_url: String, wire_format: WireFormat) -> Client {
    Client::new(Config {
        api_url,
        api_key_id: Some(KEY_ID.into()),
        api_private_key: Some(disposable_key()),
        wire_format,
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
}

#[tokio::test]
async fn signed_call_is_not_forwarded_across_origins() {
    for status in [307u16, 308] {
        for host_only in [false, true] {
            for wire_format in [WireFormat::Json, WireFormat::Binary] {
                let label = format!("{status} host_only={host_only} {wire_format:?}");
                let Some((redirecting, destination)) = listeners(host_only).await else {
                    eprintln!("{label}: skipped, cannot listen on 127.0.0.2");
                    continue;
                };
                let redirect_url = format!("http://{}", redirecting.local_addr().unwrap());
                let target = format!("http://{}", destination.local_addr().unwrap());
                assert_ne!(redirect_url, target, "{label}");
                let delivered = serve(destination, me_answer);
                let redirected = serve(redirecting, move |head| {
                    redirect_answer(status, &format!("{target}{}", path_of(head)))
                });

                let result = client(redirect_url, wire_format).auth.me().await;

                let delivered = delivered.lock().unwrap().clone();
                assert!(
                    delivered.iter().all(|head| !is_signed(head)),
                    "{label}: destination received a signed request"
                );
                assert!(
                    delivered.is_empty(),
                    "{label}: destination received {delivered:?}"
                );
                assert_eq!(redirected.lock().unwrap().len(), 1, "{label}");
                assert!(
                    result.is_err(),
                    "{label}: Me succeeded although its only answer was a redirect"
                );
            }
        }
    }
}

/// Control: a signed call answered directly succeeds in both wire formats,
/// and the origin receives the signed request.
#[tokio::test]
async fn signed_call_without_redirect_succeeds() {
    for wire_format in [WireFormat::Json, WireFormat::Binary] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let received = serve(listener, me_answer);
        let me = client(api_url, wire_format)
            .auth
            .me()
            .await
            .unwrap_or_else(|err| panic!("{wire_format:?}: {err:?}"));
        assert_eq!(me.api_key_id.as_deref(), Some(KEY_ID));
        let received = received.lock().unwrap().clone();
        assert_eq!(received.len(), 1, "{wire_format:?}");
        assert!(is_signed(&received[0]), "{wire_format:?}");
    }
}
