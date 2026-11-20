//! The WebSocket clients open `wss://` connections in a process that never
//! built a `Client` and never installed a rustls crypto provider. Every TLS
//! connector in the crate names its provider, so none of them depends on a
//! process-level default, and the crate never installs one for the
//! application.
//!
//! This file is its own test binary, so no other test can install a provider
//! in its process first.

use std::sync::Arc;

use polyester::auth::Credentials;
use polyester::{marketstream, tradews};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A TLS peer that reads one ClientHello record, reports it, and refuses the
/// handshake with a fatal `handshake_failure` alert.
async fn refusing_tls_server() -> (u16, mpsc::UnboundedReceiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hellos, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _)) = listener.accept().await else {
                return;
            };
            let hellos = hellos.clone();
            tokio::spawn(async move {
                let mut header = [0u8; 5];
                if tcp.read_exact(&mut header).await.is_err() {
                    return;
                }
                let len = u16::from_be_bytes([header[3], header[4]]) as usize;
                let mut body = vec![0u8; len];
                if tcp.read_exact(&mut body).await.is_err() {
                    return;
                }
                let mut record = header.to_vec();
                record.extend_from_slice(&body);
                let _ = hellos.send(record);
                // Alert record: level fatal (2), description handshake_failure (40).
                let _ = tcp
                    .write_all(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28])
                    .await;
                let _ = tcp.shutdown().await;
            });
        }
    });
    (port, rx)
}

/// The record is a TLS handshake record holding a ClientHello for `localhost`.
fn assert_client_hello(record: &[u8]) {
    assert_eq!(record[0], 0x16, "TLS handshake record");
    assert_eq!(record[5], 0x01, "ClientHello");
    assert!(
        record
            .windows(b"localhost".len())
            .any(|w| w == b"localhost"),
        "server name indication names the host"
    );
}

async fn connect_market_stream(port: u16) -> polyester::Result<()> {
    let config = marketstream::Config::new(
        format!("wss://localhost:{port}/ws/market.sbe"),
        Arc::new(polyester::catalogs::Manager::new()),
    );
    marketstream::connect(config).await.map(drop)
}

async fn connect_trade_session(port: u16) -> polyester::Result<()> {
    let credentials = Credentials::new("ak_test", &"11".repeat(32)).unwrap();
    let config = tradews::Config::new(
        format!("wss://localhost:{port}/ws/trade.sbe"),
        credentials,
        Arc::new(polyester::catalogs::Manager::new()),
    );
    tradews::connect(config).await.map(drop)
}

#[tokio::test]
async fn wss_connects_without_a_client_or_a_process_provider() {
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "the test process starts with no rustls provider installed"
    );
    let (port, mut hellos) = refusing_tls_server().await;

    // No Client built: each stream reaches the TLS handshake and returns the
    // refusal as an error, where it panicked inside rustls.
    let err = connect_market_stream(port).await.unwrap_err();
    assert!(matches!(err, polyester::Error::Realtime { .. }), "{err:?}");
    assert_client_hello(&hellos.recv().await.unwrap());

    let err = connect_trade_session(port).await.unwrap_err();
    assert!(
        matches!(
            err,
            polyester::Error::Realtime { .. } | polyester::Error::TradeSession(_)
        ),
        "{err:?}"
    );
    assert_client_hello(&hellos.recv().await.unwrap());

    // A Client built afterwards works, and the streams still connect the
    // same way once it exists.
    let _client = polyester::Client::new(polyester::Config::default()).unwrap();
    connect_market_stream(port).await.unwrap_err();
    assert_client_hello(&hellos.recv().await.unwrap());
    connect_trade_session(port).await.unwrap_err();
    assert_client_hello(&hellos.recv().await.unwrap());

    // The crate chose its provider per connector and installed none for the
    // process, so an application remains free to install its own.
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("the application can still install its own provider");
    connect_market_stream(port).await.unwrap_err();
    assert_client_hello(&hellos.recv().await.unwrap());
}
