//! Token renewal on private Centrifugo connections, against a real local
//! Centrifugo-shaped socket and token API that issue short-lived tokens.

use super::*;
use crate::codecs::decode::ledger_transfer_batch_from_bytes;
use crate::codecs::scalars::format_id;
use crate::models::LedgerTransfer;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

const RENEWAL_ACCOUNT: u64 = 2_305_843_009_213_693_953;

pub(super) fn transfers_channel() -> String {
    format!(
        "private:ledger:transfers:{}:proto",
        format_id(RENEWAL_ACCOUNT)
    )
}

/// How the server answers a renewal command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RenewalAction {
    Answer,
    /// Answers with Centrifugo error 109, token expired.
    Refuse,
}

type TokenHook = Arc<dyn Fn(&str) -> (Duration, u16) + Send + Sync>;
type CommandHook = Arc<dyn Fn(&str) -> (Duration, RenewalAction) + Send + Sync>;

#[derive(Clone, Default)]
pub(super) struct RenewalServerConfig {
    pub unit: Duration,
    /// Defaults to three tenths of a unit.
    pub grace: Duration,
    /// In units; 0 issues a token that does not expire.
    pub conn_ttl: u32,
    pub sub_ttl: u32,
    pub publication: Option<Vec<u8>>,
    pub publish_every: Option<Duration>,
}

#[derive(Default)]
pub(super) struct Counters {
    pub conn_tokens: AtomicUsize,
    pub sub_tokens: AtomicUsize,
    pub accepted: AtomicUsize,
    pub subscribes: AtomicUsize,
    pub refreshes: AtomicUsize,
    pub sub_renewals: AtomicUsize,
    pub sub_expiries: AtomicUsize,
    pub conn_expiries: AtomicUsize,
    /// Tokens presented that were used before or never minted.
    pub reused: AtomicUsize,
    /// Renewals carrying a subscription token minted before the connection's
    /// own subscribe: work left over from an earlier connection.
    pub stale: AtomicUsize,
    pub published: AtomicUsize,
}

impl Counters {
    pub fn get(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

struct ServerState {
    cfg: RenewalServerConfig,
    counters: Counters,
    /// token -> used
    minted: Mutex<HashMap<String, bool>>,
    token_hook: Mutex<Option<TokenHook>>,
    command_hook: Mutex<Option<CommandHook>>,
    /// Open connections, by accept order; sending closes one.
    conns: Mutex<HashMap<usize, mpsc::UnboundedSender<()>>>,
}

/// A Centrifugo-shaped socket with its token API. It issues connection and
/// subscription tokens that expire ttl units after they are used, lets a
/// token run `grace` past its TTL like Centrifugo, and then ends the
/// subscription with 2501 or closes the connection with 3005. Every token it
/// mints is single use.
pub(super) struct RenewalServer {
    pub ws_url: String,
    pub api_url: String,
    state: Arc<ServerState>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for RenewalServer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl RenewalServer {
    pub async fn spawn(mut cfg: RenewalServerConfig) -> Self {
        if cfg.grace.is_zero() {
            cfg.grace = cfg.unit * 3 / 10;
        }
        let state = Arc::new(ServerState {
            cfg,
            counters: Counters::default(),
            minted: Mutex::default(),
            token_hook: Mutex::default(),
            command_hook: Mutex::default(),
            conns: Mutex::default(),
        });
        let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_url = format!("http://{}", api.local_addr().unwrap());
        let ws_url = format!("ws://{}", ws.local_addr().unwrap());
        let api_state = state.clone();
        let api_task = tokio::spawn(async move {
            while let Ok((stream, _)) = api.accept().await {
                tokio::spawn(serve_token(api_state.clone(), stream));
            }
        });
        let ws_state = state.clone();
        let ws_task = tokio::spawn(async move {
            let mut next = 0;
            while let Ok((stream, _)) = ws.accept().await {
                next += 1;
                tokio::spawn(serve_socket(ws_state.clone(), stream, next));
            }
        });
        Self {
            ws_url,
            api_url,
            state,
            tasks: vec![api_task, ws_task],
        }
    }

    pub fn counters(&self) -> &Counters {
        &self.state.counters
    }

    /// Runs before each token response: sleeps, then answers with the status.
    pub fn set_token_hook(&self, hook: impl Fn(&str) -> (Duration, u16) + Send + Sync + 'static) {
        *lock_unpoisoned(&self.state.token_hook) = Some(Arc::new(hook));
    }

    /// Sees every renewal command: sleeps, then answers as it decides.
    pub fn set_command_hook(
        &self,
        hook: impl Fn(&str) -> (Duration, RenewalAction) + Send + Sync + 'static,
    ) {
        *lock_unpoisoned(&self.state.command_hook) = Some(Arc::new(hook));
    }

    /// Closes every open connection from the server side; returns how many.
    pub fn drop_all(&self) -> usize {
        let conns = std::mem::take(&mut *lock_unpoisoned(&self.state.conns));
        for close in conns.values() {
            let _ = close.send(());
        }
        conns.len()
    }

    pub fn client(&self) -> Client {
        let credentials = Credentials::new(
            "ak_test",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        Client::new(
            self.ws_url.clone(),
            self.api_url.clone(),
            Some(credentials),
            Some(1000),
        )
        .allow_insecure_http(true)
    }
}

fn use_token(state: &ServerState, token: &str) -> bool {
    let mut minted = lock_unpoisoned(&state.minted);
    match minted.get_mut(token) {
        Some(used) if !*used => {
            *used = true;
            true
        }
        _ => {
            state.counters.reused.fetch_add(1, Ordering::SeqCst);
            false
        }
    }
}

/// The mint order of a token this server issued.
fn token_serial(token: &str) -> usize {
    token
        .split_once('-')
        .and_then(|(_, serial)| serial.parse().ok())
        .unwrap_or(0)
}

async fn serve_token(state: Arc<ServerState>, mut stream: tokio::net::TcpStream) {
    let mut buf = vec![0u8; 16 * 1024];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    let raw = String::from_utf8_lossy(&buf[..n]);
    let target = raw.split_whitespace().nth(1).unwrap_or("/");
    let path = target.split('?').next().unwrap_or(target);
    let kind = match path {
        "/v1/rt/token" => "conn",
        "/v1/rt/subscribe" => "sub",
        _ => {
            let _ = stream
                .write_all(b"HTTP/1.1 404 X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
            return;
        }
    };
    let serial = if kind == "conn" {
        &state.counters.conn_tokens
    } else {
        &state.counters.sub_tokens
    }
    .fetch_add(1, Ordering::SeqCst)
        + 1;
    let hook = lock_unpoisoned(&state.token_hook).clone();
    let (delay, status) = hook.map_or((Duration::ZERO, 200), |hook| hook(kind));
    tokio::time::sleep(delay).await;
    let body = if status == 200 {
        let token = format!("{kind}-{serial}");
        lock_unpoisoned(&state.minted).insert(token.clone(), false);
        format!(r#"{{"token":"{token}"}}"#)
    } else {
        r#"{"code":"permission_denied","message":"token refused"}"#.to_owned()
    };
    let response = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

#[derive(Debug, Default)]
struct Command {
    id: u32,
    kind: &'static str,
    channel: String,
    token: String,
}

fn varint(bytes: &[u8], at: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[*at];
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

/// Parses a frame of length-delimited Commands. Connect and refresh carry
/// token = 1; subscribe and sub_refresh carry channel = 1 and token = 2.
fn parse_commands(frame: &[u8]) -> Vec<Command> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < frame.len() {
        let len = varint(frame, &mut at) as usize;
        let record = &frame[at..at + len];
        at += len;
        if record.is_empty() {
            out.push(Command {
                kind: "pong",
                ..Default::default()
            });
            continue;
        }
        let mut cmd = Command::default();
        let mut r = 0;
        while r < record.len() {
            let key = varint(record, &mut r);
            let (field, wire) = (key >> 3, key & 7);
            if wire == 0 {
                let value = varint(record, &mut r);
                if field == 1 {
                    cmd.id = value as u32;
                }
                continue;
            }
            assert_eq!(wire, 2, "unexpected wire type in command");
            let len = varint(record, &mut r) as usize;
            let body = &record[r..r + len];
            r += len;
            cmd.kind = match field {
                4 => "connect",
                5 => "subscribe",
                14 => "refresh",
                15 => "sub_refresh",
                other => panic!("unexpected command field {other}"),
            };
            let mut b = 0;
            while b < body.len() {
                let key = varint(body, &mut b);
                let (num, wire) = (key >> 3, key & 7);
                if wire == 0 {
                    varint(body, &mut b);
                    continue;
                }
                let len = varint(body, &mut b) as usize;
                let value = String::from_utf8_lossy(&body[b..b + len]).into_owned();
                b += len;
                match (cmd.kind, num) {
                    ("connect" | "refresh", 1) => cmd.token = value,
                    (_, 1) => cmd.channel = value,
                    (_, 2) => cmd.token = value,
                    _ => {}
                }
            }
        }
        out.push(cmd);
    }
    out
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn put_bytes(out: &mut Vec<u8>, field: u64, value: &[u8]) {
    put_varint(out, (field << 3) | 2);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_uint(out: &mut Vec<u8>, field: u64, value: u64) {
    put_varint(out, field << 3);
    put_varint(out, value);
}

fn delimit(reply: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, reply.len() as u64);
    out.extend(reply);
    out
}

fn expiry_result(expires_field: u64, ttl_field: u64, ttl: u32) -> Vec<u8> {
    let mut out = Vec::new();
    if ttl > 0 {
        put_uint(&mut out, expires_field, 1);
        put_uint(&mut out, ttl_field, u64::from(ttl));
    }
    out
}

fn reply_frame(id: u32, result_field: u64, result: &[u8]) -> Vec<u8> {
    let mut reply = Vec::new();
    put_uint(&mut reply, 1, u64::from(id));
    put_bytes(&mut reply, result_field, result);
    delimit(reply)
}

fn error_frame(id: u32, code: u32, message: &str) -> Vec<u8> {
    let mut error = Vec::new();
    put_uint(&mut error, 1, u64::from(code));
    put_bytes(&mut error, 2, message.as_bytes());
    let mut reply = Vec::new();
    put_uint(&mut reply, 1, u64::from(id));
    put_bytes(&mut reply, 2, &error);
    delimit(reply)
}

fn unsubscribe_push(channel: &str, code: u32, reason: &str) -> Vec<u8> {
    let mut unsubscribe = Vec::new();
    put_uint(&mut unsubscribe, 2, u64::from(code));
    put_bytes(&mut unsubscribe, 3, reason.as_bytes());
    let mut push = Vec::new();
    put_bytes(&mut push, 2, channel.as_bytes());
    put_bytes(&mut push, 7, &unsubscribe);
    let mut reply = Vec::new();
    put_bytes(&mut reply, 4, &push);
    delimit(reply)
}

fn publication_frame(data: &[u8]) -> Vec<u8> {
    let mut publication = Vec::new();
    put_bytes(&mut publication, 4, data);
    let mut push = Vec::new();
    put_bytes(&mut push, 4, &publication);
    let mut reply = Vec::new();
    put_bytes(&mut reply, 4, &push);
    delimit(reply)
}

fn deadline(cfg: &RenewalServerConfig, ttl: u32) -> Option<Instant> {
    (ttl > 0).then(|| Instant::now() + cfg.unit * ttl + cfg.grace)
}

async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

async fn serve_socket(state: Arc<ServerState>, stream: tokio::net::TcpStream, n: usize) {
    let accepted = tokio_tungstenite::accept_hdr_async(
        stream,
        |_: &_, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            response.headers_mut().insert(
                "sec-websocket-protocol",
                CENTRIFUGO_PROTOBUF_SUBPROTOCOL.parse().unwrap(),
            );
            Ok(response)
        },
    )
    .await;
    let Ok(ws) = accepted else { return };
    state.counters.accepted.fetch_add(1, Ordering::SeqCst);
    let (close_tx, mut close_rx) = mpsc::unbounded_channel();
    lock_unpoisoned(&state.conns).insert(n, close_tx);
    let (mut sink, mut source) = ws.split();
    let cfg = state.cfg.clone();
    let mut commands = VecDeque::new();
    let mut channel = String::new();
    let mut subscribed_with = 0;
    let mut phase = 0; // 0 awaiting connect, 1 awaiting subscribe, 2 running
    let mut conn_expires = None;
    let mut sub_expires = None;
    let mut publish = cfg.publish_every.map(tokio::time::interval);
    loop {
        if let Some(cmd) = commands.pop_front() {
            let cmd: Command = cmd;
            if cmd.kind == "pong" {
                continue;
            }
            let frame = match (phase, cmd.kind) {
                (0, "connect") if use_token(&state, &cmd.token) => {
                    phase = 1;
                    conn_expires = deadline(&cfg, cfg.conn_ttl);
                    reply_frame(cmd.id, 5, &expiry_result(3, 4, cfg.conn_ttl))
                }
                (1, "subscribe") if use_token(&state, &cmd.token) => {
                    phase = 2;
                    state.counters.subscribes.fetch_add(1, Ordering::SeqCst);
                    channel = cmd.channel.clone();
                    subscribed_with = token_serial(&cmd.token);
                    sub_expires = deadline(&cfg, cfg.sub_ttl);
                    reply_frame(cmd.id, 6, &expiry_result(1, 2, cfg.sub_ttl))
                }
                (2, "refresh" | "sub_refresh") => {
                    let hook = lock_unpoisoned(&state.command_hook).clone();
                    let (delay, action) = hook
                        .map_or((Duration::ZERO, RenewalAction::Answer), |hook| {
                            hook(cmd.kind)
                        });
                    tokio::time::sleep(delay).await;
                    match action {
                        RenewalAction::Refuse => error_frame(cmd.id, 109, "token expired"),
                        RenewalAction::Answer if cmd.kind == "sub_refresh" => {
                            if token_serial(&cmd.token) <= subscribed_with {
                                state.counters.stale.fetch_add(1, Ordering::SeqCst);
                            }
                            if cmd.channel != channel || !use_token(&state, &cmd.token) {
                                error_frame(cmd.id, 109, "token expired")
                            } else {
                                state.counters.sub_renewals.fetch_add(1, Ordering::SeqCst);
                                sub_expires = deadline(&cfg, cfg.sub_ttl);
                                reply_frame(cmd.id, 15, &expiry_result(1, 2, cfg.sub_ttl))
                            }
                        }
                        RenewalAction::Answer => {
                            if !use_token(&state, &cmd.token) {
                                error_frame(cmd.id, 109, "token expired")
                            } else {
                                state.counters.refreshes.fetch_add(1, Ordering::SeqCst);
                                conn_expires = deadline(&cfg, cfg.conn_ttl);
                                reply_frame(cmd.id, 14, &expiry_result(3, 4, cfg.conn_ttl))
                            }
                        }
                    }
                }
                _ => break,
            };
            if sink.send(Message::Binary(frame.into())).await.is_err() {
                break;
            }
            continue;
        }
        tokio::select! {
            message = source.next() => match message {
                Some(Ok(Message::Binary(frame))) => commands.extend(parse_commands(&frame)),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = close_rx.recv() => break,
            _ = until(sub_expires) => {
                state.counters.sub_expiries.fetch_add(1, Ordering::SeqCst);
                sub_expires = None;
                let frame = unsubscribe_push(&channel, 2501, "subscription expired");
                if sink.send(Message::Binary(frame.into())).await.is_err() {
                    break;
                }
            }
            _ = until(conn_expires) => {
                state.counters.conn_expiries.fetch_add(1, Ordering::SeqCst);
                let _ = sink
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::from(3005),
                        reason: "expired".into(),
                    })))
                    .await;
                break;
            }
            _ = async {
                match publish.as_mut() {
                    Some(interval) => { interval.tick().await; }
                    None => std::future::pending::<()>().await,
                }
            }, if phase == 2 => {
                if let Some(data) = &cfg.publication {
                    if sink.send(Message::Binary(publication_frame(data).into())).await.is_err() {
                        break;
                    }
                    state.counters.published.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
    lock_unpoisoned(&state.conns).remove(&n);
    let _ = sink.close().await;
}

pub(super) fn transfer_publication() -> Vec<u8> {
    use buffa::Message as _;
    crate::proto::ledger::read::v1::TransferRowBatch {
        events: vec![crate::proto::ledger::read::v1::TransferRow {
            asset_id: 2,
            transfer_id: "7".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec()
}

pub(super) async fn subscribe_transfers(client: &Client) -> TypedSubscription<LedgerTransfer> {
    client
        .subscribe_proto_batch(&transfers_channel(), ledger_transfer_batch_from_bytes)
        .await
        .expect("subscribe transfers")
}

pub(super) fn record_errors(sub: &TypedSubscription<LedgerTransfer>) -> Arc<Mutex<Vec<Error>>> {
    let errors: Arc<Mutex<Vec<Error>>> = Arc::default();
    let sink = errors.clone();
    sub.set_on_error(move |err| lock_unpoisoned(&sink).push(err));
    errors
}

/// A transfer subscription whose subscription token lives one second stays on
/// its first connection and subscription across three token lifetimes: the
/// client renews the token before the server ends the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_subscription_renews_short_lived_token_without_reconnect() {
    let srv = RenewalServer::spawn(RenewalServerConfig {
        unit: Duration::from_secs(1),
        sub_ttl: 1,
        publication: Some(transfer_publication()),
        publish_every: Some(Duration::from_millis(100)),
        ..Default::default()
    })
    .await;
    let client = srv.client();
    let mut sub = subscribe_transfers(&client).await;
    let errors = record_errors(&sub);

    let started = Instant::now();
    let mut received_after = 0;
    while started.elapsed() < Duration::from_millis(3600) {
        if timeout(Duration::from_millis(200), sub.recv())
            .await
            .ok()
            .flatten()
            .is_some()
            && started.elapsed() > Duration::from_secs(3)
        {
            received_after += 1;
        }
    }
    let c = srv.counters();
    let mut failures = Vec::new();
    let expiries = Counters::get(&c.sub_expiries);
    if expiries != 0 {
        failures.push(format!(
            "server ended the subscription {expiries} times for an expired token"
        ));
    }
    let accepted = Counters::get(&c.accepted);
    if accepted != 1 {
        failures.push(format!("client opened {accepted} connections, want 1"));
    }
    let subscribes = Counters::get(&c.subscribes);
    if subscribes != 1 {
        failures.push(format!("client subscribed {subscribes} times, want 1"));
    }
    let renewals = Counters::get(&c.sub_renewals);
    if renewals < 3 {
        failures.push(format!(
            "client renewed the subscription token {renewals} times in 3.6 token lifetimes, want at least 3"
        ));
    }
    let reused = Counters::get(&c.reused);
    if reused != 0 {
        failures.push(format!("client presented {reused} used or unknown tokens"));
    }
    if sub.resubscribes() != 0 {
        failures.push(format!("resubscribes() = {}, want 0", sub.resubscribes()));
    }
    let reported = lock_unpoisoned(&errors).clone();
    if !reported.is_empty() {
        failures.push(format!("healthy renewal reported {reported:?}"));
    }
    if received_after == 0 {
        failures.push("no transfer delivered after the renewals".to_owned());
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A connection token that lives one second is renewed on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_connection_renews_short_lived_token_without_reconnect() {
    let srv = RenewalServer::spawn(RenewalServerConfig {
        unit: Duration::from_secs(1),
        conn_ttl: 1,
        ..Default::default()
    })
    .await;
    let client = srv.client();
    let _sub = subscribe_transfers(&client).await;

    tokio::time::sleep(Duration::from_millis(3600)).await;
    let c = srv.counters();
    let mut failures = Vec::new();
    let expiries = Counters::get(&c.conn_expiries);
    if expiries != 0 {
        failures.push(format!(
            "server closed {expiries} connections for an expired token"
        ));
    }
    let accepted = Counters::get(&c.accepted);
    if accepted != 1 {
        failures.push(format!("client opened {accepted} connections, want 1"));
    }
    let refreshes = Counters::get(&c.refreshes);
    if refreshes < 3 {
        failures.push(format!(
            "client renewed the connection token {refreshes} times in 3.6 token lifetimes, want at least 3"
        ));
    }
    let reused = Counters::get(&c.reused);
    if reused != 0 {
        failures.push(format!("client presented {reused} used or unknown tokens"));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn wait_for(what: &str, mut pred: impl FnMut() -> bool, within: Duration) {
    let started = Instant::now();
    while !pred() {
        assert!(started.elapsed() < within, "{what} not within {within:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn subscription_refusal(err: &Error) -> bool {
    err.to_string()
        .contains("subscription token renewal refused: centrifugo error 109")
}

/// A renewal the token API or the server refuses is reported. The server then
/// ends the subscription at expiry, the client resubscribes with a fresh token,
/// says so through `resubscribes`, and renews on the new connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_renewal_refused_reports_then_resubscribes() {
    let unit = Duration::from_millis(100);
    type Refusal = fn(&Error) -> bool;
    let token_api_refuses: Refusal = auth::is_refusal;
    let server_refuses: Refusal = subscription_refusal;
    for (name, refused_by_api, want) in [
        ("token API refuses", true, token_api_refuses),
        ("server refuses", false, server_refuses),
    ] {
        let srv = RenewalServer::spawn(RenewalServerConfig {
            unit,
            sub_ttl: 2,
            ..Default::default()
        })
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        if refused_by_api {
            // The second subscription token is the first renewal.
            srv.set_token_hook(move |kind| {
                let refuse = kind == "sub" && calls.fetch_add(1, Ordering::SeqCst) + 1 == 2;
                (Duration::ZERO, if refuse { 403 } else { 200 })
            });
        } else {
            srv.set_command_hook(move |kind| {
                let refuse = kind == "sub_refresh" && calls.fetch_add(1, Ordering::SeqCst) == 0;
                let action = if refuse {
                    RenewalAction::Refuse
                } else {
                    RenewalAction::Answer
                };
                (Duration::ZERO, action)
            });
        }
        let mut client = srv.client();
        client.renewal_unit = unit;
        let sub = subscribe_transfers(&client).await;
        let errors = record_errors(&sub);
        let c = srv.counters();

        wait_for(
            name,
            || sub.resubscribes() == 1 && Counters::get(&c.sub_renewals) >= 2,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(Counters::get(&c.sub_expiries), 1, "{name}: expiries");
        assert_eq!(Counters::get(&c.accepted), 2, "{name}: connections");
        let reported = lock_unpoisoned(&errors).clone();
        assert!(
            reported.first().is_some_and(want),
            "{name}: first reported error {reported:?}, want the refused renewal"
        );
        assert!(
            reported
                .get(1)
                .is_some_and(|err| err.to_string().contains("code 2501")),
            "{name}: reported {reported:?}, want the server's 2501 after the refusal"
        );
        assert!(sub.is_alive(), "{name}: subscription ended");
    }
}

/// A connection the server drops reconnects with fresh tokens and renews them
/// on the new connection; nothing from the old connection reaches it.
/// The connection token's lifetime runs from the connect reply, not from the
/// end of setup: a slow subscription token must not push the first connection
/// renewal past the server's expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_renewal_counts_from_connect_reply() {
    let unit = Duration::from_millis(200);
    let srv = RenewalServer::spawn(RenewalServerConfig {
        unit,
        grace: Duration::from_millis(60),
        conn_ttl: 2,
        ..Default::default()
    })
    .await;
    let sub_calls = Arc::new(AtomicUsize::new(0));
    // Only the setup's subscription token is slow, by more than the grace.
    srv.set_token_hook(move |kind| {
        let slow = kind == "sub" && sub_calls.fetch_add(1, Ordering::SeqCst) == 0;
        let delay = if slow {
            Duration::from_millis(150)
        } else {
            Duration::ZERO
        };
        (delay, 200)
    });
    let mut client = srv.client();
    client.renewal_unit = unit;
    let sub = subscribe_transfers(&client).await;

    tokio::time::sleep(unit * 8).await;
    let c = srv.counters();
    let mut failures = Vec::new();
    let expiries = Counters::get(&c.conn_expiries);
    if expiries != 0 {
        failures.push(format!(
            "server closed {expiries} connections for an expired connection token"
        ));
    }
    let accepted = Counters::get(&c.accepted);
    if accepted != 1 {
        failures.push(format!("client opened {accepted} connections, want 1"));
    }
    let refreshes = Counters::get(&c.refreshes);
    if refreshes < 3 {
        failures.push(format!(
            "client renewed the connection token {refreshes} times, want at least 3"
        ));
    }
    if sub.resubscribes() != 0 {
        failures.push(format!("resubscribes() = {}, want 0", sub.resubscribes()));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_renewal_continues_after_disconnect() {
    let unit = Duration::from_millis(100);
    let srv = RenewalServer::spawn(RenewalServerConfig {
        unit,
        conn_ttl: 2,
        sub_ttl: 1,
        ..Default::default()
    })
    .await;
    let mut client = srv.client();
    client.renewal_unit = unit;
    let sub = subscribe_transfers(&client).await;
    let c = srv.counters();

    let within = Duration::from_secs(5);
    wait_for(
        "first renewal",
        || Counters::get(&c.sub_renewals) >= 1,
        within,
    )
    .await;
    assert_eq!(srv.drop_all(), 1);
    wait_for("resubscribe", || Counters::get(&c.subscribes) == 2, within).await;
    let (renewed, refreshed) = (Counters::get(&c.sub_renewals), Counters::get(&c.refreshes));
    wait_for(
        "renewals on the new connection",
        || Counters::get(&c.sub_renewals) >= renewed + 2 && Counters::get(&c.refreshes) > refreshed,
        within,
    )
    .await;
    assert_eq!(sub.resubscribes(), 1);
    assert_eq!(
        Counters::get(&c.sub_expiries) + Counters::get(&c.conn_expiries),
        0
    );
    assert_eq!(Counters::get(&c.reused) + Counters::get(&c.stale), 0);
}

/// SplitMix64: a small seeded generator, so a failing run replays.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn duration(&mut self, max: Duration) -> Duration {
        Duration::from_nanos(self.below(max.as_nanos() as u64 + 1))
    }
}

/// Renewals race reconnects: token fetches and renewal answers take random
/// time, and the server drops the connection at random moments, so a drop
/// lands before, during or after a renewal. Whatever the order, a healthy
/// client never lets a token expire, never sends a token twice or on a
/// connection it was not fetched for, reconnects only for the drops, and stops
/// renewing once closed. `RENEWAL_SEED` replays a run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_renewal_interleaves_with_reconnects() {
    let seed = std::env::var("RENEWAL_SEED")
        .ok()
        .map(|seed| seed.parse::<u64>().expect("RENEWAL_SEED is a u64"))
        .unwrap_or_else(|| OsRng.next_u64());
    eprintln!("RENEWAL_SEED={seed}");
    let mut seeds = SplitMix(seed);
    for iteration in 0..8 {
        let iteration_seed = seeds.next();
        eprintln!("iteration {iteration} seed {iteration_seed}");
        run_renewal_interleaving(iteration_seed).await;
    }
}

async fn run_renewal_interleaving(seed: u64) {
    let unit = Duration::from_millis(40);
    let mut rng = SplitMix(seed);
    let cfg = RenewalServerConfig {
        unit,
        // Grace covers the slowest token fetch plus the slowest answer.
        grace: unit * 3,
        conn_ttl: rng.below(3) as u32,
        sub_ttl: 1 + rng.below(2) as u32,
        ..Default::default()
    };
    let srv = RenewalServer::spawn(cfg.clone()).await;
    let shared = Arc::new(Mutex::new(SplitMix(seed ^ 0x5555)));
    let token_rng = shared.clone();
    srv.set_token_hook(move |_| (lock_unpoisoned(&token_rng).duration(unit), 200));
    let command_rng = shared.clone();
    srv.set_command_hook(move |_| {
        (
            lock_unpoisoned(&command_rng).duration(unit / 2),
            RenewalAction::Answer,
        )
    });
    let mut client = srv.client();
    client.renewal_unit = unit;
    let sub = subscribe_transfers(&client).await;
    let c = srv.counters();
    let within = Duration::from_secs(5);

    let drops = 1 + rng.below(2) as usize;
    for drop in 0..drops {
        tokio::time::sleep(rng.duration(unit * 4)).await;
        assert_eq!(srv.drop_all(), 1, "seed {seed}: drop {drop}");
        wait_for(
            "resubscribe",
            || Counters::get(&c.subscribes) == drop + 2,
            within,
        )
        .await;
    }
    // Each renewal waits a TTL plus the injected delays; two more of each
    // show renewal running on the last connection.
    let (renewed, refreshed) = (Counters::get(&c.sub_renewals), Counters::get(&c.refreshes));
    wait_for(
        "renewals after the last reconnect",
        || {
            Counters::get(&c.sub_renewals) >= renewed + 2
                && (cfg.conn_ttl == 0 || Counters::get(&c.refreshes) >= refreshed + 2)
        },
        within,
    )
    .await;

    let context = format!(
        "seed {seed}, conn_ttl {}, sub_ttl {}",
        cfg.conn_ttl, cfg.sub_ttl
    );
    assert_eq!(
        Counters::get(&c.sub_expiries) + Counters::get(&c.conn_expiries),
        0,
        "tokens expired on a healthy client ({context})"
    );
    assert_eq!(
        Counters::get(&c.reused) + Counters::get(&c.stale),
        0,
        "used or stale tokens ({context})"
    );
    assert_eq!(
        Counters::get(&c.accepted),
        drops + 1,
        "connections ({context})"
    );
    assert_eq!(sub.resubscribes(), drops as u64, "resubscribes ({context})");
    assert!(sub.is_alive(), "subscription ended ({context})");

    sub.close();
    wait_for("subscription task to end", || !sub.is_alive(), within).await;
    let settled = Counters::get(&c.sub_tokens) + Counters::get(&c.conn_tokens);
    tokio::time::sleep(unit * 4).await;
    assert_eq!(
        Counters::get(&c.sub_tokens) + Counters::get(&c.conn_tokens),
        settled,
        "token fetched after close ({context})"
    );
}
