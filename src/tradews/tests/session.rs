//! The session against a local stand-in server: the signed upgrade and the
//! cancel-on-disconnect opt-in, readiness only after the subscription is
//! confirmed, the connection's seq, the timer gate, deadlines, missing answers
//! and pongs, unknown outcomes, typed refusals and a full event queue. No
//! frame leaves this machine.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signature, VerifyingKey};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use crate::auth::Credentials;
use crate::errors::Error;
use crate::tradews::codec::HEADER;
use crate::tradews::sbe::{
    WriteBuf, batch_cancel_ack_codec, batch_create_ack_codec, batch_item_status,
    batch_replace_ack_codec, cancel_all_after_ack_codec, cancel_all_after_status,
    command_ack_codec, command_reject_codec, command_type, event_channels, order_event_codec,
    order_status, pong_codec, reject_code, subscription_ack_codec,
};
use crate::tradews::{
    BatchCancelOrders, BatchCreateOrders, CancelAllOrders, CancelItem, CancelReason, Command,
    Config, Deadline, Event, NotSent, OrderRef, OrderStatus, PostOnlyOrder, RejectCode, Side,
    TradeError, TradeSession, connect,
};
use crate::types::{Price, Quantity, QuantityDomain};

// A fixed test key: the seed's bytes are 1..=32.
const SEED_HEX: &str = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
const WAIT: Duration = Duration::from_secs(5);

/// How the stand-in serves one connection.
#[derive(Clone)]
struct Script {
    ack_subscribe: bool,
    /// Answer the Subscribe with a CommandAck instead.
    misanswer_subscribe: bool,
    answer_pings: bool,
    answer_commands: bool,
    /// A `CancelAllAfter` that arms is answered ARMED, expiring this long
    /// after now.
    timer_expires_in: Duration,
    /// Close the socket after this many command answers.
    close_after: Option<usize>,
    /// Echo the cancel-on-disconnect opt-in on the 101.
    echo_cod: bool,
    /// Refuse the upgrade with this status and body.
    refuse: Option<(u16, &'static str)>,
    /// Refuse every command with this code instead of acknowledging it.
    reject: Option<reject_code::RejectCode>,
    /// Frames sent right after the SubscriptionAck.
    after_ready: Vec<Vec<u8>>,
    /// Close the socket, unanswered, on a frame of this template.
    close_on: Option<u16>,
    /// Answer every `CancelAllAfter` after the first this much later.
    delay_later_timers: Option<Duration>,
    /// Stop reading the socket after the SubscriptionAck, with a small
    /// receive buffer, so the client's writes back up.
    stall_after_ready: bool,
}

fn serve() -> Script {
    Script {
        ack_subscribe: true,
        misanswer_subscribe: false,
        answer_pings: true,
        answer_commands: true,
        timer_expires_in: Duration::from_secs(10),
        close_after: None,
        echo_cod: true,
        refuse: None,
        reject: None,
        after_ready: Vec::new(),
        close_on: None,
        delay_later_timers: None,
        stall_after_ready: false,
    }
}

/// What one connection saw: its upgrade headers and each client frame's
/// (template, seq, correlation ID).
#[derive(Clone, Debug, Default)]
struct Seen {
    headers: Vec<(String, String)>,
    path: String,
    frames: Vec<(u16, u64, u64)>,
}

type Record = Arc<Mutex<Vec<Seen>>>;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn subscription_ack(seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + subscription_ack_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = subscription_ack_codec::SubscriptionAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.seq(seq);
    e.channels(event_channels::EventChannels(0b111));
    buf
}

fn pong(seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + pong_codec::SBE_BLOCK_LENGTH as usize + 4];
    let mut e = pong_codec::PongEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.seq(seq);
    e.payload(b"");
    buf
}

fn ack(seq: u64, correlation_id: u64, kind: command_type::CommandType) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + command_ack_codec::SBE_BLOCK_LENGTH as usize + 4];
    let mut e = command_ack_codec::CommandAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.seq(seq);
    e.correlation_id(correlation_id);
    e.command_type(kind);
    e.order_id(seq * 1000);
    e.ts_ns(now_ns());
    e.cancelled_orders_encoder(0, Default::default());
    buf
}

fn reject(seq: u64, correlation_id: u64, code: reject_code::RejectCode) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + command_reject_codec::SBE_BLOCK_LENGTH as usize + 4];
    let mut e = command_reject_codec::CommandRejectEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.seq(seq);
    e.correlation_id(correlation_id);
    e.code(code);
    e.ts_ns(now_ns());
    e.rate_limit_retry_after_ns(250_000_000);
    e.detail(b"");
    buf
}

fn timer_ack(seq: u64, correlation_id: u64, timeout_sec: u32, expires_in: Duration) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + cancel_all_after_ack_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = cancel_all_after_ack_codec::CancelAllAfterAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    let now = now_ns();
    e.seq(seq);
    e.correlation_id(correlation_id);
    if timeout_sec == 0 {
        e.status(cancel_all_after_status::CancelAllAfterStatus::DISABLED);
    } else {
        e.status(cancel_all_after_status::CancelAllAfterStatus::ARMED);
        e.timeout_sec(timeout_sec);
        e.expires_at_ts_ns(now + expires_in.as_nanos() as u64);
    }
    e.ts_ns(now);
    buf
}

macro_rules! batch_ack {
    ($codec:ident :: $encoder:ident, $entry:expr, $seq:expr, $corr:expr, |$g:ident| $item:block) => {{
        let mut buf = vec![0u8; HEADER + $codec::SBE_BLOCK_LENGTH as usize + 4 + $entry];
        let mut e = $codec::$encoder::default()
            .wrap(WriteBuf::new(&mut buf), HEADER)
            .header(0)
            .parent()
            .unwrap();
        e.seq($seq);
        e.correlation_id($corr);
        e.ts_ns(now_ns());
        let mut g = e.results_encoder(1, Default::default());
        g.advance().unwrap();
        g.status(batch_item_status::BatchItemStatus::ACCEPTED);
        let $g = &mut g;
        $item
            buf
    }};
}

fn order_event(order_id: u64, cancel: u8) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER + order_event_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = order_event_codec::OrderEventEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.symbol_id(7);
    e.order_id(order_id);
    e.version(2);
    e.status(order_status::OrderStatus::CANCELED);
    e.event_ts_ns(now_ns());
    e.gateway_ts_ns(now_ns());
    buf[HEADER + crate::tradews::codec::ORDER_CANCEL_REASON] = cancel;
    buf
}

fn answer(script: &Script, template: u16, seq: u64, corr: u64, frame: &[u8]) -> Option<Vec<u8>> {
    use command_type::CommandType as T;
    if template == 6 {
        return if script.misanswer_subscribe {
            Some(ack(seq, corr, T::CREATE_ORDER))
        } else {
            script.ack_subscribe.then(|| subscription_ack(seq))
        };
    }
    if template == 1 {
        return script.answer_pings.then(|| pong(seq));
    }
    if !script.answer_commands {
        return None;
    }
    if let Some(code) = script.reject {
        return Some(reject(seq, corr, code));
    }
    Some(match template {
        2 => ack(seq, corr, T::CREATE_ORDER),
        3 => ack(seq, corr, T::CANCEL_ORDER),
        4 => ack(seq, corr, T::MODIFY_ORDER),
        5 => ack(seq, corr, T::CANCEL_ALL_ORDERS),
        7 => batch_ack!(
            batch_create_ack_codec::BatchCreateAckEncoder,
            35,
            seq,
            corr,
            |g| {
                g.order_id(seq * 1000);
            }
        ),
        8 => batch_ack!(
            batch_replace_ack_codec::BatchReplaceAckEncoder,
            20,
            seq,
            corr,
            |g| {
                g.old_order_id(seq * 1000);
            }
        ),
        9 => batch_ack!(
            batch_cancel_ack_codec::BatchCancelAckEncoder,
            11,
            seq,
            corr,
            |g| {
                g.order_id(seq * 1000);
            }
        ),
        10 => {
            let timeout = u32::from_le_bytes(frame[HEADER + 16..HEADER + 20].try_into().unwrap());
            timer_ack(seq, corr, timeout, script.timer_expires_in)
        }
        _ => return None,
    })
}

/// Starts the stand-in; returns its URL and what each connection saw.
// The handshake callback's error type is tungstenite's HTTP response.
#[allow(clippy::result_large_err)]
async fn stand_in(scripts: Vec<Script>) -> (String, Record) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    if scripts.iter().any(|s| s.stall_after_ready) {
        socket.set_recv_buffer_size(4096).unwrap();
    }
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(16).unwrap();
    let url = format!("ws://{}/ws/trade.sbe", listener.local_addr().unwrap());
    let record: Record = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&record);
    tokio::spawn(async move {
        let mut scripts = scripts.into_iter();
        let mut script = scripts.next();
        while let Some(current) = script.clone() {
            let (tcp, _) = listener.accept().await.unwrap();
            let captured: Arc<Mutex<Seen>> = Arc::default();
            let capture = Arc::clone(&captured);
            let plan = current.clone();
            let callback = move |request: &Request, mut response: Response| {
                let mut c = capture.lock().unwrap();
                c.path = request.uri().path().to_string();
                c.headers = request
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect();
                if let Some((status, body)) = plan.refuse {
                    let mut refusal = ErrorResponse::new(Some(body.to_string()));
                    *refusal.status_mut() = StatusCode::from_u16(status).unwrap();
                    return Err(refusal);
                }
                if plan.echo_cod
                    && let Some(grace) = request.headers().get("x-cancel-on-disconnect-grace-sec")
                {
                    let h = response.headers_mut();
                    h.insert(
                        "x-cancel-on-disconnect",
                        HeaderValue::from_static("ACCOUNT"),
                    );
                    h.insert("x-cancel-on-disconnect-grace-sec", grace.clone());
                }
                Ok(response)
            };
            let accepted = tokio_tungstenite::accept_hdr_async(tcp, callback).await;
            // Other processes probe local ports on a shared machine: only a
            // WebSocket upgrade request takes a script.
            if current.refuse.is_some() {
                let upgrade = captured.lock().unwrap().clone();
                if !upgrade.path.is_empty() {
                    seen.lock().unwrap().push(upgrade);
                    script = scripts.next();
                }
                continue;
            }
            let Ok(mut ws) = accepted else { continue };
            script = scripts.next();
            let index = {
                let mut all = seen.lock().unwrap();
                all.push(captured.lock().unwrap().clone());
                all.len() - 1
            };
            let mut answers = 0;
            let mut timers = 0;
            while let Some(Ok(msg)) = ws.next().await {
                let Message::Binary(frame) = msg else {
                    continue;
                };
                let template = u16::from_le_bytes([frame[2], frame[3]]);
                let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
                let corr = u64::from_le_bytes(frame[16..24].try_into().unwrap());
                seen.lock().unwrap()[index]
                    .frames
                    .push((template, seq, corr));
                if current.close_on == Some(template) {
                    let _ = ws.close(None).await;
                    break;
                }
                let Some(reply) = answer(&current, template, seq, corr, &frame) else {
                    continue;
                };
                if template == 10 {
                    timers += 1;
                    if timers > 1
                        && let Some(delay) = current.delay_later_timers
                    {
                        tokio::time::sleep(delay).await;
                    }
                }
                if ws.send(Message::Binary(reply.into())).await.is_err() {
                    break;
                }
                if template == 6 {
                    for pushed in &current.after_ready {
                        let _ = ws.send(Message::Binary(pushed.clone().into())).await;
                    }
                    if current.stall_after_ready {
                        // Hold the socket open, never reading it again.
                        std::future::pending::<()>().await;
                    }
                }
                if !matches!(template, 1 | 6) {
                    answers += 1;
                    if current.close_after == Some(answers) {
                        let _ = ws.close(None).await;
                        break;
                    }
                }
            }
        }
    });
    (url, record)
}

fn credentials() -> Credentials {
    Credentials::new("42", SEED_HEX).unwrap()
}

fn config(url: String) -> Config {
    let mut config = Config::new(url, credentials());
    config.ping_interval = Duration::from_millis(100);
    config.answer_timeout = Duration::from_millis(600);
    config
}

async fn start(url: String) -> TradeSession {
    connect(config(url)).await.expect("connects")
}

async fn ready(session: &TradeSession) -> u64 {
    tokio::time::timeout(WAIT, session.ready())
        .await
        .expect("ready in time")
        .expect("ready")
}

/// The first event `pick` accepts, within 5 s.
async fn wait<T>(session: &mut TradeSession, mut pick: impl FnMut(&Event) -> Option<T>) -> T {
    tokio::time::timeout(WAIT, async {
        loop {
            let event = session.recv().await.expect("an event");
            if let Some(found) = pick(&event) {
                return found;
            }
        }
    })
    .await
    .expect("the event in time")
}

async fn disconnected(session: &mut TradeSession) -> (String, Vec<u64>) {
    wait(session, |e| match e {
        Event::Disconnected {
            error, unanswered, ..
        } => Some((error.to_string(), unanswered.clone())),
        _ => None,
    })
    .await
}

/// Waits for connection `n` to be ready, reading events meanwhile.
async fn ready_on(session: &mut TradeSession, n: u64) {
    wait(session, |e| {
        matches!(e, Event::Ready { connection } if *connection == n).then_some(())
    })
    .await;
}

fn within(ms: u64) -> Deadline {
    Deadline::within(Duration::from_millis(ms)).unwrap()
}

fn order(client: &str) -> PostOnlyOrder {
    PostOnlyOrder {
        symbol_id: 7,
        side: Side::Buy,
        price: Price::from_ticks(1_000_000_000, None).unwrap(),
        qty: Quantity::from_scaled(1, None, QuantityDomain::OrderBase, None, Some(7)).unwrap(),
        client_order_id: client.into(),
    }
}

fn trade_error(result: crate::errors::Result<impl std::fmt::Debug>) -> TradeError {
    match result {
        Err(Error::TradeSession(e)) => e,
        other => panic!("not a trading error: {other:?}"),
    }
}

fn not_sent(result: crate::errors::Result<impl std::fmt::Debug>) -> NotSent {
    match trade_error(result) {
        TradeError::NotSent { reason, .. } => reason,
        other => panic!("not refused locally: {other:?}"),
    }
}

async fn arm(session: &TradeSession) {
    let ack = session
        .cancel_all_after(Duration::from_secs(10), within(1_000))
        .await
        .expect("armed");
    assert!(ack.armed && ack.expires_at.is_some());
}

#[tokio::test]
async fn the_upgrade_is_signed_and_opts_into_cancel_on_disconnect() {
    let (url, record) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    let seen = record.lock().unwrap()[0].clone();
    let get = |name: &str| {
        seen.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no {name} header"))
    };
    assert_eq!(get("x-api-key-id"), "42");
    assert_eq!(get("x-cancel-on-disconnect"), "ACCOUNT");
    assert_eq!(get("x-cancel-on-disconnect-grace-sec"), "15");
    let nonce = get("x-api-nonce");
    assert!(
        nonce.len() == 32
            && nonce
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
    // The server's canonical string: ts, GET, path, query, sha256(""), nonce.
    let canonical = format!(
        "{}\nGET\n{}\n\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nws-nonce:{nonce}",
        get("x-api-timestamp"),
        seen.path
    );
    let signature: Vec<u8> = hex::decode(get("x-api-signature")).unwrap();
    let key =
        ed25519_dalek::SigningKey::from_bytes(&hex::decode(SEED_HEX).unwrap().try_into().unwrap());
    VerifyingKey::from_bytes(&key.verifying_key().to_bytes())
        .unwrap()
        .verify_strict(
            canonical.as_bytes(),
            &Signature::from_slice(&signature).unwrap(),
        )
        .expect("a signature over the canonical upgrade");
}

#[tokio::test]
async fn each_upgrade_signs_a_fresh_nonce() {
    let a = credentials()
        .sign_websocket_upgrade("wss://example.test/ws/trade.sbe")
        .await
        .unwrap();
    let b = credentials()
        .sign_websocket_upgrade("wss://example.test/ws/trade.sbe")
        .await
        .unwrap();
    assert_ne!(a["X-API-NONCE"], b["X-API-NONCE"]);
}

#[tokio::test]
async fn opting_out_sends_no_cancel_on_disconnect_header() {
    let script = Script {
        echo_cod: false,
        ..serve()
    };
    let (url, record) = stand_in(vec![script]).await;
    let mut config = config(url);
    config.cancel_on_disconnect = None;
    let session = connect(config).await.unwrap();
    ready(&session).await;
    let seen = record.lock().unwrap()[0].clone();
    assert!(
        !seen
            .headers
            .iter()
            .any(|(k, _)| k.starts_with("x-cancel-on-disconnect"))
    );
}

#[tokio::test]
async fn an_upgrade_that_does_not_confirm_cancel_on_disconnect_is_refused() {
    let script = Script {
        echo_cod: false,
        ..serve()
    };
    let (url, _) = stand_in(vec![script]).await;
    let err = connect(config(url)).await.err().expect("refused");
    assert!(
        matches!(err, Error::TradeSession(TradeError::NotProtected)),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_invalid_grace_period_is_refused_before_connecting() {
    for grace in [9, 121] {
        let mut config = config("ws://127.0.0.1:9/ws/trade.sbe".into());
        config.cancel_on_disconnect = Some(Duration::from_secs(grace));
        assert!(matches!(
            connect(config).await.err(),
            Some(Error::Validation(_))
        ));
    }
}

#[tokio::test]
async fn a_refused_upgrade_reports_its_status_and_code() {
    let (url, _) = stand_in(vec![Script {
        refuse: Some((504, "ORDER_CANCEL_OUTCOME_UNKNOWN")),
        ..serve()
    }])
    .await;
    let err = connect(config(url)).await.err().expect("refused");
    match &err {
        Error::TradeSession(TradeError::Upgrade { status, code }) => {
            assert_eq!(
                (*status, code.as_str()),
                (504, "ORDER_CANCEL_OUTCOME_UNKNOWN")
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(err.mutation_outcome_unknown());

    let (url, _) = stand_in(vec![Script {
        refuse: Some((401, r#"{"code":"SIGNATURE_INVALID"}"#)),
        ..serve()
    }])
    .await;
    let err = connect(config(url)).await.err().expect("refused");
    assert!(
        matches!(&err, Error::ApiKeyRejected { status: 401, code, .. } if code == "SIGNATURE_INVALID"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_lost_connection_reconnects_with_the_same_opt_in_through_a_504() {
    let close_after_arm = Script {
        close_after: Some(1),
        ..serve()
    };
    let unknown = Script {
        refuse: Some((504, "ORDER_CANCEL_OUTCOME_UNKNOWN")),
        ..serve()
    };
    let (url, record) = stand_in(vec![close_after_arm, unknown, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    arm(&session).await;
    disconnected(&mut session).await;
    let error = wait(&mut session, |e| match e {
        Event::Disconnected { error, .. } => Some(error.clone()),
        _ => None,
    })
    .await;
    assert!(error.mutation_outcome_unknown(), "{error:?}");
    ready_on(&mut session, 2).await;
    let all = record.lock().unwrap().clone();
    assert_eq!(all.len(), 3);
    for seen in &all {
        assert!(
            seen.headers
                .iter()
                .any(|(k, v)| k == "x-cancel-on-disconnect" && v == "ACCOUNT"),
            "every upgrade opts in"
        );
    }
}

#[tokio::test]
async fn nothing_is_sent_before_the_server_confirms_the_subscription() {
    let unconfirmed = Script {
        ack_subscribe: false,
        ..serve()
    };
    let (url, record) = stand_in(vec![unconfirmed, serve()]).await;
    let mut session = start(url).await;
    wait(&mut session, |e| {
        matches!(e, Event::Connected { connection: 1 }).then_some(())
    })
    .await;
    assert_eq!(
        not_sent(
            session
                .cancel_all_after(Duration::from_secs(10), within(1_000))
                .await
        ),
        NotSent::NotReady
    );
    let (reason, _) = disconnected(&mut session).await;
    assert!(reason.contains("no SubscriptionAck"), "{reason}");
    ready_on(&mut session, 2).await;
    // The first connection carried its Subscribe (seq 1) and nothing else
    // but pings.
    let first = record.lock().unwrap()[0].frames.clone();
    assert_eq!(first[0], (6, 1, 0));
    assert!(first[1..].iter().all(|(t, _, _)| *t == 1), "{first:?}");
}

#[tokio::test]
async fn a_wrong_answer_to_the_subscription_ends_the_connection() {
    let misanswered = Script {
        misanswer_subscribe: true,
        ..serve()
    };
    let (url, _) = stand_in(vec![misanswered, serve()]).await;
    let mut session = start(url).await;
    let (reason, _) = disconnected(&mut session).await;
    assert!(reason.contains("subscription answered"), "{reason}");
    ready_on(&mut session, 2).await;
}

#[tokio::test]
async fn seq_starts_at_one_and_steps_by_one_on_every_connection() {
    let close_after_two = Script {
        close_after: Some(2),
        ..serve()
    };
    let (url, record) = stand_in(vec![close_after_two, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    arm(&session).await;
    session
        .cancel_order(7, &OrderRef::Client("c1".into()), within(500))
        .await
        .unwrap();
    ready_on(&mut session, 2).await;
    arm(&session).await;
    let all = record.lock().unwrap().clone();
    for seen in &all {
        let seqs: Vec<u64> = seen.frames.iter().map(|(_, seq, _)| *seq).collect();
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "{:?}",
            seen.frames
        );
    }
    assert_eq!(
        all[1].frames[0],
        (6, 1, 0),
        "a new connection subscribes with seq 1"
    );
}

#[tokio::test]
async fn an_unanswered_command_ends_the_connection_and_is_an_unknown_outcome() {
    let silent = Script {
        answer_commands: false,
        ..serve()
    };
    let (url, _) = stand_in(vec![silent, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    let mut cancel_all = CancelAllOrders::new(vec![]);
    cancel_all.correlation_id = 5;
    let result = session.cancel_all_orders(&cancel_all, within(500)).await;
    let err = result.clone().err().unwrap();
    assert!(
        err.mutation_outcome_unknown() && !err.is_retryable(),
        "{err:?}"
    );
    match trade_error(result) {
        TradeError::OutcomeUnknown {
            command,
            correlation_id,
            reason,
        } => {
            assert_eq!((command, correlation_id), (Command::CancelAllOrders, 5));
            assert!(
                reason.contains("did not answer CancelAllOrders seq 2"),
                "{reason}"
            );
        }
        other => panic!("{other:?}"),
    }
    let (_, unanswered) = disconnected(&mut session).await;
    assert_eq!(unanswered, [5]);
    ready_on(&mut session, 2).await;
}

#[tokio::test]
async fn a_disconnect_makes_every_write_in_flight_an_unknown_outcome() {
    // The server closes the connection on the order, without an answer.
    let close_on_create = Script {
        close_on: Some(2),
        ..serve()
    };
    let (url, _) = stand_in(vec![close_on_create, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    arm(&session).await;
    let result = session.create_order(&order("lost-1"), within(500)).await;
    let err = result.clone().err().unwrap();
    assert!(err.mutation_outcome_unknown(), "{err:?}");
    assert!(matches!(
        trade_error(result),
        TradeError::OutcomeUnknown {
            command: Command::CreateOrder,
            ..
        }
    ));
    let (_, unanswered) = disconnected(&mut session).await;
    assert_eq!(unanswered.len(), 1);
    ready_on(&mut session, 2).await;
}

#[tokio::test]
async fn a_missing_pong_ends_the_connection() {
    let no_pong = Script {
        answer_pings: false,
        ..serve()
    };
    let (url, _) = stand_in(vec![no_pong, serve()]).await;
    let mut session = start(url).await;
    let (reason, _) = disconnected(&mut session).await;
    assert!(reason.contains("no Pong"), "{reason}");
}

#[tokio::test]
async fn pongs_report_their_round_trip() {
    let (url, _) = stand_in(vec![serve()]).await;
    let mut session = start(url).await;
    let rtt = wait(&mut session, |e| match e {
        Event::Pong { round_trip } => Some(*round_trip),
        _ => None,
    })
    .await;
    assert!(rtt < WAIT);
}

#[tokio::test]
async fn no_order_leaves_outside_an_armed_timer() {
    let (url, record) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    // Unarmed: refused.
    assert_eq!(
        not_sent(session.create_order(&order("c1"), within(500)).await),
        NotSent::NotArmed { expires_at: None }
    );
    // Armed for 10 s: a 500 ms window ends well inside it.
    arm(&session).await;
    session
        .create_order(&order("c3"), within(500))
        .await
        .unwrap();
    // A window that could end past the expiry minus the guard: refused.
    assert!(matches!(
        not_sent(session.create_order(&order("c4"), within(9_500)).await),
        NotSent::NotArmed {
            expires_at: Some(_)
        }
    ));
    // Disarmed: refused again.
    let disarmed = session
        .cancel_all_after(Duration::ZERO, within(1_000))
        .await
        .unwrap();
    assert!(!disarmed.armed && disarmed.expires_at.is_none());
    assert_eq!(
        not_sent(
            session
                .batch_create_orders(&BatchCreateOrders::new(vec![order("c5")]), within(500))
                .await
        ),
        NotSent::NotArmed { expires_at: None }
    );
    let creates = record.lock().unwrap()[0]
        .frames
        .iter()
        .filter(|(t, _, _)| *t == 2 || *t == 7)
        .count();
    assert_eq!(creates, 1, "only the order inside the armed timer left");
}

#[tokio::test]
async fn a_timer_too_close_to_expiry_refuses_orders() {
    let short = Script {
        timer_expires_in: Duration::from_millis(900),
        ..serve()
    };
    let (url, _) = stand_in(vec![short]).await;
    let session = start(url).await;
    ready(&session).await;
    arm(&session).await;
    // 500 ms window + 100 ms margin is past 900 ms - 500 ms guard.
    assert!(matches!(
        not_sent(session.create_order(&order("c2"), within(500)).await),
        NotSent::NotArmed {
            expires_at: Some(_)
        }
    ));
}

#[tokio::test]
async fn the_gate_can_be_turned_off() {
    let (url, _) = stand_in(vec![serve()]).await;
    let mut config = config(url);
    config.timer_gate = None;
    let session = connect(config).await.unwrap();
    ready(&session).await;
    let ack = session
        .create_order(&order("free"), within(500))
        .await
        .unwrap();
    assert!(ack.order_id.is_some());
}

#[tokio::test]
async fn cancels_and_the_timer_itself_need_no_armed_timer() {
    let (url, _) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    let ack = session
        .cancel_order(7, &OrderRef::Id(9), within(500))
        .await
        .unwrap();
    assert!(ack.order_id.is_some());
    let all = session
        .cancel_all_orders(&CancelAllOrders::new(vec![]), within(500))
        .await
        .unwrap();
    assert_eq!(all.cancelled, []);
    let batch = session
        .batch_cancel_orders(
            &BatchCancelOrders::new(vec![CancelItem {
                symbol_id: 7,
                order: OrderRef::Id(9),
            }]),
            within(500),
        )
        .await
        .unwrap();
    assert!(batch.items[0].accepted);
    arm(&session).await;
}

#[tokio::test]
async fn a_command_past_its_deadline_is_refused_not_sent() {
    let (url, record) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    let late = Deadline::new(
        SystemTime::now() - Duration::from_secs(2),
        Duration::from_millis(500),
    )
    .unwrap();
    assert_eq!(
        not_sent(session.cancel_order(7, &OrderRef::Id(9), late).await),
        NotSent::Expired
    );
    assert!(
        !record.lock().unwrap()[0]
            .frames
            .iter()
            .any(|(t, _, _)| *t == 3)
    );
}

#[tokio::test]
async fn the_timer_gate_does_not_cross_a_reconnect() {
    let close_after_arm = Script {
        close_after: Some(1),
        ..serve()
    };
    let (url, _) = stand_in(vec![close_after_arm, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    arm(&session).await;
    ready_on(&mut session, 2).await;
    // Connection 2 starts unarmed: the old acknowledgement does not open it.
    assert_eq!(
        not_sent(session.create_order(&order("c3"), within(500)).await),
        NotSent::NotArmed { expires_at: None }
    );
}

#[tokio::test]
async fn refusals_are_typed_and_say_whether_the_outcome_is_unknown() {
    let cross = Script {
        reject: Some(reject_code::RejectCode::POST_ONLY_CROSS),
        ..serve()
    };
    let (url, _) = stand_in(vec![cross]).await;
    let mut gate_off = config(url);
    gate_off.timer_gate = None;
    let session = connect(gate_off).await.unwrap();
    ready(&session).await;
    let result = session.create_order(&order("x1"), within(500)).await;
    let err = result.clone().err().unwrap();
    assert!(!err.mutation_outcome_unknown() && !err.is_retryable());
    let TradeError::Rejected(r) = trade_error(result) else {
        panic!("not a refusal")
    };
    assert_eq!(
        (r.command, r.code, r.retry_after),
        (
            Command::CreateOrder,
            RejectCode::POST_ONLY_CROSS,
            Some(Duration::from_millis(250))
        )
    );

    for (code, command, unknown, retryable) in [
        (
            reject_code::RejectCode::ORDER_CREATE_OUTCOME_UNKNOWN,
            Command::CreateOrder,
            true,
            false,
        ),
        (
            reject_code::RejectCode::UNAVAILABLE,
            Command::CreateOrder,
            false,
            true,
        ),
        (
            reject_code::RejectCode::UNAVAILABLE,
            Command::BatchCreateOrders,
            true,
            false,
        ),
        (
            reject_code::RejectCode::REQUEST_DEADLINE_EXCEEDED,
            Command::CancelOrder,
            false,
            true,
        ),
        (
            reject_code::RejectCode::RATE_LIMIT_EXCEEDED,
            Command::CancelAllOrders,
            false,
            true,
        ),
    ] {
        let (url, _) = stand_in(vec![Script {
            reject: Some(code),
            ..serve()
        }])
        .await;
        let mut gate_off = config(url);
        gate_off.timer_gate = None;
        let session = connect(gate_off).await.unwrap();
        ready(&session).await;
        let err = match command {
            Command::CreateOrder => session.create_order(&order("x2"), within(500)).await.err(),
            Command::BatchCreateOrders => session
                .batch_create_orders(&BatchCreateOrders::new(vec![order("x3")]), within(500))
                .await
                .err(),
            Command::CancelOrder => session
                .cancel_order(7, &OrderRef::Id(1), within(500))
                .await
                .err(),
            _ => session
                .cancel_all_orders(&CancelAllOrders::new(vec![]), within(500))
                .await
                .err(),
        }
        .expect("refused");
        assert_eq!(
            (err.mutation_outcome_unknown(), err.is_retryable()),
            (unknown, retryable),
            "{code:?} on {command}: {err:?}"
        );
        if code == reject_code::RejectCode::REQUEST_DEADLINE_EXCEEDED {
            assert!(err.is_request_deadline_exceeded());
        }
        if code == reject_code::RejectCode::RATE_LIMIT_EXCEEDED {
            assert_eq!(err.retry_after(), Some(0.25));
        }
    }
}

#[tokio::test]
async fn a_sequence_refusal_ends_the_connection() {
    let out_of_sequence = Script {
        reject: Some(reject_code::RejectCode::SEQUENCE_OUT_OF_ORDER),
        ..serve()
    };
    let (url, _) = stand_in(vec![out_of_sequence, serve()]).await;
    let mut session = start(url).await;
    ready_on(&mut session, 1).await;
    let err = session
        .cancel_order(7, &OrderRef::Id(1), within(500))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::TradeSession(TradeError::Rejected(_))));
    let (reason, _) = disconnected(&mut session).await;
    assert!(reason.contains("SEQUENCE_OUT_OF_ORDER"), "{reason}");
    ready_on(&mut session, 2).await;
}

#[tokio::test]
async fn order_events_carry_cancel_on_disconnect_and_cancel_all_after() {
    let script = Script {
        after_ready: vec![order_event(11, 21), order_event(12, 12)],
        ..serve()
    };
    let (url, _) = stand_in(vec![script]).await;
    let mut session = start(url).await;
    let mut reasons = Vec::new();
    while reasons.len() < 2 {
        if let Some(update) = wait(&mut session, |e| match e {
            Event::Order(update) => Some(Some(update.clone())),
            Event::Pong { .. } => Some(None),
            _ => None,
        })
        .await
        {
            assert_eq!(update.status, OrderStatus::Canceled);
            reasons.push((update.order_id, update.cancel_reason));
        }
    }
    assert_eq!(
        reasons,
        [
            (11, Some(CancelReason::CancelOnDisconnect)),
            (12, Some(CancelReason::CancelAllAfter))
        ]
    );
}

#[tokio::test]
async fn a_full_event_queue_stops_the_session() {
    let script = Script {
        after_ready: (0..8).map(|i| order_event(i + 1, 1)).collect(),
        ..serve()
    };
    let (url, _) = stand_in(vec![script]).await;
    let mut config = config(url);
    config.queue_size = 4;
    let mut session = connect(config).await.unwrap();
    tokio::time::timeout(WAIT, async {
        while session.is_alive() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stopped");
    assert!(matches!(session.err(), Some(Error::QueueOverflow(_))));
    // Queued events stay readable, then the stream ends.
    while session.recv().await.is_some() {}
    assert_eq!(
        not_sent(session.cancel_order(7, &OrderRef::Id(1), within(500)).await),
        NotSent::Closed
    );
}

#[tokio::test]
async fn invalid_commands_are_refused_before_they_are_queued() {
    let (url, record) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    let mut bad = order("ok");
    bad.client_order_id = "x".repeat(37);
    assert!(matches!(
        session.create_order(&bad, within(500)).await,
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        session
            .cancel_all_after(Duration::from_secs(5), within(500))
            .await,
        Err(Error::Validation(_))
    ));
    assert!(
        record.lock().unwrap()[0]
            .frames
            .iter()
            .all(|(t, _, _)| *t == 6 || *t == 1)
    );
}

#[tokio::test]
async fn closing_refuses_later_commands() {
    let (url, _) = stand_in(vec![serve()]).await;
    let session = start(url).await;
    ready(&session).await;
    session.close();
    tokio::time::timeout(WAIT, async {
        while session.is_alive() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(session.ready().await.is_err());
    assert_eq!(
        not_sent(session.cancel_order(7, &OrderRef::Id(1), within(500)).await),
        NotSent::Closed
    );
}

#[test]
fn urls_end_in_the_trading_path() {
    assert_eq!(
        crate::tradews::url("wss://api.example.test").unwrap(),
        "wss://api.example.test/ws/trade.sbe"
    );
    assert_eq!(
        crate::tradews::url("wss://api.example.test/ws/trade.sbe").unwrap(),
        "wss://api.example.test/ws/trade.sbe"
    );
    assert!(crate::tradews::url("https://api.example.test").is_err());
    assert!(crate::tradews::url("wss://api.example.test/?a=1").is_err());
}

#[tokio::test]
async fn a_disarm_closes_the_gate_before_its_answer() {
    let slow = Script {
        delay_later_timers: Some(Duration::from_millis(500)),
        ..serve()
    };
    let (url, record) = stand_in(vec![slow]).await;
    let session = start(url).await;
    ready(&session).await;
    arm(&session).await;
    // The disarm is written first; its answer is late. An order behind it
    // may not rely on the timer it disarms.
    let behind = order("behind-disarm");
    let (disarm, create) = tokio::join!(
        session.cancel_all_after(Duration::ZERO, within(1_000)),
        session.create_order(&behind, within(500)),
    );
    assert_eq!(not_sent(create), NotSent::NotArmed { expires_at: None });
    assert!(!disarm.unwrap().armed);
    assert!(
        !record.lock().unwrap()[0]
            .frames
            .iter()
            .any(|(t, _, _)| *t == 2),
        "no order was written"
    );
}

#[tokio::test]
async fn a_shorter_refresh_bounds_the_gate_before_its_answer() {
    let slow = Script {
        delay_later_timers: Some(Duration::from_millis(500)),
        timer_expires_in: Duration::from_secs(60),
        ..serve()
    };
    let (url, _) = stand_in(vec![slow]).await;
    let session = start(url).await;
    ready(&session).await;
    session
        .cancel_all_after(Duration::from_secs(60), within(1_000))
        .await
        .unwrap();
    // Armed for 60 s, a 15 s window fits; refreshed to 10 s, it does not.
    let behind = order("behind-refresh");
    let (refresh, create) = tokio::join!(
        session.cancel_all_after(Duration::from_secs(10), within(1_000)),
        session.create_order(&behind, within(15_000)),
    );
    assert!(matches!(
        not_sent(create),
        NotSent::NotArmed {
            expires_at: Some(_)
        }
    ));
    assert!(refresh.is_ok());
}

#[tokio::test]
async fn a_write_the_peer_never_reads_ends_the_connection_in_time() {
    let stalled = Script {
        stall_after_ready: true,
        ..serve()
    };
    let (url, _) = stand_in(vec![stalled, serve()]).await;
    let mut gate_off = config(url);
    gate_off.timer_gate = None;
    gate_off.answer_timeout = Duration::from_millis(100);
    let mut session = connect(gate_off).await.unwrap();
    ready_on(&mut session, 1).await;
    let session = Arc::new(session);
    let batch = |i: usize| {
        BatchCreateOrders::new((0..20).map(|j| order(&format!("stall-{i}-{j}"))).collect())
    };
    let calls: Vec<_> = (0..400)
        .map(|i| {
            let session = Arc::clone(&session);
            let batch = batch(i);
            tokio::spawn(async move { session.batch_create_orders(&batch, within(60_000)).await })
        })
        .collect();
    let results = tokio::time::timeout(Duration::from_secs(3), async {
        let mut out = Vec::new();
        for call in calls {
            out.push(call.await.unwrap());
        }
        out
    })
    .await
    .expect("every command answered although the writes stalled");
    let unknown = results
        .iter()
        .filter(|r| {
            r.as_ref()
                .err()
                .is_some_and(Error::mutation_outcome_unknown)
        })
        .count();
    assert!(unknown > 0, "the written commands are unknown outcomes");
    assert!(
        results.iter().all(|r| match r {
            Err(e) =>
                e.mutation_outcome_unknown()
                    || matches!(e, Error::TradeSession(TradeError::NotSent { .. })),
            Ok(_) => false,
        }),
        "never acknowledged, never refused by the server"
    );
}
