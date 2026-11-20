//! Frames captured from the server, one WebSocket message per file, listed by
//! each capture's frames.tsv. POLYESTER_MARKETSTREAM_GOLDEN names the
//! directory of captures; the tests pass without it.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use super::frames::window;
use crate::marketstream::client::{
    depth_book, feed_of, handle, offline, record_subscribe, recovering,
};
use crate::marketstream::codec::{self, Msg};
use crate::marketstream::sbe::{ReadBuf, message_header_codec, ping_codec, subscribe_codec};
use crate::marketstream::{Config, Event, FeedState, Pairs, Stream, Topic};

struct Frame {
    file: String,
    direction: String,
    message: String,
    data: Vec<u8>,
}

fn captures() -> Option<Vec<(String, Vec<Frame>)>> {
    let root = PathBuf::from(std::env::var_os("POLYESTER_MARKETSTREAM_GOLDEN")?);
    let mut out = Vec::new();
    let mut dirs: Vec<_> = std::fs::read_dir(&root)
        .expect("golden directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.join("frames.tsv").is_file())
        .collect();
    dirs.sort();
    assert!(!dirs.is_empty(), "no captures under {}", root.display());
    for dir in dirs {
        let index = std::fs::read_to_string(dir.join("frames.tsv")).unwrap();
        let frames = index
            .lines()
            .skip(1)
            .filter_map(|line| {
                let cols: Vec<&str> = line.split('\t').collect();
                (cols.len() >= 3).then(|| Frame {
                    file: cols[0].to_owned(),
                    direction: cols[1].to_owned(),
                    message: cols[2].to_owned(),
                    data: std::fs::read(dir.join(cols[0])).unwrap(),
                })
            })
            .collect();
        out.push((
            dir.file_name().unwrap().to_string_lossy().into_owned(),
            frames,
        ));
    }
    Some(out)
}

fn kind(msg: &Msg) -> &'static str {
    match msg {
        Msg::Trades { .. } => "Trades",
        Msg::TradesSkipped { .. } => "TradesSkipped",
        Msg::TradesThrough { .. } => "TradesThrough",
        Msg::Bbo { .. } => "BestBidOffer",
        Msg::DepthSnapshot { .. } => "DepthSnapshot",
        Msg::DepthUpdate { .. } => "DepthUpdate",
        Msg::MarketStatus { .. } => "MarketStatus",
        Msg::SubscribeResponse { .. } => "SubscribeResponse",
        Msg::Pong { .. } => "Pong",
    }
}

/// Every server frame decodes to its message, and every client frame is what
/// this client encodes for the same request.
#[test]
fn golden_frames_decode_and_encode() {
    let Some(captures) = captures() else { return };
    let mut seen: HashMap<String, usize> = HashMap::new();
    for (capture, frames) in &captures {
        for f in frames {
            if f.direction == "client" {
                let h = message_header_codec::MessageHeaderDecoder::default()
                    .wrap(ReadBuf::new(&f.data), 0);
                let again = match f.message.as_str() {
                    "Subscribe" => {
                        let d = subscribe_codec::SubscribeDecoder::default().header(h, 0);
                        let id = d.request_id();
                        let mut items = d.items_decoder();
                        let mut topics = Vec::new();
                        while items.advance().unwrap().is_some() {
                            topics.push(Topic::new(
                                items.symbol_id(),
                                Stream::from_wire(items.stream()).unwrap(),
                            ));
                        }
                        codec::encode_subscribe(id, &topics)
                    }
                    "Ping" => codec::encode_ping(
                        ping_codec::PingDecoder::default().header(h, 0).request_id(),
                    ),
                    other => panic!("{capture}/{}: client message {other}", f.file),
                };
                assert_eq!(again, f.data, "{capture}/{}", f.file);
                *seen.entry(format!("client {}", f.message)).or_default() += 1;
                continue;
            }
            let msg = codec::decode(&f.data)
                .unwrap_or_else(|e| panic!("{capture}/{}: {e}", f.file))
                .expect("known template");
            assert_eq!(kind(&msg), f.message, "{capture}/{}", f.file);
            if let Msg::DepthSnapshot {
                stream: Some(stream),
                levels_per_side,
                ..
            } = msg
            {
                assert_eq!(levels_per_side, window(stream), "{capture}/{}", f.file);
            }
            *seen.entry(f.message.clone()).or_default() += 1;
        }
    }
    for m in [
        "client Subscribe",
        "client Ping",
        "SubscribeResponse",
        "Pong",
        "Trades",
        "TradesThrough",
        "BestBidOffer",
        "DepthSnapshot",
        "DepthUpdate",
        "MarketStatus",
    ] {
        assert!(seen.contains_key(m), "no captured {m}");
    }
}

struct AnyPairs;

impl Pairs for AnyPairs {
    fn base_quantity_scale(&self, _: u32) -> Option<u32> {
        Some(8)
    }
    fn symbol(&self, _: u32) -> Option<String> {
        None
    }
    fn base_quantity_scale_and_symbol(&self, _: u32) -> Option<(u32, Option<String>)> {
        Some((8, None))
    }
    fn symbol_id(&self, _: &str) -> Option<u32> {
        None
    }
}

/// Each capture replays through the client's handlers without a protocol
/// error: books follow the depth stream's exact-base chain, and trades follow
/// their frontier.
#[test]
fn golden_captures_replay() {
    let Some(captures) = captures() else { return };
    for (capture, frames) in captures {
        let mut config = Config::new("ws://127.0.0.1/ws/market.sbe", Arc::new(AnyPairs));
        config.queue_size = 1 << 16;
        let (shared, mut events) = offline(config);
        let mut last_depth: HashMap<u32, u64> = HashMap::new();
        // A capture may keep only some frames of a template: trades of a pair
        // whose baseline it did not keep are not replayed.
        let mut baseline = HashSet::new();
        for f in &frames {
            if f.direction == "client" {
                // The captured client's requests, recorded as this client
                // records what it sends: responses are matched to them by ID.
                if f.message == "Subscribe" {
                    let h = message_header_codec::MessageHeaderDecoder::default()
                        .wrap(ReadBuf::new(&f.data), 0);
                    let d = subscribe_codec::SubscribeDecoder::default().header(h, 0);
                    let id = d.request_id();
                    let mut items = d.items_decoder();
                    let mut topics = Vec::new();
                    while items.advance().unwrap().is_some() {
                        topics.push(Topic::new(
                            items.symbol_id(),
                            Stream::from_wire(items.stream()).unwrap(),
                        ));
                    }
                    record_subscribe(&shared, id, topics);
                }
                continue;
            }
            let msg = codec::decode(&f.data).unwrap().unwrap();
            match &msg {
                Msg::TradesThrough { symbol_id, .. } => {
                    baseline.insert(*symbol_id);
                }
                Msg::Trades { symbol_id, .. } | Msg::TradesSkipped { symbol_id, .. }
                    if !baseline.contains(symbol_id) =>
                {
                    continue;
                }
                _ => {}
            }
            if let Err(err) = handle(&shared, &f.data) {
                panic!("{capture}/{}: {err}", f.file);
            }
            match msg {
                Msg::DepthUpdate {
                    symbol_id, last, ..
                } => {
                    last_depth.insert(symbol_id, last);
                }
                Msg::DepthSnapshot {
                    symbol_id,
                    stream: Some(Stream::Depth),
                    update_id,
                    ..
                } => {
                    last_depth.insert(symbol_id, update_id);
                }
                _ => {}
            }
        }
        for (symbol_id, last) in &last_depth {
            if recovering(&shared, Topic::new(*symbol_id, Stream::Depth)) {
                // A capture of the server's old order (LIVE before the
                // replacement snapshot) leaves the stream recovering; the
                // capture has no answer to its requests.
                eprintln!("{capture} leaves pair {symbol_id}'s depth stream recovering");
                continue;
            }
            let (synced, at) =
                depth_book(&shared, Topic::new(*symbol_id, Stream::Depth)).expect("a depth book");
            // A capture that ends stale leaves the book waiting for its
            // replacement snapshot.
            let stale = feed_of(&shared, *symbol_id) == Some(FeedState::Stale);
            assert!(
                synced != stale && at == *last,
                "{capture}: depth book of {symbol_id} at {at} (synced {synced}, stale {stale}), the stream ended at {last}"
            );
        }
        let mut gaps = 0;
        while let Ok(e) = events.try_recv() {
            if matches!(e, Event::TradesGap(_)) {
                gaps += 1;
            }
        }
        assert_eq!(
            gaps, 0,
            "{capture}: trade gaps in a capture without lost trades"
        );
    }
}
