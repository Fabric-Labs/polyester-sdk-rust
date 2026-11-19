//! Frames both ways.
//!
//! - `server.frames` is derived from the server's own SBE encoder output, with
//!   every ID and time rewritten: each frame decodes to the values it holds.
//! - `client.frames` is what this encoder writes for a fixed set of commands,
//!   byte for byte; re-record it with `TRADEWS_RECORD_CLIENT_FRAMES=1`.

use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::Error;
use crate::tradews::codec::{self, Answer, HEADER, Msg};
use crate::tradews::sbe::{
    self, WriteBuf, batch_create_ack_codec, batch_item_status, batch_replace_ack_codec,
    cancel_all_after_codec, cancel_all_listing_status, cancel_all_orders_codec, command_ack_codec,
    command_reject_codec, command_type, execution_event_codec, fee_asset, market_resync_codec,
    market_resync_reason, oco_ack_codec, order_event_codec, order_status, reject_code,
    replace_action, side,
};
use crate::tradews::{
    BatchCancelOrders, BatchCreateOrders, BatchReplaceOrders, BatchStatus, CancelAllAfterOptions,
    CancelAllListing, CancelAllOrders, CancelItem, CancelReason, CancelledOrder, Deadline,
    FeeAsset, ModifyBehavior, ModifyOrder, OcoAck, OcoCommand, OrderRef, OrderStatus,
    PostOnlyOrder, RejectCode, ReplaceAction, ReplaceItem, ResyncReason, Side, TradeError,
};
use crate::types::{Price, Quantity, QuantityDomain};

pub(super) const ORDER: u64 = 5_764_607_523_034_234_881;
const OTHER: u64 = 9_223_372_036_854_775_837;
const ACK_NS: u64 = 1_801_234_567_890_123_456;
const GATEWAY_NS: u64 = 1_801_234_567_000_000_000;
const CLIENT: &str = "c0ffee00-1234-4abc-8def-0123456789ab";
const PAIR: u32 = 7;

fn unhex(s: &str) -> Vec<u8> {
    hex::decode(s).expect("hex frame")
}

/// The frames of one section of a fixture: one hex frame per line, `#`
/// comments, and `## name` lines that start a section.
pub(super) fn frames(text: &str, section: &str) -> Vec<Vec<u8>> {
    let mut current = "";
    let mut out = Vec::new();
    for line in text.lines() {
        if let Some(name) = line.strip_prefix("## ") {
            current = name.trim();
            continue;
        }
        let data = line.split('#').next().unwrap_or("").trim();
        if !data.is_empty() && current == section {
            out.push(unhex(data));
        }
    }
    out
}

fn server() -> Vec<Vec<u8>> {
    frames(include_str!("server.frames"), "server")
}

fn decoded(i: usize) -> Msg {
    codec::decode(&server()[i]).expect("decodes")
}

fn at(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

fn qty(scaled: i64) -> Quantity {
    Quantity::from_scaled(scaled, None, QuantityDomain::OrderBase, None, Some(PAIR)).unwrap()
}

fn price(ticks: i64) -> Price {
    Price::from_ticks(ticks, None).unwrap()
}

fn answer(msg: Msg) -> (u64, Answer) {
    match msg {
        Msg::Answer { seq, answer } => (seq, answer),
        other => panic!("not an answer: {other:?}"),
    }
}

#[test]
fn acks_rejects_pongs_and_subscription_acks_decode() {
    let (seq, a) = answer(decoded(0));
    assert_eq!(seq, 1);
    let Answer::Order { command_type, ack } = a else {
        panic!("not an order ack: {a:?}")
    };
    assert_eq!(
        command_type,
        u8::from(command_type::CommandType::CREATE_ORDER)
    );
    assert_eq!(
        (
            ack.correlation_id,
            ack.order_id,
            ack.symbol_id,
            ack.acked_at
        ),
        (9001, Some(ORDER), Some(PAIR), at(ACK_NS))
    );

    let (seq, a) = answer(decoded(1));
    let Answer::Reject(r) = a else {
        panic!("not a reject")
    };
    assert_eq!((seq, r.correlation_id, r.ts_ns), (2, 9002, ACK_NS));
    assert_eq!(r.code, RejectCode::POST_ONLY_CROSS);
    assert_eq!(r.code.to_string(), "POST_ONLY_CROSS");
    assert_eq!((r.retry_after, r.existing_order_id), (None, None));
    assert_eq!(r.detail, "would cross");

    assert_eq!(decoded(2), Msg::Pong { seq: 3 });
    assert_eq!(
        decoded(3),
        Msg::SubscriptionAck {
            seq: 4,
            channels: 0b111
        }
    );
}

#[test]
fn executions_and_order_events_decode_with_their_reasons() {
    let Msg::Execution(e) = decoded(4) else {
        panic!("not an execution")
    };
    assert_eq!(
        (e.symbol_id, e.match_id, e.match_seq, e.order_id),
        (PAIR, 31337, 1, ORDER)
    );
    assert_eq!(
        (e.side, e.maker, e.fee_asset, e.settled),
        (Some(Side::Sell), true, FeeAsset::Quote, false)
    );
    assert_eq!(
        (e.qty, e.price, e.remaining_qty),
        (qty(400_000), price(95_123_450_000_000), Some(qty(600_000)))
    );
    assert_eq!((e.match_time, e.gateway_time), (at(ACK_NS), at(GATEWAY_NS)));

    let Msg::Order(o) = decoded(5) else {
        panic!("not an order event")
    };
    assert_eq!(
        (o.symbol_id, o.order_id, o.version, o.status),
        (PAIR, ORDER, 3, OrderStatus::Canceled)
    );
    assert_eq!((o.remaining_qty, o.cumulative_qty), (qty(0), qty(400_000)));
    assert_eq!((o.event_time, o.gateway_time), (at(ACK_NS), at(GATEWAY_NS)));
    assert_eq!(
        (o.cancel_reason, o.reject_reason),
        (Some(CancelReason::CancelAllAfter), None)
    );
    assert!(o.status.is_terminal());

    let Msg::Order(o) = decoded(6) else {
        panic!("not an order event")
    };
    assert_eq!(
        (o.version, o.status, o.cancel_reason),
        (2, OrderStatus::Rejected, None)
    );
    assert_eq!(o.reject_reason, Some(RejectCode::POST_ONLY_CROSS));
}

#[test]
fn batch_acks_keep_each_items_outcome_in_order() {
    let (seq, a) = answer(decoded(7));
    let Answer::BatchCreate(ack) = a else {
        panic!("not a batch create ack")
    };
    assert_eq!(
        (seq, ack.correlation_id, ack.acked_at),
        (5, 9005, at(ACK_NS))
    );
    assert_eq!(ack.retry_after, None);
    assert_eq!(
        ack.items
            .iter()
            .map(|i| (i.accepted, i.order_id, i.code.map(|c| c.to_string())))
            .collect::<Vec<_>>(),
        [
            (true, Some(880_001), None),
            (false, None, Some("INSUFFICIENT_FUNDS".to_owned()))
        ]
    );
    assert!(!ack.items[1].outcome_unknown());

    let (_, a) = answer(decoded(8));
    let Answer::BatchReplace(ack) = a else {
        panic!("not a batch replace ack")
    };
    assert_eq!(
        (ack.correlation_id, ack.batch_request_id, ack.status),
        (9006, 0x5eed, BatchStatus::PartiallyAdmitted)
    );
    assert_eq!(
        ack.items
            .iter()
            .map(|i| (
                i.accepted,
                i.order_id,
                i.replacement_order_id,
                i.action,
                i.code.map(|c| c.to_string())
            ))
            .collect::<Vec<_>>(),
        [
            (
                true,
                Some(880_011),
                Some(880_012),
                Some(ReplaceAction::Replaced),
                None
            ),
            (
                true,
                Some(880_013),
                None,
                Some(ReplaceAction::Amended),
                None
            ),
            (
                false,
                Some(880_014),
                None,
                None,
                Some("ORDER_UNKNOWN".to_owned())
            )
        ]
    );

    let (_, a) = answer(decoded(9));
    let Answer::BatchCancel(ack) = a else {
        panic!("not a batch cancel ack")
    };
    assert_eq!(ack.correlation_id, 9007);
    assert_eq!(
        ack.items
            .iter()
            .map(|i| (i.accepted, i.order_id, i.code))
            .collect::<Vec<_>>(),
        [
            (true, Some(880_021), None),
            (false, Some(880_022), Some(RejectCode::UNAVAILABLE))
        ]
    );
}

#[test]
fn resyncs_timer_acks_and_cancel_all_listings_decode() {
    let Msg::MarketResync(r) = decoded(10) else {
        panic!("not a resync")
    };
    assert_eq!(
        (r.symbol_id, r.reason, r.gateway_time),
        (PAIR, ResyncReason::Discontinuity, at(GATEWAY_NS))
    );

    let (seq, a) = answer(decoded(11));
    let Answer::Timer(t) = a else {
        panic!("not a timer ack")
    };
    assert_eq!((seq, t.correlation_id, t.armed), (8, 9008, true));
    assert_eq!(t.timeout, Duration::from_secs(10));
    assert_eq!(t.expires_at, Some(at(ACK_NS + 10_000_000_000)));
    assert_eq!(t.acked_at, at(ACK_NS));

    let (seq, a) = answer(decoded(12));
    let Answer::CancelAll(c) = a else {
        panic!("not a cancel-all ack")
    };
    assert_eq!((seq, c.correlation_id), (9, 9009));
    assert_eq!(
        (
            c.matched_orders,
            c.matched_triggers,
            c.submitted_cancels,
            c.failed_cancels,
            c.truncated
        ),
        (2, 3, 2, 0, false)
    );
    assert_eq!(c.listing, CancelAllListing::Unspecified);
    assert_eq!(
        c.cancelled,
        [
            CancelledOrder {
                order_id: ORDER,
                client_order_id: Some(CLIENT.into())
            },
            CancelledOrder {
                order_id: OTHER,
                client_order_id: None
            }
        ]
    );
}

fn frame_error(result: crate::errors::Result<Msg>) -> bool {
    matches!(result, Err(Error::TradeSession(TradeError::Frame { .. })))
}

#[test]
fn every_short_long_or_foreign_frame_is_refused_before_a_read() {
    for frame in server() {
        for len in 0..frame.len() {
            let got = codec::decode(&frame[..len]);
            assert!(
                frame_error(got.clone()),
                "{len} of {}: {got:?}",
                frame.len()
            );
        }
        let mut longer = frame.clone();
        longer.push(0);
        assert!(frame_error(codec::decode(&longer)), "a trailing byte");
    }
    let mut other_version = server()[0].clone();
    other_version[6] += 1;
    assert!(matches!(
        codec::decode(&other_version),
        Err(Error::TradeSession(TradeError::Schema {
            schema_id: 7,
            version
        })) if version == codec::SCHEMA_VERSION + 1
    ));
    let mut other_schema = server()[0].clone();
    other_schema[4] = 20;
    assert!(matches!(
        codec::decode(&other_schema),
        Err(Error::TradeSession(TradeError::Schema {
            schema_id: 20,
            ..
        }))
    ));
    // A template this client does not know is skipped, not refused:
    // `unknown_templates_are_skipped_whole`.
    // A batch result count past the frame's end.
    let mut overlong = server()[7].clone();
    overlong[HEADER + batch_create_ack_codec::SBE_BLOCK_LENGTH as usize + 2] = 9;
    assert!(frame_error(codec::decode(&overlong)));
    // A block shorter than this version's.
    let mut short_block = server()[2].clone();
    short_block[0] -= 1;
    assert!(frame_error(codec::decode(&short_block)));
}

/// A message of a template this client does not know, of this schema and
/// version: a 12-byte block, then a group of two 3-byte entries and a 5-byte
/// data field, whose layout only that template's schema would tell.
pub(super) fn unknown(template_id: u16) -> Vec<u8> {
    let mut f = Vec::new();
    for v in [12, template_id, codec::SCHEMA_ID, codec::SCHEMA_VERSION] {
        f.extend(u16::to_le_bytes(v));
    }
    f.extend([0xab; 12]);
    f.extend(3u16.to_le_bytes());
    f.extend(2u16.to_le_bytes());
    f.extend([1u8; 6]);
    f.extend(5u32.to_le_bytes());
    f.extend(b"hello");
    f
}

#[test]
fn unknown_templates_are_skipped_whole() {
    // BatchReplaceItemReject (32) from a newer server, and any other.
    for template_id in [32, 99, u16::MAX] {
        assert_eq!(
            codec::decode(&unknown(template_id)).unwrap(),
            Msg::Unknown { template_id }
        );
    }
    // A header alone, declaring an empty block.
    let mut bare = unknown(32)[..HEADER].to_vec();
    bare[..2].copy_from_slice(&0u16.to_le_bytes());
    assert_eq!(
        codec::decode(&bare).unwrap(),
        Msg::Unknown { template_id: 32 }
    );
    // Each WebSocket message is one SBE message: the known frames around an
    // unknown one decode as they do alone.
    let stream = [server()[0].clone(), unknown(32), server()[1].clone()];
    let got: Vec<Msg> = stream.iter().map(|f| codec::decode(f).unwrap()).collect();
    assert_eq!(
        got,
        [decoded(0), Msg::Unknown { template_id: 32 }, decoded(1)]
    );
    // Shorter than its header and the block the header declares: refused.
    let frame = unknown(32);
    for len in 0..HEADER + 12 {
        let got = codec::decode(&frame[..len]);
        assert!(frame_error(got.clone()), "{len}: {got:?}");
    }
    // Another schema or version is refused whatever the template.
    let mut newer = unknown(32);
    newer[6..8].copy_from_slice(&(codec::SCHEMA_VERSION + 1).to_le_bytes());
    assert!(matches!(
        codec::decode(&newer),
        Err(Error::TradeSession(TradeError::Schema { .. }))
    ));
    let mut other = unknown(32);
    other[4..6].copy_from_slice(&20u16.to_le_bytes());
    assert!(matches!(
        codec::decode(&other),
        Err(Error::TradeSession(TradeError::Schema {
            schema_id: 20,
            ..
        }))
    ));
}

/// An `OcoAck` as the generated encoder writes it; a create names both legs.
pub(super) fn oco_ack(
    seq: u64,
    correlation_id: u64,
    command: command_type::CommandType,
    client_list_id: &[u8],
) -> Vec<u8> {
    let mut buf =
        vec![0u8; HEADER + oco_ack_codec::SBE_BLOCK_LENGTH as usize + 4 + client_list_id.len()];
    let mut e = oco_ack_codec::OcoAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.seq(seq);
    e.correlation_id(correlation_id);
    e.command_type(command);
    e.order_list_id(ORDER);
    if command == command_type::CommandType::CREATE_OCO {
        e.take_profit_trigger_id(OTHER);
        e.stop_loss_trigger_id(OTHER + 1);
    }
    e.ts_ns(ACK_NS);
    e.client_list_id(client_list_id);
    buf
}

#[test]
fn oco_acks_decode_for_a_create_and_a_cancel() {
    use command_type::CommandType as T;
    assert_eq!(
        codec::decode(&oco_ack(3, 9003, T::CREATE_OCO, b"oco-1")).unwrap(),
        Msg::OcoAck {
            seq: 3,
            ack: OcoAck {
                correlation_id: 9003,
                command: OcoCommand::Create,
                order_list_id: ORDER,
                take_profit_trigger_id: Some(OTHER),
                stop_loss_trigger_id: Some(OTHER + 1),
                client_list_id: Some("oco-1".into()),
                acked_at: at(ACK_NS),
            },
        }
    );
    assert_eq!(
        codec::decode(&oco_ack(4, 9004, T::CANCEL_OCO, b"")).unwrap(),
        Msg::OcoAck {
            seq: 4,
            ack: OcoAck {
                correlation_id: 9004,
                command: OcoCommand::Cancel,
                order_list_id: ORDER,
                take_profit_trigger_id: None,
                stop_loss_trigger_id: None,
                client_list_id: None,
                acked_at: at(ACK_NS),
            },
        }
    );
    // A command type this client does not name keeps its number.
    let mut other = oco_ack(5, 9005, T::CANCEL_OCO, b"");
    other[HEADER + codec::OCO_ACK_COMMAND] = 40;
    assert!(matches!(
        codec::decode(&other).unwrap(),
        Msg::OcoAck { ack, .. } if ack.command == OcoCommand::Unknown(40)
    ));
    // Every short frame, a trailing byte and an ID that is not UTF-8 are
    // refused.
    let frame = oco_ack(3, 9003, T::CREATE_OCO, b"oco-1");
    for len in 0..frame.len() {
        let got = codec::decode(&frame[..len]);
        assert!(frame_error(got.clone()), "{len}: {got:?}");
    }
    let mut longer = frame.clone();
    longer.push(0);
    assert!(frame_error(codec::decode(&longer)));
    assert!(frame_error(codec::decode(&oco_ack(
        3,
        9003,
        T::CREATE_OCO,
        &[0xff, 0xfe]
    ))));
}

/// A deadline in the past, so the fixture does not depend on today's date.
fn deadline(window_ms: u64) -> Deadline {
    Deadline::new(
        UNIX_EPOCH + Duration::from_millis(1_790_000_000_123),
        Duration::from_millis(window_ms),
    )
    .unwrap()
}

fn order(side: Side, ticks: i64, client: &str) -> PostOnlyOrder {
    PostOnlyOrder {
        symbol_id: PAIR,
        side,
        price: price(ticks),
        qty: qty(1_000_000),
        client_order_id: client.into(),
    }
}

/// The commands of `client.frames`, in order, with `seq` and correlation ID
/// 1 to 10 (0 for the subscription and the ping, which carry none).
fn client_frames() -> Vec<(&'static str, Vec<u8>)> {
    let mut modify = ModifyOrder::new(PAIR, OrderRef::Client(CLIENT.into()));
    modify.correlation_id = 4;
    modify.price = Some(price(95_100_000_000_000));
    let mut cancel_all = CancelAllOrders::new(vec![]);
    cancel_all.correlation_id = 8;
    let mut batch_create = BatchCreateOrders::new(vec![
        order(Side::Buy, 95_000_000_000_000, "q-b1"),
        order(Side::Sell, 95_200_000_000_000, "q-a1"),
    ]);
    batch_create.correlation_id = 3;
    let mut batch_replace = BatchReplaceOrders::new(
        PAIR,
        vec![
            ReplaceItem {
                order: OrderRef::Id(ORDER),
                qty: Some(qty(2_000_000)),
                price: Some(price(95_050_000_000_000)),
            },
            ReplaceItem {
                order: OrderRef::Client("q-a1".into()),
                qty: Some(qty(500_000)),
                price: None,
            },
        ],
    );
    batch_replace.correlation_id = 5;
    let mut batch_cancel = BatchCancelOrders::new(vec![
        CancelItem {
            symbol_id: PAIR,
            order: OrderRef::Id(ORDER),
        },
        CancelItem {
            symbol_id: PAIR,
            order: OrderRef::Client("q-b1".into()),
        },
    ]);
    batch_cancel.correlation_id = 7;
    let frames = vec![
        (
            "Subscribe executions|orders|marketResync",
            codec::encode_subscribe(0),
        ),
        (
            "CreateOrder pair 7 BUY 1000000 @ 95123450000000 post-only, 500 ms window",
            codec::encode_create(
                &order(Side::Buy, 95_123_450_000_000, CLIENT),
                2,
                &deadline(500),
            )
            .unwrap(),
        ),
        (
            "BatchCreateOrders: BUY @ 95000000000000, SELL @ 95200000000000, 500 ms window",
            codec::encode_batch_create(&batch_create, &deadline(500)).unwrap(),
        ),
        (
            "ModifyOrder by client ID: price 95100000000000, AMEND_OR_REPLACE, 500 ms window",
            codec::encode_modify(&modify, &deadline(500)).unwrap(),
        ),
        (
            "BatchReplaceOrders pair 7: by order ID 2000000 @ 95050000000000, by client ID qty 500000",
            codec::encode_batch_replace(&batch_replace, &deadline(500)).unwrap(),
        ),
        (
            "CancelOrder by client ID, 500 ms window",
            codec::encode_cancel(PAIR, &OrderRef::Client(CLIENT.into()), 6, &deadline(500))
                .unwrap(),
        ),
        (
            "BatchCancelOrders: by order ID, by client ID, 500 ms window",
            codec::encode_batch_cancel(&batch_cancel, &deadline(500)).unwrap(),
        ),
        (
            "CancelAllOrders: every pair, both sides, 500 ms window",
            codec::encode_cancel_all(&cancel_all, &deadline(500)).unwrap(),
        ),
        (
            "CancelAllAfter 10 s, every pair, 1000 ms window",
            codec::encode_cancel_all_after(
                Duration::from_secs(10),
                9,
                Default::default(),
                &deadline(1_000),
            )
            .unwrap(),
        ),
        ("Ping", codec::encode_ping(0)),
    ];
    frames
        .into_iter()
        .enumerate()
        .map(|(i, (comment, mut frame))| {
            codec::set_seq(&mut frame, i as u64 + 1);
            (comment, frame)
        })
        .collect()
}

fn client_fixture() -> String {
    let mut out = String::from(
        "# Client frames, one hex frame per line, seq 1 to 10, written by this encoder.\n\
         # Re-record with TRADEWS_RECORD_CLIENT_FRAMES=1, never by hand.\n\
         ## client\n",
    );
    for (comment, frame) in client_frames() {
        let _ = writeln!(out, "{}  # {comment}", hex::encode(frame));
    }
    out
}

#[test]
fn the_client_fixture_is_what_the_encoder_writes() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tradews/tests/client.frames");
    let fresh = client_fixture();
    if std::env::var_os("TRADEWS_RECORD_CLIENT_FRAMES").is_some() {
        std::fs::write(&path, &fresh).unwrap();
        return;
    }
    let recorded = std::fs::read_to_string(&path).unwrap();
    assert!(
        recorded == fresh,
        "{} differs from the encoder; re-record with TRADEWS_RECORD_CLIENT_FRAMES=1",
        path.display()
    );
}

#[test]
fn client_frames_carry_the_schema_and_the_set_seq() {
    for (i, (_, frame)) in client_frames().into_iter().enumerate() {
        let header = sbe::message_header_codec::MessageHeaderDecoder::default()
            .wrap(sbe::ReadBuf::new(&frame), 0);
        assert_eq!(
            (header.schema_id(), header.version()),
            (codec::SCHEMA_ID, codec::SCHEMA_VERSION)
        );
        assert_eq!(
            u64::from_le_bytes(frame[HEADER..HEADER + 8].try_into().unwrap()),
            i as u64 + 1
        );
    }
}

/// Cancel-all and the dead-man switch send includeTriggers: 0 unless the
/// caller asks to stop triggers too, so today's behaviour is the default.
#[test]
fn cancel_all_and_the_switch_send_include_triggers() {
    for include in [false, true] {
        let mut cancel = CancelAllOrders::new(vec![PAIR]);
        cancel.include_triggers = include;
        let frame = codec::encode_cancel_all(&cancel, &deadline(500)).unwrap();
        let header = sbe::message_header_codec::MessageHeaderDecoder::default()
            .wrap(sbe::ReadBuf::new(&frame), 0);
        let d = sbe::cancel_all_orders_codec::CancelAllOrdersDecoder::default().header(header, 0);
        assert_eq!(
            d.include_triggers(),
            u8::from(include),
            "cancel-all {include}"
        );

        let options = if include {
            CancelAllAfterOptions::with_triggers()
        } else {
            CancelAllAfterOptions::default()
        };
        let frame =
            codec::encode_cancel_all_after(Duration::from_secs(30), 7, options, &deadline(500))
                .unwrap();
        let header = sbe::message_header_codec::MessageHeaderDecoder::default()
            .wrap(sbe::ReadBuf::new(&frame), 0);
        let d = sbe::cancel_all_after_codec::CancelAllAfterDecoder::default().header(header, 0);
        assert_eq!(d.include_triggers(), u8::from(include), "switch {include}");
    }
    assert!(!CancelAllOrders::new(vec![]).include_triggers);
    assert!(
        CancelAllOrders::new(vec![])
            .with_triggers()
            .include_triggers
    );
}

#[test]
fn every_order_is_post_only_gtc_limit_with_its_deadline() {
    let frame = &client_frames()[1].1;
    let header = sbe::message_header_codec::MessageHeaderDecoder::default()
        .wrap(sbe::ReadBuf::new(frame), 0);
    let mut d = sbe::create_order_codec::CreateOrderDecoder::default().header(header, 0);
    assert_eq!(d.order_type(), sbe::order_type::OrderType::LIMIT);
    assert_eq!(d.time_in_force(), sbe::time_in_force::TimeInForce::GTC);
    assert!(d.flags().get_post_only());
    assert_eq!(d.stp_mode(), sbe::stp_mode::STPMode::EXPIRE_MAKER);
    assert_eq!(
        (d.timestamp_ms(), d.recv_window_ms()),
        (Some(1_790_000_000_123), Some(500))
    );
    let coordinates = d.client_order_id_decoder();
    assert_eq!(d.client_order_id_slice(coordinates), CLIENT.as_bytes());
    let modify = &client_frames()[3].1;
    let header = sbe::message_header_codec::MessageHeaderDecoder::default()
        .wrap(sbe::ReadBuf::new(modify), 0);
    let d = sbe::modify_order_codec::ModifyOrderDecoder::default().header(header, 0);
    assert_eq!(
        d.behavior(),
        sbe::modify_behavior::ModifyBehavior::AMEND_OR_REPLACE
    );
    assert_eq!(d.new_qty_scaled(), None);
}

/// Encodes one message with a generated encoder into a zeroed frame of its
/// block length.
macro_rules! generated {
    ($codec:ident :: $encoder:ident, |$e:ident| $body:block) => {{
        let mut buf = vec![0u8; HEADER + $codec::SBE_BLOCK_LENGTH as usize];
        let mut $e = $codec::$encoder::default()
            .wrap(WriteBuf::new(&mut buf), HEADER)
            .header(0)
            .parent()
            .unwrap();
        $body
        buf
    }};
}

#[test]
fn raw_offsets_are_the_generated_codecs() {
    let f = generated!(cancel_all_orders_codec::CancelAllOrdersEncoder, |e| {
        e.side(side::Side::SELL);
    });
    assert_eq!(f[HEADER + codec::CANCEL_ALL_SIDE], 2);
    let f = generated!(cancel_all_after_codec::CancelAllAfterEncoder, |e| {
        e.side(side::Side::SELL);
    });
    assert_eq!(f[HEADER + codec::CANCEL_ALL_AFTER_SIDE], 2);
    let f = generated!(command_reject_codec::CommandRejectEncoder, |e| {
        e.code(reject_code::RejectCode::SEQUENCE_OUT_OF_ORDER);
    });
    assert_eq!(
        u16::from_le_bytes([
            f[HEADER + codec::REJECT_CODE],
            f[HEADER + codec::REJECT_CODE + 1]
        ]),
        1001
    );
    let f = generated!(command_ack_codec::CommandAckEncoder, |e| {
        e.listing_status(
            cancel_all_listing_status::CancelAllListingStatus::NOT_RETAINED_READ_OPEN_ORDERS,
        );
    });
    assert_eq!(f[HEADER + codec::ACK_LISTING], 2);
    let f = generated!(order_event_codec::OrderEventEncoder, |e| {
        e.status(order_status::OrderStatus::FILLED);
        e.reject_reason(reject_code::RejectCode::REQUEST_DEADLINE_EXCEEDED);
        e.cancel_reason(sbe::cancel_reason::CancelReason::CANCEL_ON_DISCONNECT);
    });
    assert_eq!(f[HEADER + codec::ORDER_STATUS], 5);
    assert_eq!(
        u16::from_le_bytes([
            f[HEADER + codec::ORDER_REJECT_REASON],
            f[HEADER + codec::ORDER_REJECT_REASON + 1]
        ]),
        308
    );
    assert_eq!(f[HEADER + codec::ORDER_CANCEL_REASON], 21);
    let f = generated!(execution_event_codec::ExecutionEventEncoder, |e| {
        e.fee_asset(fee_asset::FeeAsset::BASE);
    });
    assert_eq!(f[HEADER + codec::EXECUTION_FEE_ASSET], 2);
    let f = generated!(market_resync_codec::MarketResyncEncoder, |e| {
        e.reason(market_resync_reason::MarketResyncReason::DISCONTINUITY);
    });
    assert_eq!(f[HEADER + codec::RESYNC_REASON], 2);
    let f = generated!(batch_replace_ack_codec::BatchReplaceAckEncoder, |e| {
        e.status(sbe::batch_status::BatchStatus::REJECTED);
    });
    assert_eq!(f[HEADER + codec::REPLACE_ACK_STATUS], 3);
    let f = generated!(oco_ack_codec::OcoAckEncoder, |e| {
        e.command_type(command_type::CommandType::CANCEL_OCO);
    });
    assert_eq!(f[HEADER + codec::OCO_ACK_COMMAND], 12);
    assert_eq!(u8::from(command_type::CommandType::CREATE_OCO), 11);

    // Group entries: one entry each, after the block and the group header.
    let mut buf = vec![0u8; HEADER + 64 + 4 + 35];
    let e = batch_create_ack_codec::BatchCreateAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    let mut g = e.results_encoder(1, Default::default());
    g.advance().unwrap();
    g.status(batch_item_status::BatchItemStatus::REJECTED);
    g.code(reject_code::RejectCode::ORDER_CREATE_OUTCOME_UNKNOWN);
    let entry = HEADER + 64 + 4;
    assert_eq!(
        u16::from_le_bytes([
            buf[entry + codec::ITEM_CODE],
            buf[entry + codec::ITEM_CODE + 1]
        ]),
        303
    );
    let mut buf = vec![0u8; HEADER + 73 + 4 + 20];
    let e = batch_replace_ack_codec::BatchReplaceAckEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    let mut g = e.results_encoder(1, Default::default());
    g.advance().unwrap();
    g.action(replace_action::ReplaceAction::REPLACED);
    assert_eq!(buf[HEADER + 73 + 4 + codec::REPLACE_ITEM_ACTION], 2);
}

#[test]
fn an_absent_side_is_the_schemas_null_not_the_generated_one() {
    // The generated Rust enums spell a ZeroNullUint8 enum's null as 0xff;
    // the schema's null is 0.
    assert_eq!(u8::from(side::Side::NullVal), 0xff);
    let all = &client_frames()[7].1;
    assert_eq!(all[HEADER + codec::CANCEL_ALL_SIDE], 0);
    let timer = &client_frames()[8].1;
    assert_eq!(timer[HEADER + codec::CANCEL_ALL_AFTER_SIDE], 0);
    let mut sell = CancelAllOrders::new(vec![PAIR]);
    sell.side = Some(Side::Sell);
    let frame = codec::encode_cancel_all(&sell, &deadline(500)).unwrap();
    assert_eq!(frame[HEADER + codec::CANCEL_ALL_SIDE], 2);
}

#[test]
fn named_codes_and_reasons_are_the_schemas() {
    for (code, name) in [
        (RejectCode::BAD_REQUEST, "BAD_REQUEST"),
        (
            RejectCode::CONFLICT_DUPLICATE_CLIENT_ORDER_ID,
            "CONFLICT_DUPLICATE_CLIENT_ORDER_ID",
        ),
        (RejectCode::UNAVAILABLE, "UNAVAILABLE"),
        (RejectCode::ORDER_UNKNOWN, "ORDER_UNKNOWN"),
        (
            RejectCode::CONFLICT_IDEMPOTENCY_KEY_REUSE,
            "CONFLICT_IDEMPOTENCY_KEY_REUSE",
        ),
        (RejectCode::POST_ONLY_CROSS, "POST_ONLY_CROSS"),
        (RejectCode::ORDER_ALREADY_TERMINAL, "ORDER_ALREADY_TERMINAL"),
        (RejectCode::OVERLOADED, "OVERLOADED"),
        (RejectCode::RATE_LIMIT_EXCEEDED, "RATE_LIMIT_EXCEEDED"),
        (RejectCode::SYSTEM_RATE_LIMITED, "SYSTEM_RATE_LIMITED"),
        (
            RejectCode::ORDER_CANCEL_OUTCOME_UNKNOWN,
            "ORDER_CANCEL_OUTCOME_UNKNOWN",
        ),
        (
            RejectCode::ORDER_MODIFY_OUTCOME_UNKNOWN,
            "ORDER_MODIFY_OUTCOME_UNKNOWN",
        ),
        (RejectCode::ACCOUNT_REPAIRING, "ACCOUNT_REPAIRING"),
        (
            RejectCode::ORDER_CREATE_OUTCOME_UNKNOWN,
            "ORDER_CREATE_OUTCOME_UNKNOWN",
        ),
        (
            RejectCode::REQUEST_DEADLINE_EXCEEDED,
            "REQUEST_DEADLINE_EXCEEDED",
        ),
        (RejectCode::SEQUENCE_OUT_OF_ORDER, "SEQUENCE_OUT_OF_ORDER"),
    ] {
        assert_eq!(code.name().as_deref(), Some(name));
    }
    assert!(RejectCode::ORDER_CREATE_OUTCOME_UNKNOWN.outcome_unknown());
    assert!(!RejectCode::POST_ONLY_CROSS.outcome_unknown());
    assert_eq!(RejectCode::from_raw(9999).to_string(), "UNKNOWN(9999)");
    assert_eq!(RejectCode::from_raw(9999).name(), None);

    assert_eq!(CancelReason::from_raw(0), None);
    for raw in 1..=21u8 {
        let reason = CancelReason::from_raw(raw).unwrap();
        assert!(!matches!(reason, CancelReason::Unknown(_)), "{raw}");
        assert_eq!(reason.raw(), raw);
        assert_eq!(
            reason.name(),
            sbe::cancel_reason::CancelReason::from(raw).to_string()
        );
    }
    assert_eq!(
        CancelReason::from_raw(12),
        Some(CancelReason::CancelAllAfter)
    );
    assert_eq!(
        CancelReason::from_raw(21),
        Some(CancelReason::CancelOnDisconnect)
    );
    assert_eq!(
        CancelReason::CancelOnDisconnect.name(),
        "CANCEL_ON_DISCONNECT"
    );
    assert_eq!(CancelReason::from_raw(22), Some(CancelReason::Unknown(22)));
}

fn validation(result: crate::errors::Result<Vec<u8>>) -> bool {
    matches!(result, Err(Error::Validation(_)))
}

#[test]
fn commands_the_server_would_refuse_are_refused_here() {
    let ms = Duration::from_millis;
    assert!(Deadline::within(Duration::ZERO).is_err(), "no deadline");
    assert!(Deadline::within(ms(60_001)).is_err());
    assert!(Deadline::within(Duration::from_micros(1_500)).is_err());
    assert!(Deadline::within(ms(60_000)).is_ok());
    assert!(Deadline::new(UNIX_EPOCH, ms(500)).is_err());
    assert!(
        Deadline::new(SystemTime::now() + Duration::from_secs(2), ms(500)).is_err(),
        "the server refuses a timestamp ahead of its clock without taking the seq"
    );
    let d = deadline(500);
    assert_eq!(d.expires_at(), UNIX_EPOCH + ms(1_790_000_000_623));

    let bad_orders = [
        order(Side::Buy, 1, &"x".repeat(37)),
        order(Side::Buy, 1, ""),
        order(Side::Buy, 1, "has space"),
        order(Side::Buy, 0, "zero-price"),
        PostOnlyOrder {
            symbol_id: 0,
            ..order(Side::Buy, 1, "no-pair")
        },
        PostOnlyOrder {
            qty: Quantity::from_scaled(1, None, QuantityDomain::OrderBase, None, Some(8)).unwrap(),
            ..order(Side::Buy, 1, "other-pair")
        },
        PostOnlyOrder {
            qty: Quantity::from_scaled(0, None, QuantityDomain::OrderBase, None, None).unwrap(),
            ..order(Side::Buy, 1, "zero-qty")
        },
        PostOnlyOrder {
            qty: Quantity::from_quote_scaled(1, 6, None, None).unwrap(),
            ..order(Side::Buy, 1, "quote-qty")
        },
    ];
    for o in &bad_orders {
        assert!(validation(codec::encode_create(o, 1, &d)), "{o:?}");
    }
    assert!(validation(codec::encode_batch_create(
        &BatchCreateOrders::new(
            (0..21)
                .map(|i| order(Side::Buy, 1, &format!("o{i}")))
                .collect()
        ),
        &d
    )));
    assert!(validation(codec::encode_batch_create(
        &BatchCreateOrders::new(vec![]),
        &d
    )));
    let mut zero = BatchCreateOrders::new(vec![order(Side::Buy, 1, "a")]);
    zero.correlation_id = 0;
    assert!(validation(codec::encode_batch_create(&zero, &d)));
    assert!(validation(codec::encode_cancel(
        PAIR,
        &OrderRef::Id(0),
        1,
        &d
    )));
    assert!(validation(codec::encode_cancel(0, &OrderRef::Id(1), 1, &d)));
    assert!(validation(codec::encode_cancel_all(
        &CancelAllOrders::new((1..=101).collect()),
        &d
    )));
    assert!(validation(codec::encode_cancel_all(
        &CancelAllOrders::new(vec![0]),
        &d
    )));
    for timeout in [
        Duration::from_secs(9),
        Duration::from_secs(121),
        Duration::from_millis(10_500),
    ] {
        assert!(validation(codec::encode_cancel_all_after(
            timeout,
            1,
            Default::default(),
            &d
        )));
    }
    assert!(codec::encode_cancel_all_after(Duration::ZERO, 1, Default::default(), &d).is_ok());
    let unchanged = ModifyOrder::new(PAIR, OrderRef::Id(ORDER));
    assert!(validation(codec::encode_modify(&unchanged, &d)));
    let mut zero_price = ModifyOrder::new(PAIR, OrderRef::Id(ORDER));
    zero_price.price = Some(price(0));
    assert!(validation(codec::encode_modify(&zero_price, &d)));
    let mut replace_only = ModifyOrder::new(PAIR, OrderRef::Id(ORDER));
    replace_only.behavior = ModifyBehavior::ReplaceOnly;
    replace_only.qty = Some(qty(5));
    assert!(codec::encode_modify(&replace_only, &d).is_ok());
    assert!(validation(codec::encode_batch_replace(
        &BatchReplaceOrders::new(
            PAIR,
            vec![ReplaceItem {
                order: OrderRef::Id(1),
                qty: None,
                price: None
            }]
        ),
        &d
    )));
    assert!(validation(codec::encode_batch_cancel(
        &BatchCancelOrders::new(
            (0..51)
                .map(|i| CancelItem {
                    symbol_id: PAIR,
                    order: OrderRef::Id(i + 1)
                })
                .collect()
        ),
        &d
    )));
}

#[test]
fn correlation_ids_are_fresh_and_nonzero() {
    let ids: std::collections::HashSet<u64> = (0..10_000)
        .map(|_| crate::tradews::new_correlation_id())
        .collect();
    assert_eq!(ids.len(), 10_000);
    assert!(!ids.contains(&0));
}
