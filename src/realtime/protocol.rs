//! Minimal Centrifugo v2 protobuf client-protocol codec.
//!
//! Centrifugo protobuf WebSocket frames contain one or more length-delimited
//! `Command` or `Reply` messages from the authoritative
//! `centrifugal/protocol/definitions/client.proto` schema. Polyester only needs
//! connect, subscribe, errors, pings, and publication payloads.

use super::MAX_REALTIME_MESSAGE_BYTES;
use crate::errors::{Error, Result};

const WIRE_VARINT: u8 = 0;
const WIRE_FIXED64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_FIXED32: u8 = 5;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Incoming {
    Reply { id: u32, error: Option<ProtoError> },
    Publication(Vec<u8>),
    Ping,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ProtoError {
    pub code: u32,
    pub message: String,
    pub temporary: bool,
}

pub(super) fn connect_command(id: u32, token: Option<&str>) -> Vec<u8> {
    let mut request = Vec::new();
    if let Some(token) = token {
        put_string(&mut request, 1, token);
    }
    command(id, 4, &request)
}

pub(super) fn subscribe_command(id: u32, channel: &str, token: Option<&str>) -> Vec<u8> {
    let mut request = Vec::new();
    put_string(&mut request, 1, channel);
    if let Some(token) = token {
        put_string(&mut request, 2, token);
    }
    command(id, 5, &request)
}

pub(super) fn pong_command() -> Vec<u8> {
    // A zero-length, length-delimited Command is the protobuf equivalent of
    // the JSON protocol's `{}` pong.
    vec![0]
}

fn command(id: u32, field: u32, request: &[u8]) -> Vec<u8> {
    let mut message = Vec::new();
    put_varint_field(&mut message, 1, u64::from(id));
    put_bytes(&mut message, field, request);
    length_delimit(&message)
}

pub(super) fn decode_replies(frame: &[u8]) -> Result<Vec<Incoming>> {
    if frame.len() > MAX_REALTIME_MESSAGE_BYTES {
        return Err(Error::realtime(format!(
            "centrifugo protobuf message exceeds {MAX_REALTIME_MESSAGE_BYTES} bytes"
        )));
    }
    let mut cursor = Cursor::new(frame);
    let mut out = Vec::new();
    while !cursor.is_empty() {
        let length = cursor.varint()?;
        if length > MAX_REALTIME_MESSAGE_BYTES as u64 {
            return Err(Error::realtime(format!(
                "centrifugo protobuf record exceeds {MAX_REALTIME_MESSAGE_BYTES} bytes"
            )));
        }
        let length = usize::try_from(length)
            .map_err(|_| Error::realtime("centrifugo protobuf length exceeds usize".to_owned()))?;
        let reply = cursor.take(length)?;
        decode_reply(reply, &mut out)?;
    }
    Ok(out)
}

fn decode_reply(bytes: &[u8], out: &mut Vec<Incoming>) -> Result<()> {
    if bytes.is_empty() {
        out.push(Incoming::Ping);
        return Ok(());
    }

    let mut cursor = Cursor::new(bytes);
    let mut id = 0_u32;
    let mut error = None;
    let mut saw_payload = false;
    while !cursor.is_empty() {
        let (field, wire) = cursor.key()?;
        match (field, wire) {
            (1, WIRE_VARINT) => id = to_u32(cursor.varint()?, "reply id")?,
            (2, WIRE_LEN) => error = Some(decode_error(cursor.len_bytes()?)?),
            (4, WIRE_LEN) => {
                saw_payload = true;
                decode_push(cursor.len_bytes()?, out)?;
            }
            _ => cursor.skip(wire)?,
        }
    }
    if id != 0 || error.is_some() || !saw_payload {
        out.push(Incoming::Reply { id, error });
    }
    Ok(())
}

fn decode_error(bytes: &[u8]) -> Result<ProtoError> {
    let mut cursor = Cursor::new(bytes);
    let mut error = ProtoError {
        code: 0,
        message: String::new(),
        temporary: false,
    };
    while !cursor.is_empty() {
        let (field, wire) = cursor.key()?;
        match (field, wire) {
            (1, WIRE_VARINT) => error.code = to_u32(cursor.varint()?, "error code")?,
            (2, WIRE_LEN) => {
                error.message = String::from_utf8(cursor.len_bytes()?.to_vec())
                    .map_err(|e| Error::realtime(format!("centrifugo error utf-8: {e}")))?;
            }
            (3, WIRE_VARINT) => error.temporary = cursor.varint()? != 0,
            _ => cursor.skip(wire)?,
        }
    }
    Ok(error)
}

fn decode_push(bytes: &[u8], out: &mut Vec<Incoming>) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        let (field, wire) = cursor.key()?;
        match (field, wire) {
            (4, WIRE_LEN) => decode_publication(cursor.len_bytes()?, out)?,
            _ => cursor.skip(wire)?,
        }
    }
    Ok(())
}

fn decode_publication(bytes: &[u8], out: &mut Vec<Incoming>) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        let (field, wire) = cursor.key()?;
        match (field, wire) {
            (4, WIRE_LEN) => out.push(Incoming::Publication(cursor.len_bytes()?.to_vec())),
            _ => cursor.skip(wire)?,
        }
    }
    Ok(())
}

fn length_delimit(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 5);
    put_varint(&mut out, message.len() as u64);
    out.extend_from_slice(message);
    out
}

fn put_key(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, u64::from((field << 3) | u32::from(wire)));
}

fn put_varint_field(out: &mut Vec<u8>, field: u32, value: u64) {
    put_key(out, field, WIRE_VARINT);
    put_varint(out, value);
}

fn put_string(out: &mut Vec<u8>, field: u32, value: &str) {
    put_bytes(out, field, value.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    put_key(out, field, WIRE_LEN);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn to_u32(value: u64, label: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::realtime(format!("centrifugo {label} exceeds uint32")))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn key(&mut self) -> Result<(u32, u8)> {
        let key = self.varint()?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| Error::realtime("centrifugo protobuf field overflow".to_owned()))?;
        if field == 0 {
            return Err(Error::realtime(
                "centrifugo protobuf field number is zero".to_owned(),
            ));
        }
        Ok((field, (key & 0x07) as u8))
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or_else(|| Error::realtime("truncated centrifugo protobuf".to_owned()))?;
            self.offset += 1;
            if shift == 63 && byte > 1 {
                return Err(Error::realtime(
                    "centrifugo protobuf varint overflow".to_owned(),
                ));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Error::realtime(
            "centrifugo protobuf varint overflow".to_owned(),
        ))
    }

    fn len_bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.varint()?;
        if length > MAX_REALTIME_MESSAGE_BYTES as u64 {
            return Err(Error::realtime(format!(
                "centrifugo protobuf field exceeds {MAX_REALTIME_MESSAGE_BYTES} bytes"
            )));
        }
        let length = usize::try_from(length)
            .map_err(|_| Error::realtime("centrifugo protobuf length exceeds usize".to_owned()))?;
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Error::realtime("truncated centrifugo protobuf".to_owned()))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn skip(&mut self, wire: u8) -> Result<()> {
        match wire {
            WIRE_VARINT => {
                self.varint()?;
            }
            WIRE_FIXED64 => {
                self.take(8)?;
            }
            WIRE_LEN => {
                self.len_bytes()?;
            }
            WIRE_FIXED32 => {
                self.take(4)?;
            }
            _ => {
                return Err(Error::realtime(format!(
                    "unsupported centrifugo protobuf wire type {wire}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_are_length_delimited_protobuf() {
        assert_eq!(connect_command(1, None), vec![4, 8, 1, 34, 0]);
        assert_eq!(
            subscribe_command(2, "x", None),
            vec![7, 8, 2, 42, 3, 10, 1, b'x']
        );
        assert_eq!(pong_command(), vec![0]);
    }

    #[test]
    fn decodes_ack_error_ping_and_publication_batch() {
        // Reply{id:1}, Reply{}, Reply{push:Push{pub:Publication{data:"abc"}}}
        let frame = [
            2, 8, 1, // ack
            0, // ping
            9, 34, 7, 34, 5, 34, 3, 97, 98, 99, // publication
        ];
        assert_eq!(
            decode_replies(&frame).unwrap(),
            vec![
                Incoming::Reply { id: 1, error: None },
                Incoming::Ping,
                Incoming::Publication(b"abc".to_vec()),
            ]
        );

        // Reply{id:2,error:{code:103,message:"denied",temporary:true}}
        let error = [
            16, 8, 2, 18, 12, 8, 103, 18, 6, 100, 101, 110, 105, 101, 100, 24, 1,
        ];
        assert_eq!(
            decode_replies(&error).unwrap(),
            vec![Incoming::Reply {
                id: 2,
                error: Some(ProtoError {
                    code: 103,
                    message: "denied".to_owned(),
                    temporary: true,
                }),
            }]
        );
    }

    #[test]
    fn rejects_declared_record_above_message_limit_without_allocating() {
        let mut frame = Vec::new();
        put_varint(&mut frame, MAX_REALTIME_MESSAGE_BYTES as u64 + 1);
        let err = decode_replies(&frame).expect_err("oversized record must fail closed");
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn rejects_truncated_frames() {
        assert!(decode_replies(&[5, 8, 1]).is_err());
        assert!(decode_replies(&[0x80]).is_err());
    }
}
