//! Shared proto decoders.

use crate::models::ApiData;
use buffa_types::google::protobuf::Timestamp;
use serde::Serialize;

pub fn api_data_from_proto<T: Serialize>(msg: &T) -> ApiData {
    match serde_json::to_value(msg) {
        Ok(raw) => ApiData { raw },
        Err(_) => ApiData::empty(),
    }
}

/// A response time in nanoseconds since the Unix epoch: the exact `*_ts_ns`
/// field, or its paired `Timestamp` when the nanosecond field is unset; 0
/// when neither is set or the time is before the epoch.
pub(crate) fn paired_ts_ns(ts_ns: u64, ts: Option<&Timestamp>) -> u64 {
    if ts_ns != 0 {
        return ts_ns;
    }
    ts.map_or(0, |ts| {
        u64::try_from(ts.seconds)
            .ok()
            .and_then(|secs| secs.checked_mul(1_000_000_000))
            .and_then(|ns| ns.checked_add(u64::try_from(ts.nanos).ok()?))
            .unwrap_or(0)
    })
}
