//! Shared proto decoders.

use crate::models::ApiData;
use buffa_types::google::protobuf::Timestamp;
use serde::Serialize;

/// Refuse a reply whose required public ID is 0, the proto default of an
/// absent field: it breaks the response contract, and the call may have
/// applied, so the caller must not keep `"0"` as the ID of what it changed.
pub(crate) fn require_id(operation: &str, field: &str, id: u64) -> crate::errors::Result<()> {
    if id == 0 {
        return Err(crate::errors::Error::response_contract(
            operation,
            format!("missing {field}"),
        ));
    }
    Ok(())
}

/// Refuse a reply whose required string ID is empty, the proto default of an
/// absent field, as [`require_id`] refuses a numeric ID of 0.
pub(crate) fn require_str_id(operation: &str, field: &str, id: &str) -> crate::errors::Result<()> {
    if id.is_empty() {
        return Err(crate::errors::Error::response_contract(
            operation,
            format!("missing {field}"),
        ));
    }
    Ok(())
}

/// A `GetZippedAssetSupplyGroup` answer as [`ApiData`], refused with
/// `Error::ResponseContract` without its group ID.
pub fn zipped_asset_supply_group_from_proto(
    msg: &crate::proto::chain::analytics::v1::GetZippedAssetSupplyGroupResponse,
) -> crate::errors::Result<ApiData> {
    require_str_id("GetZippedAssetSupplyGroup", "group_id", &msg.group_id)?;
    Ok(api_data_from_proto(msg))
}

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
