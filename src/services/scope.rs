use super::ServiceContext;
use crate::codecs::scalars::id_to_u64;
use crate::errors::{Error, Result};

/// Refuse sub-account ID 0: on the wire 0 means unset (the main account), so
/// it must not be sent as an explicit sub-account.
pub fn explicit_subaccount(id: u64) -> Result<u64> {
    if id == 0 {
        return Err(Error::validation(
            "subaccount_id must be a non-zero ID, not 0",
        ));
    }
    Ok(id)
}

/// Refuse an explicit `Some(0)` in a caller-built request before it is sent;
/// `None` still means no sub-account.
pub fn request_subaccount(id: Option<u64>) -> Result<()> {
    id.map_or(Ok(()), |id| explicit_subaccount(id).map(drop))
}

pub fn optional_subaccount(ctx: &ServiceContext, explicit: Option<u64>) -> Result<Option<u64>> {
    if let Some(id) = explicit {
        return explicit_subaccount(id).map(Some);
    }
    if let Some(ref s) = ctx.default_sub_account_id {
        if s.trim().is_empty() {
            return Ok(None);
        }
        // Configured defaults use the same canonical public base58
        // representation returned by subaccount APIs. Digit-only base58 IDs
        // are valid and must not be interpreted as decimal first.
        return Ok(Some(id_to_u64(s, "subaccount_id")?));
    }
    Ok(None)
}

pub fn require_account_id(ctx: &ServiceContext) -> Result<String> {
    ctx.default_account_id
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::validation("default_account_id is required for this call"))
}

pub fn resolve_account_id(ctx: &ServiceContext, explicit: Option<&str>) -> Result<String> {
    if let Some(value) = explicit {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_owned());
        }
    }
    require_account_id(ctx)
}
