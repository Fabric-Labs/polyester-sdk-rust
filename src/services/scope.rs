use super::ServiceContext;
use crate::codecs::scalars::parse_account_id;
use crate::errors::{Error, Result};
use crate::models::SubAccountScope;

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

/// The sub-account ID a call sends for `sub_account`: `None` for the main
/// account. [`SubAccountScope::Default`] takes the configured default sub-account,
/// or the main account when none is configured; [`SubAccountScope::Main`] is
/// always the main account; [`SubAccountScope::Id`] is sent as given, and 0 is
/// refused.
pub fn resolve_subaccount(
    ctx: &ServiceContext,
    sub_account: SubAccountScope,
) -> Result<Option<u64>> {
    match sub_account {
        SubAccountScope::Main => Ok(None),
        SubAccountScope::Id(id) => explicit_subaccount(id).map(Some),
        SubAccountScope::Default => {
            // Only an unset default means none: a configured one, even empty
            // or blank, is parsed and refused when it is no base58 ID. One
            // that decodes to 0, such as "1", is refused like an explicit 0.
            let Some(s) = ctx.default_sub_account_id.as_deref() else {
                return Ok(None);
            };
            Ok(Some(parse_account_id(s, "subaccount_id")?))
        }
    }
}

/// The sub-account of a caller-built request whose sub-account field is an
/// `Option<u64>`: `None` takes the configured default sub-account, as
/// [`SubAccountScope::Default`] does, and an ID is sent unchanged (0 refused). A
/// request cannot name the main account explicitly while a default is
/// configured: use a typed helper with [`SubAccountScope::Main`] for that.
pub fn optional_subaccount(ctx: &ServiceContext, explicit: Option<u64>) -> Result<Option<u64>> {
    resolve_subaccount(
        ctx,
        explicit.map_or(SubAccountScope::Default, SubAccountScope::Id),
    )
}

/// The sub-account of a caller-built request whose sub-account field is a
/// plain `u64`, where 0 is unset: a non-zero ID is sent unchanged, and 0 takes
/// the configured default sub-account, as `None` does in
/// [`optional_subaccount`]. With no default it stays 0, the main account. A
/// request cannot name the main account explicitly while a default is
/// configured: use a typed helper with [`SubAccountScope::Main`] for that.
pub fn plain_subaccount(ctx: &ServiceContext, id: u64) -> Result<u64> {
    if id != 0 {
        return Ok(id);
    }
    Ok(optional_subaccount(ctx, None)?.unwrap_or(0))
}

pub fn require_account_id(ctx: &ServiceContext) -> Result<String> {
    ctx.default_account_id
        .clone()
        .ok_or_else(|| Error::validation("default_account_id is required for this call"))
}

/// Resolve the account ID from `explicit`, or else, only when it is `None`,
/// the configured default. One that decodes to 0, such as `"1"`, or that is
/// not bare base58 (empty, blank, a decimal-looking `"0"`, or with
/// whitespace, which is not trimmed) is refused: it would name the channel of
/// account 0, or of no account.
pub fn resolve_account_id(ctx: &ServiceContext, explicit: Option<&str>) -> Result<String> {
    let account = match explicit {
        Some(value) => value.to_owned(),
        None => require_account_id(ctx)?,
    };
    parse_account_id(&account, "account_id")?;
    Ok(account)
}
