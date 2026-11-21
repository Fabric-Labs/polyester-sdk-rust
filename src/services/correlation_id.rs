//! Local fail-closed validation for client/request correlation identifiers.
//!
//! Matches the documented ASCII charset and length limits so invalid ids never
//! reach the wire as if they were accepted.

use crate::errors::{Error, Result};

pub(crate) const CLIENT_ORDER_ID_MAX_LEN: usize = 36;
pub(crate) const REQUEST_ID_MAX_LEN: usize = 64;

fn is_allowed_correlation_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '/' | '-')
}

fn validate_correlation_id(trimmed: &str, field: &str, max_len: usize) -> Result<()> {
    if trimmed.is_empty() || trimmed.len() > max_len {
        return Err(Error::validation(format!(
            "{field} must be 1 to {max_len} characters"
        )));
    }
    if !trimmed.chars().all(is_allowed_correlation_char) {
        return Err(Error::validation(format!(
            "{field} contains invalid characters; allowed: A-Z a-z 0-9 . _ : / -"
        )));
    }
    Ok(())
}

/// Validate a request ID the caller supplied, exactly as given: it is never
/// trimmed, and a blank one is refused rather than replaced.
pub(crate) fn exact_request_id(value: &str) -> Result<String> {
    validate_correlation_id(value, "request_id", REQUEST_ID_MAX_LEN)?;
    Ok(value.to_owned())
}

/// Trim and validate an optional client order id. Blank/whitespace becomes `None`.
pub(crate) fn optional_client_order_id(value: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_correlation_id(trimmed, "client_order_id", CLIENT_ORDER_ID_MAX_LEN)?;
    Ok(Some(trimmed.to_owned()))
}

/// The code the Triggers API answers for a client OCO ID that breaks its
/// rule, on `CreateOco` and the `ListTriggers` filter alike.
pub(crate) const CLIENT_LIST_ID_INVALID: &str = "ERROR_CODE_CLIENT_LIST_ID_INVALID";

/// Validate a client OCO ID (`OcoIntent.client_list_id` and the
/// `ListTriggersRequest.client_list_id` filter) as the contract does: the
/// client order ID rule, 1 to 36 ASCII letters, digits, `.`, `_`, `:`, `/` or
/// `-` (`^[A-Za-z0-9._:/-]{1,36}$`). One that breaks it is refused as
/// [`Error::Api`] with [`CLIENT_LIST_ID_INVALID`], the server's code for the
/// same mistake, and no violation. It is sent as given, never trimmed, so the
/// create and the recovery listing send the same ID; outer spaces are refused
/// like any other space.
pub(crate) fn require_client_list_id(value: &str, field: &str) -> Result<String> {
    match validate_correlation_id(value, field, CLIENT_ORDER_ID_MAX_LEN) {
        Ok(()) => Ok(value.to_owned()),
        Err(Error::Validation(message)) => Err(Error::Api {
            message,
            code: CLIENT_LIST_ID_INVALID.to_owned(),
            metadata: Vec::new(),
            violations: Vec::new(),
            answer: None,
        }),
        Err(other) => Err(other),
    }
}

/// Trim and validate a required client-style id (client order / trigger id).
pub(crate) fn require_client_style_id(value: &str, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::validation(format!(
            "{field} is required and must remain stable across retries"
        )));
    }
    validate_correlation_id(trimmed, field, CLIENT_ORDER_ID_MAX_LEN)?;
    Ok(trimmed.to_owned())
}

/// Trim and validate an optional request id. Blank/whitespace becomes `None`.
pub(crate) fn optional_request_id(value: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_correlation_id(trimmed, "request_id", REQUEST_ID_MAX_LEN)?;
    Ok(Some(trimmed.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_client_order_id_accepts_boundary_and_charset() {
        assert_eq!(optional_client_order_id(None).unwrap(), None);
        assert_eq!(optional_client_order_id(Some("")).unwrap(), None);
        assert_eq!(optional_client_order_id(Some("   ")).unwrap(), None);
        assert_eq!(
            optional_client_order_id(Some(" A.B_c:1/2-3 "))
                .unwrap()
                .as_deref(),
            Some("A.B_c:1/2-3")
        );
        let max = "a".repeat(CLIENT_ORDER_ID_MAX_LEN);
        assert_eq!(
            optional_client_order_id(Some(&max)).unwrap().as_deref(),
            Some(max.as_str())
        );
    }

    #[test]
    fn optional_client_order_id_rejects_length_and_charset() {
        let too_long = "a".repeat(CLIENT_ORDER_ID_MAX_LEN + 1);
        let err = optional_client_order_id(Some(&too_long)).unwrap_err();
        assert!(err.to_string().contains("1 to 36"));
        let err = optional_client_order_id(Some("bad id")).unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
        let err = optional_client_order_id(Some("id!")).unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn optional_request_id_allows_up_to_64() {
        let max = "r".repeat(REQUEST_ID_MAX_LEN);
        assert_eq!(
            optional_request_id(Some(&max)).unwrap().as_deref(),
            Some(max.as_str())
        );
        let too_long = "r".repeat(REQUEST_ID_MAX_LEN + 1);
        assert!(optional_request_id(Some(&too_long)).is_err());
    }

    /// A client OCO ID follows the client order ID rule and keeps its error
    /// text; it is never trimmed.
    #[test]
    fn client_list_id_follows_the_client_order_id_rule() {
        for ok in ["tp-sl:btc/2026-11-15_1", "A.B_c:1/2-3", &"x".repeat(36)] {
            assert_eq!(require_client_list_id(ok, "client_list_id").unwrap(), ok);
        }
        for (bad, want) in [
            ("", "client_list_id must be 1 to 36 characters"),
            (
                &"x".repeat(37) as &str,
                "client_list_id must be 1 to 36 characters",
            ),
            (" oco-1 ", "client_list_id contains invalid characters"),
            ("oco 1", "client_list_id contains invalid characters"),
            ("oco!", "client_list_id contains invalid characters"),
            (
                "\u{e9}t\u{e9}",
                "client_list_id contains invalid characters",
            ),
        ] {
            let err = require_client_list_id(bad, "client_list_id").unwrap_err();
            assert_eq!(
                err.auth_error_code(),
                Some(CLIENT_LIST_ID_INVALID),
                "{bad:?}: {err:?}"
            );
            assert!(matches!(&err, Error::Api { violations, .. } if violations.is_empty()));
            assert!(err.to_string().contains(want), "{bad:?}: {err}");
        }
    }
}
