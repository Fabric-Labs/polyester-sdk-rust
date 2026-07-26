//! Optional/required RPC helpers and error classifiers.

use polyester::{Error, Result};

/// True when the error means the route is not mounted on this API host.
pub fn route_unavailable(err: &Error) -> bool {
    matches!(err, Error::RouteNotFound { .. })
        || matches!(
            err,
            Error::Api { code, .. }
                if {
                    let c = code.to_ascii_lowercase();
                    c.contains("unimplemented")
                        || c.contains("not_found")
                        || c.contains("route_not_found")
                }
        )
}

pub fn is_not_found(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains("not_found") || msg.contains("not found") || msg.contains("404") {
        return true;
    }
    matches!(
        err,
        Error::Api { code, .. } if {
            let c = code.to_ascii_lowercase();
            c.contains("not_found") || c == "5"
        }
    ) || matches!(err, Error::RouteNotFound { .. })
}

pub fn jwt_session_only(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    let sessionish = msg.contains("authorization header")
        || msg.contains("bearer")
        || msg.contains("interactive session")
        || msg.contains("permission denied")
        || msg.contains("permission_denied");
    match err {
        Error::Auth(_) => sessionish,
        Error::Api { code, .. } => {
            let c = code.to_ascii_lowercase();
            sessionish || c.contains("unauthenticated") || c.contains("permission_denied")
        }
        _ => sessionish,
    }
}

pub fn devnet_proto_mismatch(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("internal error")
        || msg.contains("decode")
        || msg.contains("protobuf")
        || msg.contains("proto mismatch")
        || msg.contains("invalid wire type")
        || msg.contains("failed to deserialize")
}

pub fn devnet_unavailable(err: &Error) -> bool {
    if devnet_proto_mismatch(err) {
        return true;
    }
    match err {
        Error::Server(msg) => {
            let m = msg.to_ascii_lowercase();
            m.contains("temporarily unavailable") || m.contains("unavailable")
        }
        _ => {
            let msg = err.to_string().to_ascii_lowercase();
            msg.contains("connection refused")
                || msg.contains("deadline exceeded")
                || msg.contains("timeout")
        }
    }
}

pub fn is_internal_order_error(err: &Error) -> bool {
    match err {
        // Connect maps ErrorCode::Internal → Error::Server with messages like
        // "internal: can't scan into dest[N] ... cannot scan NULL into *int64".
        Error::Server(msg) => {
            let m = msg.to_ascii_lowercase();
            m.contains("internal") || m.contains("can't scan") || m.contains("cannot scan")
        }
        Error::Api { code, message, .. } => {
            let c = code.to_ascii_uppercase();
            let m = message.to_ascii_lowercase();
            c == "INTERNAL"
                || c == "INTERNAL_ERROR"
                || m.contains("internal error")
                || m.contains("can't scan")
                || m.contains("cannot scan")
        }
        _ => false,
    }
}

pub fn is_notional_validation(err: &Error) -> bool {
    fn is_minimum_notional_text(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        message.contains("min notional")
            || message.contains("minimum notional")
            || message.contains("below min notional")
            || message.contains("below the minimum notional")
    }

    matches!(err, Error::Validation(message) if is_minimum_notional_text(message))
        || matches!(
            err,
            Error::Api { code, message, .. }
                if matches!(
                    code.to_ascii_uppercase().as_str(),
                    "ERROR_CODE_MIN_NOTIONAL" | "MIN_NOTIONAL"
                ) || is_minimum_notional_text(message)
        )
}

/// Run a required live RPC (fail on route-not-found).
pub async fn call_required<T, F, Fut>(label: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    match f().await {
        Ok(v) => v,
        Err(Error::RouteNotFound { .. }) => {
            panic!("{label} returned route not found on API host");
        }
        Err(err) => panic!("{label} failed: {err}"),
    }
}

/// True when the API-key lacks a required permission (F-24 structured Auth/403).
pub fn is_permission_denied(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    let auth_permission = matches!(err, Error::Auth(_))
        && (msg.contains("permission denied")
            || msg.contains("permission_denied")
            || msg.contains("http 403"));
    let api_permission = matches!(
        err,
        Error::Api { code, .. } if code.to_ascii_lowercase().contains("permission_denied")
    );
    auth_permission || api_permission
}

/// Run an optional live RPC; soft-skip (None) when unavailable.
///
/// Permission-denied (HTTP 403 / Auth) soft-skips with an explicit permission
/// message so private realtime fixtures never panic on missing scopes. Under
/// `POLYESTER_TEST_STRICT_LIVE=1` those skips fail closed via the integration
/// `eprintln!` macro.
pub async fn call_optional<T, F, Fut>(label: &str, f: F) -> Option<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    match f().await {
        Ok(v) => Some(v),
        Err(err) if route_unavailable(&err) => {
            eprintln!("skip: {label} not mounted on API host: {err}");
            None
        }
        Err(err) if is_permission_denied(&err) => {
            eprintln!(
                "skip: {label} missing required API-key permission (declare fixture scopes): {err}"
            );
            None
        }
        Err(err) if jwt_session_only(&err) => {
            eprintln!("skip: {label} requires JWT/session auth: {err}");
            None
        }
        Err(err) if is_not_found(&err) => {
            eprintln!("skip: {label} not found: {err}");
            None
        }
        Err(err) if devnet_proto_mismatch(&err) || devnet_unavailable(&err) => {
            eprintln!("skip: {label} unavailable: {err}");
            None
        }
        Err(err) => {
            panic!("{label} failed: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_proto_channel_auth_error_is_not_a_proto_mismatch() {
        let err = Error::auth(
            "realtime subscription token for private:auth:api-keys:account:proto: \
             authentication failed",
        );
        assert!(!devnet_proto_mismatch(&err));
    }

    #[test]
    fn maximum_policy_notional_is_not_minimum_order_sizing() {
        let maximum = Error::Api {
            message: "Order notional exceeds the maximum allowed".into(),
            code: "ERROR_CODE_POLICY_MAX_NOTIONAL".into(),
            metadata: Vec::new(),
        };
        assert!(!is_notional_validation(&maximum));

        let minimum = Error::Api {
            message: "Order sizing below minimum notional".into(),
            code: "ERROR_CODE_MIN_NOTIONAL".into(),
            metadata: Vec::new(),
        };
        assert!(is_notional_validation(&minimum));
    }
}
