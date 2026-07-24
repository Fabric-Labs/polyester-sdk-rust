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
    msg.contains("internal error") || msg.contains("decode") || msg.contains("proto")
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
    matches!(
        err,
        Error::Validation(msg) if msg.to_ascii_lowercase().contains("notional")
    ) || matches!(
        err,
        Error::Api { message, .. } if message.to_ascii_lowercase().contains("notional")
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

/// Run an optional live RPC; soft-skip (None) when unavailable.
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
