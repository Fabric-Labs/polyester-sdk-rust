//! SDK error types (parity with Go/Python).

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use buffa::{Enumeration, Message};
use connectrpc::{ConnectError, ErrorCode, ErrorDetail};
use thiserror::Error;

use crate::proto::auth::v1::AuthErrorDetail;

/// Root result alias for the SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// Polyester SDK error.
#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("{0}")]
    Auth(String),
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Transport(String),
    #[error("{message}")]
    RateLimit {
        message: String,
        retry_after: Option<f64>,
    },
    #[error("{0}")]
    Server(String),
    #[error("{message}")]
    Api {
        message: String,
        code: String,
        metadata: Vec<(String, String)>,
    },
    #[error(
        "RPC not exposed on this API host{procedure}. The procedure may be unimplemented on \
         devnet or disabled in this environment."
    )]
    RouteNotFound { procedure: String },
    #[error("{0}")]
    Realtime(String),
    /// Realtime subscription queue was full; the subscription fails instead of
    /// silently dropping updates.
    #[error("{0}")]
    QueueOverflow(String),
}

impl Error {
    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Auth(msg.into())
    }

    pub fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(msg.into())
    }

    pub fn transport(msg: impl Into<String>) -> Self {
        Self::Transport(msg.into())
    }

    pub fn realtime(msg: impl Into<String>) -> Self {
        Self::Realtime(msg.into())
    }

    pub fn queue_overflow(msg: impl Into<String>) -> Self {
        Self::QueueOverflow(msg.into())
    }
}

fn decode_auth_error_detail(detail: &ErrorDetail) -> Option<AuthErrorDetail> {
    if !detail.type_url.ends_with("auth.v1.AuthErrorDetail") {
        return None;
    }
    let value = detail.value.as_ref()?;
    let bytes = STANDARD_NO_PAD
        .decode(value)
        .or_else(|_| STANDARD.decode(value))
        .ok()?;
    AuthErrorDetail::decode_from_slice(&bytes).ok()
}

/// Map a ConnectRPC error into an SDK error.
pub fn map_connect_error(err: ConnectError) -> Error {
    let fallback_message = err.to_string();
    for detail in &err.details {
        if let Some(auth_detail) = decode_auth_error_detail(detail) {
            let code = auth_detail
                .code
                .as_known()
                .map(|c| c.proto_name().to_owned())
                .unwrap_or_else(|| "AUTH_UNSPECIFIED".to_owned());
            let message = if auth_detail.message.is_empty() {
                fallback_message.clone()
            } else {
                auth_detail.message
            };
            return Error::Api {
                message,
                code,
                metadata: Vec::new(),
            };
        }
    }
    let code = err.code;
    let message = fallback_message;
    match code {
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => Error::Auth(message),
        ErrorCode::Unavailable | ErrorCode::Internal => Error::Server(message),
        ErrorCode::DeadlineExceeded => Error::Transport(message),
        ErrorCode::Unimplemented => {
            if message.contains("not found")
                || message.contains("unimplemented")
                || message.contains("404")
            {
                Error::RouteNotFound {
                    procedure: String::new(),
                }
            } else {
                Error::Api {
                    message,
                    code: format!("{code:?}"),
                    metadata: Vec::new(),
                }
            }
        }
        _ => Error::Api {
            message,
            code: format!("{code:?}"),
            metadata: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::AuthErrorCode;
    use buffa::EnumValue;

    #[test]
    fn map_connect_error_surfaces_auth_revision_conflict() {
        let detail_msg = AuthErrorDetail {
            code: EnumValue::Known(AuthErrorCode::AUTH_REVISION_CONFLICT),
            message: "resource changed".into(),
            ..Default::default()
        };
        let err = ConnectError::aborted("aborted").with_detail(ErrorDetail::from_message(
            "auth.v1.AuthErrorDetail",
            &detail_msg,
        ));
        match map_connect_error(err) {
            Error::Api { message, code, .. } => {
                assert_eq!(code, "AUTH_REVISION_CONFLICT");
                assert_eq!(message, "resource changed");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
