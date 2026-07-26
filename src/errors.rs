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

/// Stable auth.v1.AuthErrorDetail codes used for MFA control flow.
/// Prefer these over ConnectError message text.
pub mod auth_codes {
    pub const MFA_NOT_ENROLLED: &str = "AUTH_MFA_NOT_ENROLLED";
    pub const STEP_UP_REQUIRED: &str = "AUTH_STEP_UP_REQUIRED";
    pub const MFA_ELEVATION_REQUIRED: &str = "AUTH_MFA_ELEVATION_REQUIRED";
    pub const MFA_LAST_FACTOR_REQUIRED: &str = "AUTH_MFA_LAST_FACTOR_REQUIRED";
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

    /// Structured auth.v1.AuthErrorDetail code when this is an [`Error::Api`].
    pub fn auth_error_code(&self) -> Option<&str> {
        match self {
            Self::Api { code, .. } => Some(code.as_str()),
            _ => None,
        }
    }

    /// True when the caller must enroll an MFA factor before continuing.
    pub fn is_mfa_enrollment_required(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::MFA_NOT_ENROLLED)
    }

    /// True when the caller must retry with a fresh `X-Auth-Step-Up` proof.
    pub fn is_step_up_required(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::STEP_UP_REQUIRED)
    }

    /// True when the caller needs a recent MFA-elevated interactive session.
    pub fn is_mfa_elevation_required(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::MFA_ELEVATION_REQUIRED)
    }

    /// True when the final active MFA factor cannot be removed.
    pub fn is_mfa_last_factor_required(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::MFA_LAST_FACTOR_REQUIRED)
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
    let fallback_message = {
        let message = err.to_string();
        if message.trim().is_empty() {
            "request failed without server error details".to_owned()
        } else {
            message
        }
    };
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
        ErrorCode::ResourceExhausted => Error::RateLimit {
            message,
            retry_after: None,
        },
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

    fn map_auth(code: AuthErrorCode, message: &str) -> Error {
        let detail_msg = AuthErrorDetail {
            code: EnumValue::Known(code),
            message: message.into(),
            ..Default::default()
        };
        map_connect_error(ConnectError::permission_denied("denied").with_detail(
            ErrorDetail::from_message("auth.v1.AuthErrorDetail", &detail_msg),
        ))
    }

    #[test]
    fn map_connect_error_surfaces_auth_revision_conflict() {
        match map_auth(AuthErrorCode::AUTH_REVISION_CONFLICT, "resource changed") {
            Error::Api { message, code, .. } => {
                assert_eq!(code, "AUTH_REVISION_CONFLICT");
                assert_eq!(message, "resource changed");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_never_returns_an_empty_auth_message() {
        let mapped = map_connect_error(ConnectError::unauthenticated(""));
        match mapped {
            Error::Auth(message) => assert!(!message.trim().is_empty()),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_surfaces_rate_limits() {
        let mapped = map_connect_error(ConnectError::new(
            ErrorCode::ResourceExhausted,
            "request rate exceeded",
        ));
        match mapped {
            Error::RateLimit {
                message,
                retry_after,
            } => {
                assert_eq!(message, "resource_exhausted: request rate exceeded");
                assert_eq!(retry_after, None);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_surfaces_stable_mfa_codes() {
        let cases = [
            (
                AuthErrorCode::AUTH_MFA_NOT_ENROLLED,
                auth_codes::MFA_NOT_ENROLLED,
                Error::is_mfa_enrollment_required as fn(&Error) -> bool,
            ),
            (
                AuthErrorCode::AUTH_STEP_UP_REQUIRED,
                auth_codes::STEP_UP_REQUIRED,
                Error::is_step_up_required,
            ),
            (
                AuthErrorCode::AUTH_MFA_ELEVATION_REQUIRED,
                auth_codes::MFA_ELEVATION_REQUIRED,
                Error::is_mfa_elevation_required,
            ),
            (
                AuthErrorCode::AUTH_MFA_LAST_FACTOR_REQUIRED,
                auth_codes::MFA_LAST_FACTOR_REQUIRED,
                Error::is_mfa_last_factor_required,
            ),
        ];
        for (proto_code, want, predicate) in cases {
            let mapped = map_auth(proto_code, "mfa control flow");
            assert_eq!(mapped.auth_error_code(), Some(want));
            assert!(predicate(&mapped));
            for (_, other_code, other_predicate) in cases {
                if other_code == want {
                    continue;
                }
                assert!(!other_predicate(&mapped));
            }
        }
    }

    #[test]
    fn mfa_predicates_ignore_message_text() {
        assert!(!Error::Auth("must enroll mfa".into()).is_mfa_enrollment_required());
        assert!(
            !Error::Api {
                message: "step-up required".into(),
                code: "permission_denied".into(),
                metadata: Vec::new(),
            }
            .is_step_up_required()
        );
        assert!(
            !Error::Api {
                message: "api key mfa".into(),
                code: "AUTH_API_KEY_MFA_REQUIRED".into(),
                metadata: Vec::new(),
            }
            .is_mfa_enrollment_required()
        );
    }
}
