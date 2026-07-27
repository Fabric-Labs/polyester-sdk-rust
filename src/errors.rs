//! SDK error types (parity with Go/Python).

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use buffa::{Enumeration, Message};
use connectrpc::{ConnectError, ErrorCode, ErrorDetail};
use thiserror::Error;

use crate::proto::auth::v1::AuthErrorDetail;
use crate::user_agent::{cloudflare_1010_message, is_cloudflare_browser_ban};

/// Root result alias for the SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// Polyester SDK error.
#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("{0}")]
    Auth(String),
    #[error("{context}: permission denied (HTTP {status}, code {code}): {message} [{endpoint}]")]
    PermissionDenied {
        message: String,
        status: u16,
        code: String,
        context: String,
        endpoint: String,
    },
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Transport(String),
    /// A successful RPC returned a payload that violates the documented
    /// response contract.
    ///
    /// This is not retryable: repeating a mutation blindly can duplicate work.
    /// Because the server may already have accepted the mutation, callers must
    /// reconcile when [`Self::mutation_outcome_unknown`] returns true.
    #[error("{context}: response contract violation: {message}")]
    ResponseContract { context: String, message: String },
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

    pub fn response_contract(context: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ResponseContract {
            context: context.into(),
            message: message.into(),
        }
    }

    pub fn realtime(msg: impl Into<String>) -> Self {
        Self::Realtime(msg.into())
    }

    pub fn queue_overflow(msg: impl Into<String>) -> Self {
        Self::QueueOverflow(msg.into())
    }

    /// Whether retrying may succeed after backoff.
    ///
    /// This is a transport-level classification, not a guarantee that a
    /// mutation was not applied. For mutations, preserve the same idempotency
    /// key and reconcile server state before retrying when
    /// [`Self::mutation_outcome_unknown`] is true.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::RateLimit { .. } | Self::Server(_)
        )
    }

    /// Whether this error can occur after the server accepted a mutation.
    ///
    /// Callers must treat these failures as ambiguous: reconcile first and
    /// reuse the original idempotency key if a retry is necessary.
    pub fn mutation_outcome_unknown(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::ResponseContract { .. } | Self::Server(_)
        )
    }

    /// Server-requested retry delay in seconds, when supplied.
    pub fn retry_after(&self) -> Option<f64> {
        match self {
            Self::RateLimit { retry_after, .. } => *retry_after,
            _ => None,
        }
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

fn parse_nonnegative_f64(value: &http::HeaderValue) -> Option<f64> {
    let parsed = value.to_str().ok()?.trim().parse::<f64>().ok()?;
    (parsed.is_finite() && parsed >= 0.0).then_some(parsed)
}

fn retry_after_seconds(err: &ConnectError) -> Option<f64> {
    for headers in [err.response_headers(), err.trailers()] {
        if let Some(seconds) = headers.get("retry-after").and_then(parse_nonnegative_f64) {
            return Some(seconds);
        }
        for name in ["retry-after-ms", "grpc-retry-pushback-ms"] {
            if let Some(milliseconds) = headers.get(name).and_then(parse_nonnegative_f64) {
                return Some(milliseconds / 1_000.0);
            }
        }
    }
    None
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
    let retry_after = retry_after_seconds(&err);
    let message = fallback_message;
    if is_cloudflare_browser_ban(&message) {
        return Error::Transport(cloudflare_1010_message());
    }
    match code {
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => Error::Auth(message),
        ErrorCode::ResourceExhausted => Error::RateLimit {
            message,
            retry_after,
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
        let mut headers = http::HeaderMap::new();
        headers.insert("retry-after", http::HeaderValue::from_static("2.5"));
        let mapped = map_connect_error(
            ConnectError::new(ErrorCode::ResourceExhausted, "request rate exceeded")
                .with_headers(headers),
        );
        match mapped {
            Error::RateLimit {
                message,
                retry_after,
            } => {
                assert_eq!(message, "resource_exhausted: request rate exceeded");
                assert_eq!(retry_after, Some(2.5));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_reads_retry_pushback_milliseconds_from_trailers() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert(
            "grpc-retry-pushback-ms",
            http::HeaderValue::from_static("1250"),
        );
        let mapped = map_connect_error(
            ConnectError::new(ErrorCode::ResourceExhausted, "slow down").with_trailers(trailers),
        );
        assert_eq!(mapped.retry_after(), Some(1.25));
    }

    #[test]
    fn retry_classification_is_conservative_for_mutations() {
        let timeout = Error::transport("deadline exceeded");
        assert!(timeout.is_retryable());
        assert!(timeout.mutation_outcome_unknown());

        let limited = Error::RateLimit {
            message: "slow down".into(),
            retry_after: Some(1.0),
        };
        assert!(limited.is_retryable());
        assert!(!limited.mutation_outcome_unknown());

        let contract =
            Error::response_contract("BatchCreateOrders", "reported counts do not match items");
        assert!(!contract.is_retryable());
        assert!(contract.mutation_outcome_unknown());

        assert!(!Error::validation("bad price").is_retryable());
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
