//! SDK error types (parity with Go/Python).

use connectrpc::{ConnectError, ErrorCode};
use thiserror::Error;

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

/// Map a ConnectRPC error into an SDK error.
pub fn map_connect_error(err: ConnectError) -> Error {
    let code = err.code;
    let message = err.to_string();
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
