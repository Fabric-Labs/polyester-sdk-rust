//! SDK error types (parity with Go/Python).

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use buffa::{Enumeration, Message};
use connectrpc::{ConnectError, ErrorCode, ErrorDetail};
use std::time::Duration;
use thiserror::Error;

use crate::models::{OrderFieldViolation, RateLimitDetail};
use crate::proto::auth::v1::{AuthErrorCode, AuthErrorDetail};
use crate::proto::chain::withdraw::v1::{
    ErrorCode as WithdrawErrorCode, ErrorDetail as WithdrawErrorDetail,
};
use crate::proto::ledger::read::v1::{
    ErrorCode as LedgerErrorCode, ErrorDetail as LedgerErrorDetail,
};
use crate::proto::orders::v1::{ErrorCode as OrderErrorCode, ErrorDetail as OrderErrorDetail};
use crate::proto::polyester::ratelimit::v1::RateLimitDetail as ProtoRateLimitDetail;
use crate::proto::transfer::v1::{
    ErrorCode as TransferErrorCode, ErrorDetail as TransferErrorDetail,
};
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
        problem: Option<Box<crate::problem::HttpProblem>>,
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
        /// Structured service code, or None without a service detail.
        /// An orders detail without a code uses `ERROR_CODE_UNSPECIFIED`.
        /// Known order codes use the `ERROR_CODE_` prefix.
        code: Option<String>,
        message: String,
        retry_after: Option<f64>,
        /// Structured `polyester.ratelimit.v1.RateLimitDetail` when attached:
        /// ConnectRPC details, an in-band order rejection, or the
        /// `rate_limit` object of a REST `RATE_LIMIT_EXCEEDED` problem, whose
        /// fields are all optional. The gateway's own refusal names only
        /// `reason` and `scope` (`CLIENT_IP`, no `operation_id`); a service's
        /// also names the operation, the policy class and the remaining quota.
        /// `retry_after` prefers its `retry_after_ms`, then the `Retry-After`
        /// header, and is `None` when the server gave no hint, as for a
        /// refusal of too many concurrent requests: back off on your own
        /// schedule then.
        /// Boxed so `Error` stays small enough for Clippy's `result_large_err`.
        detail: Option<Box<RateLimitDetail>>,
    },
    #[error("{0}")]
    Server(String),
    /// A structured orders-service refusal: the request was not admitted.
    #[error("{message}")]
    AdmissionUnavailable {
        message: String,
        code: String,
        problem: Option<Box<crate::problem::HttpProblem>>,
    },
    /// A REST gateway timeout without a recognized mutation outcome code.
    /// The write may have applied; reconcile before retrying. Safe reads may retry.
    #[error("{message}")]
    GatewayTimeout {
        message: String,
        problem: Option<Box<crate::problem::HttpProblem>>,
    },
    #[error("{message}")]
    Api {
        message: String,
        code: String,
        metadata: Vec<(String, String)>,
        /// The request fields to fix, from `orders.v1.ErrorDetail.violations`,
        /// with ConnectRPC field paths such as `order.base_qty_scaled` or
        /// `order.attached_risk.trailing_stop.limit_price_ticks`. Empty when
        /// the refusal names no field. Branch on `code`, then on `rule_id`.
        violations: Vec<OrderFieldViolation>,
    },
    #[error(
        "RPC not exposed on this API host{procedure}. The procedure may be unimplemented on \
         devnet or disabled in this environment."
    )]
    RouteNotFound { procedure: String },
    /// The server does not implement this call: Connect `unimplemented` or a
    /// bare HTTP 501. A bare HTTP 404 is [`Error::RouteNotFound`] instead.
    /// Not retryable.
    #[error("not implemented: {message}")]
    NotImplemented { message: String },
    /// The API-key request timestamp is outside the server's allowed skew
    /// window (`TIMESTAMP_SKEW`), usually because the local clock is off.
    ///
    /// Retryable: the API gateway rejected the request before any service
    /// saw it, and a retry signs it again with a fresh timestamp. If it
    /// persists, fix the local clock. For large bursts, bound concurrency so
    /// requests do not wait locally past the skew window.
    #[error(
        "API-key request timestamp is outside the allowed skew window (HTTP {status}); check the local clock"
    )]
    TimestampSkew {
        status: u16,
        problem: Option<Box<crate::problem::HttpProblem>>,
    },
    /// An API-key request's credentials were refused, for example
    /// `SIGNATURE_INVALID`, `KEY_DISABLED` or `IP_NOT_ALLOWED`: by the API
    /// gateway before any service saw it, or by the service with an
    /// `auth.v1.AuthErrorDetail` (`AUTH_SIGNATURE_INVALID`, ...). `code` is
    /// the reason without the `AUTH_` prefix either way. Not retryable: fix
    /// the key, signature or allowlist. [`Error::auth_error_code`] returns
    /// `code`, [`Error::auth_code`] the typed reason.
    #[error("API-key request rejected (HTTP {status}, code {code})")]
    ApiKeyRejected {
        status: u16,
        code: String,
        problem: Option<Box<crate::problem::HttpProblem>>,
    },
    /// The method accepts only an interactive session token, and the request
    /// carried an account API key (or a session token that carries one):
    /// `AUTH_SESSION_KIND_NOT_ALLOWED`, HTTP 403. Every session-only method
    /// answers it the same way, for example a wallet trading withdraw, the
    /// auto-withdraw rule writes and the portfolio equity reads. Not
    /// retryable: call the method with a session token.
    #[error("{message}")]
    SessionRequired {
        message: String,
        problem: Option<Box<crate::problem::HttpProblem>>,
    },
    #[error("{0}")]
    Realtime(String),
    /// Realtime subscription queue was full; the subscription fails instead of
    /// silently dropping updates.
    #[error("{0}")]
    QueueOverflow(String),
    /// Realtime setup did not complete within its deadline: the server kept the
    /// connection alive, or stayed silent, without confirming the subscription.
    #[error("realtime setup of {channel:?} did not complete within {deadline:?}")]
    RealtimeSetupTimeout { channel: String, deadline: Duration },
    /// The server ended a realtime subscription and the SDK stopped. Queued
    /// messages stay readable; `reason` says why it did not resume.
    #[error(
        "server ended realtime subscription to {channel:?} (code {code}: {server_reason}): {reason}{}",
        .cause.as_ref().map(|cause| format!(": {cause}")).unwrap_or_default()
    )]
    SubscriptionEnded {
        channel: String,
        /// The server's unsubscribe code.
        code: u32,
        /// The server's unsubscribe text.
        server_reason: String,
        reason: SubscriptionEndReason,
        /// The refusal or last failure behind `reason`, if any.
        cause: Option<Box<Error>>,
    },
}

/// Why a subscription the server ended did not resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionEndReason {
    /// No sub-account's access governs the channel, or automatic reconnection
    /// is off. Subscribe again if the channel should still be live.
    NotResubscribable,
    /// A sub-account update on this client said `removed` true.
    AccessRemoved,
    /// `ListSubaccounts` no longer lists the sub-account with a role.
    NotListed,
    /// `ListSubaccounts` refused this API key; `cause` holds the refusal.
    ListRefused,
    /// A fresh subscribe token was refused; `cause` holds the refusal.
    TokenRefused,
    /// Bounded attempts to subscribe again did not hold.
    RetriesExhausted,
}

impl std::fmt::Display for SubscriptionEndReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotResubscribable => "not_resubscribable",
            Self::AccessRemoved => "access_removed",
            Self::NotListed => "not_listed",
            Self::ListRefused => "list_refused",
            Self::TokenRefused => "token_refused",
            Self::RetriesExhausted => "retries_exhausted",
        })
    }
}

/// Stable auth.v1.AuthErrorDetail codes used for MFA control flow.
/// Prefer these over ConnectError message text.
pub mod auth_codes {
    pub const MFA_NOT_ENROLLED: &str = "AUTH_MFA_NOT_ENROLLED";
    pub const STEP_UP_REQUIRED: &str = "AUTH_STEP_UP_REQUIRED";
    pub const MFA_ELEVATION_REQUIRED: &str = "AUTH_MFA_ELEVATION_REQUIRED";
    pub const MFA_LAST_FACTOR_REQUIRED: &str = "AUTH_MFA_LAST_FACTOR_REQUIRED";
    pub const INTERNAL_ERROR: &str = "AUTH_INTERNAL_ERROR";
    /// The account request changed nothing; send the same request again.
    pub const TEMPORARILY_UNAVAILABLE: &str = "AUTH_TEMPORARILY_UNAVAILABLE";
    /// The member call targeted the sub-account's owner: an owner cannot be
    /// invited to its own sub-account, and its role can be neither changed
    /// nor removed. The request changed nothing; see
    /// [`super::Error::is_subaccount_owner_role_locked`].
    pub const SUBACCOUNT_OWNER_ROLE_LOCKED: &str = "AUTH_SUBACCOUNT_OWNER_ROLE_LOCKED";
    /// API-gateway rejection of an API-key request whose timestamp is outside
    /// the allowed skew window; see [`super::Error::TimestampSkew`]. A
    /// service answers the same reason as `AUTH_TIMESTAMP_SKEW`, which maps
    /// to the same error.
    pub const TIMESTAMP_SKEW: &str = "TIMESTAMP_SKEW";
    /// A request with no credentials, or with a missing, malformed or expired
    /// session token. REST answers it as `UNAUTHENTICATED`.
    pub const AUTHENTICATION_REQUIRED: &str = "AUTH_AUTHENTICATION_REQUIRED";
    /// A session-only method called with an account API key; see
    /// [`super::Error::SessionRequired`].
    pub const SESSION_KIND_NOT_ALLOWED: &str = "AUTH_SESSION_KIND_NOT_ALLOWED";
    /// The API key signature does not verify, or the key is unknown.
    pub const SIGNATURE_INVALID: &str = "AUTH_SIGNATURE_INVALID";
    /// The timestamp or signature header is missing.
    pub const MISSING_HEADERS: &str = "AUTH_MISSING_HEADERS";
    /// The API key has expired.
    pub const KEY_EXPIRED: &str = "AUTH_KEY_EXPIRED";
    /// The API key is disabled or revoked.
    pub const KEY_DISABLED: &str = "AUTH_KEY_DISABLED";
    /// The key's IP allowlist does not include the request's source address.
    pub const IP_NOT_ALLOWED: &str = "AUTH_IP_NOT_ALLOWED";
    /// The API does not accept API keys.
    pub const API_KEY_NOT_ACCEPTED: &str = "AUTH_API_KEY_NOT_ACCEPTED";
    /// A valid signed wallet login named an account address no account uses,
    /// and the signing wallet is already linked to another account
    /// (ConnectRPC `already_exists`, REST 409). Wallets are not moved between
    /// accounts and nothing changed. Terminal: the same request is refused
    /// again. Log in with the account address the wallet already uses; see
    /// [`super::Error::is_wallet_already_linked`].
    pub const WALLET_ALREADY_LINKED: &str = "AUTH_WALLET_ALREADY_LINKED";

    /// The `auth.v1.AuthErrorCode` of a REST problem `code` or an API-gateway
    /// rejection code: the code with the `AUTH_` prefix added
    /// (`SIGNATURE_INVALID` reads `AUTH_SIGNATURE_INVALID`), a code that
    /// already has it unchanged, and `UNAUTHENTICATED` as
    /// `AUTH_AUTHENTICATION_REQUIRED`. `None` for a code that is no auth
    /// reason this SDK version knows.
    pub fn from_rest_code(code: &str) -> Option<crate::proto::auth::v1::AuthErrorCode> {
        use crate::proto::auth::v1::AuthErrorCode;
        use buffa::Enumeration;

        let code = code.trim();
        if code == "UNAUTHENTICATED" {
            return Some(AuthErrorCode::AUTH_AUTHENTICATION_REQUIRED);
        }
        AuthErrorCode::from_proto_name(code)
            .or_else(|| AuthErrorCode::from_proto_name(&format!("AUTH_{code}")))
            .filter(|value| value.to_i32() != 0)
    }
}

/// Stable REST problem codes that every service answers alike.
pub mod problem_codes {
    /// A request body over the endpoint's size limit (HTTP 413). The API
    /// refuses it before reading or authenticating the request, so nothing
    /// changed, and the same body is refused again: reduce it to fit the
    /// limit. It is an [`super::Error::Api`] with this code and the problem
    /// in [`super::Error::http_problem`]; see
    /// [`super::Error::is_request_too_large`].
    pub const REQUEST_TOO_LARGE: &str = "REQUEST_TOO_LARGE";
}

/// Stable orders.v1.ErrorDetail codes used for retry control flow.
pub mod order_codes {
    use std::sync::LazyLock;

    use buffa::Enumeration;

    use crate::proto::orders::v1::ErrorCode;

    pub const ORDER_CANCEL_OUTCOME_UNKNOWN: &str = "ERROR_CODE_ORDER_CANCEL_OUTCOME_UNKNOWN";
    pub const ORDER_MODIFY_OUTCOME_UNKNOWN: &str = "ERROR_CODE_ORDER_MODIFY_OUTCOME_UNKNOWN";
    pub const ORDER_CREATE_OUTCOME_UNKNOWN: &str = "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN";
    pub const TRIGGER_CREATE_OUTCOME_UNKNOWN: &str = "ERROR_CODE_TRIGGER_CREATE_OUTCOME_UNKNOWN";
    pub const TRIGGER_CONTROL_OUTCOME_UNKNOWN: &str = "ERROR_CODE_TRIGGER_CONTROL_OUTCOME_UNKNOWN";
    /// The account's new orders, changes and trigger orders are paused for a
    /// short time while its balances are brought back in line. Nothing
    /// changed; send the same request again shortly.
    pub const ACCOUNT_REPAIRING: &str = "ERROR_CODE_ACCOUNT_REPAIRING";
    /// The order has already ended; see
    /// [`super::Error::is_order_already_terminal`].
    pub const ORDER_ALREADY_TERMINAL: &str = "ERROR_CODE_ORDER_ALREADY_TERMINAL";
    /// The order is unknown, or a cancel named another market than the
    /// order's. A cancel by client order ID more than a minute after the
    /// order ended can answer this once the ID no longer names the order.
    pub const ORDER_UNKNOWN: &str = "ERROR_CODE_ORDER_UNKNOWN";

    /// A mutation that was sent but not confirmed in time; it may still
    /// apply. The server returns these as Connect `deadline_exceeded` with
    /// an orders error detail. Read the order or trigger back before
    /// retrying, and reuse the original idempotency key.
    ///
    /// Every generated `orders.v1.ErrorCode` whose name ends in
    /// `_OUTCOME_UNKNOWN`, so a code added to the API is covered when the
    /// SDK is regenerated.
    pub static OUTCOME_UNKNOWN: LazyLock<Box<[&'static str]>> = LazyLock::new(|| {
        ErrorCode::values()
            .iter()
            .map(Enumeration::proto_name)
            .filter(|name| name.ends_with("_OUTCOME_UNKNOWN"))
            .collect()
    });

    /// An orders error code given without its `ERROR_CODE_` prefix, as
    /// `GetBatchReplaceStatus` items carry it, as the full code an
    /// [`super::Error::Api`] carries. A code this SDK version does not know
    /// is kept unchanged. `None` for an empty code.
    pub fn from_status_code(code: &str) -> Option<String> {
        if code.is_empty() {
            return None;
        }
        let prefixed = format!("ERROR_CODE_{code}");
        let known = |name: &str| {
            ErrorCode::values()
                .iter()
                .any(|value| value.proto_name() == name)
        };
        Some(if known(code) {
            code.to_owned()
        } else if known(&prefixed) {
            prefixed
        } else {
            code.to_owned()
        })
    }
}

/// API-key credential refusals a service answers with an
/// `auth.v1.AuthErrorDetail`. They map to [`Error::ApiKeyRejected`], as the
/// API gateway's answers for the same reasons do.
const API_KEY_REJECTIONS: [AuthErrorCode; 6] = [
    AuthErrorCode::AUTH_SIGNATURE_INVALID,
    AuthErrorCode::AUTH_MISSING_HEADERS,
    AuthErrorCode::AUTH_KEY_EXPIRED,
    AuthErrorCode::AUTH_KEY_DISABLED,
    AuthErrorCode::AUTH_IP_NOT_ALLOWED,
    AuthErrorCode::AUTH_API_KEY_NOT_ACCEPTED,
];

/// [`Error::Api`] metadata key holding [`Error::existing_order_id`].
const EXISTING_ORDER_ID_METADATA: &str = "existing_order_id";

/// Error for an API-gateway rejection of an API-key request: `code` comes
/// from the gateway's `{"code": ...}` JSON body.
pub(crate) fn gateway_rejection_error(status: u16, code: &str) -> Error {
    if code == auth_codes::TIMESTAMP_SKEW {
        Error::TimestampSkew {
            status,
            problem: None,
        }
    } else {
        Error::ApiKeyRejected {
            status,
            code: code.to_owned(),
            problem: None,
        }
    }
}

fn is_outcome_unknown_code(code: &str) -> bool {
    code.ends_with("_OUTCOME_UNKNOWN")
}

/// Whether `code` is an enum's zero value: "no code was provided", never a code
/// of its own.
pub(crate) fn is_placeholder_code(code: &str) -> bool {
    matches!(
        code,
        "UNSPECIFIED" | "ERROR_CODE_UNSPECIFIED" | "AUTH_UNSPECIFIED"
    )
}

/// Whether a service detail says nothing: no code and no backoff. Field
/// violations alone name what to fix, not an outcome.
fn is_placeholder_detail(detail: &ErrorDetail) -> bool {
    if let Some(auth) = decode_auth_error_detail(detail) {
        return auth.code.to_i32() == 0;
    }
    if let Some(rate_limit) = decode_rate_limit_detail(detail) {
        return rate_limit.encode_to_bytes().is_empty();
    }
    if let Some(order) = decode_order_error_detail(detail) {
        return order.code.to_i32() == 0
            && order
                .rate_limit
                .as_option()
                .is_none_or(|rate_limit| rate_limit.encode_to_bytes().is_empty());
    }
    if let Some(withdraw) = decode_withdraw_error_detail(detail) {
        return withdraw.code.to_i32() == 0;
    }
    if let Some(ledger) = decode_ledger_error_detail(detail) {
        return ledger.code.to_i32() == 0;
    }
    if let Some(transfer) = decode_transfer_error_detail(detail) {
        return transfer.code.to_i32() == 0;
    }
    false
}

/// Whether a service detail has a code this SDK's enum can't name. Its type shows
/// that a service sent it, not what the code means: it could be a future refusal
/// or a future unknown outcome.
pub(crate) fn has_future_code(detail: &ErrorDetail) -> bool {
    future_code_label(detail).is_some()
}

/// The label a code from [`has_future_code`] is reported under.
fn future_code_label(detail: &ErrorDetail) -> Option<String> {
    let raw = if let Some(auth) = decode_auth_error_detail(detail) {
        auth.code.as_known().is_none().then(|| auth.code.to_i32())
    } else if let Some(order) = decode_order_error_detail(detail) {
        order.code.as_known().is_none().then(|| order.code.to_i32())
    } else if let Some(withdraw) = decode_withdraw_error_detail(detail) {
        withdraw
            .code
            .as_known()
            .is_none()
            .then(|| withdraw.code.to_i32())
    } else if let Some(ledger) = decode_ledger_error_detail(detail) {
        ledger
            .code
            .as_known()
            .is_none()
            .then(|| ledger.code.to_i32())
    } else if let Some(transfer) = decode_transfer_error_detail(detail) {
        transfer
            .code
            .as_known()
            .is_none()
            .then(|| transfer.code.to_i32())
    } else {
        None
    };
    raw.map(|raw| known_or_unknown_code(None, raw))
}

/// Whether a detail is a service detail with a code this SDK can name. One that
/// is empty or undecodable proves nothing.
pub(crate) fn carries_contract_code(detail: &ErrorDetail) -> bool {
    let decodes = decode_auth_error_detail(detail).is_some()
        || decode_rate_limit_detail(detail).is_some()
        || decode_order_error_detail(detail).is_some()
        || decode_withdraw_error_detail(detail).is_some()
        || decode_ledger_error_detail(detail).is_some()
        || decode_transfer_error_detail(detail).is_some();
    decodes && !is_placeholder_detail(detail) && !has_future_code(detail)
}

/// Contract REST codes that no generated error enum names.
const REST_ONLY_CODES: [&str; 21] = [
    "API_KEY_ACTION_DENY",
    "API_KEY_POLICY_NOT_FOUND",
    "INVALID_BEFORE_TS_NS",
    "INVALID_DEPTH",
    "INVALID_END_TIME",
    "INVALID_LIMIT",
    "INVALID_OFFSET",
    "INVALID_ORDER_STATUS",
    "INVALID_PAGE_TOKEN",
    "INVALID_START_TIME",
    "INVALID_SYMBOL",
    "INVALID_TRIGGER_ID",
    "INVALID_TRIGGER_STATUS",
    "ORDER_ID_REQUIRED",
    "REQUEST_TOO_LARGE",
    "ROLE_ACTION_DENY",
    "ROLE_SPOT_TRADE_DENY",
    "SERVER_ERROR",
    "SUBACCOUNT_POLICY_DENY",
    "TRAILING_STOP_ORDER_TYPE_UNSUPPORTED",
    "TRIGGER_ID_REQUIRED",
];

/// Whether the contract defines this REST problem `code`. A mutation's unknown
/// outcome is recognised by its suffix, including one added to the contract
/// after this SDK was generated.
pub(crate) fn is_recognised_rest_code(code: &str) -> bool {
    if code.is_empty() || is_placeholder_code(code) {
        return false;
    }
    if is_outcome_unknown_code(code) || REST_ONLY_CODES.contains(&code) {
        return true;
    }
    let prefixed = format!("ERROR_CODE_{code}");
    let auth = format!("AUTH_{code}");
    [code, prefixed.as_str()].iter().any(|name| {
        OrderErrorCode::from_proto_name(name).is_some()
            || WithdrawErrorCode::from_proto_name(name).is_some()
            || TransferErrorCode::from_proto_name(name).is_some()
            || LedgerErrorCode::from_proto_name(name).is_some()
    }) || [code, auth.as_str()]
        .iter()
        .any(|name| AuthErrorCode::from_proto_name(name).is_some())
}

impl Error {
    /// Original REST metadata, when a REST problem supplied this error.
    /// For `Error::Api`, this decodes the synthetic JSON `http_problem` metadata entry.
    pub fn http_problem(&self) -> Option<crate::problem::HttpProblem> {
        match self {
            Self::AdmissionUnavailable { problem, .. }
            | Self::GatewayTimeout { problem, .. }
            | Self::ApiKeyRejected { problem, .. }
            | Self::PermissionDenied { problem, .. }
            | Self::TimestampSkew { problem, .. }
            | Self::SessionRequired { problem, .. } => problem.as_deref().cloned(),
            Self::Api { metadata, .. } => metadata
                .iter()
                .find(|(key, _)| key == "http_problem")
                .and_then(|(_, value)| serde_json::from_str(value).ok()),
            _ => None,
        }
    }

    pub(crate) fn with_http_problem(mut self, value: crate::problem::HttpProblem) -> Self {
        match &mut self {
            Self::AdmissionUnavailable { problem, .. }
            | Self::GatewayTimeout { problem, .. }
            | Self::ApiKeyRejected { problem, .. }
            | Self::PermissionDenied { problem, .. }
            | Self::TimestampSkew { problem, .. }
            | Self::SessionRequired { problem, .. } => *problem = Some(Box::new(value)),
            Self::Api { metadata, .. } => {
                if let Ok(encoded) = serde_json::to_string(&value) {
                    metadata.push(("http_problem".into(), encoded));
                }
            }
            _ => {}
        }
        self
    }

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
    /// Unknown mutation outcomes always return false, including bare server
    /// and transport failures. Read back before sending a mutation again;
    /// safe reads may retry explicitly. A client order ID enables lookup,
    /// not exact replay. Explicit admission refusals, timestamp skew and
    /// account repairing may retry.
    pub fn is_retryable(&self) -> bool {
        if self.mutation_outcome_unknown() {
            return false;
        }
        match self {
            Self::RateLimit { .. }
            | Self::AdmissionUnavailable { .. }
            | Self::TimestampSkew { .. } => true,
            Self::Api { code, .. } => {
                code == auth_codes::TEMPORARILY_UNAVAILABLE
                    || code == order_codes::ACCOUNT_REPAIRING
            }
            _ => false,
        }
    }

    /// Whether this error can occur after the server accepted a mutation.
    ///
    /// True for transport, server and response-contract errors, for any HTTP
    /// error with no recognised code except a 401 or 403, for a bare 404, and
    /// for an order create, cancel or modify or a trigger create or change that
    /// was sent but not confirmed in time ([`order_codes::OUTCOME_UNKNOWN`]): it
    /// may still apply. Those outcome-unknown codes are not retryable.
    ///
    /// Callers must treat these failures as ambiguous: read the order or
    /// trigger back first and reuse the original idempotency key if a retry
    /// is necessary.
    pub fn mutation_outcome_unknown(&self) -> bool {
        match self {
            Self::Transport(_)
            | Self::ResponseContract { .. }
            | Self::Server(_)
            | Self::GatewayTimeout { .. }
            | Self::RouteNotFound { .. } => true,
            Self::Api { code, .. } => is_outcome_unknown_code(code),
            _ => false,
        }
    }

    /// Server-requested retry delay in seconds, when supplied.
    pub fn retry_after(&self) -> Option<f64> {
        match self {
            Self::RateLimit { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Structured error code: the auth.v1.AuthErrorDetail (or other service)
    /// code of an [`Error::Api`], the code of an [`Error::ApiKeyRejected`],
    /// `TIMESTAMP_SKEW` for [`Error::TimestampSkew`] and
    /// `AUTH_SESSION_KIND_NOT_ALLOWED` for [`Error::SessionRequired`].
    pub fn auth_error_code(&self) -> Option<&str> {
        match self {
            Self::Api { code, .. } | Self::ApiKeyRejected { code, .. } => Some(code.as_str()),
            Self::TimestampSkew { .. } => Some(auth_codes::TIMESTAMP_SKEW),
            Self::SessionRequired { .. } => Some(auth_codes::SESSION_KIND_NOT_ALLOWED),
            _ => None,
        }
    }

    /// Why the request's credentials or session were refused, as an
    /// `auth.v1.AuthErrorCode`, whichever transport answered: ConnectRPC's
    /// `auth.v1.AuthErrorDetail`, or a REST problem or API-gateway `code`
    /// read as [`auth_codes::from_rest_code`] does. For example
    /// `AUTH_TIMESTAMP_SKEW` (retryable, see [`Self::TimestampSkew`]),
    /// `AUTH_SIGNATURE_INVALID`, `AUTH_IP_NOT_ALLOWED` or
    /// `AUTH_SESSION_KIND_NOT_ALLOWED`.
    ///
    /// `None` for an error that carries no auth reason, and for a reason
    /// this SDK version does not know: [`Self::auth_error_code`] still
    /// returns its raw code.
    pub fn auth_code(&self) -> Option<AuthErrorCode> {
        match self {
            Self::Api { code, .. } => AuthErrorCode::from_proto_name(code),
            Self::ApiKeyRejected { code, .. } => auth_codes::from_rest_code(code),
            Self::TimestampSkew { .. } => Some(AuthErrorCode::AUTH_TIMESTAMP_SKEW),
            Self::SessionRequired { .. } => Some(AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED),
            _ => None,
        }
    }

    /// True for [`Self::SessionRequired`]: a session-only method was called
    /// with an account API key.
    pub fn is_session_required(&self) -> bool {
        matches!(self, Self::SessionRequired { .. })
    }

    /// The request fields an [`Error::Api`] names to fix; empty for every
    /// other error.
    pub fn violations(&self) -> &[OrderFieldViolation] {
        match self {
            Self::Api { violations, .. } => violations,
            _ => &[],
        }
    }

    /// True when an order cancel (or change) was refused because the order
    /// has already ended (`ERROR_CODE_ORDER_ALREADY_TERMINAL`): filled,
    /// expired or canceled, by an earlier cancel or otherwise. A repeated
    /// cancel of an order it ended answers this too. Treat it as "already
    /// done" and read the order for how it ended.
    pub fn is_order_already_terminal(&self) -> bool {
        self.auth_error_code() == Some(order_codes::ORDER_ALREADY_TERMINAL)
    }

    /// True when a request body over the endpoint's size limit was refused
    /// ([`problem_codes::REQUEST_TOO_LARGE`], HTTP 413). Nothing changed;
    /// reduce the body to fit the limit.
    pub fn is_request_too_large(&self) -> bool {
        matches!(self, Self::Api { code, .. } if code == problem_codes::REQUEST_TOO_LARGE)
    }

    /// True when a wallet login was refused because the wallet is already
    /// linked to another account ([`auth_codes::WALLET_ALREADY_LINKED`]).
    /// Terminal: log in with the account address the wallet already uses.
    pub fn is_wallet_already_linked(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::WALLET_ALREADY_LINKED)
    }

    /// True for [`Self::NotImplemented`] and its narrower case
    /// [`Self::RouteNotFound`]: the server does not offer this call.
    pub fn is_not_implemented(&self) -> bool {
        matches!(
            self,
            Self::NotImplemented { .. } | Self::RouteNotFound { .. }
        )
    }

    /// Public ID of the order that already holds the client order ID, when a
    /// create is rejected with `ERROR_CODE_CONFLICT_DUPLICATE_CLIENT_ORDER_ID`
    /// and the server knows the holder. Read it with `orders.get` to learn
    /// its current state.
    pub fn existing_order_id(&self) -> Option<&str> {
        match self {
            Self::Api { metadata, .. } => metadata
                .iter()
                .find(|(key, _)| key == EXISTING_ORDER_ID_METADATA)
                .map(|(_, value)| value.as_str()),
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

    /// True when a sub-account member call targeted the sub-account's owner
    /// ([`auth_codes::SUBACCOUNT_OWNER_ROLE_LOCKED`]): inviting the owner,
    /// accepting an invite for the owner, changing the owner's role or
    /// removing the owner. The owner's role can't be changed; the request
    /// changed nothing and is not retryable.
    ///
    /// ConnectRPC answers it as `failed_precondition` with an
    /// `auth.v1.AuthErrorDetail`, REST as HTTP 412 with the same code. Both
    /// map to [`Error::Api`] with this code.
    pub fn is_subaccount_owner_role_locked(&self) -> bool {
        self.auth_error_code() == Some(auth_codes::SUBACCOUNT_OWNER_ROLE_LOCKED)
    }
}

fn decode_detail_bytes(detail: &ErrorDetail) -> Option<Vec<u8>> {
    let value = detail.value.as_ref()?;
    STANDARD_NO_PAD
        .decode(value)
        .or_else(|_| STANDARD.decode(value))
        .ok()
}

fn decode_auth_error_detail(detail: &ErrorDetail) -> Option<AuthErrorDetail> {
    if !detail.type_url.ends_with("auth.v1.AuthErrorDetail") {
        return None;
    }
    AuthErrorDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn decode_order_error_detail(detail: &ErrorDetail) -> Option<OrderErrorDetail> {
    if !detail.type_url.ends_with("orders.v1.ErrorDetail") {
        return None;
    }
    OrderErrorDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn decode_withdraw_error_detail(detail: &ErrorDetail) -> Option<WithdrawErrorDetail> {
    if !detail.type_url.ends_with("chain.withdraw.v1.ErrorDetail") {
        return None;
    }
    WithdrawErrorDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn decode_transfer_error_detail(detail: &ErrorDetail) -> Option<TransferErrorDetail> {
    if !detail.type_url.ends_with("transfer.v1.ErrorDetail") {
        return None;
    }
    TransferErrorDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn decode_ledger_error_detail(detail: &ErrorDetail) -> Option<LedgerErrorDetail> {
    if !detail.type_url.ends_with("ledger.read.v1.ErrorDetail") {
        return None;
    }
    LedgerErrorDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn is_auth_like_error_code(code: &str) -> bool {
    code.contains("UNAUTHENTICATED")
        || code.contains("PERMISSION")
        || code.contains("API_KEY")
        || code.contains("WALLET_BINDING")
        || code.contains("POLICY")
}

fn decode_rate_limit_detail(detail: &ErrorDetail) -> Option<ProtoRateLimitDetail> {
    if !detail
        .type_url
        .ends_with("polyester.ratelimit.v1.RateLimitDetail")
    {
        return None;
    }
    ProtoRateLimitDetail::decode_from_slice(&decode_detail_bytes(detail)?).ok()
}

fn enum_label<T: Enumeration>(value: &buffa::EnumValue<T>, unknown_prefix: &str) -> String {
    match value.as_known() {
        Some(known) => known.proto_name().to_owned(),
        None => format!("{unknown_prefix}({})", value.to_i32()),
    }
}

fn rate_limit_detail_from_proto_local(msg: &ProtoRateLimitDetail) -> RateLimitDetail {
    // Kept local to avoid a codecs::decode ↔ errors import cycle.
    RateLimitDetail {
        reason: enum_label(&msg.reason, "UNKNOWN_FAILURE_REASON"),
        limit: msg.limit,
        remaining: msg.remaining,
        retry_after_ms: msg.retry_after_ms,
        policy_version: msg.policy_version,
        operation_id: msg.operation_id.clone(),
        policy_class: enum_label(&msg.policy_class, "UNKNOWN_POLICY_CLASS"),
        scope: enum_label(&msg.scope, "UNKNOWN_LIMITER_SCOPE"),
        refill_model: enum_label(&msg.refill_model, "UNKNOWN_REFILL_MODEL"),
    }
}

fn order_error_code_from_name(code: &str) -> String {
    let prefixed = format!("ERROR_CODE_{code}");
    if OrderErrorCode::from_proto_name(&prefixed).is_some() {
        prefixed
    } else {
        code.to_owned()
    }
}

pub(crate) fn is_service_code_message(code: &str) -> bool {
    code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

fn known_admission_refusal(code: &str) -> bool {
    matches!(
        code.trim_start_matches("ERROR_CODE_"),
        "UNAVAILABLE"
            | "FEE_SCHEDULE_UNAVAILABLE"
            | "POLICY_UNAVAILABLE"
            | "RATE_LIMIT_POLICY_UNAVAILABLE"
            | "PAIR_BACKLOGGED"
            | "SETTLEMENT_BACKLOGGED"
            | "ORDER_MODIFY_IN_PROGRESS"
            | "ORDER_PLACEMENT_PENDING"
            | "TRIGGER_CONTROLS_BUSY"
            | "TRIGGER_CONTROL_IN_PROGRESS"
            | "QUICKSWAP_ACCOUNT_OWNER_UNKNOWN"
    )
}

fn final_resource_code(code: &str) -> bool {
    matches!(
        code.trim_start_matches("ERROR_CODE_"),
        "ORDER_SEQUENCE_EXHAUSTED"
            | "TRIGGER_FIRE_LIMIT_REACHED"
            | "QUICKSWAP_CAPACITY_REACHED"
            | "ACCOUNT_RESERVATION_LIMIT_REACHED"
            | "TRIGGER_LIMIT_EXCEEDED"
    )
}

pub(crate) fn retryable_resource_code(code: &str) -> bool {
    matches!(
        code.trim_start_matches("ERROR_CODE_"),
        "OVERLOADED"
            | "SYSTEM_RATE_LIMITED"
            | "RATE_LIMIT_EXCEEDED"
            | "PRICE_LEVELS_EXHAUSTED"
            | "CLIENT_ORDER_IDS_EXHAUSTED"
            | "CLIENT_ORDER_ID_CAPACITY_REACHED"
            | "MARKET_LADDER_LIMIT_EXCEEDED"
            | "MARKET_TRIGGER_LIMIT_EXCEEDED"
            | "MARKET_TWAP_LIMIT_EXCEEDED"
            | "NEW_ACCOUNT_CAPACITY_REACHED"
            | "REQUEST_RECEIPTS_EXHAUSTED"
            | "RESERVATION_CAPACITY_REACHED"
            | "TRIGGER_CAPACITY_REACHED"
            | "TRIGGER_CONTROL_SHARE_EXHAUSTED"
            | "TWAP_CAPACITY_REACHED"
    )
}

fn rate_limit_error(
    message: String,
    detail: Option<RateLimitDetail>,
    header_retry: Option<f64>,
    code: Option<String>,
) -> Error {
    let retry_after = detail
        .as_ref()
        .and_then(RateLimitDetail::retry_after_seconds)
        .or(header_retry);
    Error::RateLimit {
        code,
        message,
        retry_after,
        detail: detail.map(Box::new),
    }
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
    let problem = err
        .details
        .iter()
        .find(|d| d.type_url == crate::transport::HTTP_PROBLEM_TYPE)
        .and_then(|d| d.debug.as_ref())
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let mapped = map_connect_error_inner(err);
    if let Some(problem) = problem {
        mapped.with_http_problem(problem)
    } else {
        mapped
    }
}

/// The `rate_limit` object of a REST rate-limit problem. Every field is
/// optional: the gateway's own refusal sends only `reason` and `scope`.
/// Integers arrive as JSON strings or numbers; enum names are kept as sent,
/// including ones newer than this SDK. Anything that is not an object reads as
/// no detail, and a field of the wrong type reads as absent.
fn rate_limit_detail_from_json(value: &serde_json::Value) -> Option<RateLimitDetail> {
    let fields = value.as_object()?;
    let field = |snake: &str, camel: &str| fields.get(snake).or_else(|| fields.get(camel));
    let text = |snake: &str, camel: &str| {
        field(snake, camel)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let integer = |snake: &str, camel: &str| match field(snake, camel)? {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    };
    Some(RateLimitDetail {
        reason: text("reason", "reason"),
        limit: integer("limit", "limit"),
        remaining: integer("remaining", "remaining"),
        retry_after_ms: integer("retry_after_ms", "retryAfterMs"),
        policy_version: integer("policy_version", "policyVersion"),
        operation_id: text("operation_id", "operationId"),
        policy_class: text("policy_class", "policyClass"),
        scope: text("scope", "scope"),
        refill_model: text("refill_model", "refillModel"),
    })
}

fn map_connect_error_inner(err: ConnectError) -> Error {
    let problem_rate_limit = err
        .details
        .iter()
        .find(|d| d.type_url == crate::transport::RATE_LIMIT_PROBLEM_TYPE)
        .and_then(|d| d.debug.as_ref())
        .and_then(rate_limit_detail_from_json);
    let fallback_message = {
        let message = err.to_string();
        if message.trim().is_empty() {
            "request failed without server error details".to_owned()
        } else {
            message
        }
    };
    let header_retry = retry_after_seconds(&err);
    let status = err.http_status().as_u16();
    let raw_message = err.message.as_deref().unwrap_or_default();
    let has_marker = |marker: &str| err.details.iter().any(|detail| detail.type_url == marker);
    if has_marker(crate::transport::GATEWAY_REJECTION_TYPE) {
        return gateway_rejection_error(status, raw_message);
    }
    if has_marker(crate::transport::BARE_HTTP_ERROR_TYPE) {
        // A bare 404 means the route is missing; a contract code at 501 that
        // the server does not implement the call.
        return if status == 404 {
            Error::RouteNotFound {
                procedure: String::new(),
            }
        } else if status == 504 {
            Error::GatewayTimeout {
                message: fallback_message,
                problem: None,
            }
        } else {
            Error::NotImplemented {
                message: fallback_message,
            }
        };
    }
    if raw_message == auth_codes::TIMESTAMP_SKEW {
        return Error::TimestampSkew {
            status,
            problem: None,
        };
    }
    let mut details: Vec<_> = err.details.iter().collect();
    details.sort_by_key(|detail| {
        decode_order_error_detail(detail).map_or(2, |order| {
            let code = known_or_unknown_code(
                order.code.as_known().map(|c| c.proto_name()),
                order.code.to_i32(),
            );
            if is_outcome_unknown_code(&code) {
                0
            } else if final_resource_code(&code) {
                1
            } else {
                2
            }
        })
    });
    if is_outcome_unknown_code(raw_message)
        && !details.first().is_some_and(|detail| {
            decode_order_error_detail(detail).is_some_and(|order| {
                is_outcome_unknown_code(&known_or_unknown_code(
                    order.code.as_known().map(|c| c.proto_name()),
                    order.code.to_i32(),
                ))
            })
        })
    {
        return Error::Api {
            message: fallback_message,
            code: order_error_code_from_name(raw_message),
            metadata: Vec::new(),
            violations: Vec::new(),
        };
    }
    // A detail with no code says nothing about the outcome, but its field
    // violations still name what to fix on a recognised Connect code.
    let mut placeholder_violations = Vec::new();
    let mut future_code = None;
    let server_failure = matches!(
        err.code,
        ErrorCode::Internal
            | ErrorCode::Unknown
            | ErrorCode::DataLoss
            | ErrorCode::Canceled
            | ErrorCode::Unavailable
            | ErrorCode::DeadlineExceeded
    );
    for detail in details {
        if is_placeholder_detail(detail) {
            if let Some(order) = decode_order_error_detail(detail) {
                placeholder_violations.extend(order.violations.iter().map(|v| {
                    OrderFieldViolation {
                        field_path: v.field_path.clone(),
                        rule_id: v.rule_id.clone(),
                        message: v.message.clone(),
                    }
                }));
            }
            continue;
        }
        // A code this SDK can't name stands only behind a recognised refusal.
        if server_failure && has_future_code(detail) {
            future_code = future_code.or_else(|| future_code_label(detail));
            continue;
        }
        if let Some(auth_detail) = decode_auth_error_detail(detail) {
            match auth_detail.code.as_known() {
                Some(AuthErrorCode::AUTH_TIMESTAMP_SKEW) => {
                    return Error::TimestampSkew {
                        status,
                        problem: None,
                    };
                }
                Some(AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED) => {
                    return Error::SessionRequired {
                        problem: None,
                        message: if auth_detail.message.is_empty() {
                            fallback_message
                        } else {
                            auth_detail.message
                        },
                    };
                }
                Some(known) if API_KEY_REJECTIONS.contains(&known) => {
                    return Error::ApiKeyRejected {
                        status,
                        code: known.proto_name().trim_start_matches("AUTH_").to_owned(),
                        problem: None,
                    };
                }
                _ => {}
            }
            let code = known_or_unknown_code(
                auth_detail.code.as_known().map(|c| c.proto_name()),
                auth_detail.code.to_i32(),
            );
            let message = if auth_detail.message.is_empty() {
                fallback_message.clone()
            } else {
                auth_detail.message
            };
            return Error::Api {
                message,
                code,
                metadata: Vec::new(),
                violations: Vec::new(),
            };
        }
        if let Some(rl) = decode_rate_limit_detail(detail) {
            return rate_limit_error(
                fallback_message.clone(),
                Some(rate_limit_detail_from_proto_local(&rl)),
                header_retry,
                None,
            );
        }
        if let Some(order_detail) = decode_order_error_detail(detail) {
            let code = known_or_unknown_code(
                order_detail.code.as_known().map(|c| c.proto_name()),
                order_detail.code.to_i32(),
            );
            let is_rate_limit = order_detail.rate_limit.is_set() || retryable_resource_code(&code);
            if is_rate_limit && !is_outcome_unknown_code(&code) && !final_resource_code(&code) {
                let detail = order_detail
                    .rate_limit
                    .as_option()
                    .map(rate_limit_detail_from_proto_local)
                    .or(problem_rate_limit);
                return rate_limit_error(
                    fallback_message.clone(),
                    detail,
                    header_retry,
                    Some(code),
                );
            }
            // Backoff hints and unknown mutation outcomes take precedence.
            if err.code == ErrorCode::Unavailable
                && !code.ends_with("_OUTCOME_UNKNOWN")
                && code != "ERROR_CODE_ACCOUNT_ROUTING_INVALID"
                && !final_resource_code(&code)
            {
                return if known_admission_refusal(&code) {
                    Error::AdmissionUnavailable {
                        message: fallback_message.clone(),
                        code,
                        problem: None,
                    }
                } else {
                    Error::Server(fallback_message.clone())
                };
            }
            if is_auth_like_error_code(&code) {
                return Error::Auth(fallback_message.clone());
            }
            let metadata = order_detail
                .existing_order_id
                .filter(|id| *id != 0)
                .map(|id| {
                    vec![(
                        EXISTING_ORDER_ID_METADATA.to_owned(),
                        crate::codecs::scalars::format_uint64_id(id),
                    )]
                })
                .unwrap_or_default();
            let violations = order_detail
                .violations
                .iter()
                .map(|v| OrderFieldViolation {
                    field_path: v.field_path.clone(),
                    rule_id: v.rule_id.clone(),
                    message: v.message.clone(),
                })
                .collect();
            return Error::Api {
                message: fallback_message.clone(),
                code,
                metadata,
                violations,
            };
        }
        if let Some(withdraw_detail) = decode_withdraw_error_detail(detail) {
            let code = known_or_unknown_code(
                withdraw_detail.code.as_known().map(|c| c.proto_name()),
                withdraw_detail.code.to_i32(),
            );
            if matches!(
                withdraw_detail.code.as_known(),
                Some(WithdrawErrorCode::RateLimitExceeded)
            ) {
                return rate_limit_error(fallback_message.clone(), None, header_retry, Some(code));
            }
            if err.code == ErrorCode::Unavailable {
                return Error::Server(fallback_message.clone());
            }
            if is_auth_like_error_code(&code) {
                return Error::Auth(fallback_message.clone());
            }
            return Error::Api {
                message: fallback_message.clone(),
                code,
                metadata: Vec::new(),
                violations: Vec::new(),
            };
        }
        if let Some(ledger_detail) = decode_ledger_error_detail(detail) {
            let code = known_or_unknown_code(
                ledger_detail.code.as_known().map(|c| c.proto_name()),
                ledger_detail.code.to_i32(),
            );
            if err.code == ErrorCode::Unavailable {
                return Error::Server(fallback_message.clone());
            }
            if is_auth_like_error_code(&code) {
                return Error::Auth(fallback_message.clone());
            }
            return Error::Api {
                message: fallback_message.clone(),
                code,
                metadata: Vec::new(),
                violations: Vec::new(),
            };
        }
        if let Some(transfer_detail) = decode_transfer_error_detail(detail) {
            let code = known_or_unknown_code(
                transfer_detail.code.as_known().map(|c| c.proto_name()),
                transfer_detail.code.to_i32(),
            );
            if matches!(
                transfer_detail.code.as_known(),
                Some(TransferErrorCode::RateLimitExceeded)
            ) {
                return rate_limit_error(fallback_message.clone(), None, header_retry, Some(code));
            }
            if err.code == ErrorCode::Unavailable {
                return Error::Server(fallback_message.clone());
            }
            if is_auth_like_error_code(&code) {
                return Error::Auth(fallback_message.clone());
            }
            return Error::Api {
                message: fallback_message.clone(),
                code,
                metadata: Vec::new(),
                violations: Vec::new(),
            };
        }
    }
    let named_code = err
        .details
        .iter()
        .find(|d| d.type_url == crate::transport::HTTP_PROBLEM_TYPE)
        .and_then(|d| d.debug.as_ref())
        .and_then(|d| d.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(raw_message);
    let named_code = if is_placeholder_code(named_code) {
        ""
    } else {
        named_code
    };
    let normalized_code = order_error_code_from_name(named_code);
    if err.code == ErrorCode::Unavailable
        && status == 503
        && normalized_code != "ERROR_CODE_UNAVAILABLE"
        && known_admission_refusal(&normalized_code)
    {
        return Error::AdmissionUnavailable {
            message: fallback_message,
            code: normalized_code,
            problem: None,
        };
    }
    if (err.code == ErrorCode::Unavailable
        && status == 503
        && normalized_code == "ERROR_CODE_ACCOUNT_ROUTING_INVALID")
        || is_outcome_unknown_code(named_code)
        || (err.code == ErrorCode::ResourceExhausted
            && (OrderErrorCode::from_proto_name(&normalized_code).is_some()
                || is_service_code_message(named_code))
            && !retryable_resource_code(&normalized_code))
    {
        return Error::Api {
            message: fallback_message,
            code: order_error_code_from_name(named_code),
            metadata: Vec::new(),
            violations: Vec::new(),
        };
    }
    let code = err.code;
    let message = fallback_message;
    if is_cloudflare_browser_ban(&message) {
        return Error::Transport(cloudflare_1010_message());
    }
    match code {
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => Error::Auth(message),
        ErrorCode::ResourceExhausted => {
            let code = order_error_code_from_name(named_code);
            let code = OrderErrorCode::from_proto_name(&code).map(|_| code);
            rate_limit_error(message, None, header_retry, code)
        }
        // The contract never uses unknown, data_loss or canceled, and none of
        // them says a request was refused.
        ErrorCode::Unavailable
        | ErrorCode::Internal
        | ErrorCode::Unknown
        | ErrorCode::DataLoss
        | ErrorCode::Canceled => Error::Server(match future_code {
            // Kept for diagnosis only; this variant has no code field.
            Some(label) => format!("{message} ({label})"),
            None => message,
        }),
        ErrorCode::DeadlineExceeded => Error::Transport(message),
        ErrorCode::Unimplemented => Error::NotImplemented { message },
        _ => Error::Api {
            message,
            code: if has_marker(crate::transport::HTTP_PROBLEM_TYPE) {
                order_error_code_from_name(named_code)
            } else {
                format!("{code:?}")
            },
            metadata: Vec::new(),
            violations: placeholder_violations,
        },
    }
}

/// Proto name of a known error code, or `UNKNOWN_ERROR_CODE(<n>)` for a code
/// this SDK version does not know yet, so the server's code is never lost.
pub(crate) fn known_or_unknown_code(known: Option<&str>, raw: i32) -> String {
    known.map_or_else(|| format!("UNKNOWN_ERROR_CODE({raw})"), str::to_owned)
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
    fn map_connect_error_types_the_subaccount_owner_role_lock() {
        let detail = AuthErrorDetail {
            code: EnumValue::Known(AuthErrorCode::AUTH_SUBACCOUNT_OWNER_ROLE_LOCKED),
            ..Default::default()
        };
        let err = map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "owner role locked").with_detail(
                ErrorDetail::from_message("auth.v1.AuthErrorDetail", &detail),
            ),
        );
        assert!(
            matches!(&err, Error::Api { code, .. } if code == auth_codes::SUBACCOUNT_OWNER_ROLE_LOCKED),
            "{err:?}"
        );
        assert!(err.is_subaccount_owner_role_locked());
        assert!(!err.is_retryable());
        assert!(!err.mutation_outcome_unknown());

        let other = map_auth(AuthErrorCode::AUTH_INVALID_REQUEST, "bad");
        assert!(!other.is_subaccount_owner_role_locked());
    }

    #[test]
    fn batch_replace_status_codes_map_to_order_error_codes() {
        let map = order_codes::from_status_code;
        assert_eq!(
            map("POST_ONLY_CROSS").as_deref(),
            Some("ERROR_CODE_POST_ONLY_CROSS")
        );
        assert_eq!(
            map("PRICE_LEVELS_EXHAUSTED").as_deref(),
            Some("ERROR_CODE_PRICE_LEVELS_EXHAUSTED")
        );
        // Already prefixed, or unknown to this SDK version: kept as given.
        assert_eq!(
            map("ERROR_CODE_POST_ONLY_CROSS").as_deref(),
            Some("ERROR_CODE_POST_ONLY_CROSS")
        );
        assert_eq!(
            map("SOME_FUTURE_REFUSAL").as_deref(),
            Some("SOME_FUTURE_REFUSAL")
        );
        assert_eq!(map(""), None);
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
                detail,
                code,
            } => {
                assert_eq!(message, "resource_exhausted: request rate exceeded");
                assert_eq!(retry_after, Some(2.5));
                assert!(detail.is_none());
                assert!(code.is_none());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_surfaces_nested_rate_limit_detail() {
        use crate::proto::orders::v1::{
            ErrorCode as OrderErrorCode, ErrorDetail as OrderErrorDetail,
        };
        use crate::proto::polyester::ratelimit::v1::{
            FailureReason, LimiterScope, PolicyClass, RateLimitDetail as ProtoRateLimitDetail,
            RefillModel,
        };

        let mut headers = http::HeaderMap::new();
        headers.insert("retry-after", http::HeaderValue::from_static("9"));
        let detail_msg = OrderErrorDetail {
            code: OrderErrorCode::RateLimitExceeded.into(),
            rate_limit: ProtoRateLimitDetail {
                reason: FailureReason::QUOTA_EXCEEDED.into(),
                limit: Some(100),
                remaining: Some(0),
                retry_after_ms: Some(2500),
                policy_version: Some(3),
                operation_id: "orders.create".into(),
                policy_class: PolicyClass::TRADING_PLACE.into(),
                scope: LimiterScope::API_KEY.into(),
                refill_model: RefillModel::CONTINUOUS.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let mapped = map_connect_error(
            ConnectError::new(ErrorCode::ResourceExhausted, "slow down")
                .with_headers(headers)
                .with_detail(ErrorDetail::from_message(
                    "orders.v1.ErrorDetail",
                    &detail_msg,
                )),
        );
        match mapped {
            Error::RateLimit {
                retry_after,
                detail,
                ..
            } => {
                assert_eq!(retry_after, Some(2.5));
                let detail = detail.expect("detail");
                assert_eq!(detail.reason, "QUOTA_EXCEEDED");
                assert_eq!(detail.limit, Some(100));
                assert_eq!(detail.remaining, Some(0));
                assert_eq!(detail.retry_after_ms, Some(2500));
                assert_eq!(detail.policy_version, Some(3));
                assert_eq!(detail.operation_id, "orders.create");
                assert_eq!(detail.policy_class, "TRADING_PLACE");
                assert_eq!(detail.scope, "API_KEY");
                assert_eq!(detail.refill_model, "CONTINUOUS");
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
        assert!(!timeout.is_retryable());
        assert!(timeout.mutation_outcome_unknown());

        let limited = Error::RateLimit {
            code: None,
            message: "slow down".into(),
            retry_after: Some(1.0),
            detail: None,
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
    fn order_unavailable_classification() {
        for name in [
            "ERROR_CODE_RATE_LIMIT_POLICY_UNAVAILABLE",
            "ERROR_CODE_POLICY_UNAVAILABLE",
            "ERROR_CODE_FEE_SCHEDULE_UNAVAILABLE",
            "ERROR_CODE_UNAVAILABLE",
            "ERROR_CODE_PAIR_BACKLOGGED",
            "ERROR_CODE_ORDER_MODIFY_IN_PROGRESS",
            "ERROR_CODE_ORDER_PLACEMENT_PENDING",
            "ERROR_CODE_SETTLEMENT_BACKLOGGED",
            "ERROR_CODE_TRIGGER_CONTROLS_BUSY",
            "ERROR_CODE_TRIGGER_CONTROL_IN_PROGRESS",
            "ERROR_CODE_ACCOUNT_ROUTING_INVALID",
            "ERROR_CODE_UPSTREAM_ERROR",
        ] {
            let code = OrderErrorCode::from_proto_name(name).unwrap();
            let mapped = map_order(code, ErrorCode::Unavailable);
            assert!(
                matches!(
                    mapped,
                    Error::Server(_) | Error::AdmissionUnavailable { .. } | Error::Api { .. }
                ),
                "{code:?}: {mapped:?}"
            );
            assert!(!matches!(mapped, Error::Auth(_)));
            assert_eq!(
                mapped.is_retryable(),
                known_admission_refusal(name),
                "{code:?}: {mapped:?}"
            );
            assert_eq!(
                mapped.mutation_outcome_unknown(),
                !known_admission_refusal(name) && name != "ERROR_CODE_ACCOUNT_ROUTING_INVALID"
            );
        }
    }

    #[test]
    fn withdraw_unavailable_classification() {
        for name in [
            "ERROR_CODE_SERVICE_UNAVAILABLE",
            "ERROR_CODE_ACCOUNT_SHARD_UNAVAILABLE",
            "ERROR_CODE_CAPITAL_VIEW_UNAVAILABLE",
            "ERROR_CODE_CHAIN_METADATA_UNAVAILABLE",
            "ERROR_CODE_DESTINATION_VALIDATION_UNAVAILABLE",
            "ERROR_CODE_FEE_UNAVAILABLE",
            "ERROR_CODE_STEP_UP_UNAVAILABLE",
            "ERROR_CODE_SUPPLY_UNAVAILABLE",
        ] {
            let detail = WithdrawErrorDetail {
                code: WithdrawErrorCode::from_proto_name(name).unwrap().into(),
                ..Default::default()
            };
            let mapped = map_connect_error(
                ConnectError::new(ErrorCode::Unavailable, "retry later").with_detail(
                    ErrorDetail::from_message("chain.withdraw.v1.ErrorDetail", &detail),
                ),
            );
            assert!(matches!(mapped, Error::Server(_)), "{name}: {mapped:?}");
            assert!(!matches!(mapped, Error::Auth(_)));
            assert!(!mapped.is_retryable(), "{name}: {mapped:?}");
            assert!(mapped.mutation_outcome_unknown());
        }
    }

    #[test]
    fn transfer_unavailable_classification() {
        for name in [
            "ERROR_CODE_SERVICE_UNAVAILABLE",
            "ERROR_CODE_ACCOUNT_SHARD_UNAVAILABLE",
            "ERROR_CODE_CAPITAL_VIEW_UNAVAILABLE",
            "ERROR_CODE_STEP_UP_UNAVAILABLE",
        ] {
            let detail = TransferErrorDetail {
                code: TransferErrorCode::from_proto_name(name).unwrap().into(),
                ..Default::default()
            };
            let mapped = map_connect_error(
                ConnectError::new(ErrorCode::Unavailable, "retry later").with_detail(
                    ErrorDetail::from_message("transfer.v1.ErrorDetail", &detail),
                ),
            );
            assert!(matches!(mapped, Error::Server(_)), "{name}: {mapped:?}");
            assert!(!matches!(mapped, Error::Auth(_)));
            assert!(!mapped.is_retryable(), "{name}: {mapped:?}");
            assert!(mapped.mutation_outcome_unknown());
        }
    }

    #[test]
    fn fund_movement_unavailable_preconditions_are_not_retryable() {
        let withdraw = WithdrawErrorDetail {
            code: WithdrawErrorCode::from_proto_name("ERROR_CODE_SOURCE_SMART_ACCOUNT_UNAVAILABLE")
                .unwrap()
                .into(),
            ..Default::default()
        };
        let transfer = TransferErrorDetail {
            code: TransferErrorCode::from_proto_name("ERROR_CODE_SMART_ACCOUNT_UNAVAILABLE")
                .unwrap()
                .into(),
            ..Default::default()
        };
        for detail in [
            ErrorDetail::from_message("chain.withdraw.v1.ErrorDetail", &withdraw),
            ErrorDetail::from_message("transfer.v1.ErrorDetail", &transfer),
        ] {
            let mapped = map_connect_error(
                ConnectError::new(ErrorCode::FailedPrecondition, "not ready").with_detail(detail),
            );
            assert!(matches!(mapped, Error::Api { .. }), "{mapped:?}");
            assert!(!mapped.is_retryable(), "{mapped:?}");
        }
    }

    #[test]
    fn order_policy_denial_classification() {
        for name in [
            "ERROR_CODE_POLICY_MARKET_DENY",
            "ERROR_CODE_POLICY_TRADING_HALTED",
            "ERROR_CODE_POLICY_SPOT_TRADE_DENY",
            "ERROR_CODE_POLICY_SPOT_READ_DENY",
        ] {
            let code = OrderErrorCode::from_proto_name(name).unwrap();
            let mapped = map_order(code, ErrorCode::PermissionDenied);
            assert!(matches!(mapped, Error::Auth(_)), "{name}: {mapped:?}");
            assert!(!mapped.is_retryable(), "{name}: {mapped:?}");
        }
    }

    #[test]
    fn order_unavailable_precondition_classification() {
        for name in [
            "ERROR_CODE_MARKET_PRICE_UNAVAILABLE",
            "ERROR_CODE_ORDER_LIMITS_UNAVAILABLE",
        ] {
            let code = OrderErrorCode::from_proto_name(name).unwrap();
            let mapped = map_order(code, ErrorCode::FailedPrecondition);
            assert!(matches!(mapped, Error::Api { .. }), "{name}: {mapped:?}");
            assert!(!mapped.is_retryable(), "{name}: {mapped:?}");
        }
    }

    fn map_order(code: OrderErrorCode, connect_code: ErrorCode) -> Error {
        let detail = OrderErrorDetail {
            code: EnumValue::Known(code),
            ..Default::default()
        };
        map_connect_error(
            ConnectError::new(connect_code, "order error")
                .with_detail(ErrorDetail::from_message("orders.v1.ErrorDetail", &detail)),
        )
    }

    #[test]
    fn attached_leg_and_trigger_refusals_follow_retry_guidance() {
        for (proto_code, connect_code) in [
            (
                OrderErrorCode::ERROR_CODE_ATTACHED_LEGS_EXCEEDED,
                ErrorCode::FailedPrecondition,
            ),
            (
                OrderErrorCode::ERROR_CODE_FEE_ASSET_INVALID,
                ErrorCode::InvalidArgument,
            ),
            (
                OrderErrorCode::ERROR_CODE_FEE_CEILING_INVALID,
                ErrorCode::InvalidArgument,
            ),
            (
                OrderErrorCode::ERROR_CODE_UNSUPPORTED_FIELD_SET,
                ErrorCode::InvalidArgument,
            ),
            (
                OrderErrorCode::ERROR_CODE_TRIGGER_FIRE_LIMIT_REACHED,
                ErrorCode::FailedPrecondition,
            ),
            (
                OrderErrorCode::ERROR_CODE_CONDITION_FLAGS_INVALID,
                ErrorCode::InvalidArgument,
            ),
            (
                OrderErrorCode::ERROR_CODE_CONDITION_PARAMETER_SET,
                ErrorCode::InvalidArgument,
            ),
            // TWAP capacity explicitly permits retrying later.
            (
                OrderErrorCode::ERROR_CODE_TWAP_CAPACITY_REACHED,
                ErrorCode::ResourceExhausted,
            ),
            (
                OrderErrorCode::ERROR_CODE_CHILD_ORDER_INCONSISTENT,
                ErrorCode::FailedPrecondition,
            ),
        ] {
            let want = proto_code.proto_name();
            let mapped = map_order(proto_code, connect_code);
            if proto_code == OrderErrorCode::ERROR_CODE_TWAP_CAPACITY_REACHED {
                assert!(matches!(&mapped,Error::RateLimit{code:Some(code),..} if code==want));
                assert!(mapped.is_retryable());
                assert!(!mapped.mutation_outcome_unknown());
                continue;
            }
            match &mapped {
                Error::Api { code, .. } => assert_eq!(code, want),
                other => panic!("{want}: unexpected error {other:?}"),
            }
            assert!(!mapped.is_retryable(), "{want} must not be retryable");
            assert!(
                !mapped.mutation_outcome_unknown(),
                "{want} has a known outcome"
            );
        }
    }

    #[test]
    fn outcome_unknown_set_is_every_generated_outcome_unknown_order_code() {
        let generated: Vec<&str> = OrderErrorCode::values()
            .iter()
            .map(|code| code.proto_name())
            .filter(|name| name.ends_with("_OUTCOME_UNKNOWN"))
            .collect();
        for want in [
            "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN",
            "ERROR_CODE_ORDER_CANCEL_OUTCOME_UNKNOWN",
            "ERROR_CODE_ORDER_MODIFY_OUTCOME_UNKNOWN",
            "ERROR_CODE_TRIGGER_CREATE_OUTCOME_UNKNOWN",
            "ERROR_CODE_TRIGGER_CONTROL_OUTCOME_UNKNOWN",
        ] {
            assert!(generated.contains(&want), "{want} missing from orders.v1");
        }
        let mut listed: Vec<&str> = order_codes::OUTCOME_UNKNOWN.to_vec();
        let mut expected = generated.clone();
        listed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(listed, expected);
    }

    #[test]
    fn outcome_unknown_order_codes_require_reconciliation_and_are_not_retryable() {
        let codes: Vec<OrderErrorCode> = OrderErrorCode::values()
            .iter()
            .copied()
            .filter(|code| code.proto_name().ends_with("_OUTCOME_UNKNOWN"))
            .collect();
        assert!(codes.contains(&OrderErrorCode::ERROR_CODE_TRIGGER_CONTROL_OUTCOME_UNKNOWN));
        let mut wrong = Vec::new();
        for proto_code in codes {
            let want = proto_code.proto_name();
            let mapped = map_order(proto_code, ErrorCode::DeadlineExceeded);
            match &mapped {
                Error::Api { code, .. } => assert_eq!(code, want),
                other => panic!("unexpected error: {other:?}"),
            }
            if !mapped.mutation_outcome_unknown() {
                wrong.push(format!("{want}: mutation_outcome_unknown() is false"));
            }
            // A blind retry of a create could place a second order.
            if mapped.is_retryable() {
                wrong.push(format!("{want}: is_retryable() is true"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");

        let definite = OrderErrorDetail {
            code: EnumValue::Known(OrderErrorCode::ERROR_CODE_TRIGGER_WOULD_TRIGGER_IMMEDIATELY),
            ..Default::default()
        };
        let mapped = map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "would trigger now").with_detail(
                ErrorDetail::from_message("orders.v1.ErrorDetail", &definite),
            ),
        );
        match &mapped {
            Error::Api { code, .. } => {
                assert_eq!(code, "ERROR_CODE_TRIGGER_WOULD_TRIGGER_IMMEDIATELY")
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(!mapped.mutation_outcome_unknown());
        assert!(!mapped.is_retryable());
    }

    #[test]
    fn duplicate_client_order_id_rejection_exposes_existing_order_id() {
        let detail = OrderErrorDetail {
            code: EnumValue::Known(OrderErrorCode::ERROR_CODE_CONFLICT_DUPLICATE_CLIENT_ORDER_ID),
            existing_order_id: Some(77),
            ..Default::default()
        };
        let err = map_connect_error(
            ConnectError::new(ErrorCode::AlreadyExists, "duplicate client order id").with_detail(
                ErrorDetail::from_message("type.googleapis.com/orders.v1.ErrorDetail", &detail),
            ),
        );
        assert_eq!(
            err.existing_order_id(),
            Some(crate::codecs::scalars::format_uint64_id(77).as_str())
        );
        assert_eq!(
            err.auth_error_code(),
            Some("ERROR_CODE_CONFLICT_DUPLICATE_CLIENT_ORDER_ID")
        );
        assert!(!err.is_retryable());

        let without_holder = map_connect_error(
            ConnectError::new(ErrorCode::AlreadyExists, "duplicate client order id").with_detail(
                ErrorDetail::from_message(
                    "type.googleapis.com/orders.v1.ErrorDetail",
                    &OrderErrorDetail {
                        code: EnumValue::Known(
                            OrderErrorCode::ERROR_CODE_CONFLICT_DUPLICATE_CLIENT_ORDER_ID,
                        ),
                        ..Default::default()
                    },
                ),
            ),
        );
        assert_eq!(without_holder.existing_order_id(), None);
    }

    #[test]
    fn timestamp_skew_message_is_a_retryable_skew_error() {
        let err = map_connect_error(ConnectError::new(
            ErrorCode::Unauthenticated,
            auth_codes::TIMESTAMP_SKEW,
        ));
        assert!(
            matches!(err, Error::TimestampSkew { status: 401, .. }),
            "{err:?}"
        );
        assert!(err.is_retryable() && !err.mutation_outcome_unknown());
    }

    fn map_auth_with(connect_code: ErrorCode, code: AuthErrorCode, message: &str) -> Error {
        let detail = AuthErrorDetail {
            code: EnumValue::Known(code),
            message: message.into(),
            ..Default::default()
        };
        map_connect_error(ConnectError::new(connect_code, message).with_detail(
            ErrorDetail::from_message("auth.v1.AuthErrorDetail", &detail),
        ))
    }

    #[test]
    fn connect_timestamp_skew_detail_is_the_retryable_skew_error() {
        let err = map_auth_with(
            ErrorCode::Unauthenticated,
            AuthErrorCode::AUTH_TIMESTAMP_SKEW,
            "API key timestamp is outside the allowed window",
        );
        assert!(
            matches!(err, Error::TimestampSkew { status: 401, .. }),
            "{err:?}"
        );
        assert!(err.is_retryable() && !err.mutation_outcome_unknown());
        assert_eq!(err.auth_code(), Some(AuthErrorCode::AUTH_TIMESTAMP_SKEW));
        assert_eq!(err.auth_error_code(), Some(auth_codes::TIMESTAMP_SKEW));
    }

    #[test]
    fn connect_api_key_refusals_map_like_the_gateway_answers() {
        for (proto_code, rest_code) in [
            (AuthErrorCode::AUTH_SIGNATURE_INVALID, "SIGNATURE_INVALID"),
            (AuthErrorCode::AUTH_MISSING_HEADERS, "MISSING_HEADERS"),
            (AuthErrorCode::AUTH_KEY_EXPIRED, "KEY_EXPIRED"),
            (AuthErrorCode::AUTH_KEY_DISABLED, "KEY_DISABLED"),
            (AuthErrorCode::AUTH_IP_NOT_ALLOWED, "IP_NOT_ALLOWED"),
            (
                AuthErrorCode::AUTH_API_KEY_NOT_ACCEPTED,
                "API_KEY_NOT_ACCEPTED",
            ),
        ] {
            let err = map_auth_with(ErrorCode::Unauthenticated, proto_code, "refused");
            assert!(
                matches!(&err, Error::ApiKeyRejected { status: 401, code, .. } if code == rest_code),
                "{err:?}"
            );
            assert_eq!(err.auth_code(), Some(proto_code));
            assert!(!err.is_retryable(), "{rest_code} is not retryable");

            // The gateway's REST code reads as the same typed reason.
            let gateway = gateway_rejection_error(401, rest_code);
            assert_eq!(gateway.auth_code(), Some(proto_code), "{rest_code}");
            assert!(!gateway.is_retryable());
        }
        let skew = gateway_rejection_error(401, auth_codes::TIMESTAMP_SKEW);
        assert_eq!(skew.auth_code(), Some(AuthErrorCode::AUTH_TIMESTAMP_SKEW));
    }

    #[test]
    fn rest_auth_codes_read_as_auth_error_codes() {
        let read = auth_codes::from_rest_code;
        assert_eq!(
            read("SIGNATURE_INVALID"),
            Some(AuthErrorCode::AUTH_SIGNATURE_INVALID)
        );
        assert_eq!(
            read("AUTH_SESSION_KIND_NOT_ALLOWED"),
            Some(AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED)
        );
        assert_eq!(
            read("UNAUTHENTICATED"),
            Some(AuthErrorCode::AUTH_AUTHENTICATION_REQUIRED)
        );
        assert_eq!(read("UNSPECIFIED"), None);
        assert_eq!(read("SOME_FUTURE_REASON"), None);
        assert_eq!(read(""), None);

        let unauthenticated = gateway_rejection_error(401, "UNAUTHENTICATED");
        assert_eq!(
            unauthenticated.auth_code(),
            Some(AuthErrorCode::AUTH_AUTHENTICATION_REQUIRED)
        );
        // An auth reason newer than this SDK keeps its raw code.
        let detail = AuthErrorDetail {
            code: EnumValue::Unknown(99),
            ..Default::default()
        };
        let unknown = map_connect_error(ConnectError::unauthenticated("new reason").with_detail(
            ErrorDetail::from_message("auth.v1.AuthErrorDetail", &detail),
        ));
        assert_eq!(unknown.auth_code(), None);
        assert_eq!(unknown.auth_error_code(), Some("UNKNOWN_ERROR_CODE(99)"));
        // Errors without an auth reason have none.
        assert_eq!(Error::transport("down").auth_code(), None);
        assert_eq!(
            map_order(
                OrderErrorCode::ERROR_CODE_BAD_PRICE,
                ErrorCode::InvalidArgument
            )
            .auth_code(),
            None
        );
    }

    #[test]
    fn session_kind_not_allowed_is_one_typed_session_required_error() {
        let err = map_auth_with(
            ErrorCode::PermissionDenied,
            AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED,
            "interactive session required",
        );
        match &err {
            Error::SessionRequired { message, .. } => {
                assert_eq!(message, "interactive session required")
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(err.is_session_required());
        assert_eq!(
            err.auth_code(),
            Some(AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED)
        );
        assert_eq!(
            err.auth_error_code(),
            Some(auth_codes::SESSION_KIND_NOT_ALLOWED)
        );
        assert!(!err.is_retryable() && !err.mutation_outcome_unknown());
        assert!(!map_auth(AuthErrorCode::AUTH_INVALID_REQUEST, "bad").is_session_required());
    }

    #[test]
    fn withdraw_policy_refusal_message_reaches_the_caller() {
        let detail = WithdrawErrorDetail {
            code: EnumValue::Known(WithdrawErrorCode::PermissionDenied),
            ..Default::default()
        };
        let err = map_connect_error(
            ConnectError::permission_denied("api key policy does not allow READ_AUTO_WITHDRAW")
                .with_detail(ErrorDetail::from_message(
                    "chain.withdraw.v1.ErrorDetail",
                    &detail,
                )),
        );
        assert!(matches!(err, Error::Auth(_)), "{err:?}");
        assert!(
            err.to_string()
                .contains("api key policy does not allow READ_AUTO_WITHDRAW"),
            "{err}"
        );
    }

    #[test]
    fn order_errors_carry_their_field_violations() {
        use crate::proto::orders::v1::FieldViolation;
        let detail = OrderErrorDetail {
            code: EnumValue::Known(OrderErrorCode::ERROR_CODE_TRIGGER_BUY_REQUIRES_LIMIT),
            violations: vec![FieldViolation {
                field_path: "order.attached_risk.trailing_stop.limit_price_ticks".into(),
                rule_id: "buy_required".into(),
                message: "limit_price_ticks is required when the parent order sells".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = map_connect_error(
            ConnectError::new(ErrorCode::InvalidArgument, "trailing stop maximum required")
                .with_detail(ErrorDetail::from_message("orders.v1.ErrorDetail", &detail)),
        );
        assert_eq!(
            err.auth_error_code(),
            Some("ERROR_CODE_TRIGGER_BUY_REQUIRES_LIMIT")
        );
        assert_eq!(
            err.violations(),
            [OrderFieldViolation {
                field_path: "order.attached_risk.trailing_stop.limit_price_ticks".into(),
                rule_id: "buy_required".into(),
                message: "limit_price_ticks is required when the parent order sells".into(),
            }]
        );
        assert!(Error::transport("down").violations().is_empty());
    }

    #[test]
    fn cancel_of_an_ended_order_is_already_terminal() {
        let ended = map_order(
            OrderErrorCode::ERROR_CODE_ORDER_ALREADY_TERMINAL,
            ErrorCode::FailedPrecondition,
        );
        assert!(ended.is_order_already_terminal());
        assert!(!ended.is_retryable() && !ended.mutation_outcome_unknown());
        let unknown = map_order(
            OrderErrorCode::ERROR_CODE_ORDER_UNKNOWN,
            ErrorCode::NotFound,
        );
        assert!(!unknown.is_order_already_terminal());
        assert_eq!(unknown.auth_error_code(), Some(order_codes::ORDER_UNKNOWN));
    }

    #[test]
    fn unavailable_keeps_nested_rate_limit_delay() {
        use crate::proto::polyester::ratelimit::v1::FailureReason;
        for code in [
            OrderErrorCode::ERROR_CODE_UNAVAILABLE,
            OrderErrorCode::ERROR_CODE_RATE_LIMIT_POLICY_UNAVAILABLE,
            OrderErrorCode::RateLimitExceeded,
        ] {
            let detail = OrderErrorDetail {
                code: code.into(),
                rate_limit: ProtoRateLimitDetail {
                    reason: FailureReason::AUTHORITY_UNAVAILABLE.into(),
                    retry_after_ms: Some(1250),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            };
            let mapped = map_connect_error(
                ConnectError::new(ErrorCode::Unavailable, "back off")
                    .with_detail(ErrorDetail::from_message("orders.v1.ErrorDetail", &detail)),
            );
            assert!(
                matches!(
                    mapped,
                    Error::RateLimit {
                        retry_after: Some(1.25),
                        ..
                    }
                ),
                "{mapped:?}"
            );
            assert!(mapped.is_retryable());
            assert!(!mapped.mutation_outcome_unknown());
        }
    }

    #[test]
    fn unavailable_keeps_rate_limit_classification() {
        let details = [
            ErrorDetail::from_message(
                "orders.v1.ErrorDetail",
                &OrderErrorDetail {
                    code: OrderErrorCode::RateLimitExceeded.into(),
                    ..Default::default()
                },
            ),
            ErrorDetail::from_message(
                "chain.withdraw.v1.ErrorDetail",
                &WithdrawErrorDetail {
                    code: WithdrawErrorCode::RateLimitExceeded.into(),
                    ..Default::default()
                },
            ),
            ErrorDetail::from_message(
                "transfer.v1.ErrorDetail",
                &TransferErrorDetail {
                    code: TransferErrorCode::RateLimitExceeded.into(),
                    ..Default::default()
                },
            ),
        ];
        for detail in details {
            let mut headers = http::HeaderMap::new();
            headers.insert("retry-after", http::HeaderValue::from_static("2"));
            let mapped = map_connect_error(
                ConnectError::new(ErrorCode::Unavailable, "back off")
                    .with_headers(headers)
                    .with_detail(detail),
            );
            assert!(
                matches!(
                    mapped,
                    Error::RateLimit {
                        retry_after: Some(2.0),
                        ..
                    }
                ),
                "{mapped:?}"
            );
            assert!(mapped.is_retryable());
            assert!(!mapped.mutation_outcome_unknown());
        }
    }

    #[test]
    fn unavailable_outcome_unknown_stays_non_retryable() {
        for code in OrderErrorCode::values().iter().copied() {
            if !code.proto_name().ends_with("_OUTCOME_UNKNOWN") {
                continue;
            }
            let mapped = map_order(code, ErrorCode::Unavailable);
            assert!(matches!(mapped, Error::Api { .. }), "{mapped:?}");
            assert_eq!(mapped.auth_error_code(), Some(code.proto_name()));
            assert!(!mapped.is_retryable(), "{mapped:?}");
            assert!(mapped.mutation_outcome_unknown(), "{mapped:?}");
        }
    }

    #[test]
    fn account_repairing_is_retryable_and_changed_nothing() {
        let repairing = map_order(
            OrderErrorCode::ERROR_CODE_ACCOUNT_REPAIRING,
            ErrorCode::FailedPrecondition,
        );
        assert!(matches!(repairing, Error::Api { .. }), "{repairing:?}");
        assert_eq!(
            repairing.auth_error_code(),
            Some(order_codes::ACCOUNT_REPAIRING)
        );
        assert!(repairing.is_retryable(), "{repairing:?}");
        assert!(!repairing.mutation_outcome_unknown());

        // The order itself is being canceled; retrying the change cannot succeed.
        let order_repairing = map_order(
            OrderErrorCode::ERROR_CODE_ORDER_REPAIRING,
            ErrorCode::FailedPrecondition,
        );
        assert!(!order_repairing.is_retryable());
        assert!(!order_repairing.mutation_outcome_unknown());
    }

    #[test]
    fn auth_temporarily_unavailable_is_retryable_and_changed_nothing() {
        let mapped = map_auth(AuthErrorCode::AUTH_TEMPORARILY_UNAVAILABLE, "try again");
        assert_eq!(
            mapped.auth_error_code(),
            Some(auth_codes::TEMPORARILY_UNAVAILABLE)
        );
        assert!(mapped.is_retryable());
        assert!(!mapped.mutation_outcome_unknown());

        let other = map_auth(AuthErrorCode::AUTH_REVISION_CONFLICT, "resource changed");
        assert!(!other.is_retryable());
    }

    #[test]
    fn map_connect_error_surfaces_auth_internal_error() {
        let mapped = map_auth(AuthErrorCode::AUTH_INTERNAL_ERROR, "auth backend failed");
        assert_eq!(mapped.auth_error_code(), Some(auth_codes::INTERNAL_ERROR));
        match mapped {
            Error::Api { message, code, .. } => {
                assert_eq!(code, "AUTH_INTERNAL_ERROR");
                assert_eq!(message, "auth backend failed");
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
    fn map_connect_error_surfaces_withdraw_and_transfer_details() {
        let withdraw = crate::proto::chain::withdraw::v1::ErrorDetail {
            code: EnumValue::Known(WithdrawErrorCode::InsufficientFunds),
            ..Default::default()
        };
        match map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "insufficient").with_detail(
                ErrorDetail::from_message("chain.withdraw.v1.ErrorDetail", &withdraw),
            ),
        ) {
            Error::Api { code, .. } => assert_eq!(code, "ERROR_CODE_INSUFFICIENT_FUNDS"),
            other => panic!("unexpected withdraw error: {other:?}"),
        }

        let transfer = TransferErrorDetail {
            code: EnumValue::Known(TransferErrorCode::PermissionDenied),
            ..Default::default()
        };
        match map_connect_error(ConnectError::permission_denied("denied").with_detail(
            ErrorDetail::from_message("transfer.v1.ErrorDetail", &transfer),
        )) {
            Error::Auth(_) => {}
            other => panic!("unexpected transfer error: {other:?}"),
        }

        let expired = OrderErrorDetail {
            code: EnumValue::Known(OrderErrorCode::CancelRequestExpired),
            ..Default::default()
        };
        match map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "expired")
                .with_detail(ErrorDetail::from_message("orders.v1.ErrorDetail", &expired)),
        ) {
            Error::Api { code, .. } => assert_eq!(code, "ERROR_CODE_CANCEL_REQUEST_EXPIRED"),
            other => panic!("unexpected expired error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_keeps_unknown_error_codes() {
        let order = OrderErrorDetail {
            code: EnumValue::Unknown(999),
            ..Default::default()
        };
        match map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "new rule")
                .with_detail(ErrorDetail::from_message("orders.v1.ErrorDetail", &order)),
        ) {
            Error::Api { code, message, .. } => {
                assert_eq!(code, "UNKNOWN_ERROR_CODE(999)");
                assert!(message.ends_with("new rule"), "{message}");
            }
            other => panic!("unexpected order error: {other:?}"),
        }

        let transfer = TransferErrorDetail {
            code: EnumValue::Unknown(998),
            ..Default::default()
        };
        match map_connect_error(
            ConnectError::new(ErrorCode::FailedPrecondition, "new rule").with_detail(
                ErrorDetail::from_message("transfer.v1.ErrorDetail", &transfer),
            ),
        ) {
            Error::Api { code, .. } => assert_eq!(code, "UNKNOWN_ERROR_CODE(998)"),
            other => panic!("unexpected transfer error: {other:?}"),
        }
    }

    #[test]
    fn map_connect_error_surfaces_auth_terms_not_accepted() {
        match map_auth(AuthErrorCode::AUTH_TERMS_NOT_ACCEPTED, "terms") {
            Error::Api { message, code, .. } => {
                assert_eq!(code, "AUTH_TERMS_NOT_ACCEPTED");
                assert_eq!(message, "terms");
            }
            other => panic!("unexpected error: {other:?}"),
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
                violations: Vec::new(),
            }
            .is_step_up_required()
        );
        assert!(
            !Error::Api {
                message: "api key mfa".into(),
                code: "AUTH_API_KEY_MFA_REQUIRED".into(),
                metadata: Vec::new(),
                violations: Vec::new(),
            }
            .is_mfa_enrollment_required()
        );
    }
}
