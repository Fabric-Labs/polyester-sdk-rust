//! Thin helpers around Connect unary calls.

use crate::errors::Result;
use crate::transport::Factory;
use buffa::Message;
use connectrpc::client::{CallOptions, UnaryResponse};
use connectrpc::{ConnectError, ErrorCode};
use serde::Serialize;
use std::future::Future;
use std::time::Duration;

#[inline]
pub async fn await_public<R>(
    call: impl Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
) -> Result<UnaryResponse<R>> {
    call.await.map_err(Factory::map_error)
}

#[inline]
pub async fn await_auth<M, R, Fut>(
    factory: &Factory,
    procedure: &str,
    request: M,
    call: impl FnOnce(M, CallOptions) -> Fut,
) -> Result<UnaryResponse<R>>
where
    M: Message + Serialize,
    Fut: Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
{
    let opts = factory.sign_options_async(procedure, &request).await?;
    call(request, opts).await.map_err(Factory::map_error)
}

/// Backoff for a side-effect-free read the server may answer with a
/// retryable `unavailable` while it catches up with recent activity.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnavailableRetry {
    pub first_delay: Duration,
    pub max_delay: Duration,
    /// No retry starts once the next delay would end past this budget,
    /// counted from the first attempt.
    pub budget: Duration,
}

/// Trigger reads (`GetTrigger`, `ListTriggers`): 25 ms, doubling, capped at
/// 500 ms, giving up after about 2 s.
pub(crate) const TRIGGER_READ_RETRY: UnavailableRetry = UnavailableRetry {
    first_delay: Duration::from_millis(25),
    max_delay: Duration::from_millis(500),
    budget: Duration::from_secs(2),
};

impl UnavailableRetry {
    pub(crate) fn next_delay(&self, delay: Duration) -> Duration {
        delay.saturating_mul(2).min(self.max_delay)
    }
}

/// Sign and send one authenticated call. The outer error is a local signing
/// failure; the inner result is the server's answer, unmapped, so a caller
/// can decide on its Connect code.
pub(crate) async fn try_auth<M, R, Fut>(
    factory: &Factory,
    procedure: &str,
    request: M,
    call: impl FnOnce(M, CallOptions) -> Fut,
) -> Result<std::result::Result<UnaryResponse<R>, ConnectError>>
where
    M: Message + Serialize,
    Fut: Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
{
    let opts = factory.sign_options_async(procedure, &request).await?;
    Ok(call(request, opts).await)
}

/// [`await_auth`] for a read without side effects: Connect `unavailable`
/// (HTTP 503) is retried with the same request, signed again, under
/// `policy`. Other errors, and `unavailable` once the budget is spent,
/// return as usual.
pub(crate) async fn await_auth_retrying_unavailable<M, R, Fut>(
    factory: &Factory,
    procedure: &str,
    request: M,
    policy: UnavailableRetry,
    call: impl Fn(M, CallOptions) -> Fut,
) -> Result<UnaryResponse<R>>
where
    M: Message + Serialize + Clone,
    Fut: Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
{
    let start = tokio::time::Instant::now();
    let mut delay = policy.first_delay;
    loop {
        match try_auth(factory, procedure, request.clone(), &call).await? {
            Ok(resp) => return Ok(resp),
            Err(err)
                if err.code == ErrorCode::Unavailable
                    && start.elapsed() + delay <= policy.budget =>
            {
                tokio::time::sleep(delay).await;
                delay = policy.next_delay(delay);
            }
            Err(err) => return Err(Factory::map_error(err)),
        }
    }
}
