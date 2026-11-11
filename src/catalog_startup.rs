//! Load a startup catalog: bounded retries of an idempotent read, then
//! validation.
//!
//! The client loads the spot config and the funding (deposit/withdraw)
//! config when it is created, each on its own. A read that fails the way a
//! dropped or stalled connection does (`unavailable`, `deadline_exceeded`) is
//! read again, at most [`STARTUP_ATTEMPTS`] times in all, with backoff, never
//! past the startup deadline, every retry on a connection pool of its own.
//! Anything else, and a response that does not validate, ends the load at
//! once. Only these reads are retried here: they change nothing on the
//! server.

use std::future::Future;
use std::time::Duration;

use connectrpc::client::ClientConfig;
use connectrpc::{ConnectError, ErrorCode};
use tokio::time::Instant;

use crate::catalogs::Manager as CatalogManager;
use crate::codecs::decode::{deposit_withdraw_config_from_proto, spot_config_from_proto};
use crate::connect::chain::zipper::v1::ZipperServiceClient;
use crate::connect::marketdata::v1::MarketDataServiceClient;
use crate::errors::{CatalogLoadFailure, Error, Result};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigRequest;
use crate::proto::marketdata::v1::GetSpotConfigRequest;
use crate::services::unary;
use crate::transport::{Factory, SharedTransport};

pub(crate) const STARTUP_ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

const SPOT_OPERATION: &str = "marketdata.v1.MarketDataService/GetSpotConfig";
const FUNDING_OPERATION: &str = "chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";

/// Load and install the spot config, within `deadline`.
pub(crate) async fn load_spot(
    factory: &Factory,
    catalogs: &CatalogManager,
    deadline: Instant,
) -> Result<()> {
    let response = read_with_retry(
        factory,
        "spot",
        SPOT_OPERATION,
        deadline,
        |transport, config| async move {
            let client = MarketDataServiceClient::new(transport, config);
            unary::await_public_connect(client.get_spot_config(GetSpotConfigRequest::default()))
                .await
                .map(|response| response.into_owned())
        },
    )
    .await?;
    let (response, attempts, elapsed) = response;
    let install = spot_config_from_proto(&response)
        .and_then(|spot| catalogs.hydrate_spot_config_json(spot.raw));
    install.map_err(|source| invalid("spot", SPOT_OPERATION, attempts, elapsed, source))
}

/// Load and install the funding (deposit/withdraw) config, within `deadline`.
pub(crate) async fn load_funding(
    factory: &Factory,
    catalogs: &CatalogManager,
    deadline: Instant,
) -> Result<()> {
    let response = read_with_retry(
        factory,
        "funding",
        FUNDING_OPERATION,
        deadline,
        |transport, config| async move {
            let client = ZipperServiceClient::new(transport, config);
            unary::await_public_connect(
                client.get_deposit_withdraw_config(GetDepositWithdrawConfigRequest::default()),
            )
            .await
            .map(|response| response.into_owned())
        },
    )
    .await?;
    let (response, attempts, elapsed) = response;
    let config = deposit_withdraw_config_from_proto(&response);
    catalogs
        .hydrate_zipper_config(&config)
        .map_err(|source| invalid("funding", FUNDING_OPERATION, attempts, elapsed, source))
}

fn invalid(
    catalog: &'static str,
    operation: &'static str,
    attempts: u32,
    elapsed: Duration,
    source: Error,
) -> Error {
    Error::CatalogLoad(Box::new(CatalogLoadFailure {
        catalog,
        operation,
        failure: "invalid",
        attempts,
        elapsed,
        response_ref: None,
        source,
    }))
}

async fn read_with_retry<R, F, Fut>(
    factory: &Factory,
    catalog: &'static str,
    operation: &'static str,
    deadline: Instant,
    read: F,
) -> Result<(R, u32, Duration)>
where
    F: Fn(SharedTransport, ClientConfig) -> Fut,
    Fut: Future<Output = std::result::Result<R, ConnectError>>,
{
    let started = Instant::now();
    let request_timeout = factory.config.timeout;
    let mut delay = INITIAL_BACKOFF;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = request_timeout.min(remaining).max(Duration::from_millis(1));
        let config = factory.connect_config().with_default_timeout(timeout);
        let transport = if attempt == 1 {
            factory.transport()
        } else {
            factory.fresh_transport()?
        };
        // The client's timeout ends the call; this one only guards a call
        // that would ignore it.
        let outcome = tokio::time::timeout(
            timeout + Duration::from_millis(250),
            read(transport, config),
        )
        .await
        .unwrap_or_else(|_| Err(ConnectError::deadline_exceeded("catalog read timed out")));
        let err = match outcome {
            Ok(response) => return Ok((response, attempt, started.elapsed())),
            Err(err) => err,
        };
        if !retryable(&err) || attempt >= STARTUP_ATTEMPTS || Instant::now() + delay >= deadline {
            return Err(read_failure(
                catalog,
                operation,
                err,
                attempt,
                started.elapsed(),
            ));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_BACKOFF);
    }
}

/// A failure a dropped or stalled connection, or an edge that is briefly
/// unavailable, causes. A rate refusal is not one.
fn retryable(err: &ConnectError) -> bool {
    matches!(
        err.code,
        ErrorCode::Unavailable | ErrorCode::DeadlineExceeded
    ) && !matches!(Factory::map_error(err.clone()), Error::RateLimit { .. })
}

fn failure_kind(err: &ConnectError) -> &'static str {
    let message = err.message.as_deref().unwrap_or_default();
    match err.code {
        ErrorCode::DeadlineExceeded => "timeout",
        // hyper's own words: a connection could not be opened, or one failed
        // while the request was on it.
        ErrorCode::Unavailable if message.contains("client error (Connect)") => "connect_failed",
        ErrorCode::Unavailable
            if message.contains("client error (SendRequest)")
                || message.contains("connection closed")
                || message.contains("connection reset")
                || message.contains("close_notify") =>
        {
            "connection_closed"
        }
        ErrorCode::Unavailable => "unavailable",
        _ => "error",
    }
}

fn read_failure(
    catalog: &'static str,
    operation: &'static str,
    err: ConnectError,
    attempts: u32,
    elapsed: Duration,
) -> Error {
    let response_ref = ["cf-ray", "x-request-id"].iter().find_map(|name| {
        err.response_headers()
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    });
    let failure = failure_kind(&err);
    Error::CatalogLoad(Box::new(CatalogLoadFailure {
        catalog,
        operation,
        failure,
        attempts,
        elapsed,
        response_ref,
        source: Factory::map_error(err),
    }))
}
