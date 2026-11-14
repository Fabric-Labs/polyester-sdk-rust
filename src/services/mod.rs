//! Service wrappers over generated Connect clients.

mod alerts;
mod api_keys;
mod auth;
mod balances;
mod changelog;
mod correlation_id;
mod deposit_withdraw;
mod market_data;
mod orders;
mod profile;
mod scope;
mod statements;
pub(crate) mod system;
mod thin;
mod triggers;
pub(crate) mod unary;

pub use alerts::AlertsService;
pub use api_keys::ApiKeysService;
pub use auth::AuthService;
pub use balances::BalancesService;
pub use changelog::{
    ChangelogService, ListChangelogChangesOptions, ListChangelogDeploymentGroupsOptions,
    ListChangelogPostsOptions,
};
pub use deposit_withdraw::{
    DepositService, PreparedTradingWithdraw, WithdrawService, ZipperService,
    new_trading_withdraw_idempotency_key, new_trading_withdraw_nonce,
};
pub use market_data::{
    CreateSubscriptionOptions, ListMarketOverviewOptions, MarketDataService,
    MarketOverviewCreateSubscriptionOptions, MarketOverviewService, OrderbookService,
};
pub(crate) use orders::scale_order;
pub use orders::{OrdersService, TradesService};
pub use profile::ProfileService;
pub use statements::{ExportStatementOptions, StatementsService};
pub use system::ListStatusEventsOptions;
pub use thin::*;
pub use triggers::TriggersService;

use crate::catalogs::Manager as CatalogManager;
use crate::errors::{Error, Result};
use crate::transport::Factory;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::realtime::Client as RealtimeClient;

/// Reject an explicit zero on optional ListOpen-style limits (`Option` wire
/// fields where omission means server default and `Some(0)` is invalid).
///
/// Plain `u32` / zero-means-default endpoints should pass `0` through instead.
fn positive_limit(limit: Option<u32>, endpoint: &str) -> Result<Option<u32>> {
    match limit {
        Some(0) => Err(Error::validation(format!(
            "{endpoint} limit must be positive when explicitly supplied"
        ))),
        other => Ok(other),
    }
}

/// Shared dependencies for service constructors.
#[derive(Clone)]
pub struct ServiceContext {
    pub factory: Factory,
    pub catalogs: Arc<CatalogManager>,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    pub realtime: RealtimeClient,
    /// Outcome of the spot config's load.
    pub catalog_ready: Arc<OnceCell<Result<()>>>,
    /// Outcome of the funding (deposit/withdraw) config's load.
    pub funding_ready: Arc<OnceCell<Result<()>>>,
    pub hydrate_catalogs_enabled: bool,
}

impl ServiceContext {
    /// Wait for construction-time catalog hydration when enabled, before a
    /// call resolves a display symbol or reads a pair's scales.
    ///
    /// A failed hydration is returned as [`Error::CatalogHydration`], so a
    /// call names why the catalogs are missing instead of refusing the symbol.
    pub async fn wait_for_catalogs(&self) -> Result<()> {
        self.hydration_outcome()
            .await
            .map_err(Error::catalog_hydration)
    }

    /// Wait for the funding (deposit/withdraw) config the client began
    /// loading when it was created, for a call that decodes with it. A failed
    /// load is returned as [`Error::CatalogHydration`] naming it.
    pub async fn wait_for_funding_catalog(&self) -> Result<()> {
        self.outcome_of(&self.funding_ready, || self.catalogs.is_funding_ready())
            .await
            .map_err(Error::catalog_hydration)
    }

    /// Wait for construction-time catalog hydration, whatever its outcome, for
    /// a call that only labels or scales its results from the catalogs and
    /// works without them.
    pub(crate) async fn settle_catalogs(&self) {
        let _ = self.hydration_outcome().await;
    }

    async fn hydration_outcome(&self) -> Result<()> {
        self.outcome_of(&self.catalog_ready, || self.catalogs.is_spot_ready())
            .await
    }

    /// The outcome of one catalog's load: `Ok` once `is_ready`, otherwise
    /// what the load ended with.
    async fn outcome_of(
        &self,
        load: &OnceCell<Result<()>>,
        is_ready: impl Fn() -> bool,
    ) -> Result<()> {
        if !self.hydrate_catalogs_enabled {
            return Ok(());
        }
        if is_ready() {
            return Ok(());
        }
        if let Some(result) = load.get() {
            return result.clone();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while load.get().is_none() {
            if is_ready() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::validation(
                    "catalogs are not ready; await client.wait_for_catalogs() before placing orders",
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if is_ready() {
            return Ok(());
        }
        load.get().cloned().unwrap_or_else(|| {
            Err(Error::validation(
                "catalogs are not ready; await client.wait_for_catalogs() before placing orders",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_limits_preserve_omission_and_reject_explicit_zero() {
        assert_eq!(positive_limit(None, "list_open_orders").unwrap(), None);
        assert_eq!(
            positive_limit(Some(1), "list_open_orders").unwrap(),
            Some(1)
        );
        let err = positive_limit(Some(0), "list_open_orders").unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("positive"));
    }
}
