//! Service wrappers over generated Connect clients.

mod api_keys;
mod auth;
mod balances;
mod correlation_id;
mod deposit_withdraw;
mod market_data;
mod orders;
mod profile;
mod scope;
mod thin;
mod triggers;
mod unary;

pub use api_keys::ApiKeysService;
pub use auth::AuthService;
pub use balances::BalancesService;
pub use deposit_withdraw::{
    DepositService, PreparedTradingWithdraw, WithdrawService, ZipperService,
    new_trading_withdraw_idempotency_key, new_trading_withdraw_nonce,
};
pub use market_data::{
    CreateSubscriptionOptions, ListMarketOverviewOptions, MarketDataService,
    MarketOverviewCreateSubscriptionOptions, MarketOverviewService, OrderbookService,
};
pub use orders::{OrdersService, TradesService};
pub use profile::ProfileService;
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
    pub catalog_ready: Arc<OnceCell<Result<()>>>,
    pub hydrate_catalogs_enabled: bool,
}

impl ServiceContext {
    /// Wait for construction-time catalog hydration when enabled.
    ///
    /// Propagates hydration failure so order paths do not proceed with empty catalogs.
    pub async fn wait_for_catalogs(&self) -> Result<()> {
        if !self.hydrate_catalogs_enabled {
            return Ok(());
        }
        if self.catalogs.is_ready() {
            return Ok(());
        }
        if let Some(result) = self.catalog_ready.get() {
            return result.clone();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while self.catalog_ready.get().is_none() {
            if self.catalogs.is_ready() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::validation(
                    "catalogs are not ready; await client.wait_for_catalogs() before placing orders",
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if self.catalogs.is_ready() {
            return Ok(());
        }
        self.catalog_ready.get().cloned().unwrap_or_else(|| {
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
