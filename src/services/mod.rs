//! Service wrappers over generated Connect clients.

mod auth;
mod balances;
mod deposit_withdraw;
mod market_data;
mod orders;
mod scope;
mod thin;
mod triggers;
mod unary;

pub use auth::AuthService;
pub use balances::BalancesService;
pub use deposit_withdraw::{DepositService, WithdrawService, ZipperService};
pub use market_data::{MarketDataService, MarketOverviewService, OrderbookService};
pub use orders::{OrdersService, TradesService};
pub use thin::*;
pub use triggers::TriggersService;

use crate::catalogs::Manager as CatalogManager;
use crate::transport::Factory;
use std::sync::Arc;

#[cfg(feature = "realtime")]
use crate::realtime::Client as RealtimeClient;

/// Shared dependencies for service constructors.
#[derive(Clone)]
pub struct ServiceContext {
    pub factory: Factory,
    pub catalogs: Arc<CatalogManager>,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    #[cfg(feature = "realtime")]
    pub realtime: RealtimeClient,
}
