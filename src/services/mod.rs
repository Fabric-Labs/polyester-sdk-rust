//! Service wrappers over generated Connect clients.

mod api_keys;
mod auth;
mod balances;
mod deposit_withdraw;
mod market_data;
mod orders;
mod profile;
mod scope;
mod thin;
mod triggers;
mod unary;

pub use api_keys::{ApiKeysService, UpdateApiKeyParams};
pub use auth::AuthService;
pub use balances::BalancesService;
pub use deposit_withdraw::{DepositService, WithdrawService, ZipperService};
pub use market_data::{
    CreateSubscriptionOptions, ListMarketOverviewOptions, MarketDataService,
    MarketOverviewCreateSubscriptionOptions, MarketOverviewService, OrderbookService,
};
pub use orders::{OrdersService, TradesService};
pub use profile::ProfileService;
pub use thin::*;
pub use triggers::TriggersService;

use crate::catalogs::Manager as CatalogManager;
use crate::transport::Factory;
use std::sync::Arc;

use crate::realtime::Client as RealtimeClient;

/// Shared dependencies for service constructors.
#[derive(Clone)]
pub struct ServiceContext {
    pub factory: Factory,
    pub catalogs: Arc<CatalogManager>,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    pub realtime: RealtimeClient,
}
