//! Root Polyester SDK client.

use crate::auth::{self, Credentials};
use crate::catalogs::Manager as CatalogManager;
use crate::errors::Result;
use crate::services::{
    AddressBookService, ApiKeysService, AuthService, BalancesService, ChainAnalyticsService,
    DepositService, GuardSignerService, HeatmapService, InternalTransfersService, LayoutService,
    LifecycleService, MarketDataService, MarketOverviewService, OrderbookService, OrdersService,
    PoliciesService, PolychartService, ResolveService, ServiceContext, SocialVerificationService,
    SubAccountsService, TradesService, TransfersService, TriggersService, WhiteboardService,
    WithdrawService, ZipperService,
};
use crate::transport::{
    Config as TransportConfig, DEFAULT_API_URL, DEFAULT_WS_URL, Factory, WireFormat,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

#[cfg(feature = "realtime")]
use crate::realtime::Client as RealtimeClient;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub api_key_id: Option<String>,
    pub api_private_key: Option<String>,
    pub api_url: String,
    pub ws_url: String,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    pub timeout: Duration,
    pub wire_format: WireFormat,
    pub hydrate_catalogs: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key_id: None,
            api_private_key: None,
            api_url: DEFAULT_API_URL.to_owned(),
            ws_url: DEFAULT_WS_URL.to_owned(),
            default_sub_account_id: None,
            default_account_id: None,
            timeout: Duration::from_secs(10),
            wire_format: WireFormat::Binary,
            hydrate_catalogs: true,
        }
    }
}

/// Async Polyester SDK entrypoint.
pub struct Client {
    pub api_url: String,
    pub ws_url: String,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    pub catalogs: Arc<CatalogManager>,
    #[cfg(feature = "realtime")]
    pub realtime: RealtimeClient,

    pub auth: AuthService,
    pub market_data: MarketDataService,
    pub market_overview: MarketOverviewService,
    pub zipper: ZipperService,
    pub chain_analytics: ChainAnalyticsService,
    pub heatmap: HeatmapService,
    pub lifecycle: LifecycleService,
    pub balances: BalancesService,
    pub orderbook: OrderbookService,
    pub orders: OrdersService,
    pub trades: TradesService,
    pub triggers: TriggersService,
    pub transfers: TransfersService,
    pub internal_transfers: InternalTransfersService,
    pub deposit: DepositService,
    pub api_keys: ApiKeysService,
    pub policies: PoliciesService,
    pub sub_accounts: SubAccountsService,
    pub resolve: ResolveService,
    pub address_book: AddressBookService,
    pub social_verification: SocialVerificationService,
    pub whiteboard: WhiteboardService,
    pub polychart: PolychartService,
    pub layout: LayoutService,
    pub guard_signer: GuardSignerService,
    pub withdraw: WithdrawService,
    catalog_ready: Arc<OnceCell<()>>,
    hydrate_catalogs_enabled: bool,
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        let hydrate_catalogs_enabled = config.hydrate_catalogs;
        let credentials = Credentials::load(
            config.api_key_id.as_deref(),
            config.api_private_key.as_deref(),
            false,
        )?;

        let transport_cfg = TransportConfig {
            api_url: config.api_url.clone(),
            ws_url: config.ws_url.clone(),
            timeout: config.timeout,
            wire_format: config.wire_format,
        };
        let factory = Factory::new(transport_cfg, credentials.clone())?;
        let catalogs = Arc::new(CatalogManager::new());

        #[cfg(feature = "realtime")]
        let realtime = RealtimeClient::new(
            config.ws_url.clone(),
            config.api_url.clone(),
            credentials,
            None,
        );

        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            #[cfg(feature = "realtime")]
            realtime: realtime.clone(),
        };

        let client = Self {
            api_url: config.api_url,
            ws_url: config.ws_url,
            default_sub_account_id: config.default_sub_account_id,
            default_account_id: config.default_account_id,
            catalogs,
            #[cfg(feature = "realtime")]
            realtime,
            auth: AuthService::new(ctx.clone()),
            market_data: MarketDataService::new(ctx.clone()),
            market_overview: MarketOverviewService::new(ctx.clone()),
            zipper: ZipperService::new(ctx.clone()),
            chain_analytics: ChainAnalyticsService::new(ctx.clone()),
            heatmap: HeatmapService::new(ctx.clone()),
            lifecycle: LifecycleService::new(ctx.clone()),
            balances: BalancesService::new(ctx.clone()),
            orderbook: OrderbookService::new(ctx.clone()),
            orders: OrdersService::new(ctx.clone()),
            trades: TradesService::new(ctx.clone()),
            triggers: TriggersService::new(ctx.clone()),
            transfers: TransfersService::new(ctx.clone()),
            internal_transfers: InternalTransfersService::new(ctx.clone()),
            deposit: DepositService::new(ctx.clone()),
            api_keys: ApiKeysService::new(ctx.clone()),
            policies: PoliciesService::new(ctx.clone()),
            sub_accounts: SubAccountsService::new(ctx.clone()),
            resolve: ResolveService::new(ctx.clone()),
            address_book: AddressBookService::new(ctx.clone()),
            social_verification: SocialVerificationService::new(ctx.clone()),
            whiteboard: WhiteboardService::new(ctx.clone()),
            polychart: PolychartService::new(ctx.clone()),
            layout: LayoutService::new(ctx.clone()),
            guard_signer: GuardSignerService::new(ctx.clone()),
            withdraw: WithdrawService::new(ctx),
            catalog_ready: Arc::new(OnceCell::new()),
            hydrate_catalogs_enabled,
        };

        client.start_catalog_hydration();
        Ok(client)
    }

    /// Build from `POLYESTER_API_KEY_ID` / `POLYESTER_API_PRIVATE_KEY` / `POLYESTER_ACCOUNT_ID`.
    pub fn from_env() -> Result<Self> {
        let mut config = Config {
            api_key_id: std::env::var(auth::API_KEY_ID_ENV).ok(),
            api_private_key: std::env::var(auth::API_PRIVATE_KEY_ENV).ok(),
            default_account_id: auth::account_id_from_env(),
            ..Default::default()
        };
        if let Ok(url) = std::env::var("POLYESTER_API_URL")
            && !url.trim().is_empty()
        {
            config.api_url = url;
        }
        if let Ok(url) = std::env::var("POLYESTER_WS_URL")
            && !url.trim().is_empty()
        {
            config.ws_url = url;
        }
        // Force from_env credential loading
        let credentials = Credentials::load(None, None, true)?;
        let transport_cfg = TransportConfig {
            api_url: config.api_url.clone(),
            ws_url: config.ws_url.clone(),
            timeout: config.timeout,
            wire_format: config.wire_format,
        };
        let factory = Factory::new(transport_cfg, credentials.clone())?;
        let catalogs = Arc::new(CatalogManager::new());
        #[cfg(feature = "realtime")]
        let realtime = RealtimeClient::new(
            config.ws_url.clone(),
            config.api_url.clone(),
            credentials,
            None,
        );
        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            #[cfg(feature = "realtime")]
            realtime: realtime.clone(),
        };
        let client = Self {
            api_url: config.api_url,
            ws_url: config.ws_url,
            default_sub_account_id: config.default_sub_account_id,
            default_account_id: config.default_account_id,
            catalogs,
            #[cfg(feature = "realtime")]
            realtime,
            auth: AuthService::new(ctx.clone()),
            market_data: MarketDataService::new(ctx.clone()),
            market_overview: MarketOverviewService::new(ctx.clone()),
            zipper: ZipperService::new(ctx.clone()),
            chain_analytics: ChainAnalyticsService::new(ctx.clone()),
            heatmap: HeatmapService::new(ctx.clone()),
            lifecycle: LifecycleService::new(ctx.clone()),
            balances: BalancesService::new(ctx.clone()),
            orderbook: OrderbookService::new(ctx.clone()),
            orders: OrdersService::new(ctx.clone()),
            trades: TradesService::new(ctx.clone()),
            triggers: TriggersService::new(ctx.clone()),
            transfers: TransfersService::new(ctx.clone()),
            internal_transfers: InternalTransfersService::new(ctx.clone()),
            deposit: DepositService::new(ctx.clone()),
            api_keys: ApiKeysService::new(ctx.clone()),
            policies: PoliciesService::new(ctx.clone()),
            sub_accounts: SubAccountsService::new(ctx.clone()),
            resolve: ResolveService::new(ctx.clone()),
            address_book: AddressBookService::new(ctx.clone()),
            social_verification: SocialVerificationService::new(ctx.clone()),
            whiteboard: WhiteboardService::new(ctx.clone()),
            polychart: PolychartService::new(ctx.clone()),
            layout: LayoutService::new(ctx.clone()),
            guard_signer: GuardSignerService::new(ctx.clone()),
            withdraw: WithdrawService::new(ctx),
            catalog_ready: Arc::new(OnceCell::new()),
            hydrate_catalogs_enabled: config.hydrate_catalogs,
        };
        client.start_catalog_hydration();
        Ok(client)
    }

    fn start_catalog_hydration(&self) {
        if !self.hydrate_catalogs_enabled || tokio::runtime::Handle::try_current().is_err() {
            return;
        }

        let ready = self.catalog_ready.clone();
        let market_data = self.market_data.clone();
        let zipper = self.zipper.clone();
        let catalogs = self.catalogs.clone();
        tokio::spawn(async move {
            ready
                .get_or_init(|| async move {
                    Self::hydrate_catalogs_with(market_data, zipper, catalogs).await;
                })
                .await;
        });
    }

    async fn hydrate_catalogs_with(
        market_data: MarketDataService,
        zipper: ZipperService,
        catalogs: Arc<CatalogManager>,
    ) {
        if let Ok(spot) = market_data.get_spot_config().await {
            catalogs.hydrate_spot_config_json(spot.raw);
        }
        if let Ok(zipper) = zipper.get_deposit_withdraw_config().await
            && let Ok(json) = serde_json::to_value(&zipper)
        {
            catalogs.hydrate_zipper_config_json(json);
        }
    }

    /// Best-effort catalog hydration from spot + zipper configs.
    ///
    /// This can be called explicitly to refresh the caches after construction.
    pub async fn hydrate_catalogs(&self) -> Result<()> {
        Self::hydrate_catalogs_with(
            self.market_data.clone(),
            self.zipper.clone(),
            self.catalogs.clone(),
        )
        .await;
        let _ = self.catalog_ready.set(());
        Ok(())
    }

    /// Wait until construction-time best-effort catalog hydration finishes.
    ///
    /// If the client was created outside a Tokio runtime, this starts hydration
    /// on the caller's current runtime. Returns immediately when hydration was
    /// disabled in [`Config`].
    pub async fn wait_for_catalogs(&self) -> Result<()> {
        if !self.hydrate_catalogs_enabled {
            return Ok(());
        }
        self.catalog_ready
            .get_or_init(|| async {
                Self::hydrate_catalogs_with(
                    self.market_data.clone(),
                    self.zipper.clone(),
                    self.catalogs.clone(),
                )
                .await;
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::DEFAULT_API_URL;

    #[test]
    fn config_defaults_point_at_devnet() {
        let cfg = Config::default();
        assert_eq!(cfg.api_url, DEFAULT_API_URL);
        assert!(cfg.hydrate_catalogs);
        assert!(cfg.api_key_id.is_none());
    }

    #[test]
    fn client_new_without_creds_exposes_service_tree() {
        let client = Client::new(Config::default()).expect("client");
        assert!(!client.api_url.is_empty());
        assert_eq!(
            client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
            8
        );
        // Touch service handles so the surface stays wired.
        let _ = (
            &client.auth,
            &client.market_data,
            &client.market_overview,
            &client.orders,
            &client.trades,
            &client.triggers,
            &client.balances,
            &client.orderbook,
            &client.zipper,
            &client.transfers,
            &client.api_keys,
            &client.policies,
            &client.sub_accounts,
            &client.deposit,
            &client.withdraw,
        );
    }

    #[tokio::test]
    async fn wait_for_catalogs_completes_best_effort_hydration() {
        let client = Client::new(Config {
            api_url: "http://127.0.0.1:9".into(),
            timeout: Duration::from_millis(50),
            hydrate_catalogs: true,
            ..Default::default()
        })
        .expect("client");

        client.wait_for_catalogs().await.expect("best effort");
        assert!(client.catalog_ready.get().is_some());
    }

    #[tokio::test]
    async fn wait_for_catalogs_returns_immediately_when_disabled() {
        let client = Client::new(Config {
            api_url: "http://127.0.0.1:9".into(),
            timeout: Duration::from_millis(50),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .expect("client");

        client.wait_for_catalogs().await.expect("disabled");
        assert!(client.catalog_ready.get().is_none());
    }
}
