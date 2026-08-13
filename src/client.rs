//! Root Polyester SDK client.

use crate::auth::{self, Credentials};
use crate::catalogs::Manager as CatalogManager;
use crate::errors::{Error, Result};
use crate::services::{
    AddressBookService, ApiKeysService, AuthService, BalancesService, ChainAnalyticsService,
    DepositService, FeeService, GuardSignerService, HeatmapService, InternalTransfersService,
    LayoutService, LifecycleService, MarketDataService, MarketOverviewService, OrderbookService,
    OrdersService, PoliciesService, PolychartService, RateLimitService, ServiceContext,
    SocialVerificationService, SubAccountsService, TradesService, TransfersService,
    TriggersService, VipService, WhiteboardService, WithdrawService, ZipperService,
};
use crate::transport::{
    Config as TransportConfig, DEFAULT_API_URL, DEFAULT_WS_URL, Factory, WireFormat,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::realtime::Client as RealtimeClient;

/// Client configuration.
#[derive(Clone)]
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

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("api_key_id", &self.api_key_id)
            .field(
                "api_private_key",
                &self.api_private_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("api_url", &self.api_url)
            .field("ws_url", &self.ws_url)
            .field("default_sub_account_id", &self.default_sub_account_id)
            .field("default_account_id", &self.default_account_id)
            .field("timeout", &self.timeout)
            .field("wire_format", &self.wire_format)
            .field("hydrate_catalogs", &self.hydrate_catalogs)
            .finish()
    }
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
    pub address_book: AddressBookService,
    pub social_verification: SocialVerificationService,
    pub whiteboard: WhiteboardService,
    pub polychart: PolychartService,
    pub layout: LayoutService,
    pub guard_signer: GuardSignerService,
    pub vip: VipService,
    pub fees: FeeService,
    pub rate_limits: RateLimitService,
    pub withdraw: WithdrawService,
    catalog_ready: Arc<OnceCell<Result<()>>>,
    catalog_hydrate_lock: Arc<tokio::sync::Mutex<()>>,
    catalog_last_error: Arc<std::sync::Mutex<Option<Error>>>,
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

        let realtime = RealtimeClient::with_timeout(
            config.ws_url.clone(),
            config.api_url.clone(),
            credentials,
            None,
            config.timeout,
        );

        let catalog_ready = Arc::new(OnceCell::new());
        let catalog_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let catalog_last_error = Arc::new(std::sync::Mutex::new(None));
        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            realtime: realtime.clone(),
            catalog_ready: catalog_ready.clone(),
            hydrate_catalogs_enabled,
        };

        let client = Self {
            api_url: config.api_url,
            ws_url: config.ws_url,
            default_sub_account_id: config.default_sub_account_id,
            default_account_id: config.default_account_id,
            catalogs,
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
            address_book: AddressBookService::new(ctx.clone()),
            social_verification: SocialVerificationService::new(ctx.clone()),
            whiteboard: WhiteboardService::new(ctx.clone()),
            polychart: PolychartService::new(ctx.clone()),
            layout: LayoutService::new(ctx.clone()),
            guard_signer: GuardSignerService::new(ctx.clone()),
            vip: VipService::new(ctx.clone()),
            fees: FeeService::new(ctx.clone()),
            rate_limits: RateLimitService::new(ctx.clone()),
            withdraw: WithdrawService::new(ctx),
            catalog_ready,
            catalog_hydrate_lock,
            catalog_last_error,
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
        let realtime = RealtimeClient::with_timeout(
            config.ws_url.clone(),
            config.api_url.clone(),
            credentials,
            None,
            config.timeout,
        );
        let catalog_ready = Arc::new(OnceCell::new());
        let catalog_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let catalog_last_error = Arc::new(std::sync::Mutex::new(None));
        let hydrate_catalogs_enabled = config.hydrate_catalogs;
        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            realtime: realtime.clone(),
            catalog_ready: catalog_ready.clone(),
            hydrate_catalogs_enabled,
        };
        let client = Self {
            api_url: config.api_url,
            ws_url: config.ws_url,
            default_sub_account_id: config.default_sub_account_id,
            default_account_id: config.default_account_id,
            catalogs,
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
            address_book: AddressBookService::new(ctx.clone()),
            social_verification: SocialVerificationService::new(ctx.clone()),
            whiteboard: WhiteboardService::new(ctx.clone()),
            polychart: PolychartService::new(ctx.clone()),
            layout: LayoutService::new(ctx.clone()),
            guard_signer: GuardSignerService::new(ctx.clone()),
            vip: VipService::new(ctx.clone()),
            fees: FeeService::new(ctx.clone()),
            rate_limits: RateLimitService::new(ctx.clone()),
            withdraw: WithdrawService::new(ctx),
            catalog_ready,
            catalog_hydrate_lock,
            catalog_last_error,
            hydrate_catalogs_enabled,
        };
        client.start_catalog_hydration();
        Ok(client)
    }

    fn start_catalog_hydration(&self) {
        if !self.hydrate_catalogs_enabled {
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            let error = Error::validation(
                "catalog hydration was not started because Client was constructed outside a \
                 Tokio runtime; await client.wait_for_catalogs() before placing orders",
            );
            *crate::realtime::lock_unpoisoned(&self.catalog_last_error) = Some(error.clone());
            let _ = self.catalog_ready.set(Err(error));
            return;
        }

        let ready = self.catalog_ready.clone();
        let hydrate_lock = self.catalog_hydrate_lock.clone();
        let last_error = self.catalog_last_error.clone();
        let market_data = self.market_data.clone();
        let zipper = self.zipper.clone();
        let catalogs = self.catalogs.clone();
        tokio::spawn(async move {
            ready
                .get_or_init(|| async move {
                    let _guard = hydrate_lock.lock().await;
                    let result = Self::hydrate_catalogs_with(market_data, zipper, catalogs).await;
                    *crate::realtime::lock_unpoisoned(&last_error) = result.clone().err();
                    result
                })
                .await;
        });
    }

    async fn hydrate_catalogs_with(
        market_data: MarketDataService,
        zipper: ZipperService,
        catalogs: Arc<CatalogManager>,
    ) -> Result<()> {
        // Fetch both configs before mutating catalogs so a zipper failure cannot
        // leave a partially installed spot catalog.
        let spot = market_data.get_spot_config().await.map_err(|e| {
            Error::validation(format!("catalog hydration failed (spot config): {e}"))
        })?;
        let zipper_cfg = zipper.get_deposit_withdraw_config().await.map_err(|e| {
            Error::validation(format!("catalog hydration failed (zipper config): {e}"))
        })?;
        let zipper_json = serde_json::to_value(&zipper_cfg).map_err(|e| {
            Error::validation(format!("catalog hydration failed (zipper encode): {e}"))
        })?;
        catalogs.hydrate_spot_and_zipper_json(spot.raw, zipper_json)?;
        Ok(())
    }

    /// Hydrate spot + zipper catalogs. Returns an error when either fetch or
    /// parse leaves catalogs unusable.
    pub async fn hydrate_catalogs(&self) -> Result<()> {
        let _guard = self.catalog_hydrate_lock.lock().await;
        let result = Self::hydrate_catalogs_with(
            self.market_data.clone(),
            self.zipper.clone(),
            self.catalogs.clone(),
        )
        .await;
        *crate::realtime::lock_unpoisoned(&self.catalog_last_error) = result.clone().err();
        let _ = self.catalog_ready.set(result.clone());
        result
    }

    /// Wait until construction-time catalog hydration finishes.
    ///
    /// Returns [`Err`] when hydration failed (HTTP/transport error, malformed
    /// config, or invalid scales). Concurrent waiters share one attempt.
    ///
    /// Returns immediately when hydration was disabled in [`Config`].
    pub async fn wait_for_catalogs(&self) -> Result<()> {
        if !self.hydrate_catalogs_enabled {
            return Ok(());
        }
        if self.catalogs.is_ready() {
            return Ok(());
        }
        if matches!(self.catalog_ready.get(), Some(Err(_))) {
            let _guard = self.catalog_hydrate_lock.lock().await;
            if self.catalogs.is_ready() {
                return Ok(());
            }
            let result = Self::hydrate_catalogs_with(
                self.market_data.clone(),
                self.zipper.clone(),
                self.catalogs.clone(),
            )
            .await;
            *crate::realtime::lock_unpoisoned(&self.catalog_last_error) = result.clone().err();
            return result;
        }
        let last_error = self.catalog_last_error.clone();
        let hydrate_lock = self.catalog_hydrate_lock.clone();
        let market_data = self.market_data.clone();
        let zipper = self.zipper.clone();
        let catalogs = self.catalogs.clone();
        self.catalog_ready
            .get_or_init(|| async move {
                let _guard = hydrate_lock.lock().await;
                let result = Self::hydrate_catalogs_with(market_data, zipper, catalogs).await;
                *crate::realtime::lock_unpoisoned(&last_error) = result.clone().err();
                result
            })
            .await
            .clone()
    }

    /// Most recent catalog hydration error, if any.
    pub fn catalogs_last_error(&self) -> Option<Error> {
        crate::realtime::lock_unpoisoned(&self.catalog_last_error).clone()
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
    fn config_debug_redacts_private_key() {
        let config = Config {
            api_key_id: Some("ak_test".into()),
            api_private_key: Some("super-secret-private-key".into()),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(rendered.contains("ak_test"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("super-secret-private-key"));
    }

    #[test]
    fn ed25519_keypair_debug_redacts_secret() {
        let keypair = crate::models::Ed25519Keypair {
            public_key_hex: "abcd".into(),
            secret_key_hex: "super-secret-seed-hex".into(),
            public_key: vec![1, 2, 3],
            secret_key: b"super-secret-seed-bytes".to_vec(),
        };
        let rendered = format!("{keypair:?}");
        assert!(rendered.contains("abcd"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("super-secret-seed-hex"));
        assert!(!rendered.contains("super-secret-seed-bytes"));
    }

    #[test]
    fn catalog_error_state_recovers_from_a_poisoned_mutex() {
        let client = Client::new(Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let state = Arc::clone(&client.catalog_last_error);
        let _ = std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("poison catalog error state");
        })
        .join();

        assert!(client.catalogs_last_error().is_none());
        *crate::realtime::lock_unpoisoned(&client.catalog_last_error) =
            Some(Error::transport("recovered"));
        assert_eq!(
            client.catalogs_last_error().map(|error| error.to_string()),
            Some("recovered".to_owned())
        );
    }

    #[test]
    fn client_new_without_creds_exposes_service_tree() {
        let client = Client::new(Config::default()).expect("client");
        assert!(!client.api_url.is_empty());
        let catalog_start = client
            .catalog_ready
            .get()
            .expect("construction outside Tokio records catalog state")
            .as_ref()
            .expect_err("catalog hydration cannot start without a runtime");
        assert!(
            catalog_start
                .to_string()
                .contains("outside a Tokio runtime")
        );
        assert_eq!(
            client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
            None
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
            &client.vip,
            &client.fees,
            &client.rate_limits,
        );
    }

    #[tokio::test]
    async fn wait_for_catalogs_errors_when_hydration_fails() {
        let client = Client::new(Config {
            api_url: "http://127.0.0.1:9".into(),
            timeout: Duration::from_millis(50),
            hydrate_catalogs: true,
            ..Default::default()
        })
        .expect("client");

        let err = client
            .wait_for_catalogs()
            .await
            .expect_err("unreachable API must fail closed");
        assert!(
            err.to_string().contains("catalog hydration failed"),
            "unexpected error: {err}"
        );
        assert!(client.catalogs_last_error().is_some());
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
