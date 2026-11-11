//! Root Polyester SDK client.

use crate::auth::{self, Credentials};
use crate::catalogs::Manager as CatalogManager;
use crate::environment::{
    ENV_NAME_ENV, POLYESTER_DEVNET_ENVIRONMENT, PolyesterEnvironment, environment_from_name,
    parse_polyester_environment,
};
use crate::errors::{Error, Result};
use crate::models::{
    CancelAllOpts, ListTriggersOpts, StopAllTradingFailure, StopAllTradingResult,
    StopAllTradingScope,
};
use crate::services::{
    AddressBookService, ApiKeysService, AuthService, BalancesService, ChainAnalyticsService,
    ChangelogService, DepositService, FeeService, GuardSignerService, HeatmapService,
    InternalTransfersService, LifecycleService, MarketDataService, MarketOverviewService,
    OrderbookService, OrdersService, PoliciesService, RateLimitService, ServiceContext,
    SocialVerificationService, SubAccountsService, TradesService, TransfersService,
    TriggersService, VipService, WithdrawService, ZipperService,
};
use crate::transport::{
    Config as TransportConfig, DEFAULT_API_URL, DEFAULT_WS_URL, Factory, WireFormat,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::realtime::Client as RealtimeClient;

/// Triggers per `ListTriggers` page in [`Client::stop_all_trading`]: the
/// server's maximum.
const STOP_ALL_TRADING_PAGE_LIMIT: u32 = 1000;

/// Client configuration.
#[derive(Clone)]
pub struct Config {
    pub environment: Option<PolyesterEnvironment>,
    pub api_key_id: Option<String>,
    pub api_private_key: Option<String>,
    pub api_url: String,
    pub ws_url: String,
    pub default_sub_account_id: Option<String>,
    pub default_account_id: Option<String>,
    pub timeout: Duration,
    pub wire_format: WireFormat,
    pub hydrate_catalogs: bool,
    /// Allow non-loopback `http://` / `ws://` endpoints.
    pub allow_insecure_http: bool,
    /// Receive window of every signed API request, in milliseconds, sent as
    /// the signed `X-API-RECV-WINDOW` header: the server processes a request
    /// only until its signing timestamp plus the window, and refuses it after
    /// that, having changed nothing, as the retryable
    /// [`crate::Error::TimestampSkew`], or
    /// [`crate::order_codes::REQUEST_DEADLINE_EXCEEDED`] once authenticated.
    /// It must be from 1 to 60000; [`Client::new`] refuses any other value
    /// with [`crate::Error::Validation`]. `None`, the default, sends no header,
    /// and the server applies 5000. Realtime token requests always use the
    /// default.
    pub recv_window_ms: Option<u32>,
    /// Deadline of the catalog loads [`Self::hydrate_catalogs`] starts: the
    /// spot config and the funding (deposit/withdraw) config each load on
    /// their own, and a read that times out or loses its connection is read
    /// again, at most 3 times in all, 250ms and then 500ms apart, each retry
    /// on a new connection, all within this long of [`Client::new`]. `None`,
    /// the default, means twice [`Self::timeout`]; [`Client::new`] refuses a
    /// zero duration with [`crate::Error::Validation`].
    pub catalog_timeout: Option<Duration>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field(
                "environment",
                &self.environment.as_ref().map(|env| &env.name),
            )
            .field("api_key_id", &self.api_key_id)
            .field(
                "api_private_key",
                &self.api_private_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "api_url",
                &crate::transport::url_without_userinfo(&self.api_url),
            )
            .field(
                "ws_url",
                &crate::transport::url_without_userinfo(&self.ws_url),
            )
            .field("default_sub_account_id", &self.default_sub_account_id)
            .field("default_account_id", &self.default_account_id)
            .field("timeout", &self.timeout)
            .field("wire_format", &self.wire_format)
            .field("hydrate_catalogs", &self.hydrate_catalogs)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .field("recv_window_ms", &self.recv_window_ms)
            .field("catalog_timeout", &self.catalog_timeout)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            environment: None,
            api_key_id: None,
            api_private_key: None,
            api_url: DEFAULT_API_URL.to_owned(),
            ws_url: DEFAULT_WS_URL.to_owned(),
            default_sub_account_id: None,
            default_account_id: None,
            timeout: Duration::from_secs(10),
            wire_format: WireFormat::Binary,
            hydrate_catalogs: true,
            allow_insecure_http: false,
            recv_window_ms: None,
            catalog_timeout: None,
        }
    }
}

/// Async Polyester SDK entrypoint.
pub struct Client {
    pub environment: PolyesterEnvironment,
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
    pub guard_signer: GuardSignerService,
    pub vip: VipService,
    pub fees: FeeService,
    pub rate_limits: RateLimitService,
    /// Public product changelog; needs no credentials.
    pub changelog: ChangelogService,
    pub withdraw: WithdrawService,
    /// Outcome of the spot config's load.
    catalog_ready: Arc<OnceCell<Result<()>>>,
    catalog_hydrate_lock: Arc<tokio::sync::Mutex<()>>,
    catalog_last_error: Arc<std::sync::Mutex<Option<Error>>>,
    /// Outcome of the funding (deposit/withdraw) config's load.
    funding_ready: Arc<OnceCell<Result<()>>>,
    funding_hydrate_lock: Arc<tokio::sync::Mutex<()>>,
    funding_last_error: Arc<std::sync::Mutex<Option<Error>>>,
    hydrate_catalogs_enabled: bool,
    catalog_timeout: Duration,
    factory: Factory,
}

fn catalog_timeout(config: &Config) -> Result<Duration> {
    match config.catalog_timeout {
        None => Ok(config.timeout * 2),
        Some(timeout) if timeout.is_zero() => Err(Error::validation(
            "catalog_timeout must be a positive duration",
        )),
        Some(timeout) => Ok(timeout),
    }
}

fn resolve_environment(config: &Config) -> Result<(PolyesterEnvironment, String, String)> {
    let resolved = match &config.environment {
        Some(env) => parse_polyester_environment(env)?,
        None => POLYESTER_DEVNET_ENVIRONMENT.clone(),
    };
    let api_url = if config.api_url.is_empty()
        || (config.environment.is_some() && config.api_url == DEFAULT_API_URL)
    {
        resolved.api_url.clone()
    } else {
        config.api_url.clone()
    };
    let ws_url = if config.ws_url.is_empty()
        || (config.environment.is_some() && config.ws_url == DEFAULT_WS_URL)
    {
        resolved.websocket_url.clone()
    } else {
        config.ws_url.clone()
    };
    Ok((resolved, api_url, ws_url))
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        let hydrate_catalogs_enabled = config.hydrate_catalogs;
        let (environment, api_url, ws_url) = resolve_environment(&config)?;
        let credentials = Credentials::load(
            config.api_key_id.as_deref(),
            config.api_private_key.as_deref(),
            false,
        )?;

        let transport_cfg = TransportConfig {
            api_url: api_url.clone(),
            ws_url: ws_url.clone(),
            timeout: config.timeout,
            wire_format: config.wire_format,
            allow_insecure_http: config.allow_insecure_http,
            recv_window_ms: config.recv_window_ms,
        };
        let factory = Factory::new(transport_cfg, credentials.clone())?;
        let catalogs = Arc::new(CatalogManager::new());

        let realtime = RealtimeClient::with_timeout(
            ws_url.clone(),
            api_url.clone(),
            credentials,
            None,
            config.timeout,
        )
        .allow_insecure_http(config.allow_insecure_http);

        let catalog_ready = Arc::new(OnceCell::new());
        let catalog_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let catalog_last_error = Arc::new(std::sync::Mutex::new(None));
        let funding_ready = Arc::new(OnceCell::new());
        let funding_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let funding_last_error = Arc::new(std::sync::Mutex::new(None));
        let catalog_timeout = catalog_timeout(&config)?;
        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            realtime: realtime.clone(),
            catalog_ready: catalog_ready.clone(),
            funding_ready: funding_ready.clone(),
            hydrate_catalogs_enabled,
        };

        let client = Self {
            environment,
            api_url,
            ws_url,
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
            guard_signer: GuardSignerService::new(ctx.clone()),
            vip: VipService::new(ctx.clone()),
            fees: FeeService::new(ctx.clone()),
            rate_limits: RateLimitService::new(ctx.clone()),
            changelog: ChangelogService::new(ctx.clone()),
            factory: ctx.factory.clone(),
            withdraw: WithdrawService::new(ctx),
            catalog_ready,
            catalog_hydrate_lock,
            catalog_last_error,
            funding_ready,
            funding_hydrate_lock,
            funding_last_error,
            hydrate_catalogs_enabled,
            catalog_timeout,
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
        if let Ok(name) = std::env::var(ENV_NAME_ENV)
            && !name.trim().is_empty()
        {
            config.environment = Some(environment_from_name(&name)?);
        }
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
        config.allow_insecure_http = crate::transport::env_allow_insecure_http();
        let (environment, api_url, ws_url) = resolve_environment(&config)?;
        // Force from_env credential loading
        let credentials = Credentials::load(None, None, true)?;
        let transport_cfg = TransportConfig {
            api_url: api_url.clone(),
            ws_url: ws_url.clone(),
            timeout: config.timeout,
            wire_format: config.wire_format,
            allow_insecure_http: config.allow_insecure_http,
            recv_window_ms: config.recv_window_ms,
        };
        let factory = Factory::new(transport_cfg, credentials.clone())?;
        let catalogs = Arc::new(CatalogManager::new());
        let realtime = RealtimeClient::with_timeout(
            ws_url.clone(),
            api_url.clone(),
            credentials,
            None,
            config.timeout,
        )
        .allow_insecure_http(config.allow_insecure_http);
        let catalog_ready = Arc::new(OnceCell::new());
        let catalog_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let catalog_last_error = Arc::new(std::sync::Mutex::new(None));
        let funding_ready = Arc::new(OnceCell::new());
        let funding_hydrate_lock = Arc::new(tokio::sync::Mutex::new(()));
        let funding_last_error = Arc::new(std::sync::Mutex::new(None));
        let catalog_timeout = catalog_timeout(&config)?;
        let hydrate_catalogs_enabled = config.hydrate_catalogs;
        let ctx = ServiceContext {
            factory,
            catalogs: catalogs.clone(),
            default_sub_account_id: config.default_sub_account_id.clone(),
            default_account_id: config.default_account_id.clone(),
            realtime: realtime.clone(),
            catalog_ready: catalog_ready.clone(),
            funding_ready: funding_ready.clone(),
            hydrate_catalogs_enabled,
        };
        let client = Self {
            environment,
            api_url,
            ws_url,
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
            guard_signer: GuardSignerService::new(ctx.clone()),
            vip: VipService::new(ctx.clone()),
            fees: FeeService::new(ctx.clone()),
            rate_limits: RateLimitService::new(ctx.clone()),
            changelog: ChangelogService::new(ctx.clone()),
            factory: ctx.factory.clone(),
            withdraw: WithdrawService::new(ctx),
            catalog_ready,
            catalog_hydrate_lock,
            catalog_last_error,
            funding_ready,
            funding_hydrate_lock,
            funding_last_error,
            hydrate_catalogs_enabled,
            catalog_timeout,
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
            for (last_error, ready) in [
                (&self.catalog_last_error, &self.catalog_ready),
                (&self.funding_last_error, &self.funding_ready),
            ] {
                *crate::realtime::lock_unpoisoned(last_error) = Some(error.clone());
                let _ = ready.set(Err(error.clone()));
            }
            return;
        }
        // Both start now and load on their own, so spot trading never waits
        // for, or fails with, the funding config.
        let deadline = tokio::time::Instant::now() + self.catalog_timeout;
        for kind in [CatalogKind::Spot, CatalogKind::Funding] {
            let load = self.load_state(kind);
            let factory = self.factory.clone();
            let catalogs = self.catalogs.clone();
            tokio::spawn(async move {
                load.ready
                    .get_or_init(|| async move {
                        let _guard = load.lock.lock().await;
                        let result = kind.load(&factory, &catalogs, deadline).await;
                        *crate::realtime::lock_unpoisoned(&load.last_error) = result.clone().err();
                        result
                    })
                    .await;
            });
        }
    }

    fn load_state(&self, kind: CatalogKind) -> LoadState {
        match kind {
            CatalogKind::Spot => LoadState {
                ready: self.catalog_ready.clone(),
                lock: self.catalog_hydrate_lock.clone(),
                last_error: self.catalog_last_error.clone(),
            },
            CatalogKind::Funding => LoadState {
                ready: self.funding_ready.clone(),
                lock: self.funding_hydrate_lock.clone(),
                last_error: self.funding_last_error.clone(),
            },
        }
    }

    /// Load one catalog again now, within a new `catalog_timeout`.
    async fn reload(&self, kind: CatalogKind) -> Result<()> {
        let load = self.load_state(kind);
        let _guard = load.lock.lock().await;
        if kind.is_ready(&self.catalogs) {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.catalog_timeout;
        let result = kind.load(&self.factory, &self.catalogs, deadline).await;
        *crate::realtime::lock_unpoisoned(&load.last_error) = result.clone().err();
        let _ = load.ready.set(result.clone());
        result
    }

    /// Load the spot and the funding catalogs again. Returns the spot
    /// config's error when it failed, otherwise the funding config's.
    pub async fn hydrate_catalogs(&self) -> Result<()> {
        let (spot, funding) = tokio::join!(
            self.reload_unconditionally(CatalogKind::Spot),
            self.reload_unconditionally(CatalogKind::Funding)
        );
        spot.and(funding)
    }

    async fn reload_unconditionally(&self, kind: CatalogKind) -> Result<()> {
        let load = self.load_state(kind);
        let _guard = load.lock.lock().await;
        let deadline = tokio::time::Instant::now() + self.catalog_timeout;
        let result = kind.load(&self.factory, &self.catalogs, deadline).await;
        *crate::realtime::lock_unpoisoned(&load.last_error) = result.clone().err();
        let _ = load.ready.set(result.clone());
        result
    }

    /// Wait until the spot config, which every spot trading call needs, is
    /// ready.
    ///
    /// Returns `Ok` once it has loaded and validated, without waiting for the
    /// funding config (see [`Self::wait_for_funding_catalog`]), so spot
    /// trading can start while that one is still loading or failed to load.
    /// Returns [`Error::CatalogLoad`] when the spot config could not be
    /// loaded, naming the operation, the kind of failure and the response
    /// reference; calls that name a market then fail closed. After a failed
    /// load, each call loads it again first. Concurrent waiters share one
    /// attempt.
    ///
    /// Returns immediately when hydration was disabled in [`Config`].
    pub async fn wait_for_catalogs(&self) -> Result<()> {
        self.wait_for(CatalogKind::Spot).await
    }

    /// Wait until the funding (deposit/withdraw) config is ready, as
    /// [`Self::wait_for_catalogs`] does for the spot config. Read the zipper
    /// catalog and [`CatalogManager::ledger_id_for_asset`] after it returns
    /// `Ok`. Returns [`Error::CatalogLoad`] naming the funding config when it
    /// could not be loaded.
    pub async fn wait_for_funding_catalog(&self) -> Result<()> {
        self.wait_for(CatalogKind::Funding).await
    }

    async fn wait_for(&self, kind: CatalogKind) -> Result<()> {
        if !self.hydrate_catalogs_enabled || kind.is_ready(&self.catalogs) {
            return Ok(());
        }
        let load = self.load_state(kind);
        if matches!(load.ready.get(), Some(Err(_))) {
            return self.reload(kind).await;
        }
        let factory = self.factory.clone();
        let catalogs = self.catalogs.clone();
        let deadline = tokio::time::Instant::now() + self.catalog_timeout;
        let LoadState {
            ready,
            lock,
            last_error,
        } = load;
        ready
            .get_or_init(|| async move {
                let _guard = lock.lock().await;
                let result = kind.load(&factory, &catalogs, deadline).await;
                *crate::realtime::lock_unpoisoned(&last_error) = result.clone().err();
                result
            })
            .await
            .clone()
    }

    /// Why the spot config failed to load, if it did: the
    /// [`Error::CatalogLoad`] that [`Self::wait_for_catalogs`] returns.
    pub fn catalogs_last_error(&self) -> Option<Error> {
        crate::realtime::lock_unpoisoned(&self.catalog_last_error).clone()
    }

    /// Stop all automated and resting trading of `scope`: cancel every live
    /// trigger, then every open order.
    ///
    /// Cancel-all and the dead-man switch cancel open orders, never
    /// triggers: a stop-loss, take-profit or trailing stop that has not fired
    /// stays armed, a TWAP keeps running, and a paused trigger stays paused.
    /// This pages through the triggers in the live statuses (`created`,
    /// `armed`, `running`, `paused`, attached legs included, up to 1000 per
    /// page), cancels each one, and only then cancels all open orders, so an
    /// order a trigger places before its cancel is still canceled.
    ///
    /// A trigger whose cancel fails does not stop the run; it is listed in
    /// [`StopAllTradingResult::trigger_failures`]. An error listing the
    /// triggers returns before anything is canceled; an error of the final
    /// cancel-all is returned as is.
    pub async fn stop_all_trading(
        &self,
        scope: StopAllTradingScope,
    ) -> Result<StopAllTradingResult> {
        let mut trigger_ids = Vec::new();
        let mut seen_tokens = std::collections::HashSet::new();
        let mut page_token = None;
        loop {
            let page = self
                .triggers
                .list_with(ListTriggersOpts {
                    symbol: scope.symbol.clone(),
                    status: ["created", "armed", "running", "paused"]
                        .map(str::to_owned)
                        .to_vec(),
                    limit: Some(STOP_ALL_TRADING_PAGE_LIMIT),
                    page_token: page_token.clone(),
                    subaccount_id: scope.subaccount_id,
                    ..Default::default()
                })
                .await?;
            trigger_ids.extend(page.triggers.into_iter().map(|t| t.trigger_id));
            if page.next_page_token.is_empty() {
                break;
            }
            if !seen_tokens.insert(page.next_page_token.clone()) {
                return Err(Error::response_contract(
                    "ListTriggers pagination",
                    "next_page_token repeats an earlier page_token",
                ));
            }
            page_token = Some(page.next_page_token);
        }

        let mut canceled_trigger_ids = Vec::new();
        let mut trigger_failures = Vec::new();
        for trigger_id in trigger_ids {
            match self
                .triggers
                .cancel_by_id(&trigger_id, scope.subaccount_id)
                .await
            {
                Ok(_) => canceled_trigger_ids.push(trigger_id),
                Err(error) => trigger_failures.push(StopAllTradingFailure { trigger_id, error }),
            }
        }

        let orders = self
            .orders
            .cancel_all_with(CancelAllOpts {
                symbol: scope.symbol,
                subaccount_id: scope.subaccount_id,
                request_id: scope.request_id,
                ..Default::default()
            })
            .await?;
        Ok(StopAllTradingResult {
            canceled_trigger_ids,
            trigger_failures,
            orders,
        })
    }
}

/// One of the two catalogs the client loads when it is created.
#[derive(Clone, Copy)]
enum CatalogKind {
    Spot,
    Funding,
}

impl CatalogKind {
    async fn load(
        self,
        factory: &Factory,
        catalogs: &CatalogManager,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        match self {
            Self::Spot => crate::catalog_startup::load_spot(factory, catalogs, deadline).await,
            Self::Funding => {
                crate::catalog_startup::load_funding(factory, catalogs, deadline).await
            }
        }
    }

    fn is_ready(self, catalogs: &CatalogManager) -> bool {
        match self {
            Self::Spot => catalogs.is_spot_ready(),
            Self::Funding => catalogs.is_funding_ready(),
        }
    }
}

/// A catalog load's shared outcome, lock and last error.
struct LoadState {
    ready: Arc<OnceCell<Result<()>>>,
    lock: Arc<tokio::sync::Mutex<()>>,
    last_error: Arc<std::sync::Mutex<Option<Error>>>,
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
        assert!(cfg.environment.is_none());
    }

    #[test]
    fn client_uses_named_environment_urls() {
        let client = Client::new(Config {
            environment: Some(crate::POLYESTER_TESTNET_ENVIRONMENT.clone()),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .expect("client");
        assert_eq!(client.api_url, crate::POLYESTER_TESTNET_ENVIRONMENT.api_url);
        assert_eq!(client.environment.chain_id, 888169);
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
    fn config_debug_redacts_url_userinfo() {
        let config = Config {
            api_url: "https://fake-user:fake-url-password-marker@api.example.test".into(),
            ws_url: "wss://fake-user:fake-url-password-marker@ws.example.test".into(),
            ..Default::default()
        };
        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!rendered.contains("fake-url-password-marker"), "{rendered}");
            assert!(!rendered.contains("fake-user"), "{rendered}");
            assert!(rendered.contains("api.example.test"), "{rendered}");
            assert!(rendered.contains("ws.example.test"), "{rendered}");
        }
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
    fn client_rejects_remote_plaintext_api_url() {
        let err = match Client::new(Config {
            api_url: "http://api.example.test".into(),
            hydrate_catalogs: false,
            ..Default::default()
        }) {
            Ok(_) => panic!("remote http"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::Validation(_)), "{err}");

        Client::new(Config {
            api_url: "http://api.example.test".into(),
            ws_url: "ws://api.example.test".into(),
            allow_insecure_http: true,
            hydrate_catalogs: false,
            ..Default::default()
        })
        .expect("insecure opt-in");
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
            &client.changelog,
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
