//! Price alerts (`alerts.v1.PriceAlertService`).
//!
//! Available on devnet; not yet routed on testnet, where the calls fail
//! with a route or not-implemented error. Alerts belong to the root account:
//! only an API key scoped to the root account manages them.

use crate::types::Price;
use buffa_types::google::protobuf::Timestamp;

/// Side of the alert price that fires an alert. Both are inclusive:
/// `Above` fires at a price at or above it, `Below` at or below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertDirection {
    Above,
    Below,
}

impl AlertDirection {
    /// `"above"` or `"below"`, as [`PriceAlert::direction`] reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Above => "above",
            Self::Below => "below",
        }
    }
}

/// Price an alert watches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertPriceSource {
    /// The market's last traded price.
    LastPrice,
    /// The market's index price.
    IndexPrice,
}

impl AlertPriceSource {
    /// `"last"` or `"index"`, as [`PriceAlert::price_source`] reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LastPrice => "last",
            Self::IndexPrice => "index",
        }
    }
}

/// Where a fired alert is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertChannel {
    /// The in-app inbox.
    InApp,
    /// The Telegram account linked to the Polyester account.
    Telegram,
}

impl AlertChannel {
    /// `"in_app"` or `"telegram"`, as [`PriceAlert::channels`] reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InApp => "in_app",
            Self::Telegram => "telegram",
        }
    }
}

/// Status filter of [`ListAlertsOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertStatus {
    Active,
    Triggered,
    /// The contract's spelling, unlike the triggers' `canceled`.
    Cancelled,
}

impl AlertStatus {
    /// `"active"`, `"triggered"` or `"cancelled"`, as [`PriceAlert::status`]
    /// reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Triggered => "triggered",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A price alert: a notification when a market's price crosses a level.
#[derive(Debug, Clone, PartialEq)]
pub struct PriceAlert {
    /// Public alert ID.
    pub alert_id: String,
    pub symbol_id: u32,
    /// Market name from the loaded catalogs; empty when they don't know it.
    pub symbol: String,
    /// `"above"` or `"below"`; `"unspecified"` for a value this SDK version
    /// does not know.
    pub direction: String,
    /// The price the alert watches, `"last"` or `"index"`, always stated by
    /// the server; `"unspecified"` for a value this SDK version does not know.
    pub price_source: String,
    /// Alert price.
    pub price: Price,
    /// The watched price when the alert was created or last set.
    pub reference_price: Price,
    /// Whether the alert fires on the next crossing. An alert whose
    /// condition already held when it was set starts unarmed and fires only
    /// after the price moves away and crosses back.
    pub armed: bool,
    /// `"active"`, `"triggered"` or `"cancelled"`; `"unspecified"` for a value
    /// this SDK version does not know.
    pub status: String,
    /// `"in_app"` and/or `"telegram"`; `"unspecified"` for a value this SDK
    /// version does not know.
    pub channels: Vec<String>,
    /// Revision, the `expected_revision` of the next update.
    pub revision: u64,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    /// When the alert fired; `None` unless it fired.
    pub triggered_at: Option<Timestamp>,
    /// Price that fired the alert; `None` unless it fired.
    pub triggered_price: Option<Price>,
}

/// One page of [`crate::services::AlertsService::list`].
#[derive(Debug, Clone, PartialEq)]
pub struct PriceAlertsPage {
    /// Newest first.
    pub alerts: Vec<PriceAlert>,
    /// Pass back as [`ListAlertsOptions::page_token`] with the same filters;
    /// empty on the last page.
    pub next_page_token: String,
}

/// Result of [`crate::services::AlertsService::update`].
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAlertResult {
    /// The alert as it is now.
    pub alert: PriceAlert,
    /// What the call did: `"applied"` (it changed the alert), `"unchanged"`
    /// (the alert already held the requested values) or `"replayed"` (the
    /// idempotency key was already used for this request, so nothing was
    /// applied now); `"unspecified"` for a value this SDK version does not
    /// know. Tell a retry from a fresh change by this, never by revision.
    pub outcome: String,
}

/// Inputs of [`crate::services::AlertsService::create`].
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAlertParams {
    /// Market name, such as `BTC-USDT`. Leave it empty and set
    /// [`symbol_id`](Self::symbol_id) to name the market by its ID instead.
    pub symbol: String,
    /// Connect market ID. With an empty `symbol` it names the market; with
    /// both set they must name the same market.
    pub symbol_id: Option<u32>,
    pub direction: AlertDirection,
    /// Alert price; must be positive. A price bound to a market must be
    /// bound to this alert's market.
    pub price: Price,
    /// `None` watches the index price where the market has one, and the last
    /// traded price otherwise.
    pub price_source: Option<AlertPriceSource>,
    /// One or two distinct channels.
    pub channels: Vec<AlertChannel>,
    /// Optional key, 1 to 128 of `A-Z a-z 0-9 . _ : -`, per caller: a retry
    /// with the same key and request returns the alert it created.
    pub idempotency_key: Option<String>,
}

impl CreateAlertParams {
    /// An alert on the market named `symbol`.
    pub fn new(
        symbol: impl Into<String>,
        direction: AlertDirection,
        price: Price,
        channels: Vec<AlertChannel>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            symbol_id: None,
            direction,
            price,
            price_source: None,
            channels,
            idempotency_key: None,
        }
    }

    /// [`Self::new`] for the market with Connect ID `symbol_id`.
    pub fn for_symbol_id(
        symbol_id: u32,
        direction: AlertDirection,
        price: Price,
        channels: Vec<AlertChannel>,
    ) -> Self {
        let mut params = Self::new(String::new(), direction, price, channels);
        params.symbol_id = Some(symbol_id);
        params
    }

    /// Watch `source` instead of the server's default.
    pub fn with_price_source(mut self, source: AlertPriceSource) -> Self {
        self.price_source = Some(source);
        self
    }

    /// Send `key` so a retry creates the alert at most once.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// Inputs of [`crate::services::AlertsService::update`]. The fields set are
/// the ones changed; with none set the update changes no field, which sets a
/// fired (`triggered`) alert again and leaves an active one unchanged.
///
/// Fields may be added in minor releases, so code outside this crate builds it
/// with a constructor and sets fields afterwards instead of a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAlertParams {
    /// Public alert ID (base58, or decimal).
    pub alert_id: String,
    /// The alert's current `revision`; a stale one is refused with
    /// `REVISION_CONFLICT`: read the alert again.
    pub expected_revision: u64,
    pub direction: Option<AlertDirection>,
    /// New alert price; must be positive. Not checked against the alert's
    /// market, which the update does not name.
    pub price: Option<Price>,
    /// New channels: one or two distinct ones.
    pub channels: Option<Vec<AlertChannel>>,
    /// Optional key, 1 to 128 of `A-Z a-z 0-9 . _ : -`: one per intended
    /// change, the same for every retry of it. A repeat returns the alert as
    /// it is now with outcome `"replayed"` and applies nothing again.
    pub idempotency_key: Option<String>,
}

impl UpdateAlertParams {
    /// An update of `alert_id` at `expected_revision` that changes nothing
    /// yet; set the fields to change.
    pub fn new(alert_id: impl Into<String>, expected_revision: u64) -> Self {
        Self {
            alert_id: alert_id.into(),
            expected_revision,
            direction: None,
            price: None,
            channels: None,
            idempotency_key: None,
        }
    }

    pub fn with_direction(mut self, direction: AlertDirection) -> Self {
        self.direction = Some(direction);
        self
    }

    /// Move the alert to `price`, which must be unbound
    /// (`Price::from_decimal_str(.., None)`, `Price::from_ticks(.., None)`):
    /// the update names the alert, not its market, so a bound price is
    /// refused before sending.
    pub fn with_price(mut self, price: Price) -> Self {
        self.price = Some(price);
        self
    }

    pub fn with_channels(mut self, channels: Vec<AlertChannel>) -> Self {
        self.channels = Some(channels);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// Filters of [`crate::services::AlertsService::list`] and
/// [`crate::services::AlertsService::all`].
///
/// Fields may be added in minor releases: start from `Default::default()`
/// and set fields afterwards.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListAlertsOptions {
    /// Keep only alerts in these statuses, at most 3 distinct ones; empty
    /// keeps all.
    pub statuses: Vec<AlertStatus>,
    /// Keep only alerts on this market, by name.
    pub symbol: Option<String>,
    /// Keep only alerts on this market, by Connect ID.
    pub symbol_id: Option<u32>,
    /// Page size; `None` uses the server default of 50 (at most 200).
    pub limit: Option<u32>,
    /// Continue from a previous page's `next_page_token`, with the same
    /// filters.
    pub page_token: Option<String>,
}
