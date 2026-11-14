//! Server build version and status events (`system.v1.SystemService`).
//!
//! Status event enum values are lowercase labels: `""` when unset and
//! `unknown(<n>)` for a value newer than this SDK.

use buffa_types::google::protobuf::Timestamp;

/// The exact build an environment serves, from
/// [`crate::Client::get_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    /// Version of the build: `MAJOR.MINOR.PATCH` for a release, such as
    /// `"0.10.0"`; a build made after a release appends the number of source
    /// revisions since it and `g` with the first 12 hexadecimal digits of its
    /// own revision, such as `"0.10.0-12-g4e21a2f09670"`. It is not a semantic
    /// version: compare [`ServerVersion::release`] instead of parsing it.
    pub version: String,
    /// Full 40-character lowercase hexadecimal source revision of the build.
    pub commit: String,
    /// Environment that answered, such as `"devnet"`, `"testnet"`,
    /// `"mainnet"` or `"local"`. Kept as the server sends it, so a value this
    /// SDK version does not know is not refused.
    pub environment: String,
}

impl ServerVersion {
    /// The release the build follows: [`ServerVersion::version`] up to its
    /// first `-`, such as `"0.10.0"` for `"0.10.0-12-g4e21a2f09670"`.
    pub fn release(&self) -> &str {
        self.version
            .split_once('-')
            .map_or(self.version.as_str(), |(release, _)| release)
    }
}

/// Status event kind labels.
///
/// Planned maintenance, announced ahead of its window.
pub const STATUS_EVENT_KIND_MAINTENANCE: &str = "maintenance";
/// Unplanned disruption.
pub const STATUS_EVENT_KIND_INCIDENT: &str = "incident";

/// Status event state labels: where an event stands at the time of the
/// response. It follows the event's window, except a cancelled event.
///
/// The window has not started.
pub const STATUS_EVENT_STATE_SCHEDULED: &str = "scheduled";
/// The window has started and not ended.
pub const STATUS_EVENT_STATE_IN_PROGRESS: &str = "in_progress";
/// The window has ended.
pub const STATUS_EVENT_STATE_COMPLETED: &str = "completed";
/// Called off; its window no longer applies.
pub const STATUS_EVENT_STATE_CANCELLED: &str = "cancelled";

/// Incident impact labels. Maintenance, and an incident not yet assessed,
/// read `""`.
///
/// Working, but slower or less reliable than usual.
pub const STATUS_EVENT_IMPACT_DEGRADED: &str = "degraded";
/// Unavailable for some users, markets or assets.
pub const STATUS_EVENT_IMPACT_PARTIAL_OUTAGE: &str = "partial_outage";
/// Unavailable.
pub const STATUS_EVENT_IMPACT_MAJOR_OUTAGE: &str = "major_outage";

/// Affected service labels.
///
/// Placing, changing and cancelling orders, and order execution.
pub const STATUS_EVENT_SERVICE_TRADING: &str = "trading";
/// Prices, trades, candles and order books.
pub const STATUS_EVENT_SERVICE_MARKET_DATA: &str = "market_data";
/// Deposits.
pub const STATUS_EVENT_SERVICE_DEPOSITS: &str = "deposits";
/// Withdrawals.
pub const STATUS_EVENT_SERVICE_WITHDRAWALS: &str = "withdrawals";
/// Transfers between accounts and sub-accounts.
pub const STATUS_EVENT_SERVICE_TRANSFERS: &str = "transfers";
/// Sign-in, account management and API keys.
pub const STATUS_EVENT_SERVICE_ACCOUNTS: &str = "accounts";
/// The REST and WebSocket interfaces for programmatic clients.
pub const STATUS_EVENT_SERVICE_API: &str = "api";
/// QuickSwap swaps.
pub const STATUS_EVENT_SERVICE_QUICKSWAP: &str = "quickswap";
/// The web app.
pub const STATUS_EVENT_SERVICE_APP: &str = "app";

/// One progress note on a status event. Notes are never edited or removed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatusEventUpdate {
    /// Position of the note on its event: 1 for the first, then increasing
    /// by 1.
    pub id: u32,
    /// Plain-text note for users.
    pub message: String,
    /// When the note was posted.
    pub created_at: Option<Timestamp>,
}

/// One scheduled maintenance or incident, from
/// [`crate::Client::list_status_events`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatusEvent {
    /// Stable public identifier in base58; it never changes when the event
    /// is updated. Match events across pages and polls by it.
    pub id: String,
    /// A `STATUS_EVENT_KIND_*` label.
    pub kind: String,
    /// A `STATUS_EVENT_STATE_*` label, at the time of the response.
    pub state: String,
    /// Short title.
    pub title: String,
    /// Plain-text summary for users. It can be revised; progress notes are
    /// in `updates`.
    pub message: String,
    /// Start of the window. It no longer changes once the window has
    /// started.
    pub start_time: Option<Timestamp>,
    /// End of the window: always set for maintenance; `None` for an incident
    /// with no expected end yet.
    pub end_time: Option<Timestamp>,
    /// `STATUS_EVENT_SERVICE_*` labels of the affected parts of the
    /// platform.
    pub services: Vec<String>,
    /// Symbols of the affected markets, such as `"BTC-USDT"`, ascending;
    /// empty when the event is not limited to particular markets.
    pub markets: Vec<String>,
    /// Time of the latest change to the event, a new progress note included.
    /// Its state moving with the clock is not a change.
    pub updated_at: Option<Timestamp>,
    /// A `STATUS_EVENT_IMPACT_*` label for an incident; `""` for maintenance
    /// and for an incident not yet assessed.
    pub impact: String,
    /// Codes of the affected assets, such as `"SOL"`, ascending; empty when
    /// the event is not limited to particular assets.
    pub assets: Vec<String>,
    /// When the event was cancelled; set only when `state` is
    /// [`STATUS_EVENT_STATE_CANCELLED`].
    pub cancelled_at: Option<Timestamp>,
    /// Progress notes, oldest first.
    pub updates: Vec<StatusEventUpdate>,
    /// HTTPS link to a long-form announcement; `None` when the event has
    /// none.
    pub details_url: Option<String>,
}

/// One page of status events, latest start first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatusEventsPage {
    pub events: Vec<StatusEvent>,
    /// Cursor for the next page; empty when no more results exist. Send it
    /// back with the same `states` and `kinds`.
    pub next_page_token: String,
}
