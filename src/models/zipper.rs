//! Zipper deposit/withdraw config models (Go `models/zipper.go` parity).

use buffa_types::google::protobuf::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperTokenConfig {
    pub address: String,
    pub decimals: u32,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ZipperAssetChainVariant {
    pub zipped_asset_id: u32,
    pub chain_id: u32,
    pub is_native_asset: bool,
    pub network_fee: String,
    pub deposit_min_amount: String,
    pub withdraw_min_amount: String,
    /// Latest zToken supply of this variant: `supply_q` at the asset's
    /// `quantity_scale`, such as `97.08173223`. Set only while
    /// `supply_status` is [`SUPPLY_STATUS_AVAILABLE`] or
    /// [`SUPPLY_STATUS_STALE`]; `Some("0")` then is a real supply of zero.
    /// `None` when no supply is known (missing, unspecified or a status this
    /// SDK version does not know), and when the asset's scale is not one a
    /// quantity can be formatted at. `None` never means zero.
    pub supply: Option<String>,
    /// Whether `supply` is known and current: [`SUPPLY_STATUS_AVAILABLE`],
    /// [`SUPPLY_STATUS_STALE`] (the last value read, not refreshed for longer
    /// than expected) or [`SUPPLY_STATUS_MISSING`]. Empty when unspecified,
    /// and `"unknown(n)"` for a status this SDK version does not know; read
    /// both as missing. Use [`Self::supply_is_current`].
    pub supply_status: String,
    /// When `supply` was read from Polyester chain. Set only with `supply`.
    pub supply_observed_at: Option<Timestamp>,
    pub source_token: ZipperTokenConfig,
    pub z_token: ZipperTokenConfig,
    /// Whether this route accepts new deposits: [`ROUTE_STATUS_NORMAL`] or
    /// [`ROUTE_STATUS_SUSPENDED`]. Empty while availability is not known, and
    /// `"unknown(n)"` for a status this SDK version does not know: neither
    /// means the route is available, and neither means it is suspended.
    /// While it is suspended, do not send funds to a deposit address on this
    /// route; wait until it returns to normal. Use
    /// [`Self::accepts_deposits`].
    pub deposit_status: String,
    /// Whether this route accepts new withdrawals, read as `deposit_status`.
    /// A suspension can stop one direction and not the other. A Trading
    /// withdrawal accepted before the route became suspended and not yet
    /// sent stays pending, with its funds held, and is sent once the route
    /// returns to normal; one already being sent when the suspension takes
    /// effect may fail instead, and its funds then return to the account. A
    /// new withdrawal on a suspended route is refused with
    /// `ERROR_CODE_ROUTE_SUSPENDED`. Use [`Self::accepts_withdrawals`].
    pub withdrawal_status: String,
}

/// Route status label: the route accepts new requests.
pub const ROUTE_STATUS_NORMAL: &str = "normal";
/// Route status label: the route does not accept new requests. Do not send
/// funds on it until it returns to [`ROUTE_STATUS_NORMAL`]. No reason is
/// published.
pub const ROUTE_STATUS_SUSPENDED: &str = "suspended";

/// Supply status label: the route's supply was read recently and is
/// current.
pub const SUPPLY_STATUS_AVAILABLE: &str = "available";
/// Supply status label: the route's supply is the last one read, but it has
/// not been refreshed for longer than expected and may no longer be accurate.
/// Do not size a withdrawal against it as if it were current.
pub const SUPPLY_STATUS_STALE: &str = "stale";
/// Supply status label: no supply is known for the route yet. It is not a
/// supply of zero.
pub const SUPPLY_STATUS_MISSING: &str = "missing";

/// Whether a supply status label carries a supply value: available or stale.
pub(crate) fn supply_status_carries_supply(status: &str) -> bool {
    status == SUPPLY_STATUS_AVAILABLE || status == SUPPLY_STATUS_STALE
}

impl ZipperAssetChainVariant {
    /// True only while the route's supply is known and current
    /// ([`SUPPLY_STATUS_AVAILABLE`]). Size a withdrawal against `supply`
    /// only then: stale and missing supplies refuse withdrawals on the route
    /// with `ERROR_CODE_SUPPLY_UNAVAILABLE`.
    pub fn supply_is_current(&self) -> bool {
        self.supply_status == SUPPLY_STATUS_AVAILABLE
    }

    /// True only when the route accepts new deposits
    /// ([`ROUTE_STATUS_NORMAL`]); false while suspended or not known.
    pub fn accepts_deposits(&self) -> bool {
        self.deposit_status == ROUTE_STATUS_NORMAL
    }

    /// True only when the route accepts new withdrawals
    /// ([`ROUTE_STATUS_NORMAL`]); false while suspended or not known.
    pub fn accepts_withdrawals(&self) -> bool {
        self.withdrawal_status == ROUTE_STATUS_NORMAL
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperChainConfig {
    pub chain_id: u32,
    pub code: String,
    pub name: String,
    pub native_chain_id: String,
    pub native_currency_symbol: String,
    pub explorer_url: String,
    pub icon: String,
    pub required_confirmations: u32,
    pub confirmation_time_seconds: u32,
    pub is_case_sensitive: bool,
    pub min_address_length: u32,
    pub max_address_length: u32,
    /// Memo or destination-tag rule for withdrawal destinations on this
    /// network: `"required"`, `"optional"` or `"not_supported"`. Empty when
    /// the network names none, which does not mean a memo can be omitted;
    /// `"UNKNOWN(n)"` for a rule this SDK version does not know. It does not
    /// describe deposit instructions.
    pub memo_requirement: String,
    /// Display label for the memo, such as `"Destination tag"` or `"Memo"`.
    /// Empty when memos are not supported.
    pub memo_label: String,
    /// Human-readable memo format guidance for display, exactly as sent.
    /// Use `memo_requirement` as the rule; do not parse this text. Empty when
    /// no guidance applies.
    pub memo_format_hint: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ZipperAssetConfig {
    pub asset: String,
    pub ledger_id: u32,
    pub name: String,
    pub icon: String,
    pub quantity_scale: u32,
    pub quantity_display_decimals: u32,
    pub u_asset_id: String,
    pub variants: Vec<ZipperAssetChainVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZipperChainContractConfig {
    pub name: String,
    pub address: String,
    #[serde(rename = "type")]
    pub contract_type: String,
    pub description: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DepositWithdrawConfig {
    pub chains: Vec<ZipperChainConfig>,
    pub assets: Vec<ZipperAssetConfig>,
    pub contracts: Vec<ZipperChainContractConfig>,
    pub ts_ms: i64,
}

/// One route's supply from the supply stream, or as the catalogs hold it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ZippedAssetSupplyUpdate {
    pub zipped_asset_id: u32,
    /// The route's zToken supply at the asset's `quantity_scale`, set only
    /// while `status` is [`SUPPLY_STATUS_AVAILABLE`] or
    /// [`SUPPLY_STATUS_STALE`]. `None` means not known, never zero.
    pub supply: Option<String>,
    /// [`SUPPLY_STATUS_AVAILABLE`] or [`SUPPLY_STATUS_STALE`] on the stream:
    /// a route whose supply stops being refreshed gets one stale update
    /// repeating its last value and observation time, and turns available
    /// with its next reading. Labels as
    /// [`ZipperAssetChainVariant::supply_status`].
    pub status: String,
    /// When `supply` was read from Polyester chain. Set only with `supply`.
    pub observed_at: Option<Timestamp>,
}

impl ZippedAssetSupplyUpdate {
    /// True only while the supply is known and current
    /// ([`SUPPLY_STATUS_AVAILABLE`]).
    pub fn supply_is_current(&self) -> bool {
        self.status == SUPPLY_STATUS_AVAILABLE
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ZippedAssetSupplyBatch {
    pub updates: Vec<ZippedAssetSupplyUpdate>,
}
