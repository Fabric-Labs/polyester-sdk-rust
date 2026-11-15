//! Dust conversion models (`transfer.v1.DustConversionService`).
//!
//! Every amount is exact: an [`AssetAmount`] at the 18-decimal ledger scale,
//! decoded from the contract's `U128` without a float.

use crate::types::AssetAmount;
use buffa_types::google::protobuf::Timestamp;

/// One available Trading balance judged for conversion into a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DustBalance {
    /// Numeric asset ID.
    pub asset_id: u32,
    /// The whole available balance (what no open order or other reservation
    /// holds), exact at 18 decimals.
    pub amount: AssetAmount,
    /// Whether the balance converts, as a lowercase label: `eligible`,
    /// `target_asset`, `tradable` (sell or spend it instead),
    /// `above_value_limit` (worth more than 10 USD), `no_price`,
    /// `unsupported_asset` or `source_limit` (one conversion takes at most
    /// 20 balances; it waits for the next). Empty when unset, `UNKNOWN(<n>)`
    /// for a value this SDK does not know.
    pub eligibility: String,
    /// The balance's USD value at 18 decimals, rounded down; `None` when the
    /// asset has no current price (never valued at zero).
    pub usd_value: Option<AssetAmount>,
}

/// The account's Trading balances judged for one target, from
/// [`crate::services::DustConversionService::list_balances`]. It reserves
/// nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DustBalances {
    /// Target asset the balances were judged for.
    pub target_asset_id: u32,
    /// Every asset the account can convert dust into, in preference order.
    pub targets: Vec<u32>,
    /// The fee in basis points, taken from the payout.
    pub fee_bps: u32,
    /// Every nonzero available Trading balance, in asset ID order.
    pub balances: Vec<DustBalance>,
    /// The eligible balances' total USD value at 18 decimals, rounded down.
    pub gross_usd: AssetAmount,
    /// The payout they would bring now, in the target asset at 18 decimals,
    /// after the fee.
    pub payout: AssetAmount,
    /// Whether the target can be paid now. When false, a quote is refused
    /// with `ERROR_CODE_DUST_TARGET_UNAVAILABLE`.
    pub target_available: bool,
}

/// The USD index price a quote used for one asset.
#[derive(Debug, Clone, PartialEq)]
pub struct DustAssetPrice {
    /// Numeric asset ID.
    pub asset_id: u32,
    /// The price as sent: USD per whole unit is `price_ticks / 10^price_scale`.
    pub price_ticks: i64,
    /// Decimal scale of `price_ticks`.
    pub price_scale: u32,
    /// USD per whole unit, exact: `price_ticks / 10^price_scale`.
    pub price: rust_decimal::Decimal,
    /// Time of the oldest index observation the price used.
    pub as_of: Option<Timestamp>,
}

/// A dust conversion quote, valid for 30 seconds, from
/// [`crate::services::DustConversionService::quote`].
#[derive(Debug, Clone, PartialEq)]
pub struct DustQuote {
    /// The quote to convert with
    /// [`crate::services::DustConversionService::convert`]; `None` when no
    /// balance is eligible or the payout rounds to zero (`balances` says
    /// why).
    pub quote_id: Option<String>,
    /// After this time a conversion of the quote is refused with
    /// `ERROR_CODE_DUST_QUOTE_EXPIRED`.
    pub expires_at: Option<Timestamp>,
    /// Target asset.
    pub target_asset_id: u32,
    /// The fee in basis points, taken from the payout.
    pub fee_bps: u32,
    /// The sources' total USD value at 18 decimals, rounded down.
    pub gross_usd: AssetAmount,
    /// The payout in the target asset at 18 decimals, after the fee, rounded
    /// down to the target's quantity precision.
    pub payout: AssetAmount,
    /// Every nonzero available Trading balance, sources and others, in asset
    /// ID order.
    pub balances: Vec<DustBalance>,
    /// Prices of the sources and the target, in asset ID order.
    pub prices: Vec<DustAssetPrice>,
}

/// One balance a conversion moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DustSource {
    /// Numeric asset ID.
    pub asset_id: u32,
    /// The whole quoted balance, exact at 18 decimals.
    pub amount: AssetAmount,
}

/// One conversion of the account's dust into a target asset.
#[derive(Debug, Clone, PartialEq)]
pub struct DustConversion {
    /// Stable conversion ID, for
    /// [`crate::services::DustConversionService::get_conversion`]. Never
    /// empty.
    pub conversion_id: String,
    /// The quote the conversion converted.
    pub quote_id: String,
    /// `processing` (the outcome is not known yet: it completes or is
    /// refused without another request; read it again), `completed` (every
    /// source moved and the payout was credited, atomically) or `refused`
    /// (nothing moved; `refusal_code` says why). `UNKNOWN(<n>)` for a state
    /// this SDK does not know.
    pub state: String,
    /// Why a `refused` conversion was refused: the `transfer.v1.ErrorCode`
    /// as a lowercase label without its `ERROR_CODE_` prefix, for example
    /// `dust_price_moved` or `dust_balance_changed`;
    /// `UNKNOWN_ERROR_CODE(<n>)` for a code this SDK does not know. `None`
    /// unless refused.
    pub refusal_code: Option<String>,
    /// Target asset.
    pub target_asset_id: u32,
    /// The sources, in asset ID order.
    pub sources: Vec<DustSource>,
    /// The payout in the target asset at 18 decimals, after the fee: the
    /// sources' value at the index prices when the conversion was created.
    pub payout: AssetAmount,
    /// The payout the quote showed.
    pub quoted_payout: AssetAmount,
    /// The fee taken from the payout.
    pub fee: AssetAmount,
    /// The fee in basis points.
    pub fee_bps: u32,
    /// When the conversion was created.
    pub created_at: Option<Timestamp>,
    /// When it completed; `None` until `completed`.
    pub completed_at: Option<Timestamp>,
}

/// One page of the account's conversions, newest first.
#[derive(Debug, Clone, PartialEq)]
pub struct DustConversionsList {
    pub conversions: Vec<DustConversion>,
    /// Cursor for the next page; empty on the last page.
    pub next_page_token: String,
}
