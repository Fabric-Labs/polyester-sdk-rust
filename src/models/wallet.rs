//! Smart account wallet balances (`chain.withdraw.v1.WithdrawService/GetWalletBalances`).

/// The zTokens an account's smart account wallet holds on Polyester Chain,
/// from [`crate::services::WithdrawService::get_wallet_balances`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletBalances {
    /// Smart account wallet whose balances these are.
    pub smart_account_address: String,
    /// Every zToken the wallet holds a nonzero balance of that could be read,
    /// ordered by `zipped_asset_id`; empty when it holds none. An asset whose
    /// balance could not be read is in `unavailable_assets` instead.
    pub balances: Vec<WalletBalance>,
    /// Zipped assets whose zToken balance could not be read, ordered by
    /// `zipped_asset_id`. The wallet's balance of each is unknown, not zero:
    /// read again later. Empty when every balance was read; when none can be
    /// read at all, the call fails as unavailable instead.
    pub unavailable_assets: Vec<UnavailableWalletAsset>,
}

impl WalletBalances {
    /// Whether every balance was read: `unavailable_assets` is empty. Until
    /// then, an asset missing from `balances` may still be held.
    pub fn is_complete(&self) -> bool {
        self.unavailable_assets.is_empty()
    }
}

/// A zipped asset whose zToken balance could not be read from Polyester
/// Chain: its balance is unknown, not zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableWalletAsset {
    /// Zipped asset of the zToken, as [`WalletBalance::zipped_asset_id`].
    pub zipped_asset_id: u32,
    /// Unified asset the zToken stands for.
    pub unified_asset_id: u32,
    /// External network of the zipped asset.
    pub chain_id: u32,
    /// zToken contract address on Polyester Chain listed for the zipped
    /// asset.
    pub ztoken_address: String,
    /// Why the balance could not be read: `no_contract` (no zToken contract
    /// at the listed address), `read_failed` (Polyester Chain answered the
    /// read with an error) or `invalid_balance` (the read returned a value
    /// that is not a valid balance); empty when no reason was given, and
    /// `unknown(<n>)` for a reason this SDK does not name.
    pub reason: String,
}

/// One zToken a smart account wallet holds, such as the refund of a
/// withdrawal Zipper rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletBalance {
    /// Zipped asset of the zToken: the network-specific asset.
    pub zipped_asset_id: u32,
    /// Unified asset the zToken stands for.
    pub unified_asset_id: u32,
    /// External network of the zipped asset, as a chain of the deposit and
    /// withdraw config names it.
    pub chain_id: u32,
    /// zToken contract address on Polyester Chain.
    pub ztoken_address: String,
    /// Balance as an unsigned integer in 18-decimal units, as a decimal
    /// string: every zToken has 18 decimals.
    pub amount_e18: String,
    /// The same balance in whole units, exact, such as `"12.5"`.
    pub amount: String,
}
