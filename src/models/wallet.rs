//! Smart account wallet balances (`chain.withdraw.v1.WithdrawService/GetWalletBalances`).

/// The zTokens an account's smart account wallet holds on Polyester Chain,
/// from [`crate::services::WithdrawService::get_wallet_balances`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletBalances {
    /// Smart account wallet whose balances these are.
    pub smart_account_address: String,
    /// Every zToken the wallet holds a nonzero balance of, ordered by
    /// `zipped_asset_id`; empty when it holds none.
    pub balances: Vec<WalletBalance>,
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
