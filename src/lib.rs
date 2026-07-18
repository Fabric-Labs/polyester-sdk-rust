//! Official Rust SDK for Polyester APIs.
//!
//! Built on [Connect for Rust](https://github.com/connectrpc/connect-rust) and
//! Polyester's published Protobuf contracts (Buffa + Connect codegen).
//!
//! # Quick start
//!
//! ```rust,no_run
//! use polyester::{Client, Config};
//! use polyester::types::{Price, Quantity};
//!
//! #[tokio::main]
//! async fn main() -> polyester::Result<()> {
//!     let client = Client::new(Config {
//!         api_key_id: Some("ak_...".into()),
//!         api_private_key: Some("...".into()),
//!         default_account_id: Some("...".into()),
//!         ..Default::default()
//!     })?;
//!     let _ = client.auth.me().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Qty / price
//!
//! Order write APIs take [`types::Price`] / [`types::Quantity`] only:
//! - Humans: `Price::from_decimal_str("1.5")` / `Quantity::from_decimal_str("0.01", scale, …)`
//! - Bots: `Price::from_ticks(1_500_000)` / `Quantity::from_scaled(…)`
//!
//! Floats and bare integers are rejected.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::module_inception)]

/// Generated Buffa message types (`crate::proto` for Connect stubs).
pub mod proto;

/// Generated ConnectRPC service clients and server traits.
#[path = "connect_gen/mod.rs"]
pub mod connect;

pub mod auth;
pub mod catalogs;
pub mod client;
pub mod codecs;
pub mod errors;
pub mod marketoverview;
pub mod models;
pub mod orderbook;
pub mod services;
pub mod transport;
pub mod types;

#[cfg(feature = "realtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "realtime")))]
pub mod realtime;

pub use client::{Client, Config};
pub use errors::{Error, Result};
pub use types::{
    AssetAmount, Price, PriceTicks, QtyScaled, Quantity, QuantityDomain,
    resolve_asset_amount_scaled, resolve_price_ticks, resolve_qty_scaled,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
