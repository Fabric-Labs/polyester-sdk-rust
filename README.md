# Polyester Rust SDK

Official Rust SDK for Polyester APIs — parity with `polyester-sdk-go` and
`polyester-sdk-python`, built on [Connect for Rust](https://github.com/connectrpc/connect-rust)
(Buffa + Connect **0.8.x**) and the checked-in `src/gen/` protobuf bundle.

**Status:** Alpha (`0.1.0-alpha.3`, git tag `v0.1.0a3`). Proprietary license (not open source).
API-key only — no browser login or JWT flows.

Full cross-language comparison:
[SDK capability matrix](https://polyester.ai/docs/developer-docs/getting-started/sdk-capability-matrix).

**MSRV:** Rust 1.88+

## Install

```toml
[dependencies]
polyester-sdk = "0.1.0-alpha.3"
```

Git install (if you prefer pinning a tag before crates.io mirrors):

```toml
[dependencies]
polyester-sdk = { git = "https://github.com/Fabric-Labs/polyester-sdk-rust", tag = "v0.1.0a3" }
```

```rust
use polyester::{Client, Config, Price, Quantity};
```

Pin the Connect runtime: this crate depends on `connectrpc` / `buffa` **0.8.x**.
Review upstream notes before upgrading.

## Quick start

```rust,no_run
use polyester::{Client, Config, Result};
use polyester::models::{CreateOrderType, CreateSide};
use polyester::types::{Price, Quantity};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(Config {
        api_key_id: Some("ak_...".into()),
        api_private_key: Some("/* 64-char hex seed */".into()),
        default_account_id: Some("/* Profile Account ID */".into()),
        ..Default::default()
    })?;

    let _me = client.auth.me().await?;
    let _spot = client.market_data.get_spot_config().await?;

    // Humans: decimal strings / Decimal
    let price = Price::from_decimal_str("1.5", None)?;
    let qty = Quantity::from_decimal_str("0.01", 8, None, None)?;

    // Bots: scaled ints (no string round-trip)
    let _price2 = Price::from_ticks(1_500_000, None)?;
    let _qty2 = Quantity::from_scaled(1_000_000, Some(8), Default::default(), None, None)?;

    let params = client.orders.create_params(
        "BTC-USDT",
        CreateSide::Buy,
        CreateOrderType::Limit,
        qty,
        Some(price),
    );
    let _ = client.orders.create(params).await?;
    Ok(())
}
```

Or load credentials from the environment:

```bash
export POLYESTER_API_KEY_ID=ak_...
export POLYESTER_API_PRIVATE_KEY=...
export POLYESTER_ACCOUNT_ID=...
```

```rust,no_run
let client = polyester::Client::from_env()?;
```

## Qty / price (POLY-3262)

Public order/trigger write paths take **`Price` / `Quantity` wrappers only**:

| Audience | Constructor |
|---|---|
| Humans / demos | `Price::from_decimal_str` / `Quantity::from_decimal_str` (or `from_decimal`) |
| Bots / MMs | `Price::from_ticks` / `Quantity::from_scaled` |

- **Reject floats** (`f32`/`f64`) — they are not accepted on these APIs.
- **Reject bare integers** on public order APIs — use the named constructors.
- **Reject excess fractional digits** on decimal→scaled conversion (no silent floor).
- Price ticks are fixed **1e6**; qty scale comes from pair `base_quantity_scale` (catalog).
- Transfer/withdraw amounts use the separate **`AssetAmount`** type, so order
  quantities cannot be passed as ledger amounts.

## Catalog readiness

`Config::hydrate_catalogs` defaults to `true`. When constructed inside a Tokio
runtime, the client starts best-effort spot and zipper catalog hydration in the
background. Await readiness before decimal writes that depend on catalog scales:

```rust,no_run
let client = polyester::Client::from_env()?;
client.wait_for_catalogs().await?;
```

If a client is constructed before entering a Tokio runtime,
`wait_for_catalogs()` starts hydration on the current runtime. Scaled bot inputs
(`Price::from_ticks`, `Quantity::from_scaled`, `AssetAmount::from_scaled`) do not
need catalog lookup solely to scale the value.

Realtime subscription handles stop their background tasks when explicitly
closed or dropped. Call `close()` when prompt shutdown matters; `Drop` provides
the cleanup safety net.

## Layout

| Path | Role |
|---|---|
| `src/gen/buffa`, `src/gen/connect` | Checked-in Buffa + Connect codegen (Yvan / monorepo sync) |
| `src/proto`, `src/connect_gen` | Module tree mounting gen as `crate::proto` / `crate::connect` |
| `src/auth`, `src/transport` | Ed25519 API-key signing + Connect `HttpClient` |
| `src/types`, `src/codecs` | `Price` / `Quantity` / scalars |
| `src/services`, `src/client` | Ergonomic async `Client` surface |
| `src/realtime` | WebSocket subscriptions (`realtime` feature, default on) |
| `src/catalogs`, `src/orderbook` | Catalog cache + local book helpers |

Proto stubs are updated when a new `src/gen/` bundle is landed. Day-to-day SDK
work does not require local `buf` generation. After replacing gen files, run:

```bash
python3 scripts/gen_module_tree.py
```

## Auth signing

Authenticated Connect calls sign the **exact protobuf body bytes** with:

```text
timestamp_ms
METHOD
pathname
canonical_query
hex(sha256(body))
```

Headers: `X-API-KEY-ID`, `X-API-TIMESTAMP`, `X-API-SIGNATURE`.

## Development

```bash
source "$HOME/.cargo/env"   # if cargo is not on PATH yet
cargo check
cargo test --lib --all-features
cargo test --test integration --all-features
cargo clippy --all-targets -- -D warnings
```

Live integration tests under `tests/integration/` need
`POLYESTER_API_KEY_ID` / `POLYESTER_API_PRIVATE_KEY` (and usually
`POLYESTER_ACCOUNT_ID`). Without those env vars they soft-skip.

Optional tiers (same gates as Go/Python):

| Env | Enables |
|---|---|
| `POLYESTER_TEST_MUTATION=1` | Order/trigger/market-order write round-trips |
| `POLYESTER_TEST_FUNDED=1` | Balance-changing transfers / fills |
| `POLYESTER_TEST_TRADE_E2E=1` + `POLYESTER_TEST_MAKER_*` | Maker+taker fill e2e |
| `POLYESTER_TEST_INTERNAL_TRANSFER_DEST` | Internal / unified→user transfers |

With a local `.env`, `dotenvy` loads it automatically (`.env` is gitignored).

CI rejects private `ledger.write` symbols in public gen (same gate as Go/Python).

CI also requires every public Connect RPC in gen to be wrapped or listed in
`sdk-coverage.toml`. Contributors: `python3 scripts/check_sdk_coverage.py`.

## Examples

Runnable cookbook examples live in the sibling repo
[`polyester-examples-rust`](https://github.com/Fabric-Labs/polyester-examples-rust)
(REST market data, realtime streams, decimal + scaled-int order paths, batch create, RSI bot).

## License

Proprietary — see [LICENSE](LICENSE).
