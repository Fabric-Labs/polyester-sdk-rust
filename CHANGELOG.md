# Changelog

## Unreleased

## 0.1.0a20

Package version: `0.1.0-alpha.20`. Git tag: `v0.1.0a20`.

### Breaking
- Order identity for `get` / `cancel_with` / `modify` / `wait_for_order_trades_complete` and batch cancel/modify items is now a typed `models::OrderKey` (`OrderId` / `ClientOrderId`) instead of dual optional `order_id` / `client_order_id` fields. Convenience helpers `cancel_by_order_id` and `cancel_by_client_order_id` remain as thin wrappers.

### Fixed
- Reject market creates that also supply a limit `price`.
- Decimal price parsing stays exact (no float intermediate).
- TWAP trigger projection coverage for proto decode paths.

## 0.1.0a19

Package version: `0.1.0-alpha.19`. Git tag: `v0.1.0a19`.

### Fixed
- Outbound Connect and realtime-token HTTP requests now send an explicit `User-Agent: polyester-sdk-rust/<version>` instead of relying on hyper's accidental omission of the header, so edge WAF rules that ban browser signatures (Cloudflare error 1010) cannot silently break every Rust client.
- Cloudflare error 1010 responses are mapped to `Error::Transport` with an explicit WAF message instead of being misclassified as auth / permission failures.
- Concurrent identical authenticated balance reads soft-skip like the sibling balances probe when the Balances scope is unavailable, instead of panicking the live suite for a fixture gap.

### Breaking
- Public orderbook snapshot decoders now return `Result` so malformed levels cannot be represented with missing price or quantity fields.
- `Price` and `Quantity` metadata is now immutable after validated construction; replace public field reads with `symbol()`, `scale()`, `domain()`, and `symbol_id()` getters.

### Fixed
- Managed orderbooks reject malformed levels and invalid sequence ranges atomically, keep the prior sequence/book, and request a snapshot refresh.
- Snapshot depth `1` and `1000` requests now use the matching protocol variants.
- Singular cancel and lookup by client-order-id validate the documented identifier constraints before contacting the transport.

### Testing
- The 10k identical-sign runtime-safety probe joins in chunks so CPU-bound Ed25519 work cannot starve the current-thread ticker for the whole burst.

## 0.1.0a18

Package version: `0.1.0-alpha.18`. Git tag: `v0.1.0a18`.

### Breaking
- Public `cancel_all_from_proto` and `cancel_all_after_from_proto` codecs now return `Result` so callers must handle malformed success responses.

### Fixed
- Catalog `RwLock` reads and writes recover from poisoning instead of panicking on write or treating a poisoned lock as a missing symbol/scale on read.
- `client_order_id`, `new_client_order_id`, `client_trigger_id`, and caller-supplied `request_id` values are validated locally (ASCII charset; 1-36 / 1-64 length) and rejected with `Error::Validation` before the request is sent.
- Local orderbook bucketing uses checked multiply/add and rejects negative prices/quantities instead of overflowing or emitting levels with missing fields.
- `CancelAll` / `CancelAllAfter` response decoding rejects empty or unknown statuses (`submitted`/`dry_run`, `armed`/`disabled`) instead of returning `Ok` for ambiguous success payloads.

### Testing
- Catalog unit coverage poisons the manager lock and asserts hydrated scale/identity lookups and subsequent hydrates still succeed.
- Hardening coverage asserts invalid correlation ids fail closed as `Error::Validation` without contacting Connect.
- Signing capacity unit coverage no longer races the wall clock between seed and allocation.
- Hardening coverage asserts empty/unknown cancel-all and cancel-all-after statuses fail closed through the public service.

## 0.1.0a17

Package version: `0.1.0-alpha.17`. Git tag: `v0.1.0a17`.

### Breaking
- `CreateOrderParams.client_order_id` is now `Option<String>` (API-optional). Pass `None` for one-shot creates; set a stable value when you may retry after an ambiguous failure. `OrdersService::create_params` takes `Option<&str>`. Create-order response decoding no longer requires a non-empty echoed client id.
- Trigger creation still requires a stable client trigger id. Order mutation `request_id` values (`modify`, batch create/cancel/modify, `cancel_all`, `cancel_all_after`) are generated when omitted (TypeScript/Go/Python parity) instead of being regenerated from wall-clock time or rejected; provide a stable value when retrying - a blind retry that omits `request_id` mints a new id.
- `CreateOrderParams` exposes fee source, self-trade prevention, and market slippage controls.
- `get_current_candle` returns `Option<Candle>` when no row exists; orderbook bucket parsing and updates return validation errors for invalid increments.
- `CancelAllAfterResult.effective_timeout_sec` is `u32`, preserving the full wire range.

### Fixed
- Order mutation `request_id` handling matches TypeScript/Go/Python: generate when omitted for `modify`, batch create/cancel/modify, `cancel_all`, and `cancel_all_after` (fixes the broken convenience `cancel_all` path that always failed validation).
- `format_id(0)` now returns the canonical base58 zero (`"1"`) instead of aliasing id `1` as `"2"`; Rust now preserves distinct zero/one round-trips and matches Python/TypeScript encoding.
- Digit-only canonical base58 default subaccount IDs no longer resolve as decimal IDs.
- Managed overview/orderbook overflow closes the consumer channel and underlying stream, delivers the error callback, and remains explicitly closeable.
- Singular order, trigger, withdrawal, and internal-transfer responses reject empty/default success payloads.
- Internal transfers require exactly one destination and a non-empty idempotency key; trigger strategies validate required and mutually exclusive fields.
- Ask buckets round up while bid buckets round down, preserving executable spread semantics.
- Independently constructed credentials for one key share a process allocator; one API key per process is documented because the protocol has no cross-process nonce.

### Testing
- Market roundtrip waits for reserved-balance reconciliation (ledger lag) before asserting no residual holds.
- Added public Connect wire coverage for digit-only subaccount scope and malformed singular mutation responses.
- Added socket-backed managed-overflow coverage for receiver termination, callback delivery, task cancellation, and connection cleanup.

## 0.1.0a16

Package version: `0.1.0-alpha.16`. Git tag: `v0.1.0a16`.

### Breaking
- `UserTrade` adds `fee_source` and `referral_share_scaled`. Consumers using exhaustive struct literals must initialize the new fields (or use `..` where appropriate). Use `fee_source == "received"` to subtract base-denominated BUY fees when calculating net sellable quantity.
- Batch create/cancel counters and cancel-all counters change from `i32` to `u32`; batch-modify counters are also `u32`. Remove signed casts and update explicitly typed variables.
- `BalanceHistory.points` and `EquityHistory.points` change from `i32` to `u32`, matching the protobuf fields and preserving their complete range.
- Response-integrity decoders for batch cancel/modify, address-book mutations, deposit-address creation, and singular lifecycle lookups now return `Result` and reject malformed responses. Direct codec consumers must propagate or handle the error; high-level service methods already do this.

### Fixed
- Spot-config JSON restores `baseQuantityScale` under the canonical proto-JSON key so consumers can re-deserialize `GetSpotConfigResponse` without a duplicate-field error (regression from a15).
- Concurrent identical requests receive unique authentication timestamps across cloned credentials. Async SDK calls queue timestamp allocation without blocking Tokio threads, cap future skew at five seconds, and return a retryable capacity error if the bounded wait is exhausted. Direct synchronous signing returns the same error immediately instead of sleeping.
- Batch create, modify, and cancel responses reconcile aggregate counts against per-item outcomes and reject unknown/ambiguous result states.
- Columnar candles reject misaligned OHLCV arrays instead of emitting empty fields.
- Address-book mutations, deposit-address creation, and singular lifecycle lookups reject missing required entities instead of returning placeholder models.
- Public batch and cancel-all counters preserve their unsigned protobuf range.
- User trades expose fee source and referral share, so received-asset fees can be distinguished from quote fees and BUY net quantity can be calculated correctly.
- Catalog error state recovers from a poisoned mutex instead of panicking.
- Balance and equity history point counts preserve the protobuf `u32` range.

### Testing
- Public-service Connect fault injection covers inconsistent batch counts, misaligned candle columns, and missing required entities in addition to decoder-level boundary tests.
- The funded BUY-to-SELL acceptance test waits for complete fill projection and sells net received base quantity after received-asset fees.
- State-changing live integration tests share a process-wide guard, preventing concurrent tests on one QA account from consuming each other's balances or corrupting reconciliation snapshots.
- Legacy one-way market BUY and SELL probes are ignored in the release suite; the self-contained net-quantity BUY-to-SELL roundtrip provides the same live mutation coverage without leaving a position behind.
- A 10,000-identical-request current-thread Tokio regression asserts unique bounded signatures while independent timers continue to tick.

## 0.1.0a15

Package version: `0.1.0-alpha.15`. Git tag: `v0.1.0a15`.

### Breaking
- `BalanceHistorySeries.balance_q` is now `Vec<u64>`, matching the protobuf wire type and preserving values above `i64::MAX`.
- `BalanceHistorySeries.account_code` is now `i32`, preserving unknown negative protobuf enum values instead of wrapping them into large unsigned values.
- Trading withdrawals now require an explicit non-empty `idempotency_key` and non-zero `nonce`; retrying never creates a new request identity implicitly.
- API request signing returns an error for unusable clocks or malformed absolute URLs.
- Service-owned `connect_client()` escape hatches are no longer public. Use the high-level service methods, which apply request signing, or construct an explicitly low-level generated client from `polyester::connect`.

### Features
- `TypedSubscription::recv_result` and `set_on_error` make terminal realtime failures directly observable.
- Errors expose `is_retryable`, `mutation_outcome_unknown`, and `retry_after`; withdrawal helpers generate cryptographically random keys/nonces.

### Fixed
- Batch-create decoding rejects missing outcomes and inconsistent aggregate counts; unknown rejection enum values retain their numeric code.
- Realtime reconnects use capped exponential backoff with per-subscription jitter.
- `SnapshotThenStream::start` cannot miss transient initial readiness and now obeys the configured startup deadline.
- Signing timestamps no longer drift without a future-skew bound under large bursts.
- Catalog hydration rejects conflicting identities atomically; scale-dependent market data, orderbooks, and Zipper supply fail closed instead of guessing a scale. Valid proto3 scale `0` values survive protobuf-to-JSON conversion.
- REST and realtime public market trades carry catalog quantity-scale metadata.
- Unknown enum values are preserved as `UNKNOWN(n)` rather than collapsing to an empty string.
- Removed the dead per-service `authenticated` transport flag and duplicate Connect configuration; authentication is enforced only where it actually occurs, in signed high-level service calls.
- Realtime publication decoders reject empty and oversized payloads before protobuf conversion.

### Testing
- All 20 publication decoders used by the 22 typed subscription APIs are exercised against malformed lengths/tags, 4,096 deterministic mutation cases, and oversized payloads. A local WebSocket fault-injection test verifies every decoder error terminates the feed and reaches `recv_result()`.
- Added a `cargo-fuzz` target covering the same decoder surface for coverage-guided malformed-protobuf campaigns.
- Integration tests no longer trip `clippy::uninlined_format_args` under `-D warnings`.

## 0.1.0a14

Package version: `0.1.0-alpha.14`. Git tag: `v0.1.0a14`.

### Fixed
- ConnectRPC responses are capped at 4 MiB explicitly, including catalog hydration.
- The funded market roundtrip can use external order-book liquidity when dedicated maker credentials are unavailable and cleans up only its own client order IDs.

### Testing
- Hardening coverage now injects corrupt protobuf catalog responses and slow-drip token/JSON-RPC bodies.
- Tests that use non-dry-run `cancel_all` require an explicit dedicated-account cleanup gate.

## 0.1.0a13

Package version: `0.1.0-alpha.13`. Git tag: `v0.1.0a13`.

### Breaking
- Realtime HTTP 401/403 token responses map to structured `Error::PermissionDenied { message, status, code, context, endpoint }` (richer than the a12 Auth mapping).

### Fixed
- Realtime WebSocket messages, frames, and protobuf record/field lengths are capped at 8 MiB before publication decoding.
- `SnapshotThenStream` tracks public handles independently from background `Arc` references; dropping the last handle now stops the coordinator, and close interrupts reconnect delays and in-flight snapshot retries.

### Testing
- Hardening L2 covers oversized realtime messages and combined reconnect/retry/cancellation fault injection.
- Live/smoke helpers exclusively use `POLYESTER_TEST_TRADE_SYMBOL` (legacy smoke-symbol fallbacks removed).

## 0.1.0a12

Package version: `0.1.0-alpha.12`. Git tag: `v0.1.0a12`.

### Breaking
- `wait_for_catalogs` / `hydrate_catalogs` return `Err` when spot/zipper hydration fails or catalogs are unusable (was Ok-after-fail). Use `catalogs_last_error()` to inspect.
- `format_qty_scaled` / `format_ledger_u64` return `Result<String>` and reject scales above `MAX_PROTOCOL_SCALE` (36) instead of panicking on pathological `format!` widths.
- Catalog hydrate rejects oversized IDs/scales (no silent `as u32` truncation) and scales > 36.
- Realtime HTTP 403 token responses map to `Error::Auth` (status, label, truncated body) instead of opaque `Error::Realtime("… HTTP 403")`.
- Candle decode (`candles_from_proto` / `candles_columns_from_proto` / realtime candle decode) and zipped supply decode now return `Result` and reject invalid protocol scales instead of mapping them to empty strings via `unwrap_or_default`. Public `MarketDataService::get_candles*` propagates the error.
- `AssetAmount::from_scaled` validates optional scale against `MAX_PROTOCOL_SCALE` (same as `Quantity::from_scaled`).

### Fixed
- Realtime token exchange applies one deadline to request **and** bounded body collect; timeout sourced from `Config.timeout`.
- JSON-RPC applies the same e2e deadline, caps bodies at 1 MiB, and validates `jsonrpc=="2.0"`, matching `id`, and exactly one of `result`|`error`.
- `TypedSubscription::close` / Drop aborts the JoinHandle; read loop `select!`s stop vs WS read so close does not linger up to 30s.
- `SnapshotThenStream` surfaces reconnect/`request_refresh` errors via `err()`, retries once, then fail-closes.
- Catalog hydrate is atomic: invalid later rows and zipper failure after a successful spot fetch no longer leave a partially installed catalog.
- Catalog readiness now requires usable spot and zipper snapshots, and `wait_for_catalogs` can recover after a transient failed hydration instead of remaining permanently poisoned.
- Construction outside Tokio records an immediate catalog-readiness error; `wait_for_catalogs` retries on the caller's runtime instead of order paths polling an initializer that never started.
- ConnectRPC `ResourceExhausted` responses map to `Error::RateLimit` instead of generic `Error::Api`.
- `SnapshotThenStream::refresh_snapshot` retains the pending buffer across failed attempts, sets `err()` on failure, and clears it on success so recovery merges each buffered publication exactly once.
- `wait_for_order_trades_complete` requires a terminal order, uses checked trade-quantity accumulation, and applies its deadline to in-flight `GetOrder` calls.

### Features
- `OrdersService::wait_for_order_trades_complete` polls until sum(trade qtys) equals `cum_qty` or timeout.
- `MAX_PROTOCOL_SCALE = 36` exported from `codecs`.

### Testing
- L1+L2 local mock HTTP/WS suite (`tests/hardening.rs`) for token stall, 403, JSON-RPC, close/100-sub soak, catalogs, and scale.
- Live: heartbeat uses `POLYESTER_TEST_TRADE_SYMBOL`; market BUY→SELL roundtrip carries filled qty; BatchModify 5×40 regression (gated).

## 0.1.0a11

Package version: `0.1.0-alpha.11`. Git tag: `v0.1.0a11`.

### Breaking
- `AssetBalance` drops `trading_updated_at_ns` / `funding_updated_at_ns` / `reserved_updated_at_ns`. Use `trading_revision` (orders trading/reserved/available) and `funding_revision` (orders funding independently) instead.
- `Manager::base_quantity_scale_for_symbol` / `base_quantity_scale_for_symbol_id` return `Option<u32>` and no longer invent scale `8` when unknown/unhydrated. Decode-only paths keep an explicit `unwrap_or(8)`.

### Fixed
- Order/trigger write paths wait for catalog hydration before resolving pair quantity scale, preventing first-order false `INSUFFICIENT_FUNDS` when a pair (e.g. ETH-USDT scale 6) was encoded at invented scale 8.

## 0.1.0a10

Package version: `0.1.0-alpha.10`. Git tag: `v0.1.0a10`.

### Fixed
- Realtime now negotiates the `centrifuge-protobuf` WebSocket subprotocol and uses binary, length-delimited Centrifugo commands, replies, pings, and publications. Previous releases selected `:proto` channels while speaking the JSON client protocol, so subscriptions could handshake but receive no binary publications.
- Concurrent authenticated calls now receive distinct monotonic signing timestamps, preventing identical same-millisecond requests from colliding with replay protection.
- BUY trailing-stop requests are rejected locally because the wire strategy is SELL-only; they are no longer silently encoded as SELL.
- Authentication failures without server detail now carry a non-empty fallback message.
- `Config` Debug output redacts `api_private_key`.
- Realtime subscription-token HTTP exchange enforces a 10s timeout and 64 KiB response body cap.
- Public ID parsing prefers canonical base58 when an all-digit string round-trips via `format_id` (e.g. `format_id(4) == "5"` no longer cancels order 5).
- `batch_modify` no longer invents quantity scale 8 when `symbol` is missing; unscaled `new_qty` requires a symbol or a Quantity with known scale.
- WebSocket read timeout is treated as connection death (reconnect / error) instead of a silent no-op that freezes half-open feeds.
- Typed subscriptions expose `resubscribes` / `take_resubscribed` after reconnect gaps (no Centrifugo recover cursor).
- Orderbook / market-overview `close()` drops the update sender so `recv()` cannot hang forever.
- `SnapshotThenStream` Drop stops the background loop when the last handle is released.
- Orderbook sequence numbers stay `u64` end-to-end; inverted/invalid seq fails toward refresh instead of disabling gap detection.
- Candle subscriptions normalize aliases (`MIN_1` / `min1`) to the live channel label (`1m`).
- `GetTrades` results expose `next_page_token`.
- Fully filled orders preserve `leaves_qty == 0` / `cum_qty` instead of mapping zero to `None`.
- `AssetAmount` fields are private; `as_i64` uses fallible `try_from` (no truncation).
- Guard-approval `nonce_space` values above uint192 return `Error::Validation` instead of panicking.

### Changed
- CI runs `cargo test --lib --test ui` only. Live `tests/integration` soft-skips without credentials and is local-only (`POLYESTER_TEST_STRICT_LIVE=1` for release QA).
- Triggers expose string-ID helpers (`get_by_id` / `pause_by_id` / `resume_by_id` / `cancel_by_id`) for base58 public IDs.

## 0.1.0a9

Package version: `0.1.0-alpha.9`. Git tag: `v0.1.0a9`.

### Fixed
- `TriggersList` and `TriggerEventsList` now surface `next_page_token` from list responses so trigger pagination can continue through the high-level wrappers

## 0.1.0a8

Package version: `0.1.0-alpha.8`. Git tag: `v0.1.0a8`.

### Breaking
- Stable MFA auth error codes: `AUTH_API_KEY_MFA_REQUIRED` is removed; use `AUTH_MFA_NOT_ENROLLED`, `AUTH_STEP_UP_REQUIRED`, `AUTH_MFA_ELEVATION_REQUIRED`, and `AUTH_MFA_LAST_FACTOR_REQUIRED` from `AuthErrorDetail`
- Remove JWT/session-only handwritten wrappers that cannot work with API-key auth:
  - `PoliciesService`: all unary list/get/create/update/delete/set methods and policy update builders/params (`UpdateApiPolicyParams`, `UpdateSubaccountPolicyParams`, `build_update_*_policy_request`)
  - `ApiKeysService`: `create` / `update` / `delete` (and `UpdateApiKeyParams` / `build_update_api_key_request`)
  - `SubAccountsService`: create/update/delete and member/invite mutation helpers (and `UpdateSubaccountParams` / `build_update_subaccount_request`)
  - `AddressBookService`: entry/tag mutation helpers (and address-book update builders/params)
  - `ProfileService`: `get` / `update` / `get_username_history` (keep `subscribe_identity`)
  - `ResolveService` / `Client::resolve` removed entirely
- Capability matrix: Profile/Policies marked subscribe-only; Account resolve unsupported for this API-key SDK

### Features
- `Error::is_mfa_enrollment_required` / `is_step_up_required` / `is_mfa_elevation_required` / `is_mfa_last_factor_required` classify MFA control flow from structured auth codes only (no message heuristics)
- `errors::auth_codes` constants and public method-option `MFARequirement` documentation metadata
- `PoliciesService::subscribe_api_policies` typed subscribe for `private:auth:api-policies:{account}:proto`
- `PoliciesService::subscribe_subaccount_policies` alias for the existing subaccount-policies subscribe path

### Testing
- Unit coverage for MFA auth-code mapping and predicates
- Unit coverage for API/subaccount policy realtime protobuf decode
- Private realtime mutation publish tests dropped (mutations are session-only); subscribe-connect coverage retained
- Live integration coverage now rejects private-channel authentication failures instead of treating an idle or failed background task as a successful subscription
- Remove duplicate funded transfer coverage that could submit the same configured transfer twice in a full test run

### Changed
- CI no longer auto-commits `sdk-capabilities.json` / README on pull requests. Capability refresh + optional bot commit runs only on merge to `main`.
- Realtime subscribe methods wait for the initial websocket handshake and retain background reconnect errors for inspection through `err()` / `take_err()`

### Fixed
- Sign API-key requests over the actual JSON body when `WireFormat::Json` is configured, instead of always signing protobuf bytes
- Canonical query / realtime subscription URL encoding now preserves RFC 3986 unreserved characters (`-` `_` `.` `~`). Previously `NON_ALPHANUMERIC` escaped hyphens as `%2D`, which produced `SIGNATURE_INVALID` on private channels such as `api-keys` and `api-policies`
- Direct realtime queues fail closed with `Error::QueueOverflow`; a zero queue setting is clamped to one
- Snapshot-then-stream subscribes before fetching its snapshot, can retry a transient snapshot failure, applies only current-generation orderbook snapshots, and fails closed if its recovery buffer overflows
- Conditional triggers reject `post_only` for market, IOC, and FOK child executions
- `Credentials::new` rejects an empty API key ID
- Correct the capability label: API-key auth uses Ed25519 signatures, not HMAC

## 0.1.0a7

Package version: `0.1.0-alpha.7`. Git tag: `v0.1.0a7`.

### Breaking
- Order and trigger create now map onto the execution variants. `CreateOrderRequest`/`BatchCreateOrdersRequest` carry `OrderIntent`s; `CreateTriggerRequest` carries a `TriggerIntent` with a strategy oneof. The flat public `CreateOrderParams` / `CreateTriggerParams` APIs are unchanged.
- `OrdersService::batch_create` drops the `allow_partial` argument (removed from the wire).
- Invalid `post_only` combinations are rejected: `post_only` is only honored on GTC limit orders/triggers (market, IOC, and FOK reject it).
- Ladder triggers only support the `linear` distribution; any other value is rejected.
- Trailing-stop triggers require `trailing_distance_ticks` or `trailing_distance_bps` and are always an implicit SELL market-IOC strategy (`side`/`order_type`/`time_in_force`/`post_only` are ignored).
- Attached-risk TP/SL legs no longer carry `trigger_price_source` on the wire; the child execution (`market`/`limit` + `limit_price`) is derived from a `RiskExecution`. `TrailingStopPolicy` drops `trigger_price_source`/`order_type`.
- `Trigger` read model now exposes full proto fields (order params, timestamps, detail blocks, `post_only`, `parent_order_id`, child order ids), projected from the `configuration` + `runtime_details` oneofs
- `Order` read model adds `post_only` and `attached_risk`
- `TriggersService::list_with` accepts validated `ListTriggersOpts.status` labels (`created`/`armed`/`running`/`completed`/`cancelled`/`failed`/`paused`)

### Fixed
- `orders.get_with` / list with `include_attached_risk` now returns policy data on `Order.attached_risk`
- `CreateOrderResponse` / `CreateTriggerResponse` / batch-create items no longer carry a status field; admission acks synthesize `"accepted"` and batch items decode the `accepted`/`rejected` outcome oneof

## 0.1.0a6

Package version: `0.1.0-alpha.6`. Git tag: `v0.1.0a6`.

### Fixed
- `CreateSubaccountResult.revision` is returned from create so clients can pass `expected_revision` on the next mutation without a follow-up read

## 0.1.0a5

Package version: `0.1.0-alpha.5`. Git tag: `v0.1.0a5`.

### Breaking
- Durable auth PATCH contract: API-key, subaccount, and address-book entry updates use nested mutable specs, a non-empty FieldMask, and a positive `expected_revision` (`UpdateApiKeyParams` / `UpdateSubaccountParams` / `UpdateAddressBookEntryParams`)
- Soft-delete subaccount requires `expected_revision`; durable resource models expose `revision`
- Address-book tag updates use optional `UpdateAddressBookTagParams` (no revision/mask); empty name is rejected
- Connect `AuthErrorDetail` maps `AUTH_REVISION_CONFLICT` onto `Error::Api { code: "AUTH_REVISION_CONFLICT", .. }`
- Policy creates nest under `policy`; policy update builders (`UpdateSubaccountPolicyParams` / `UpdateApiPolicyParams`) replace flattened request fields

### Testing
- Live funded UserOp tests: Funding → Trading and Funding → external withdraw, gated by `POLYESTER_TEST_CHAIN_USEROP=1`
- Unit coverage for nested FieldMask request builders, presence/clear semantics, revision decode, and revision-conflict error mapping

### Changed
- Realtime (`tokio-tungstenite`) and on-chain Funding helpers (`alloy-*`, `k256`) are always-on dependencies, not optional features. Empty `realtime` / `chain` feature stubs remain for Cargo compatibility.
- Clippy cleanups in always-on `chain` (needless borrows, `too_many_arguments` allow, collapsible status poll)

## 0.1.0a4

Package version: `0.1.0-alpha.4`. Git tag: `v0.1.0a4`.

### Features
- Optional Cargo feature `chain` smart-account path: CREATE2 Safe prediction, ERC-4337 UserOp submit (bundler + paymaster), Funding → external / Funding → Trading calldata, Zipper fee quote, and full FundingAccount / GuardRegistry whitelist encoders
- Realtime delivery is fail-closed on queue overflow (`Error::QueueOverflow`); managed snapshot-then-stream subscriptions refresh Connect snapshots on reconnect and expose recovery hooks

### Changed
- Connect RPC coverage gate no longer commits dashboard reports under `docs/`; CI fails on unexpected gaps only (`sdk-coverage.toml` + `scripts/check_sdk_coverage.py`)

### Docs
- README `Supported surface` table is generated from `sdk-capabilities.json` (`--write-capabilities`); links to the public [SDK capability matrix](https://polyester.ai/docs/developer-docs/getting-started/sdk-capability-matrix)
- README expanded toward Python parity (credentials, auth patterns, orders, balances, market data, realtime)
- CI auto-commits refreshed `sdk-capabilities.json` + README capability table when they drift (same-repo)
- README documents on-chain Funding UserOps (caller-supplied owner EOA → derive Polyester Safe) vs Trading withdraw RPCs; realtime overflow / reconnect recovery contract

### Testing
- Live smoke on Polyester testnet: Funding → BSC USDT withdraw UserOp via `PolyesterSmartAccount`

## 0.1.0a3

Package version: `0.1.0-alpha.3`. Git tag: `v0.1.0a3`.

### Features
- Raw and typed Centrifugo subscription handles stop their background tasks on `Drop` (in addition to explicit `close()`)
- Connect RPC wrapper coverage gate: `scripts/check_sdk_coverage.py` + `sdk-coverage.toml`

### Docs
- README notes Drop cleanup for realtime subscriptions

## 0.1.0a2

Package version: `0.1.0-alpha.2`. Git tag: `v0.1.0a2`.

### Breaking
- Authoritative freshness: `Order.state_revision` → `Order.version`; balance `trading_version` / `funding_version` / `reserved_version` → `trading_updated_at_ns` / `funding_updated_at_ns` / `reserved_updated_at_ns`; subaccount and API-key `updated_at` are configuration timestamps; API-key `last_used_at` stays independent activity time
- Dual-path qty/price typing and broader Go/Python API parity

### Features
- Generated reconciliation and policy types exposed in the public SDK surface
- Internal transfer amounts use U128 wire types end-to-end

## 0.1.0-alpha.1

Initial alpha tag (`v0.1.0-alpha.1`). Later tags use `v0.1.0aN` while the crate version stays `0.1.0-alpha.N`.
