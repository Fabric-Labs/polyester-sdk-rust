# Changelog

## Unreleased

### Breaking
- Connect market, order, trigger, fee, policy, and orderbook payloads are
  `symbol_id`-only. `GetSpotConfig` is the remaining message that still
  returns both `symbol` and `symbol_id`. Public methods still accept display
  `symbol` and resolve it through the hydrated catalog.
- Market overview, trigger list, cancel-all, and cancel-all-after no longer
  forward raw symbol strings. Unknown symbols fail closed.
- Trigger `modify` / `resume` require `symbol` or `symbol_id` so the Connect
  request can carry the required market identifier.
- Policy models dropped perpetual rules. `SpotMarketRule` is `symbol_id` plus
  optional catalog display `symbol`.
- `TradingRateLimitRule.tier` is now `vip_tier`.
- `TriggerEvent.reason` is removed. `Trigger` and `TriggerEvent` expose
  `cancel_reason` and `failure_reason` (empty when unset or unspecified)
  decoded from the proto `terminal_reason` oneof as prefix-stripped labels
  (`user_request`, `insufficient_funds`).

### Added
- `TradesService::list_with` / `ListUserTradesOpts` for symbol filters,
  `after_match_id`, `limit`, and `page_token`. `after_match_id` requires a
  resolved non-zero `symbol_id` (display `symbol` or numeric id).
  `trades.list(subaccount_id, limit)` remains a thin wrapper.
- `AddressBookViewInvalidation.view_revision`. `get_view` stays a proto
  pass-through (`ApiData.raw`); callers set `minimum_view_revision` on the
  request, and regenerated raw views include `view_revision`.

### Docs
- README links the sibling
  [`polyester-examples-rust`](https://github.com/Fabric-Labs/polyester-examples-rust)
  cookbook near the top.
- README examples use placeholder `YOUR_ACCOUNT_ID` and `BTC-USDT`
  instead of a live Account ID and `BNB-USDT`.

## 0.1.0a39

Package version: `0.1.0-alpha.39`. Git tag: `v0.1.0a39`.

### Added
- Address-book writes on `AddressBookService`: `create_entry`, `update_entry`,
  `delete_entry`, `copy_entry`, `create_tag`, `update_tag`, and `delete_tag`.
  Create and update accept atomic `new_tags` (created and attached in the same
  protected request). When `tag_ids` is also selected on update, the result is
  those ids plus the new tags; otherwise existing tags are preserved and
  `new_tags` are appended.
- `AddressBookEntry.tags` so create/update responses can prove attached tags.
- `errors::auth_codes::INTERNAL_ERROR` for `AUTH_INTERNAL_ERROR`.

### Changed
- Twitter social-verification `start` forwards a leading `@` on the handle
  (server CEL now accepts it).

## 0.1.0a38

Package version: `0.1.0-alpha.38`. Git tag: `v0.1.0a38`.

### Added
- VIP catalog and caller-root status: `client.vip.list_vip_tiers()` /
  `client.vip.get_vip_status()` (`vip.v1.VIPService`). Optional qualification
  metrics, timestamps, and next-tier thresholds stay omitted when unset.
- Effective spot fee rates: `client.fees.get_spot_fee_rates()`
  (`fees.v1.FeeService`), optionally filtered by subaccount and `symbol_id`.
- Public trading rate-limit catalog and authenticated account/API-key limits:
  `client.rate_limits.get_rate_limit_config()` /
  `client.rate_limits.get_trading_rate_limits()` (`ratelimit.v1.RateLimitService`).
  `policy_class` uses full protobuf enum names.

### Changed
- SDK boundary cleanup: raw `symbol`/`symbols` filters on market overview,
  triggers list, and cancel-all / cancel-all-after are forwarded (trim /
  empty-omit) without catalog fail-closed validation or catalog waits solely
  for that validation. Symbol→`symbol_id` conversion paths still wait/resolve.
- Removed client-side pair-constraint preflight (tick/step/min qty/min
  notional, attached-risk tick checks, quote-budget minima). Catalogs still
  provide scales/IDs for encoding; optional zero catalog minima no longer
  affect hydration because those fields are ignored by the SDK.
- `TsNs::from_wire` no longer rejects millisecond-shaped timestamps.
- Pagination: `positive_limit` (reject explicit `Some(0)`) is kept only for
  ListOpen-style optional limits. Zero-means-default proto `limit` fields
  accept `0` as the server default again.

## 0.1.0a37

Package version: `0.1.0-alpha.37`. Git tag: `v0.1.0a37`.

### Added
- Market overview `index_price` from proto `index_price_ticks` (multi-venue index
  in quote units scaled by 1e6; `None` when no fresh index is available).

## 0.1.0a36

Package version: `0.1.0-alpha.36`. Git tag: `v0.1.0a36`.

### Added
- Fail-closed catalog-backed symbol filters for market overview and raw symbol paths.
- Positive-limit validation that preserves omitted defaults.
- Catalog pair-constraint accessors and deterministic order/trigger preflight.
- Shared `ts_ns` response-contract validation that rejects millisecond-shaped values.
- Focused regression tests for F001/F004/F006/F007/F018–F020/F027/F008/F031.

### Fixed
- Unknown non-empty symbol filters no longer degrade into unfiltered requests.
- Zero-valued optional catalog minimums are treated as unset so live catalogs hydrate.

## 0.1.0a35

Package version: `0.1.0-alpha.35`. Git tag: `v0.1.0a35`.

### Breaking
- `Error::RateLimit` gains a `detail: Option<Box<RateLimitDetail>>` field.
  Update exhaustively matched destructuring sites.

### Added
- Public `RateLimitDetail` model for `polyester.ratelimit.v1` quota rejection
  payloads (`reason`, presence-aware `limit` / `remaining` / `retry_after_ms` /
  `policy_version`, plus `operation_id`, `policy_class`, `scope`, `refill_model`).
- Connect `ResourceExhausted` mapping populates `Error::RateLimit.detail` from
  top-level `RateLimitDetail` or nested `orders.v1.ErrorDetail.rate_limit`.
  `retry_after` prefers `detail.retry_after_ms`, then `Retry-After` /
  `Retry-After-Ms` / `grpc-retry-pushback-ms` headers.
- Preview / batch create / batch replace / batch cancel rejections surface
  `rate_limit` on `OrderErrorDetail` and batch result items when present.

## 0.1.0a34

Package version: `0.1.0-alpha.34`. Git tag: `v0.1.0a34`.

### Breaking
- Generated `TriggerEvent.fire_price_ticks` is now optional (absent for
  time-scheduled TWAP slice fires). Decoded `TriggerEvent.fire_price` is `None`
  when the wire field is unset.

### Added
- `ListOpenOrdersOpts.trigger_id` and `ListOrderHistoryOpts.trigger_id` filter
  child orders created by a standalone trigger (TWAP/ladder slice children and
  their execution prices).

## 0.1.0a33

Package version: `0.1.0-alpha.33`. Git tag: `v0.1.0a33`.

### Breaking
- `Client::get_flow_by_tx` now returns `LifecycleFlowsList` instead of silently
  returning only the first match. Zero-limit transaction lookups now default to
  50; use `Client::list_flows_by_tx` with `next_page_token` for pagination.

### Added
- Generated `polyester.ratelimit.v1` contracts expose structured quota
  rejection details used by order and trade WebSocket responses.

## 0.1.0a32

Package version: `0.1.0-alpha.32`. Git tag: `v0.1.0a32`.

### Added
- `withdraw.validate_destination` wraps `ValidateWithdrawDestination` and maps
  validation codes to snake labels (`valid`, `invalid_address`,
  `denylisted_address`, …) for preflight checks before Trading → external
  withdraws.

## 0.1.0a31

Package version: `0.1.0-alpha.31`. Git tag: `v0.1.0a31`.

### Added
- Lifecycle reason catalog maps trading-withdraw failure codes to snake labels:
  `trading_withdraw_policy_denied`, `trading_withdraw_contract_reverted`, and
  `trading_withdraw_execution_failed` .

## 0.1.0a30

Package version: `0.1.0-alpha.30`. Git tag: `v0.1.0a30`.

### Docs
- README documents `UserTrade` fee e18 fields and `fee_is_rebate` polarity.

## 0.1.0a29

Package version: `0.1.0-alpha.29`. Git tag: `v0.1.0a29`.

### Breaking
- `UserTrade` fee fields move from asset-scaled integers to fixed 18-decimal
  magnitudes: `fee_scaled` → `fee_amount_e18`, `referral_share_scaled` →
  `referral_share_amount_e18` (decimal strings of wire `U128`). Convert to the
  fee asset's catalog scale before subtracting from BUY fill quantity.
- `UserTrade` adds sparse `fee_is_rebate`. When true, `fee_amount_e18` is a
  rebate credit rather than a fee debit (proto3 omits false). Exhaustive struct
  literals must initialize the new fields.

## 0.1.0a28

Package version: `0.1.0-alpha.28`. Git tag: `v0.1.0a28`.

### Docs
- crates.io documentation URL points to the Rust SDK docs on polyester.ai
  (`/docs/sdk/rust/get-started/overview`).

## 0.1.0a27

Package version: `0.1.0-alpha.27`. Git tag: `v0.1.0a27`.

### Fixed
- `Ed25519Keypair` `Debug` redacts `secret_key_hex` / `secret_key` (same posture as `Config`).
- Attached `TrailingStop` encode rejects non-positive distance/max slippage and rejects
  supplied `trigger_price_source` / `order_type` instead of silently ignoring them.
- Decode omits attached trailing legs that lack a positive distance (no fabricated
  `Ticks(0)` stop).

### Docs
- README / installation: live integration + `a7_strict_live` require a git checkout;
  those paths are excluded from the crates.io package.

## 0.1.0a26

Package version: `0.1.0-alpha.26`. Git tag: `v0.1.0a26`.

### Breaking
- Trigger snapshots no longer expose `child_order_ids`. Child-order history is
  authoritative on trigger events: set `ListTriggerEventsRequest.event_type` to
  `EVENT_FIRED` and read `child_order_id` / `child_seq` from decoded events.
- Decoded `TriggerEvent.event_type` labels are now `fired` / `canceled` /
  `updated` (not proto names like `EVENT_FIRED`).

### Added
- `ListTriggerEventsRequest.event_type` is available on the generated request
  (optional filter).
- `TriggerEvent` thickens with `subaccount_id`, `symbol_id`, `trigger_type`,
  `child_seq`, `child_order_id`, `fire_price`, and `reason`.

## 0.1.0a25

Package version: `0.1.0-alpha.25`. Git tag: `v0.1.0a25`.

### Breaking
- `PreviewOrderResult` is now admission-oriented:
  `admissible`, optional `rejection` (`OrderErrorDetail` /
  `OrderFieldViolation`), optional `resolved_base_qty`, optional
  `protected_price_bound` (renamed from `price_bound`), and required
  `evaluated_at_ms`. Removed `estimated_quote_debit`, `estimated_fee`,
  `estimated_net_base_qty`, `fee_asset`, and `fresh_at_ts_ns`.
  Known Preview rejection codes use TypeScript-compatible labels such as
  `BAD_QTY`; unknown open-enum values use `UNKNOWN_ERROR_CODE(<n>)`.
- `LifecycleFlowSummary` thickens with `lifecycle_reason` (snake labels +
  `unknown_reason_<n>`) and optional `zipper_reason`
  (`ZipperReasonDetails { code, reason_id, message }`) after the
  `FlowReason` -> `LifecycleReason` rename. Tx-match flows preserve
  `owner_account_id`; present-zero preview sizing/protection values are kept.

## 0.1.0a24

Package version: `0.1.0-alpha.24`. Git tag: `v0.1.0a24`.

### Breaking
- `CreateOrderParams::max_quote_debit_scaled` and
  `PreviewOrderParams::max_quote_debit_scaled` now take a typed `Quantity`
  with `QuantityDomain::OrderQuote` instead of a bare `i64`. Construct quote
  budgets with `Quantity::from_quote_scaled`, `from_quote_decimal_str`, or
  `from_quote_decimal`; the SDK validates the embedded scale against the pair's
  catalog `quote_quantity_scale`.
- `PreviewOrderResult` now exposes typed `estimated_quote_debit` and
  `estimated_fee` values instead of bare `*_scaled` integers.
- Wire regen: `PreviewOrder` now wraps a full
  `OrderIntent` (same contract as create). `PreviewOrderParams` gains
  `client_order_id`, `self_trade_prevention`, and `attached_risk` for intent
  parity; preview still does not place a hold or claim a client order id.
- `TrailingStopTrigger` carries child `side` on the wire. Standalone create
  remains SELL-only; trigger reads project attached trailing `side` and
  `parent_order_id` instead of hard-coding sell / omitting parent linkage.

### Added
- Catalog quote-quantity-scale lookup by symbol and symbol ID.
- Local validation rejects create, cancel, and replace batches above 20 items.

### Fixed
- Transfer and trading-withdraw amounts fail closed when neither the
  `AssetAmount` nor request parameters provide a source scale.
- Spot-config decoding preserves valid zero quote quantity scales.

## 0.1.0a23

Package version: `0.1.0-alpha.23`. Git tag: `v0.1.0a23`.

### Changed
- First publish to [crates.io](https://crates.io/crates/polyester-sdk). Install via Cargo registry; git-tag pins remain supported for private clones. No API changes from `0.1.0-alpha.22`.

## 0.1.0a22

Package version: `0.1.0-alpha.22`. Git tag: `v0.1.0a22`.

### Breaking
- `OrderFeeSource` / `fee_source` are replaced by `FeeAsset` / `fee_asset`.
  Use `FeeAsset::Base` (BUY only) where older clients used the removed
  received-asset fee mode; SELL orders must use `FeeAsset::Quote`.
- `CreateOrderParams.quantity` is now optional because create sizing is an
  explicit oneof: set exactly one of base `quantity` or
  `max_quote_debit_scaled`. Create results now expose `resolved_base_qty` and
  `submitted_max_quote_debit_scaled`; order history exposes the submitted
  quote-debit budget when present.

### Added
- `OrdersService::preview` resolves advisory sizing, price bounds, and fees
  before submission.
- `BatchReplaceStatusResult::is_settled` and `is_batch_replace_settled` report
  when every item is `working`, `rejected`, or `terminal`.

### Changed
- Batch-replace predecessor IDs can be stale after admission. Use each item's
  `replacement_order_id`, reuse the same `request_id` on retry, and poll
  `get_batch_replace_status` for reconciliation; its phases
  (`admitted`/`working`/`rejected`/`terminal`) are not execution finality.

## 0.1.0a21

Package version: `0.1.0-alpha.21`. Git tag: `v0.1.0a21`.

### Breaking
- `OrdersService::batch_modify` and `BatchModify*` models are replaced by admission-oriented
  `OrdersService::batch_replace` and `BatchReplace*`. Batch replacement is now a same-symbol
  quote-refresh operation returning an admission receipt; poll `get_batch_replace_status` with
  its `batch_request_id` for execution finality. Item `behavior` and request
  `behavior_default` / `allow_partial` controls are removed.

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
