# Changelog

## Unreleased

### Breaking
- Stable MFA auth error codes (POLY-2919): `AUTH_API_KEY_MFA_REQUIRED` is removed; use `AUTH_MFA_NOT_ENROLLED`, `AUTH_STEP_UP_REQUIRED`, `AUTH_MFA_ELEVATION_REQUIRED`, and `AUTH_MFA_LAST_FACTOR_REQUIRED` from `AuthErrorDetail`
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
- POLY-3739: `PoliciesService::subscribe_api_policies` typed subscribe for `private:auth:api-policies:{account}:proto`
- `PoliciesService::subscribe_subaccount_policies` alias for the existing subaccount-policies subscribe path

### Testing
- Unit coverage for MFA auth-code mapping and predicates
- Unit coverage for API/subaccount policy realtime protobuf decode
- Private realtime mutation publish tests dropped (mutations are session-only); subscribe-connect coverage retained

### Changed
- CI no longer auto-commits `sdk-capabilities.json` / README on pull requests. Capability refresh + optional bot commit runs only on merge to `main`.

## 0.1.0a7

Package version: `0.1.0-alpha.7`. Git tag: `v0.1.0a7`.

### Breaking
- Order and trigger create now map onto the POLY-3701 execution variants. `CreateOrderRequest`/`BatchCreateOrdersRequest` carry `OrderIntent`s; `CreateTriggerRequest` carries a `TriggerIntent` with a strategy oneof. The flat public `CreateOrderParams` / `CreateTriggerParams` APIs are unchanged.
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
