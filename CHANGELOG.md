# Changelog

## Unreleased

- A method that names a market by symbol, or needs a pair's catalog scales, now waits for the catalog hydration the client starts in the background. It read the catalogs at once, so a call made right after `Client::new`, such as `orderbook.create_subscription` for `"DOGE-USDT"`, refused a valid symbol with `Error::Validation` (`unknown symbol ...` or `symbol_id is required for orderbook subscriptions`) while hydration was still running; only order and trigger paths and `market_overview.list` waited. A symbol the hydrated catalogs don't know returns the unknown-symbol `Error::Validation`. With `hydrate_catalogs: false` nothing waits and the refusals are unchanged. Affected: `market_data.get_trades`, `get_trades_with`, `get_candles`, `get_candles_with`, `get_current_candle`, `get_candles_columns`, `subscribe_trades` and `subscribe_candles`; `orderbook.get` and `orderbook.create_subscription`; `heatmap.get`, `heatmap.get_with_options` and `heatmap.subscribe_live`; and `zipper.subscribe_zipped_asset_supply`, whose updates are decoded with the catalog scales.
- `market_overview.subscribe`, `market_overview.get_spot_volume_history` without symbols, `fees.get_spot_fee_rates`, and `triggers.list`, `triggers.list_with` without a symbol, `triggers.get` and `triggers.get_by_id` now wait for hydration in progress before they label and scale their results. Called right after the client was created, they returned an empty `symbol` and, for the overview, rows without the sizes and volumes the catalog scales decode. A failed hydration still leaves those fields empty and returns no error.
- **Breaking:** `Error` gains `Error::CatalogHydration(Box<Error>)`, which a method that needs the catalogs returns when hydration failed. The boxed error is the failure that `Client::wait_for_catalogs` returns and `Client::catalogs_last_error` holds, and the message still starts `catalog hydration failed`. It is not retryable as is: `wait_for_catalogs` or `hydrate_catalogs` hydrates again. The methods that already waited returned the failure itself, an `Error::Validation`. With hydration on, `orderbook.create_subscription` refuses a symbol the hydrated catalogs don't know with `unknown symbol ...` instead of `symbol_id is required for orderbook subscriptions`. Affected: `ServiceContext::wait_for_catalogs`; `orders.create`, `preview`, `batch_create`, `batch_replace` and `modify`; `orders.list_history_with`, `cancel_with`, `cancel_all_with` and `cancel_all_after`, and `trades.list_with`, with a symbol; `triggers.create` and `modify`, and `triggers.list_with` and `resume_with` with a symbol; `market_overview.list`, and `market_overview.get_spot_volume_history` and `create_subscription` with symbols; `Client::stop_all_trading`; and the methods named above. Migration: add an `Error::CatalogHydration(failure)` arm to exhaustive matches on `Error`, and match it where you matched the `Error::Validation` of a failed hydration.
- **Breaking:** `ZipperChainConfig` gains the network's memo rule for withdrawal destinations: `memo_requirement` (`"required"`, `"optional"` or `"not_supported"`; empty when the network names none, which does not mean a memo can be omitted; `"UNKNOWN(n)"` for a rule this version does not know), `memo_label`, such as `"Destination tag"`, and `memo_format_hint`, display guidance kept exactly as sent and not parsed. XRP reports `"optional"`. They were not decoded. Affected: `zipper.get_deposit_withdraw_config` and `codecs::decode::deposit_withdraw_config_from_proto`. Migration: a `ZipperChainConfig` struct literal must set the three fields; use `memo_requirement` as the rule and the label and hint for display only.
- **Breaking:** `InternalTransferResult` gains `flow_id`, the transfer's lifecycle flow ID, which `lifecycle.get_flow` reads; empty when the server sends none. Affected: `internal_transfers.create` and `codecs::decode::internal_transfer_from_proto`. Migration: an `InternalTransferResult` struct literal must set `flow_id`. Looking a transfer up by its request or transfer ID (`GetInternalTransfer`) is session-only and is not wrapped.
- **Breaking:** `ApiPolicy` and `SubaccountPolicy` gain `actions`, the policy's granted `auth.v1.PolicyAction` names in the order the server sent them, such as `"READ_SPOT"`; `"UNKNOWN(n)"` for an action this version does not know. They were dropped. This includes the account permissions API-key policies can now grant, from `READ_API_KEYS` on: `READ_API_KEYS`, `READ_API_POLICIES`, `READ_SUBACCOUNT_POLICIES`, `READ_SUBACCOUNT_PERMISSIONS`, `READ_VIP_STATUS`, `READ_SOCIAL_VERIFICATION`, `READ_PROFILE`, `READ_ACCOUNT_DIRECTORY`, `READ_WITHDRAW_VALIDATION`, `READ_DAILY_CLAIMS`, `CLAIM_DAILY_REWARD`, `READ_CHECKPOINTS` and `READ_REWARDS`. Each name parses back with `PolicyAction::from_proto_name`. Affected: `policies.subscribe`, `policies.subscribe_subaccount_policies`, `policies.subscribe_api_policies`, and `codecs::decode::api_policy_from_proto`, `subaccount_policy_from_proto`, `api_policy_from_bytes`, `subaccount_policy_from_bytes` and the policy list, get, create and update decoders. Migration: an `ApiPolicy` or `SubaccountPolicy` struct literal must set `actions`.
- API keys can now make account reads that needed a session, each gated by its own permission on the key's API-key policy. The wrapped ones: `api_keys.list` and `api_keys.get` need `READ_API_KEYS`; `vip.get_vip_status` needs `READ_VIP_STATUS` and a main-account key, where it always refused an API key; `social_verification.get` needs `READ_SOCIAL_VERIFICATION` and a main-account key, and returns the link status only. A key without the permission is refused with `permission_denied` and `AUTH_API_KEY_ACCESS_DENIED`, and the message names the permission. The rustdoc and README no longer list VIP status as session-only. Affected: `api_keys.list`, `api_keys.get`, `vip.get_vip_status` and `social_verification.get`.
- `withdraw.validate_destination` with an API key now needs `READ_WITHDRAW_VALIDATION` on the key's policy; `EXTERNAL_WITHDRAW` alone no longer opens it. Policies that granted `EXTERNAL_WITHDRAW` were given `READ_WITHDRAW_VALIDATION` when the API changed, but a policy you create or rewrite must include it. Without it the call is refused with `permission_denied` and `ERROR_CODE_PERMISSION_DENIED`. Affected: `withdraw.validate_destination`.
- `ERROR_CODE_PATH_BODY_ID_MISMATCH` and `AUTH_PATH_BODY_ID_MISMATCH`, the REST refusals of a body ID that differs from the path ID, are recognised and keep their specific codes: `Error::Api` with that code, `Error::auth_code()` returning `AuthErrorCode::AUTH_PATH_BODY_ID_MISMATCH` for the auth one. Neither is retryable. ConnectRPC requests have no path ID and do not receive them. Affected: error mapping of every service method.
- Ledger transfers with the transfer codes the contract added keep their number in `LedgerTransfer::transfer_type`: `TRADING_WITHDRAW_FEE_LEG` (1066), `QUICKSWAP_POLYESTER_FEE` (1067), `QUICKSWAP_BROKER_FEE` (1068), `QUICKSWAP_AFFILIATE_FEE` (1069), `FEE_SWEEP` (1072), `FEE_REFILL` (1073) and `SYSTEM_ADJUSTMENT` (1074); `TransferCode::from_i32` (`buffa::Enumeration`) names them. A system adjustment, such as seeding an account from a system account or correcting a balance, credits or debits the account, so read `is_debit` for its direction; it is no credit by type. Affected: `transfers.list`, `transfers.subscribe`, `balances.list_transfers`, and `codecs::decode::transfer_row_from_proto`, `transfers_list_from_proto` and `ledger_transfer_batch_from_bytes`.
- The reasons for the services this SDK does not wrap now say which reads API keys can call and with which permission: policy, profile, account resolve, effective sub-account permission, daily claim, checkpoint and reward award reads. Reward acknowledgement, reward destinations, policy and profile writes, and the internal transfer lookup stay session-only.

## 0.17.0

Package version: `0.17.0`. Git tag: `v0.17.0`. Released 2026-11-10. Built on public contract `c8d9827`.

- **Breaking:** A managed order book adds one view when it applies a snapshot together with the deltas buffered behind it. It added one view per buffered delta and then the same book again, so a stale copy stayed queued: after `set_bucket`, the next view read could be the copy at the previous bucket. Affected: `OrderbookService::create_subscription` and `orderbook::Subscription::updates`. Migration: read one view per snapshot, at the last buffered sequence; read `book_seq` instead of counting views.
- Added `orderbook::Subscription::since_last_publication`, the time since the managed book's stream last delivered a publication, or since it started. The server republishes each book's whole view about every 30 s even when nothing changes, so an age far above that means the stream stalled while the socket stayed open, not that the market is quiet. The SDK sets no threshold; poll it and refresh or resubscribe on a stall.
- **Breaking:** `CreateOrderParams` and `PreviewOrderParams` gain `symbol_id` and a `for_symbol_id` constructor, so `OrdersService::create`, `OrdersService::preview` and `OrdersService::batch_create` accept an order that names its market by Connect ID, as the Go and Python SDKs do. With an empty `symbol` the ID names the market; with both set they must name the same market. The ID must be in the hydrated catalog, and an unknown or zero ID or a contradicting symbol is `Error::Validation` before sending. An order with an empty `symbol` was refused ("symbol must be non-empty"); it is now accepted when `symbol_id` is set. Both structs are `#[non_exhaustive]`, so code outside the crate keeps compiling. Migration: none needed for orders that name a symbol, since `symbol_id` defaults to `None`; to order by market ID, use `CreateOrderParams::for_symbol_id` or set `symbol_id`.
- The trailing distance in ticks is documented as what it is: a price delta in 1e-9 quote-unit ticks, the scale of every price tick, sent as given. On BTC-USDT, 50 USDT is `50_000_000_000` ticks and 0.5 USDT is `500_000_000` (`Price::from_decimal_str("0.5", None)?.as_ticks()`). It is not a count of the pair's tick size. Affected docs: `TrailingDistance::Ticks`, `CreateTriggerParams::trailing_distance_ticks`, `ModifyTriggerParams::trailing_distance_ticks`, `TriggerTrailingDetails::trailing_distance` and the README's trigger section.
- The live social-verification start test runs only with `POLYESTER_TEST_MUTATION=1`, and a hardening test keeps every live test that writes behind a write gate.

## 0.16.0

Package version: `0.16.0`. Git tag: `v0.16.0`. Released 2026-11-10. Built on public contract `c8d9827`.

- **Breaking:** `orders.create`, `orders.preview` and `orders.batch_create` refuse a limit order with `market_client_ref_price: Some(_)` with `Error::Validation` (`market_client_ref_price is only valid for market orders`), before anything is sent, as they already refuse `market_max_slippage` on a limit order. The public contract carries the reference price on market orders only. On a limit order the SDK accepted it, then dropped it, so the order was created without it and read back with `market_client_ref_price` `None`. Migration: set `market_client_ref_price` to `None` on limit orders; it never had an effect on them.
- **Breaking:** `MarketOverviewEntry` gains the 24-hour range, the top of the book and two times the server sends for each market: `high_24h` and `low_24h` (`Option<Price>`), `best_bid` and `best_ask` (`Option<Price>`), `best_bid_qty` and `best_ask_qty` (`Option<Quantity>`, base quantity, at the pair's base quantity scale and with its symbol when the catalogs know them), and `last_trade_ts_ns` and `listed_ts_ns` (epoch nanoseconds as a decimal `String`). They were dropped, so the overview showed neither the 24-hour range nor the best bid and ask. Values are exact, as sent; a price and its size are `None` when that side of the book is empty, the range is `None` when the server sends none, and a time is empty when there is none. Affected: `MarketOverviewService::list`, `subscribe` and `create_subscription`, and `codecs::decode::market_overview_entry_from_proto`, `market_overview_list_from_proto`, `market_overview_batch_from_proto` and `market_overview_batch_from_bytes`, which leave the size's scale and symbol unset. `list` and `subscribe` return `Error::Validation` (`market overview book size invalid`) for a book size the server sends that does not fit the pair's base quantity scale. Migration: a `MarketOverviewEntry` struct literal must now set the eight new fields, and equality of `MarketOverviewEntry` now compares them; compare the fields you need instead of whole entries.
- **Breaking:** `AddressBookViewInvalidation::scope` is now an `Option<AccountScope>` (`scope_type`, `root_account_id`, `sub_account_id`) instead of a `String`, so an event names the address book that changed. A sub-account's change reported only the root account, so it read the same as a change to the root account's book. `scope_type` is `SCOPE_ROOT` or `SCOPE_SUBACCOUNT`; `sub_account_id` is the sub-account's public ID, and empty for a root scope; `scope` is `None` when the event names none. Affected: `AddressBookService::subscribe` and `subscribe_view_invalidations`, and `codecs::decode::address_book_invalidation_from_proto` and `address_book_invalidation_from_bytes`. Migration: read `root_account_id` from the `AccountScope` where you read `scope`, and refresh the sub-account's book when `scope_type` is `SCOPE_SUBACCOUNT`.

## 0.15.1

Package version: `0.15.1`. Git tag: `v0.15.1`. Released 2026-11-09. Built on public contract `c8d9827`.

- Order reads now return an attached trailing stop's buy-back limit as `TrailingStop::limit_price`: the maximum price of a trailing stop on an order that sells, the most its exit pays per unit, exactly as the server returns it. It was always `None` on a read, though the server returned it, so a trailing stop read back without its cap and only `triggers.get` showed it. It stays `None` when the server sends none, as for the trailing stop of an order that buys. A `TrailingStop` read from an order and passed back to `orders.modify` now keeps its limit. Affected: `orders.get_with`, `orders.list_open_with` and `orders.list_history_with` with `include_attached_risk`, and `orders.subscribe`, and `codecs::decode::order_from_proto`, `get_order_from_proto`, `orders_list_from_open`, `orders_list_from_history` and `orders_unit_from_proto`.
- The `TrailingStop` docs, its deprecated `order_type` note and the README's attached trailing stop section now say what the trailing stop sends when it fires: with a buy-back `limit_price`, a limit IOC child at that price, which fills what it can at or below the limit and cancels the rest instead of resting; without one, a market child. They said the child is always market. The README also said an attached BUY trailing leg reads as a market IOC trigger; `triggers.get` reads it as a limit IOC child with its maximum price as `limit_price`, as for a standalone BUY one. The `Error::Validation` for an `order_type` on an attached trailing stop now reads "attached trailing_stop takes no order_type: its child is a limit IOC at limit_price when the order sells, and a market order when it buys"; `order_type` there is still refused.

## 0.15.0

Package version: `0.15.0`. Git tag: `v0.15.0`. Released 2026-11-09. Built on public contract `c8d9827`.

- **Breaking:** `BatchCreateResultItem`, `BatchCancelResultItem` and `BatchReplaceAdmissionItem` gain `violations: Vec<OrderFieldViolation>`, the field violations (`field_path`, `rule_id`, `message`) a rejected item carries, in the order the server sent them, as `orders.preview` already reports them. A batch create item refused for `ERROR_CODE_PRICE_TICK_SIZE` now names `order.attached_risk.take_profit.trigger_price_ticks` and rule `tick_size`, for example. They were dropped, so a caller could not tell which field to fix. `violations` is empty for an accepted item and for a rejection that names no field. Affected: `orders.batch_create`, `orders.batch_cancel` and `orders.batch_replace`, and `codecs::decode::batch_create_from_proto`, `batch_cancel_from_proto` and `batch_replace_from_proto`. Migration: a struct literal of these three types no longer compiles without `violations`; add `violations: Vec::new()`.
- **Breaking:** A rejected `orders.batch_cancel` item that resolved no order, such as an `ORDER_UNKNOWN` rejection by client order ID, now reads `order_id` `""`. It read `"0"`, an order ID no order has. An item that resolved its order keeps that ID, accepted or rejected. Affected: `orders.batch_cancel` and `codecs::decode::batch_cancel_from_proto`. Migration: test `order_id` for `""` instead of `"0"`.
- **Breaking:** `triggers.create` refuses a sell TWAP with `order_type: CreateOrderType::Market` and `max_slippage_bps: Some(10000)` with `Error::Validation` (`max_slippage_bps must be between 1 and 9999`), before anything is sent. The server refuses it with `MAX_SLIPPAGE_INVALID`, since a 100% cap leaves a sell no positive price; a sell stop-loss or take-profit market child was already refused the same way. A buy TWAP keeps 1 to 10000. Migration: cap a sell TWAP at 9999 bps or less, or set `max_slippage_ticks` instead.
- **Breaking:** `market_data.get_trades_with`, `market_data.get_candles_with`, `market_data.get_candles_columns` and `orders.list_history_with` refuse a `symbol` and a `symbol_id` that name different markets with `Error::Validation` (`… symbol mismatch: "LTC-USDT" does not match symbol_id 1 (BTC-USDT)`), before anything is sent. They sent the ID and ignored the name, so a caller got data for a market they didn't name. A name the catalog maps to another ID, or that is not the catalog's name for the ID, is refused; a name and an ID the catalog doesn't know are sent as before. A name alone, an ID alone, or both naming one market are unchanged. `orders.list_history_with` now waits for catalogs whenever a `symbol` is given, not only when it is given alone. Every other method that takes both already refused a contradiction. Migration: pass the name or the ID alone, or both for the same market.

## 0.14.0

Package version: `0.14.0`. Git tag: `v0.14.0`. Released 2026-11-08. Built on public contract `c8d9827`.

The `c8d9827` contract changes (received transfers in flow lists, receive windows, and the replay-capacity, receive-window and request-deadline refusals) are available on devnet with its next deployment, and on testnet with its next release.
- **Breaking:** `triggers.create` refuses `time_in_force: Some(CreateTimeInForce::Gtd)` for every trigger type with `Error::Validation`, before anything is sent. No trigger child has a GTD execution, and the selection was replaced instead: a market stop or take-profit was sent as a market IOC child, and a TWAP, ladder or trailing stop sent its fixed GTC or IOC child. A limit stop already refused it. Migration: leave `time_in_force` unset, or choose `Gtc`, `Ioc` or `Fok` where the trigger type accepts it.
- **Breaking:** `triggers.create` refuses a TWAP with a market child whose `max_slippage_ticks` is zero or negative with `Error::Validation`, as every other create-time tick cap already did. Migration: send a positive tick cap, or leave `max_slippage_ticks` unset for the pair default.
- **Breaking:** `Error::auth_code` reads the auth reason of an `Error::Api` from its REST problem code, as it does for `Error::ApiKeyRejected`. A REST HTTP 400 `MISSING_HEADERS` problem now reads `AUTH_MISSING_HEADERS`, as the HTTP 401 one does, instead of `None`. Migration: none, unless you relied on `None` for that answer.
- **Breaking:** A REST HTTP 403 problem whose contract code refuses the operation rather than the key's credentials, such as `SUBACCOUNT_READ_FORBIDDEN` or `QUICKSWAP_NOT_PERMITTED`, is now `Error::PermissionDenied` with that `code`, the problem's `detail` as `message`, the request path as `endpoint` and the procedure as `context`, instead of `Error::ApiKeyRejected`. This applies to every `Client` service method. Credential refusals (`SIGNATURE_INVALID`, `MISSING_HEADERS`, `KEY_EXPIRED`, `KEY_DISABLED`, `IP_NOT_ALLOWED`, `API_KEY_NOT_ACCEPTED`), `TIMESTAMP_SKEW` and codes outside the contract keep their errors. `Error::auth_code` also reads the auth reason of an `Error::PermissionDenied` code. Migration: match `Error::PermissionDenied { code, .. }` for these refusals; `Error::auth_error_code` no longer returns their code.
- **Breaking:** The crate no longer compiles the generated `proto::swap::quickswap` and `connect::swap::quickswap` modules. Quick Swap is served by the TypeScript SDK only; these modules had no `Client` wrapper, and their generated JSON client failed a whole read on a Quick Swap status newer than the crate. Migration: use the TypeScript SDK for Quick Swap.
- **Breaking:** `InternalTransferResult`, returned by `internal_transfers.create`, gains `status`: `"accepted"`, `"rejected"` or `"failed"`, as the service answered the request; empty when the response names none, and `"UNKNOWN(n)"` for a status this version does not know. Receipts that differed only in their status read the same before. `"accepted"` means the transfer has a durable transfer ID, not that it has settled. Migration: a struct literal of `InternalTransferResult` must set `status`.
- **Breaking:** `heatmap.subscribe_live` now subscribes to the channel the server publishes live buckets on, `public:spot:market:heatmap:<1s|1m|5m|1h>:<symbol_id>:proto`. It used the interval's enum name (`...:heatmap:INTERVAL_1S:...`), which the server acknowledged but never published on, so every interval received nothing. `codecs::heatmap_interval_channel_name` now returns that segment as a `Result<&'static str>` instead of a `String` holding the enum name. `heatmap.subscribe_live`, `heatmap.get` and `heatmap.get_with_options` (through `codecs::resolve_heatmap_interval`) now refuse `INTERVAL_UNSPECIFIED`, and `subscribe_live` refuses any interval other than `1s`, `1m`, `5m`, `1h` or an enum name such as `INTERVAL_1S` with a validation error before connecting. Migration: pass one of those intervals and handle the `Result` if you call `heatmap_interval_channel_name` yourself; code that subscribed to the lowercase channel directly as a workaround can use `subscribe_live` again.
- **Breaking:** `heatmap.get` and `heatmap.get_with_options` request the depth you pass when it is a supported depth (1, 5, 10, 20, 50, 100, 200, 500 or 1000), and any other depth up to 1000 as the smallest supported depth covering it, the rule `orderbook.get` follows. They requested 5 for 0 to 5, so 1 returned 5 levels, and 200 for anything above 100, so 500 and 1000 returned 200 levels. 0 now requests 50 instead of 5. A depth above 1000 is now a validation error before any request, instead of requesting 200. The response's `depth` names the depth requested. `codecs::heatmap_depth_for_levels` follows the same rule and now returns a `Result<HeatmapDepth>`. Migration: pass a depth from 0 to 1000, read `depth` from the response for the depth you got, and handle the `Result` if you call `heatmap_depth_for_levels` yourself.
- **Breaking:** Live market overview rows from `market_overview.subscribe` and `market_overview.create_subscription` now have the `symbol` and the formatted `volume_24h_base` that rows from `market_overview.list` have. A live update replaced the row with one whose `symbol` was empty and `volume_24h_base` was `None`. Migration: none, unless you filled them from the catalogs yourself, which is no longer needed.
- **Breaking:** `MarketOverviewEntry` gains `change_24h_bps`, the 24-hour change of the last price in basis points, and `sparklines`, a `Vec<MarketOverviewSparkline>` (`interval`, such as `"SPARKLINE_24H"`, and `close_prices`, newest first as the server sends them), from `market_overview.list`, `market_overview.subscribe`, `market_overview.create_subscription` and `codecs::decode::market_overview_entry_from_proto`, `market_overview_list_from_proto` and `market_overview_batch_from_proto`. The server sent both, and sent sparklines whenever `include_sparklines` was set, but they were dropped. A managed subscription keeps the snapshot's sparklines when a live update, which carries none, replaces a row. Added `codecs::decode::market_overview_sparkline_from_proto`. A `MarketOverviewEntry { .. }` literal without the new fields no longer compiles. Migration: add `change_24h_bps` and `sparklines` to any `MarketOverviewEntry` you build yourself.
- **Breaking:** `catalogs::Manager::orderbook_price_buckets_for_symbol` returns the buckets of a catalog hydrated from the API, which sends them as `marketdata.orderbookPriceBuckets`; it returned none for every pair. A numeric bucket now reads as a plain decimal with every digit, so BONK-USDT's buckets read `"0.000000001"` to `"0.00001"` instead of `"1e-9"` to `"0.00001"`, and a whole number such as `1.0` reads `"1"` instead of `"1.0"`, as in the Go and Python SDKs. Migration: none, unless you parsed the `"1.0"` or exponent forms.
- **Breaking:** `ExportGuardSignerWalletResult`'s `Debug` output prints `encrypted_private_key: "[REDACTED]"` instead of the private key, so `{:?}`, `{:#?}` and logging the result, alone or inside another value, never reveal it. The type has no `Display` or serde implementation. Read `encrypted_private_key` directly when you need the key. `Ed25519Keypair`'s `Debug` already redacted `secret_key_hex` and `secret_key`. Affected: `GuardSignerService::export_wallet` and `codecs::decode::export_wallet_from_proto`. Migration: none, unless you parsed the key out of the `Debug` output; read the field instead.
- **Breaking:** Errors and diagnostics no longer carry the user name or password of a URL. When the API refused a realtime connection or subscription token with HTTP 403, `Error::PermissionDenied`'s `endpoint` and its message held the token URL with the API URL's userinfo, password included; they now hold it without userinfo and keep the status, code and the rest of the endpoint. `Config`'s `Debug` output prints `api_url` and `ws_url` without userinfo, and as `[REDACTED INVALID URL]` when a URL with an `@` does not parse. HTTP 401 token refusals and REST errors already left the userinfo out. Affected: every private subscription (`AddressBookService::subscribe` and `subscribe_view_invalidations`, `ApiKeysService::subscribe`, `BalancesService::subscribe`, `LifecycleService::subscribe_open_flows`, `OrdersService::subscribe`, `PoliciesService::subscribe`, `subscribe_api_policies` and `subscribe_subaccount_policies`, `SubAccountsService::subscribe` and `subscribe_api_keys`, `TradesService::subscribe`, `TransfersService::subscribe`, `TriggersService::subscribe` and `subscribe_events`), `realtime::Client::subscribe_private_orders`, `subscribe_proto`, `subscribe_proto_batch`, `subscribe_proto_with_options` and `subscribe_raw` on a private channel, and `Config`'s `Debug`. Migration: none, unless you matched `endpoint` against a URL that included userinfo; compare it with the URL without userinfo.
- **Breaking:** `lifecycle.list_flows` with an `account_selector` (owner account or smart account address) now also returns the transfers that account received, such as a transfer from another user or from another sub-account of the same main account, or a reward payout. It used to return only the flows the account owns. A `page_token` is now bound to the request it was returned for: sent with a different account, participation, scope or order it is refused as `Error::Validation`, and tokens issued before this change are refused. `ListFlowsRequest::participation` (`PARTICIPATION_OWNER` or `PARTICIPATION_RECIPIENT`) narrows a list that selects an account. `LifecycleFlowSummary` gains `participation` (`owner` or `recipient` on a list that selects an account, `unspecified` on an unscoped list, a flow detail and a realtime update, empty on a transaction lookup), `source_address` and `destination_address`, from `lifecycle.list_flows`, `lifecycle.get_flow`, `lifecycle.get_flow_by_tx`, `lifecycle.list_flows_by_tx`, `lifecycle.subscribe_open_flows` and `lifecycle.subscribe_flow_detail`, and `codecs::decode::flow_summary_message_from_proto`, `flows_list_from_proto`, `flow_from_get_response`, `flow_from_get_by_tx_response`, `flows_by_tx_list_from_proto`, `flow_summary_from_bytes` and `flow_detail_from_bytes`; a struct literal of `LifecycleFlowSummary` must name them or use `..Default::default()`. Migration: for the previous owner-only list, set `participation` to `PARTICIPATION_OWNER`; for a transfer, read the counterparty from `destination_address` when `participation` is `owner` and from `source_address` when it is `recipient`; and restart pagination from the first page instead of reusing a stored `page_token`.
- **Breaking:** An order or trigger write whose receive window (10 seconds without `Config::recv_window_ms`) ends before it is sent on for admission is now refused with `ERROR_CODE_REQUEST_DEADLINE_EXCEEDED` (308; REST 400 `REQUEST_DEADLINE_EXCEEDED`, ConnectRPC `failed_precondition`) and changes nothing. Such a request used to be processed late. It is an `Error::Api` with code `order_codes::REQUEST_DEADLINE_EXCEEDED`, which `Error::is_request_deadline_exceeded` reports. It is never an unknown outcome, and `Error::is_retryable` now returns true for it. Affected: `orders.create`, `batch_create`, `batch_cancel`, `batch_replace`, `cancel_all_after`, `cancel`, `cancel_with`, `cancel_by_client_order_id`, `cancel_by_order_id`, `cancel_all`, `cancel_all_with` and `modify`; `triggers.create`, `cancel`, `cancel_by_id`, `pause`, `pause_by_id`, `resume`, `resume_by_id`, `resume_with` and `modify`; and `Client::stop_all_trading`. Migration: handle it like `Error::TimestampSkew`: send the request again, which signs it with a fresh timestamp.
- **Breaking:** `Config::recv_window_ms` (and `transport::Config::recv_window_ms`) sets the receive window of every signed API request, sent as the signed `X-API-RECV-WINDOW` header: the server processes a request only until its signing timestamp plus that many milliseconds. It must be from 1 to 60000, and `Client::new`, `Client::from_env` and `transport::Factory::new` refuse any other value with `Error::Validation`. `None`, the default, sends no header, and the server applies 10000 as before. Realtime token requests always use the default. A struct literal of either `Config` must name the field or use `..Default::default()`. Added `Credentials::sign_request_with_recv_window`, `Credentials::sign_request_with_recv_window_async`, `auth::canonical_signing_string_with_recv_window`, `auth::validate_recv_window_ms`, `auth::HEADER_RECV_WINDOW`, `auth::MIN_RECV_WINDOW_MS` and `auth::MAX_RECV_WINDOW_MS` for requests you sign yourself; the window's decimal string is signed on its own line right after the timestamp. Migration: add `recv_window_ms: None` to a `Config` literal that lists every field.
- `auth_codes::RECV_WINDOW_INVALID` and `Error::is_recv_window_invalid` name `AUTH_RECV_WINDOW_INVALID` (REST 401 `RECV_WINDOW_INVALID`, ConnectRPC `unauthenticated` with `auth.v1.AuthErrorDetail`): an `X-API-RECV-WINDOW` header that is not one whole number from 1 to 60000, empty, or repeated. It is an `Error::ApiKeyRejected`, like the other API-key refusals, and not retryable without fixing the window.
- **Breaking:** `auth_codes::REPLAY_CAPACITY_EXHAUSTED` and `Error::is_replay_capacity_exhausted` name `AUTH_REPLAY_CAPACITY_EXHAUSTED` (REST 429 `REPLAY_CAPACITY_EXHAUSTED`, ConnectRPC `resource_exhausted` with `auth.v1.AuthErrorDetail`), which every service that checks signed requests can answer when the API key, or its account, holds its full share of the record that keeps signed requests from being replayed until their receive windows end. It is an `Error::RateLimit` with that code, which `Error::auth_error_code` and `Error::auth_code` read, and `Error::is_retryable` returns true: the credentials are valid, the request was not processed, and the share frees as earlier windows end. It carries no `Retry-After`, so `retry_after()` is `None` unless the response named one: back off briefly and send again, which signs again; a shorter `Config::recv_window_ms` makes room sooner. It used to be an `Error::Api` that was not retryable. Migration: match `Error::RateLimit` for it and retry after backing off (a retry signs with a fresh timestamp) instead of treating `Error::Api` as terminal.
- **Breaking:** A consumer that subscribes to an account's order stream while that stream is reconnecting now gets `PrivateOrdersEvent::Connected` first, then the cached `Snapshot`, `Ready` and `Stale`, as a consumer joining a connected stream does. It got the cached snapshot at once, and `Connected` only after the reconnect, so a consumer that set up its state on `Connected` received orders before it. Affected: `orders.subscribe` and `realtime::Client::subscribe_private_orders`. Migration: none, unless you read the snapshot before `Connected`; set up state on `Connected` and apply the snapshot that follows it.
- **Breaking:** A REST refusal whose code belongs to the transfer, withdraw or ledger service now reads with the `ERROR_CODE_` prefix in `Error::Api`'s `code`, as its typed ConnectRPC detail already did and as order codes already did over REST. `internal_transfers.create` refused with `SAME_SOURCE_DESTINATION` over REST and `ERROR_CODE_SAME_SOURCE_DESTINATION` over ConnectRPC; both now read `ERROR_CODE_SAME_SOURCE_DESTINATION`. Likewise `ERROR_CODE_AMOUNT_BELOW_MINIMUM` from the withdraw writes (`create_to_funding`, `create_to_external_chain`, `create_api_key_to_funding`, `create_api_key_to_external_chain`, `create_wallet_trading_withdraw` and `submit_prepared`) and `ERROR_CODE_INVALID_SUBACCOUNT_ID` from `balances.list`, `get_balance_history`, `get_equity_history` and `list_transfers`, and `transfers.list`. A code no service's `ErrorCode` names stays as sent, and so does a transfer or withdraw code the auth service also names, `INVALID_REQUEST`, which REST can't attribute, so `Error::auth_code` still reads `AUTH_INVALID_REQUEST`. Migration: compare against the `ERROR_CODE_` form, or strip the prefix before comparing.

## 0.13.0

Package version: `0.13.0`. Git tag: `v0.13.0`. Released 2026-11-07. Built on public contract `1bf038e`.

- **Breaking:** `ChangelogPostSummary` (and so `ChangelogPost::summary`) gains `revision` and `updated_at`, from `changelog.list_posts`, `changelog.posts` and `changelog.get_post` (and `changelog_posts_page_from_proto` and `changelog_post_from_proto`). A published post can now be republished, for example to correct its copy or withdraw a change: `revision` starts at 1 and increases with each republication, and `updated_at` is when the current revision was published. Its `slug`, `published_at` and change ids stay the same. A server that predates revisions sends neither: `revision` reads 0 and `updated_at` `None`. To follow edits, read a post again when its `revision` differs from the one you hold. Migration: a struct literal of `ChangelogPostSummary` must set the two fields or end with `..Default::default()`.
- **Breaking:** `changelog.changes` yields each change id once per stream, and `changelog.breaking_changes_since` and `changelog.action_required_since` return each change once. When a post is republished while a walk pages through it, the next page restarts at that post's first change, so changes already received came again and were returned twice. `changes` skips the repeat; the two helpers keep the change at its first position with the wording last received. The helpers still filter on first publication, which a republication does not move, so they don't report an edit to an older post. Migration: none, unless you counted repeats; compare `revision` from `changelog.posts` to find edited posts.
- **Breaking:** A REST `RATE_LIMIT_EXCEEDED` problem now fills the `detail` of `Error::RateLimit` from its `rate_limit` object, for every call, whichever side refused. Every field is optional: the gateway's own refusal sends only `reason` and `scope` (`CLIENT_IP`, with no `operation_id`), and a service's also names the operation, the policy class and the remaining quota. An integer field that is not ASCII digits within the unsigned 64-bit range, such as `"²"` or a 5000-digit string, reads as absent. `Error::retry_after` now prefers the problem's `retry_after_ms` over the `Retry-After` header, as for a ConnectRPC `RateLimitDetail`, and stays `None` when the server gave no hint, as for a refusal of too many concurrent requests. It used to drop the object and read only `Retry-After`. The gateway's ConnectRPC refusal (`resource_exhausted`, "rate limit exceeded, retry later") still has no detail and falls back to `Retry-After` when present. A 429 problem with another code, such as `AUTH_RESOURCE_EXHAUSTED`, stays an `Error::Api` that waiting does not resolve. Migration: wait for `retry_after()` when it is set, which can now be shorter than `Retry-After`, and back off on your own schedule when it is `None`.
- `problem_codes::REQUEST_TOO_LARGE` and `Error::is_request_too_large` name the uniform HTTP 413 refusal that every service now answers for a request body over its size limit, before authenticating it, on known-length and chunked bodies alike. Ledger reads (balances, balance and equity history, transfers) answered `PAYLOAD_TOO_LARGE` instead until server release 9.2.6, so match `REQUEST_TOO_LARGE` from then on. It was already an `Error::Api` with that code and its `http_problem()`, which is not retryable and leaves no unknown outcome: reduce the body to fit the limit. A ConnectRPC message over the limit once decoded keeps its Connect `resource_exhausted` answer.
- `auth_codes::WALLET_ALREADY_LINKED` and `Error::is_wallet_already_linked` name `AUTH_WALLET_ALREADY_LINKED`, which `Error::auth_error_code` and `Error::auth_code` read from the ConnectRPC `auth.v1.AuthErrorDetail` (`already_exists`) and the REST 409 problem alike: a valid wallet login whose wallet is already linked to another account. It is a terminal `Error::Api`, never retryable; log in with the account address the wallet already uses.
- `orders.get_batch_replace_status`, `orders.wait_batch_replace` and `orders.batch_replace` document the 60-second status window: the status is readable as soon as `batch_replace` returns, `unavailable` is retryable (retry the same read within the window): `Error::AdmissionUnavailable` when the server attaches the orders unavailable detail, otherwise `Error::Server`, and `not_found` never means a rejected replacement. After the window, reconcile the individual orders and never send an accepted batch again. An item's `updated_ts_ns` is an observation time that can be a later recovery time. `wait_batch_replace` still polls through `not_found` within the window, for APIs that answered it shortly after admission.
- **Breaking:** An account or sub-account ID string that decodes to 0 now returns `Error::Validation` before anything is sent. `"1"` is the public ID of 0, and `"0"`, `"00"` and the same with spaces read as 0. A configured `default_sub_account_id` of that form was sent as sub-account 0, which the server reads as unset, so the call addressed the main account instead of the sub-account named. An account ID of that form, passed to a subscribe or configured as `default_account_id`, subscribed to the channel of account 0, and an internal transfer's `destination_account_id` or `destination_subaccount_id` was sent as 0. An account ID that doesn't decode at all, such as `"acct_test"`, is now refused the same way instead of naming a channel no account has. No default sub-account, or an empty one, still addresses the main account. Affected, through `default_sub_account_id`: `api_keys.list`; `fees.get_spot_fee_rates`; `internal_transfers.create`; `orders.list_open`, `list_open_with`, `list_history`, `list_history_with`, `get`, `get_with`, `wait_for_order_trades_complete`, `create`, `preview`, `modify`, `batch_create`, `batch_cancel`, `batch_replace`, `get_batch_replace_status`, `wait_batch_replace`, `cancel_all_after`, `cancel_with`, `cancel_by_order_id`, `cancel_by_client_order_id`, `cancel_all` and `cancel_all_with`; `rate_limits.get_trading_rate_limits`; `trades.list` and `list_with`; `triggers.create`, `modify`, `list_with`, `get_by_id`, `cancel_by_id`, `pause_by_id`, `resume_with` and `resume_by_id`; and `withdraw.create_wallet_trading_withdraw`. Through the account ID or `default_account_id`: `api_keys.subscribe`, `address_book.subscribe`, `balances.subscribe`, `lifecycle.subscribe_open_flows`, `orders.subscribe`, `policies.subscribe`, `policies.subscribe_subaccount_policies`, `policies.subscribe_api_policies`, `sub_accounts.subscribe`, `sub_accounts.subscribe_api_keys`, `trades.subscribe`, `transfers.subscribe`, `triggers.subscribe`, `triggers.subscribe_events` and `realtime::Client::subscribe_private_orders`. Added `codecs::parse_account_id`, which parses an account or sub-account ID and refuses 0. Migration: to address the main account, leave `default_sub_account_id` unset instead of setting it to `"1"` or `"0"`, and configure or pass the public ID of a real account wherever an account ID is required.

## 0.12.0

Package version: `0.12.0`. Git tag: `v0.12.0`. Released 2026-11-07. Built on public contract `748066a`.

- `orders.wait_for_order_trades_complete` documents when it reports completion: once the order has ended (`filled`, `canceled` or `rejected`) and its trades add up to `cum_qty`. Its behavior is unchanged, and the Go and Python SDKs now follow the same rule. `executions_complete` alone is not completion, because it is also true for a working order whose fills so far are all recorded.
- **Breaking:** An explicit sub-account ID of 0 returns a validation error before sending instead of being sent: `subaccount_id: Some(0)` in any params or `_with` options, in a request passed to `balances.list`, `balances.get_balance_history`, `balances.get_equity_history`, `balances.list_transfers`, `transfers.list`, `orders.cancel`, `triggers.list`, `triggers.get`, `triggers.cancel`, `triggers.pause`, `triggers.resume` or `triggers.list_events`, and `sub_accounts.get(0, ..)`. Migration: pass `None` to act on the main account; on the wire 0 means no sub-account (the main account).
- **Breaking:** `Order::quote_used` and `Order::quote_unspent` return the exact amount at high base quantity scales whenever it fits a `Decimal`, instead of `None`. They rescaled each amount to 9 plus the base scale in a 128-bit integer before combining them, which overflowed: at base scale 30, a quote budget of 1 at quote scale 6 became 10^39, and at base scale 36 a fee of 0.000001 did too. Trailing zeros are now dropped before rescaling, so only an amount that needs more digits than a `Decimal` holds is `None`, as documented. `Order::cum_quote` is unchanged. Migration: none, unless you treated `None` at those scales as an amount that can't be shown.
- `orders.batch_cancel` and `orders.batch_replace` accept up to 50 items, the limit the public contract sets, instead of 20; 51 or more still return a validation error before sending. `orders.batch_create` keeps its limit of 20.
- **Breaking:** A `BatchReplaceAdmissionItem` from `orders.batch_replace` (and `batch_replace_from_proto`) has an empty `replacement_order_id` when the server sent no successor, as for an amended or rejected item, instead of `"0"`. `old_order_id` is likewise empty when the server named no order. Status items from `orders.get_batch_replace_status` and `orders.wait_batch_replace` already left an absent successor empty, as the Python SDK does. Migration: test for an empty string instead of `"0"`; a successor ID is still set for a replaced item.
- **Breaking:** `triggers.modify` returns a validation error before sending for a `trigger_price` or `activation_price` bound to another market when `symbol` or `symbol_id` is given, as trigger creation and order writes already did. It used to send the price's ticks, so a SOL-USDT price could move a BTC-USDT stop. With only `symbol_id` and a bound price it now waits for the catalogs to learn the market's name. A bound price for a `symbol_id` the catalogs don't know is refused too, since it can't be checked. Without either the trigger's market isn't known and the price is sent as before. Migration: pass a price for the trigger's market, or an unbound one.
- **Breaking:** `Order` from `orders.get`, `orders.get_with`, `orders.list_open`, `orders.list_open_with`, `orders.list_history`, `orders.list_history_with`, `orders.wait_for_order_trades_complete` and `orders.subscribe` (and the `codecs::decode` functions `order_from_proto`, `get_order_from_proto`, `orders_list_from_open`, `orders_list_from_history` and `orders_unit_from_proto`) gains the order's terminal time, origin and MARKET slippage protection, which the server already sent, so struct literals must initialize them. `terminal_ts_ns` is when the order became terminal, in Unix nanoseconds, and is empty while it is open. `origin` (the new `OrderOrigin`) names its `scope` (`direct`, `attached_risk`, `standalone_trigger` or `system`) and, for a child of a trigger or attached-risk leg, its `trigger_type`, `trigger_id`, `parent_order_id` and `child_seq`; it is `None` when the server sends none. A MARKET order's `market_max_slippage` (a `Price` distance) or `market_max_slippage_bps`, and the `market_client_ref_price` it was measured from, are `None` or 0 when the order has none. Migration: add the five fields to any `Order { .. }` literal you build, for example in tests.
- **Breaking:** `orders.wait_for_order_trades_complete` returns `Error::ResponseContract` when a `next_page_token` repeats a page token of the same read, instead of reading the same pages until its timeout, and a read that finishes after the timeout (for example while it decodes) is a timeout instead of a success. Its timeout already bounded every page read, and its result already carried the final read's `executions_complete`. Migration: handle `Error::ResponseContract` like any other failed read, and pass a timeout long enough to read every execution page.
- `changelog.list_changes`, `changelog.breaking_changes_since`, `changelog.action_required_since` and the `changelog.changes` stream accept a time bound before 1970 and send it exactly, as protobuf's own conversion does, instead of returning `Error::Validation`.

## 0.11.0

Package version: `0.11.0`. Git tag: `v0.11.0`. Released 2026-11-07. Built on public contract `748066a`.

- **Breaking:** A zipped-asset supply at a quantity scale of 0 now reads as whole units. `subscribe_zipped_asset_supply` reported a BONK supply of 3582802 as `0.000000000003582802`, because `format_ledger_u64` and `format_ledger_u128` treated scale 0 as 18. They now format at the scale you pass, including 0. A zipped asset that isn't in the hydrated catalog is still refused. Migration: pass `LEDGER_SCALE` for 18-decimal balances.
- **Breaking:** `GuardApproval` gains `nonce_space` and `deadline_unix`, the exact signed values from `sign_protected_action` and `batch_sign_protected_actions`, and `RotateGuardSignerWalletResult` gains `approval`, the rotation's signed approval or `None` when the API returns none. Submit them with the signature. Migration: struct literals must initialize the new fields.
- **Breaking:** `SubaccountPolicy` gains `spot_market_scope`, `max_order_notional`, `max_open_orders` and `trading_halted`, and `ApiPolicy` gains `spot_market_scope`, from policy reads and realtime policy updates. The scope is `"ALL"` or `"ALLOWLIST"`, empty when unspecified and `UNKNOWN(n)` for a newer value; with `"ALL"`, `spot_markets` is for display only. A cap of 0 means no explicit cap. Migration: struct literals must initialize the new fields.
- **Breaking:** `orderbook.get` reports the snapshot depth it requested as `depth`. A depth between snapshot depths requests the smallest one covering it, as before, but reported the depth you passed: `Some(11)` reported 11 for a 20-level snapshot. A depth above 1000 is now an `Error::Validation` before any request, instead of requesting 1000 and reporting the depth passed. `None` and `Some(0)` request 50 explicitly instead of leaving the depth unset for the server's default 50, and still report 50. Migration: read `depth` for the depth you got, pass at most 1000, and trim the levels yourself if you need fewer. Added `orderbook_snapshot_depth` and `ORDERBOOK_SNAPSHOT_DEPTHS` in `polyester::codecs::decode`.
- **Breaking:** `orderbook::apply_delta` refuses a delta with a positive quantity at price zero like any other malformed delta: it changes neither side, keeps the sequence and asks for a snapshot refresh, instead of applying it, advancing the sequence and leaving a book `build_orderbook_data` can't render. A managed subscription therefore rebuilds its book from a fresh snapshot on such a delta and stays open, instead of closing with an invalid-price error. Migration: none needed; code that resubscribed on that error no longer sees it.
- **Breaking:** `LedgerTransfer` from `transfers.list` and `transfers.subscribe` gains `balance_after` and `link_id`, which the server already sent, so struct literals must initialize them. `balance_after` is the account bucket's balance after the transfer, an exact integer at the 18-decimal ledger scale like `amount`; it is `None` when the server sends none, and a zero balance is `Some("0")`. `link_id` is the decimal correlation ID shared by related legs, `None` when there is none. Migration: add `balance_after` and `link_id` to any `LedgerTransfer` you build yourself.
- Private realtime subscriptions (balances, transfers, trades, triggers, trigger events, lifecycle flows, sub-accounts, API keys, API and sub-account policies, and address books) now renew their subscription and connection tokens on the same connection before they expire. The server used to end each one about once a token lifetime with code 2501, or close the connection, and it reconnected, reporting the error and counting a resubscribe every time. A renewal that the token API or the server refuses is reported through the error callback; the server then ends the subscription and it resubscribes as before. `orders.subscribe` already renewed its tokens and is unchanged.
- **Breaking:** `orders.batch_create`, `orders.preview`, `orders.modify` and `triggers.create` check a quantity's scale against the market its symbol names and return `Error::Validation` before sending anything for a mismatch. A symbol with surrounding whitespace used to find its market but not that market's scale, so a scaled `Quantity` bound to no market was checked only against its own scale: 1 BTC written at scale 6 or 10 went out as 0.01 or 100 BTC on a scale-8 market, and a batch sent every item. Migration: build a quantity at its market's base quantity scale, which `client.catalogs.base_quantity_scale_for_symbol` returns, or bind it to the market.
- `orders.create` and `orders.preview` now accept a symbol with surrounding whitespace, as `orders.batch_create` did, and a `Quantity` or `Price` bound to `BTC-USDT` is accepted for `" BTC-USDT "` on every order and trigger write. `orders.create` used to refuse it after encoding, saying the quantity scale was unavailable.

## 0.10.0

Package version: `0.10.0`. Git tag: `v0.10.0`. Released 2026-11-07. Built on public contract `748066a`.

- `orderbook.create_subscription` at a depth the server doesn't publish no longer goes silent after its first view. Depths 5, 10, 25 and 100, for example, were accepted by the server and got no updates, with no error. Any depth from 1 to 500 now subscribes to the smallest published depth that covers it (1, 20, 50, 200 or 500), takes its snapshot there, and trims each view to the depth you asked for. Sequence-gap, availability and bucket behaviour are unchanged, and a view can update when only levels outside it changed.
- **Breaking:** `orderbook.create_subscription` returns `Error::Validation` before connecting for a depth above 500, which used to be clamped to 500 and report that depth, and for `Some(0)`, which used to be clamped to 1. `None` still means 50. Migration: pass a depth from 1 to 500, or read a snapshot of up to 1000 levels with `orderbook.get`, which is unchanged.
- **Breaking:** `orderbook.subscribe_deltas` returns `Error::Validation` before connecting for any depth other than 1, 20, 50, 200 or 500, instead of subscribing to a stream that sends nothing or clamping to 1 or 500. Raw deltas can't be trimmed. Migration: pass a published depth, or use `orderbook.create_subscription` for a book at any depth up to 500.
- Added `ORDERBOOK_STREAM_DEPTHS`, `orderbook_stream_tier` and `require_published_stream_depth` in `polyester_sdk::codecs::decode`, the published stream depths and the checks the client uses.

## 0.9.0

Package version: `0.9.0`. Git tag: `v0.9.0`. Released 2026-11-07. Built on public contract `748066a`.

**Known issue:** a managed order book subscribed at a depth other than 1, 20, 50,
200 or 500 can stop updating with no error. Use one of those depths until the
next release.

- **Breaking:** A new order stream subscription the server never confirms now ends after 30 seconds with `Error::RealtimeSetupTimeout`, instead of waiting forever while heartbeats keep the connection alive. A resubscribe on a confirmed stream that gets no answer reports the same error as an event and subscribes again with backoff.
- **Breaking:** A realtime subscription the server ends no longer stays open and silent. The receiver delivers what is queued and finishes, and `err()`, `recv_result()` and the error callback report the new `Error::SubscriptionEnded` with the channel, the server's code and text, and `reason` (`SubscriptionEndReason`). Update exhaustive matches. Migration: handle the finished receiver; subscribe again if you still need the stream. Server unsubscribe codes from 2500 ask for a new subscribe and reconnect as before.
- A sub-account's balances, transfers, trades, triggers and lifecycle subscriptions now resume after a server unsubscribe while `ListSubaccounts` lists the sub-account with a role, using a fresh token and reporting the gap through `take_resubscribed()`. They stop with the reason after a `removed` update on this client, when not listed, when the list or token is refused, or after 6 attempts. List reads and attempts are paced across the client.
- **Breaking:** Realtime setup now has one deadline, 30 seconds or twice the configured HTTP timeout if longer, covering the connection, both tokens and both confirmations. A server sending only pings previously held setup open forever; it now fails with the new `Error::RealtimeSetupTimeout`. Update exhaustive matches.
- Managed market overviews finish their update stream after terminal failure, after queued rows are drained, keeping the typed error and callback.
- Managed order books request an unavailable snapshot again, after 1 s doubling to 8 s, until it is available or the subscription stops. Repeated unavailable answers add no view. A market that stays unavailable keeps being requested once every 8 s. A failed request ends the subscription like a failed sequence-gap refresh.
- **Breaking:** An HTTP error with no recognised code no longer reports a known failure, because a gateway or an edge can send it too. No code, an empty code, the placeholder `ERROR_CODE_UNSPECIFIED` or `AUTH_UNSPECIFIED`, a service detail with no code or with a code this SDK can't name, unless a recognised refusal Connect code stands behind it, an unrecognised code, Connect `unknown`, `data_loss` or `canceled`, HTML, text or an empty body now return `Error::Server` (`Error::GatewayTimeout` for a bare 504) with outcome unknown and not retryable at every status except 401 and 403, which stay `Error::Auth` or `Error::ApiKeyRejected`. That includes 400, 404 with a JSON body, 405, 409, 412, 422, 500, 501 and 520, which returned `Error::Api`, `Error::RouteNotFound` or `Error::NotImplemented`. Trigger reads still retry 429, 502, 503 and 504; retry other reads yourself if you want to. Creates are sent once. A lowercase unrecognised code at 429 is no longer retryable. A bare 404 stays `Error::RouteNotFound`, now with outcome unknown. A well-formed unrecognised code at 429 stays a known `Error::Api`. REST `INTERNAL_ERROR`, `SERVER_ERROR` and `GATEWAY_MISCONFIGURED` at 500 now return `Error::Server`, REST codes at 413 keep their code instead of `Unknown`, and REST `TRIGGER_NOT_FOUND`, `NOT_FOUND` and other contract codes at 404 return `Error::Api` instead of `Error::RouteNotFound`. Migration: read back after these errors instead of treating them as refusals, and branch on `Error::Api` codes only for codes the contract defines; a validation code newer than this SDK reads outcome unknown until you upgrade, which errs toward reading back.
- **Breaking:** `canonical_signing_string` returns the decoded URL path and a validation error when that path is not valid UTF-8. Migrate by signing with `Credentials::sign_request`, which signs those bytes as they are.
- **Breaking:** A ledger refusal carrying `ledger.read.v1.ErrorDetail` now reports `Error::Api` with its specific code, such as `ERROR_CODE_INVALID_SUBACCOUNT_ID` (11), instead of code `InvalidArgument`. Migrate by branching on the specific code.
- Trigger reads return a 503 `ACCOUNT_ROUTING_INVALID` on the first request with its code, instead of retrying it for two seconds. Real unavailability keeps its bounded retry.
- `sign_request` and `sign_request_async` sign the decoded URL path, as the server verifies it, including percent-escapes that are not UTF-8. A path with percent-escapes no longer gets `401 SIGNATURE_INVALID`. The shared signing vectors now cover escaped paths.
- **Breaking:** Bare REST `UNAVAILABLE` cannot establish an orders-service refusal: report outcome unknown and non-retryable. Only a typed `orders.v1.ErrorDetail` carrying `UNAVAILABLE` establishes a known, retryable refusal. Migration: read back after an unknown outcome; retry reads explicitly.
- **Breaking:** The additional retry-later resource codes `CLIENT_ORDER_IDS_EXHAUSTED`, `CLIENT_ORDER_ID_CAPACITY_REACHED`, `MARKET_LADDER_LIMIT_EXCEEDED`, `MARKET_TRIGGER_LIMIT_EXCEEDED`, `MARKET_TWAP_LIMIT_EXCEEDED`, `NEW_ACCOUNT_CAPACITY_REACHED`, `REQUEST_RECEIPTS_EXHAUSTED`, `RESERVATION_CAPACITY_REACHED`, `TRIGGER_CAPACITY_REACHED`, `TRIGGER_CONTROL_SHARE_EXHAUSTED`, `TWAP_CAPACITY_REACHED` report outcome known and retryable, preserving any server retry delay.
- **Breaking:** Unknown mutation outcomes are never retryable, including bare Connect unavailable/internal/deadline failures, gateway 502/503 responses, bare or HTML 429 responses and client timeouts. Migration: read back after an unknown outcome; retry reads explicitly. Bounded trigger reads, status waits and snapshot recovery use read context; creates are never automatically resent.
- **Breaking:** Structured order-service admission refusals now report outcome known while remaining retryable. This corrects the previous outcome-unknown result for typed `orders.v1.ErrorDetail` codes `UNAVAILABLE`, `FEE_SCHEDULE_UNAVAILABLE`, `POLICY_UNAVAILABLE`, `RATE_LIMIT_POLICY_UNAVAILABLE` and other documented temporary admission refusals. Specific code-only Connect messages now preserve their refusal code. `ACCOUNT_ROUTING_INVALID` is known and non-retryable. Unrecognized dependency failures remain ambiguous.
- **Breaking:** REST validation refusals retain their service codes with outcome known and no retry, while timestamp skew retains its explicit safe-retry guidance. Account repairing retains its explicit safe-retry guidance. REST `RATE_LIMIT_EXCEEDED` and `PRICE_LEVELS_EXHAUSTED` are known admission refusals with retry delays preserved. All REST, typed Connect and message-only resource codes now follow their contract retry guidance; outcome-unknown codes and details take precedence over standalone backoff details.
- **Breaking:** Structured temporary admission refusals use `Error::AdmissionUnavailable { message, code, problem }`; update exhaustive matches. REST refusal codes now remain `Error::Api` with problem metadata.
- **Breaking:** Batch-create items now refuse any subaccount override instead of silently using the batch scope. Migration: set each item's `subaccount_id` to `None` and pass the batch-level `subaccount_id`.
- **Breaking:** `triggers.cancel_by_id`, `pause_by_id` and `get_by_id` now use the configured default subaccount when given `None`, matching the other typed helpers, so `stop_all_trading` cancels triggers in the scope it listed them. Raw-request methods such as `triggers.cancel` still send the request as built.

- **Breaking:** A live order-book delta with a zero or missing price and positive quantity now closes the managed stream with an invalid-price error at every bucket and depth, including when the row would sort outside the requested depth; `bucket_side` and `build_orderbook_data` refuse it too. Migration: handle the stream error and resubscribe.
- Managed order books now display a zero bid bucket when a coarse bucket exceeds the market price, preserving bid floor and ask ceiling rounding. Zero-quantity removals at price zero stay harmless.

- **Breaking:** A plain REST gateway 504 is now non-retryable with mutation outcome unknown. Reads may retry a 504 explicitly; writes read back first. A client order ID enables reconciliation, not exact replay of a single create. Automatic read retries remain operation-specific; creates are never automatically resent.
- **Breaking:** `OVERLOADED` and `SYSTEM_RATE_LIMITED` now return `Error::RateLimit` instead of API/server errors on Connect and REST. They are retryable admission refusals with no unknown mutation outcome. The new `code: Option<String>` field retains the service code; known order codes use the `ERROR_CODE_` prefix. Update constructions and exhaustive field patterns for the new variant field. `ORDER_SEQUENCE_EXHAUSTED` remains a permanent refusal, including REST 429 and Connect responses without typed details. Supplied retry delays are preserved.
- **Breaking:** A Connect message naming an `*_OUTCOME_UNKNOWN` code now classifies as non-retryable with outcome unknown, including codes introduced by a newer server. Known order names are normalized to `ERROR_CODE_…`; future names remain as sent.
- **Breaking:** Unknown REST `*_OUTCOME_UNKNOWN` codes now remain API errors with problem metadata and are non-retryable with outcome unknown. Bare gateway timeouts use the new `Error::GatewayTimeout` variant; update exhaustive matches.

## 0.8.1

Package version: `0.8.1`. Git tag: `v0.8.1`. Released 2026-11-06. Built on public contract `748066a`.

- Order books detect gaps after a sequence-zero snapshot and refetch before applying later deltas.
- Setup queue overflow keeps its typed error, and unknown market selectors explain how to hydrate catalogs.
- Order book snapshots preserve sequence zero as a valid value, independently of availability.
- Managed order books finish their update stream after terminal failure, keeping the error and callback.
- Conflicting order book market name and ID selectors are rejected before connecting.
- Realtime subscriptions preserve publications sent alongside or before the subscribe reply.

## 0.8.0

Package version: `0.8.0`. Git tag: `v0.8.0`. Released 2026-11-06. Built on public contract `748066a`.

- Create results carry the destination pair's catalog scale for resolved base
  quantities, so a raw quantity of 16000 at scale 8 formats as 0.00016.

- Fixed API-key request signing to preserve percent-encoded query bytes outside valid UTF-8 text and sort decoded bytes before encoding, matching the server. Added shared regression vectors for repeated parameters, prefix keys, escaped values and empty queries.

- **Breaking:** transfer history now returns the opaque `next_page_token` string
  instead of an integer `next_cursor`. Copy it into the next `ListTransfersRequest`
  with the same scope, filters and direction. An empty string ends pagination.
- **Breaking:** `EquityHistory` gains a `btc_prices_q` field, so struct literals
  must initialize it. It preserves the returned BTC-USDT closes as integers
  scaled by 1e9, in timestamp order. An omitted series is an empty vector.

- **Breaking:** `ListMarketOverviewOptions` gains `page_token`, so exhaustive
  struct literals must initialize it. Reuse the returned token and filters to
  read the next market-overview page.
- Heatmap history now offers `get_with_options` for explicit time bounds and
  opaque continuation tokens. A continuation omits a new default time window.
- **Breaking:** Trigger creation now rejects options that do not apply to the selected strategy or child execution before sending. Migration: remove incompatible options, or select the trigger type and execution that support them (use `trailing_stop` for trailing distances and activation price).

## 0.7.2

Package version: `0.7.2`. Git tag: `v0.7.2`. Released 2026-11-06.

- REST errors that say an order or trigger outcome is unknown now use the
  same non-retryable API error as Connect. Read the order or trigger back
  before retrying: the first request may still apply. A gateway timeout
  without a recognised outcome code remains a retryable transport/server
  failure with an unknown outcome; reconcile before retrying a mutation.

## 0.7.1

Package version: `0.7.1`. Git tag: `v0.7.1`. Released 2026-11-06.

- Service-unavailable responses now raise retryable server errors on Connect
  for orders, withdrawals and internal transfers, matching REST. This includes
  policy and fee lookups, backlogs, operations in progress and temporarily
  unavailable dependencies. Policy denials and permanent precondition failures
  remain non-retryable. Rate-limit errors keep their retry delay, and unknown
  mutation outcomes remain non-retryable even on an unavailable response.
- These failures now use the existing server error, which does not expose
  the structured service code previously available on API errors.

## 0.7.0

Package version: `0.7.0`. Git tag: `v0.7.0`. Released 2026-11-05. Built on
public contract `748066a`.

- **Breaking:** a trailing stop created with an activation price already
  reached now ends `FAILED` with
  `TRIGGER_FAILURE_REASON_TRIGGER_WOULD_TRIGGER_IMMEDIATELY` (244).
  `ModifyTrigger` and order modifies that would make a trailing stop act at
  once are refused with `ERROR_CODE_TRIGGER_WOULD_TRIGGER_IMMEDIATELY` (302),
  leaving the existing state unchanged. To start trailing from the market,
  clear the activation price with a present zero.
- **Breaking:** remove the public holds-list method, hold models and decoders.
  Read aggregate held trading funds from each asset's `reserved` balance in
  the balances API. Individual hold IDs and expiry times have no replacement.
- **Breaking:** listing events for an unreadable trigger now returns the same
  not-found error as reading that trigger, instead of an empty history.
  Events reads retry a catching-up `UNAVAILABLE` with the trigger-read policy.
- **Breaking:** `SpotFeeRate` gains public fields, so exhaustive struct literals
  must initialize them. `SpotFeeRate` and `SpotFeeRatesList` no longer implement `Eq`.
- Expose reserve fee rates, the next scheduled change and the full fee
  breakdown. Omitted optional components remain absent and omitted account
  discounts decode as an empty list.
- **Breaking:** `Error::PermissionDenied`, `Error::ApiKeyRejected`,
  `Error::TimestampSkew` and `Error::SessionRequired` gain
  optional problem metadata. Include `..` in variant patterns when ignoring it.
- Preserve REST problem status, code, detail and canonical type on realtime
  authorization and early gateway authentication errors.
- Pin execution-history continuation pages to the returned exchange order ID
  so reuse of a client order ID cannot change generations between pages.

**Attached-exit self-trade prevention, sub-account balances (contract
`54dc59d`).** Breaking for code that builds `Order`, `GetSubaccountResult` or
`GetSubaccountOpts` with a struct literal naming every field.
- `GetSubaccountOpts::include_balances` asks a sub-account read for the
  sub-account's balances, and `GetSubaccountResult.balances` returns them as a
  `BalancesList`, the shape of `balances.list`. `None` unless asked for; an
  empty list when the sub-account holds none. It needs the `READ_BALANCES`
  action on the sub-account, or the read fails with `PermissionDenied`; a
  root-scoped API key cannot read a sub-account's balances. A read that
  cannot reach the balances right now fails with the retryable
  `AUTH_TEMPORARILY_UNAVAILABLE`.
- `Order.self_trade_prevention_mode` reports the order's mode
  (`expire_taker`, `expire_maker`, `expire_both`, `UNKNOWN(<n>)` for a newer
  one). The order's attached take-profit, stop-loss and trailing-stop exits
  use it; set it with `CreateOrderParams.self_trade_prevention`.
- An attached exit never trades with the order it protects. When it fires
  while that order still rests, the order's unfilled rest is canceled first
  and the order ends with `cancel_reason` `CANCEL_REASON_ATTACHED_EXIT_FIRED`
  (on reads and on the private order stream).
- A self-trade prevention block of an attached exit is a refused fire, not an
  end: a `fire_refused` trigger event with `fire_refusal_reason`
  `self_trade_prevented`, whose `child_seq` names the child order self-trade
  prevention expired. The leg stays armed. Reject reason
  `ERROR_CODE_SELF_TRADE_PREVENTED` decodes too; no request is answered with
  it.
- A trigger a halted market refuses now reads `market_halted` as its
  `failure_reason` or `fire_refusal_reason`, never the unnamed
  `UNKNOWN_TRIGGER_FAILURE_REASON(212)`. Unknown reasons still decode with
  their number.
- A buy sized by a quote amount can carry attached exits; the server checks
  the legs against the pair's minimums.
- Docs: `Order.cum_notional`, `cum_quote()` and `quote_used()` are exactly the
  settled quote when the pair's `base_quantity_scale` is at most 9; above
  that they can exceed it by less than 1e-18 per fill.

**Exact order totals, refused trigger fires, API-key refusal reasons
(contract `e96897c`).** Breaking for code that builds these types with struct
literals or matches `Error` exhaustively.
- `Order::avg_price()` is the exact average execution price from the new
  `Order.avg_price_e18` (18 decimals; `avg_px` stays rounded down to 1e-9).
  `Order.cum_notional` / `Order::cum_quote(base_quantity_scale)` is the exact
  quote amount of the fills before fees, and `Order.fee_total`
  (`OrderFeeTotal`: `amount_e18` / `amount()`, `is_rebate`, `qty`) their fee
  net of rebates. `Order::quote_used(base_scale)` and
  `Order::quote_unspent(base_scale, quote_scale)` tell how much of a
  quote-sized buy's amount the fills used; `None` until the fee total covers
  every fill. All exact `Decimal`s, zero with no fills, and `None` rather
  than a rounded value when a result needs more digits than a `Decimal`
  holds.
- Trigger events: `fire_refused` (also a `list_events` filter) for a fire
  refused while the trigger stays live, with the new
  `TriggerEvent.fire_refusal_reason` (`rate_limited`, `account_repairing`,
  `market_halted`, `min_notional`, ...). It is not terminal. The failure reason
  `account_repairing` is named. Unknown event types and reasons still read as
  `UNKNOWN(<n>)` / `UNKNOWN_TRIGGER_FAILURE_REASON(<n>)`.
- `Error::auth_code()` returns the typed `auth.v1.AuthErrorCode` of a refusal
  on ConnectRPC (`AuthErrorDetail`) and REST (`code` with `AUTH_` added,
  `UNAUTHENTICATED` as `AUTH_AUTHENTICATION_REQUIRED`);
  `errors::auth_codes::from_rest_code` maps a REST code, and `auth_codes` names
  the seven new values (`SIGNATURE_INVALID`, `MISSING_HEADERS`, `KEY_EXPIRED`,
  `KEY_DISABLED`, `IP_NOT_ALLOWED`, `API_KEY_NOT_ACCEPTED`, plus the existing
  `TIMESTAMP_SKEW`).
- Changed: a ConnectRPC `AUTH_TIMESTAMP_SKEW` detail is the retryable
  `Error::TimestampSkew`, and the other API-key reasons are
  `Error::ApiKeyRejected` with the REST form of the code (`SIGNATURE_INVALID`),
  the same errors the API gateway's answers map to. They were `Error::Api`.
- New `Error::SessionRequired` (`is_session_required()`) for
  `AUTH_SESSION_KIND_NOT_ALLOWED`: a session-only method called with an
  account API key, read from the Connect `permission_denied` detail and the
  REST 403 `code` alike. It was `Error::Api` on Connect and
  `Error::ApiKeyRejected` on REST.
- `Error::Api` gains `violations` (`Error::violations()`): the request fields
  an order refusal names, with ConnectRPC field paths. A missing or wrong
  attached trailing-stop maximum on create, preview or batch create is now
  refused before sending with the server's code, rule ID and field path
  (`ERROR_CODE_BAD_PRICE` / `sell_unset` or `buy_at_least_activation`,
  `ERROR_CODE_TRIGGER_BUY_REQUIRES_LIMIT` / `buy_required`) as `Error::Api`
  instead of `Error::Validation`; a modify names `new_attached_risk...` and
  a batch replace item `items[N].new_attached_risk...`.
- `Error::is_order_already_terminal()` and
  `order_codes::ORDER_ALREADY_TERMINAL` / `ORDER_UNKNOWN`: a cancel of an order
  that has already ended, a repeat of the cancel that ended it included, is
  now always refused with `ERROR_CODE_ORDER_ALREADY_TERMINAL`. Treat it as
  "already done".
- `client.stop_all_trading(scope)` cancels every live trigger, then every open
  order. Cancel-all and the dead-man switch cancel orders only, never
  triggers; `cancel_all`, `cancel_all_after` and `cancel` document it.
- Fixed: with `WireFormat::Json`, a response naming an enum value newer than
  this SDK (a new changelog surface, for example) failed the whole call with
  a retryable `Error::Server`. The value now reads as unset (and is dropped
  from a repeated field), as protojson's `DiscardUnknown` does; binary
  responses keep the value's number.
- Changelog: surface labels `market_data`, `quickswap` and `rewards`;
  `ChangelogSection.surface` names the product area a section covers;
  `list_changes` refuses more than 10 `surfaces` before sending.
- Docs: an API key's `permission_denied` message names the missing policy
  action and reaches the caller as is; a quote-sized buy that spent its amount
  ends `filled`; a ladder whose levels were all canceled before any filled
  ends `cancelled`.

**New: public product changelog (`client.changelog`).** Read the changelog of
the environment you call without an API key. It is available on
devnet now and on testnet with its next release.
- `list_posts`, `get_post` and `list_changes` wrap
  `changelog.v1.ChangelogService`; `posts` and `changes` stream every page.
- `breaking_changes_since(t)` and `action_required_since(t)` collect the
  matching changes published since `t`.
- Typed models `ChangelogPostSummary`, `ChangelogPost`, `ChangelogSection`,
  `ChangelogChange` and `ChangelogChangeEntry`. Enums are lowercase labels and
  unknown values read as `unknown(<n>)`; change ids are base58.
- The generated module tree now compiles `changelog.v1`.

**Batch replace refusals, trigger read-your-writes, owner role lock.**
- `orders.wait_batch_replace(batch_request_id, subaccount_id, timeout)` polls
  `get_batch_replace_status` with a short backoff until no item is
  `admitted`, polling through the `not_found` and `unavailable` answers of the
  first seconds after admission. It stops at the 60-second status window (or
  an earlier `timeout`) with `Error::Transport`: reconcile through the orders
  then.
- `BatchReplaceStatusItem::error_code()` turns an item's `code`
  (`POST_ONLY_CROSS`) into the orders error code (`ERROR_CODE_POST_ONLY_CROSS`),
  keeping a code this SDK does not know unchanged;
  `errors::order_codes::from_status_code` does the same for any string.
- Fixed: `get_batch_replace_status` failed with `Error::ResponseContract` for
  an item admitted and then refused when applied (phase `rejected`, still
  counted in `accepted_count`). It now decodes; an item rejected at admission
  must still read `rejected`.
- A status item with no successor, every `rejected` item included, has an
  empty `replacement_order_id` (was `"0"`). The admission's planned
  `replacement_order_id` is not proof of an order.
- `triggers.get` and `triggers.list` (and `get_by_id`, `list_with`) retry
  `unavailable` / HTTP 503 with the same request: 25 ms, doubling, at most
  500 ms, giving up after about 2 s. The server answers it right after a
  trigger change until the change is readable.
- `errors::auth_codes::SUBACCOUNT_OWNER_ROLE_LOCKED` and
  `Error::is_subaccount_owner_role_locked()`: a member call targeting the
  sub-account's owner. A REST 412 problem with that code now maps to the same
  `Error::Api` as the ConnectRPC `failed_precondition` answer.
- Docs: batch replace items amend in place or replace, as the trading
  WebSocket schema now states; market-halt cancel reasons on orders
  (`CANCEL_REASON_MARKET_HALTED`) and triggers (`market_halted`).

## 0.6.0

Package version: `0.6.0`. Git tag: `v0.6.0`. Released 2026-11-02.

**What a trading bot gains.** A sell order can carry an attached trailing stop
again. That trailing stop buys back, and the server now requires the most its
exit pays; 0.5.0 could not send it, so every sell order with an attached
trailing stop was rejected. Set it with `TrailingStop::with_limit_price`.

This is a minor release because input structs changed shape. The order and
trigger inputs that grow optional fields are now `#[non_exhaustive]` with
constructors, so future optional fields arrive in compatible releases. The Go
and Python SDKs ship the same fix as 0.5.1, without breaking changes.

An environment requires the limit price once it runs the 0.5.0 contract:
devnet first, then testnet after its next upgrade.

**Breaking changes**
- `CreateOrderParams`, `PreviewOrderParams`, `CreateTriggerParams`, `RiskLeg`
  and `TrailingStop` are `#[non_exhaustive]`: code outside the crate can no
  longer build them with a struct literal. Use `CreateOrderParams::new(symbol,
  side, order_type)`, `PreviewOrderParams::new(symbol, side, order_type)`,
  `CreateTriggerParams::new(symbol, trigger_type, side, order_type, qty)`,
  `RiskLeg::market(trigger_price)` / `RiskLeg::limit(trigger_price,
  limit_price)` or `TrailingStop::new(distance)`, then set optional fields
  (`params.price = Some(..)`) or chain the leg setters
  (`.with_activation_price(..)`, `.with_limit_price(..)`,
  `.with_trigger_price_source(..)`, `.with_max_slippage(..)`).
  `OrdersService::create_params` is unchanged. Inputs that derive `Default`
  (`AttachedRisk` and the list and read options) are unchanged: keep
  `..Default::default()`.
- `TrailingStop` has a new public field, `limit_price: Option<Price>`.
- Creating a sell order whose attached trailing stop has no `limit_price` now
  fails locally with `Error::Validation`; the server rejected it already.

### Fixed
- Attached trailing stops send `TrailingStop::limit_price` (or
  `TrailingStop::with_limit_price`) as the trailing stop's maximum buy-back
  price. On order create, preview and batch create, a sell order's trailing
  stop requires it and a buy order's trailing stop rejects it. On every path
  it must be positive and at least `activation_price` when that is set. A
  modify or batch replace does not carry the order's side, so the server
  checks the side rule there. Order reads do not return the limit price yet.

## 0.5.0

Package version: `0.5.0`. Git tag: `v0.5.0`. Released 2026-11-02.

**What a trading bot gains.** An attached take-profit, stop-loss or trailing
stop now says when it starts protecting the position: its trigger events
include `armed`, once, when the parent order's first fill makes the leg live.
A bot can wait for it instead of polling the leg's status. Three new error
codes are final refusals: the request did not apply, so do not resend it
unchanged.

An environment reports `armed` events and the new codes once it runs this
contract: devnet first, then testnet after its next upgrade.

**Breaking changes**
- None in the SDK API.

### Added
- Trigger events decode the new `armed` event type: an attached take-profit,
  stop-loss or trailing-stop leg reports it once, when its parent order's
  first fill makes the leg live (or when a leg paused before that fill
  resumes). It is not terminal. `trigger_event_type_from_label` accepts
  `"armed"` for the event filter. Attached trailing stops still report
  `activated` when they start tracking.
- Error codes `AUTH_SUBACCOUNT_OWNER_ROLE_LOCKED` (the sub-account's owner
  cannot be invited, re-roled or removed),
  `ERROR_CODE_TRIGGER_SYMBOL_MISMATCH` (modify, pause or resume named a listed
  pair other than the trigger's own; name the trigger's pair or omit it) and
  the balance reads' `ERROR_CODE_INVALID_SUBACCOUNT_ID` (a zero or malformed
  sub-account ID).
- Generated messages add `Subaccount.removed` (real-time sub-account updates
  only: the caller lost access, so drop the sub-account),
  `TrailingStopPolicy.limit_price_ticks`, `Checkpoint.opening_value_usd`,
  `ListMyRewardAwardsResponse.awards_may_change` and the Swap reason
  `MAX_ORDER_QUANTITY`, and the trade WebSocket protobufs follow binary schema
  34.

### Server behaviour on this contract
- Batch replace amends an order in place, keeping its order ID and queue
  priority, when an item keeps the price, does not raise the total quantity
  and keeps the client order ID. Otherwise it replaces the order under a new
  order ID.
- An attached trailing stop on a selling order buys back, and the server now
  requires its maximum buy-back price. This SDK's
  `AttachedRisk::trailing_stop` does not send that price yet, so place a
  selling order with only take-profit and stop-loss legs until a later release
  adds it. A buying order's attached trailing stop is unchanged.
- Ladder outcomes: a ladder is completed once no child order remains open and
  at least one reached the book; it is failed only when every child order was
  rejected before reaching the book.

## 0.4.0

Package version: `0.4.0`. Git tag: `v0.4.0`. Released 2026-11-02.

**What a trading bot gains.** An order now says why it ended, in the same
words everywhere. A rejected order's `reject_reason` is the error code the
Orders API returns for the same refusal, and a canceled order's
`cancel_reason` says why it was canceled, on order reads and on the private
order stream alike. Before, the numeric reason used the matching engine's own
numbering, which clashed with the API's error codes. `OrderbookData::available`
tells a bot when a book's levels are unknown rather than empty, so it does not
quote into a book that is being rebuilt. `Trigger::version` picks out the
newest copy of a trigger, and nine new error codes are final refusals: the
request did not apply, so do not resend it unchanged.

This release works on devnet now, and on testnet after testnet's next upgrade.

**Reason labels in this SDK.** Both reasons are full proto enum names.
`reject_reason` is the `orders.v1.ErrorCode` name, the same string as
`Error::Api`'s `code`, for example `ERROR_CODE_POST_ONLY_CROSS`.
`cancel_reason` is the `orders.v1.CancelReason` name, for example
`CANCEL_REASON_USER_REQUEST`. The Go and Python SDKs format these labels
differently.

**Breaking changes**
- `Client::whiteboard`, `Client::polychart` and `Client::layout` are removed.
- The generated `orders.v1.Order` loses `terminal_reason_code` and
  `terminal_reason`, and the private order stream's `OrderEvent` loses
  `reason_code`. Read `reject_reason` and `cancel_reason` instead.
- `OrderStreamEvent::reason_code` is replaced by `reject_reason` and
  `cancel_reason`, both `Option<String>`.
- `GetSubaccountOpts::include_balances` is removed.
- `orderbook::build_orderbook_data` takes the book's availability as a new
  last argument.

### Breaking
- `Client::whiteboard`, `Client::polychart` and `Client::layout` are removed,
  because those APIs are no longer part of the public API.
- The order terminal reason fields are removed: the generated
  `orders.v1.Order` no longer has `terminal_reason_code` or `terminal_reason`,
  and the private order stream's `OrderEvent` no longer has `reason_code`.
  Read the typed `reject_reason` and `cancel_reason` instead, and replace any
  matching on `terminal_reason` text with them.
- `OrderStreamEvent::reason_code` is replaced by `reject_reason` and
  `cancel_reason`, both `Option<String>`. A rejected order's event names its
  refusal with the order error code the Orders API returns (for example
  `ERROR_CODE_POST_ONLY_CROSS`). A canceled order's event names why it ended
  (for example `CANCEL_REASON_USER_REQUEST`), and so does the replaced event
  of the order a replace ended (`CANCEL_REASON_REPLACED`). Both are `None`
  otherwise.
- `GetSubaccountOpts::include_balances` is removed: a sub-account read no
  longer returns balances. Read them with the balances service for the
  sub-account.
- `orderbook::build_orderbook_data` takes the book's availability as a new
  last argument.

### Added
- `Order::reject_reason` and `Order::cancel_reason` on order reads and on the
  orders of the private order stream's snapshots and units. A rejected order's
  reject reason is formatted like `Error::Api`'s `code` for the same refusal;
  a canceled order's cancel reason is a `CancelReason` proto name. Both are
  `None` for every other status, and a value this SDK does not know keeps its
  number (`UNKNOWN_ERROR_CODE(<n>)`, `UNKNOWN(<n>)`).
- `OrderbookData::available`: false while the book is being built or after it
  was invalidated, when its levels are unknown rather than empty. It is the
  server's value as sent, never inferred from the levels; a managed order book
  subscription carries the value of its last snapshot. A server that predates
  the field reports false.
- `Trigger::version`: 1 for the first state, higher for every later state
  change. Keep the copy with the highest version.
- Order error codes `ERROR_CODE_ATTACHED_LEGS_EXCEEDED`,
  `ERROR_CODE_FEE_ASSET_INVALID`, `ERROR_CODE_FEE_CEILING_INVALID`,
  `ERROR_CODE_UNSUPPORTED_FIELD_SET`, `ERROR_CODE_TRIGGER_FIRE_LIMIT_REACHED`,
  `ERROR_CODE_CONDITION_FLAGS_INVALID`, `ERROR_CODE_CONDITION_PARAMETER_SET`,
  `ERROR_CODE_TWAP_CAPACITY_REACHED` and `ERROR_CODE_CHILD_ORDER_INCONSISTENT`
  surface as `Error::Api` codes. Their outcome is known: they are not
  retryable and `mutation_outcome_unknown()` is false.

## 0.3.0

Package version: `0.3.0`. Git tag: `v0.3.0`. Released 2026-10-31.

**What a trading bot gains.** Public trade, fill, ledger transfer and
trigger-event subscriptions now deliver every event; before, all but one
event of each batch was lost. `SpotConfig` and the catalog expose each pair's
order limits as typed values, including the new cap on one market order's
notional, so a bot can size orders before it sends them. A clock-skew
rejection from the API gateway is the retryable `Error::TimestampSkew` and no
longer ends a private order stream for good; other API-key rejections keep
their code. A duplicate client order ID names the order that already holds
it, replacement orders report the quantity they inherited, and slippage caps
and trigger inputs are checked before sending the way the API checks them.

This release works on devnet today, and on testnet after testnet's next
upgrade, which brings the new private order stream.

**Breaking changes**
- Batched subscriptions yield every event, and the single-event decoders
  (`market_trade_from_bytes`, `user_trade_from_bytes`,
  `ledger_transfer_from_bytes`, `trigger_event_from_bytes`) are replaced by
  their `..._batch_from_bytes` versions, which return one result per event.
- `spot_config_from_proto` returns `Result`.
- Connect `unimplemented` and a bare HTTP 501 are `Error::NotImplemented`
  instead of `Error::RouteNotFound` or `Error::Api`.
- `max_slippage_bps` is at most 9999 for a market order that sells, every
  trailing stop and `triggers.modify`.
- The `mark` trigger price source is rejected locally.
- A buy trailing stop's `limit_price` must be at least its
  `activation_price`.
- Public structs gain fields, so code that builds them with a struct literal
  must set them.

### Breaking
- **Batched realtime publications deliver every event.** Public trades,
  user fills, ledger transfers and trigger events arrive as batches; before,
  each batch was decoded as one event and the rest were lost. The
  subscriptions now yield every event in order. The single-event decoders
  `market_trade_from_bytes`, `user_trade_from_bytes`,
  `ledger_transfer_from_bytes` and `trigger_event_from_bytes` are replaced by
  `market_trade_batch_from_bytes`, `user_trade_batch_from_bytes`,
  `ledger_transfer_batch_from_bytes` and `trigger_event_batch_from_bytes`,
  which return one result per event.
- `spot_config_from_proto` returns `Result`: a malformed order limit is an
  error instead of an untyped value.
- Connect `unimplemented` and a bare HTTP 501 are `Error::NotImplemented`
  instead of `Error::RouteNotFound` or `Error::Api`. A bare HTTP 404 is still
  `Error::RouteNotFound`; `Error::is_not_implemented()` is true for both.
- Slippage caps match the API. A market order that sells, every trailing
  stop (standalone or attached, buy or sell) and `triggers.modify` take at
  most 9999 bps; they accepted 10000, which the API rejects. A buy market
  order, a buy stop-loss or take-profit market child and a TWAP slice still
  take 10000.
- The `mark` trigger price source is rejected locally (standalone triggers
  and attached legs): spot markets have no mark price.
- A buy trailing stop's `limit_price` must be at least its
  `activation_price`.
- Public structs gain fields (see Added), so code that builds them with a
  struct literal must set them.

### Fixed
- **API-gateway rejections keep their code.** The gateway rejects API-key
  requests with a plain JSON body such as `401 {"code":"TIMESTAMP_SKEW"}`.
  It was reported as a generic `Error::Auth` with no code, so a clock-skew
  rejection was not retryable and ended a private order stream for good. Now
  `TIMESTAMP_SKEW` is the retryable `Error::TimestampSkew`, and any other
  code (`SIGNATURE_INVALID`, `KEY_DISABLED`, `IP_NOT_ALLOWED`, ...) is the
  non-retryable `Error::ApiKeyRejected` with its code and HTTP status. This
  covers Connect calls and the realtime token requests.
- A market overview subscription created with `symbols` keeps only those
  markets in buffered and live updates, not only in its first snapshot.

### Added
- Typed spot-config order limits: `SpotConfig.pair_order_limits`
  (`PairOrderLimits` with `max_limit_qty_base`, `max_market_qty_base`,
  `max_order_notional_quote`, `max_market_notional_quote` and
  `max_open_orders_per_pair`) and `SpotConfig.max_open_orders_total`. The
  decimal limits are `None` when the pair has no maximum. The catalog
  exposes the same values: `catalogs.pair_order_limits_for_symbol()`,
  `pair_order_limits_for_symbol_id()` and `max_open_orders_total()`.
- `Order.inherited_cum_qty`: the filled quantity a replacement inherited
  from its predecessors.
- `action_taken` on `BatchReplaceAdmissionItem` and `BatchReplaceStatusItem`:
  `replaced`, or `amended` for a cancel-only outcome with no successor.
- A duplicate client order ID rejection names the order that already holds
  the ID, when the server knows it: `Error::existing_order_id()` for single
  creates and `BatchCreateResultItem.existing_order_id` for batch creates.
- `LedgerTransfer.transfer_id`, shared by both legs of a transfer.
- `triggers.modify` and `triggers.resume_with` / `resume_by_id` no longer
  need a symbol; without one they target the trigger's own market.
- `TriggerTrailingDetails.trigger_price`: a trailing stop's current
  threshold.
- `VIPStatus.settled_volume_quality` and `average_aop_quality`
  (`VIPMetricQuality`: `status` and `incomplete_hours`). When the volume's
  status is `price_unavailable`, `settled_volume_30d_usd` is a lower bound.
- `realtime::Client::subscribe_proto_batch` for batched channels. An event
  that fails to decode is reported through the subscription's error
  callback, and the rest of its batch is still delivered.
- `auth_codes::TIMESTAMP_SKEW`; `Error::auth_error_code()` also returns the
  gateway code of `Error::TimestampSkew` and `Error::ApiKeyRejected`.

### Internal
- `sdk-capabilities.json` no longer carries `publicDescriptorSha256` or `generatedAt`, so a proto publication that only changes `src/gen/` and `manifest.json` no longer makes it stale; `manifest.json` stays the single source of the descriptor hash.

## 0.2.1

Package version: `0.2.1`. Git tag: `v0.2.1`.

### Breaking/Safety
- **Outcome-unknown errors are no longer retryable; read the order back and
  reuse the idempotency key.** `Error::is_retryable()` now returns false for
  every order error code ending in `_OUTCOME_UNKNOWN`
  (`ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN`,
  `ERROR_CODE_ORDER_CANCEL_OUTCOME_UNKNOWN`,
  `ERROR_CODE_ORDER_MODIFY_OUTCOME_UNKNOWN`,
  `ERROR_CODE_TRIGGER_CREATE_OUTCOME_UNKNOWN`,
  `ERROR_CODE_TRIGGER_CONTROL_OUTCOME_UNKNOWN`). In `0.2.0` it returned
  true, so a generic retry loop could send a create again and place a
  second order. `Error::mutation_outcome_unknown()` stays true for them.
  This matches the Go, Python and TypeScript SDKs.
- `order_codes::OUTCOME_UNKNOWN` is now a `static` read from the generated
  `orders.v1.ErrorCode` names ending in `_OUTCOME_UNKNOWN`, instead of a
  hand-written `const` list, so a code added to the API is covered when the
  SDK is regenerated. It still dereferences to a `[&str]` slice
  (`.contains()`, `.iter()`); it can no longer be used in a `const`.

### Fixed
- `ERROR_CODE_TRIGGER_CONTROL_OUTCOME_UNKNOWN` (a trigger cancel, pause,
  resume or modify that was sent but not confirmed in time) was missing from
  `order_codes::OUTCOME_UNKNOWN`, so `mutation_outcome_unknown()` returned
  false for it. It now returns true.

### Changed
- `Error::is_retryable()` returns true for `ERROR_CODE_ACCOUNT_REPAIRING`:
  the account's orders are paused for a short time while its balances are
  brought back in line, nothing changed, and the same request can be sent
  again shortly. `ERROR_CODE_ORDER_REPAIRING` stays non-retryable.
- New constants `order_codes::TRIGGER_CONTROL_OUTCOME_UNKNOWN` and
  `order_codes::ACCOUNT_REPAIRING`.
- The crate description now reads "Rust SDK for the Polyester API with
  generated protobuf types and ConnectRPC clients."

## 0.2.0

Package version: `0.2.0`. Git tag: `v0.2.0`.

This release replaces `0.1.0-alpha.49` and covers every change since it.
From `0.2.0` on, the Go, Python and Rust SDKs share one version number and
are released together. Versions are plain SemVer (`0.2.0`, `0.2.1`, ...) and
git tags are `vX.Y.Z`; the alpha pre-release suffixes (`0.1.0-alpha.N`, tag
`v0.1.0aN`) are retired. Upgrade from any `0.1.0-alpha.*` release: those
releases use hosts that no longer exist and a price scale 1,000 times off
against the current API.

### Breaking
- **Trading prices now use the published fixed scale of 9 decimals: one
  price tick is 0.000000001 quote units.** `PRICE_TICK_SCALE` is `9`, so
  `Price::from_decimal_str`, `Price::as_decimal`, `Price::format`, order,
  trigger, trade, orderbook, market-overview and primary candle prices
  encode and decode at 1e-9. Earlier releases used 1e-6 and are 1,000
  times off against the current API. Code that builds `Price::from_ticks`
  values by hand must multiply its old tick values by 1,000.
- `OrdersService::subscribe` streams account orders over the private
  WebSocket (`/v1/ws/private`) and returns a `PrivateOrdersSubscription`
  of `PrivateOrdersEvent`s (`Connected`, `Snapshot`, `Unit`, `Ready`,
  `Stale`, `Disconnected`, `Error`) instead of a `TypedSubscription<Order>`. The
  realtime order channels it used are retired and no longer issue
  subscribe tokens. Snapshot and unit orders use the same `Order` model as
  the order reads; units also carry removed order IDs and order events.
  The call returns at once: the handshake result arrives as `Connected` or
  `Error`. The stream resumes from its last position after a gap or a
  reconnect, and calls for the same account share one server subscription.
  `Ready` arrives once per server subscription when the stream reaches the
  account's read boundary; wait for it before relying on the stream. A
  consumer that joins a ready stream gets it right after its catch-up
  snapshot. `Stale` carries an `OrdersStale` with `stale`, the lowercase
  `reason` (`view_behind`, `order_unavailable`, ...) and `order_ids`: with
  `order_unavailable` only those orders are not current, the rest of the
  view is and units keep arriving. A later report replaces the earlier one,
  and a consumer that joins a stale stream gets the current report.
- Removed `codecs::decode::order_from_bytes`, the decoder for the retired
  order channel's publications.
- `ModifyTriggerParams.limit_price` is removed: a trigger's child limit
  price is fixed at create, and the API rejects any modify that sets it.
- `CancelAllOrdersResult`, `CancelAllAfterResult`, `GetOrderResult`,
  `LifecycleFlowSummary`, `MarketTradesResult`, `GetTradesOpts` and
  `Trigger` gain fields (below). These structs are not `#[non_exhaustive]`,
  so code that builds them with struct literals must add them.

### Fixed
- `POLYESTER_TESTNET_ENVIRONMENT` and `environment_from_name("testnet")` now
  use `https://api.testnet.polyester.com` and `wss://api.testnet.polyester.com`.
  Earlier versions pointed at `api-testnet.polyester.com`, which does not
  exist, so every testnet call failed with a DNS error. The devnet preset and
  `DEFAULT_API_URL` / `DEFAULT_WS_URL` now use the documented
  `api.devnet.polyester.com` host.
- An order create, trigger create, order cancel or order modify that was sent
  but not confirmed in time (`ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN`,
  `ERROR_CODE_TRIGGER_CREATE_OUTCOME_UNKNOWN`,
  `ERROR_CODE_ORDER_CANCEL_OUTCOME_UNKNOWN`,
  `ERROR_CODE_ORDER_MODIFY_OUTCOME_UNKNOWN`) now reports
  `mutation_outcome_unknown()` and `is_retryable()` true. It may still apply:
  read the order or trigger back before retrying. Earlier versions reported
  it as a definite failure. `errors::order_codes` lists these codes.
- `AUTH_TEMPORARILY_UNAVAILABLE` is now retryable: the account request
  changed nothing, so send the same request again.
- **Stop-loss and take-profit triggers with a market child now send their
  slippage cap.** `max_slippage_ticks` / `max_slippage_bps` were dropped on
  create, so the child used the pair's default market slippage instead of
  the requested cap. At most one of them is accepted, a sell child takes
  fewer than 10000 bps, and a limit child rejects them.
- Order reads no longer report an attached-risk leg whose state is
  `not_configured`; the server sends that state for a leg the order does
  not have.
- An API error whose order, auth, withdraw or transfer code is newer than
  this SDK version now reports `code = "UNKNOWN_ERROR_CODE(<n>)"` instead
  of `ERROR_CODE_UNSPECIFIED` or `AUTH_UNSPECIFIED`, so the server's code is
  never lost.

### Added
- Index-price triggers and attached legs: `CreateTriggerParams.trigger_price_source`
  (`last`, `index` or `mark`, the labels trigger reads use) is sent for
  stop-loss, take-profit and trailing-stop triggers, and
  `RiskLeg.trigger_price_source` / `TrailingStop.trigger_price_source` are
  sent on order create and modify. `None` watches the last trade price.
  Order reads now fill these leg fields, which are no longer deprecated, so
  a read leg can be sent back unchanged. An unknown label is a validation
  error, and TWAP and ladder triggers reject a price source.
- `CancelAllOrdersResult.listing_status`: `included`, or
  `not_retained_read_open_orders` when a retry with the same `request_id`
  returns the original counts with an empty `cancelled_orders`. An empty
  retried list does not mean nothing was canceled.
- `CancelAllAfterResult.effective_scope`: the account, symbol and side the
  armed switch cancels (`DeadManSwitchScope`); `None` when disabled.
- `GetOrderResult.executions_complete`: true when the order's trades across
  every page add up to its filled quantity.
- `LifecycleFlowSummary.is_delayed`: an open flow that has made no progress
  for well beyond its expected step time. Lifecycle reason
  `deposit_source_not_confirmed`.
- Trigger reads name the cancel reasons `account_repair`,
  `pair_drain_trigger`, `pair_drain_twap`, `pair_drain_ladder` and
  `pair_drain_attached_leg`, and the failure reasons
  `index_price_unavailable` and `trigger_would_trigger_immediately`. Spot
  config reports halted pairs as `PAIR_STATUS_HALTED`.
- Standalone BUY trailing stops: `CreateTriggerParams.limit_price` is the
  required maximum price of a BUY trailing stop (`limit_price_ticks`), and
  must be `None` for a SELL one. Trigger reads show a BUY trailing stop
  as an IOC limit child (`order_type` `limit`, `time_in_force` `ioc`) with
  the maximum price as `Trigger.limit_price`; the trailing details carry the
  trough price.
- `Trigger.max_slippage`: the slippage cap of a stop-loss or take-profit
  market child, or of a trailing stop.
- `CancelAllOrdersResult.cancelled_orders` (order ID and client order ID,
  oldest first, at most 200), `total_cancelled` and `truncated`.
- Gap-free trade backfill: `GetTradesOpts.after_trade_id` (not combinable
  with `start`, `end` or `page_token`), and
  `MarketTradesResult.complete_through_trade_id`, `next_after_trade_id` and
  `epoch_ends`. Zero is a valid cursor and watermark.
- `OrderMutationResult` (order create), `ModifyOrderResult` and accepted
  `BatchCreateResultItem`s carry `take_profit_trigger_id`,
  `stop_loss_trigger_id` and `trailing_stop_trigger_id`: the public ID of
  each attached-risk leg's trigger, and `None` for a leg the order does not
  have. These structs are not `#[non_exhaustive]`, so code that builds them
  with struct literals or destructures them exhaustively must add the new
  fields.
- `realtime::Client::subscribe_private_orders(account_id)` and
  `proto::privatews` (the private WebSocket contract).
- Trigger events report `event_type = "activated"` when a standalone
  trailing trigger's activation condition is met and tracking begins.
  `trigger_event_type_from_label("activated")` filters for them.
- The generated `proto` and `connect` modules now include `checkpoints::v1`,
  `claims::v1`, `rewards::v1`, `swap::v1` and `swap::quickswap::v1`. Their
  code was already in the package but was not compiled into the crate.
  These services have no `Client` wrappers: they need an interactive
  session or a broker key, which this API-key SDK does not use.

### Changed
- `MaxSlippage::Ticks` and the trigger create/modify `max_slippage_ticks`
  are now `i64` absolute price ticks, matching the 64-bit contract fields.
- Candle and market-overview base volume now use the base asset
  `market_data_volume_scale` from `get_spot_config`. Primary candle prices
  use the fixed price scale of 9.
- Composite reference-candle OHLC uses the pair `reference_price_scale`.
  `GetCandlesOpts.include_reference` requests that series.
  `MarketOverviewEntry.volume_24h_base` is the decoded decimal when the
  catalog scale is known; `volume_24h_base_scaled` stays the integer.
- The crate documentation link and the README links to the SDK capability
  matrix and market order price protection point at the public docs on
  `polyester.com`.

## 0.1.0a49

Package version: `0.1.0-alpha.49`. Git tag: `v0.1.0a49`.

### Fixed
- Limit-order create now rejects `market_max_slippage` instead of dropping
  the supplied protection.

## 0.1.0a48

Package version: `0.1.0-alpha.48`. Git tag: `v0.1.0a48`.

### Added
- Limit GTD orders: `CreateTimeInForce::Gtd` plus required `expire_at`
  encode `OrderIntent.limit_gtd` (price, optional `post_only`, 1s–30d
  expiry). Order reads expose `expire_at` for GTD and omit it otherwise.

### Changed
- Generated Connect/protobuf bindings now include
  `ClaimsService.GetDailyClaimStatus` and `ClaimDailyReward`. Those RPCs
  stay JWT/session-only and are not wrapped in this API-key SDK.

## 0.1.0a47

Package version: `0.1.0-alpha.47`. Git tag: `v0.1.0a47`.

### Breaking
- `POLYESTER_TESTNET_ENVIRONMENT` is now public testnet
  (`api-testnet.polyester.com`, chain `888169`). The previous preset (devnet
  URLs, chain `888168`) is `POLYESTER_DEVNET_ENVIRONMENT` and remains the
  client / chain-helper default. Environment fields are owned `String`s.

### Added
- Paginated order lineage history: `Order.lineage` / `UserTrade.lineage`
  (`id` + one-based `generation`), `GetOrderResult.transfers` /
  `next_page_token`, and `UserTradesList.transfers`. `GetOrderOpts` accepts
  `include_execution_history`, `limit`, and `page_token`.
  `ListUserTradesOpts` accepts mutually exclusive `order_id` / `lineage_id`,
  optional `through_generation` (requires `lineage_id`), and
  `include_transfers`. `wait_for_order_trades_complete` pages execution
  history; an empty page is not a settlement watermark. `cum_qty` /
  `avg_px` are lineage-cumulative.
- First-class `Config.environment` / `POLYESTER_DEVNET_ENVIRONMENT` /
  `POLYESTER_TESTNET_ENVIRONMENT`. `create_polyester_environment` and
  `PolyesterEnvironment::with_urls` support custom / VPC endpoints.
  `Client::from_env` reads `POLYESTER_ENV=devnet|testnet`.

## 0.1.0a46

Package version: `0.1.0-alpha.46`. Git tag: `v0.1.0a46`.

### Breaking
- `DepositWithdrawConfig.polyester_chain_id` is removed. The zipper config
  no longer publishes a dedicated Polyester chain id.
- `JsonRpcClient::new` now returns `Result` and rejects remote `http://`
  endpoints. Use loopback HTTP, HTTPS, or `JsonRpcClient::new_insecure`.
- Remote `http://` API bases and `ws://` realtime URLs are rejected unless
  `Config::allow_insecure_http` is set (or `POLYESTER_ALLOW_INSECURE_HTTP=1`
  for `Client::from_env`). Loopback plaintext is unchanged.

### Added
- `MarketOverviewService::get_currency_conversion_config` and
  `get_currency_conversion_rates` wrap the public currency-conversion RPCs.
  Fiat `units_per_usd_e8` is currency units per 1 USD at 1e8 scale (USD
  identity is 100_000_000). Stablecoin `usd_per_unit_e8` is observed USD
  per unit at the same scale. A missing fiat snapshot or omitted
  stablecoin is unobserved, not zero. Rates fail with unavailable
  (HTTP 503) before any observation exists. Fraction digits are
  presentation defaults, not rate precision.

### Changed
- Document that a reused `client_order_id` on create is a conflict after the
  first admission (`CONFLICT_DUPLICATE_CLIENT_ORDER_ID`). Reconcile with
  `get` / `list_open` before creating again.
- Address-book live create uses a generated external destination (not the
  internal-transfer account-id fixture) and treats `write denied` as a
  missing `MANAGE_ADDRESS_BOOK` scope.
- MSRV is Rust 1.90 (was 1.88).

### Security
- Close SDK01-011 through SDK01-014: Connect, realtime token exchange,
  websocket, and chain JSON-RPC no longer silently use remote plaintext.
- Bump lockfile `h2` to 0.4.19 (RUSTSEC-2026-0258) and `ruint` to 1.20.0
  (RUSTSEC-2026-0220). `derivative`, `paste`, and `rkyv` remain transitive
  and unmaintained.

## 0.1.0a45

Package version: `0.1.0-alpha.45`. Git tag: `v0.1.0a45`.

### Breaking
- `OrdersService::cancel_all_with` now sends repeated Connect `symbol_ids`
  instead of a single `symbol_id`. `CancelAllOpts` accepts `symbol`,
  `symbols`, `symbol_id`, or `symbol_ids` (mutually exclusive once any
  selects a pair). An empty collection still matches all symbols; at most
  100 positive IDs are accepted and duplicates are ignored. Reuse
  `request_id` only when retrying identical cancellation criteria, and
  handle `ERROR_CODE_CANCEL_REQUEST_EXPIRED`.

### Added
- `MarketOverviewService::get_spot_volume_history` wraps the public
  `GetSpotVolumeHistory` RPC (trailing 24-hour USD volume samples). Omit
  filters for every configured pair, or send up to 2,000 distinct positive
  pair IDs.
- Candle decode now preserves exact traded `quote_volume` decimal strings
  on `CandlePoint` and columnar candle responses.
- Market overview rows expose optional `volume_24h_base_scaled`,
  `volume_24h_quote_scaled`, and canonical `volume_24h_usd_scaled` (1e6 USD
  scale). Absent values stay `None` when the signed 64-bit amount or USD
  valuation is unavailable.
- Ladder trigger details decode `executed_levels` and `executed_qty`.
- TWAP market-IOC create can set per-slice `max_slippage_ticks` or
  `max_slippage_bps`.
- Connect withdraw and internal-transfer failures unpack the new
  `ErrorDetail` / `ErrorCode` types.

### Changed
- Generated Connect/protobuf bindings now include `AuthService.AcceptTerms`.
  That RPC stays JWT/session-only and is not wrapped in this API-key SDK.
  `AUTH_TERMS_NOT_ACCEPTED` is surfaced from Connect error details.

## 0.1.0a44

Package version: `0.1.0-alpha.44`. Git tag: `v0.1.0a44`.

### Changed
- Generated Connect/protobuf bindings now include
  `GetPortfolioEquityHistorySeries` and `GetPortfolioEquitySnapshot`. Those
  LedgerRead RPCs stay JWT/session-only (interactive root-account session)
  and are not wrapped in this API-key SDK.
- Equity history decode maps the additive `EquitySeries.portfolio_account`
  grouping (`account_id`, `remaining`) so the shared series type keeps
  portfolio identity instead of dropping it.
- Trigger modify proto validation is stricter: trigger price and trailing
  distance must be positive, and slippage / trailing BPS stay in 1–10000.

## 0.1.0a43

Package version: `0.1.0-alpha.43`. Git tag: `v0.1.0a43`.

### Fixed
- Qty/price strings with trailing zeros past scale are accepted when the
  value is exact after trim. `"1.500000000"` at scale 8 matches `"1.5"`;
  `"1.500000001"` still rejects. This is the string path, not `Decimal`.

## 0.1.0a42

Package version: `0.1.0-alpha.42`. Git tag: `v0.1.0a42`.

### Breaking
- `MeResult.api_key_id` is the public `ak_` + 32-hex string from
  `MeResponse.api_key_id`, not a formatted numeric ID. It is absent when the
  call was not API-key authenticated.
- Subaccount `status` is decoded from `SubaccountStatus` (`active` /
  `disabled`; unspecified is empty). `GetSubaccountOpts.invites_direction`
  encodes `incoming` / `outgoing` onto `SubaccountInviteDirection` and
  rejects unknown labels.
- Cancel, cancel-all, cancel-all-after, and batch-cancel outcomes decode from
  closed proto enums onto the existing public labels (`accepted`,
  `submitted`, `dry_run`, `armed`, `disabled`, `rejected`). Unspecified or
  unknown values fail closed.

### Changed
- Generated Connect/protobuf bindings now include `GenerateUsernameOptions`
  and `ClaimGeneratedUsername`. Those Profile RPCs stay JWT/session-only and
  are not wrapped in this API-key SDK.

## 0.1.0a41

Package version: `0.1.0-alpha.41`. Git tag: `v0.1.0a41`.

### Breaking
- `RiskLeg.trigger_price` and `TrailingStop.distance` are now optional so
  state-only attached-risk responses remain representable without fabricated
  policy values. Both fields remain required for writes. `RiskLeg` and
  `TrailingStop` also gain an optional `state` field; exhaustive struct
  literals must initialize it (normally `None` for writes).

### Added
- Attached-risk take-profit, stop-loss, and trailing-stop legs expose typed
  runtime state (`status`, armed/terminal nanosecond timestamps, trigger ID,
  and child order ID). A decoded leg is retained when it has usable policy data
  or meaningful runtime state, including state-only responses. Empty wrappers,
  zero-price TP/SL policy, and missing/nonpositive trailing distance are
  omitted when no state exists. Legacy responses containing both stop-loss and
  trailing legs preserve both instead of hiding stop-loss.

### Fixed
- Market-IOC and attached trailing-stop `max_slippage_bps`, plus standalone and
  attached `trailing_distance_bps`, enforce the inclusive 1–10000 range.
  Standalone trigger modify retains `max_slippage_bps=0` as its explicit clear
  sentinel.
- Batch create rejects duplicate non-empty `client_order_id` values locally;
  omitted and blank IDs remain allowed.
- Stale or equal order-book reset deltas cannot clear or rewind a newer
  snapshot. Newer/overlapping resets still replace the book, while malformed
  resets request refresh without mutation.
- `oco=true` attached risk requires take-profit plus exactly one stop-loss or
  trailing-stop leg; invalid one-leg policies fail instead of being downgraded.
- API base URLs containing a query string or fragment fail during client
  construction, before Connect procedure joining or request signing.

## 0.1.0a40

Package version: `0.1.0-alpha.40`. Git tag: `v0.1.0a40`.

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
- `LedgerTransfer.source` / `destination` (`TransferSide`) including optional
  `chain_id` for external-address sides. This is the Zipper
  `ChainConfig.chain_id`, not an EIP-155 or Polyester chain id.

### Changed
- `Order.orig_qty` is documented as the current accepted total (updates after
  a successful modify). Batch-create `request_id` is the batch idempotency
  key; item `client_order_id` remains optional.
- Trigger modify omits `activation_price` / `max_slippage_*` to preserve
  current values and sends explicit zero to clear them. Create/modify reject
  `max_slippage_bps` outside 1–10000; modify still accepts `0` to clear.

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
