# Changelog

## Unreleased

- **Breaking:** The trading WebSocket speaks SBE schema 50 (semantic version 2.6.0), which the server now requires: it refuses every version 49 frame. `CancelAllOrders` gains `include_triggers` (`bool`, `false` by default and from `CancelAllOrders::new`, with the `with_triggers()` builder): `true` also stops the account's waiting triggers in the request's scope, each ending canceled with reason `cancel_all`. `CancelAllAck` gains `matched_triggers`, the triggers that matched (zero without `include_triggers`). New `TradeSession::cancel_all_after_with` and `TradeSender::cancel_all_after_with` take `CancelAllAfterOptions` (new, `include_triggers`), so the dead-man switch can stop triggers too when it expires; `cancel_all_after` keeps cancelling orders only. Migration: upgrade before the server moves to this release; set `include_triggers` (or `..CancelAllOrders::new(..)`) where you build a `CancelAllOrders` or a `CancelAllAck` literally. Nothing else changes unless you ask to stop triggers.
- **Breaking (server behaviour):** A ledger read refused because the API key policy, sub-account policy or sub-account role does not grant its action (`balances.list`, the balance history, equity and `transfers` reads, and `GetPortfolioEquitySnapshot`) now answers REST `403` `PERMISSION_DENIED` (type `.../permission-denied`), whose detail names the action to grant, such as `API key policy does not grant READ_BALANCES`; the REST codes `API_KEY_ACTION_DENY`, `ROLE_ACTION_DENY` and `SUBACCOUNT_POLICY_DENY` (and their type URIs) are gone from the contract, and the SDK no longer recognises them as contract codes. Over ConnectRPC, which this SDK uses, the refusal is unchanged (`permission_denied`, `ERROR_CODE_PERMISSION_DENIED`) except that its message names the action; statement exports and account-flow reads keep `PERMISSION_DENIED` with the new detail text. Migration: match `PERMISSION_DENIED` (`Error::PermissionDenied`, or `Error::auth_error_code()`) instead of the three removed codes, and read the missing action from the message rather than matching the old detail texts.
- Unchanged since 0.26.0, for migration from earlier releases: order error code 68 and trigger failure reason 24 are `ERROR_CODE_QUOTE_SIZE_TOO_SMALL` and `TRIGGER_FAILURE_REASON_QUOTE_SIZE_TOO_SMALL` (REST `QUOTE_SIZE_TOO_SMALL`), formerly `..._MAX_QUOTE_DEBIT_TOO_SMALL`; see the 0.26.0 entry. 0.93.5 does not change them.
- **Breaking:** `Error::TimestampSkew` gains `window` (`Option<TimestampWindow>`, new), and `Error::timestamp_window()` (new) reads it: the server time, the receive window the timestamp was checked against and how far a timestamp may run ahead, as a skew refusal now states them (ConnectRPC `AuthErrorDetail.timestamp_window`, REST problem `timestampWindow`), so a client can resync its clock from the refusal alone. A refusal without a readable window keeps `None`, and its status and Retry-After are kept either way. Migration: a pattern that names the fields of `Error::TimestampSkew` adds `window` or `..`.
- **Breaking:** `WalletBalances` gains `unavailable_assets` (`Vec<UnavailableWalletAsset>`, new): `withdraw.get_wallet_balances` lists the zipped assets whose zToken balance could not be read, with the `reason` (`no_contract`, `read_failed`, `invalid_balance`). Their balance is unknown, not zero: an asset missing from `balances` was read as not held before. `is_complete()` (new) is true when every balance was read. Migration: check `is_complete()` before treating an absent asset as zero, and set `unavailable_assets` where you build a `WalletBalances`.
- **Breaking:** `AssetInfo` gains `market_stats` (`Option<AssetMarketStats>`, new, with `AssetMarketValue` and `AssetMarketFlag`): `market_data.list_asset_info` returns each asset's circulating, total and maximum supply (or the `max_supply_unlimited` flag), market capitalization, fully diluted market capitalization and rank, each an exact decimal string with its `source` (`coinmarketcap` or `polyester`) and `as_of` time. A present datapoint without its value, source or `as_of`, or with a value that is not a nonnegative decimal (a positive integer for `rank`), and an unlimited-supply flag without its source or `as_of`, are refused with `Error::ResponseContract`; `codecs::decode::asset_info_from_proto` and `asset_info_list_from_proto` now return a `Result`. Assets with market datapoints and no editorial are now listed, with an empty editorial. Migration: set `market_stats` where you build an `AssetInfo`; handle the `Result` of `codecs::decode::asset_info_from_proto` and `asset_info_list_from_proto` if you call them directly: propagate it with `?`, or match `Error::ResponseContract`, which means the reply broke the contract, so use none of it, and retry the read later or report it (the service calls already return the error the same way).
- **Breaking (server behaviour):** A price alert that asks for `AlertChannel::Telegram` is refused where no Telegram account is linked (`TELEGRAM_NOT_LINKED`) or where the environment offers no Telegram alerts (`TELEGRAM_UNAVAILABLE`), both `failed_precondition`, final, on `alerts.create` and on an `alerts.update` that changes an alert whose channels then include Telegram. `errors::alert_codes` gains both. Migration: offer Telegram only once it is linked, and on these codes send the alert in-app only.
- New `sub_accounts.list_account_activity`: the main account's and every sub-account's security and activity events in one feed, newest first (`auth.v1.AccountActivityService/ListAccountActivity`), as `AccountActivityList` of `AccountActivityEvent` (new), each naming its sub-account, actor and API key as base58 IDs (`None` for the main account, a session or none); an event without its creation time is an `Error::ResponseContract`. An API key needs `READ_ACCOUNT_ACTIVITY`. A `limit` above 200 is refused before sending.
- A withdrawal's `destination_chain_id` and `withdraw.validate_destination`'s chain are the Polyester chain ID (`ZipperChainConfig::chain_id`), not the network's native chain ID. A native ID is now refused as a mistake: `validate_destination` answers `valid: false` with `UNSUPPORTED_CHAIN`, and a withdrawal fails with `Error::Api` `ERROR_CODE_INVALID_DESTINATION_CHAIN` (new in `errors::withdraw_codes`, not retryable), where it failed as a retryable unavailable error.
- Docs: the `READ_NOTIFICATIONS` and `READ_ACCOUNT_ACTIVITY` API-key permissions; `CONVERT_DUST` and `READ_FUNDING_FLOWS` are product actions valid on both policy kinds; a linked X/Twitter or Discord account's `changeAvailableAt` says when it may be changed or unlinked again; a withdrawal on a route whose supply is stale or missing is refused with `ERROR_CODE_SUPPLY_UNAVAILABLE`.
- **Breaking:** Zipper supply carries its status, as the contract now does: `ZipperAssetChainVariant` gains `supply_status` (`"available"`, `"stale"`, `"missing"`, `""` when unspecified, `"unknown(<n>)"` for a newer value) and `supply_observed_at`, and `ZippedAssetSupplyUpdate` gains `status` and `observed_at`. `supply` becomes `Option<String>` on both: `Some` only while the status is available or stale; a missing route reads `None`, where it read `"0"`. `supply_is_current()` is true only while available; `catalogs.zipped_asset_supply(id)` (new) returns the status, supply and observation time, and `catalogs.supply_for_zipped_asset_id` returns `None` for a supply that is not known. An available or stale supply always carries its observation time: `zipper.get_deposit_withdraw_config`, `zipper.subscribe_zipped_asset_supply` and catalog hydration refuse one without it with `Error::ResponseContract` before anything is cached, and `codecs::decode::deposit_withdraw_config_from_proto` now returns a `Result`. The zipper models no longer derive `Eq` (`Timestamp` has none). Against a server without supply status every supply reads `None`. Migration: read `supply_status` (or `supply_is_current()`) before `supply`; treat `None` as not known, never as zero; set the new fields where you build these types; compare with `PartialEq`; handle the `Result` of `codecs::decode::deposit_withdraw_config_from_proto` if you call it directly: propagate it with `?`, or match `Error::ResponseContract`, which means the reply broke the contract, so use none of it, and retry the read later or report it (the service calls already return the error the same way).
- **Breaking:** `statements.export`, `statements.export_page`, `ExportStatementOptions`, `StatementPage` and `Statement` are removed: the contract removed `LedgerReadService.ExportStatement`. Migration: create the statement as a file with `statements.create_export`, poll `statements.get_export` until it is `ready`, then download it through `statements.get_export_download_url`; read its rows with `codecs::decode::parse_statement_csv`, which is unchanged.
- New `statements.get_export_download_url(export_id)`: a URL that downloads a `ready` statement file without credentials, valid for at most 60 seconds, with its `expires_at` and `filename` (`StatementExportDownloadUrl`, whose `Debug` redacts the URL). New `statements.cancel_export(export_id)` cancels a `queued` or `processing` export, which then ends `canceled`; `errors::ledger_codes` gains `STATEMENT_EXPORT_NOT_CANCELABLE` and `STATEMENT_EXPORT_CANCELED`.
- **Breaking:** `StatementExportsList` gains `in_progress_exports` and `max_in_progress_exports` (`u32`): how many of the main account's exports are queued or processing, and how many may be at once. Migration: add the fields where you build a `StatementExportsList`.
- **Breaking:** `internal_transfers.batch_create` moves 1 to 500 legs all or nothing, as the contract now does: every leg is credited, or none moves. The batch `status` is `accepted`, `processing` or `refused` (`submitted` is gone); a leg is `credited`, `processing` or `not_moved` (`accepted`, `pending`, `refused` and `not_submitted` are gone). `InternalTransferBatchLegResult` loses `request_id` and renames `accepted_at_ts_ns` to `credited_at_ts_ns`; a credited leg always carries its `transfer_id`, `flow_id`, `credited_at_ts_ns`, echoed asset and amount, asset code and resolved destination (a canonical base58 root account ID, a sub-account ID read the same way when it names one, and a `0x` smart-account address of 40 hex digits), and a reply without any of them is an `Error::ResponseContract`. More than 500 legs are refused before sending (was 10). Migration: read `accepted` for a batch that moved everything and `refused` for one that moved nothing; follow `processing` with `get_batch`; map leg `accepted` to `credited`, `pending` to `processing`, and `refused`/`not_submitted` to `not_moved`; read the batch's `request_id` instead of the leg's.
- New `internal_transfers.get_batch(request_id)` reads a batch's current outcome and every leg's state by the `request_id` `batch_create` returned, without resubmitting it. `errors::transfer_codes` gains `BATCH_TOO_MANY_SOURCES` (more than 10 distinct source accounts in one batch).
- New `Client::get_time()`: the server clock that checks signed requests' timestamps (`system.v1.SystemService/GetTime`), as exact milliseconds (`ServerTime::server_time_ms`). Public: no API key.
- **Breaking:** An attached take-profit, stop-loss or trailing-stop value refused before sending carries the code the Orders API and the trading WebSocket answer for it, as `Error::Api` with that `code` and no violation, where it was `Error::Validation`, on `orders.create`, `preview`, `batch_create`, `modify` and `batch_replace`: a `trigger_price` that is not positive, `ERROR_CODE_TRIGGER_PRICE_INVALID`; a limit child's price that is not positive, `ERROR_CODE_BAD_PRICE`; a trailing stop's missing, non-positive or out-of-range (1–10000 bps) distance, `ERROR_CODE_TRAILING_DISTANCE_INVALID`; a `MaxSlippage::Ticks` that is not positive, `ERROR_CODE_SLIPPAGE_TICKS_INVALID`; a `MaxSlippage::Bps` below 1, `ERROR_CODE_SLIPPAGE_BPS_INVALID`, and of 10000 or more, `ERROR_CODE_MAX_SLIPPAGE_INVALID`. The values refused are unchanged. An attached trailing stop's explicit zero `activation_price` stays this SDK's own `Error::Validation`: the contract reads 0 as no activation price, and only `None` means none here. Migration: match these refusals with `Error::auth_error_code()` (or `Error::Api { code, .. }`) instead of `Error::Validation`.
- **Breaking:** A client OCO ID that breaks its rule (1 to 36 ASCII letters, digits, `.`, `_`, `:`, `/` or `-`) is refused before sending as `Error::Api` with `ERROR_CODE_CLIENT_LIST_ID_INVALID`, the code the Triggers API now answers for it, where it was `Error::Validation`, by `triggers.create_oco` and the `client_list_id` filter of `triggers.list_with`. The message text is unchanged. Migration: match `ERROR_CODE_CLIENT_LIST_ID_INVALID` instead of `Error::Validation`.
- **Breaking:** A `triggers.modify` that names no field to change is refused before sending as `Error::Api` with `ERROR_CODE_TRIGGER_PATCH_EMPTY`, the code the Triggers API now answers for it, where it was `Error::Validation`. Migration: match `ERROR_CODE_TRIGGER_PATCH_EMPTY`.
- **Breaking:** `OrderErrorDetail` gains `price_band_limit` (`Option<Price>`): a preview refused `PRICE_BAND_VIOLATION` names the band limit its limit price is past (the highest admitted buy price, the lowest admitted sell price), as the market last published its band; `None` for every other rejection. Previews now check a limit price against the band. Migration: add `price_band_limit: None` (or `..`) where you build an `OrderErrorDetail`.
- **Breaking:** `TriggerLadderDetails` gains `requested_levels`, `placed_levels`, `rejected_levels` and `canceled_levels` (`i32`, never negative) and `outcome` (`Option<String>`: `all_admitted`, `mixed_rejections` or `all_children_rejected` once the ladder has ended, `UNKNOWN(<n>)` for an outcome this SDK does not name, `None` while it runs). Migration: add the new fields where you build a `TriggerLadderDetails`.
- New `Error::is_order_unknown()`: true for `ERROR_CODE_ORDER_UNKNOWN`. That code no longer proves an order never existed: an ended order answers it by client order ID once the ID's window has passed, and by order ID after exchange maintenance the order ended before. For an order you placed and know ended, treat a cancel answered with it as done; otherwise read the order. The earlier advice to retry a cancel by order ID to tell the two apart is withdrawn. `Error::is_order_already_terminal()` still matches `ERROR_CODE_ORDER_ALREADY_TERMINAL` only.
- The trading WebSocket names `RejectCode` 318 `CLIENT_LIST_ID_INVALID` (schema semantic version 2.5.0, kept by schema 50 above). Earlier releases read it as `UNKNOWN(318)`.
- Docs: `MaxSlippage::Ticks`, a market order's `market_max_slippage` and the trailing distance are absolute amounts in 1e-9 quote units, not counts of the pair's tick size, and need not be on the tick; a market order's protection price is put on the tick grid toward its reference. `Order::version` is the change counter the order's WebSocket events carry: compare it to keep a read from undoing a newer state. `Order::fee_total` is complete once its quantity equals `cum_qty`, and fills settle after the order ends. A trigger resume or modify by a market-limited API key is judged on the trigger's own pair, named or not.
- **Breaking:** Sub-account reads keep every field the API sends. `SubAccountInvite` gains `subaccount_id` (the sub-account the invite is for), `inviter_account_id` (`None` when unset), `created_at_ms` and `responded_at_ms` (Unix milliseconds, `None` when unset, as while pending), `grantee_username`, `inviter_username`, `subaccount_label`, `inviter_root_smart_account_address`, `grantee_root_smart_account_address` and `require_member_mfa`. `SubAccountMember` gains `mfa_enrolled`, `username`, `smart_account_address` and `avatar_url`. `SubAccount` gains `subaccount_policy_id` (`None` when no policy is attached), `require_member_mfa`, `smart_account_salt_nonce` (`None` when unset), `icon`, `color`, `owner_username`, `owner_avatar_url` and `owner_root_smart_account_address`. Migration: code that builds these structs itself sets the new fields, or `..Default::default()` on `SubAccountInvite` and `SubAccountMember`.
- **Breaking:** `guard_signer.get_status` keeps the live on-chain signer and the nonce: `GuardSignerStatus` gains `onchain_signer_address` (empty when the server could not read the chain), `nonce` (base-10 text) and `nonce_space`. Migration: code that builds a `GuardSignerStatus` itself sets the new fields.
- **Breaking:** `auth.me` keeps the session of the presented access token: `MeResult` gains `session`, a `MeSession` (new) with `session_id`, `session_level` (its proto name, such as `"PRIMARY_AUTHENTICATED"`), `authentication_methods` and `auth_time_ms`; `None` when the server sends no session, as for an API key. Migration: code that builds a `MeResult` itself sets `session`.
- **Breaking:** `market_data.list_asset_info` refuses an asset ID of 0 or above 2147483647, or more than 1000 IDs, with `Error::Validation` before sending, by the contract's rule on `ListAssetInfoRequest.asset_ids` (`max_items: 1000`, items `gt: 0, lte: 2147483647`); such a list was sent and refused by the API. An empty list still reads every asset. Migration: pass only ledger asset IDs from 1 to 2147483647, at most 1000 per call.
- **Breaking:** `deposit.create_address` refuses a `chain_id` of 0 with `Error::Validation` before sending, by the contract's `uint32.gt = 0` rule on `CreateDepositAddressRequest.chain_id`; it was sent and refused by the API. Migration: set the source chain's ID.
- **Breaking:** `orders.create`, `orders.preview` and batch items refuse `max_quote_debit_scaled` on anything but a BUY market order or a BUY limit IOC order with `Error::Validation`, by the contract's rule `order_intent.max_quote_debit_eligibility`, as `quote_proceeds_scaled` is already refused off a SELL IOC; a SELL order or a BUY limit GTC, GTD or FOK order with a quote budget was sent and refused by the API. Migration: size a SELL or a resting BUY with `quantity`, or set `time_in_force` to IOC on a BUY limit sized by a quote budget.
- `marketstream::connect` and `tradews::connect` open `wss://` connections in a program that never built a `Client`; they panicked inside rustls ("Could not automatically determine the process-level CryptoProvider") unless something had installed a process-wide crypto provider first. Every TLS connection of the crate (the API client, chain RPC, the realtime token fetch and all four WebSockets) now uses one rustls configuration that names its provider (ring) itself.
- **Breaking:** Building a `Client` (or using chain RPC or the realtime client) no longer installs ring as the process-wide rustls crypto provider. The SDK's own connections need no process provider. Migration: an application whose other rustls code relied on the SDK installing one calls `rustls::crypto::ring::default_provider().install_default()` itself at startup.
- **Breaking:** Every list call refuses a page size above its contract maximum with `Error::Validation` before sending, where it was sent and refused by the API: 1000 for `orders.list_open_with`, `orders.list_history_with`, `orders.get_with` (executions), `trades.list_with`, `transfers.list`, `balances.list_transfers`, `triggers.list`, `triggers.list_with`, `triggers.list_events`, `triggers.list_events_by_id` and `market_data.get_trades_with`; 10000 for `market_data.get_candles_with` and `get_candles_columns`; 20000 for `heatmap.get`; 2000 for `market_overview.list`; 500 for `address_book.list_entries`, `list_transfer_destinations` and `list_internal_transfer_whitelist_entries`, `lifecycle.list_flows`, `list_flows_by_tx`, `get_flow_by_tx` and `changelog.list_changes`; 200 for `address_book.list_transfer_counterparties`, `address_book.get_view`, `sub_accounts.list_activity` and `Client::list_status_events`; 100 for `changelog.list_posts` and `changelog.list_deployment_groups`. The order reads' optional `limit` is 1 to 1000 in the contract, so `Some(0)` on `orders.list_history_with`, `orders.get_with` and `trades.list_with` is refused as `orders.list_open_with` already refused it; it was sent and refused by the API. `None` (or 0 on a plain `u32` limit) still leaves the page size to the server's default, and 1 and the maximum are sent unchanged. Each maximum is pinned to the contract's generated field docs. `heatmap.get` and `heatmap.get_with_options` also refuse a limit of 0: the field is always on the wire and the contract takes 1 to 20000, so 0 was sent and refused by the API. Migration: ask for at most the maximum and follow `next_page_token` for more; pass `None` instead of `Some(0)` for the default page size. `heatmap.get` has no default: pass 1 to 20000.
- **Breaking:** A zero price on create is refused before sending in every form (`"0"`, `"0.00"`, 0 ticks), naming the price, where it was encoded as no price: a trigger's limit child (`limit_price` of a stop-loss or take-profit limit child, GTC, IOC or FOK, and of TWAP limit slices), a trailing stop's `activation_price` (it was sent as no activation price) and an OCO leg's `limit_price` are refused with `Error::Api` carrying the field's code (`ERROR_CODE_BAD_PRICE` or `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_INVALID`); a limit order's `price`, and an attached take-profit or stop-loss `trigger_price`, its limit child's `limit_price` and an attached trailing stop's `activation_price`, with `Error::Validation`. The contract takes each of these as a positive price. A ladder's zero `ladder_price_min` or `ladder_price_max` was already refused; the refusal now names the price. `triggers.modify` refuses a zero `trigger_price` too, with `ERROR_CODE_TRIGGER_PRICE_INVALID`; a zero `activation_price` on modify still clears it, as the contract defines. Migration: pass a positive price, or leave an optional price such as `activation_price` `None`.
- **Breaking:** A public ID given as text is read as base58 only, the form the SDK returns and the app shows, byte by byte as the API reads it, by every call that takes an order, trigger, OCO, alert, batch, export, account or sub-account ID as a string. A digit-only string is no longer read as a decimal number, which made the same kind of input mean different IDs (e.g. `"23"` was ID 60 while `"123"` was ID 123 and `"100"` was ID 100); now `"23"` is ID 60, and `"100"` or `"00023"`, which hold a `0` outside the base58 alphabet, are refused. Whitespace is refused, ASCII or not, where it was trimmed, and so is any other character outside the alphabet, such as a digit of another script (`"٢٣"`, `"２３"`). Each refusal is `Error::Validation` naming the field, before sending. An ID passed as a `u64`, such as `SubAccountScope::Id` or a request's ID field, is unchanged. A supplied empty or blank ID is refused too, never read as absent: an empty or blank account on a subscription (`lifecycle.subscribe_open_flows(Some(""))` subscribed to the public feed, and the private subscriptions took the configured default account), an empty `Config::default_sub_account_id` or `default_account_id` (it read as no default), an empty trigger filter on `orders.list_open_with` and `list_history_with` (it read as no filter), and an empty internal-transfer `destination_account_id` or `destination_subaccount_id` (it was skipped for the other destination given with it). `lifecycle.get_flow`, `get_account_flow` and `subscribe_flow_detail` refuse a flow ID that breaks the contract's `^flow_[1-9A-HJ-NP-Za-km-z]+$` (6 to 64 characters), a blank or padded one included, where a padded one was sent. Only `None` (an unset field) leaves an ID out; the `POLYESTER_ACCOUNT_ID` environment variable is still trimmed. Migration: pass a raw number as an integer (`u64`), or convert it with the SDK's public-ID formatter, `polyester::codecs::format_id`; pass `None` to mean no ID.
- **Breaking:** A public ID given as text must be in canonical form, as the SDK writes it (no leading `1`s); `"123"` was read as the same ID as `"23"` and is now refused with `Error::Validation`. Migration: use the ID exactly as the SDK or the app shows it, or pass the integer.
- `orders.wait_for_order_trades_complete` returns each settlement leg once. The contract repeats a match's legs on every execution page that holds one of its fills, and the wait returned them once per page, so a match on two pages counted its fees and amounts twice. A leg whose `tx_id` an earlier page carried is now dropped, keeping the first occurrence's position; legs within one page, and legs without a `tx_id`, are all kept. Trades are unchanged.
- `orders.list_history_with` and `trades.list_with` take the contract's time-window filters: `ListOrderHistoryOpts` and `ListUserTradesOpts` gain `start_ts_ns` and `end_ts_ns` (`Option<u64>`, inclusive, Unix nanoseconds). Each is sent only when set, and `Some(0)` is sent: an explicit `end_ts_ns` of 0 returns an empty page. A `start_ts_ns` after a non-zero `end_ts_ns` is refused with `Error::Validation` before sending, as the contract requires. Affected: `models::ListOrderHistoryOpts`, `models::ListUserTradesOpts` (new fields; construct them with `..Default::default()`).
- **Breaking:** Safe address prediction refuses the zero address as an owner with `Error::Validation` naming it (`owner_address`, or `owners[i]` for `predict_safe_address_with_data`). The Safe contract refuses the zero address as an owner, so the Safe predicted for it could never be deployed or controlled. An owner that is not 20 bytes of hex was already refused; its error now names the owner too. Affected: `chain::predict_safe_address`, `chain::predict_polyester_smart_account_address`, `chain::predict_safe_address_with_data`. Migration: pass each owner's 20-byte address; there is no usable Safe for the zero address.
- **Breaking:** `PolyesterSmartAccount::get_nonce` decodes the EntryPoint `eth_call` result as `0x` and exactly one 32-byte ABI word, like `chain::quote_zipper_fee`, and returns `Error::Transport` for anything else. An empty result `"0x"`, which an `eth_call` to an address without code returns, was read as nonce 0, as was any result shorter than a word, such as `"0x1"`. Affected: `chain::PolyesterSmartAccount::get_nonce`, `chain::PolyesterSmartAccount::send_calls`. Migration: none for a standard Ethereum JSON-RPC endpoint and EntryPoint, which return a whole word; check the environment's EntryPoint address if this error appears.
- **Breaking:** `PolyesterSmartAccount::send_calls` reads a hex gas value from the paymaster as a JSON-RPC quantity: `0x` and at least one hex digit, with no sign and no leading zero (`"0x0"` is zero). A leading zero such as `"0x01"` or a sign such as `"0x+1"` was accepted; it is now `Error::Transport`. An empty `"0x"` was already refused. Affected: `chain::PolyesterSmartAccount::send_calls` (`pimlico_getUserOperationGasPrice` and `pm_sponsorUserOperation` gas fields). Migration: none for a paymaster that encodes quantities as the JSON-RPC specification requires.
- **Breaking:** A whitespace-only order book `bucket`, such as `" "`, is refused with `Error::Validation`, as the Go and Python SDKs refuse it; it was read as no bucketing. Only `""` (or no bucket) means no bucketing. As for every price input, whitespace around a valid bucket such as `" 0.01 "` is still ignored, and a whitespace-only price is refused. Affected: `orderbook.create_subscription` (`CreateSubscriptionOptions.bucket`), `orderbook::Subscription::set_bucket`, `orderbook::parse_bucket_ticks`. Migration: pass `""` or `None` for no bucketing.
- **Breaking:** `orderbook.get` sends its depth as a level count per side, as the contract's `GetOrderBookRequest.depth` now is (a `u32`; the `Depth` enum `DEPTH_1` … `DEPTH_1000` is removed). Any depth from 1 to the server's maximum is sent as given and its snapshot holds that many levels per side; `None` or `Some(0)` asks for the default of 50 and reports `depth` 50. A depth between the old enum values was rounded up to the next one (20 levels for `Some(11)`), and a depth above 1000 was refused with `Error::Validation` before sending; the maximum is now the server's (5,000 by default, reported as `max_depth` in every snapshot), and a deeper request is refused by the server with `invalid_argument` (`Error::Api` with code `InvalidArgument`), never truncated. `codecs::decode::depth_enum_for_levels` and `ORDERBOOK_SNAPSHOT_DEPTHS` are removed, and `codecs::decode::orderbook_snapshot_depth` now returns the depth a request reports (`u32`, 50 for 0) instead of a `Result`. `OrderbookData` gains `book_epoch` and `max_depth`. A managed subscription still rides the stream depths 1, 20, 50, 200 and 500 and fetches its snapshot at that depth. The heatmap keeps its own depths (`codecs::HEATMAP_DEPTHS`, new). Affected: `orderbook.get`, `codecs::decode::orderbook_snapshot_depth`, `codecs::decode::depth_enum_for_levels`, `codecs::decode::ORDERBOOK_SNAPSHOT_DEPTHS`, `models::OrderbookData`. Migration: pass the number of levels you want (`DEPTH_20` is `Some(20)`); read `max_depth` from any snapshot instead of assuming 1000, and on `invalid_argument` ask for at most `max_depth`; code that builds an `OrderbookData` itself sets `book_epoch` (`String::new()` when it is not a snapshot) and `max_depth` (0).
- `OrderbookData::book_epoch` (a decimal `u64`, like `book_seq`) is the epoch of a snapshot's `book_seq`, and `OrderbookData::max_depth` the largest depth the server accepts; both are set on `orderbook.get` snapshots only, and a managed subscription view leaves `book_epoch` empty and `max_depth` 0, since the stream carries no epoch. `codecs::decode::compare_snapshots` (new) orders two snapshots by `(book_epoch, book_seq)`: a higher epoch is newer whatever its `book_seq`, since a new epoch may restart it; a view without an epoch, or a malformed position, is `Error::Validation`. A snapshot is a frozen view: never apply stream updates to it; fetch it again to refresh it. Affected: `orderbook.get`, `models::OrderbookData`.
- **Breaking:** A trigger or OCO field refused before sending carries the code the Triggers API answers for the same mistake, as `Error::Api` with that `code` and no violation, where it was `Error::Validation`: `ERROR_CODE_CLIENT_TRIGGER_ID_INVALID` (an empty or longer than 36-byte client trigger ID), `ERROR_CODE_TRIGGER_QTY_INVALID` (a trigger quantity that is not positive), `ERROR_CODE_BAD_QTY` (an OCO's), `ERROR_CODE_TRIGGER_PRICE_INVALID` (a missing or zero stop-loss, take-profit or OCO leg trigger price, a missing or zero ladder price, a ladder maximum not above its minimum), `ERROR_CODE_BAD_PRICE` (a limit child, TWAP limit slice or OCO limit leg without a price, a buy trailing stop's maximum price that is not positive), `ERROR_CODE_TRAILING_DISTANCE_INVALID` (a missing distance, ticks not positive, basis points outside 1 to 10000; on a modify too), `ERROR_CODE_SLIPPAGE_TICKS_INVALID`, `ERROR_CODE_SLIPPAGE_BPS_INVALID` (a cap outside 1 to 10000, or 0 to 10000 on a modify), `ERROR_CODE_MAX_SLIPPAGE_INVALID` (10000 basis points on a selling child, a selling TWAP slice or a trailing stop), `ERROR_CODE_TWAP_DURATION_INVALID`, `ERROR_CODE_TWAP_INTERVAL_INVALID`, `ERROR_CODE_LADDER_LEVELS_INVALID` and `ERROR_CODE_FEE_ASSET_NOT_ALLOWED` (base fees on a sell trigger). The server, on ConnectRPC and REST alike, now answers these codes where it answered `ERROR_CODE_VALIDATION_ERROR`, still with `invalid_argument` (HTTP 400) and the same field violation, and a modify's invalid activation price answers `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_INVALID` where it answered `ERROR_CODE_TRIGGER_PRICE_INVALID`; an OCO's non-positive quantity answers `ERROR_CODE_BAD_QTY`, a leg's trigger price `ERROR_CODE_TRIGGER_PRICE_INVALID` and its limit price `ERROR_CODE_BAD_PRICE`. A request without a strategy, a child or an execution still answers `ERROR_CODE_VALIDATION_ERROR`, and the SDK's shape refusals (both trailing distances, a buy trailing stop without a maximum price, a sell one with one) stay `Error::Validation`. Affected: `triggers.create`, `triggers.create_oco`, `triggers.modify`. Migration: match `Error::Api` by `code` for these refusals, whether the SDK or the server refused them; replace a match on `ERROR_CODE_VALIDATION_ERROR` for any of them with the field's code, and on `ERROR_CODE_TRIGGER_PRICE_INVALID` for a modify's activation price with `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_INVALID`.
- **Breaking:** An order that sells with `fee_asset` `Base` is refused with `Error::Api` carrying `ERROR_CODE_FEE_ASSET_NOT_ALLOWED`, the server's code for it, where it was `Error::Validation`. A sell must pay its fee in quote; a buy may pay it in base only on a pair whose `allow_buy_fee_from_base` is true in `market_data.get_spot_config`, which the server checks. Affected: `orders.create`, `orders.preview`, `orders.batch_create`. Migration: match `Error::Api` with code `ERROR_CODE_FEE_ASSET_NOT_ALLOWED` instead of `Error::Validation`, or leave `fee_asset` unset (quote) on sells.
- **Breaking:** A client trigger ID is checked by the contract's rule only, 1 to 36 bytes of UTF-8 with no character set, and is sent exactly as given; an empty or longer ID is refused before sending with `Error::Api` carrying `ERROR_CODE_CLIENT_TRIGGER_ID_INVALID`. IDs the API accepts are no longer refused: spaces, other punctuation and non-ASCII characters were refused with `Error::Validation`. Surrounding spaces were trimmed before sending, so an ID with them is now sent with them, and an ID of spaces only is sent instead of refused. Affected: `triggers.create` (`CreateTriggerParams::client_trigger_id`). Migration: trim a client trigger ID yourself if you relied on the SDK trimming it; an ID without surrounding spaces is sent as before.
- **Breaking:** An HTTP 504 is an unknown outcome whatever code its body names. A 504 problem with a retryable code, such as `RATE_LIMIT_EXCEEDED`, `OVERLOADED`, `REPLAY_CAPACITY_EXHAUSTED`, `AUTH_REPLAY_CAPACITY_EXHAUSTED`, `AUTH_TEMPORARILY_UNAVAILABLE` or `TIMESTAMP_SKEW`, was an `Error::RateLimit` (or another refusal) that `is_retryable()` reported retryable with a known outcome; the API answers 504 only where a write may have applied, and an edge answers it for a request that may have reached the API. The HTTP status now decides outcome certainty before any code, message or typed detail is read: a 504 is `Error::GatewayTimeout`, not retryable, `mutation_outcome_unknown()` true, and retried by the SDK's safe reads. That holds for a REST problem and for a ConnectRPC error body alike, whatever its code (`unavailable`, `internal`, `unknown`), message (`TIMESTAMP_SKEW`) or typed detail (a rate-limit detail) says; a ConnectRPC server-failure body at 504 is no longer handed to the ConnectRPC mapping. A 504 whose code, ConnectRPC message or order detail names an `*_OUTCOME_UNKNOWN` code is `Error::Api` with that code, still an unknown outcome. A ConnectRPC `deadline_exceeded` at 504, which ConnectRPC answers when the request's own deadline passes, is that deadline: `Error::Transport`, as ConnectRPC `deadline_exceeded` reads, whatever its message says. Affected: every call. Migration: read a write back before sending it again after a 504, whatever its body says.
- **Breaking:** Every error that comes from an HTTP answer carries that answer's metadata, `Error::answer()` (new): an `AnswerMeta` (new) with the answer's `status`, the HTTP status it actually had, and `retry_after`, the delay in seconds it asked for with `Retry-After` (or `Retry-After-Ms`). The status is captured where the answer arrives and is never inferred from a ConnectRPC code: a ConnectRPC `unauthenticated` answered with 503 reports 503, and the `status` fields of `Error::PermissionDenied`, `Error::TimestampSkew` and `Error::ApiKeyRejected` report the same real status. It is one boxed field, `answer: Option<Box<AnswerMeta>>`, on every variant that can come from an answer: `Error::Auth`, `Error::PermissionDenied`, `Error::Transport` (a server deadline included), `Error::RateLimit`, `Error::Server`, `Error::AdmissionUnavailable`, `Error::GatewayTimeout`, `Error::Api` (an `AUTH_TEMPORARILY_UNAVAILABLE` refusal included), `Error::RouteNotFound`, `Error::NotImplemented`, `Error::TimestampSkew`, `Error::ApiKeyRejected`, `Error::SessionRequired` and `Error::Realtime`. That covers every path that turns an answer into an error: ConnectRPC and REST answers (an answer whose body cannot be read included), the realtime token, subscription and sub-account answers, a refused WebSocket upgrade of the realtime or private order stream, and a JSON-RPC endpoint's failed answer in `chain`. The metadata is read from the answer's headers as soon as they arrive, before its body, so an error the body causes carries it too: a body cut short, larger than the SDK reads, or still arriving at the deadline (an API call reads it as an answer with no readable body, by its status: `Error::Server` at 503, where a body still arriving at the deadline was `Error::Transport` with no answer), and a redirect, which is never followed, to a valid or a malformed `Location` (`Error::Server` for an API call, `Error::Realtime` for a realtime token, `Error::Transport` for JSON-RPC). `Error::retry_after()` reads the delay from it, a rate-limit detail's `retry_after_ms` first, where it returned only `Error::RateLimit`'s. An error with no answer behind it, such as a refused connection or a check made before sending, carries none. `Error::Auth`, `Error::Transport` and `Error::Realtime` are struct variants, `Error::Auth { message, answer }`, `Error::Transport { message, answer }` and `Error::Realtime { message, answer }`, where they were tuple variants of a `String`. `Error::Server` is a struct variant too, `Error::Server { message, code, answer }`, where it was `Error::Server(String)`, so a temporary failure keeps its code (a 503 problem's `UPSTREAM_ERROR`, a service detail's code, or an unrecognised code kept for diagnosis); `Error::GatewayTimeout` gains `code`. Every retry of a safe read waits at least the delay, and returns the error at once without retrying when the delay would end past its budget or deadline, where it retried after its own short delay: the trigger reads (`triggers.get`, `triggers.list`, `triggers.list_events`), the catalog reads at startup and the batch replace status wait. Affected: the variants above, `Error::retry_after`, `triggers.get`, `triggers.list`, `triggers.list_events`, `orders.wait_batch_replace`, catalog loading, realtime subscriptions, `chain::JsonRpcClient`. Migration: match `Error::Auth { message, .. }`, `Error::Transport { message, .. }`, `Error::Realtime { message, .. }` and `Error::Server { message, .. }` instead of the tuple forms, and build them with `Error::auth(..)`, `Error::transport(..)` and `Error::realtime(..)`; add `..` to patterns that list every field of the variants above, and set `answer: None` where you construct one.
- REST rate-limit problems are read as the API now sends them: the detail is the problem body's `rateLimit` member with camelCase fields (`operationId`, `policyClass`, `refillModel`, `retryAfterMs`, `policyVersion`), its integers as decimal strings or JSON numbers. The `rate_limit` member and snake_case fields are no longer read. ConnectRPC errors are unchanged. Affected: `Error::RateLimit::detail` and `Error::retry_after` of REST `RATE_LIMIT_EXCEEDED` (HTTP 429) and `RATE_LIMIT_AUTHORITY_UNAVAILABLE` (HTTP 503) answers.
- Orders and triggers refused for insufficient available balance answer ConnectRPC `failed_precondition` (it was `invalid_argument`), as withdrawals and internal transfers do; the HTTP status stays 400. The SDK returns the same `Error::Api` with code `ERROR_CODE_INSUFFICIENT_FUNDS` either way: an account-state refusal that is not retryable unchanged and whose outcome is known. Add funds or reduce the order first.
- REST routes answer a temporary failure with 503 instead of 502 or 504, keeping the problem `code`. A 503 reads as ConnectRPC `unavailable` does, and one whose code names a deadline as `deadline_exceeded` does: a write's outcome is unknown and it is not retried, and a read is retried. A 504 stays an unknown outcome whatever its body. Tests pin it; no behavior changed.
- Docs: `Order::price` is the limit price of a LIMIT order and the protection price of a MARKET order, the worst price it may fill at (a BUY at or below it, a SELL at or above it), for every market order: direct, or placed by a stop-loss, take-profit, attached leg, TWAP or trailing stop. A trailing stop's market order with a slippage cap now reads its protection price where it read none. Tell market from limit orders by `order_type`, never by the price.
- **Breaking:** `Error` gains the variant `MarketStream(marketstream::StreamError)`, which reports a market-data stream message the client refused: `StreamError::Schema` (another schema; the stream stops), `StreamError::Frame` (a malformed message) or `StreamError::Unsubscribe` (an unsubscribe answered with neither OK nor NOT_SUBSCRIBED for a stream the server holds; the client reconnects). Migration: add an `Error::MarketStream` arm where you match `Error` exhaustively.
- New `marketstream` module: a client for the public binary market-data WebSocket (`/ws/market.sbe`), the low-latency feed of trades, best bid and offer, top-20 depth and the live depth window per spot pair, encoded with SBE. Connecting needs no API key. `Client::market_stream` and `Client::market_stream_with` (new) connect to the client's environment once the spot catalogs are loaded; `marketstream::connect` connects with an explicit `marketstream::Config`. `MarketStream::subscribe` subscribes to `Topic`s and returns one `SubscribeResult` per topic, and `MarketStream::unsubscribe` and `MarketStream::unsubscribe_symbol` end one topic or every stream of a pair (`SubscribeResult::Ok` or `SubscribeResult::NotSubscribed` per topic). Events arrive from `MarketStream::recv` as `Event::Trades`, `TradesThrough`, `TradesGap`, `TradesBackfill`, `BestBidOffer`, `DepthSnapshot`, `DepthUpdate`, `MarketStatus`, `SubscribeResponse`, `Pong`, `Connected` and `Disconnected`. Prices are `types::Price` and quantities `types::Quantity` with the pair's catalog scale.
- The market-data stream client keeps a local book per pair from `DepthSnapshot` and exact-base `DepthUpdate` messages (`MarketStream::order_book`), each kept to the live window its snapshot names (`DepthSnapshot::levels_per_side`), and the newest best bid and offer (`MarketStream::best_bid_offer`). It tracks each pair's trade IDs across reconnects: every trade the connection did not receive live is reported in a `TradesGap`, and recovered from the trade history API by trade ID into a `TradesBackfill` (`marketstream::backfill`; `MarketDataService` implements `marketstream::TradeHistory`). A depth update the book cannot apply, or a pair reported live again before its book's replacement snapshot, is a gap of that depth stream alone: its book goes stale, the client unsubscribes the stream and subscribes it again on the same connection, and the new subscription's snapshot replaces the book. Updates arriving meanwhile are dropped, a second gap does not start a second recovery, and every other stream of the connection carries on; only a lost connection, or an unsubscribe answered with anything but `SubscribeResult::Ok` or `SubscribeResult::NotSubscribed` for a stream the server holds (`StreamError::Unsubscribe`), reconnects every stream. Each topic's requests take effect in the order they are made, whether the caller's, a resubscription or a depth recovery's: an answer overtaken by a later request of the same topic is delivered as an `Event::SubscribeResponse` but not applied, and the client then brings the server to what the caller last asked for. What the server holds is followed from every answer, applied or not. Every stream is live only once the subscription it holds delivered its initial state (snapshot, quote or trades baseline); what an earlier subscription delivered stays stale until then, and trade accounting carries on across it. A stream answered `SubscribeResult::AlreadySubscribed` whose subscription's initial state the client never took is unsubscribed and subscribed again, and its trades meanwhile are dropped. When a connection is lost, every book, quote and trades stream it fed is stale before `Event::Disconnected` (`OrderBook::stale`, `BestBidOffer::stale`, `MarketStream::live`) until the new connection delivers fresh state. Every depth snapshot replaces the book, whenever it arrives; depth20 books and quotes never go back to an older state than one they held. A subscribe or unsubscribe that was sent takes effect even when the caller stops waiting, and its answer is matched by request ID; an unsubscribe drops the topics' books, quotes and trade tracking when it is sent, and one left unanswered by a lost connection counts as done. It pings the server, reconnects with jittered backoff and resubscribes, and fails with `Error::QueueOverflow` rather than drop messages when the consumer falls behind.
- The stream's decoders are generated from the published schema (`sbe/marketstream-v<version>.xml`, schema id 20) with Real Logic's sbe-tool, pinned by `sbe/marketstream.lock.json`; `scripts/generate_marketstream_sbe.sh` regenerates and checks them. Messages of unknown templates are skipped; a message of a newer schema version is read by its declared block lengths, and one whose layout cannot be read that way is refused, as is a message of another schema.
- **Breaking:** `Error` gains the variant `TradeSession(tradews::TradeError)`, which reports a trading WebSocket command the server refused (`TradeError::Rejected`, with a typed `RejectCode`), one refused locally before anything was sent (`TradeError::NotSent`), one whose answer was lost (`TradeError::OutcomeUnknown`), a refused upgrade (`TradeError::Upgrade`), a server that did not confirm cancel-on-disconnect (`TradeError::NotProtected`) and a malformed or foreign message (`TradeError::Frame`, `TradeError::Schema`). `Error::mutation_outcome_unknown` is true for a lost answer, a refusal whose code ends in `_OUTCOME_UNKNOWN`, a batch refused with `UNAVAILABLE`, and an upgrade refused with `504` `ORDER_CANCEL_OUTCOME_UNKNOWN`; `Error::is_retryable`, `Error::retry_after` and `Error::is_request_deadline_exceeded` cover its refusals. Migration: add an `Error::TradeSession` arm where you match `Error` exhaustively.
- New `tradews` module: a client for the trading WebSocket with SBE (`/ws/trade.sbe`) that places and manages post-only, good-till-cancelled limit orders. `tradews::connect` signs the upgrade with the API key and opts into cancel-on-disconnect for the whole account (`X-Cancel-On-Disconnect: ACCOUNT`, 15 seconds by default, `Config::cancel_on_disconnect`), and refuses a server that does not echo the opt-in. The session subscribes to executions, orders and marketResync and sends no command before the server confirms it (`TradeSession::ready`, `Event::Ready`); one writer numbers every frame of a connection from `seq` 1, and a ping or command left unanswered for `Config::answer_timeout` ends the connection, which reconnects with backoff. Commands: `create_order`, `cancel_order`, `modify_order` (`ModifyBehavior::AmendOrReplace`, `AmendOnly`, `ReplaceOnly`), `cancel_all_orders`, `cancel_all_after` (arm, refresh, or disarm with `Duration::ZERO`), `batch_create_orders`, `batch_replace_orders` and `batch_cancel_orders`, each with a request `Deadline` that the session also enforces before sending. With `Config::timer_gate` (on by default), a create, modify, batch create or batch replace is sent only while the cancel-all-after timer acknowledged on the connection is armed past its deadline plus a margin; writing a disarm closes the gate and writing a refresh bounds it by its timeout before their answers arrive. A write the server does not take within `Config::answer_timeout` ends the connection too, and every command that may have been written is reported as an unknown outcome. Prices are `types::Price` ticks and quantities `types::Quantity`, exact integers; client order IDs, batch sizes, cancel-all pairs, timer and grace ranges and deadlines are checked before sending. Events: `Event::Order` (`OrderStatus`, `RejectCode`, `CancelReason` including `CancelAllAfter` and `CancelOnDisconnect`), `Event::Execution`, `Event::MarketResync`, `Event::Connected`, `Event::Ready`, `Event::Disconnected` (with the correlation IDs a lost connection left unanswered) and `Event::Pong`. A consumer that falls behind stops the session with `Error::QueueOverflow` rather than lose an event.
- New `auth::Credentials::sign_websocket_upgrade`, which signs a WebSocket upgrade with a fresh single-use nonce (`auth::HEADER_NONCE`, `X-API-NONCE`), as the trading WebSocket requires.
- The trading WebSocket codecs are generated from the published schema (`sbe/tradews-v49.xml`, schema id 7) with Real Logic's sbe-tool 1.35.6, pinned by `sbe/tradews.lock.json`; `scripts/generate_tradews_sbe.sh --proto` regenerates them from polyester-proto and `--check` compares them. A message of another schema or version stops the session; every frame is checked against its declared layout before it is read.
- The market-data stream reads the spot config again on every reconnect before it subscribes anything, so an environment reset that renumbers pairs never maps a new pair ID to an old pair's symbol or scale. Until the read succeeds the new connection sends nothing and every stream stays not live; a failed read is retried with backoff (`Event::PairsReloadFailed`). Each subscribed stream follows its pair by symbol to the pair's new ID (`Event::PairsReloaded` lists every renumbered pair), the state of any pair whose ID, symbol or scale changed is dropped, and a stream whose pair is no longer listed ends with `Event::Ended` carrying `StreamError::PairNotListed`. A read for a connection that a newer one has replaced installs nothing. A stream remembers each subscription by the pair's symbol, never by its ID, and every connection decodes with an immutable snapshot of the spot config it read: live messages and its automatic trade recovery through `MarketDataService` (`TradeHistory::trades_after_with`) use that snapshot's scales, so a recovered trade has the same scale as a live one, and no other stream's reload or catalog refresh can retarget a subscription or change a scale mid-connection. `Client::market_stream` also installs each read into `Client::catalogs` for the client's other users. A trades recovery that answers after its connection was replaced is dropped and its `TradesBackfill` carries `StreamError::Superseded`. `marketstream::connect` reads the first connection's pairs before it returns. New: `marketstream::PairsSource` and `marketstream::LoadedPairs`, `TradeHistory::trades_after_with` (provided), `marketstream::Config::reload` (set by `Client::market_stream` when the client hydrates its catalogs), `marketstream::Renumbered`, `StreamError::Superseded`, and `Pairs::symbol_id`, which every `Pairs` implementation must now provide. Affected: `marketstream::Pairs`, `marketstream::Config`, `marketstream::Event`, `marketstream::StreamError`. Migration: implement `Pairs::symbol_id` for your own pair tables, and add arms for the new `Event` and `StreamError` variants where you match them exhaustively.
- New `tradews::TradeSession::split`, which divides a trading session into a `TradeSender` (`Clone + Send + Sync`, every command method) and a `TradeEvents` (`recv`), as a WebSocket stream splits into its sink and stream, so tasks can keep commands in flight while another drains the events. With one `TradeSession`, `recv(&mut self)` and the commands borrowed the same value: a task draining events could not run beside tasks awaiting answers, and a lock around the session held commands behind `recv` until the bounded event queue overflowed. The events half owns the session: dropping it closes the session as `TradeSession::close` does, every command sent and unanswered is answered `TradeError::OutcomeUnknown` and every later one `NotSent::Closed`; dropping every sender ends nothing. Closing is a fence for every handle: from the moment either half closes the session or its events half is dropped, a command made through any clone, or still queued, is refused with `NotSent::Closed` and never written, while a command whose frame may already be on the wire stays `TradeError::OutcomeUnknown`. Shutdown, the timer gate and the unknown-outcome rules are unchanged, and `TradeSession` keeps every method. Affected: `tradews::TradeSession`, `tradews::TradeSender`, `tradews::TradeEvents` (new).
- The trading WebSocket and the market-data stream keep a refused WebSocket upgrade's answer and honour its `Retry-After`. `TradeError::Upgrade` gains `answer` (`Option<Box<AnswerMeta>>`), and the market-data stream's refused upgrade is an `Error::Realtime` with its `answer`, so `Error::answer` returns the answer's status and the delay it asked for with `Retry-After` (or `Retry-After-Ms`), and `Error::retry_after` the delay; a refused upgrade's `401` or `403` carries its answer too. Before, the market-data stream kept neither and the trading WebSocket only the status. Each then waits the longer of its backoff and that delay before connecting again, so a `503` with `Retry-After: 30` is not retried for 30 seconds: a server's `Retry-After` is never shortened, and the backoff's 30-second ceiling applies to the backoff alone. `tradews::Event::Disconnected` and `marketstream::Event::Disconnected` report the wait in `delay` and the answer in `error`, so a caller that will not wait can close the session or stream: closing either while it waits to reconnect, even with the wait already due, or while its reconnect upgrade is in flight, starts no upgrade after the close and ends the one in flight. `tradews::TradeError` no longer implements `Eq`, as `AnswerMeta` holds a delay in seconds. Affected: `tradews::TradeError::Upgrade`, `tradews::Event::Disconnected`, `marketstream::Event::Disconnected`, `Error::answer`, `Error::retry_after`.
- New `tradews::TradeSender::disable_cancel_on_disconnect` (also on `TradeSession`): stops sending `X-Cancel-On-Disconnect` on every later upgrade of the session, for every clone of its sender, so a reconnect during a shutdown or kill sequence no longer renews the account's cancel-on-disconnect. Each upgrade reads the switch once, before it builds its request: one already in flight keeps its opt-in and still requires the server to confirm it, and every upgrade that starts after the call returns opts out, including the one that ends a retry wait already pending. The switch cannot be undone; open a new session to opt in again. Additive: `Config::cancel_on_disconnect` and every session that never calls it behave as before. The README's trading WebSocket section shows it in a kill sequence with `cancel_all_after` as the backstop.
- New `ReconnectLimiter` (`polyester::ReconnectLimiter`, in the new `reconnect` module): an opt-in cap on how many WebSocket upgrades the trading sessions and market-data streams that share it have in flight at once, for a process that runs many of them, such as one trading session per sub-account. Create it once with `ReconnectLimiter::new(max_concurrent_upgrades)` (from 1; zero is `Error::Validation`) and give a clone to `tradews::Config::reconnect_limiter` and `marketstream::Config::reconnect_limiter` (new, `None` by default). A session takes a slot only for an upgrade, after its backoff and any `Retry-After` wait have run out, and gives it back when the upgrade is accepted, refused or times out; waiting for a slot adds to that wait and never shortens a server's `Retry-After`. Waiting sessions get slots first in, first out. The first upgrade of `tradews::connect` and `marketstream::connect` takes a slot too, so sessions started together are bounded like sessions dropped together. Closing a session that waits for a slot ends the wait with no upgrade afterwards, as does dropping a `connect` future that waits. `ReconnectLimiter::upgrades_in_flight` reports the slots taken. Without a limiter, nothing changes.

## 0.32.0

Package version: `0.32.0`. Git tag: `v0.32.0`. Released 2026-11-16. Built on public contract `1945423`.

- **Breaking:** A zero `new_price` on `orders.modify` or a batch replace item is refused with `Error::Validation` (`new_price must be positive`) before sending, in every form: `Price::from_ticks(0, ..)`, `Price::from_decimal_str("0", ..)` and `Price::from_decimal(Decimal::ZERO, ..)`. It was sent as a price of 0. A zero `new_qty` (`Quantity::from_scaled(0, ..)`; the decimal constructors already refuse 0) is refused as `new_qty must be positive` where it read `qty must be positive`. Affected: `orders.modify` (`ModifyOrderParams.new_price`, `new_qty`), `orders.batch_replace` (`BatchReplaceItem.new_price`, `new_qty`). Migration: leave `new_price` or `new_qty` `None` to keep the order's price or quantity.
- `chain::quote_zipper_fee` returns `Error::Transport` for a malformed `eth_call` result instead of panicking: non-ASCII text from the RPC endpoint panicked with "byte index is not a char boundary". Each result must be `0x` and exactly one 32-byte ABI word (64 hex digits); a short, odd-length, non-hex or empty result is refused, as is an address result whose upper 12 bytes are not zero, where a short or non-hex address result was accepted. Affected: `chain::quote_zipper_fee`.
- `orderbook.create_subscription` with only `symbol_id` works: the pair's symbol and quantity scale come from the spot catalog, and each view carries the catalog symbol. It returned `Error::Validation` asking for a catalog quantity scale for an empty symbol. A `symbol_id` that is not in the catalog is `Error::Validation` naming it, before connecting. Affected: `orderbook.create_subscription` (`CreateSubscriptionOptions.symbol_id`).
- An invalid order book `bucket` (such as `"abc"`, `"-1"` or `"0"`) is still refused with `Error::Validation`, by `orderbook.create_subscription` before connecting and by `Subscription::set_bucket`, which keeps the current bucket; the message now says what is valid: a positive price with at most 9 decimal places, such as `"0.01"`, or empty for no bucketing. Affected: `orderbook.create_subscription` (`CreateSubscriptionOptions.bucket`), `orderbook::Subscription::set_bucket`, `orderbook::parse_bucket_ticks`.

## 0.31.1

Package version: `0.31.1`. Git tag: `v0.31.1`. Released 2026-11-16. Built on public contract `1945423`.

- Public lifecycle-flow reads work without an API key. `lifecycle.list_flows`, `get_flow`, `list_flows_by_tx` and `get_flow_by_tx` are public in the contract but were signed, so a client without an API key refused them with `Error::Auth("This endpoint requires Polyester API-key credentials")` before sending. They are now sent unsigned, like the other public reads, whether or not the client has a key. `lifecycle.list_account_flows` and `get_account_flow` stay signed. A test now checks every RPC the SDK sends against the contract's public or signed marking (`testdata/rpc_auth.tsv`), and sends every public read from a client without a key.

## 0.31.0

Package version: `0.31.0`. Git tag: `v0.31.0`. Released 2026-11-16. Built on public contract `1945423`.

- **Breaking:** The generated protobuf types follow the contract: `UserProfilePatch::twitter` and `PortfolioAssetEquity::balance_q` are removed, and `polyester::proto::ai::v1::AssistantKeyView` is now the contract's enum of that name instead of the former message type. Migration: code that used those generated items directly drops the removed fields (portfolio equity balances come from the exact `balance` field) and matches `AssistantKeyView` as an enum.
- **Breaking:** Balance history is exact. `BalanceHistorySeries::balance_q` (7-decimal `u64`, which could not hold a balance above 1,844,674,407,370.9551615 units) is removed; `BalanceHistorySeries::balance` holds each point's balance as an `AssetAmount` at the fixed 18-decimal ledger scale (domain `LedgerE18`, the series' asset ID), decoded exactly from the contract's `U128`, never through a float. A series with another number of balances than the response's `points`, or a balance above 2^127 - 1 at 10^-18, is refused with `Error::ResponseContract`; this is also what a server still on the previous contract answers, whose series would otherwise read empty. `codecs::decode::balance_history_from_proto` returns `Result<BalanceHistory>`. Affected: `balances.get_balance_history`, `models::BalanceHistorySeries`, `codecs::decode::balance_history_from_proto`. Migration: read `series.balance[i].as_scaled()` (the balance times 10^18) where you read `series.balance_q[i]` (times 10^7), and handle the `Result` of the decoder.
- **Breaking:** Lifecycle flows no longer name or select the account behind them. `LifecycleFlowSummary::owner_account_id` is removed, and `ListFlowsRequest`'s account selector holds only `SmartAccountAddress`. The public reads (`lifecycle.list_flows`, `get_flow`, `list_flows_by_tx`) return only flows a chain transaction started; a transfer between Trading accounts or a Trading withdrawal not yet sent to a chain is not listed, and `get_flow` does not find it. Affected: `models::LifecycleFlowSummary`, `lifecycle.list_flows`, `get_flow`, `list_flows_by_tx`. Migration: identify an account by `smart_account_address`; replace a `list_flows` that selected `OwnerAccountId` with the new `lifecycle.list_account_flows`, signed as the account (an API key needs `READ_FUNDING_FLOWS`).
- New account flow reads, signed with the API key: `lifecycle.list_account_flows(ListAccountFlowsRequest)` lists the account's own flows and the transfers it receives, including those no chain transaction started, with the filters, ordering and pagination of `list_flows`; `lifecycle.get_account_flow(flow_id, SubAccountScope)` reads one, with its `participation`. `subaccount_id` takes the configured default sub-account when unset. A `limit` above 500, and an ID filter (`polyester_chain_ids`, `zipped_asset_ids`, `unified_asset_ids`) with more than 100 IDs, an ID of 0 or a repeated ID, are refused with `Error::Validation` before sending. A reply without the flow or its flow ID is `Error::ResponseContract`. Affected: `LifecycleService::list_account_flows`, `get_account_flow`, `codecs::decode::account_flows_list_from_proto`, `account_flow_from_get_response` (all new).
- A deposit delivered to the account's own Polyester Chain wallet, because its destination did not accept the credit, reads `lifecycle_reason` `zipper_deposit_delivered_to_wallet` (reason 106) instead of `unknown_reason_106`. It is a completed flow at the wallet, not a failure, and no Funding or Trading balance was credited. Affected: every `LifecycleFlowSummary` read.
- **Breaking:** An OCO's client list ID follows the client order ID rule: 1 to 36 ASCII letters, digits, `.`, `_`, `:`, `/` or `-` (`^[A-Za-z0-9._:/-]{1,36}$`), with the same error text. Any 1 to 36 bytes of UTF-8 were accepted; spaces, other punctuation and non-ASCII characters are now refused with `Error::Validation` before sending, by `triggers.create_oco` and by the `ListTriggersOpts::client_list_id` filter, as the API now refuses them on every path. The ID is still sent as given, never trimmed. Affected: `triggers.create_oco`, `triggers.list_with`. Migration: map client list IDs into the allowed characters before creating an OCO.
- New batch internal transfers: `internal_transfers.batch_create(BatchCreateInternalTransfersParams { legs, idempotency_key })` submits 1 to 10 Trading to Trading transfers under one idempotency key and one MFA check (an API key needs none). Each `InternalTransferBatchLeg` takes the fields of a single transfer: the asset, an exact `AssetAmount` rescaled to 18 decimals as `create` does, the source `SubAccountScope`, and exactly one destination. Every leg is checked before any runs: if one fails, the batch is `refused` and nothing moves. Otherwise legs run in order and are not atomic. `InternalTransferBatchResult` has the batch `request_id` and `status` (`submitted` or `refused`) and one `InternalTransferBatchLegResult` per leg: `status` `accepted`, `pending` (outcome not known yet: retry the batch with the same key and legs), `refused` or `not_submitted`, with its request, transfer and flow IDs, `accepted_at_ts_ns`, the exact amount and `error_code` as a lowercase label such as `insufficient_funds`. No legs or more than 10, an idempotency key empty or over 128 characters, a leg without an asset or without exactly one destination, and an amount finer than 18 decimals are refused before sending. Each leg result also carries the `destination` the server resolved (`models::ResolvedDestination` with `root_account_public_id`, `subaccount_public_id` and `smart_account_address`; `None` when the server sent none). A reply with an unknown batch status, another number of legs, legs out of order, or an accepted or pending leg without its IDs is `Error::ResponseContract`: the batch may have moved funds, so retry it with the same key. Affected: `InternalTransfersService::batch_create`, `models::BatchCreateInternalTransfersParams`, `InternalTransferBatchLeg`, `InternalTransferBatchResult`, `InternalTransferBatchLegResult`, `ResolvedDestination`, `codecs::decode::internal_transfer_batch_from_proto` (all new).
- New dust conversion, `client.dust` (`services::DustConversionService`): `list_balances` previews which Trading balances convert into a target, `quote` prices the conversion for 30 seconds (`quote_id` `None` when nothing converts), `convert` converts a quote under an idempotency key (`completed`, or `processing`: poll `get_conversion`), and `list_conversions` pages conversions newest first. Every amount, USD value, payout and fee is an exact `AssetAmount` at 18 decimals; each price keeps `price_ticks` and `price_scale` and adds the exact `price`. Eligibility, state and refusal code are lowercase labels (`eligible`, `above_value_limit`, `processing`, `refused`, `dust_price_moved`). Signed: an API key needs `CONVERT_DUST` (and `READ_BALANCES` for `list_balances`). A target of 0 for `quote`, a quote ID empty or over 64 characters, an idempotency key empty or over 128, a conversion ID that is not 1 to 39 digits without a leading zero, a `limit` above 100 and a page token over 256 characters are refused before sending; a conversion reply without the conversion or its ID is `Error::ResponseContract`. Affected: `Client::dust`, `services::DustConversionService`, `models::DustBalances`, `DustBalance`, `DustQuote`, `DustAssetPrice`, `DustConversion`, `DustSource`, `DustConversionsList`, and their decoders in `codecs::decode` (all new).
- New statement export files: `statements.create_export(CreateStatementExportOptions)` asks for a whole-period statement of one account as one CSV file produced in the background (at most 366 days; idempotent per a key of 1 to 64 letters, digits, `.`, `_`, `:` or `-`), and `statements.get_export(export_id)` and `statements.list_exports(scope, limit, page_token)` read its status (`queued`, `processing`, `ready`, `failed`, `expired`), progress (`movements`, `bytes`), `complete_through` and expiry; a failed export says `failure_reason` `too_large` or `unavailable`. A `ready` file is downloaded over REST only, with an authenticated `GET /v1/statements/exports/{export_id}/file`; this SDK does not download it. A missing start or end, an end not after the start or more than 366 days after it, an invalid key, a `limit` above 100 and a page token over 512 characters are refused before sending; a reply without the export or its ID is `Error::ResponseContract`. Signed like the statement pages: an API key needs `READ_STATEMENTS`. Affected: `StatementsService::create_export`, `get_export`, `list_exports`, `services::CreateStatementExportOptions`, `models::StatementExport`, `StatementExportsList`, and their decoders in `codecs::decode` (all new).
- **Breaking:** A statement says how far it is complete: `StatementPage::complete_through` and `Statement::complete_through` (`Option<Timestamp>`) is the time up to which the statement covers movements, the same on every page; when it is before `end_time`, request the rest later. Pages of one statement that disagree on it are `Error::ResponseContract`. A page token issued before this release is refused by the API as invalid: restart from the first page. `StatementPage` and `Statement` no longer implement `Eq` (they still implement `PartialEq`). Affected: `statements.export`, `statements.export_page`. Migration: add `complete_through` to struct literals of `StatementPage` and `Statement`, and compare them with `==` rather than requiring `Eq`.
- A withdrawal to a network whose withdrawals of the asset are suspended is refused with `ERROR_CODE_ROUTE_SUSPENDED` (REST `ROUTE_SUSPENDED`), which the new `Error::is_withdraw_route_suspended()` reports. The suspension is temporary: submit a new request once the route's `withdrawal_status` in `GetDepositWithdrawConfig` is `normal` again. Nothing was reserved for the refused request, so a fresh request is safe. It is final and not retryable as sent. Auto-withdraw runs and broker payouts report such a destination with the status-only reason `route_suspended`. Affected: the withdraw service's create methods, `Error`, `errors::withdraw_codes` (new).
- Dust and statement export refusals follow their retry rule: `ERROR_CODE_DUST_BUSY` is retryable (`Error::is_retryable`); `ERROR_CODE_DUST_WINDOW_ACTIVE`, `ERROR_CODE_DUST_DAILY_LIMIT` and `ERROR_CODE_STATEMENT_EXPORT_LIMIT_REACHED` are not, although the API answers them as `resource_exhausted`; the quote-again codes (`DUST_QUOTE_EXPIRED`, `DUST_PRICE_MOVED`, `DUST_BALANCE_CHANGED`, `DUST_NOT_ELIGIBLE`, `DUST_CONVERSION_FAILED`, `DUST_TARGET_UNAVAILABLE`) and `ERROR_CODE_IDEMPOTENCY_KEY_REUSED` are final. The codes are named in `errors::transfer_codes` and `errors::ledger_codes` (new).
- A trigger create refused with `ERROR_CODE_LADDER_LEVEL_REJECTED` names the refused order's own code and position: `Error::cause_code()` (for example `ERROR_CODE_MAX_OPEN_ORDERS_PER_PAIR`) and `Error::level_index()` (zero-based; `Some(0)` is the first order). Both are `None` for every other refusal. Affected: `triggers.create`, `Error` (new accessors).
- **Breaking:** `Error::RateLimit` gains `cause_code: Option<String>` and `level_index: Option<u32>`, so a ladder order refused for the account's rate limit (`ERROR_CODE_LADDER_LEVEL_REJECTED` with cause `ERROR_CODE_RATE_LIMIT_EXCEEDED`) keeps its cause and zero-based index, read with the same `Error::cause_code()` and `Error::level_index()`; they are `None` on every other rate-limit error. Affected: `Error::RateLimit`. Migration: code that builds `Error::RateLimit` sets `cause_code: None, level_index: None`; an exhaustive struct pattern names the two fields or ends with `..`.
- An X/Twitter or Discord link refused with `AUTH_SOCIAL_ACCOUNT_COOLDOWN` or `AUTH_SOCIAL_CHANGE_LIMIT_REACHED` says when the same request may succeed: `Error::retry_at()`. `social_verification.get` returns a failed verification's `retryAfter`. Starting a verification while an X/Twitter or Discord account is linked needs a fresh step-up proof on a session. Affected: `Error` (new accessor), `errors::auth_codes` (two new codes), `social_verification.start`, `social_verification.get`.
- `CONVERT_DUST` and `READ_FUNDING_FLOWS` read by name in API and sub-account policies.
- `testdata/contract_field_inventory.tsv` lists every field of the messages this release added or changed that the SDK wraps, with the parameter or model field that carries it; a test fails when one is missing or gone.
- Documentation: a route's `deposit_status` or `withdrawal_status` `suspended` means do not send funds on it until it is `normal` again; an empty status means not known, not suspended. A Trading withdrawal accepted before a suspension stays pending with its funds held and is sent once the route is `normal`. `TradingRateLimits::rules` are enforced per account and shared by all its API keys and sessions; `api_key_rules` are informational only.

## 0.30.0

Package version: `0.30.0`. Git tag: `v0.30.0`. Released 2026-11-15. Built on public contract `ebbbeac`.

- `triggers.create` and `orders.cancel_all_after` refuse values the published contract refuses with `Error::Validation` before sending, where the server refused them after a round trip: a TWAP's `twap_duration_ms` below 1000 and `twap_slice_interval_ms` below 100 (both were only required to be positive), a ladder's `ladder_levels` outside 2 to 100 (it was only required to be positive), and a `timeout_sec` other than 0 or 10 to 120. A slice interval longer than the duration is refused as before, now naming `twap_slice_interval_ms`. Affected: `TriggersService::create`, `OrdersService::cancel_all_after`.
- A trading withdraw's `idempotency_key` of spaces only is signed as given instead of refused: the contract only requires a non-empty key, so only `""` is refused. The key is never trimmed, as before. Affected: `WithdrawService` prepare and create methods.
- **Breaking:** A public ID that decodes to 0 is refused with `Error::Validation` before anything is sent: `"1"` (the base58 form of 0), `"0"` and `"00"`. Such an ID names nothing; the single-ID trigger and order calls sent it as 0 and the server refused it after a round trip, while an optional filter read 0 as no filter. Only the batch trigger cancel refused it. `codecs::scalars::id_to_u64` now refuses it, as `parse_account_id` already did for account IDs, so every call that takes a trigger, order, OCO, batch or alert ID does. Affected: `triggers.get_by_id`, `cancel_by_id`, `pause_by_id`, `resume_by_id`, `resume_with`, `modify`, `list_events_by_id`, `cancel_oco_by_id`, `batch_cancel`, `list_with` (parent order and OCO filters); `orders.get`, `get_with`, `cancel_with`, `modify`, `batch_cancel`, `batch_replace`, `get_batch_replace_status`, `wait_batch_replace`, `wait_for_order_trades_complete`, `list_open_with` and `list_history_with` (trigger filter); `trades.list_with` (order and lineage filters); `codecs::scalars::id_to_u64`. Migration: pass IDs exactly as the API returned them; none of them is `"1"`.
- **Breaking:** A successful get reply without the object it returns is refused with `Error::ResponseContract`: a trigger, order, API key, sub-account, lifecycle flow (or its summary), changelog post (or its summary) or deployment group, a changelog change entry or attention item without its change, and an address-book entry or tag write without its entry or tag. The policy and invite reply decoders follow the same rule: `get_subaccount_policy_from_proto`, `create_subaccount_policy_from_proto`, `update_subaccount_policy_from_proto`, `get_api_policy_from_proto`, `create_api_policy_from_proto`, `update_api_policy_from_proto`, `invite_subaccount_member_from_proto` and `respond_subaccount_invite_from_proto` return `Result` instead of `Option` and refuse a reply without its policy or invite, and the policy list decoders a policy without its ID. `triggers.get` and `get_by_id` and `api_keys.get` returned `Ok(None)`, `orders.get` a result whose `order` was `None`, `sub_accounts.get` a result whose `subaccount` was `None`, and the flow, changelog and address-book replies `Error::Transport`. A trigger, order, key or sub-account that does not exist is refused by the API with its own error, never answered with an empty success; the `Option`s these calls return are now always `Some`. `api_key_from_create_proto` and `api_key_from_update_proto` return `Result<ApiKeySummary>` instead of `Option`, and `create_subaccount_from_proto`, `subaccount_members_list_from_proto` and `resolved_accounts_from_proto` return `Result`. Affected: `triggers.get`, `get_by_id`, `orders.get`, `get_with`, `api_keys.get`, `sub_accounts.get`, `lifecycle.get_flow`, `changelog.get_post`, `get_deployment_group`, `address_book.create_entry`, `update_entry`, `copy_entry`, `create_tag`, `update_tag`, and the listed decoders. Migration: stop treating `None` from these calls as "not found"; handle `Error::ResponseContract` as a broken reply and not-found by the API error; unwrap the `Result` of the listed decoders.
- **Breaking:** `Config::ws_url` with a query string or fragment is refused with `Error::Validation` (`ws_url must not contain a query string or fragment`), as `api_url` already was. The realtime client sent the URL as configured on every connect and reconnect, outside any signature. Affected: `Client::new`. Migration: remove the query string or fragment from the WebSocket URL; `wss://host` and `wss://host/connection/websocket` are unchanged.
- **Breaking:** A reply that must carry the ID of what it created or changed, but carries 0, returns `Error::ResponseContract`: a batch trigger cancel item without its trigger ID (it read as an empty `trigger_id`), an accepted batch create item without its order ID, a cancel-all reply listing a canceled order without its ID, and an address-book entry or tag reply without its ID (they read as ID `"0"`). The call may have applied: read back before retrying. Reads refuse an object without its ID the same way: a trigger (`triggers.get`, `get_by_id`, `list`, `list_with`), a trigger event's trigger ID (`triggers.list_events`, `list_events_by_id`), an order, or an order's or trade's lineage when present (`orders.get`, `get_with`, `list_open_with`, `list_history_with`, `wait_for_order_trades_complete`, `trades.list_with`), an alert (`alerts.list`), an address-book entry, a tag of any returned entry, a view entry, whitelist entry or counterparty (`address_book.list_entries`, `list_transfer_destinations`, `get_view`, `list_internal_transfer_whitelist_entries`, `list_transfer_counterparties`), an API key (`api_keys.get`, `api_keys.list`), a sub-account, its invites (and their sub-account ID), API keys and attached policy (`sub_accounts.get`, `sub_accounts.list`, `sub_accounts.list_invites`), the caller's account and, when present, session (`auth.me`), a zipped supply group (`chain_analytics.get_zipped_asset_supply_group`), a lifecycle flow (`lifecycle.get_flow`, `list_flows`, `list_flows_by_tx`), a changelog change (`changelog.get_post`, `list_changes`, `get_deployment_group`), a ledger transfer (`transfers.list`, `balances.list_transfers`) and a status event (`Client::list_status_events`). Tags of the address-book view, sub-account members (also `sub_accounts.list_members`), status-event updates, resolved accounts (`codecs::decode::resolved_accounts_from_proto`), and the created or updated API key and created sub-account (`api_key_from_create_proto`, `api_key_from_update_proto`, `create_subaccount_from_proto`) are checked too. `testdata/reply_id_inventory.tsv` lists every ID field of every reply in the contract as required or not, and a test fails when the contract gains an ID field it does not list. The read decoders in `codecs::decode` for these replies return `Result` (the created-key, sub-account, member and resolved-account decoders are covered by the missing-object entry above), and `codecs::decode::zipped_asset_supply_group_from_proto` is new. Affected: `triggers.batch_cancel`, `orders.batch_create`, `orders.cancel_all`, `orders.cancel_all_with`, `address_book.create_entry`, `update_entry`, `copy_entry`, `create_tag`, `update_tag`, and the reads above. A trigger create, cancel, pause, resume or modify reply, or an OCO create or cancel reply, without its trigger or OCO ID returns `Error::ResponseContract`, as an order reply without its order ID does; it returned `Error::Transport`. A reply without its status or client ID still returns `Error::Transport`. Affected: `triggers.create`, `cancel_by_id`, `pause_by_id`, `resume_by_id`, `resume_with`, `modify`, `create_oco`, `cancel_oco_by_id`. Migration: handle `Error::ResponseContract` from these calls, and match it where you matched `Error::Transport` for a trigger or OCO reply without its ID; code that calls the listed decoders itself handles their `Result`.
- Dropping a client forgets its secrets: its API private key and the userinfo of its API and WebSocket URLs stay in the process for redacting error text only while a client, or a clone of its credentials, holds them. Two clients that share a secret keep it redacted until both are dropped.

## 0.29.1

Package version: `0.29.1`. Git tag: `v0.29.1`. Released 2026-11-15. Built on public contract `ebbbeac`.

- The README examples match the 0.29.0 API: the quickstart and the open-order, cancel, order read, fee and rate-limit examples pass a `SubAccountScope` where they passed `None`, the order read options and the smart account wallet balances name `SubAccountScope::Default`, and the flow lookups by transaction are `client.lifecycle.list_flows_by_tx` and `client.lifecycle.get_flow_by_tx`. The trigger prose names `status` `failed` and `failure_reason` `trigger_would_trigger_immediately` as typed reads return them, and the order read, trigger list and trigger modify options are complete examples instead of inline struct literals. Every Rust example in the README is now compiled as a doctest (`cargo test --doc`) in CI, so an example that no longer matches the API fails the build. No API change. Affected: `README.md` (documentation only).

## 0.29.0

Package version: `0.29.0`. Git tag: `v0.29.0`. Released 2026-11-15. Built on public contract `ebbbeac`.

- **Breaking:** Every typed helper takes a `SubAccountScope` (`polyester::models::SubAccountScope`) where it took an `Option<u64>` sub-account: `SubAccountScope::Default` takes `Config::default_sub_account_id`, or the main account when none is set; `SubAccountScope::Main` is always the main account; `SubAccountScope::Id(id)` (also `id.into()`) names a sub-account, and 0 is refused with `Error::Validation` before sending. With a default configured there was no way to address the main account per call. Helpers that send through a raw-request method, such as `triggers.cancel_by_id` and `orders.cancel_with`, no longer apply the default a second time. Raw-request methods are unchanged: an unset sub-account takes the default, and the typed helpers are the per-call path to the main account. `deposit.list_addresses` now takes a `SubAccountScope` too: with `SubAccountScope::Default` it lists the configured default sub-account's deposit addresses, where it used to send no sub-account and list the main account's, so a client configured for a sub-account could show another account's address. Affected: the `subaccount_id` field of `ModifyOrderParams`, `CreateOrderParams`, `PreviewOrderParams`, `CancelOrderParams`, `CancelAllOpts`, `ListOpenOrdersOpts`, `ListOrderHistoryOpts`, `ListUserTradesOpts`, `GetOrderOpts`, `StopAllTradingScope`, `CreateInternalTransferParams`, `CreateWalletTradingWithdrawParams`, `CreateTriggerParams`, `ModifyTriggerParams`, `ResumeTriggerParams`, `CreateOcoParams`, `ListTriggersOpts`, `ListTriggerEventsOpts` and `ExportStatementOptions`, and the sub-account argument of `orders.list_open`, `orders.list_history`, `orders.get`, `orders.cancel_by_client_order_id`, `orders.cancel_by_order_id`, `orders.cancel_all`, `orders.batch_create`, `orders.batch_cancel`, `orders.batch_replace`, `orders.get_batch_replace_status`, `orders.wait_batch_replace`, `orders.cancel_all_after`, `trades.list`, `triggers.get_by_id`, `triggers.cancel_by_id`, `triggers.cancel_oco_by_id`, `triggers.batch_cancel`, `triggers.pause_by_id`, `triggers.resume_by_id`, `api_keys.list`, `withdraw.get_wallet_balances`, `fees.get_spot_fee_rates`, `rate_limits.get_trading_rate_limits` and `deposit.list_addresses`. Migration: `None` → `SubAccountScope::Default`; `Some(id)` → `id.into()` or `SubAccountScope::Id(id)`; to act on the main account while a default is set, pass `SubAccountScope::Main`.
- **Breaking:** New `ModifyOrderParams.clear_attached_risk` and `BatchReplaceItem.clear_attached_risk` remove an order's attached take-profit, stop-loss and trailing stop, alone or with a new price or quantity, as the TypeScript SDK's `clearRisk` does: they send an empty `new_attached_risk`. With a `new_attached_risk` they are refused with `Error::Validation` before sending; an empty `AttachedRisk` is still refused. Affected: `orders.modify`, `orders.batch_replace`. Migration: add `clear_attached_risk: false` to struct literals of `ModifyOrderParams` and `BatchReplaceItem`.
- **Breaking:** `triggers.create_oco` refuses what the contract does not allow on an OCO with `Error::Validation` before sending: a leg's `trigger_price_source` `mark`, and `self_trade_prevention_mode` `decrement`. Both were sent and refused by the server, while a single trigger create already refused `mark` before sending. `last`, `index` and the `expire_*` modes are unchanged, and `OcoLegParams.trigger_price_source` and `CreateOcoParams.self_trade_prevention_mode` now document the accepted values. Affected: `triggers.create_oco`, `OcoLegParams`, `CreateOcoParams`. Migration: use `last` or `index`, and `expire_taker`, `expire_maker` or `expire_both`.
- **Breaking:** Order cancel and reject reasons use the short lowercase labels every Polyester SDK now uses: `Order::cancel_reason` and an order event's `cancel_reason` drop the `CANCEL_REASON_` prefix (`stp_decrement`, `user_request`, `replaced`, `price_band`), and `Order::reject_reason`, an order event's `reject_reason` and a command outcome's `reason` drop the `ERROR_CODE_` prefix (`post_only_cross`, `peg_limit_violation`, `order_unknown`). A value this SDK does not know reads as before: `UNKNOWN_ERROR_CODE(<n>)` for a reject reason, `UNKNOWN(<n>)` for a cancel reason or a command outcome. `Error::Api`'s `code`, `OrderErrorDetail::code` and the batch item `code` fields are unchanged, and trigger reasons already read this way. Affected: `orders.get`, `orders.list_open`, `orders.list_history`, `orders.subscribe` (`OrderStreamEvent`, `OrderCommandOutcome`). Migration: compare reasons with the lowercase label, for example `Some("stp_decrement")` instead of `Some("CANCEL_REASON_STP_DECREMENT")`.
- **Breaking:** Order write results keep the times and quantities the server returns. `OrderMutationResult` gains `accepted_ts_ns`, when `orders.create` was admitted (0 for a cancel); `BatchCreateOrdersResult` and `ModifyOrderResult` gain `ts_ns`, when the server answered; each is in nanoseconds since the Unix epoch, read from the exact nanosecond field, or from its paired timestamp when only that is sent, and 0 when neither is. `BatchCreateResultItem` gains `resolved_base_qty`, the base quantity the server resolved for an accepted item, at that item's pair base scale as `orders.create` returns it (`None` for a rejected item), and `BatchReplaceAdmissionItem` gains `existing_order_id`, the order already holding a rejected item's new client order ID when the server knows it, as `BatchCreateResultItem::existing_order_id` does. Affected: `orders.create`, `orders.batch_create`, `orders.modify`, `orders.batch_replace`. Migration: struct literals of these types no longer compile; add the new fields.
- `BatchReplaceOrdersResult::accepted_ts_ns` reads the paired `accepted_ts` timestamp when the server sends only that, instead of 0. Affected: `orders.batch_replace`.
- `Client::stop_all_trading` no longer reports a trigger failure when an OCO is active. It canceled each listed trigger on its own, so the cancel of an OCO's second leg, which the first leg's cancel had already ended, was refused `ERROR_CODE_TRIGGER_STATUS_INVALID` and listed in `trigger_failures`, although nothing was left live. It now cancels each OCO once with `TriggersService::cancel_oco_by_id` by its `order_list_id`, lists both legs in `canceled_trigger_ids`, and cancels the other triggers as before. A trigger or OCO listed as live whose cancel is refused `ERROR_CODE_TRIGGER_STATUS_INVALID` or `ERROR_CODE_TRIGGER_NOT_FOUND` has ended since, after a fill or an expiry for example: it is stopped, and is in neither `canceled_trigger_ids` nor `trigger_failures`. Any other refusal or error is still listed in `trigger_failures`; for an OCO, each listed leg is listed with it. A trigger listed on two pages is canceled once. Types are unchanged. Affected: `Client::stop_all_trading`, `StopAllTradingResult`.

## 0.28.0

Package version: `0.28.0`. Git tag: `v0.28.0`. Released 2026-11-14. Built on public contract `ebbbeac`.

- New account statements (`ledger.read.v1.LedgerReadService/ExportStatement`, devnet once the 0.93.2 release is deployed there): `client.statements.export(ExportStatementOptions { subaccount_id, start_time, end_time, asset_id, page_token })` exports one account's balance movements for a period of at most 366 days as CSV, every page requested with the same fields and concatenated in order (`Statement { csv }`), and `client.statements.export_page(opts)` reads one page (`StatementPage { csv, next_page_token }`: at most 2000 ledger movements, an entry never split, the header row on the first page only). It is signed like the other ledger reads: an unset `subaccount_id` takes `Config::default_sub_account_id`, `Some(0)` is refused, and an API key needs the new `READ_STATEMENTS` action on its policy (`READ_BALANCES` and `READ_INTERNAL_TRANSFERS` do not grant it). `start_time` (inclusive) and `end_time` (exclusive) are required and sent exactly; a missing one is `Error::Validation` before sending. `asset_id` `None` covers every asset. The statement is CSV only, kept as sent (RFC 4180, UTF-8, CRLF). `Statement::rows()` and `codecs::decode::parse_statement_csv` read it by header name into `StatementRow` (`time` as a `Timestamp`; `entry_id`, `kind` for the `type` column, `account`, `asset`, `amount`, `fee`, `balance`, `link_id` and `flow_id` as exact strings, never floats), so reordered columns and columns a later release appends read the same and `flow_id` is empty in a statement without it; a missing column, a malformed row, content after a closing quote or an unterminated quoted field is `Error::ResponseContract`. Affected: `Client::statements`, `services::StatementsService`, `services::ExportStatementOptions`, `models::Statement`, `StatementPage`, `StatementRow` and `codecs::decode::parse_statement_csv` (all new).
- Statements carry each entry's lifecycle flow: a last `flow_id` column holds the public flow ID of a deposit, withdrawal or internal transfer, as the transfer history returns it, and is empty for other entries such as trades. Read columns by name, as `StatementRow` does.
- New maintenance and incident status (`system.v1.SystemService/ListStatusEvents`, public, no API key, never signed): `client.list_status_events(ListStatusEventsOptions { states, kinds, limit, page_token })` returns one page of status events, latest start first (`StatusEventsPage { events, next_page_token }`; `limit` defaults to 50, maximum 200), and `client.status_events(opts)` streams every page, sending the same `states` and `kinds` with each page token. Filters are lowercase labels: `states` `scheduled`, `in_progress`, `completed` or `cancelled`, and `kinds` `maintenance` or `incident`; an unknown label is `Error::Validation` before sending, and no `states` asks for the server's default of current and upcoming events, which the SDK does not expand. A `StatusEvent` has its base58 `id`, `kind`, `state`, `title`, `message`, `start_time`, `end_time` (`None` for an incident with no expected end), affected `services` (`trading`, `market_data`, `deposits`, `withdrawals`, `transfers`, `accounts`, `api`, `quickswap`, `app`), `markets`, `assets`, an incident's `impact` (`degraded`, `partial_outage` or `major_outage`; empty for maintenance and an incident not yet assessed), `updated_at`, `cancelled_at` (set only once cancelled), progress notes in `updates` (`id`, `message`, `created_at`, oldest first) and `details_url` (`None` when there is no announcement). A value newer than this SDK reads `unknown(<n>)`. Answers can be up to 10 seconds old: poll at most that often and compare `updated_at` per `id`. Affected: `Client::list_status_events`, `Client::status_events`, `services::ListStatusEventsOptions`, `models::StatusEvent`, `StatusEventUpdate`, `StatusEventsPage` and the `STATUS_EVENT_*` label constants (all new).
- New asset information (`marketdata.v1.MarketDataService/ListAssetInfo`, public, no API key, never signed): `market_data.list_asset_info(&asset_ids)` returns team-maintained information for the given numeric asset IDs (the spot config's `ledger_id`), or every saved entry for none, complete and in ascending `asset_id` order with no paging; assets never edited are left out. An `AssetInfo` has its `asset_id`, `editorial_updated_at` and `editorial`: a plain-text `description`, `website_url`, `whitepaper_url`, `explorer_urls`, `x_url`, `telegram_url`, `discord_url`, `github_url`, the launch `issue_date` as `YYYY-MM-DD` text and `issue_price` (`AssetIssuePrice { amount, currency }`, the amount an exact decimal string in `USD`; `None` when unknown, distinct from a known zero). Empty strings and lists mean unknown. Affected: `MarketDataService::list_asset_info`, `models::AssetInfo`, `AssetEditorialInfo` and `AssetIssuePrice` (all new).
- **Breaking:** Each deposit and withdrawal route says whether it accepts new requests: `ZipperAssetChainVariant` gains `deposit_status` and `withdrawal_status` (`"normal"` or `"suspended"`, the `ROUTE_STATUS_*` constants; empty while availability is not known, and `unknown(<n>)` for a status this SDK does not know) and the methods `accepts_deposits()` and `accepts_withdrawals()`, true only for `"normal"`. A suspension can stop one direction only, and requests created before it continue to process; funds sent to a deposit address while deposits are suspended are delayed, not lost. Treat an empty status as not available, not as normal. Affected: `zipper.get_deposit_withdraw_config` and `codecs::decode::deposit_withdraw_config_from_proto`. Migration: a `ZipperAssetChainVariant` struct literal must set the two fields; check `accepts_deposits()` or `accepts_withdrawals()` before showing a network as available.
- Policy reads name the new `READ_STATEMENTS` action (38), on API-key and sub-account policies alike, and it parses back with `PolicyAction::from_proto_name`. Every sub-account policy includes it as part of its read-only floor and every sub-account role has the new `SUBACCOUNT_PERMISSION_READ_STATEMENTS` (29); existing API-key policies do not, so a key cannot export statements until its owner adds the action. The assistant key presets `READ_ONLY` and `TRADE_WITH_APPROVAL` now grant it too; a key set with a preset before keeps the policy it was bound to. Affected: `ApiPolicy::actions`, `SubaccountPolicy::actions`, `policies.subscribe`, `policies.subscribe_subaccount_policies` and `policies.subscribe_api_policies`.
- A stop-loss or take-profit whose trigger price the watched price has already reached when the trigger takes effect, a few milliseconds after it is submitted, is now always refused, on the index price as on the last price: it ends `FAILED` with `TRIGGER_FAILURE_REASON_TRIGGER_WOULD_TRIGGER_IMMEDIATELY` (244), never armed and placed nothing (server change). `triggers.create` returns the trigger ID before that; read the trigger to learn the outcome. Affected: `triggers.create` (documentation only).
- An API-key update returns the same complete key a read does, with its `policy_id`, `public_key_ed25519` and `created_by_actor`, and creation times keep their fractional seconds in reads and lists (server change). `ApiKeySummary` already carries every field at full precision; a test pins it. Affected: `api_keys.get`, `api_keys.list`, `api_keys.subscribe`.
- Changelog posts published at the same time are ordered by release, newest release first, comparing versions as numbers (`v0.92.10` before `v0.92.9`), then the other posts by slug (server change). The SDK keeps the server's order and sorts nothing itself. Affected: `changelog.list_posts`, `posts`, `list_changes` and `changes`.
- **Breaking:** Generated code follows the contract: new enums `chain::zipper::v1::RouteStatus` and `system::v1::StatusEventKind`, `StatusEventState`, `StatusEventImpact` and `AffectedService`; new values `auth::v1::PolicyAction::READ_STATEMENTS` (38) and `SubaccountPermission::SUBACCOUNT_PERMISSION_READ_STATEMENTS` (29); new messages for statements, status events and asset information; and new fields `AssetChainVariant::deposit_status` and `withdrawal_status`. Affected: `proto::chain::zipper::v1`, `proto::system::v1`, `proto::marketdata::v1`, `proto::ledger::read::v1` and `proto::auth::v1`. Migration: a struct literal of `AssetChainVariant` must set the new fields or use `..Default::default()`; a `match` over `PolicyAction` or `SubaccountPermission` needs an arm for the new value.
- QuickSwap and the notification center stay outside this SDK.

## 0.27.0

Package version: `0.27.0`. Git tag: `v0.27.0`. Released 2026-11-14. Built on public contract `74af7f3`.

- New changelog deployment groups (`changelog.v1.ChangelogService/ListDeploymentGroups` and `GetDeploymentGroup`, public, no API key): `changelog.list_deployment_groups(ListChangelogDeploymentGroupsOptions { limit, page_token })` returns one page of this environment's deployment groups, newest deployment first (`limit` defaults to 20, maximum 100), `changelog.deployment_groups(opts)` streams every page, and `changelog.get_deployment_group(slug)` returns one group, such as `testnet-v0.92.9`, with the changes that need attention and the highlights of its release posts. A group (`ChangelogDeploymentGroupSummary`) has its `slug`, `environment` (`devnet`, `testnet` or `mainnet`), `occurred_at`, its own `release_version`, `release_tag` and `previous_release_version` (`Option<String>`, `None` when unknown or for the first group), every release it brought (`versions`, ascending) and their published release posts (`posts`, `ChangelogDeploymentGroupPost` with `slug`, `release_version`, `title`, `published_at`, `revision` and `updated_at`). `ChangelogDeploymentGroup` adds `attention` (`ChangelogDeploymentGroupAttention`: the breaking and action-required changes with their post's `post_slug` and `release_version`) and `highlights` (`ChangelogDeploymentGroupHighlight`). An unknown slug is `Error::Api` with code `NotFound`, and an answer without its group is refused as a contract violation. The list is empty, and every slug unknown, until the environment publishes release posts. Affected: `changelog.list_deployment_groups`, `changelog.deployment_groups`, `changelog.get_deployment_group`, `services::ListChangelogDeploymentGroupsOptions`, the `ChangelogDeploymentGroup*` models and `codecs::decode::changelog_deployment_groups_page_from_proto` / `changelog_deployment_group_from_proto` (all new).
- **Breaking:** Changelog posts carry their release: `ChangelogPostSummary` gains `release_version` (`Option<String>`, the release a post announces, such as `0.92.9`), `environments` (`Vec<ChangelogEnvironmentStatus>`, the release's status on devnet, testnet and mainnet in that order, with `live_at` set once it went live there) and `deployment_group_slug` (`Option<String>`, the group that brought the release to this environment, `None` while it is pending here). They are `None` or empty on posts that announce no release. `environments` changes when the release goes live somewhere without a new `revision`, so compare the whole summary to notice it. `ChangelogChange` gains `environment_exceptions` (`Vec<ChangelogChangeException>`: an `environment`, an `availability` of `ahead`, `pending` or `unavailable`, and `live_at` for `ahead`), the environments where a change differs from its release; `ChangelogChange` stays `Eq`. Labels newer than this SDK read `unknown(<n>)`; new `CHANGELOG_ENVIRONMENT_*` and `CHANGELOG_AVAILABILITY_*` constants name the others. Affected: `ChangelogPostSummary`, `ChangelogChange`, `changelog.list_posts`, `posts`, `get_post`, `list_changes`, `changes`, `breaking_changes_since` and `action_required_since`. Migration: struct literals of `ChangelogPostSummary` or `ChangelogChange` without `..Default::default()` no longer compile; set the new fields (`None` and empty).
- **Breaking:** `ListTransfersRequest::ts_max_us`, the upper time bound of `transfers.list` and `balances.list_transfers`, is `Option<u64>`: `None` sends no upper bound, and `Some(0)` is an explicit bound at the Unix epoch, which the API answers with an empty final page. Both wire formats keep the difference. Affected: `transfers.list`, `balances.list_transfers` and `proto::ledger::read::v1::ListTransfersRequest`. Migration: use `None` where you sent `0` for "unbounded", and `Some(t)` for a bound.
- **Breaking (server behaviour):** `triggers.modify` refuses a new or different nonzero activation price once a trailing stop has started tracking, including while it is paused, with `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE` (315; REST `TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE`, HTTP 400, ConnectRPC `failed_precondition`): an `Error::Api` with that code on both transports, final and not retryable, and the whole patch changed nothing. Clearing the activation price with zero, repeating the current one, and edits before tracking begins are still accepted. Don't infer tracking from the trigger's status alone: a resumed trailing stop can read `armed` while it still waits for its first activation. Affected: `triggers.modify`. Migration: handle the code as a refused edit and keep the existing trigger; to wait for a new activation price, cancel the trigger and create a new one.
- **Breaking (server values):** `batch_request_id`, the ID `orders.batch_replace` returns and `orders.get_batch_replace_status` takes, is an opaque public ID that means nothing outside the account that submitted the batch. Pass it back unchanged, as the SDK does, and compare it only with other `batch_request_id` values of that account. An ID received before the change is answered `not_found`. Affected: `orders.batch_replace`, `orders.get_batch_replace_status`, `orders.wait_batch_replace` and `Order::batch_request_id`. Migration: reconcile batches whose IDs you stored before the change by their orders, as for a batch whose status window ended.
- `Order::batch_request_id` stays set after the order ends, filled or canceled, also for a batch-replaced order a trigger placed, and a later batch that replaces the order does not change it. Orders that ended before the server kept it may read empty. Tests pin it on ended orders in history reads and on the order stream. Affected: `orders.list_history`, `orders.get` and `orders.subscribe`.
- **Breaking:** A managed order book follows the depth stream's rule: an update applies only to a book at exactly its `book_seq_start`, the previous update's `book_seq_end`. An update that starts anywhere else, also one past the book's sequence or overlapping it, and a malformed update, are a gap: the update is not applied, the next view reports `available: false` with the levels the book had, later updates are ignored, and the book is current again with the next `reset` (the full view the publisher sends periodically, currently about every 30 seconds, and after a gap), a manual `refresh_snapshot()` or a reconnect. The book no longer fetches a snapshot on a gap, since one would not line up with the stream either. `orderbook::apply_delta` follows the same rule; its second value means the book is out of sync. Affected: `orderbook.create_subscription`, `orderbook::Subscription`, `OrderbookData::available` and `orderbook::apply_delta`. Migration: read `available` on each view instead of expecting a gap to refetch, and, if you call `apply_delta` yourself, wait for the next reset when it reports a gap instead of fetching a snapshot.
- **Breaking:** A REST answer of `503 AUTH_TEMPORARILY_UNAVAILABLE` is `Error::Api` with that code, retryable after a backoff and certain to have changed nothing, as ConnectRPC's `unavailable` with that auth reason already was. It was `Error::Server`, read as an unknown outcome and not retryable. Trigger reads (`triggers.get_by_id`, `triggers.list`, `triggers.list_events`) retry it by themselves, over REST and ConnectRPC; writes are sent once. Every REST endpoint now answers a credential refusal with the bare documented code (`UNAUTHENTICATED`, `MISSING_HEADERS`, `SIGNATURE_INVALID`, `TIMESTAMP_SKEW`, `RECV_WINDOW_INVALID`, `KEY_EXPIRED`, `KEY_DISABLED`, `IP_NOT_ALLOWED`, all HTTP 401, and `REPLAY_CAPACITY_EXHAUSTED`, HTTP 429), one title and one non-empty detail per code; `auth_code()` reads each as its `AuthErrorCode`, and a test pins every one. A path the API does not serve answers a `404 NOT_FOUND` problem, which reads as `Error::Api` with code `ERROR_CODE_NOT_FOUND`, a refusal. Branch on codes, not on `title`, `detail` or message text. Affected: every call; `errors::map_connect_error`. Migration: match `Error::Api` with `auth_error_code() == Some(auth_codes::TEMPORARILY_UNAVAILABLE)` (or `is_retryable()`) instead of `Error::Server` for this refusal.
- Transfer history: `LedgerTransfer::transfer_type` 1065 (`TRADING_WITHDRAW_REQUEST_FEE`) is not a fee but the debit of a withdrawal from the Trading account that was rejected or failed to execute, for the whole amount sent for it, its request fee included; 1075 (`WITHDRAW_FAILED`) is the same debit for the Funding account. A rejected withdrawal refunds that amount less the request fee to the account's own wallet; one that failed to execute is not refunded. Codes and numbers are unchanged. Affected: `transfers.list`, `transfers.subscribe` and `balances.list_transfers` (documentation only).
- Lifecycle flows: a withdrawal whose execution failed after its destination transaction was committed (`lifecycle_reason` `zipper_withdraw_execution_failed`) reads `zipper_reason.reason_id` `withdrawal.execution_failed.under_review` and a message that it is under review by the bridge operator and its funds were not returned, whatever the bridge code, which is 0 when the bridge gave none. Show the message, or localize the reason id, rather than text keyed by the code, and don't show the withdrawal as refunded. Affected: `lifecycle.get_flow`, `list_flows`, `list_flows_by_tx`, `get_flow_by_tx`, `subscribe_open_flows` and `subscribe_flow_detail` (`ZipperReasonDetails`, documentation and a test).
- **Breaking:** Generated code follows the contract: new enums `changelog::v1::Environment` and `ChangeAvailability` and `auth::v1::AssistantKeyPreset`; new values `orders::v1::ErrorCode::ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE` (315), `notifications::v1::NotificationType::BROKER_ALLOWLIST_CHANGE` and the assistant's `DisplaySetting` values; new messages for deployment groups and broker allowlist notices; new fields such as `AssistantKeySpec::preset` (whose `policy_id` is no longer required: set exactly one of the two), `PostSummary` release fields and `Change::environment_exceptions`. Affected: `proto::changelog::v1`, `proto::orders::v1`, `proto::auth::v1`, `proto::notifications::v1` and `proto::ai::v1`. Migration: add arms to exhaustive matches on these generated enums, and end generated struct literals with `..Default::default()`.
- Assistant key presets (`AssistantKeyPreset`) and broker payout-address notifications (`BROKER_ALLOWLIST_CHANGE`) are devnet only and not wrapped by this SDK: assistant key set-up needs an interactive session, and the notification center is not part of this SDK yet.
- The REST error catalogs the tests check are the contract's at `74af7f3`, which adds `TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE` and moves the API-key credential codes out of the transfer history catalog; a dedicated test keeps them.
- **Breaking:** Every method that takes a caller-built request with a sub-account field now applies `Config::default_sub_account_id` when the request leaves it unset (`None`, or 0 for a plain `u64` field), as the typed helpers and the Go and Python SDKs do; a sub-account set on the request is still sent unchanged, and with no default configured nothing changes. A client configured with a default sub-account read the main account's balances and transfers before, with no error; these reads now address the default sub-account (or are refused for a key scoped to the main account). Affected: `balances.list`, `balances.get_balance_history`, `balances.get_equity_history`, `balances.list_transfers`, `transfers.list`, `orders.cancel`, `triggers.list`, `triggers.get`, `triggers.list_events`, `triggers.cancel`, `triggers.cancel_oco`, `triggers.pause`, `triggers.resume`, `deposit.create_address`, `sub_accounts.list_members`, `sub_accounts.list_activity`, `address_book.list_entries`, `list_transfer_counterparties`, `list_transfer_destinations`, `list_internal_transfer_whitelist_entries`, `get_withdraw_whitelist_view`, `get_view`, `create_entry`, `create_tag`, and `guard_signer.get_status`, `create_wallet`, `sign_protected_action`, `batch_sign_protected_actions`, `rotate_wallet` and `export_wallet`. `address_book.copy_entry` still sends `target_subaccount_id` as built: it names the book the entry is copied to. Migration: to read or act on the main account, use a client without `default_sub_account_id`.
- **Breaking:** Address-book entries carry every field the contract sends: `AddressBookEntry` gains `scope` (`Option<AccountScope>`), `note`, `created_at` and `updated_at` (`Option<Timestamp>`, `None` when unset), and the destination as `external` (`Option<ExternalWithdrawAddress>`: `polychain_chain_id`, `address`) or `internal` (`Option<InternalTransferAccount>`: `root_account_id` and `target_account_id` as public IDs, `target_scope_type`, `smart_account_address`, `root_username`, `sub_account_label`). `AddressBookTag` gains `scope`, `created_at` and `updated_at`. Three types no longer derive `Eq`, because `Timestamp` does not implement it: `AddressBookEntry`, `AddressBookTag` and `AddressBookEntriesList`; each keeps `PartialEq`, so `==` and `assert_eq!` still work. `AddressBooksList` now derives `Default` and `Eq`. Affected: `address_book.list_entries`, `create_entry`, `update_entry`, `copy_entry`, `create_tag`, `update_tag`, `AddressBookEntry`, `AddressBookTag`, `AddressBookEntriesList`, `models::{ExternalWithdrawAddress, InternalTransferAccount}` (new). Migration: struct literals of `AddressBookEntry` and `AddressBookTag` need the new fields (or `..Default::default()`), and code that needs `Eq` on `AddressBookEntry`, `AddressBookTag` or `AddressBookEntriesList` (an `Eq` bound, `#[derive(Eq)]` on a type that contains one) must use `PartialEq` instead or compare the entries' `address_book_entry_id` and `revision`.
- **Breaking:** The address-book view and lists are typed and use the public IDs every other read returns, where they returned protobuf JSON (`ApiData`) with decimal IDs: an entry's ID in `address_book.get_view` is now the base58 ID `list_entries` returns, not its decimal value, and account, sub-account, tag and counterparty IDs are base58 too. `get_view` returns `AddressBookView` (`books`, `entries` with `external` and `internal` entry views, `recent_destinations`, `tags`, `withdraw_whitelist`, `recent_destinations_truncated`, `view_revision`); `get_withdraw_whitelist_view` returns `Option<WithdrawWhitelistView>` (`None` when the server sends no view); `list_transfer_counterparties` returns `TransferCounterpartiesList`; `list_transfer_destinations` returns `AddressBookTransferDestinationsList`; `list_internal_transfer_whitelist_entries` returns `InternalTransferWhitelistEntriesList`; and `AddressBooksList::books` (`list_books`) is `Vec<AddressBookSummary>`. Enum fields read as their contract names (`"INTERNAL_ACCOUNT"`, `"WITHDRAW_TO"`, `"DESTINATION_WHITELIST_ACTIVE"`) and times as `Option<Timestamp>`. Affected: those six methods, `codecs::decode::{address_book_view_from_proto, withdraw_whitelist_view_from_proto, list_counterparties_from_proto, list_destinations_from_proto, list_internal_whitelist_from_proto}` (new) and `list_books_from_proto`. Migration: read the typed fields instead of `ApiData::raw`, and drop any decimal-to-base58 conversion of view IDs.
- **Breaking:** `sub_accounts.get` takes the sub-account's public ID as `&str`, as `sub_accounts.list` returns it (base58, or decimal), where it took a `u64`; one that does not parse, or names 0, is refused with `Error::Validation` before sending. Affected: `SubAccountsService::get`. Migration: pass the ID string, for example `client.sub_accounts.get(&sub.subaccount_id, opts)`, or `&codecs::scalars::format_id(n)` for a numeric ID.
- New `triggers.list_events_by_id(trigger_id, ListTriggerEventsOpts)` lists a trigger's events by the public trigger ID that create and list calls return (base58, or decimal), with optional `subaccount_id` (the configured default when `None`), `limit`, `event_type` label (`fired`, `canceled`, `updated`, `failed`, `activated`, `armed` or `fire_refused`; another is refused with `Error::Validation` before sending) and `page_token`. Affected: `TriggersService::list_events_by_id`, `models::ListTriggerEventsOpts` (both new).

## 0.26.0

Package version: `0.26.0`. Git tag: `v0.26.0`. Released 2026-11-13. Built on public contract `78792a5`.

- New iceberg orders: `CreateOrderParams::display_quantity` and `PreviewOrderParams::display_quantity` (`Option<Quantity>`, base quantity at the pair's base scale, as `quantity`) set the most of a limit GTC or GTD order the order book shows at once, sent as the contract's `LimitGtc.display_qty_scaled` / `LimitGtd.display_qty_scaled`, in single creates, previews and every `orders.batch_create` item. Once the shown part is fully executed the next part is shown at the same price, behind the orders already resting there. Refused with `Error::Validation` before sending: a display on a market, limit IOC or limit FOK order (`display_quantity is valid only for limit GTC or limit GTD orders`) and one that is not positive. The API checks the rest: below `quantity`, a multiple of the quantity step and at least the pair's minimum quantity (`ERROR_CODE_ICEBERG_DISPLAY_INVALID`, `ERROR_CODE_ICEBERG_DISPLAY_TOO_SMALL`), at least `quantity` / 100 rounded up (`ICEBERG_DISPLAY_TOO_SMALL`), and no attached risk (`ERROR_CODE_ICEBERG_NOT_ALLOWED`). The display cannot be modified; a replace keeps it. `None` sends a plain order, as before. Affected: `CreateOrderParams`, `PreviewOrderParams` (both `#[non_exhaustive]`), `orders.create`, `orders.preview` and `orders.batch_create`.
- New static entry peg: `CreateOrderParams::peg` and `PreviewOrderParams::peg` (`Option<OrderPeg>`) take a limit order's price once, at entry, from the best bid or best ask: `OrderPeg::new(PegReference::BestBid | PegReference::BestAsk, offset_ticks)`, sent as the contract's `OrderIntent.peg`. `offset_ticks` is a signed number of the pair's configured price increments (its tick size, not 1e-9 price ticks) added to the reference and sent as given: `-2` is two increments below it; it does not select occupied price levels. `price` stays required, as the BUY's highest or the SELL's lowest acceptable price. Refused with `Error::Validation` before sending, as combinations the contract never allows: a peg on a market order, together with `max_quote_debit_scaled` or `quote_proceeds_scaled`, or together with `attached_risk`. The server refuses a pegged price beyond `price` (`ERROR_CODE_PEG_LIMIT_VIOLATION`), an empty reference side (`ERROR_CODE_PEG_REFERENCE_UNAVAILABLE`) and other unsupported terms (`ERROR_CODE_INVALID_PEG`). The price never follows the book afterwards; read the order for the price it took. Affected: `CreateOrderParams`, `PreviewOrderParams`, `orders.create`, `orders.preview`, `orders.batch_create`, `models::{OrderPeg, PegReference}` (new, both `#[non_exhaustive]`).
- New quote-proceeds sizing of a SELL ("sell 100 USDT worth"): `CreateOrderParams::quote_proceeds_scaled` and `PreviewOrderParams::quote_proceeds_scaled` (`Option<Quantity>`, an `OrderQuote` amount at the pair's quote scale, built like `max_quote_debit_scaled`), sent as the contract's `OrderIntent.quote_proceeds_scaled`. The order sells whole quantity steps until its proceeds before fees reach the target and never receives more; the fee is charged in quote on those proceeds. Set exactly one of `quantity`, `max_quote_debit_scaled` and `quote_proceeds_scaled`; the error now reads `set exactly one of quantity, max_quote_debit_scaled or quote_proceeds_scaled`. Refused with `Error::Validation` before sending, as the contract's `order_intent.quote_proceeds_eligibility` rule: anything but a SELL market order or SELL limit IOC, and a target that is not positive. The server refuses attached risk (`VALIDATION_ERROR`, rule `no_attached_risk`), a target too small for one step (`ERROR_CODE_QUOTE_SIZE_TOO_SMALL`) and a target whose base, at the order's limit or maximum slippage price, is not free (`INSUFFICIENT_FUNDS`). Affected: `CreateOrderParams`, `PreviewOrderParams`, `orders.create`, `orders.preview` and `orders.batch_create`.
- **Breaking:** `OrderSelfTradePrevention` gains `Decrement`, sent as the contract's `DECREMENT`: both orders are reduced by the quantity that would have traded, with no trade; the order left with no quantity is canceled with `CANCEL_REASON_STP_DECREMENT`, a resting order that keeps a quantity keeps its place in the queue, and a fill-or-kill order that would meet such an order is rejected `FOK_INSUFFICIENT_LIQUIDITY`. It is valid with attached risk. `CreateTriggerParams::self_trade_prevention_mode` and `CreateOcoParams::self_trade_prevention_mode` accept `"decrement"` (any case); the refusal of another label now reads `self_trade_prevention_mode must be expire_taker, expire_maker, expire_both, or decrement`. Order and trigger reads label the mode `decrement`; 0.25.0 read it as `UNKNOWN(4)`. The incoming order's mode decides, so a resting order of any mode can be reduced, or canceled with `CANCEL_REASON_STP_DECREMENT`, by another sub-account's `decrement` order. Affected: `OrderSelfTradePrevention`, `orders.create`, `orders.preview`, `orders.batch_create`, `triggers.create`, `triggers.create_oco`, `Order::self_trade_prevention_mode`, `Trigger::self_trade_prevention_mode` and `codecs::decode::enum_value_self_trade_prevention_mode`. Migration: an exhaustive `match` on `OrderSelfTradePrevention` needs a `Decrement` arm.
- **Breaking:** Order reads carry an iceberg order's quantities and a SELL's quote proceeds target: `Order` gains `display_qty` and `hidden_qty` (`Option<Quantity>`, at the pair's base scale as `leaves_qty`; zero for a plain order, and `hidden_qty` zero once the order has ended; the book shows `leaves_qty` minus `hidden_qty`) and `submitted_quote_proceeds_scaled` (`Option<i64>`, at the pair's quote scale, `None` for an order sized otherwise; for such a SELL `cum_notional` and `Order::cum_quote` are the proceeds received before fees). Affected: `Order`, `orders.get`, `orders.get_with`, `orders.list_open`, `orders.list_open_with`, `orders.list_history`, `orders.list_history_with`, `orders.wait_for_order_trades_complete`, `orders.subscribe`, `codecs::decode::order_from_proto`, `get_order_from_proto`, `orders_list_from_open`, `orders_list_from_history` and `orders_unit_from_proto`. Migration: an `Order` struct literal no longer compiles; set the new fields (`None` when unknown), or read orders from the SDK instead of constructing them.
- **Breaking:** Create results echo the quote sizing that was submitted: `OrderMutationResult` gains `submitted_quote_proceeds_scaled` (`Option<i64>`), and `BatchCreateResultItem` gains `submitted_max_quote_debit_scaled` and `submitted_quote_proceeds_scaled` (`Option<i64>`, at the pair's quote scale; `None` for an item sized otherwise or rejected), which batch items did not expose before. Affected: `OrderMutationResult`, `BatchCreateResultItem`, `orders.create`, `orders.batch_create`, `codecs::decode::order_mutation_from_create` and `batch_create_from_proto`. Migration: struct literals of these results no longer compile; set the new fields (`None`).
- Orders read the contract's new cancel reasons by name, never as `UNKNOWN(<n>)`: `CANCEL_REASON_STP_DECREMENT` (19, `decrement` self-trade prevention reduced the order to nothing), `CANCEL_REASON_PRICE_BAND` (20, the rest of a market order, or another order whose price the exchange sets, that the pair's price band stopped) and `CANCEL_REASON_CANCEL_ON_DISCONNECT` (21, cancel-on-disconnect of the trading WebSocket; an order the account's own cancel-all-after timer cancels still reads `CANCEL_REASON_CANCEL_ALL_AFTER`). Fills before the cancel stand. Affected: `Order::cancel_reason` and `OrderStreamEvent::cancel_reason`.
- **Breaking:** Spot pairs publish their price band: `PairOrderLimits` gains `price_band` (`Option<PriceBand>`, new `models::PriceBand` with `lower_bps`, `upper_bps` and `reference_window_ms`, all `u32`; `None` when the pair has no band), from `market_data.get_spot_config()` and from the catalogs. With the band's reference price R and tick size T the limits are `ceil(R * (1 - lower_bps / 10000) / T) * T` and `floor(R * (1 + upper_bps / 10000) / T) * T`; a limit buy above the upper limit or a limit sell below the lower one is refused `ERROR_CODE_PRICE_BAND_VIOLATION`, and a market order trades only to the limit, its rest canceled with `CANCEL_REASON_PRICE_BAND`. R is not published; the last trade price is a close estimate. A `price_band` that is not an object, or a field that is not an unsigned 32-bit integer, fails catalog hydration as other malformed limits do. Affected: `PairOrderLimits`, `market_data.get_spot_config`, `catalogs.pair_order_limits_for_symbol`, `catalogs.pair_order_limits_for_symbol_id` and `codecs::decode::spot_config_from_proto`. Migration: a `PairOrderLimits` struct literal without `..Default::default()` no longer compiles; set `price_band` (`None` for no band).
- **Breaking:** A trigger failure reason reads `quote_size_too_small` instead of `max_quote_debit_too_small` (value 24, renamed by the contract: either quote sizing too small for one step), and a standalone trigger whose order the price band refuses fails with `price_band_violation` (14). Affected: `Trigger::failure_reason`, `TriggerEvent::failure_reason` and `fire_refusal_reason`. Migration: match `quote_size_too_small`.
- **Breaking:** The contract renamed two generated enum values (wire values unchanged): `orders::v1::ErrorCode::ERROR_CODE_MAX_QUOTE_DEBIT_TOO_SMALL` (`MaxQuoteDebitTooSmall`) is now `ERROR_CODE_QUOTE_SIZE_TOO_SMALL` (`QuoteSizeTooSmall`), and `triggers::v1::TriggerFailureReason::TRIGGER_FAILURE_REASON_MAX_QUOTE_DEBIT_TOO_SMALL` (`MaxQuoteDebitTooSmall`) is now `TRIGGER_FAILURE_REASON_QUOTE_SIZE_TOO_SMALL` (`QuoteSizeTooSmall`). `Error::Api` and `Order::reject_reason` read code 68 as `ERROR_CODE_QUOTE_SIZE_TOO_SMALL`, and a REST problem names it `QUOTE_SIZE_TOO_SMALL`. Generated enums gain values: `SelfTradePreventionMode::DECREMENT`, `CancelReason` 19 to 21, `ErrorCode` 78 and 309 to 314, `TriggerFailureReason::TRIGGER_FAILURE_REASON_PRICE_BAND_VIOLATION`, `PolicyAction::READ_PRICE_ALERTS` and `MANAGE_PRICE_ALERTS`, `changelog::v1::Surface::ASSISTANT` and the new `PegReference`; generated messages gain the fields above (`StaticPeg`, `PriceBand`, `display_qty_scaled`, `hidden_qty_scaled`, `quote_proceeds_scaled`, `submitted_quote_proceeds_scaled`). Affected: `proto::orders::v1`, `proto::triggers::v1`, `proto::marketdata::v1`, `proto::auth::v1` and `proto::changelog::v1`. Migration: use the new names, and add arms to exhaustive matches on these generated enums.
- **Breaking (server behaviour):** A cancel, pause, resume or modify of a trigger that has already ended (completed, canceled or failed), a repeat of a cancel that succeeded included, is refused with `ERROR_CODE_TRIGGER_STATUS_INVALID` (HTTP 400) for at least 60 seconds after it ended, then may be refused with `ERROR_CODE_TRIGGER_NOT_FOUND`; a repeated cancel no longer returns the canceled trigger. An ended ladder or TWAP answers the same instead of `ERROR_CODE_TRIGGER_NOT_MODIFIABLE`. New `Error::is_trigger_already_ended()` is true for `ERROR_CODE_TRIGGER_STATUS_INVALID` from either transport, and new `order_codes::TRIGGER_STATUS_INVALID` and `order_codes::TRIGGER_NOT_FOUND` name both codes; the helper leaves out `TRIGGER_NOT_FOUND`, which also means a wrong ID. Affected: `triggers.cancel`, `cancel_by_id`, `pause`, `pause_by_id`, `resume`, `resume_by_id`, `resume_with` and `modify`; `Error` (new accessor). Migration: treat `is_trigger_already_ended()` as "already done", and `TRIGGER_NOT_FOUND` the same only for a trigger you know you created.
- A ladder level the order book rejects when it arrives (for example `post_only_cross`) is reported as a `fire_refused` event with that level's `child_seq`, `fire_refusal_reason` and price as `fire_price`, after the level's `fired` event, while the ladder keeps working its other levels; a level whose `fired` is followed by a `fire_refused` of the same `child_seq` was not placed. A ladder fails only when every level was rejected. The events decoded already; the docs of `TriggerEvent::child_seq` and `child_order_id` say so and a test pins it. Affected: `triggers.list_events` and `triggers.subscribe_events` (docs only).
- **Breaking (server values):** Trigger IDs are opaque per-account public IDs, like order IDs: every trigger got a new ID when the change reached an environment, and IDs and `list` or `list_events` page tokens issued before it are refused (`TRIGGER_NOT_FOUND`, `not_found`, or `INVALID_TRIGGER_ID` on an order listing filtered by `trigger_id`). The SDK derives nothing from a trigger ID's value. Affected: every call that takes or returns a trigger ID. Migration: drop stored trigger IDs and page tokens and read the triggers again with `triggers.list`.
- **Breaking (server behaviour):** An order refused because spot trading is not allowed says which policy lacks the permission: new `ERROR_CODE_API_KEY_SPOT_TRADE_DENY` (78) when the API key's policy does not grant `TRADE_SPOT` (also when neither policy does), `ERROR_CODE_POLICY_SPOT_TRADE_DENY` (23) now only for the sub-account's policy, `ERROR_CODE_API_KEY_POLICY_REQUIRED` (27) for an API key with no policy, and `ERROR_CODE_API_KEY_MARKET_DENY` (29) instead of `ERROR_CODE_POLICY_MARKET_DENY` (20) for a main-account key whose own market list excludes the pair. All are final permission refusals, read as below. Affected: `orders.create`, `orders.preview`, `orders.batch_create`, cancels and trigger controls. Migration: wherever you handle `POLICY_SPOT_TRADE_DENY`, also handle `API_KEY_SPOT_TRADE_DENY` (enable Spot Trading Write on the API key's policy) and `API_KEY_POLICY_REQUIRED` (attach a policy to the API key); handle `API_KEY_MARKET_DENY` where you handled `POLICY_MARKET_DENY` for a main-account key.
- **Breaking:** An order refusal answered with ConnectRPC `permission_denied` and an `orders.v1.ErrorDetail` is `Error::PermissionDenied { code, .. }` with the order code, as the same refusal answered as a REST problem already was: same variant, same `code` spelling without the `ERROR_CODE_` prefix (`API_KEY_SPOT_TRADE_DENY`, `POLICY_SPOT_TRADE_DENY`, `API_KEY_POLICY_REQUIRED`, `API_KEY_MARKET_DENY`, `POLICY_MARKET_DENY`, `SUBACCOUNT_READ_FORBIDDEN`, `QUICKSWAP_NOT_PERMITTED`, ...), same message, HTTP status 403, and `endpoint` the called procedure. Before, an auth-like order code (`POLICY_*`, `API_KEY_*`, `PERMISSION_DENIED`) was `Error::Auth` with the code dropped, and the others `Error::Api` with an `ERROR_CODE_` code. It stays not retryable, and `Error::Auth` stays for authentication failures. Affected: every order and trigger call; `errors::map_connect_error`. Migration: match `Error::PermissionDenied { code, .. }` instead of `Error::Auth` (or `Error::Api`) for order permission refusals.
- **Breaking (server behaviour):** A batch replace whose items include one refused for its value (a price off the pair's tick size, a REST decimal finer than the price scale, or an attached-risk price that is off the tick or otherwise invalid: `PRICE_TICK_SIZE`, `TRIGGER_PRICE_INVALID`, `TRIGGER_PRICE_SOURCE_UNSUPPORTED`, `TRAILING_DISTANCE_INVALID`, `MAX_SLIPPAGE_INVALID`) no longer fails as a whole. The call succeeds, and only that item is rejected, at its `item_index`, with its `code` and an indexed field violation such as `items[1].new_attached_risk.take_profit.trigger_price_ticks` in `violations`; the other items are processed as usual, and `orders.get_batch_replace_status` reports the item rejected with the same code. A malformed request (an item that names no order or both IDs, an empty patch, an attached-risk policy of the wrong shape) is still refused as a whole. A test pins a rejected item's code and indexed violation next to an admitted one. Affected: `orders.batch_replace` (`BatchReplaceOrdersResult::results`, `BatchReplaceAdmissionItem::code`, `violations`). Migration: read the per-item results instead of treating an error as "nothing was applied", and resend only the rejected items, corrected, under a fresh `request_id`.
- The SDK recognises the contract's new error codes by name over both transports: `API_KEY_SPOT_TRADE_DENY` (78, 403), `ICEBERG_NOT_ALLOWED` (309), `ICEBERG_DISPLAY_TOO_SMALL` (310), `ICEBERG_DISPLAY_INVALID` (311), `PEG_LIMIT_VIOLATION` (312), `PEG_REFERENCE_UNAVAILABLE` (313), `INVALID_PEG` (314) and the renamed `QUOTE_SIZE_TOO_SMALL` (68); none is retryable. The tests' REST error catalogs follow the contract's error list. Affected: `Error::Api` codes; tests.
- Policy reads name the price-alert permissions `READ_PRICE_ALERTS` (36) and `MANAGE_PRICE_ALERTS` (37). **Breaking (server behaviour):** price alerts are devnet only, and an API key needs `READ_PRICE_ALERTS` on its policy to list alerts and `MANAGE_PRICE_ALERTS` to create, update or cancel them (neither implies the other); without it the call is refused with `permission_denied`, an `Error::Api` whose `auth_error_code()` is `AUTH_API_KEY_ACCESS_DENIED`, before any alert is read or changed. Session tokens are unchanged. Affected: `alerts.create`, `alerts.list`, `alerts.all`, `alerts.update`, `alerts.cancel`; `ApiPolicy::actions` and `SubaccountPolicy::actions`. Migration: grant the two actions on the API-key policies of keys that call the price-alert service.
- The changelog names the AI assistant: `ChangelogChange::surfaces` and `ChangelogSection::surface` read `assistant` (new `CHANGELOG_SURFACE_ASSISTANT`), and `changelog.list_changes` filters by it and takes up to 11 `surfaces` (10 before). Affected: `changelog.list_changes`, `changelog.get_post` and `ListChangelogChangesOptions::surfaces`.
- The docs of `LedgerTransfer::tx_id`, the transfer's lifecycle flow ID, say when it is set: the `withdraw_failed` (1075) debit of a rejected or failed withdrawal carries its withdrawal's flow ID in `balances.list_transfers` (joined at read time, so an early read can return it empty), the realtime transfer stream sends that debit once without it, and new Trading to Funding transfers carry it from their first realtime update. Affected: `LedgerTransfer` (docs only).
- New `triggers.create_oco(CreateOcoParams)` creates an OCO (one-cancels-the-other, `triggers.v1.TriggersService/CreateOco`): a take-profit and a stop-loss that both trade `side` for `qty` of a quantity the account already holds, reserved once and shared by both legs. Each leg is an `OcoLegParams` built with `OcoLegParams::market(trigger_price)` (a market order, IOC) or `OcoLegParams::limit(trigger_price, limit_price)` (a resting limit order, GTC), with an optional `trigger_price_source` (`last`, `index` or `mark`, the labels trigger reads use). It returns `CreateOcoResult`: the OCO's public `order_list_id`, the echoed `client_list_id`, the legs' `take_profit_trigger_id` and `stop_loss_trigger_id`, and `accepted_ts_ns`, when admission completed in nanoseconds since the Unix epoch (the contract's `accepted_at_ts_ns`, or `accepted_at` when only that is sent; 0 when neither is). `client_list_id` is required, any 1 to 36 bytes of UTF-8 as the contract allows, non-ASCII and spaces included, and is sent exactly as given, never trimmed: reuse it to retry, since the identical params with the same ID return the same OCO; a create that fails with `TRIGGER_CREATE_OUTCOME_UNKNOWN` may have created it, so list the triggers with `ListTriggersOpts::client_list_id` before retrying. Refused with `Error::Validation` before sending: a missing or malformed `client_list_id`, a non-positive trigger price, a limit leg without `limit_price` or a market leg with one, another price source label, and a self-trade prevention mode other than `expire_taker`, `expire_maker`, `expire_both` or `decrement`. The server refuses legs the wrong way round for the side (the take-profit must be above the stop-loss for a SELL OCO and below it for a BUY OCO), a price source the market does not support, and a self-trade prevention mode the OCO does not take (the contract documents `decrement` as unsupported on an OCO; the SDK sends it and the server decides). Affected: `TriggersService::create_oco`, `models::{CreateOcoParams, OcoLegParams, CreateOcoResult}` and `codecs::decode::create_oco_from_proto` (all new).
- New `triggers.cancel_oco_by_id(order_list_id, subaccount_id)` cancels both legs of an OCO (`triggers.v1.TriggersService/CancelOco`) and returns `CancelOcoResult` with the OCO's `order_list_id`, both legs' `status`, `"canceled"`, and the answer's `ts_ns` (the contract's `ts_ns`, or `ts` when only that is sent; 0 when neither is); `triggers.cancel_oco(CancelOcoRequest)` sends a request as built, as `triggers.cancel` does. An ID that is not an OCO of the account, such as a leg's trigger ID, is refused with `TRIGGER_NOT_FOUND` and nothing is canceled; an ID that does not parse, or names 0, is refused with `Error::Validation` before sending. Affected: `TriggersService::cancel_oco`, `TriggersService::cancel_oco_by_id`, `models::CancelOcoResult` and `codecs::decode::cancel_oco_from_proto` (all new).
- New `triggers.batch_cancel(trigger_ids, subaccount_id, request_id)` cancels up to 50 distinct triggers by their public IDs in one request (`triggers.v1.TriggersService/BatchCancelTriggers`), each as `triggers.cancel` cancels it, and returns `BatchCancelTriggersResult`: one `BatchCancelTriggersResultItem` per trigger in request order, with `status` (`"accepted"` or `"rejected"`), `trigger_id`, `code`, `rate_limit` and `violations`, read as `orders.batch_cancel` reads its items, the accepted and rejected counts, and `ts_ns`, the server time of the answer in nanoseconds since the Unix epoch (`ts_ns`, or `ts` when only that is sent; 0 when neither is). As for `orders.batch_cancel`, a `request_id` is generated when omitted; pass a stable one to retry, since within 2 minutes the same `request_id` with the same trigger IDs returns the original results and cancels nothing again. No trigger ID, more than 50, the same trigger twice, or an ID that does not parse or names 0 is refused with `Error::Validation` before sending. Affected: `TriggersService::batch_cancel`, `models::{BatchCancelTriggersResult, BatchCancelTriggersResultItem}` and `codecs::decode::batch_cancel_triggers_from_proto` (all new).
- **Breaking:** Trigger reads name the OCO a take-profit or stop-loss belongs to: `Trigger` gains `order_list_id` (`Option<String>`), the OCO's public ID, as `CreateOcoResult::order_list_id` spells it, and `client_list_id` (`String`), as given to `triggers.create_oco`. Both triggers of an OCO carry them, and neither has a parent order; any other trigger reads `None` and empty, also when the server sends an `order_list_id` of 0. Affected: `Trigger`, `triggers.get`, `get_by_id`, `list`, `list_with` and `subscribe`, and `codecs::decode::trigger_from_proto`, `trigger_from_bytes`, `triggers_list_from_proto` and `get_trigger_from_proto`. Migration: a `Trigger` struct literal no longer compiles; set `order_list_id` (`None`) and `client_list_id` (empty), or read triggers from the SDK instead of constructing them.
- **Breaking:** `ListTriggersOpts` gains `order_list_id` (`Option<String>`, the OCO's public ID in base58 or decimal) and `client_list_id` (`Option<String>`, any 1 to 36 bytes of UTF-8, sent exactly as given and never trimmed, so the key a create used finds its OCO; another length is refused with `Error::Validation` before sending), sent as the `order_list_id` and `client_list_id` filters of `ListTriggers`, so `list_with` can list only the two legs of one OCO. An `order_list_id` that does not parse, or names 0, is refused with `Error::Validation` before sending. Leaving both `None` sends the request as before. Affected: `triggers.list_with` and `ListTriggersOpts`. Migration: a `ListTriggersOpts` struct literal must set the new fields or end with `..Default::default()`.
- `sdk-coverage.toml` lists the contract's new notification center (`notifications.v1.NotificationService`) as not wrapped: it is not part of the trading SDK surface yet. Its generated messages and client are in `proto::notifications::v1` and `connect::notifications::v1`. Affected: `sdk-coverage.toml`.

## 0.25.0

Package version: `0.25.0`. Git tag: `v0.25.0`. Released 2026-11-12. Built on public contract `e339360`.

- **Breaking:** `sub_accounts.get` with `GetSubaccountOpts::include_members`, `include_invites` or `include_policy` returns the sub-account's members, invites and attached policy: `GetSubaccountResult` gains `members` (`Vec<SubAccountMember>`, read as `sub_accounts.list_members` reads them), `invites` (`Vec<SubAccountInvite>`, read as `sub_accounts.list_invites` reads them, filtered by `invites_direction`) and `policy` (`Option<SubaccountPolicy>`, read as `policies.subscribe_subaccount_policies` reads it). The server sends them, as the contract's `GetSubaccountResponse.members`, `invites` and `policy`, and the SDK dropped them, so the three options had no effect. `members` and `invites` are empty and `policy` is `None` when not asked for or not sent; `policy` is also `None` when no policy is attached. Affected: `GetSubaccountResult`, `sub_accounts.get` and `codecs::decode::get_subaccount_from_proto`. Migration: a `GetSubaccountResult` struct literal without `..Default::default()` no longer compiles; set `members`, `invites` and `policy` (empty and `None` when none), or read the view from the SDK instead of constructing it. Code that compares whole results now also compares `members`, `invites` and `policy`.
- **Breaking:** A create, update or cancel of a price alert whose answer carries an alert with `alert_id` 0 returns `Error::ResponseContract` (`CreateAlert: response alert has no alert_id`, likewise `UpdateAlert` and `CancelAlert`), as an answer with no alert already did. It was returned as a made-up alert with `alert_id` `"0"`, `symbol_id` 0, `revision` 0 and status `"unspecified"`, and an update also returned the answer's `outcome`, so a broken answer read as success. The alert is checked before the outcome is read, for every outcome. `mutation_outcome_unknown()` is true for the error: the change may have applied. Tests now also pin that a bound price is never sent for a market the catalogs can't name, whether they were never loaded or failed to load. Affected: `alerts.create`, `alerts.update`, `alerts.cancel`, `codecs::decode::required_price_alert` and `codecs::decode::update_alert_result_from_proto`. Migration: handle `Error::ResponseContract` from these calls and read the alerts back with `alerts.list` before retrying; code that checked for `alert_id == "0"` can drop that check.
- **Breaking:** `alerts.update` refuses a `Price` bound to a market with `Error::Validation` (`update alert cannot check a price bound to ETH-USDT against the alert's market; pass an unbound price: Price::from_decimal_str(.., None) or Price::from_ticks(.., None)`) before sending. The update names the alert, not its market, so the SDK cannot prove the binding without another read, and a price bound to ETH-USDT was sent for an alert on BTC-USDT. This includes an alert's own `price` and `reference_price` passed back, which read back bound to the alert's market. An unbound price is sent as before. Affected: `alerts.update` and `UpdateAlertParams::with_price`. Migration: pass `Price::from_ticks(alert.price.as_ticks(), None)?` or `Price::from_decimal_str("91000", None)?` instead of a bound price.

## 0.24.0

Package version: `0.24.0`. Git tag: `v0.24.0`. Released 2026-11-12. Built on public contract `e339360`.

- **Breaking:** API keys read back how they are shown and who created them: `ApiKeySummary` gains `icon`, the user-chosen icon or emoji, `color`, the user-chosen color token, both as sent (at most 32 characters each), and `created_by_actor`, the read-only label of who created the key, such as a username or account label. Each is empty when the server sends none. They were dropped although the server sends them. A main-account key's `subaccount_id` stays empty whether the server leaves `subaccount_id` unset or sends an explicit 0; a test now pins it. Affected: `ApiKeySummary`, `api_keys.list`, `api_keys.get`, `api_keys.subscribe`, `sub_accounts.subscribe_api_keys`, `sub_accounts.get` (its new `api_keys`), `codecs::decode::api_key_from_proto`, `api_keys_list_from_proto`, `api_key_from_get_proto`, `api_key_from_create_proto`, `api_key_from_update_proto`, `api_key_from_bytes` and `get_subaccount_from_proto`. Migration: an `ApiKeySummary` struct literal no longer compiles; set the new fields (empty strings when unset), or read keys from the SDK instead of constructing them. Code that compares whole keys now also compares `icon`, `color` and `created_by_actor`.
- **Breaking:** `sub_accounts.get` with `GetSubaccountOpts::include_api_keys` returns the sub-account's API keys: `GetSubaccountResult` gains `api_keys`, read as `api_keys.list` reads them, with every `ApiKeySummary` field. The server sends them, as the contract's `GetSubaccountResponse.api_keys`, only to an owner or admin of the sub-account, and the SDK dropped them, so `include_api_keys` had no effect. `api_keys` is empty when they were not asked for or none were sent. Affected: `GetSubaccountResult`, `sub_accounts.get` and `codecs::decode::get_subaccount_from_proto`. Migration: a `GetSubaccountResult` struct literal without `..Default::default()` no longer compiles; set `api_keys` (empty when none), or read the view from the SDK instead of constructing it. Code that compares whole results now also compares `api_keys`.

## 0.23.0

Package version: `0.23.0`. Git tag: `v0.23.0`. Released 2026-11-12. Built on public contract `e339360`.

- **Breaking:** API keys read back with their restrictions and scope: `ApiKeySummary` gains `policy_id`, the public ID of the attached API policy in the form of `ApiPolicy::policy_id` (empty when none is attached, and then the key has no permissions); `subaccount_id`, the public ID of the sub-account the key is scoped to (empty for a key of the main account); `ip_whitelist`, the CIDR allowlist as sent (empty for no IP restriction); and `expires_at`, the exact UTC `Timestamp` after which the key cannot authenticate whatever its status (`None` when it does not expire). They were dropped although the server sends them. `public_key_ed25519` stays hex. Affected: `ApiKeySummary`, `api_keys.list`, `api_keys.get`, `api_keys.subscribe`, `sub_accounts.subscribe_api_keys`, `codecs::decode::api_key_from_proto`, `api_keys_list_from_proto`, `api_key_from_get_proto`, `api_key_from_create_proto`, `api_key_from_update_proto` and `api_key_from_bytes`. Migration: an `ApiKeySummary` struct literal no longer compiles; set the new fields (empty strings, an empty `ip_whitelist`, `expires_at: None`), or read keys from the SDK instead of constructing them.
- **Breaking:** An order admitted by a batch replace keeps that batch's ID: `Order` gains `batch_request_id`, the public ID `orders.batch_replace` returned as `BatchReplaceOrdersResult::batch_request_id`, exact for every 64-bit ID, and empty for an order of any other origin (the contract's 0), never `"0"`. It was dropped although the server sends it. Affected: `Order`, `orders.get`, `orders.get_with`, `orders.list_open`, `orders.list_open_with`, `orders.list_history`, `orders.list_history_with`, `orders.wait_for_order_trades_complete`, `orders.subscribe`, `codecs::decode::order_from_proto`, `get_order_from_proto`, `orders_list_from_open`, `orders_list_from_history` and `orders_unit_from_proto`. Migration: an `Order` struct literal no longer compiles; set `batch_request_id` (empty for no batch), or read orders from the SDK instead of constructing them.

## 0.22.0

Package version: `0.22.0`. Git tag: `v0.22.0`. Released 2026-11-12. Built on public contract `e339360`.

- **Breaking:** API keys need account permissions on their API-key policy for calls any active key could make before (server change, devnet now and testnet with its next release): `CREATE_DEPOSIT_ADDRESS` for `deposit.create_address`, `READ_DEPOSIT_ADDRESSES` for `deposit.list_addresses`, `READ_SUBACCOUNTS` for `sub_accounts.list` and `sub_accounts.get`, `READ_SUBACCOUNT_ACTIVITY` for `sub_accounts.list_activity`, `READ_SUBACCOUNT_MEMBERS` for `sub_accounts.list_members` and the members of `sub_accounts.get`, `READ_SUBACCOUNT_INVITES` for `sub_accounts.list_invites` and the invites of `sub_accounts.get`, and `READ_GUARD_SIGNER_STATUS` for `guard_signer.get_status`. A key scoped to the main account also needs `READ_ADDRESS_BOOK` (or `MANAGE_ADDRESS_BOOK`) to read the main account's address book, as a sub-account's book already did. A key without the permission is refused with `Error::PermissionDenied` (`AUTH_API_KEY_ACCESS_DENIED`; `AUTH_SUBACCOUNT_ACCESS_DENIED` for the address book); `sub_accounts.get` leaves out members and invites a key may not read instead of failing. `guard_signer.get_status` no longer answers a key for a sub-account another main account owns, even one shared with it. Existing policies were granted these permissions when the server change was deployed; keys without a policy were not. Policy streams report the seven new actions by name. Affected: `deposit.create_address`, `deposit.list_addresses`, `sub_accounts.list`, `sub_accounts.get`, `sub_accounts.list_activity`, `sub_accounts.list_members`, `sub_accounts.list_invites`, `guard_signer.get_status`, the `address_book` reads, and the policy subscriptions. Migration: attach an API-key policy that grants these permissions to keys that make these calls; when you rewrite a policy's whole action list, keep the permissions you read back; read Guard Signer status of a shared sub-account with a key of the main account that owns it.
- New `client.get_version()` returns the exact build the API host serves (`system.v1.SystemService/GetVersion`) as `ServerVersion`: `version`, `commit` and `environment`, all strings, and `release()`, the release a build follows (`"0.10.0"` for `"0.10.0-12-g4e21a2f09670"`). It needs no API key and is never signed. Affected: `Client::get_version`, `models::ServerVersion` (new).
- New `withdraw.get_wallet_balances(sub_account_id)` reads the zTokens the account's smart account wallet holds on Polyester Chain (`chain.withdraw.v1.WithdrawService/GetWalletBalances`), where a withdrawal Zipper rejected is now refunded net of the fee Zipper keeps: `WalletBalances` with the wallet address and each `WalletBalance`'s zipped and unified asset IDs, chain, zToken address, `amount_e18` and `amount` in whole units. An API key needs `READ_BALANCES`. Affected: `WithdrawService::get_wallet_balances`, `models::WalletBalances`, `models::WalletBalance` (new).
- New `client.alerts` manages price alerts (`alerts.v1.PriceAlertService`): `create`, `list`, `all` (a stream over every page), `update` and `cancel`. Price alerts are available on devnet; they are not yet routed on testnet, where these calls fail. They belong to the main account: an API key scoped to a sub-account is refused with `Error::PermissionDenied` and code `SUBACCOUNT_KEY_REFUSED`. An alert watches a market's last traded or index price, fires once when the price crosses its level (`Above` and `Below` are inclusive), and is delivered in-app and/or by Telegram. Prices are the exact `types::Price`, as for orders. `create` and `update` take an optional idempotency key that makes a retry safe; `update` sends only the fields set, with the alert's last-read revision, and an update that sets none sets a triggered alert again; its `outcome` (`"applied"`, `"unchanged"`, `"replayed"`) tells a retry from a fresh change. Statuses keep the contract's spelling, `"cancelled"`. Refusals keep their contract codes (`errors::alert_codes`); `PRICE_UNAVAILABLE` changed nothing and is retryable, and `SERVICE_UNAVAILABLE` leaves a mutation's outcome unknown, as other services' unavailable answers do. The input types are `#[non_exhaustive]` with constructors. Affected: `Client::alerts`, `services::AlertsService`, `models::{CreateAlertParams, UpdateAlertParams, ListAlertsOptions, PriceAlert, PriceAlertsPage, UpdateAlertResult, AlertDirection, AlertPriceSource, AlertChannel, AlertStatus}`, `errors::alert_codes` (all new); the capability table gains "Price alerts (devnet)".
- **Breaking:** `AUTH_REPLAY_DETECTED` (new `auth_codes::REPLAY_DETECTED`, REST `REPLAY_DETECTED`) is a final `Error::ApiKeyRejected` that `is_retryable()` reports false. Over ConnectRPC it was a plain `Error::Api`. The server refuses with it a signed trading WebSocket login whose nonce was already used; send a new login with a fresh nonce. This SDK sends no signed WebSocket login, so its calls do not receive it today; a replayed signed HTTP request is still refused as `AUTH_SIGNATURE_INVALID`. The assistant-key refusals (`AUTH_ASSISTANT_KEY_*`) are recognised by name. Affected: `Error::ApiKeyRejected` matches. Migration: match it as `Error::ApiKeyRejected`, not `Error::Api`.
- Withdraw refusals expose `Error::withdraw_field_path()` (the request field they name), `withdraw_min_floor_e18()` (the destination's payout floor on `ERROR_CODE_MIN_AMOUNT_BELOW_FLOOR`) and `withdraw_payable_from()`. A broker-vault withdrawal to a destination added to the vault's whitelist less than 24 hours ago is refused with `ERROR_CODE_EXTERNAL_DESTINATION_COOLING_DOWN`, which the new `Error::is_withdraw_destination_cooling_down()` reports; it is not a whitelist error and not retryable before `withdraw_payable_from()`. Affected: the withdraw service's methods; `Error` (new accessors). Only an auto-withdraw rule change, which needs an interactive session and this SDK does not wrap, is refused with `ERROR_CODE_MIN_AMOUNT_BELOW_FLOOR` today.
- `social_verification.get` accepts `"telegram"`: a Telegram account waiting for the user's confirmation reads `STATUS_AWAITING_CONFIRMATION` with the handle of the Telegram account that sent the code; an API key reads the link status only, so the display name and provider user ID stay empty. Starting, marking ready, confirming and unlinking need an interactive session. Affected: `social_verification.get`, `start` and `mark_ready`.
- API keys report `purpose`: `"STANDARD"` for your own keys, `"ASSISTANT"` for keys created for the Polyester assistant. Spot configs list their assets in the new `SpotConfig::assets` (`SpotAsset`), whose `included_in_equity` says whether the asset counts in total equity; `None` means unknown, never `false`. Affected: `api_keys.list`, `api_keys.get` and the API-key subscriptions (`ApiKeySummary::purpose`, new); `market_data.get_spot_config` and the catalogs (`SpotConfig::assets`, `models::SpotAsset`, new).
- A lifecycle flow whose deposit Zipper transferred out for compliance names its reason `zipper_compliance_transferred`; it read `unknown_reason_105`. A withdrawal Zipper rejects now ends `refunded` with the same `zipper_validation_rejected` or `zipper_execution_rejected` reason instead of `failed`: accept both. Rate-limit details name the assistant's policy classes (`ASSISTANT_CONTROL`, `ASSISTANT_MESSAGE`, `ASSISTANT_READ`), and transfer history labels code 1075 `withdraw_failed` (the debit of a withdrawal Zipper rejected or that failed to execute). Affected: the lifecycle reads and streams (`lifecycle_reason`); `RateLimitDetail::policy_class`; `transfers.list` and the transfer stream.
- The docs of `orders.cancel_all` and `orders.cancel_all_after` say what they leave running: standalone triggers and TWAPs keep running, and an attached leg whose exit order is canceled ends, with its OCO siblings. Affected: `orders.cancel_all`, `orders.cancel_all_after`.
- **Breaking:** `ApiKeySummary` and `SpotConfig` gain fields (`purpose`, `assets`), so struct literals of them no longer compile. Affected: those types. Migration: set the new fields in struct literals (an empty `purpose`, the spot config's `assets`), or read these types from the SDK instead of constructing them.
- `sdk-coverage.toml` drops `AutoWithdrawService/DisableAutoWithdrawRule`, which the public contract removed (this SDK never wrapped it), and records why the contract's other new methods are not wrapped: the assistant, assistant keys, Telegram link confirmation and unlinking, and pausing or resuming an auto-withdraw rule accept only interactive sessions; the auto-withdraw run preview is left with the other unwrapped auto-withdraw reads; Quick Swap referrals stay outside this SDK. The capability table gains "Server build version" and "Price alerts (devnet)". Affected: `sdk-coverage.toml`, `sdk-capabilities.json`, the README capability table.
- **Breaking:** The generated `chain.withdraw.v1` `AutoWithdrawService` client loses `DisableAutoWithdrawRule` and its request and response messages, which the public contract removed, and gains `SetAutoWithdrawRuleEnabled` and `PreviewAutoWithdrawRun`. Affected: the generated auto-withdraw client and messages. Migration: pause or resume a rule with `SetAutoWithdrawRuleEnabled` from an interactive session; API keys could never call either.

## 0.21.0

Package version: `0.21.0`. Git tag: `v0.21.0`. Released 2026-11-12. Built on public contract `a3e3784`.

- **Breaking:** Trigger reads report a market TWAP's per-slice slippage cap in `Trigger::max_slippage`, in the variant it was set in: `Some(MaxSlippage::Bps(50))` for `max_slippage_bps: 50`, `Some(MaxSlippage::Ticks(25_000_000_000))` for ticks, and `None` when no cap was set, in which case each slice uses the pair's default market slippage. The decoder dropped the TWAP's `market_ioc.max_slippage`, so every market TWAP read `None` and a saved cap could not be told from no cap. Stop-loss and take-profit market children and trailing stops read as before. Affected: `triggers.get`, `get_by_id`, `list`, `list_with` and `subscribe`, and `codecs::decode::trigger_from_proto`, `trigger_from_bytes`, `triggers_list_from_proto` and `get_trigger_from_proto`. Migration: code that took a `Some` `max_slippage` to mean a stop-loss, take-profit or trailing stop must check `trigger_type`.
- A trailing stop's cap reads in `Trigger::max_slippage` from its configuration, as before, and now also from its runtime `TriggerDetails::Trailing` when a trigger carries no configuration; the details keep their own copy. Affected: the trigger reads and decoders named above.
- `triggers.subscribe` names each trigger's pair in `Trigger::symbol` from the loaded catalogs, as `triggers.get` and `triggers.list` do; it was empty. Without catalogs, or for a pair they do not list, `symbol` stays empty. Affected: `triggers.subscribe`.
- **Breaking:** `SubAccount` gains `role` and `removed`. `role` is the caller's role on the sub-account as its proto name (`Some("VIEWER")`, `Some("TRADER")`, ...), as member and invite roles read, and `None` when the server sends none, as on a removal: no role is assumed. `removed` is true when the caller lost access to the sub-account, for example after its owner removed the caller as a member; drop it from the caller's list. Both were dropped, so a removal decoded like an active update with the same revision and `updated_at`. A role or access change keeps `revision` and `updated_at`, so apply each update as it arrives. The managed resubscribe of a sub-account's channels reads `removed` as before. Affected: `sub_accounts.subscribe`, `sub_accounts.list` and `sub_accounts.get` (which list only accessible sub-accounts, so `removed` is false there), and `codecs::decode::subaccount_from_proto`, `subaccount_from_bytes`, `subaccounts_list_from_proto` and `get_subaccount_from_proto`. Migration: a `SubAccount` struct literal must set `role` and `removed`; remove a sub-account from your list on an update with `removed: true`.
- **Breaking:** `triggers.create` refuses a ladder with `order_type: CreateOrderType::Market` with `Error::Validation` (`order_type=market does not apply to a ladder: a ladder places limit GTC orders only`), before anything is sent. The contract's ladder is a LINEAR limit GTC ladder; the SDK accepted a market order type and created resting limit orders. A ladder with `CreateOrderType::Limit` is sent as before. Affected: `triggers.create`. Migration: create a ladder with `CreateOrderType::Limit`.
- **Breaking:** `triggers.modify` takes `max_slippage_bps` from 0 (clear) to 10000, as the API does. It refused 10000 with `Error::Validation` (`max_slippage_bps must be between 1 and 9999`), although a buy stop-loss or take-profit market child takes a 10000 bps cap at create and at modify. The server refuses 10000 for a sell stop-loss or take-profit and for a trailing stop with `ERROR_CODE_MAX_SLIPPAGE_INVALID`, and leaves the trigger unchanged. Above 10000 still fails locally, now with `max_slippage_bps must be between 1 and 10000`. `triggers.create` is unchanged. Affected: `triggers.modify`. Migration: expect the server's `ERROR_CODE_MAX_SLIPPAGE_INVALID` instead of a local `Error::Validation` when a modify sends 10000 to a sell or trailing trigger.
- **Breaking:** `ListTriggersOpts` gains `trigger_type` (`Option<CreateTriggerType>`) and `parent_order_id` (`Option<String>`, the order's ID in base58 or decimal, as order reads and `Trigger::parent_order_id` spell it), sent as the `trigger_type` and `parent_order_id` filters of `ListTriggers`, so `list_with` can list only ladders, or only the triggers attached to one order. A parent order ID that does not parse, or names order 0, is refused with `Error::Validation` before sending. Leaving both `None` sends the request as before. Affected: `triggers.list_with` and `ListTriggersOpts`. Migration: a `ListTriggersOpts` struct literal must set the new fields or end with `..Default::default()`.

## 0.20.1

Package version: `0.20.1`. Git tag: `v0.20.1`. Released 2026-11-12. Built on public contract `a3e3784`.

- Requests signed with an API key could be refused by servers running the new clock rule, which refuses a timestamp more than one second ahead of the server clock as `TIMESTAMP_SKEW` (devnet runs it now; testnet gets it with its next release). The SDK gives each request of a key its own millisecond timestamp, and during a burst it let those timestamps run up to five seconds ahead of the local clock, so a burst of more than about a thousand signed requests, or a few hundred on a host whose clock is a little fast, got `Error::TimestampSkew`. Automatic timestamps now lead the local clock by at most 500 ms (`auth::MAX_SIGNING_FUTURE_SKEW_MS` is `500`, was `5_000`). A larger burst waits for the clock in `sign_request_async` and the client, as it did at the old ceiling; the low-level `Credentials::sign_request` returns its retryable `Error::RateLimit` once a burst runs 500 ms ahead of the clock, roughly 500 more signatures than milliseconds elapsed, instead of 5000 ms ahead. Keep the host clock synchronized: the other half second is all the room the server leaves for skew. The docs also give the server's new default receive window, 5000 ms (was 10000), used when `recv_window_ms` is `None`. Affected: every method that signs with an API key, such as `auth.me`, `orders.create` and `balances.list`; the realtime connection and subscription token requests behind every private subscription and the private order stream; `Credentials::sign_request`, `sign_request_with_recv_window` and `sign_request_async` without an explicit timestamp; `auth::MAX_SIGNING_FUTURE_SKEW_MS`.

## 0.20.0

Package version: `0.20.0`. Git tag: `v0.20.0`. Released 2026-11-11. Built on public contract `a3e3784`.

- **Breaking:** `orders.create`, `orders.preview` and `orders.batch_create` refuse a market order with `time_in_force: Some(CreateTimeInForce::Fok)` or `Some(CreateTimeInForce::Gtc)` with `Error::Validation` (`tif=fok is only valid for limit orders`, `tif=gtc is only valid for limit orders`), before anything is sent, as they already refuse `Gtd` (`tif=gtd / expire_at is only valid for limit orders`). The public contract's only market execution is `market_ioc`: "Market orders always execute as immediate-or-cancel". The SDK dropped the requested time in force and sent the order as IOC, so a buy or sell that asked for fill-or-kill could fill partly. `None` and `Some(CreateTimeInForce::Ioc)` still send `market_ioc`. Affected: `orders.create`, `orders.preview` and `orders.batch_create`, buy and sell. Migration: leave a market order's `time_in_force` `None` or set `Ioc`; for fill-or-kill, send a limit order with `Fok` and a limit price.
- **Breaking:** `triggers.create` refuses `time_in_force: Some(CreateTimeInForce::Gtc)` on a market child (a stop-loss, take-profit or TWAP with `order_type: CreateOrderType::Market`) and on a trailing stop with `Error::Validation` (`time_in_force=Gtc applies only to limit children; a market or trailing_stop child executes IOC`), before anything is sent, as it already refused `Fok` there. Those children always execute IOC; the SDK accepted `Gtc` and sent IOC. Limit stop-loss and take-profit children keep `Gtc`, `Ioc` and `Fok`, and ladder and TWAP limit slices keep `Gtc`. Affected: `triggers.create`. Migration: leave `time_in_force` `None` (or set `Ioc`) on market and trailing-stop children.
- Order and trigger reads now give each quantity the pair's base quantity scale from the loaded catalogs, as the order book and market overview do. They decoded every quantity without a scale, so `order.cum_qty.unwrap().format(None)` failed (`format requires a known scale`) even with the catalogs loaded, and a caller had to look the scale up for each read. These reads wait for the spot config load in progress, never for the funding config; without catalogs, after a failed spot load, or for a pair the catalogs don't know, quantities keep no scale as before. `Order::cum_quote`, `quote_used` and `quote_unspent` still take the scales. Affected: `orders.get`, `get_with`, `list_open`, `list_open_with`, `list_history` and `list_history_with` (`orig_qty`, `cum_qty`, `leaves_qty`, `inherited_cum_qty`, `fee_total.qty`, and each trade's `qty`); `trades.list` and `list_with`, and each fill of `trades.subscribe`; `orders.subscribe` (snapshot and unit orders, and a fill's `qty` when its unit carries the order); and `triggers.get`, `get_by_id`, `list`, `list_with` and `subscribe` (`qty`, and a TWAP's or ladder's `executed_qty`). The `codecs::decode` functions are unchanged and still decode without a scale.
- **Breaking:** A canceled trigger's `status` reads `canceled`, the contract's spelling, which trigger events (`TriggerEvent::event_type`) and order statuses already use; it read `cancelled`. The trigger status filter and `TRIGGER_STATUS_VALUES` use `canceled` too, and nothing accepts `cancelled` any more: the status filter refuses it with `Error::Validation` (`invalid trigger status "cancelled"; expected one of: created, armed, running, completed, canceled, failed, paused`), and `codecs::decode::trigger_status_from_label` and `trigger_event_type_from_label` return `Err` (`invalid trigger status "cancelled"; ...`, `invalid trigger event type "cancelled"; ...`) instead of naming the same status. The Go, Python and TypeScript SDKs spell it the same way. Affected: `triggers.get`, `get_by_id`, `list`, `list_with` (its `status` filter), `cancel` and every other trigger mutation result's `status`, `TRIGGER_STATUS_VALUES`, and `codecs::decode::trigger_status_label`, `trigger_status_from_label`, `trigger_event_type_from_label`, `trigger_from_proto` and the trigger mutation decoders. Migration: compare a trigger's `status` with `"canceled"`, and pass `"canceled"` to the status filter and the event-type parser.

## 0.19.0

Package version: `0.19.0`. Git tag: `v0.19.0`. Released 2026-11-11. Built on public contract `a3e3784`.

- **Breaking:** Spot trading no longer depends on the funding (deposit/withdraw) config. The client read the spot config and then the funding config and installed both together, so when the funding read failed, for example because the connection dropped (`client error (SendRequest)`) or the read stalled, nothing was installed: `wait_for_catalogs` returned the error and every call that names a market returned `Error::CatalogHydration` until a later `wait_for_catalogs` or `hydrate_catalogs` loaded both. The two now load on their own and are installed on their own. `wait_for_catalogs` returns `Ok` once the spot config has loaded and validated, and spot calls go on while the funding config is still loading or failed to load. The new `Client::wait_for_funding_catalog` waits for the funding config and returns `Error::CatalogLoad` naming it when it could not be loaded, loading it again on the next call as `wait_for_catalogs` does for the spot config; `zipper.subscribe_zipped_asset_supply`, which decodes with its scales, waits for it, sends nothing until it loads and returns `Error::CatalogHydration` naming it, without sending, when it failed. Only what reads the funding config waits: deposit addresses and withdrawals (`deposit.list_addresses`, `deposit.create_address`, `withdraw.create_*`) and `zipper.get_deposit_withdraw_config` read nothing from it and work while it is loading or failed. `catalogs.ledger_id_for_asset` keeps its signature and doesn't wait: `None` means either that the funding config has not loaded (yet, or its load failed) or that it does not know the asset. With `hydrate_catalogs: false` nothing changes. A spot config that is missing, empty or invalid still fails every spot call closed. `wait_for_catalogs`, `wait_for_funding_catalog`, `hydrate_catalogs` and `catalogs_last_error` now return the new `Error::CatalogLoad` where they returned `Error::Validation` with the failure flattened into text (`catalog hydration failed (zipper config): ...`); `hydrate_catalogs` loads both and returns the spot config's error first. `catalogs::Manager` gains `is_spot_ready` and `is_funding_ready`; `is_ready` still means both. `ServiceContext` gains the public field `funding_ready`. Affected: `Client::wait_for_catalogs`, `Client::hydrate_catalogs`, `Client::catalogs_last_error`, `Client::wait_for_funding_catalog` (new), `zipper.subscribe_zipped_asset_supply`, `catalogs::Manager`, `services::ServiceContext`, `Error` (new variant `CatalogLoad`), and every call that waits for the catalogs, such as `orders.create`, `triggers.create`, `orderbook.get` and `market_data.get_trades`. Migration: match `Error::CatalogLoad(failure)` where you matched `Error::Validation` from `wait_for_catalogs`, and read `failure.source` for the failure; call `wait_for_funding_catalog` before reading the zipper catalog or `ledger_id_for_asset`; set `funding_ready` where you build a `ServiceContext`, which has no `Default`, and add an `Error::CatalogLoad` arm where you match `Error` exhaustively.
- No SDK error prints a credential anywhere in its chain. An answer or a transport error quoting a URL with userinfo, such as a proxy's, reached the fields of the `Error` built from it, so `Display` and `Debug` printed the credential. Every error mapped from a Connect call is now redacted where it is mapped: each URL its fields quote, problem details and a rate limit's `detail` included, loses its userinfo, query and fragment, whatever characters they hold, and a proxy URL from the environment reads `[proxy]`. The variant, codes and `source()` chain stay as they are. A URL the URL parser refuses is dropped whole, and a userinfo holding `?` or `#` is dropped too. The client's own secrets never reach an error's text however an answer quotes them: the exact userinfo and password of its API and WebSocket URLs, of a proxy the environment sets, and its API private key read `[REDACTED]`, raw and percent-decoded. Limitations: a password shorter than 4 characters is not replaced where it stands alone, since replacing so short a string would mangle ordinary text; and a request signature is not redacted when the server echoes it back, since it is single-use and expires with its receive window. Affected: every service method that returns a mapped error, such as `orders.create`, `market_data.get_spot_config` and `balances.list`, and the catalog loads behind `wait_for_catalogs` and `wait_for_funding_catalog`.
- A catalog read at startup that times out, cannot connect, loses its connection or is answered `unavailable` (such as an HTTP 502, 503 or 504, but not a rate limit) is read again instead of failing the load: at most 3 reads per catalog, 250ms and then 500ms apart, each retry on a connection pool of its own, so it never waits behind a dropped or stalled HTTP/2 connection, all within `Config::catalog_timeout` of `Client::new`. **Breaking:** `catalog_timeout` is a new field of `Config` (`Option<Duration>`, `None` meaning twice `timeout`); `Client::new` refuses `Some(Duration::ZERO)` with `Error::Validation`. Each read gets at most `timeout`. A read that keeps stalling now fails within that deadline. Any other error answer, and a catalog that does not validate, fails at once; only these two reads are retried, and mutations never are. Affected: the catalogs `Client::new` and `Client::from_env` load with `hydrate_catalogs` (`marketdata.v1.MarketDataService/GetSpotConfig` and `chain.zipper.v1.ZipperService/GetDepositWithdrawConfig`), `Config` and its `Debug`. Migration: add `..Default::default()` where you build a `Config` with every field.
- `Error::CatalogLoad` says what failed: its `CatalogLoadFailure` names the catalog (`"spot"` or `"funding"`), the operation, the kind of failure (`"timeout"`, `"connection_closed"`, `"connect_failed"`, `"unavailable"`, `"invalid"` or `"error"`), the number of reads, the time they took and the edge's response reference (the `cf-ray` header, or `x-request-id` when there is none) when a response arrived, and keeps the last failure, typed, as `source`. Its message reads, for example, `catalog hydration failed: funding config (chain.zipper.v1.ZipperService/GetDepositWithdrawConfig) connection closed after 3 attempts in 0.8s: ...`, without URL credentials. `Error::CatalogHydration` around it shows the same message. Affected: `Error`, `CatalogLoadFailure` (new, re-exported at the crate root).
- **Breaking:** `models::ZipperAssetChainVariant` gains `supply`, the variant's zToken supply: `supply_q` at its asset's quantity scale, such as `97.08173223` for 9708173223 at scale 8 and `3582802` at scale 0, and `0` for a zero supply; empty only for an asset scale no quantity can be formatted at. `zipper.get_deposit_withdraw_config` dropped it, and the catalogs knew a variant's supply only after a `zipper.subscribe_zipped_asset_supply(true)` update, so `catalogs.supply_for_zipped_asset_id` returned `None` until then. The zipper config the client hydrates now seeds it; a config hydrated later replaces the supply of each variant it names and keeps the live updates of the others. It is encoded as `supply` in JSON. Affected: `zipper.get_deposit_withdraw_config`, `catalogs.supply_for_zipped_asset_id`, `catalogs.hydrate_zipper_config`, `catalogs.hydrate_zipper_config_json` and `catalogs.hydrate_spot_and_zipper_json` (which read a variant's `supply` string), and `codecs::decode::zipper::deposit_withdraw_config_from_proto`. Migration: a `ZipperAssetChainVariant` struct literal no longer compiles without `supply`; set it, or read the value from a decoded config.
- **Breaking:** `Error::PermissionDenied` of a refused realtime subscription token request names its `endpoint` as `.../v1/rt/subscribe`, without `?channel=...`, which its `context` still names: a URL in an error keeps only its scheme, host, port and path, never a query or fragment, as in the Go and Python SDKs. Tests also pin that a redirect whose `Location` does not parse, with a non-numeric port or an unclosed IPv6 host, never brings that `Location`'s userinfo, query or fragment into an error: neither ConnectRPC nor the realtime token client reads the `Location`, so a signed call or a token request answered with 301, 307 or 308 fails with the redirect status, in JSON and binary. Affected: `Error::PermissionDenied` from the realtime subscription token request behind every private subscription, such as `orders.subscribe` and `balances.subscribe`. Migration: read the channel from `context`, not from `endpoint`.
- Tests pin that a request signed with an API key is never forwarded to another origin by an HTTP redirect. The ConnectRPC transport (connectrpc's hyper client), the realtime token client (hyper-util's client) and the WebSocket handshake (tungstenite) follow no redirects, so a signed call answered with a 307 or 308 whose `Location` names another host or port fails with the redirect status and the destination receives nothing, in JSON and binary; the same holds for a realtime token request and every redirect status. No behaviour changes. Affected: none; the tests cover `auth.me` over ConnectRPC and the realtime connection token request.

## 0.18.0

Package version: `0.18.0`. Git tag: `v0.18.0`. Released 2026-11-10. Built on public contract `a3e3784`.

- A method that names a market by symbol, or needs a pair's catalog scales, now waits for the catalog hydration the client starts in the background. It read the catalogs at once, so a call made right after `Client::new`, such as `orderbook.create_subscription` for `"DOGE-USDT"`, refused a valid symbol with `Error::Validation` (`unknown symbol ...` or `symbol_id is required for orderbook subscriptions`) while hydration was still running; only order and trigger paths and `market_overview.list` waited. A symbol the hydrated catalogs don't know returns the unknown-symbol `Error::Validation`. With `hydrate_catalogs: false` nothing waits and the refusals are unchanged. Affected: `market_data.get_trades`, `get_trades_with`, `get_candles`, `get_candles_with`, `get_current_candle`, `get_candles_columns`, `subscribe_trades` and `subscribe_candles`; `orderbook.get` and `orderbook.create_subscription`; `heatmap.get`, `heatmap.get_with_options` and `heatmap.subscribe_live`; and `zipper.subscribe_zipped_asset_supply`, whose updates are decoded with the catalog scales.
- `market_overview.subscribe`, `market_overview.get_spot_volume_history` without symbols, `fees.get_spot_fee_rates`, and `triggers.list`, `triggers.list_with` without a symbol, `triggers.get` and `triggers.get_by_id` now wait for hydration in progress before they label and scale their results. Called right after the client was created, they returned an empty `symbol` and, for the overview, rows without the sizes and volumes the catalog scales decode. A failed hydration still leaves those fields empty and returns no error.
- **Breaking:** `Error` gains `Error::CatalogHydration(Box<Error>)`, which a method that needs the catalogs returns when hydration failed. The boxed error is the failure that `Client::wait_for_catalogs` returns and `Client::catalogs_last_error` holds, and the message still starts `catalog hydration failed`. It is not retryable as is: `wait_for_catalogs` or `hydrate_catalogs` hydrates again. The methods that already waited returned the failure itself, an `Error::Validation`. With hydration on, `orderbook.create_subscription` refuses a symbol the hydrated catalogs don't know with `unknown symbol ...` instead of `symbol_id is required for orderbook subscriptions`. Affected: `ServiceContext::wait_for_catalogs`; `orders.create`, `preview`, `batch_create`, `batch_replace` and `modify`; `orders.list_history_with`, `cancel_with`, `cancel_all_with` and `cancel_all_after`, and `trades.list_with`, with a symbol; `triggers.create` and `modify`, and `triggers.list_with` and `resume_with` with a symbol; `market_overview.list`, and `market_overview.get_spot_volume_history` and `create_subscription` with symbols; `Client::stop_all_trading`; and the methods named above. Migration: add an `Error::CatalogHydration(failure)` arm to exhaustive matches on `Error`, and match it where you matched the `Error::Validation` of a failed hydration.
- **Breaking:** `ZipperChainConfig` gains the network's memo rule for withdrawal destinations: `memo_requirement` (`"required"`, `"optional"` or `"not_supported"`; empty when the network names none, which does not mean a memo can be omitted; `"UNKNOWN(n)"` for a rule this version does not know), `memo_label`, such as `"Destination tag"`, and `memo_format_hint`, display guidance kept exactly as sent and not parsed. XRP reports `"optional"`. They were not decoded. Affected: `zipper.get_deposit_withdraw_config` and `codecs::decode::deposit_withdraw_config_from_proto`. Migration: a `ZipperChainConfig` struct literal must set the three fields; use `memo_requirement` as the rule and the label and hint for display only.
- **Breaking:** `InternalTransferResult` gains `flow_id`, the transfer's lifecycle flow ID, which `lifecycle.get_flow` reads; empty when the server sends none. Affected: `internal_transfers.create` and `codecs::decode::internal_transfer_from_proto`. Migration: an `InternalTransferResult` struct literal must set `flow_id`. Looking a transfer up by its request or transfer ID (`GetInternalTransfer`) is session-only and is not wrapped.
- **Breaking:** `ApiPolicy` and `SubaccountPolicy` gain `actions`, the policy's granted `auth.v1.PolicyAction` names in the order the server sent them, such as `"READ_SPOT"`; `"UNKNOWN(n)"` for an action this version does not know. They were dropped. This includes the account permissions API-key policies can now grant, from `READ_API_KEYS` on: `READ_API_KEYS`, `READ_API_POLICIES`, `READ_SUBACCOUNT_POLICIES`, `READ_SUBACCOUNT_PERMISSIONS`, `READ_VIP_STATUS`, `READ_SOCIAL_VERIFICATION`, `READ_PROFILE`, `READ_ACCOUNT_DIRECTORY`, `READ_WITHDRAW_VALIDATION`, `READ_DAILY_CLAIMS`, `CLAIM_DAILY_REWARD`, `READ_CHECKPOINTS` and `READ_REWARDS`. Each name parses back with `PolicyAction::from_proto_name`. Affected: `policies.subscribe`, `policies.subscribe_subaccount_policies`, `policies.subscribe_api_policies`, and `codecs::decode::api_policy_from_proto`, `subaccount_policy_from_proto`, `api_policy_from_bytes`, `subaccount_policy_from_bytes` and the policy list, get, create and update decoders. Migration: an `ApiPolicy` or `SubaccountPolicy` struct literal must set `actions`.
- API keys can now make account reads that needed a session, each gated by its own permission on the key's API-key policy. The wrapped ones: `api_keys.list` and `api_keys.get` need `READ_API_KEYS`; `vip.get_vip_status` needs `READ_VIP_STATUS` and a main-account key, where it always refused an API key; `social_verification.get` needs `READ_SOCIAL_VERIFICATION` and a main-account key, and returns the link status only. A key without the permission is refused with `permission_denied` and `AUTH_API_KEY_ACCESS_DENIED`, and the message names the permission. The rustdoc and README no longer list VIP status as session-only. Affected: `api_keys.list`, `api_keys.get`, `vip.get_vip_status` and `social_verification.get`.
- **Breaking:** `withdraw.validate_destination` with an API key now needs `READ_WITHDRAW_VALIDATION` on the key's policy; `EXTERNAL_WITHDRAW` alone no longer opens it. Policies that granted `EXTERNAL_WITHDRAW` were given `READ_WITHDRAW_VALIDATION` when the API changed, but a policy you create or rewrite must include it. Without it the call is refused with `permission_denied` and `ERROR_CODE_PERMISSION_DENIED`. Affected: `withdraw.validate_destination`. Migration: include `READ_WITHDRAW_VALIDATION` in a new API-key policy whose key validates destinations, and keep it, with every other account permission you read back, when you replace a policy's whole action list, or the update removes it.
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
