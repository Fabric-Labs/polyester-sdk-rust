# Public contract 78792a5 follow-up

Based on `78792a5` (exchange release 0.93.0); the previous contract is `e339360`. Generated output is inherited from the generated change and is unchanged by the handwritten follow-up.

| Change note | Handwritten SDK decision |
| --- | --- |
| 2026-11-04T0745Z-orders-v1 | Covered: an attached leg's minimums refusal keeps its `order.attached_risk.<leg>` field violation on `Error::violations()`, preview rejections and batch items. A batch-create test pins an `order.attached_risk.stop_loss` violation. |
| 2026-11-07T1626Z-triggers-v1 | `Error::is_trigger_already_ended()` reports `ERROR_CODE_TRIGGER_STATUS_INVALID` from either transport; `TRIGGER_NOT_FOUND` is left out (it also means a wrong ID). Cancel, pause, resume and modify docs say both mean "already ended". Ladder `fire_refused` after `fired` with the same `child_seq` is documented and pinned by a test. |
| 2026-11-07T2159Z-marketdata-v1 | `PairOrderLimits::price_band` (`Option<PriceBand>`) from `get_spot_config` and the catalogs; `CANCEL_REASON_PRICE_BAND` and `price_band_violation` read by name. |
| 2026-11-07T2213Z-tradews-v1 | No SDK wrapper for the trading WebSocket. Reachable effect: the `CANCEL_REASON_CANCEL_ON_DISCONNECT` cancel reason. |
| 2026-11-07T2310Z-orders-v1 | `OrderSelfTradePrevention::Decrement`; reads label `decrement`; `CANCEL_REASON_STP_DECREMENT` reads by name. |
| 2026-11-07T2315Z-orders-v1 | `CreateOrderParams::display_quantity` / `PreviewOrderParams::display_quantity`, limit GTC and GTD only, positive; `Order::display_qty` and `hidden_qty`. Display rules beyond shape are the API's. |
| 2026-11-08T0251Z-tradews-v1 | No SDK wrapper for the trading WebSocket. |
| 2026-11-08T0634Z-orders-v1 | Description of `ATTACHED_RISK_NOT_SUPPORTED` only; the code already passes through. |
| 2026-11-08T2026Z-orders-v1 | `CANCEL_REASON_CANCEL_ON_DISCONNECT` reads by name. |
| 2026-11-08T2040Z-orders-v1 | `CreateOrderParams::peg` (`OrderPeg`, `PegReference`); `offset_ticks` is a count of the pair's price increments, sent as given. Market, quote sizing and attached risk with a peg are refused locally. |
| 2026-11-08T2220Z-triggers-v1 | OCO create and cancel and OCO trigger fields were added before this follow-up. This follow-up checks `client_list_id` as the contract does, any 1 to 36 bytes of UTF-8 sent unchanged on the create and the `client_list_id` listing filter alike, and keeps the create's `accepted_ts_ns` and the cancel's `ts_ns`. `decrement` on an OCO is sent; the server decides. |
| 2026-11-08T2256Z-orders-v1 | Covered: order decoders read `origin` on every revision. |
| 2026-11-09T1435Z-ledger-read-v1 | Docs only: `LedgerTransfer::tx_id` carries the flow ID from the first realtime update of a Trading to Funding transfer. |
| 2026-11-09T1527Z-triggers-v1 | Batch trigger cancel was added before this follow-up; this follow-up keeps the answer's `ts_ns`. |
| 2026-11-09T1555Z-swap-quickswap-v1 | Skipped: Quick Swap is served by the TypeScript SDK only. |
| 2026-11-10T0141Z-tradews-v1 | No SDK wrapper for the trading WebSocket; `REQUEST_DEADLINE_EXCEEDED` at admission was already handled. |
| 2026-11-10T0504Z-orders-v1 | `Decrement` is accepted with attached risk and on trigger creates. |
| 2026-11-10T0734Z-orders-v1 | `CreateOrderParams::quote_proceeds_scaled`, SELL market or limit IOC only, positive, quote scale; `submitted_quote_proceeds_scaled` on `Order`, `OrderMutationResult` and `BatchCreateResultItem` (with `submitted_max_quote_debit_scaled`). Renamed code 68 and failure reason 24 read `QUOTE_SIZE_TOO_SMALL` / `quote_size_too_small`. |
| 2026-11-10T1048Z-ai-v1 | Skipped: the assistant API is not wrapped. |
| 2026-11-10T1344Z-notifications-v1, 2026-11-10T1436Z-notifications-v1 | Skipped: the notification center is listed as not wrapped. |
| 2026-11-11T0047Z-alerts-v1 | `READ_PRICE_ALERTS` and `MANAGE_PRICE_ALERTS` read by name (policies are not written by this SDK). Price alerts documented devnet only; a missing permission is `Error::Api` with `auth_error_code()` `AUTH_API_KEY_ACCESS_DENIED`. |
| 2026-11-11T0139Z-orders-v1 | `API_KEY_SPOT_TRADE_DENY`, `API_KEY_POLICY_REQUIRED` and `API_KEY_MARKET_DENY` classify as `POLICY_SPOT_TRADE_DENY` and `POLICY_MARKET_DENY` do: `Error::PermissionDenied` with the code without its `ERROR_CODE_` prefix, the same over REST and ConnectRPC, not retryable. |
| 2026-11-11T0210Z-orders-v1 | Covered: `TRIGGER_WOULD_TRIGGER_IMMEDIATELY` on a market create or preview passes through as `Error::Api` or a preview rejection. |
| 2026-11-11T0412Z-swap-quickswap-v1, 2026-11-11T0619Z-swap-quickswap-v1, 2026-11-12T0542Z-swap-quickswap-v1 | Skipped: Quick Swap is served by the TypeScript SDK only. |
| 2026-11-11T0552Z-ledger-v1 | Docs only: the `withdraw_failed` debit carries its withdrawal's flow ID in `list_transfers`, not on the realtime stream. |
| 2026-11-11T2140Z-triggers-v1 | Docs and changelog only: the SDK derives nothing from a trigger ID's value. |
| 2026-11-11T2224Z-orders-v1 | Covered: a batch replace item refused for its value keeps its `item_index`, `code` and indexed violations; a test pins one. |
| 2026-11-11T2240Z-changelog-v1 | `assistant` surface label and filter; at most 11 surfaces. |
| 2026-11-11T2306Z-tradews-v1 | No SDK wrapper for the trading WebSocket. |
