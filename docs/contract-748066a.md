# Public contract 748066a follow-up

Based on `748066a20d62cea3ac5b7568797458266a8bc67b`. Generated output is inherited from the generated PR and is unchanged by the handwritten follow-up.

| Change note | Handwritten SDK decision |
| --- | --- |
| 2026-11-03T1601Z-orders-v1 | Existing quote-used/unspent helpers document exactness through base scale 9 and a possible excess below 1e-18 per fill above that scale; no rounding added. |
| 2026-11-03T1601Z-triggers-v1 | Market halts use the existing market_halted failure label (8); no special mapping for unnamed 212. Generic unknown-value preservation stays. |
| 2026-11-03T1943Z-auth-v1 | Generated descriptors inherited; no handwritten text promises reserved field or enum names/numbers. |
| 2026-11-03T2227Z-triggers-v1 | Server handles blocked attached-leg rearming. No client re-fire loop or interpretation of repeated fires exists. |
| 2026-11-04T0002Z-triggers-v1 | Status/reason decoding is generic. No client infers user cancellation from a drained ladder or a system-canceled child. |
| 2026-11-04T0020Z-triggers-v1 | Documented breaking immediate-trailing admission behavior: create ends FAILED with reason 244; trigger/order modify is refused with code 302. To start trailing from the market, clear activation with a present zero. No local code contradicts server admission. |
| 2026-11-04T0201Z-orders-v1 | Optional account selectors remain omitted to let the server choose the key-bound account. Balances use fixed 18-decimal units. Execution-history pages are pinned to the returned order ID; history includes lineage trades and is compared with lineage cumulative quantity, not one generation's delta. Columnar candle pages retain ascending order, with continuation toward older candles; no auto-concatenation helper exists. Fee totals may refresh at the same version; cancel-all is admission, and settlement may precede trade projection. |
| 2026-11-04T0211Z-ledger-read-v1 | Removed holds-list wrappers, models, exports and decoders. Reservation fixtures read per-asset reserved balances and require the reservation to be observed before testing its release. |
| 2026-11-04T0229Z-fees-v1 | Added both reserve rates, next-change time and typed full breakdown. Omitted components/times are optional, absent discounts are empty and absent all-markets is false. The largest-buy sizing helper and rate-recomputation helper are deferred: neither existed in this SDK, and serving the exact decimal strings lets callers use the authoritative values without introducing a new arithmetic API. No float-based sizing helper was added. |
| 2026-11-04T0312Z-triggers-v1 | Preserve refusal codes and field violations, including transport-specific field paths. No renaming of Connect price fields to REST names. |
| 2026-11-04T0421Z-triggers-v1 | Events and trigger reads share not-found mapping and the bounded UNAVAILABLE retry policy. Regression tests assert attempts and unchanged request payloads. |
| 2026-11-04T0430Z-claims-v1 | Shared HTTP problem decoder derives type with lowercase and underscore-to-hyphen replacement; original code remains available as the discriminator. The SDK's REST surface is realtime token exchange; normal service calls use ConnectRPC. |
| 2026-11-04T0604Z-transfer-v1 | Realtime authorization and early gateway authentication use the shared problem decoder, retaining status, original code, detail (including empty) and canonical type. |

Inspect REST metadata through `error.http_problem()` (`code`, `status`, `detail`, `problem_type`). Pattern matches on enriched auth error variants should include `..`. For `Error::Api`, `metadata` may contain a synthetic `http_problem` entry holding the JSON-encoded problem; prefer `http_problem()` for typed access.

The sizing suggestions are intentionally not implemented. To size a buy externally, use integer quote units of 10^-18, round both notional and reserved fee upward, then search quantities on the market step. The reserve rate, rather than the effective fill rate, controls the budget. Refetch fee rates at the optional next-change time.
