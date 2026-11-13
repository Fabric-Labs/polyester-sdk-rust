# Public contract 74af7f3 follow-up

Based on `74af7f3` (exchange release 0.93.1); the previous contract is `78792a5`. Generated output is inherited from the generated change and is unchanged by the handwritten follow-up.

| Change note | Handwritten SDK decision |
| --- | --- |
| 2026-11-11T0421Z-ai-v1 | Skipped: the assistant API is not wrapped. |
| 2026-11-11T2227Z-triggers-v1 | `ERROR_CODE_TRIGGER_ACTIVATION_PRICE_NOT_MODIFIABLE` (315) passes through `triggers.modify` as `Error::Api` with that code from ConnectRPC and REST, final and not retryable; a test pins both transports. The REST error catalog fixtures include it. |
| 2026-11-12T0043Z-ledger-v1 | Docs only: `LedgerTransfer::transfer_type` describes 1065 and 1075 as the debit of a failed withdrawal, not a fee. The SDK has no transfer-code labels. |
| 2026-11-12T0054Z-orders-v1 | Covered: `batch_request_id` is a public-ID string, passed back unchanged to `get_batch_replace_status`. Docs say it is opaque and how to reconcile older IDs. |
| 2026-11-12T0105Z-auth-v1 | Skipped: `SetAssistantKeys` is not wrapped (interactive session with step-up, devnet only). The generated `AssistantKeySpec::preset` is available. |
| 2026-11-12T0106Z-ai-v1 | Skipped: the assistant API is not wrapped. |
| 2026-11-12T0155Z-chain-lifecycle-v1 | No SDK change: the SDK reads no OpenAPI document, and the README points at none for REST paths. |
| 2026-11-12T0205Z-ledger-read-v1 | `ListTransfersRequest::ts_max_us` is `Option<u64>` from the generated change; tests pin that `None` is absent and `Some(0)` is sent as an explicit zero through `transfers.list` on the binary and JSON wires. The SDK's order history and trade reads send no upper time bound. |
| 2026-11-12T0211Z-marketoverview-v1 | The managed order book applies an update only at exactly its `book_seq_start`; any other update or a malformed one leaves the book out of sync (`available: false`) until the next reset, a manual refresh or a reconnect, without fetching a snapshot. `orderbook::apply_delta` follows the same rule. Market overview reads use ConnectRPC, which does not change; an `INVALID_ARGUMENT` refusal passes through as an error. |
| 2026-11-12T0310Z-ai-v1 | Skipped: the assistant API is not wrapped. |
| 2026-11-12T0428Z-orders-v1 | Covered: order decoders read `batch_request_id` on working and ended orders; a test pins filled and canceled orders in history reads and on the order stream. |
| 2026-11-12T0507Z-auth-v1 | REST `503 AUTH_TEMPORARILY_UNAVAILABLE` maps like its ConnectRPC answer, a retryable `Error::Api` that changed nothing (it was an unknown-outcome `Error::Server`). A test pins every bare credential code of the note's tables and the `404 NOT_FOUND` problem. The SDK has no endpoint-specific `AUTH_` alias table; it reads `AUTH_` codes generically because ConnectRPC sends them. |
| 2026-11-12T0646Z-ai-v1 | Skipped: the assistant API is not wrapped. |
| 2026-11-12T0654Z-changelog-v1 | `list_deployment_groups`, `deployment_groups` (stream) and `get_deployment_group`; release fields on `ChangelogPostSummary` and `environment_exceptions` on `ChangelogChange`, with optional strings and timestamps kept as `Option`. |
| 2026-11-12T0826Z-notifications-v1 | Skipped: the notification center is listed as not wrapped; the generated broker allowlist notice builds. |
| 2026-11-12T1358Z-chain-lifecycle-v1 | Not applicable: lifecycle reads expose flow summaries, not step amounts. |
| 2026-11-12T1522Z-chain-lifecycle-v1 | Docs and a test: `ZipperReasonDetails` keeps the server's `reason_id` and `message` for an execution failure under review, whatever the code; the SDK has no text table keyed by the code. Auto-withdraw is not wrapped. |
