//! POLY-3746 L2 integration tests via public SDK APIs + local mock HTTP/WS.
//!
//! These exercise production paths (subscribe_raw, JsonRpcClient, wait_for_catalogs,
//! Quantity::format, SnapshotThenStream) against in-process servers — not private helpers.

#[path = "api_key_display/mod.rs"]
mod api_key_display;
#[path = "batch_replace_status_errors/mod.rs"]
mod batch_replace_status_errors;
#[path = "catalog_hydration_wait/mod.rs"]
mod catalog_hydration_wait;
#[path = "catalog_startup_recovery/mod.rs"]
mod catalog_startup_recovery;
#[allow(dead_code)]
#[path = "hardening_support/centrifugo.rs"]
mod centrifugo;
#[path = "changelog_revisions/mod.rs"]
mod changelog_revisions;
#[path = "contract_74af7f3/mod.rs"]
mod contract_74af7f3;
#[path = "contract_e339360/mod.rs"]
mod contract_e339360;
#[path = "error_chain_redaction/mod.rs"]
mod error_chain_redaction;
mod hardening_support;
#[path = "limit_market_ref_price/mod.rs"]
mod limit_market_ref_price;
#[path = "live_write_gates/mod.rs"]
mod live_write_gates;
#[path = "market_data/mod.rs"]
mod market_data;
#[path = "market_tif/mod.rs"]
mod market_tif;
#[path = "order_by_symbol_id/mod.rs"]
mod order_by_symbol_id;
#[path = "order_read_scales/mod.rs"]
mod order_read_scales;
#[path = "order_trades_wait/mod.rs"]
mod order_trades_wait;
#[path = "orderbook_depths/mod.rs"]
mod orderbook_depths;
#[path = "orders_and_batches/mod.rs"]
mod orders_and_batches;
#[path = "read_model_retention/mod.rs"]
mod read_model_retention;
#[path = "readable_error_bodies/mod.rs"]
mod readable_error_bodies;
#[path = "recv_window_replay/mod.rs"]
mod recv_window_replay;
#[path = "request_size_rate_limit/mod.rs"]
mod request_size_rate_limit;
#[path = "signed_redirects/mod.rs"]
mod signed_redirects;
#[path = "subaccount_get_expansions/mod.rs"]
mod subaccount_get_expansions;
#[path = "trigger_oco_and_batch_cancel/mod.rs"]
mod trigger_oco_and_batch_cancel;
#[path = "trigger_reads_and_filters/mod.rs"]
mod trigger_reads_and_filters;
#[path = "url_userinfo/mod.rs"]
mod url_userinfo;
#[path = "zipper_supply/mod.rs"]
mod zipper_supply;

use buffa::Message;
use futures_util::future::FutureExt;
use hardening_support::{
    GET_BALANCES_PATH, GET_ORDER_PATH, GET_TRADES_PATH, MockHttpServer, MockWsServer,
    SPOT_CONFIG_PATH, ZIPPER_CONFIG_PATH, centrifugo_ok_reply, connect_proto_ok, test_credentials,
    wait_until,
};
use polyester::Error;
use polyester::auth::Credentials;
use polyester::chain::JsonRpcClient;
use polyester::codecs::{MAX_PROTOCOL_SCALE, format_ledger_u128, format_qty_scaled};
use polyester::realtime::Client as RealtimeClient;
use polyester::realtime::{
    MAX_REALTIME_MESSAGE_BYTES, SnapshotThenStream, SnapshotThenStreamConfig,
};
use polyester::transport::MAX_CONNECT_RESPONSE_BYTES;
use polyester::{Client, Config, Quantity, QuantityDomain};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const PRIVATE_CHANNEL: &str = "private:spot:trades:acct:proto";
const PUBLIC_CHANNEL: &str = "public:spot:market:trades:1:proto";
type PublicationDecoder =
    Arc<dyn Fn(&[u8]) -> polyester::Result<()> + Send + Sync + std::panic::RefUnwindSafe>;

fn publication_decoders() -> Vec<(&'static str, PublicationDecoder)> {
    use polyester::codecs::decode::{
        account_identity_from_bytes, address_book_invalidation_from_bytes, api_key_from_bytes,
        api_policy_from_bytes, asset_balance_from_bytes, candle_point_from_bytes,
        flow_detail_from_bytes, flow_summary_from_bytes, heatmap_live_bucket_from_bytes,
        ledger_transfer_batch_from_bytes, market_overview_batch_from_bytes,
        market_trade_batch_from_bytes, orderbook_delta_from_bytes, subaccount_from_bytes,
        subaccount_policy_from_bytes, trigger_event_batch_from_bytes, trigger_from_bytes,
        user_trade_batch_from_bytes, zipped_asset_supply_batch_from_bytes,
    };

    macro_rules! decoder {
        ($name:literal, $decode:expr) => {
            (
                $name,
                Arc::new(move |bytes: &[u8]| ($decode)(bytes).map(|_| ())) as PublicationDecoder,
            )
        };
    }

    vec![
        decoder!("user_trade_batch", user_trade_batch_from_bytes),
        decoder!("asset_balance", asset_balance_from_bytes),
        decoder!("ledger_transfer_batch", ledger_transfer_batch_from_bytes),
        decoder!("trigger", trigger_from_bytes),
        decoder!("trigger_event_batch", trigger_event_batch_from_bytes),
        decoder!("market_trade_batch", market_trade_batch_from_bytes(8)),
        decoder!("orderbook_delta", orderbook_delta_from_bytes),
        decoder!("flow_summary", flow_summary_from_bytes),
        decoder!("flow_detail", flow_detail_from_bytes),
        decoder!("account_identity", account_identity_from_bytes),
        decoder!(
            "candle_point",
            candle_point_from_bytes(1, "1m".to_owned(), 8)
        ),
        decoder!("heatmap_live_bucket", heatmap_live_bucket_from_bytes),
        decoder!("market_overview_batch", market_overview_batch_from_bytes),
        decoder!("zipped_asset_supply_batch", |bytes| {
            zipped_asset_supply_batch_from_bytes(bytes, |_| Some(18))
        }),
        decoder!("api_key", api_key_from_bytes),
        decoder!("subaccount", subaccount_from_bytes),
        decoder!("subaccount_policy", subaccount_policy_from_bytes),
        decoder!("api_policy", api_policy_from_bytes),
        decoder!(
            "address_book_invalidation",
            address_book_invalidation_from_bytes
        ),
    ]
}

#[test]
fn l2_all_publication_decoders_are_panic_free_for_adversarial_protobuf() {
    let decoders = publication_decoders();
    let fixed_corpus: &[&[u8]] = &[
        &[],
        &[0x0f],
        &[0x0a],
        &[0x0a, 0xff],
        &[0x80; 11],
        &[0xff; 32],
        &[0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f],
    ];

    for (name, decode) in &decoders {
        for payload in fixed_corpus {
            let result = std::panic::catch_unwind(|| decode(payload));
            assert!(result.is_ok(), "{name} panicked for {payload:02x?}");
            assert!(
                result.unwrap().is_err(),
                "{name} accepted malformed protobuf {payload:02x?}"
            );
        }
    }

    // Deterministic corpus: 5,000 arbitrary byte strings and 5,000
    // structurally well-framed protobuf messages, each exercised against all
    // 20 publication decoders (200,000 decoder invocations). The latter gets
    // beyond the first tag and into nested/manual conversion logic.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for case in 0..5_000_usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let len = 1 + (usize::try_from(state).unwrap_or(usize::MAX) % 512);
        let mut payload = vec![0_u8; len];
        for byte in &mut payload {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        for (name, decode) in &decoders {
            let result = std::panic::catch_unwind(|| decode(&payload));
            assert!(result.is_ok(), "{name} panicked for mutation case {case}");
        }
    }
    for case in 0..5_000_usize {
        let mut payload = Vec::new();
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let fields = 1 + (state as usize % 8);
        for _ in 0..fields {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let field_number = 1 + (state as u8 % 20);
            match state % 3 {
                0 => {
                    payload.push(field_number << 3);
                    let mut value = state;
                    loop {
                        let byte = (value & 0x7f) as u8;
                        value >>= 7;
                        if value == 0 {
                            payload.push(byte);
                            break;
                        }
                        payload.push(byte | 0x80);
                    }
                }
                1 => {
                    payload.push((field_number << 3) | 1);
                    payload.extend_from_slice(&state.to_le_bytes());
                }
                _ => {
                    payload.push((field_number << 3) | 2);
                    let len = state as usize % 24;
                    payload.push(len as u8);
                    for _ in 0..len {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        payload.push(state as u8);
                    }
                }
            }
        }
        for (name, decode) in &decoders {
            let result = std::panic::catch_unwind(|| decode(&payload));
            assert!(
                result.is_ok(),
                "{name} panicked for well-framed mutation case {case}"
            );
        }
    }

    let oversized = vec![0_u8; MAX_REALTIME_MESSAGE_BYTES + 1];
    for (name, decode) in &decoders {
        let error = decode(&oversized).expect_err("oversized publication must fail closed");
        assert!(
            error.to_string().contains("exceeds"),
            "{name} returned the wrong oversized-payload error: {error}"
        );
    }
}

#[tokio::test]
async fn l2_all_publication_decode_errors_surface_through_the_public_websocket_api() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(active.clone(), vec![0x0f])
        .await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);

    for (name, decode) in publication_decoders() {
        let mut sub = rt
            .subscribe_proto_with_options(PUBLIC_CHANNEL, move |bytes| decode(bytes), false)
            .await
            .unwrap_or_else(|err| panic!("{name} handshake failed: {err}"));
        let error = tokio::time::timeout(Duration::from_secs(2), sub.recv_result())
            .await
            .unwrap_or_else(|_| panic!("{name} decode failure did not terminate the feed"))
            .unwrap_err();
        assert!(
            error.to_string().contains("proto decode"),
            "{name} returned the wrong error: {error}"
        );
        assert!(
            !sub.is_alive(),
            "{name} feed remained alive after decode error"
        );
        sub.close();
    }

    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_secs(2),
    )
    .await;
}

#[tokio::test]
async fn mock_http_server_hides_only_a_port_probe() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let seen = Arc::new(std::sync::Mutex::new(Vec::<(String, String)>::new()));
    let log = seen.clone();
    let http = MockHttpServer::spawn(move |request| {
        log.lock().unwrap().push((request.method, request.path));
        hardening_support::HttpScript::NotFound
    })
    .await;
    let address = http.base_url().replace("http://", "");
    for request_line in [
        "HEAD / HTTP/1.1",
        "HEAD /v1/rt/token HTTP/1.1",
        "GET / HTTP/1.1",
    ] {
        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        let request = format!("{request_line}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 404"), "{request_line}");
    }
    assert_eq!(http.requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        *seen.lock().unwrap(),
        [
            ("HEAD".to_owned(), "/v1/rt/token".to_owned()),
            ("GET".to_owned(), "/".to_owned()),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn l2_auth_identical_request_burst_is_unique_future_bounded_and_runtime_safe() {
    use polyester::auth::{HEADER_SIGNATURE, HEADER_TIMESTAMP, MAX_SIGNING_FUTURE_SKEW_MS};
    use std::collections::{BTreeMap, HashSet};
    use std::time::{SystemTime, UNIX_EPOCH};

    const FROM_THREADS: usize = 2_000;
    const FROM_RUNTIME: usize = 3_000;
    const URL: &str = "https://api.example.test/orders.v1.OrdersService/CreateOrder";
    const BODY: &[u8] = b"identical-order-body";

    fn signed_tuple(headers: BTreeMap<String, String>) -> (u64, String, u64) {
        (
            headers[HEADER_TIMESTAMP].parse::<u64>().unwrap(),
            headers[HEADER_SIGNATURE].clone(),
            u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap(),
        )
    }

    // Credentials with one key id share a process-wide timestamp allocator. This
    // burst holds it at its ceiling for seconds, so it needs a key of its own or
    // every other test signing with that id waits behind it.
    let credentials = test_credentials("ak_signing_burst", TEST_KEY);

    // Several threads bring the allocator to its ceiling however slow one
    // signature is on this host, so the runtime's signers below have to wait.
    let claimed = Arc::new(AtomicUsize::new(0));
    let threads = (0..8)
        .map(|_| {
            let credentials = credentials.clone();
            let claimed = claimed.clone();
            std::thread::spawn(move || {
                let mut tuples = Vec::new();
                while claimed.fetch_add(1, Ordering::SeqCst) < FROM_THREADS {
                    loop {
                        match credentials.sign_request("POST", URL, BODY, None) {
                            Ok(headers) => {
                                tuples.push(signed_tuple(headers));
                                break;
                            }
                            Err(Error::RateLimit { .. }) => {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            Err(other) => panic!("{other:?}"),
                        }
                    }
                }
                tuples
            })
        })
        .collect::<Vec<_>>();
    let mut tuples = Vec::with_capacity(FROM_THREADS + FROM_RUNTIME);
    for thread in threads {
        tuples.extend(thread.join().unwrap());
    }

    // The ticker only runs while signing yields the runtime. A wait that blocked
    // the thread would leave it almost no ticks, however busy the host.
    let signed = Arc::new(AtomicUsize::new(0));
    let ticker = tokio::spawn({
        let signed = signed.clone();
        async move {
            let mut ticks = 0usize;
            while signed.load(Ordering::SeqCst) < FROM_RUNTIME {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ticks += 1;
            }
            ticks
        }
    });
    // Let the ticker arm its first timer before creating runnable tasks so
    // task-spawn overhead is not misclassified as signing backpressure.
    tokio::task::yield_now().await;
    let handles = (0..FROM_RUNTIME)
        .map(|_| {
            let credentials = credentials.clone();
            let signed = signed.clone();
            tokio::spawn(async move {
                let headers = credentials
                    .sign_request_async("POST", URL, BODY, None)
                    .await
                    .unwrap();
                signed.fetch_add(1, Ordering::SeqCst);
                signed_tuple(headers)
            })
        })
        .collect::<Vec<_>>();
    let mut from_runtime = Vec::with_capacity(handles.len());
    for handle in handles {
        from_runtime.push(handle.await.unwrap());
    }
    let ticks = ticker.await.unwrap();

    // A signature allocated at the ceiling waited at least a millisecond first.
    let at_ceiling = from_runtime
        .iter()
        .filter(|(timestamp, _, observed_at_ms)| {
            *timestamp + 100 >= *observed_at_ms + MAX_SIGNING_FUTURE_SKEW_MS
        })
        .count();
    assert!(
        at_ceiling > 0,
        "the burst never reached the signing ceiling"
    );
    assert!(
        ticks >= at_ceiling / 100,
        "signing backpressure stalled current-thread Tokio: {ticks} timer ticks across {at_ceiling} signatures at the ceiling"
    );

    tuples.extend(from_runtime);
    assert_eq!(tuples.len(), FROM_THREADS + FROM_RUNTIME);
    let timestamps = tuples.iter().map(|item| item.0).collect::<HashSet<_>>();
    let signatures = tuples.iter().map(|item| &item.1).collect::<HashSet<_>>();
    assert_eq!(
        timestamps.len(),
        FROM_THREADS + FROM_RUNTIME,
        "duplicate signing timestamp"
    );
    assert_eq!(
        signatures.len(),
        FROM_THREADS + FROM_RUNTIME,
        "duplicate authentication tuple"
    );
    for (timestamp, _, observed_at_ms) in &tuples {
        assert!(
            *timestamp <= *observed_at_ms + MAX_SIGNING_FUTURE_SKEW_MS,
            "signed timestamp {timestamp} exceeded its per-request bounded ceiling {}",
            *observed_at_ms + MAX_SIGNING_FUTURE_SKEW_MS
        );
    }

    let later = credentials
        .sign_request_async(
            "GET",
            "https://api.example.test/auth.v1.AuthService/Me",
            b"",
            None,
        )
        .await
        .unwrap()[HEADER_TIMESTAMP]
        .parse::<u64>()
        .unwrap();
    let wall_clock_later = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    assert!(later <= wall_clock_later + MAX_SIGNING_FUTURE_SKEW_MS);
}

#[tokio::test]
async fn l2_scale_dependent_public_paths_fail_closed_without_catalogs() {
    let client = Client::new(Config {
        api_url: "http://127.0.0.1:1".into(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();

    // Display-symbol Connect paths must fail at catalog resolution first.
    // Guessing a symbol_id (or skipping to scale 8) would send a fabricated wire id.
    let orderbook_err = client
        .orderbook
        .get("ETH-USDT", Some(50))
        .await
        .expect_err("orderbook must not guess a symbol_id");
    assert!(matches!(orderbook_err, Error::Validation(_)));
    assert!(
        orderbook_err.to_string().contains("unknown symbol"),
        "{orderbook_err}"
    );

    // An explicit symbol_id still cannot format quantities without a catalog scale.
    let trades_err = client
        .market_data
        .get_trades_with(polyester::models::GetTradesOpts {
            symbol_id: Some(2),
            ..Default::default()
        })
        .await
        .expect_err("explicit symbol id must still require a known scale");
    assert!(matches!(trades_err, Error::Validation(_)));
    assert!(trades_err.to_string().contains("catalog quantity scale"));
}

#[tokio::test]
async fn digit_only_public_subaccount_id_uses_canonical_base58_on_order_wire() {
    use buffa::Message as _;
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
    use polyester::proto::orders::v1::{CreateOrderRequest, CreateOrderResponse};
    use polyester::{Price, Quantity};
    use std::sync::Mutex;

    let observed = Arc::new(Mutex::new(None));
    let observed_handler = observed.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path == "/orders.v1.OrdersService/CreateOrder" {
            let decoded = CreateOrderRequest::decode_from_slice(&request.body).unwrap();
            *observed_handler.lock().unwrap() = decoded.subaccount_id;
            return connect_proto_ok(&CreateOrderResponse {
                order_id: 9,
                client_order_id: "scope-wire".into(),
                ..Default::default()
            });
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scope-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some("5".into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    client
        .orders
        .create({
            let mut p = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
            p.quantity = Some(
                Quantity::from_scaled(
                    1,
                    Some(8),
                    QuantityDomain::OrderBase,
                    Some("BTC-USDT".into()),
                    Some(1),
                )
                .unwrap(),
            );
            p.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).unwrap());
            p.time_in_force = Some(CreateTimeInForce::Gtc);
            p.client_order_id = Some("scope-wire".into());
            p
        })
        .await
        .unwrap();

    assert_eq!(*observed.lock().unwrap(), Some(4));
}

#[tokio::test]
async fn singular_mutation_default_connect_responses_fail_closed() {
    use polyester::models::{
        CreateInternalTransferParams, CreateOrderParams, CreateOrderType, CreateSide,
        CreateTimeInForce, CreateTradingWithdrawParams, CreateTriggerParams, CreateTriggerType,
    };
    use polyester::proto::chain::withdraw::v1::CreateTradingWithdrawResponse;
    use polyester::proto::marketdata::v1::GetCandlesResponse;
    use polyester::proto::orders::v1::CreateOrderResponse;
    use polyester::proto::transfer::v1::CreateInternalTransferResponse;
    use polyester::proto::triggers::v1::CreateTriggerResponse;
    use polyester::{AssetAmount, Price, Quantity};

    let server = MockHttpServer::spawn(|request| match request.path.as_str() {
        "/orders.v1.OrdersService/CreateOrder" => connect_proto_ok(&CreateOrderResponse::default()),
        "/triggers.v1.TriggersService/CreateTrigger" => {
            connect_proto_ok(&CreateTriggerResponse::default())
        }
        "/chain.withdraw.v1.WithdrawService/CreateTradingWithdraw" => {
            connect_proto_ok(&CreateTradingWithdrawResponse::default())
        }
        "/transfer.v1.InternalTransferService/CreateInternalTransfer" => {
            connect_proto_ok(&CreateInternalTransferResponse::default())
        }
        "/marketdata.v1.MarketDataService/GetCandles" => {
            connect_proto_ok(&GetCandlesResponse::default())
        }
        _ => hardening_support::HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("fault-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "assets": [{
                "asset": "BTC",
                "quantity_scale": 8,
                "market_data_volume_scale": 8
            }],
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_asset": "BTC",
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();

    let quantity = Quantity::from_scaled(
        1,
        Some(8),
        QuantityDomain::OrderBase,
        Some("BTC-USDT".into()),
        Some(1),
    )
    .unwrap();
    let price = Price::from_ticks(1, Some("BTC-USDT".into())).unwrap();
    let order_error = client
        .orders
        .create({
            let mut p = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
            p.quantity = Some(quantity.clone());
            p.price = Some(price.clone());
            p.time_in_force = Some(CreateTimeInForce::Gtc);
            p.client_order_id = Some("fault-order".into());
            p.subaccount_id = Some(4);
            p
        })
        .await
        .unwrap_err();
    assert!(matches!(order_error, Error::ResponseContract { .. }));

    let trigger_error = client
        .triggers
        .create({
            let mut p = CreateTriggerParams::new(
                "BTC-USDT",
                CreateTriggerType::StopLoss,
                CreateSide::Sell,
                CreateOrderType::Market,
                quantity,
            );
            p.trigger_price = Some(price);
            p.subaccount_id = Some(4);
            p.client_trigger_id = "fault-trigger".into();
            p
        })
        .await
        .unwrap_err();
    assert!(matches!(trigger_error, Error::Transport(_)));

    let amount = AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
    let withdraw_error = client
        .withdraw
        .create_to_funding(CreateTradingWithdrawParams {
            asset_id: 7,
            amount: amount.clone(),
            payload_signature: vec![1],
            destination_address: String::new(),
            idempotency_key: "fault-withdraw".into(),
            amount_scale: Some(18),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: 1,
        })
        .await
        .unwrap_err();
    assert!(matches!(withdraw_error, Error::Transport(_)));

    let transfer_error = client
        .internal_transfers
        .create(CreateInternalTransferParams {
            asset_id: 7,
            quantity: amount,
            idempotency_key: "fault-transfer".into(),
            subaccount_id: Some(4),
            destination_account_id: Some("2".into()),
            destination_subaccount_id: None,
            destination_smart_account_address: None,
            quantity_scale: Some(18),
        })
        .await
        .unwrap_err();
    assert!(matches!(transfer_error, Error::Transport(_)));

    assert!(
        client
            .market_data
            .get_current_candle("BTC-USDT", "1m")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn l2_invalid_client_order_and_trigger_ids_fail_closed_without_hitting_connect() {
    use polyester::models::{
        CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce, CreateTriggerParams,
        CreateTriggerType,
    };
    use polyester::{Price, Quantity, QuantityDomain};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let http = MockHttpServer::spawn(move |_req| {
        hits_cb.fetch_add(1, Ordering::SeqCst);
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();

    let quantity = Quantity::from_scaled(
        1,
        Some(8),
        QuantityDomain::OrderBase,
        Some("BTC-USDT".into()),
        Some(1),
    )
    .unwrap();
    let price = Price::from_ticks(1, Some("BTC-USDT".into())).unwrap();

    let order_err = client
        .orders
        .create({
            let mut p = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
            p.quantity = Some(quantity.clone());
            p.price = Some(price.clone());
            p.time_in_force = Some(CreateTimeInForce::Gtc);
            p.client_order_id = Some("bad id!".into());
            p
        })
        .await
        .expect_err("invalid client_order_id must fail locally");
    assert!(
        matches!(order_err, Error::Validation(_)),
        "want Validation, got {order_err:?}"
    );

    let trigger_err = client
        .triggers
        .create({
            let mut p = CreateTriggerParams::new(
                "BTC-USDT",
                CreateTriggerType::StopLoss,
                CreateSide::Sell,
                CreateOrderType::Market,
                quantity,
            );
            p.trigger_price = Some(price);
            p.client_trigger_id = "x".repeat(37);
            p
        })
        .await
        .expect_err("oversized client_trigger_id must fail locally");
    assert!(
        matches!(trigger_err, Error::Validation(_)),
        "want Validation, got {trigger_err:?}"
    );

    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "invalid correlation ids must not reach Connect"
    );
}

#[tokio::test]
async fn managed_overview_overflow_terminates_receiver_callback_task_and_socket() {
    use polyester::proto::marketoverview::v1::{
        ListMarketOverviewResponse, MarketOverview, MarketOverviewBatch,
    };
    use polyester::services::MarketOverviewCreateSubscriptionOptions;
    use std::sync::atomic::AtomicBool;

    let active = Arc::new(AtomicUsize::new(0));
    let publication = MarketOverviewBatch {
        markets: vec![MarketOverview {
            symbol_id: 1,
            last_price_ticks: 1,
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let ws = MockWsServer::spawn_centrifugo_publication_flood_after_handshake(
        active.clone(),
        publication,
        500,
    )
    .await;
    let http = MockHttpServer::spawn(|request| {
        if request.path == "/marketoverview.v1.MarketOverviewService/ListMarketOverview" {
            connect_proto_ok(&ListMarketOverviewResponse::default())
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let mut subscription = client
        .market_overview
        .create_subscription(MarketOverviewCreateSubscriptionOptions::default())
        .await
        .unwrap();
    let callback_fired = Arc::new(AtomicBool::new(false));
    let callback_flag = callback_fired.clone();
    subscription.set_on_error(move |error| {
        assert!(matches!(error, Error::QueueOverflow(_)));
        callback_flag.store(true, Ordering::SeqCst);
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while subscription.err().is_none() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        matches!(subscription.err(), Some(Error::QueueOverflow(_))),
        "unexpected terminal state: err={:?}, active={}, connects={}",
        subscription.err(),
        active.load(Ordering::SeqCst),
        ws.connects.load(Ordering::SeqCst)
    );
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), subscription.updates().recv())
            .await
            .expect("overflow must terminate the managed receiver");
        if next.is_none() {
            break;
        }
    }
    assert!(matches!(subscription.err(), Some(Error::QueueOverflow(_))));
    assert!(callback_fired.load(Ordering::SeqCst));
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_secs(2),
    )
    .await;

    // Explicit close remains idempotent after overflow and cannot leave the
    // already-terminated transport alive.
    subscription.close();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn managed_orderbook_overflow_terminates_receiver_callback_task_and_socket() {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta};
    use polyester::services::CreateSubscriptionOptions;
    use std::sync::atomic::AtomicBool;

    let active = Arc::new(AtomicUsize::new(0));
    let publication = OrderBookDelta {
        symbol_id: 1,
        book_seq_start: 1,
        book_seq_end: 2,
        ..Default::default()
    }
    .encode_to_vec();
    let ws = MockWsServer::spawn_centrifugo_publication_flood_after_handshake(
        active.clone(),
        publication,
        500,
    )
    .await;
    let http = MockHttpServer::spawn(|request| {
        if request.path == "/orderbook.v1.OrderbookService/GetOrderBook" {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 1,
                ..Default::default()
            })
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    let mut subscription = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            symbol_id: Some(1),
            depth: Some(50),
            bucket: None,
        })
        .await
        .unwrap();
    let callback_fired = Arc::new(AtomicBool::new(false));
    let callback_flag = callback_fired.clone();
    subscription.set_on_error(move |error| {
        assert!(matches!(error, Error::QueueOverflow(_)));
        callback_flag.store(true, Ordering::SeqCst);
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while subscription.err().is_none() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        matches!(subscription.err(), Some(Error::QueueOverflow(_))),
        "unexpected terminal state: err={:?}, active={}, connects={}",
        subscription.err(),
        active.load(Ordering::SeqCst),
        ws.connects.load(Ordering::SeqCst)
    );
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), subscription.updates().recv())
            .await
            .expect("overflow must terminate the managed receiver");
        if next.is_none() {
            break;
        }
    }
    assert!(callback_fired.load(Ordering::SeqCst));
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_secs(2),
    )
    .await;
    subscription.close();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

/// Seeds a managed orderbook subscription from a snapshot with the given
/// `available`, sends one delta at seq 2, and returns the first view at seq 2.
/// Every earlier view must carry the snapshot's `available` too.
async fn orderbook_view_after_delta(
    snapshot_available: bool,
    reset: bool,
) -> polyester::models::OrderbookData {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    use polyester::services::CreateSubscriptionOptions;

    let publication = OrderBookDelta {
        symbol_id: 1,
        book_seq_start: 1,
        book_seq_end: 2,
        reset,
        bids: vec![PriceLevel {
            price_ticks: 2_000_000_000,
            qty_scaled: 300,
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let ws =
        MockWsServer::spawn_centrifugo_publication_after_handshake(Arc::default(), publication)
            .await;
    let http = MockHttpServer::spawn(move |request| {
        if request.path == "/orderbook.v1.OrderbookService/GetOrderBook" {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 1,
                available: snapshot_available,
                ..Default::default()
            })
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    let mut subscription = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            symbol_id: Some(1),
            depth: Some(50),
            bucket: None,
        })
        .await
        .unwrap();
    let view = loop {
        let view = tokio::time::timeout(Duration::from_secs(5), subscription.updates().recv())
            .await
            .expect("orderbook update deadline")
            .expect("orderbook stream open");
        if view.book_seq == "2" {
            break view;
        }
        assert_eq!(view.available, snapshot_available, "seed view {view:?}");
    };
    subscription.close();
    view
}

/// The managed book reports its snapshot's wire `available` on every update;
/// neither levels nor a reset delta make an unavailable book available.
#[tokio::test]
async fn l2_orderbook_subscription_carries_the_snapshot_availability() {
    for snapshot_available in [true, false] {
        for reset in [true, false] {
            let view = orderbook_view_after_delta(snapshot_available, reset).await;
            assert_eq!(view.bids.len(), 1, "delta applied (reset={reset})");
            assert_eq!(
                view.available, snapshot_available,
                "snapshot available={snapshot_available}, reset={reset}"
            );
        }
    }
}

#[test]
fn l2_contradictory_catalog_refreshes_fail_atomically_through_public_manager() {
    let client = Client::new(Config {
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    let err = client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [
                {"symbol": "ETH-USDT", "symbol_id": 2, "base_quantity_scale": 6},
                {"symbol": "SOL-USDT", "symbol_id": 2, "base_quantity_scale": 8}
            ]
        }))
        .unwrap_err();
    assert!(err.to_string().contains("symbol_id 2"));
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("ETH-USDT"),
        None
    );
}

#[tokio::test]
async fn l2_rest_and_realtime_market_trades_attach_catalog_scale_metadata() {
    use polyester::proto::marketdata::v1::{GetTradesResponse, MarketTrade as ProtoMarketTrade};

    for (symbol, symbol_id, scale, expected) in
        [("ETH-USDT", 2_u32, 6_u32, "1"), ("BTC-USDT", 1, 8, "0.01")]
    {
        let proto_trade = ProtoMarketTrade {
            symbol_id,
            match_id: 7,
            qty_scaled: 1_000_000,
            price_ticks: 1_000_000,
            ..Default::default()
        };
        let response = GetTradesResponse {
            trades: vec![proto_trade.clone()],
            ..Default::default()
        };
        let http = MockHttpServer::spawn(move |req| {
            if req.path == GET_TRADES_PATH {
                connect_proto_ok(&response)
            } else {
                hardening_support::HttpScript::NotFound
            }
        })
        .await;
        let active = Arc::new(AtomicUsize::new(0));
        let batch = polyester::proto::marketdata::v1::MarketTradeBatch {
            events: vec![proto_trade.clone()],
            ..Default::default()
        };
        let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(
            active,
            batch.encode_to_vec(),
        )
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            ws_url: ws.ws_url(),
            hydrate_catalogs: false,
            timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": symbol,
                    "symbol_id": symbol_id,
                    "base_quantity_scale": scale
                }]
            }))
            .unwrap();

        let rest = client
            .market_data
            .get_trades(symbol, Some(1))
            .await
            .unwrap();
        assert_eq!(
            rest.trades[0].qty.as_ref().unwrap().format(None).unwrap(),
            expected
        );

        let mut realtime = client.market_data.subscribe_trades(symbol).await.unwrap();
        let streamed = tokio::time::timeout(Duration::from_secs(2), realtime.recv_result())
            .await
            .expect("realtime publication deadline")
            .unwrap()
            .expect("realtime publication");
        assert_eq!(
            streamed.qty.as_ref().unwrap().format(None).unwrap(),
            expected
        );
        realtime.close();
    }
}

fn private_rt(ws: &MockWsServer, http: &MockHttpServer, timeout: Duration) -> RealtimeClient {
    RealtimeClient::with_timeout(
        ws.ws_url(),
        http.base_url(),
        Some(test_credentials("ak_test", TEST_KEY)),
        None,
        timeout,
    )
}

async fn subscribe_raw_err(rt: &RealtimeClient, channel: &str) -> Error {
    match rt.subscribe_raw(channel).await {
        Err(err) => err,
        Ok(_) => panic!("subscribe_raw({channel}) must fail"),
    }
}

#[tokio::test]
async fn l2_token_headers_then_stalled_body_times_out_via_subscribe_raw() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![("Transfer-Encoding".into(), "chunked".into())],
                stall,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "elapsed {:?} exceeded deadline+slack; body likely outside timeout",
        started.elapsed()
    );
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("timed out") || msg.contains("timeout"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn l2_token_no_headers_times_out_via_subscribe_raw() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::NeverRespond {
                stall: Duration::from_secs(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("timed out") || msg.contains("timeout"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_slow_drip_obeys_one_overall_deadline() {
    let timeout = Duration::from_millis(180);
    let chunks = br#"{"token":"connection-ok"}"#.iter().map(|byte| vec![*byte]).collect::<Vec<_>>();
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::SlowDrip {
                status: 200,
                headers: vec![("Content-Type".into(), "application/json".into())],
                chunks: chunks.clone(),
                inter_chunk_delay: Duration::from_millis(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, timeout);
    let started = Instant::now();
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "slow-drip token body escaped the overall deadline"
    );
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("timeout") || message.contains("timed out"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_content_length_65537_rejected_via_subscribe_raw() {
    let body = vec![b'x'; 65_537];
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body: body.clone(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("too large") || msg.contains("64"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_chunked_oversized_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::ChunkedBody {
                status: 200,
                total_bytes: 70_000,
                chunk_size: 4096,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("too large") || msg.contains("64"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_empty_token_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"token":""}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("missing token") || msg.contains("token"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_malformed_json_rejected_via_subscribe_raw() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![("Content-Type".into(), "application/json".into())],
                body: b"{not-json".to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws = MockWsServer::spawn_hang_after_accept().await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("invalid token") || msg.contains("json") || msg.contains("parse"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_token_http_403_maps_to_structured_permission_denied() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == "/v1/rt/token" {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"token":"connection-ok"}"#.to_vec(),
            }
        } else if req.path.starts_with("/v1/rt/subscribe") {
            hardening_support::HttpScript::Json {
                status: 403,
                body: br#"{"code":"permission_denied","message":"missing transfer:read"}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active).await;
    let rt = private_rt(&ws, &http, Duration::from_secs(2));
    let err = subscribe_raw_err(&rt, "private:auth:transfers:acct:proto").await;
    match err {
        Error::PermissionDenied {
            message,
            status,
            code,
            context,
            endpoint,
            ..
        } => {
            assert_eq!(message, "missing transfer:read");
            assert_eq!(status, 403);
            assert_eq!(code, "permission_denied");
            assert!(context.contains("private:auth:transfers:acct:proto"));
            // The endpoint keeps no query; the context names the channel.
            assert!(endpoint.ends_with("/v1/rt/subscribe"), "{endpoint}");
        }
        other => panic!("expected structured permission error, got {other:?}"),
    }
}

#[tokio::test]
async fn l2_jsonrpc_headers_then_stalled_body_times_out() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 200,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Transfer-Encoding".into(), "chunked".into()),
        ],
        stall,
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout).expect("rpc");
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("stalled JSON-RPC body must timeout");
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    assert!(err.to_string().contains("timeout"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_no_headers_times_out() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::NeverRespond {
        stall: Duration::from_secs(30),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout).expect("rpc");
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("no-headers JSON-RPC must timeout");
    assert!(started.elapsed() < timeout + Duration::from_millis(800));
    assert!(err.to_string().contains("timeout"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_slow_drip_obeys_one_overall_deadline() {
    let timeout = Duration::from_millis(180);
    let chunks = br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#
        .iter()
        .map(|byte| vec![*byte])
        .collect::<Vec<_>>();
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::SlowDrip {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        chunks: chunks.clone(),
        inter_chunk_delay: Duration::from_millis(30),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), timeout).expect("rpc");
    let started = Instant::now();
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("slow-drip JSON-RPC body must time out");
    assert!(
        started.elapsed() < timeout + Duration::from_millis(800),
        "slow-drip JSON-RPC body escaped the overall deadline"
    );
    assert!(err.to_string().to_ascii_lowercase().contains("timeout"));
}

#[tokio::test]
async fn l2_jsonrpc_rejects_oversized_and_bad_envelope() {
    let http = MockHttpServer::spawn(|req| {
        if req.path.contains("big") {
            let body = vec![b'x'; 2 * 1024 * 1024];
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body,
            }
        } else if req.path.contains("ver") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"1.0","id":1,"result":1}"#.to_vec(),
            }
        } else if req.path.contains("noid") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"2.0","result":1}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-1,"message":"x"}}"#
                    .to_vec(),
            }
        }
    })
    .await;
    let rpc =
        JsonRpcClient::new(format!("{}/ok", http.base_url()), Duration::from_secs(2)).expect("rpc");
    let err = rpc.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().contains("both result and error"), "{err}");

    let big = JsonRpcClient::new(format!("{}/big", http.base_url()), Duration::from_secs(2))
        .expect("rpc");
    let err = big.request("eth_call", json!([])).await.unwrap_err();
    assert!(
        err.to_string().contains("exceeds") || err.to_string().contains("read body"),
        "{err}"
    );

    let ver = JsonRpcClient::new(format!("{}/ver", http.base_url()), Duration::from_secs(2))
        .expect("rpc");
    let err = ver.request("eth_call", json!([])).await.unwrap_err();
    assert!(
        err.to_string().to_ascii_lowercase().contains("jsonrpc") || err.to_string().contains("2.0"),
        "{err}"
    );

    let noid = JsonRpcClient::new(format!("{}/noid", http.base_url()), Duration::from_secs(2))
        .expect("rpc");
    let err = noid.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("id"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_rejects_all_remaining_invalid_envelopes() {
    let http = MockHttpServer::spawn(|req| {
        let body = if req.path.contains("missing-version") {
            br#"{"id":1,"result":1}"#.to_vec()
        } else if req.path.contains("wrong-id") {
            br#"{"jsonrpc":"2.0","id":999,"result":1}"#.to_vec()
        } else if req.path.contains("neither") {
            br#"{"jsonrpc":"2.0","id":1}"#.to_vec()
        } else {
            br#"{"jsonrpc":"2.0","id":1,"error":"not-an-object"}"#.to_vec()
        };
        hardening_support::HttpScript::Json { status: 200, body }
    })
    .await;

    for path in ["missing-version", "wrong-id", "neither", "malformed-error"] {
        let rpc = JsonRpcClient::new(
            format!("{}/{path}", http.base_url()),
            Duration::from_secs(2),
        )
        .expect("rpc");
        let err = rpc.request("eth_call", json!([])).await.expect_err(path);
        let message = err.to_string().to_ascii_lowercase();
        match path {
            "missing-version" => assert!(message.contains("version"), "{err}"),
            "wrong-id" => assert!(message.contains("id"), "{err}"),
            "neither" => assert!(
                message.contains("result") || message.contains("error"),
                "{err}"
            ),
            "malformed-error" => assert!(message.contains("object"), "{err}"),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn l2_jsonrpc_25_concurrent_reordered_responses_succeed() {
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Buffer 25 requests, then reply in reverse id order (reorder-safe client).
    let buffer: Arc<Mutex<Vec<(tokio::net::TcpStream, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let buffer_h = buffer.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind concurrent rpc");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let buffer = buffer_h.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                let n = match stream.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => return,
                };
                if hardening_support::is_port_probe(&buf[..n]) {
                    return;
                }
                let raw = String::from_utf8_lossy(&buf[..n]);
                let body = raw.split("\r\n\r\n").nth(1).unwrap_or("{}");
                let v: serde_json::Value = serde_json::from_str(body).unwrap_or(json!({}));
                let id = v.get("id").and_then(|i| i.as_u64()).unwrap_or(0);
                let batch = {
                    let mut guard = buffer.lock().unwrap();
                    guard.push((stream, id));
                    if guard.len() == 25 {
                        Some(std::mem::take(&mut *guard))
                    } else {
                        None
                    }
                };
                if let Some(mut batch) = batch {
                    batch.sort_by_key(|b| std::cmp::Reverse(b.1));
                    for (mut stream, id) in batch {
                        let resp = json!({"jsonrpc":"2.0","id":id,"result":format!("ok-{id}")});
                        let body = serde_json::to_vec(&resp).unwrap();
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(&body).await;
                    }
                }
            });
        }
    });

    let rpc = JsonRpcClient::new(format!("http://{addr}"), Duration::from_secs(5)).expect("rpc");
    let mut tasks = Vec::new();
    for _ in 0..25 {
        let rpc = rpc.clone();
        tasks.push(tokio::spawn(async move {
            rpc.request("eth_chainId", json!([])).await
        }));
    }
    let mut ok = 0;
    for t in tasks {
        let result = t.await.expect("join").expect("rpc ok");
        assert!(result.as_str().unwrap_or("").starts_with("ok-"));
        ok += 1;
    }
    assert_eq!(ok, 25);
}

#[tokio::test]
async fn l2_jsonrpc_success_path_still_works() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2)).expect("rpc");
    let result = rpc.request("eth_chainId", json!([])).await.expect("ok");
    assert_eq!(result, json!("0x1"));
}

#[tokio::test]
async fn l2_jsonrpc_chunked_over_1mib_rejected() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::ChunkedBody {
        status: 200,
        total_bytes: 1_100_000,
        chunk_size: 16_384,
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(5)).expect("rpc");
    let err = rpc
        .request("eth_chainId", json!([]))
        .await
        .expect_err("chunked >1MiB must fail");
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("exceed") || msg.contains("read body") || msg.contains("too large"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_jsonrpc_malformed_json_rejected() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: b"{broken".to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2)).expect("rpc");
    let err = rpc.request("eth_chainId", json!([])).await.unwrap_err();
    assert!(
        err.to_string().to_ascii_lowercase().contains("json"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_jsonrpc_error_object_returns_transport_error() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2)).expect("rpc");
    let err = rpc.request("eth_call", json!([])).await.unwrap_err();
    assert!(err.to_string().contains("boom"), "{err}");
}

#[tokio::test]
async fn l2_jsonrpc_null_result_is_preserved() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"jsonrpc":"2.0","id":1,"result":null}"#.to_vec(),
    })
    .await;
    let rpc = JsonRpcClient::new(http.base_url(), Duration::from_secs(2)).expect("rpc");
    let result = rpc.request("eth_call", json!([])).await.expect("null ok");
    assert!(result.is_null(), "expected null result, got {result}");
}

#[tokio::test]
async fn l2_close_aborts_subscription_promptly_against_local_ws() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    sub.close();
    wait_until(|| !sub.is_alive(), Duration::from_millis(750)).await;
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "close lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_drop_idle_subscription_peer_closes_promptly() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    let started = Instant::now();
    drop(sub);
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(started.elapsed() < Duration::from_millis(750));
}

#[tokio::test]
async fn l2_hundred_sub_close_returns_conn_count_to_baseline() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let mut subs = Vec::new();
    for _ in 0..100 {
        subs.push(rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe"));
    }
    wait_until(
        || active.load(Ordering::SeqCst) >= 100,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    for sub in &subs {
        sub.close();
    }
    drop(subs);
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "100-sub close soak exceeded 750ms: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_cancel_subscribe_raw_during_token_body_stall_cleans_peers() {
    let stall = Duration::from_secs(30);
    let http = MockHttpServer::spawn(move |req| {
        if req.path.starts_with("/v1/rt/") {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![("Transfer-Encoding".into(), "chunked".into())],
                stall,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let ws_active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_hang_after_accept_counted(ws_active.clone()).await;
    let rt = private_rt(&ws, &http, Duration::from_secs(30));

    let join = tokio::spawn(async move { rt.subscribe_raw(PRIVATE_CHANNEL).await });
    // Arming wait only: under CI load (shared with the 10k auth soak) the
    // subscribe task may take longer than 2s to open the token HTTP request.
    wait_until(
        || http.in_flight.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    join.abort();
    let _ = join.await;
    wait_until(
        || http.in_flight.load(Ordering::SeqCst) == 0 && ws_active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "cancel during token stall lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_cancel_subscribe_raw_during_centrifugo_wait_cleans_peers() {
    let ws_active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_hang_after_accept_counted(ws_active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let join = tokio::spawn(async move { rt.subscribe_raw(PUBLIC_CHANNEL).await });
    wait_until(
        || ws_active.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    join.abort();
    let _ = join.await;
    wait_until(
        || ws_active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "cancel during Centrifugo wait lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_close_during_reconnect_backoff_no_extra_connect() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.expect("subscribe");
    wait_until(
        || ws.connects.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    // After forced disconnect, the client enters its jittered reconnect backoff.
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_secs(2),
    )
    .await;
    let connects_before = ws.connects.load(Ordering::SeqCst);
    sub.close();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let connects_after = ws.connects.load(Ordering::SeqCst);
    assert_eq!(
        connects_before, connects_after,
        "close during reconnect backoff must not start an extra connect ({connects_before} -> {connects_after})"
    );
    assert!(!sub.is_alive());
}

#[tokio::test]
async fn l2_realtime_terminal_error_is_callback_and_result_observable() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let mut sub = rt
        .subscribe_proto_with_options(PUBLIC_CHANNEL, |bytes| Ok(bytes.to_vec()), false)
        .await
        .expect("initial handshake");

    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel();
    sub.set_on_error(move |err| {
        let _ = error_tx.send(err);
    });

    let callback_error = tokio::time::timeout(Duration::from_millis(750), error_rx.recv())
        .await
        .expect("error callback must be prompt")
        .expect("error callback channel");
    let callback_message = callback_error.to_string().to_ascii_lowercase();
    assert!(
        callback_message.contains("closed")
            || callback_message.contains("reset")
            || callback_message.contains("eof"),
        "unexpected callback error: {callback_error}"
    );

    let result_error = tokio::time::timeout(Duration::from_millis(750), sub.recv_result())
        .await
        .expect("recv_result must unblock")
        .expect_err("terminal feed failure must not look like clean EOF");
    let result_message = result_error.to_string().to_ascii_lowercase();
    assert!(
        result_message.contains("closed")
            || result_message.contains("reset")
            || result_message.contains("eof"),
        "unexpected recv_result error: {result_error}"
    );
    assert!(!sub.is_alive());
    sub.close();
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
}

#[tokio::test]
async fn l2_realtime_oversized_binary_message_fails_closed() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_oversized_after_handshake(
        active.clone(),
        MAX_REALTIME_MESSAGE_BYTES + 1,
    )
    .await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let sub = rt
        .subscribe_proto_with_options(PUBLIC_CHANNEL, |bytes| Ok(bytes.to_vec()), false)
        .await
        .expect("initial handshake");
    wait_until(
        || sub.err().is_some() && !sub.is_alive(),
        Duration::from_secs(3),
    )
    .await;
    let error = sub.err().expect("oversized message error").to_string();
    let error = error.to_ascii_lowercase();
    assert!(
        error.contains("size")
            || error.contains("too long")
            || error.contains("capacity")
            || error.contains("space limit"),
        "unexpected oversized-message error: {error}"
    );
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
}

#[tokio::test]
async fn l2_close_during_reconnect_snapshot_retry_cancels_fetch_and_socket() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_once_then_idle(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    let (stalled_tx, stalled_rx) = tokio::sync::oneshot::channel::<()>();
    let stalled_rx = Arc::new(std::sync::Mutex::new(Some(stalled_rx)));
    let fetch_stalled_rx = stalled_rx.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_bytes| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return async { Ok("initial".to_owned()) }.boxed();
            }
            if attempt == 1 {
                return async { Err(Error::transport("retry once")) }.boxed();
            }
            let stalled = fetch_stalled_rx
                .lock()
                .expect("stalled snapshot lock")
                .take()
                .expect("only one stalled retry");
            async move {
                let _ = stalled.await;
                Ok("late snapshot".to_owned())
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_snapshot, _pending| {}),
        apply_live_publications: Arc::new(|_publications| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
        on_error: None,
    });

    sts.start().await.expect("initial snapshot");
    wait_until(
        || attempts.load(Ordering::SeqCst) >= 3 && active.load(Ordering::SeqCst) == 1,
        Duration::from_secs(5),
    )
    .await;
    let started = Instant::now();
    sts.close();
    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        stalled_tx.send(()).is_err(),
        "closing during the second refresh attempt must drop the stalled fetch future"
    );
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "combined retry/cancellation cleanup lingered {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn l2_scale_format_and_catalog_reject_panic_boundary() {
    assert!(format_qty_scaled(1, 18).is_ok());
    assert!(format_qty_scaled(1, MAX_PROTOCOL_SCALE).is_ok());
    for scale in [37u32, 65534, 65535, 65536, u32::MAX] {
        assert!(
            format_qty_scaled(1, scale).is_err(),
            "scale {scale} must err"
        );
        assert!(format_ledger_u128("1", scale).is_err());
        let formatted = Quantity::from_scaled(1, Some(8), QuantityDomain::OrderBase, None, None)
            .unwrap()
            .format(Some(scale));
        assert!(formatted.is_err(), "Quantity::format({scale}) must err");
        let construct =
            Quantity::from_scaled(1, Some(scale), QuantityDomain::OrderBase, None, None);
        assert!(
            construct.is_err(),
            "from_scaled with scale {scale} must err at boundary"
        );
        let panic = std::panic::catch_unwind(|| {
            let _ = format_qty_scaled(1, scale);
        });
        assert!(panic.is_ok(), "format must not panic at scale {scale}");
    }

    let catalogs = polyester::catalogs::Manager::new();
    let err = catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 65535
            }]
        }))
        .expect_err("catalog must reject panic-boundary scale");
    assert!(err.to_string().contains("scale"));
    assert_eq!(catalogs.base_quantity_scale_for_symbol("BTC-USDT"), None);

    let err = catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "ETH-USDT",
                "symbol_id": 4294967296u64,
                "base_quantity_scale": 8
            }]
        }))
        .expect_err("catalog must reject symbol_id above u32");
    assert!(err.to_string().contains("symbol_id") || err.to_string().contains("u32"));
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_http_500() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 500,
        body: br#"{"error":"nope"}"#.to_vec(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("HTTP 500 must fail closed");
    assert!(
        err.to_string().contains("catalog hydration failed"),
        "{err}"
    );
    assert!(client.catalogs_last_error().is_some());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        None
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_empty_body() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: Vec::new(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("empty body must fail closed");
    assert!(err.to_string().contains("catalog"), "{err}");
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_malformed_protobuf() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Content-Length".into(), "1".into()),
                ],
                // Field zero with an unsupported wire type cannot decode as a
                // protobuf GetSpotConfigResponse.
                body: vec![0x0f],
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("malformed protobuf must fail closed");
    assert!(!client.catalogs.is_ready(), "{err}");
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_fail_closed_on_malformed_config() {
    let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::Json {
        status: 200,
        body: br#"{"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":65535}]}"#
            .to_vec(),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(500),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("bad scale in config must fail closed");
    assert!(
        err.to_string().contains("scale") || err.to_string().contains("catalog"),
        "{err}"
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_rejects_oversized_content_length_response() {
    let body = vec![0u8; MAX_CONNECT_RESPONSE_BYTES + 1];
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Content-Length".into(), body.len().to_string()),
                ],
                body: body.clone(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("oversized catalog response must fail closed");
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("size")
            || message.contains("limit")
            || message.contains("resource exhausted")
            || message.contains("catalog"),
        "{err}"
    );
    assert!(!client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_wait_for_catalogs_rejects_oversized_chunked_response() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == SPOT_CONFIG_PATH {
            hardening_support::HttpScript::ChunkedBody {
                status: 200,
                total_bytes: MAX_CONNECT_RESPONSE_BYTES + 1,
                chunk_size: 64 * 1024,
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    client
        .wait_for_catalogs()
        .await
        .expect_err("oversized chunked catalog response must fail closed");
    assert!(!client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_some());
}

#[tokio::test]
async fn l2_concurrent_wait_for_catalogs_share_one_attempt() {
    let stall = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 500,
        headers: vec![("Content-Type".into(), "application/json".into())],
        stall,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_millis(200),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    let (r1, r2) = tokio::join!(client.wait_for_catalogs(), client.wait_for_catalogs());
    assert!(r1.is_err());
    assert!(r2.is_err());
    // Spot + zipper = 2 requests for one shared attempt (not 4).
    let n = http.requests.load(Ordering::SeqCst);
    assert!(
        n <= 2,
        "concurrent waiters must share one attempt; saw {n} HTTP requests"
    );
}

#[tokio::test]
async fn l2_snapshot_then_stream_start_survives_immediate_disconnect_repeatedly() {
    const ATTEMPTS: usize = 60;
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;

    for attempt in 1..=ATTEMPTS {
        let rt =
            RealtimeClient::with_timeout(ws.ws_url(), "", None, None, Duration::from_millis(500));
        let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
            client: rt,
            channel: PUBLIC_CHANNEL.into(),
            decode: Arc::new(|_b| Ok(1u8)),
            fetch_snapshot: Arc::new(|| async { Ok("initial".to_string()) }.boxed()),
            read_publication: Arc::new(|p| vec![p]),
            apply_snapshot: Arc::new(|_s, _p| {}),
            apply_live_publications: Arc::new(|_p| {}),
            max_buffered: 8,
            on_reconnect: None,
            on_snapshot_refresh: None,
            on_error: None,
        });

        let started = Instant::now();
        tokio::time::timeout(Duration::from_millis(750), sts.start())
            .await
            .unwrap_or_else(|_| panic!("attempt {attempt}/{ATTEMPTS}: start hung"))
            .unwrap_or_else(|err| {
                panic!("attempt {attempt}/{ATTEMPTS}: valid handshake failed: {err}")
            });
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "attempt {attempt}/{ATTEMPTS}: start exceeded its bound"
        );
        sts.close();
    }

    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "repeated immediate disconnects leaked sockets"
    );
}

#[tokio::test]
async fn l2_snapshot_then_stream_start_obeys_configured_deadline() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_hang_after_accept_counted(active.clone()).await;
    let configured_timeout = Duration::from_millis(150);
    let rt = RealtimeClient::with_timeout(ws.ws_url(), "", None, None, configured_timeout);
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(|| async { Ok("unreachable".to_string()) }.boxed()),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_s, _p| {}),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
        on_error: None,
    });

    let started = Instant::now();
    let err = tokio::time::timeout(Duration::from_millis(750), sts.start())
        .await
        .expect("configured startup deadline must terminate")
        .expect_err("stalled Centrifugo handshake must fail");
    assert!(
        started.elapsed() >= configured_timeout,
        "startup returned before its configured deadline"
    );
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "startup exceeded its configured deadline plus test slack"
    );
    assert!(
        err.to_string().to_ascii_lowercase().contains("timed out"),
        "unexpected startup error: {err}"
    );
    assert!(sts.is_disposed());
    assert!(sts.err().is_some());

    wait_until(
        || active.load(Ordering::SeqCst) == 0,
        Duration::from_millis(750),
    )
    .await;
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "startup timeout leaked the stalled websocket"
    );
}

#[tokio::test]
async fn l2_snapshot_then_stream_reconnect_snapshot_fail_sets_err_and_not_ready() {
    use std::sync::atomic::AtomicBool;

    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_disconnect_after_handshake(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    // Succeed until the initial start marks ready; fail on reconnect refresh.
    let fail_after_ready = Arc::new(AtomicBool::new(false));
    let fail_flag = fail_after_ready.clone();
    let reported_errors = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reported_errors_cb = reported_errors.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            fetch_attempts.fetch_add(1, Ordering::SeqCst);
            let fail = fail_flag.load(Ordering::SeqCst);
            async move {
                if fail {
                    Err(Error::transport("snapshot refresh failed"))
                } else {
                    Ok("initial".to_string())
                }
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_s, _p| {}),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
        on_error: Some(Arc::new(move |err| {
            reported_errors_cb
                .lock()
                .expect("reported errors")
                .push(err.to_string());
        })),
    });
    sts.start().await.expect("initial start");
    assert!(sts.is_ready());
    fail_after_ready.store(true, Ordering::SeqCst);
    // Wait for disconnect → reconnect → failing snapshot (with one retry) → fail-closed.
    wait_until(
        || sts.err().is_some() && !sts.is_ready(),
        Duration::from_secs(5),
    )
    .await;
    assert!(sts.err().is_some());
    assert!(!sts.is_ready());
    assert!(
        sts.is_disposed(),
        "exhausted snapshot recovery must terminate fail-closed"
    );
    let reported_errors = reported_errors.lock().expect("reported errors");
    assert!(
        reported_errors
            .iter()
            .any(|err| err.contains("snapshot refresh failed")),
        "snapshot refresh failure was not surfaced to on_error: {reported_errors:?}"
    );
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "expected initial success + reconnect retries"
    );
    sts.close();
}

#[tokio::test]
async fn l2_balances_list_connect_response_preserves_ledger_scaled_integer() {
    use polyester::codecs::format_ledger_u128;
    use polyester::proto::ledger::read::v1::GetBalancesRequest;
    use polyester::proto::ledger::read::v1::{
        AssetBalance as ProtoAssetBalance, GetBalancesResponse,
    };
    use polyester::proto::polyester::r#type::v1::U128;

    const ONE_POINT_FIVE_E18: u64 = 1_500_000_000_000_000_000;
    let fixture = GetBalancesResponse {
        balances: vec![ProtoAssetBalance {
            asset_id: 1,
            trading: U128 {
                hi: 0,
                lo: ONE_POINT_FIVE_E18,
                ..Default::default()
            }
            .into(),
            funding: U128 {
                hi: 0,
                lo: ONE_POINT_FIVE_E18,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let http = MockHttpServer::spawn(move |req| {
        if req.path == GET_BALANCES_PATH {
            connect_proto_ok(&fixture)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect("client");
    let list = client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .expect("balances.list");
    assert_eq!(list.balances.len(), 1);
    assert_eq!(list.balances[0].trading, "1500000000000000000");
    assert_eq!(list.balances[0].funding, "1500000000000000000");
    assert_eq!(
        format_ledger_u128(&list.balances[0].trading, 18).expect("format"),
        "1.5"
    );
}

#[tokio::test]
async fn l2_wait_for_order_trades_complete_polls_get_order_until_trade_sum_matches() {
    use polyester::proto::orders::v1::{GetOrderResponse, Order, OrderStatus, UserTrade};

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path != GET_ORDER_PATH {
            return hardening_support::HttpScript::NotFound;
        }
        let n = calls_h.fetch_add(1, Ordering::SeqCst) + 1;
        let order = Order {
            order_id: 1,
            symbol_id: 1,
            status: OrderStatus::Filled.into(),
            cum_qty_scaled: 100,
            ..Default::default()
        };
        let resp = if n == 1 {
            GetOrderResponse {
                order: Some(order).into(),
                trades: vec![],
                ..Default::default()
            }
        } else {
            GetOrderResponse {
                order: Some(order).into(),
                trades: vec![
                    UserTrade {
                        symbol_id: 1,
                        qty_scaled: 40,
                        fee_amount_e18: polyester::proto::polyester::r#type::v1::U128 {
                            hi: 0,
                            lo: 1,
                            ..Default::default()
                        }
                        .into(),
                        fee_asset: polyester::proto::orders::v1::FeeAsset::Base.into(),
                        referral_share_amount_e18: polyester::proto::polyester::r#type::v1::U128 {
                            hi: 0,
                            lo: 1,
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    },
                    UserTrade {
                        symbol_id: 1,
                        qty_scaled: 60,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        };
        connect_proto_ok(&resp)
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect("client");
    let result = client
        .orders
        .wait_for_order_trades_complete(
            polyester::models::OrderKey::OrderId("1".into()),
            Duration::from_secs(2),
        )
        .await
        .expect("wait complete");
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let cum = result
        .order
        .as_ref()
        .and_then(|o| o.cum_qty.as_ref())
        .map(|q| q.as_scaled())
        .expect("cum");
    let sum: i64 = result
        .trades
        .iter()
        .map(|t| t.qty.as_ref().map(|q| q.as_scaled()).unwrap_or(0))
        .sum();
    assert_eq!(cum, 100);
    assert_eq!(sum, 100);
    assert_eq!(result.trades[0].fee_asset, "base");
    assert_eq!(result.trades[0].fee_amount_e18, "1");
    assert_eq!(result.trades[0].referral_share_amount_e18, "1");
    assert!(!result.trades[0].fee_is_rebate);
}

#[tokio::test]
async fn l2_wait_for_order_trades_complete_enforces_overall_deadline() {
    let http = MockHttpServer::spawn(|req| {
        if req.path == GET_ORDER_PATH {
            hardening_support::HttpScript::HeadersThenStall {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/proto".into()),
                    ("Transfer-Encoding".into(), "chunked".into()),
                ],
                stall: Duration::from_secs(30),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(5),
        ..Default::default()
    })
    .expect("client");
    let helper_timeout = Duration::from_millis(250);
    let started = Instant::now();
    let err = client
        .orders
        .wait_for_order_trades_complete(
            polyester::models::OrderKey::OrderId("1".into()),
            helper_timeout,
        )
        .await
        .expect_err("helper deadline must cover the in-flight GetOrder call");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "helper exceeded overall deadline: {:?}",
        started.elapsed()
    );
    assert!(err.to_string().contains("timed out"), "{err}");
}

fn spot_config_fixture() -> polyester::proto::marketdata::v1::GetSpotConfigResponse {
    use polyester::proto::marketdata::v1::{GetSpotConfigResponse, PairConfig};
    GetSpotConfigResponse {
        assets: vec![polyester::proto::marketdata::v1::AssetConfig {
            asset: "BTC".into(),
            market_data_volume_scale: 8,
            ..Default::default()
        }],
        pairs: vec![PairConfig {
            symbol_id: 1,
            symbol: "BTC-USDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            base_quantity_scale: 8,
            reference_price_scale: 6,
            ..Default::default()
        }],
        ts_sec: 1,
        ..Default::default()
    }
}

fn zipper_config_fixture() -> polyester::proto::chain::zipper::v1::GetDepositWithdrawConfigResponse
{
    use polyester::proto::chain::zipper::v1::{AssetConfig, GetDepositWithdrawConfigResponse};
    GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        assets: vec![AssetConfig {
            asset: "USDT".into(),
            ledger_id: 99,
            quantity_scale: 6,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn l2_wait_for_catalogs_hydrates_spot_and_zipper_then_formats() {
    use polyester::proto::marketdata::v1::GetTradesResponse;
    use polyester::{Quantity, QuantityDomain};

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let trades = GetTradesResponse::default();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else if req.path == GET_TRADES_PATH {
            connect_proto_ok(&trades)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("hydrate");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
    assert_eq!(client.catalogs.symbol_id_for_symbol("BTC-USDT"), Some(1));
    assert_eq!(client.catalogs.ledger_id_for_asset("USDT"), Some(99));
    let qty = Quantity::from_scaled(100_000_000, Some(8), QuantityDomain::OrderBase, None, None)
        .expect("qty");
    assert_eq!(qty.format(None).expect("format"), "1");
    // Public symbol resolution path after hydrate.
    let _ = client
        .market_data
        .get_trades("BTC-USDT", Some(1))
        .await
        .expect("get_trades resolves hydrated symbol");
}

#[tokio::test]
async fn l2_wait_for_catalogs_no_headers_stalls_then_times_out() {
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::NeverRespond {
        stall: Duration::from_secs(30),
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout,
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let started = Instant::now();
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("no-headers must fail");
    assert!(started.elapsed() < timeout + Duration::from_millis(1200));
    assert!(err.to_string().contains("catalog"), "{err}");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        None
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_headers_then_body_stall_times_out() {
    let stall = Duration::from_secs(30);
    let timeout = Duration::from_millis(400);
    let http = MockHttpServer::spawn(move |_| hardening_support::HttpScript::HeadersThenStall {
        status: 200,
        headers: vec![
            ("Content-Type".into(), "application/proto".into()),
            ("Transfer-Encoding".into(), "chunked".into()),
        ],
        stall,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout,
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    let started = Instant::now();
    let err = client
        .wait_for_catalogs()
        .await
        .expect_err("body stall must fail");
    assert!(started.elapsed() < timeout + Duration::from_millis(1200));
    assert!(err.to_string().contains("catalog"), "{err}");
}

#[tokio::test]
async fn l2_batch_cancel_rejects_inconsistent_server_counts_through_public_service() {
    use polyester::models::BatchCancelItem;
    use polyester::proto::orders::v1::{
        BatchCancelOrdersResponse, BatchCancelResultItem as ProtoItem,
    };

    let response = BatchCancelOrdersResponse {
        results: vec![ProtoItem {
            status: polyester::proto::orders::v1::batch_cancel_result_item::Status::Accepted.into(),
            order_id: 9,
            ..Default::default()
        }],
        accepted_count: 0,
        rejected_count: 1,
        ..Default::default()
    };
    let http = MockHttpServer::spawn(move |req| {
        if req.path == "/orders.v1.OrdersService/BatchCancelOrders" {
            connect_proto_ok(&response)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");

    let err = client
        .orders
        .batch_cancel(
            vec![BatchCancelItem {
                key: polyester::models::OrderKey::OrderId("9".into()),
                symbol_id: None,
            }],
            None,
            Some("count-mismatch".into()),
        )
        .await
        .expect_err("service must reject an ambiguous batch result");
    assert!(err.to_string().contains("response counts"), "{err}");
}

#[tokio::test]
async fn l2_cancel_all_rejects_empty_and_unknown_status_through_public_service() {
    use polyester::proto::orders::v1::{CancelAllOrdersResponse, cancel_all_orders_response};

    for status in [
        cancel_all_orders_response::Status::StatusUnspecified.into(),
        buffa::EnumValue::from(99),
    ] {
        let response = CancelAllOrdersResponse {
            status,
            matched_orders: 1,
            submitted_cancels: 1,
            ..Default::default()
        };
        let http = MockHttpServer::spawn(move |req| {
            if req.path == "/orders.v1.OrdersService/CancelAllOrders" {
                connect_proto_ok(&response)
            } else {
                hardening_support::HttpScript::NotFound
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .expect("client");

        let err = client
            .orders
            .cancel_all(None, false, None)
            .await
            .expect_err("empty/unknown cancel-all status must fail closed");
        assert!(err.to_string().contains("unknown status"), "{err}");
    }
}

#[tokio::test]
async fn l2_cancel_all_after_rejects_empty_and_unknown_status_through_public_service() {
    use polyester::proto::orders::v1::{CancelAllAfterResponse, cancel_all_after_response};

    for status in [
        cancel_all_after_response::Status::StatusUnspecified.into(),
        buffa::EnumValue::from(99),
    ] {
        let response = CancelAllAfterResponse {
            status,
            effective_timeout_sec: 15,
            ..Default::default()
        };
        let http = MockHttpServer::spawn(move |req| {
            if req.path == "/orders.v1.OrdersService/CancelAllAfter" {
                connect_proto_ok(&response)
            } else {
                hardening_support::HttpScript::NotFound
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .expect("client");

        let err = client
            .orders
            .cancel_all_after(15, None, None, None)
            .await
            .expect_err("empty/unknown cancel-all-after status must fail closed");
        assert!(err.to_string().contains("unknown status"), "{err}");
    }
}

#[tokio::test]
async fn l2_batch_replace_rejects_inconsistent_server_counts_through_public_service() {
    use polyester::Price;
    use polyester::models::BatchReplaceItem;
    use polyester::proto::orders::v1::{
        BatchReplaceAdmissionItem as ProtoItem, BatchReplaceAdmissionStatus,
        BatchReplaceItemAdmissionStatus, BatchReplaceOrdersResponse,
    };

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let response = BatchReplaceOrdersResponse {
        batch_request_id: 9,
        status: BatchReplaceAdmissionStatus::Admitted.into(),
        results: vec![ProtoItem {
            status: BatchReplaceItemAdmissionStatus::Admitted.into(),
            replacement_order_id: 9,
            ..Default::default()
        }],
        accepted_count: 0,
        rejected_count: 1,
        ..Default::default()
    };
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else if req.path == "/orders.v1.OrdersService/BatchReplaceOrders" {
            connect_proto_ok(&response)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("catalogs");

    let err = client
        .orders
        .batch_replace(
            vec![BatchReplaceItem {
                key: polyester::models::OrderKey::OrderId("9".into()),
                new_price: Some(Price::from_ticks(1, Some("BTC-USDT".into())).expect("price")),
                new_qty: None,
                new_attached_risk: None,
                new_client_order_id: None,
            }],
            "BTC-USDT",
            None,
            Some("replace-count-mismatch".into()),
        )
        .await
        .expect_err("service must reject an ambiguous batch result");
    assert!(err.to_string().contains("response counts"), "{err}");
}

#[tokio::test]
async fn l2_columnar_candles_reject_misaligned_columns_through_public_service() {
    use polyester::models::GetCandlesOpts;
    use polyester::proto::marketdata::v1::{GetCandlesColumnsResponse, Timeframe};

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let response = GetCandlesColumnsResponse {
        symbol_id: 1,
        timeframe: Timeframe::Min1.into(),
        ts_sec: vec![10, 20],
        open: vec![1, 2],
        high: vec![1],
        low: vec![1, 2],
        close: vec![1, 2],
        volume: vec![1, 2],
        ..Default::default()
    };
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else if req.path == "/marketdata.v1.MarketDataService/GetCandlesColumns" {
            connect_proto_ok(&response)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("catalogs");

    let err = client
        .market_data
        .get_candles_columns(GetCandlesOpts {
            symbol: Some("BTC-USDT".into()),
            timeframe: "1m".into(),
            ..Default::default()
        })
        .await
        .expect_err("service must reject misaligned OHLCV columns");
    assert!(err.to_string().contains("response lengths"), "{err}");
}

#[tokio::test]
async fn l2_lifecycle_get_rejects_missing_required_flow_through_public_service() {
    use polyester::proto::chain::lifecycle::v1::GetFlowResponse;

    let response = GetFlowResponse::default();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == "/chain.lifecycle.v1.LifecycleReadService/GetFlowById" {
            connect_proto_ok(&response)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");

    let err = client
        .lifecycle
        .get_flow("flow-1")
        .await
        .expect_err("service must reject a missing required flow");
    assert!(err.to_string().contains("missing flow"), "{err}");
}

#[tokio::test]
async fn l2_create_deposit_address_rejects_missing_entity_through_public_service() {
    use polyester::proto::chain::deposit::v1::{
        CreateDepositAddressRequest, CreateDepositAddressResponse,
    };

    let response = CreateDepositAddressResponse::default();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == "/chain.deposit.v1.DepositAddressService/CreateDepositAddress" {
            connect_proto_ok(&response)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");

    let err = client
        .deposit
        .create_address(CreateDepositAddressRequest {
            chain_id: 1,
            ..Default::default()
        })
        .await
        .expect_err("service must reject a missing deposit address");
    assert!(err.to_string().contains("missing deposit_address"), "{err}");
}

#[tokio::test]
async fn l2_wait_for_catalogs_success_path() {
    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("ok");
    assert!(client.catalogs_last_error().is_none());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
}

#[tokio::test]
async fn l2_wait_for_catalogs_can_retry_after_transient_failure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = attempts.clone();
    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            // POLY-9228: startup reads a 503 again up to 3 times; fail all of
            // them so the load itself fails.
            if attempts_h.fetch_add(1, Ordering::SeqCst) < 3 {
                hardening_support::HttpScript::Json {
                    status: 503,
                    body: br#"{"error":"temporary"}"#.to_vec(),
                }
            } else {
                connect_proto_ok(&spot)
            }
        } else if req.path == ZIPPER_CONFIG_PATH {
            connect_proto_ok(&zipper)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");

    client
        .wait_for_catalogs()
        .await
        .expect_err("first catalog load must surface the failure");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    client
        .wait_for_catalogs()
        .await
        .expect("second catalog load must recover");
    client
        .wait_for_funding_catalog()
        .await
        .expect("funding catalog");
    assert!(client.catalogs.is_ready());
    assert!(client.catalogs_last_error().is_none());
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
}

/// POLY-9228: a funding (zipper) failure no longer blocks spot trading.
#[tokio::test]
async fn l2_wait_for_catalogs_zipper_failure_leaves_spot_ready() {
    let spot = spot_config_fixture();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            connect_proto_ok(&spot)
        } else if req.path == ZIPPER_CONFIG_PATH {
            hardening_support::HttpScript::Json {
                status: 500,
                body: br#"{"error":"zipper down"}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        timeout: Duration::from_secs(2),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client
        .wait_for_catalogs()
        .await
        .expect("the spot config loads without the funding config");
    assert_eq!(
        client.catalogs.base_quantity_scale_for_symbol("BTC-USDT"),
        Some(8)
    );
    let err = client
        .wait_for_funding_catalog()
        .await
        .expect_err("zipper 500 must fail the funding config closed");
    assert!(err.to_string().contains("funding config"), "{err}");
    assert_eq!(client.catalogs.ledger_id_for_asset("USDT"), None);
}

#[tokio::test]
async fn l2_snapshot_then_stream_recovery_success_clears_err() {
    use std::sync::Mutex;

    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_public(active.clone()).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let fetch_attempts = attempts.clone();
    let merged = Arc::new(Mutex::new(Vec::<u8>::new()));
    let merged_cb = merged.clone();
    let sts = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: rt,
        channel: PUBLIC_CHANNEL.into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(Error::transport("snapshot refresh failed"))
                } else {
                    Ok("recovered".to_string())
                }
            }
            .boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(move |_s, pending| {
            merged_cb.lock().expect("merged").extend(pending);
        }),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
        on_error: None,
    });

    // Drive buffer retention without waiting on WS publications: inject via refresh path.
    // First refresh fails (sets err, keeps buffer); inject pubs while not ready; retry succeeds.
    assert!(sts.refresh_snapshot().await.is_err());
    assert!(sts.err().is_some());
    // Direct buffer injection through a second STS that shares apply is awkward; instead
    // re-run the unit-level retention contract via public refresh_snapshot success clear.
    sts.refresh_snapshot().await.expect("recovery");
    assert!(sts.is_ready());
    assert!(sts.err().is_none());
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    sts.close();
}

#[test]
fn withdrawal_payload_signing_and_e18_conversion_are_exact() {
    use buffa::Message;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use polyester::models::CreateApiKeyTradingWithdrawParams;
    use polyester::{AssetAmount, Client, Config, QuantityDomain};

    let client = Client::new(Config {
        api_key_id: Some("withdraw-hardening".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let prepared = client
        .withdraw
        .prepare_api_key_to_funding(CreateApiKeyTradingWithdrawParams {
            asset_id: 7,
            amount: AssetAmount::from_scaled(125, Some(2), QuantityDomain::LedgerE18, Some(7))
                .unwrap(),
            destination_address: String::new(),
            idempotency_key: "withdraw-hardening".into(),
            amount_scale: Some(2),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: Some(42),
        })
        .unwrap();

    assert_eq!(prepared.payload().deadline_ts_sec, 1_800_000_000);
    assert_eq!(
        prepared.deterministic_payload_bytes(),
        prepared.payload().encode_to_vec()
    );
    let amount = prepared.payload().amount_e18.as_option().unwrap();
    assert_eq!(
        (u128::from(amount.hi) << 64) | u128::from(amount.lo),
        1_250_000_000_000_000_000
    );

    let seed: [u8; 32] = hex::decode(TEST_KEY).unwrap().try_into().unwrap();
    let verifier = VerifyingKey::from(&ed25519_dalek::SigningKey::from_bytes(&seed));
    verifier
        .verify(
            &prepared.deterministic_payload_bytes(),
            &Signature::from_slice(prepared.payload_signature()).unwrap(),
        )
        .unwrap();
}

#[tokio::test]
async fn precomputed_withdraw_fields_and_unknown_wallet_actions_fail_closed() {
    use polyester::models::{CreateTradingWithdrawParams, CreateWalletTradingWithdrawParams};
    use polyester::{AssetAmount, Client, Config, Error, QuantityDomain};

    let client = Client::new(Config {
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let amount =
        || AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
    let missing_deadline = client
        .withdraw
        .create_to_funding(CreateTradingWithdrawParams {
            asset_id: 7,
            amount: amount(),
            payload_signature: vec![1],
            destination_address: String::new(),
            idempotency_key: "missing-deadline".into(),
            amount_scale: Some(18),
            deadline_ts_sec: None,
            nonce: 42,
        })
        .await
        .unwrap_err();
    assert!(matches!(missing_deadline, Error::Validation(_)));

    let unknown_action = client
        .withdraw
        .create_wallet_trading_withdraw(CreateWalletTradingWithdrawParams {
            action: "unknown".into(),
            asset_id: 7,
            amount: amount(),
            idempotency_key: "unknown-action".into(),
            payload_signature: vec![1],
            signer_wallet: "0x1".into(),
            destination_chain_id: 0,
            destination_address: String::new(),
            subaccount_id: None,
            amount_scale: Some(18),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: 42,
        })
        .await
        .unwrap_err();
    assert!(matches!(unknown_action, Error::Validation(_)));
}

// Silence unused import warnings when Credentials helpers evolve.
#[allow(dead_code)]
fn _creds_type_check() -> Credentials {
    test_credentials("ak", TEST_KEY)
}

#[allow(dead_code)]
fn _reply_helper() -> Vec<u8> {
    centrifugo_ok_reply(1)
}

/// `orders.subscribe` end to end over real TCP: signed token requests for the
/// base order channel, then the private WebSocket on the API host with the
/// binary protobuf subprotocol, auth first, and orders decoded like the reads.
#[tokio::test]
async fn l2_orders_subscribe_streams_over_the_private_websocket() {
    use futures_util::{SinkExt, StreamExt};
    use polyester::codecs::scalars::format_id;
    use polyester::models::PrivateOrdersEvent;
    use polyester::proto::orders::v1::Order as ProtoOrder;
    use polyester::proto::privatews::v1::{
        ClientMessage, Ready, ServerMessage, SnapshotBegin, SnapshotEnd, SnapshotPage, Subscribed,
        client_message, server_message::Frame,
    };
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::tungstenite::http::{Request as WsRequest, Response as WsResponse};

    const ACCOUNT: u64 = 8_215_573_541_720_371;
    let token_requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let upgrade = Arc::new(Mutex::new(None::<(String, String)>));
    let client_ops = Arc::new(Mutex::new(Vec::<client_message::Op>::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (requests, upgraded, ops) = (token_requests.clone(), upgrade.clone(), client_ops.clone());
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (requests, upgraded, ops) = (requests.clone(), upgraded.clone(), ops.clone());
            // Token requests arrive while the socket is open, so serve each connection on its own task.
            tokio::spawn(async move {
                let mut head = [0u8; 32];
                let n = stream.peek(&mut head).await.unwrap();
                if hardening_support::is_port_probe(&head[..n]) {
                    return;
                }
                if !head[..n].starts_with(b"GET /v1/ws/private") {
                    // A realtime token request.
                    let mut request = Vec::new();
                    let mut buf = [0u8; 4096];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = stream.read(&mut buf).await.unwrap();
                        request.extend_from_slice(&buf[..n]);
                    }
                    let line = String::from_utf8_lossy(&request)
                        .lines()
                        .next()
                        .unwrap()
                        .to_owned();
                    let token = if line.contains("/v1/rt/subscribe") {
                        "sub-token"
                    } else {
                        "connect-token"
                    };
                    requests.lock().unwrap().push(line);
                    let body = format!(r#"{{"token":"{token}"}}"#);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    return;
                }
                let mut ws = tokio_tungstenite::accept_hdr_async(
                    stream,
                    #[allow(clippy::result_large_err)]
                    move |req: &WsRequest<()>, mut response: WsResponse<()>| {
                        let protocol = req
                            .headers()
                            .get("Sec-WebSocket-Protocol")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_owned();
                        *upgraded.lock().unwrap() = Some((req.uri().path().to_owned(), protocol));
                        response.headers_mut().insert(
                            "Sec-WebSocket-Protocol",
                            "polyester.v1.proto".parse().unwrap(),
                        );
                        Ok(response)
                    },
                )
                .await
                .unwrap();
                let mut recv = async || loop {
                    if let WsMessage::Binary(bytes) = ws.next().await.unwrap().unwrap() {
                        break ClientMessage::decode_from_slice(&bytes)
                            .unwrap()
                            .op
                            .unwrap();
                    }
                };
                let auth = recv().await;
                let subscribe = recv().await;
                let id = match &subscribe {
                    client_message::Op::Subscribe(subscribe) => subscribe.id.clone(),
                    other => panic!("expected subscribe, got {other:?}"),
                };
                ops.lock().unwrap().extend([auth, subscribe]);
                let frames: Vec<(u64, Frame)> = vec![
                    (
                        0,
                        Subscribed {
                            id,
                            account: ACCOUNT,
                            ..Default::default()
                        }
                        .into(),
                    ),
                    (1, SnapshotBegin::default().into()),
                    (
                        2,
                        SnapshotPage {
                            orders: vec![ProtoOrder {
                                order_id: 9,
                                symbol_id: 2,
                                price_ticks: 2_420_000_000_000,
                                version: 1,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }
                        .into(),
                    ),
                    (
                        3,
                        SnapshotEnd {
                            cursor: "c1".into(),
                            count: 1,
                            ..Default::default()
                        }
                        .into(),
                    ),
                    (4, Ready::default().into()),
                ];
                for (seq, frame) in frames {
                    let msg = ServerMessage {
                        sub: "s1".into(),
                        seq,
                        frame: Some(frame),
                        ..Default::default()
                    };
                    ws.send(WsMessage::Binary(msg.encode_to_vec().into()))
                        .await
                        .unwrap();
                }
                // Keep the socket open until the client leaves.
                while let Some(Ok(frame)) = ws.next().await {
                    if frame.is_close() {
                        break;
                    }
                }
            });
        }
    });

    let client = Client::new(Config {
        api_url: format!("http://{addr}"),
        api_key_id: Some("orders-stream".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_account_id: Some(format_id(ACCOUNT)),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let mut orders = client.orders.subscribe(None).await.unwrap();
    let next = async |orders: &mut polyester::realtime::PrivateOrdersSubscription| {
        tokio::time::timeout(Duration::from_secs(10), orders.recv())
            .await
            .expect("event in time")
            .expect("stream open")
    };
    let first = next(&mut orders).await;
    assert!(matches!(first, PrivateOrdersEvent::Connected), "{first:?}");
    let snapshot = match next(&mut orders).await {
        PrivateOrdersEvent::Snapshot(snapshot) => snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    };
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].order_id, format_id(9));
    assert_eq!(snapshot[0].price.as_ref().unwrap().format(), "2420");
    let ready = next(&mut orders).await;
    assert!(matches!(ready, PrivateOrdersEvent::Ready), "{ready:?}");

    let account = format_id(ACCOUNT);
    assert_eq!(
        *token_requests.lock().unwrap(),
        vec![
            "GET /v1/rt/token HTTP/1.1".to_owned(),
            format!("GET /v1/rt/subscribe?channel=private%3Aspot%3Aorders%3A{account} HTTP/1.1"),
        ]
    );
    assert_eq!(
        upgrade.lock().unwrap().clone(),
        Some(("/v1/ws/private".to_owned(), "polyester.v1.proto".to_owned()))
    );
    let ops = client_ops.lock().unwrap().clone();
    match &ops[0] {
        client_message::Op::Auth(auth) => assert_eq!(auth.token, "connect-token"),
        other => panic!("the first frame must be auth, got {other:?}"),
    }
    match &ops[1] {
        client_message::Op::Subscribe(subscribe) => {
            assert_eq!(subscribe.account, ACCOUNT);
            assert_eq!(subscribe.token, "sub-token");
            assert_eq!(subscribe.cursor, "");
        }
        other => panic!("expected subscribe, got {other:?}"),
    }
    drop(orders);
    server.abort();
}

/// Gap-free backfill: an initial empty page with zero watermarks is a valid
/// answer, and feeding its `next_after_trade_id` (0) back sends an explicit
/// `after_trade_id = 0` rather than dropping the cursor.
#[tokio::test]
async fn l2_backfill_resumes_from_zero_watermarks() {
    use polyester::models::GetTradesOpts;
    use polyester::proto::marketdata::v1::{GetTradesRequest, GetTradesResponse};
    use std::sync::Mutex;

    let requests = Arc::new(Mutex::new(Vec::<GetTradesRequest>::new()));
    let seen = requests.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path == GET_TRADES_PATH {
            seen.lock()
                .unwrap()
                .push(GetTradesRequest::decode_from_slice(&request.body).unwrap());
            // Nothing traded yet: no trades and zero watermarks.
            return connect_proto_ok(&GetTradesResponse::default());
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .unwrap();

    let first = client
        .market_data
        .get_trades_with(GetTradesOpts {
            symbol: Some("BTC-USDT".into()),
            after_trade_id: Some(0),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(first.trades.is_empty());
    assert_eq!(first.complete_through_trade_id, 0);
    assert_eq!(first.next_after_trade_id, 0);

    client
        .market_data
        .get_trades_with(GetTradesOpts {
            symbol: Some("BTC-USDT".into()),
            after_trade_id: Some(first.next_after_trade_id),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(request.symbol_id, 1);
        assert_eq!(request.after_trade_id, Some(0), "the cursor 0 must be sent");
        assert!(request.page_token.is_empty());
        assert!(request.start_time.as_option().is_none());
    }
}

fn api_key_client(http: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client")
}

async fn auth_me_error(script: fn() -> hardening_support::HttpScript) -> Error {
    let http = MockHttpServer::spawn(move |_| script()).await;
    api_key_client(&http)
        .auth
        .me()
        .await
        .expect_err("rejection must fail the call")
}

fn json_body(status: u16, content_type: &str, body: &str) -> hardening_support::HttpScript {
    hardening_support::HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".into(), content_type.into()),
            ("Content-Length".into(), body.len().to_string()),
        ],
        body: body.as_bytes().to_vec(),
    }
}

// Devnet's gateway rejects a stale API-key signature as
// `401 application/json {"code":"TIMESTAMP_SKEW"}`, which is not a Connect
// error. The rejection happens before any service sees the request, so a
// retry that signs again is safe.
#[tokio::test]
async fn l2_gateway_timestamp_skew_json_is_retryable() {
    for script in [
        (|| json_body(401, "application/json", r#"{"code":"TIMESTAMP_SKEW"}"#))
            as fn() -> hardening_support::HttpScript,
        || {
            json_body(
                401,
                "application/problem+json",
                r#"{"type":"about:blank","status":401,"code":"TIMESTAMP_SKEW"}"#,
            )
        },
    ] {
        let err = auth_me_error(script).await;
        assert!(
            matches!(err, Error::TimestampSkew { status: 401, .. }),
            "{err:?}"
        );
        assert!(
            err.is_retryable(),
            "TIMESTAMP_SKEW must be retryable: {err:?}"
        );
        assert!(
            !err.mutation_outcome_unknown(),
            "the gateway rejected the request before any service saw it: {err:?}"
        );
        assert_eq!(err.auth_error_code(), Some("TIMESTAMP_SKEW"), "{err:?}");
    }
}

#[tokio::test]
async fn l2_gateway_api_key_rejections_carry_their_code_and_are_not_retryable() {
    let err =
        auth_me_error(|| json_body(401, "application/json", r#"{"code":"SIGNATURE_INVALID"}"#))
            .await;
    match &err {
        Error::ApiKeyRejected { status, code, .. } => {
            assert_eq!((*status, code.as_str()), (401, "SIGNATURE_INVALID"));
        }
        other => panic!("expected ApiKeyRejected, got {other:?}"),
    }
    assert!(!err.is_retryable(), "{err:?}");
    assert_eq!(err.auth_error_code(), Some("SIGNATURE_INVALID"), "{err:?}");

    let err =
        auth_me_error(|| json_body(403, "application/json", r#"{"code":"IP_NOT_ALLOWED"}"#)).await;
    assert!(
        matches!(&err, Error::ApiKeyRejected { status: 403, code, .. } if code == "IP_NOT_ALLOWED"),
        "{err:?}"
    );
    assert!(!err.is_retryable(), "{err:?}");
}

#[tokio::test]
async fn l2_connect_json_errors_and_other_json_bodies_keep_their_mapping() {
    // A Connect error body is still read by Connect itself.
    let err = auth_me_error(|| {
        json_body(
            401,
            "application/json",
            r#"{"code":"unauthenticated","message":"missing signature"}"#,
        )
    })
    .await;
    assert!(
        matches!(&err, Error::Auth(message) if message.contains("missing signature")),
        "{err:?}"
    );
    let err = auth_me_error(|| {
        json_body(
            503,
            "application/json",
            r#"{"code":"unavailable","message":"draining"}"#,
        )
    })
    .await;
    assert!(
        matches!(err, Error::Server(_)) && !err.is_retryable(),
        "{err:?}"
    );
    // A non-Connect code on a status other than 401/403 is left to Connect's
    // status mapping.
    let err =
        auth_me_error(|| json_body(503, "application/json", r#"{"code":"MAINTENANCE"}"#)).await;
    assert!(matches!(err, Error::Server(_)), "{err:?}");
}

#[tokio::test]
async fn l2_unimplemented_and_bare_501_map_to_not_implemented() {
    let err = auth_me_error(|| {
        json_body(
            501,
            "application/json",
            r#"{"code":"unimplemented","message":"auth.v1.AuthService/Me is not implemented"}"#,
        )
    })
    .await;
    assert!(
        matches!(&err, Error::NotImplemented { message } if message.contains("not implemented")),
        "{err:?}"
    );
    assert!(err.is_not_implemented() && !err.is_retryable(), "{err:?}");

    // A Connect `unimplemented` without a message is still the server
    // saying it does not implement the call, not a missing route.
    let err =
        auth_me_error(|| json_body(501, "application/json", r#"{"code":"unimplemented"}"#)).await;
    assert!(matches!(err, Error::NotImplemented { .. }), "{err:?}");

    // A bare HTTP 501 proves nothing about a mutation, whatever its body.
    for script in [
        (|| json_body(501, "text/plain", "Not Implemented"))
            as fn() -> hardening_support::HttpScript,
        || json_body(501, "application/json", "{}"),
        || json_body(501, "application/json", r#"{"message":"Not Implemented"}"#),
    ] {
        let err = auth_me_error(script).await;
        assert!(matches!(err, Error::Server(_)), "{err:?}");
        assert!(
            !err.is_retryable() && err.mutation_outcome_unknown(),
            "{err:?}"
        );
    }

    // A bare 404 stays the narrower RouteNotFound. A JSON body that isn't a
    // router's own message is not bare.
    for script in [
        (|| hardening_support::HttpScript::NotFound) as fn() -> hardening_support::HttpScript,
        || json_body(404, "application/json", ""),
        || {
            json_body(
                404,
                "application/json",
                r#"{"message":"404 page not found"}"#,
            )
        },
        || json_body(404, "text/html", "<h1>404 page not found</h1>"),
    ] {
        let err = auth_me_error(script).await;
        assert!(matches!(err, Error::RouteNotFound { .. }), "{err:?}");
        assert!(err.is_not_implemented(), "{err:?}");
        assert!(err.mutation_outcome_unknown(), "{err:?}");
    }
    let err = auth_me_error(|| json_body(404, "application/json", "{}")).await;
    assert!(matches!(err, Error::Server(_)), "{err:?}");

    // A Connect `not_found` error carried on HTTP 404 is a missing resource,
    // not a missing route.
    let err = auth_me_error(|| {
        json_body(
            404,
            "application/json",
            r#"{"code":"not_found","message":"no such order"}"#,
        )
    })
    .await;
    assert!(!err.is_not_implemented(), "{err:?}");
}

#[tokio::test]
async fn l2_realtime_token_gateway_rejections_are_typed() {
    for (code, retryable, expected_type) in [
        (
            "TIMESTAMP_SKEW",
            true,
            "https://polyester.dev/problems/timestamp-skew",
        ),
        (
            "SIGNATURE_INVALID",
            false,
            "https://polyester.dev/problems/signature-invalid",
        ),
    ] {
        for supplied_type in [
            None,
            Some("https://polyester.dev/problems/AUTH_SESSION_KIND_NOT_ALLOWED"),
        ] {
            let mut body = serde_json::json!({"code":code,"status":403,"detail":""});
            if let Some(value) = supplied_type {
                body["type"] = value.into();
            }
            let body = body.to_string();
            let http =
                MockHttpServer::spawn(move |_| json_body(401, "application/problem+json", &body))
                    .await;
            let active = Arc::new(AtomicUsize::new(0));
            let ws = MockWsServer::spawn_centrifugo_public(active).await;
            let rt = private_rt(&ws, &http, Duration::from_secs(2));
            let err = subscribe_raw_err(&rt, PRIVATE_CHANNEL).await;
            if retryable {
                assert!(
                    matches!(err, Error::TimestampSkew { status: 401, .. }),
                    "{err:?}"
                );
            } else {
                assert!(
                    matches!(&err, Error::ApiKeyRejected { status: 401, code, .. } if code == "SIGNATURE_INVALID"),
                    "{err:?}"
                );
            }
            assert_eq!(err.is_retryable(), retryable, "{err:?}");
            let problem = err.http_problem().expect("realtime 401 problem metadata");
            assert_eq!(problem.code, code);
            assert_eq!(problem.status, 401);
            assert_eq!(problem.detail, "");
            assert_eq!(problem.problem_type, expected_type);
        }
    }
}

/// Client whose realtime token endpoints answer, so private channels connect
/// to `ws` and receive its one publication.
async fn streaming_client(ws: &MockWsServer) -> (Client, MockHttpServer) {
    let http = MockHttpServer::spawn(|req| {
        if req.path == "/v1/rt/token" || req.path.starts_with("/v1/rt/subscribe") {
            hardening_support::HttpScript::Json {
                status: 200,
                body: br#"{"token":"t"}"#.to_vec(),
            }
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect("client");
    (client, http)
}

async fn recv_n<T>(sub: &mut polyester::realtime::TypedSubscription<T>, n: usize) -> Vec<T> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            tokio::time::timeout(Duration::from_secs(2), sub.recv_result())
                .await
                .expect("publication deadline")
                .expect("no stream error")
                .expect("stream open"),
        );
    }
    out
}

// Every realtime trade, fill, transfer and trigger-event publication is a
// batch; each of its events must reach the caller, in order.
#[tokio::test]
async fn l2_batched_publications_deliver_every_event_in_order() {
    use polyester::proto::ledger::read::v1::{TransferRow, TransferRowBatch};
    use polyester::proto::marketdata::v1::{MarketTrade, MarketTradeBatch};
    use polyester::proto::orders::v1::{UserTrade, UserTradeBatch};
    use polyester::proto::triggers::v1::{TriggerEvent, TriggerEventBatch};

    let active = Arc::new(AtomicUsize::new(0));

    let trades = MarketTradeBatch {
        events: (1..=3)
            .map(|match_id| MarketTrade {
                symbol_id: 1,
                match_id,
                qty_scaled: 1,
                price_ticks: 1,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(
        active.clone(),
        trades.encode_to_vec(),
    )
    .await;
    let (client, _http) = streaming_client(&ws).await;
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .unwrap();
    let mut sub = client
        .market_data
        .subscribe_trades("BTC-USDT")
        .await
        .unwrap();
    let got: Vec<String> = recv_n(&mut sub, 3)
        .await
        .into_iter()
        .map(|t| t.match_id)
        .collect();
    assert_eq!(got, ["1", "2", "3"]);
    sub.close();

    let fills = UserTradeBatch {
        events: (10..=11)
            .map(|match_id| UserTrade {
                symbol_id: 1,
                match_id,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(
        active.clone(),
        fills.encode_to_vec(),
    )
    .await;
    let (client, _http) = streaming_client(&ws).await;
    let mut sub = client.trades.subscribe(Some("acct")).await.unwrap();
    let got: Vec<String> = recv_n(&mut sub, 2)
        .await
        .into_iter()
        .map(|t| t.match_id)
        .collect();
    assert_eq!(got, ["10", "11"]);
    sub.close();

    let transfer_id = "340282366920938463463374607431768211455";
    let transfers = TransferRowBatch {
        events: [true, false]
            .into_iter()
            .map(|is_debit| TransferRow {
                asset_id: 2,
                is_debit,
                transfer_id: transfer_id.into(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(
        active.clone(),
        transfers.encode_to_vec(),
    )
    .await;
    let (client, _http) = streaming_client(&ws).await;
    let mut sub = client.transfers.subscribe(Some("acct")).await.unwrap();
    let got = recv_n(&mut sub, 2).await;
    assert_eq!(
        got.iter()
            .map(|t| (t.transfer_id.as_str(), t.is_debit))
            .collect::<Vec<_>>(),
        [(transfer_id, true), (transfer_id, false)]
    );
    sub.close();

    let events = TriggerEventBatch {
        events: (1..=2)
            .map(|ts_ns| TriggerEvent {
                trigger_id: 5,
                symbol_id: 1,
                ts_ns,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(
        active.clone(),
        events.encode_to_vec(),
    )
    .await;
    let (client, _http) = streaming_client(&ws).await;
    let mut sub = client
        .triggers
        .subscribe_events(Some("acct"))
        .await
        .unwrap();
    let got: Vec<String> = recv_n(&mut sub, 2)
        .await
        .into_iter()
        .map(|e| e.ts_ns)
        .collect();
    assert_eq!(got, ["1", "2"]);
    sub.close();
}

#[tokio::test]
async fn l2_batch_member_error_is_reported_without_dropping_the_rest() {
    let active = Arc::new(AtomicUsize::new(0));
    let ws =
        MockWsServer::spawn_centrifugo_publication_after_handshake(active, vec![1, 2, 3]).await;
    let rt = RealtimeClient::new(ws.ws_url(), "", None, None);
    let mut sub = rt
        .subscribe_proto_batch(PUBLIC_CHANNEL, |bytes: &[u8]| {
            Ok(bytes
                .iter()
                .map(|b| {
                    if *b == 2 {
                        Err(Error::realtime("bad member"))
                    } else {
                        Ok(*b)
                    }
                })
                .collect())
        })
        .await
        .unwrap();
    let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = reported.clone();
    sub.set_on_error(move |err| sink.lock().unwrap().push(err.to_string()));
    assert_eq!(recv_n(&mut sub, 2).await, [1, 3]);
    assert_eq!(*reported.lock().unwrap(), ["bad member"]);
    assert!(sub.is_alive(), "a bad member must not end the subscription");
    sub.close();
}

// The overview channel carries every market. A subscription filtered to some
// symbols must not pick up the others from buffered or live publications.
#[tokio::test]
async fn l2_filtered_market_overview_drops_other_markets() {
    use polyester::proto::marketoverview::v1::{
        ListMarketOverviewResponse, MarketOverview, MarketOverviewBatch,
    };
    use polyester::services::MarketOverviewCreateSubscriptionOptions;

    let row = |symbol_id, last_price_ticks| MarketOverview {
        symbol_id,
        last_price_ticks,
        ..Default::default()
    };
    let publication = MarketOverviewBatch {
        markets: vec![row(1, 5_000_000_000), row(2, 7_000_000_000)],
        ..Default::default()
    }
    .encode_to_vec();
    let active = Arc::new(AtomicUsize::new(0));
    let ws = MockWsServer::spawn_centrifugo_publication_after_handshake(active, publication).await;
    let snapshot = ListMarketOverviewResponse {
        markets: vec![row(1, 1_000_000_000)],
        ..Default::default()
    };
    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        "/marketoverview.v1.MarketOverviewService/ListMarketOverview" => {
            connect_proto_ok(&snapshot)
        }
        SPOT_CONFIG_PATH => connect_proto_ok(&spot),
        ZIPPER_CONFIG_PATH => connect_proto_ok(&zipper),
        _ => hardening_support::HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: true,
        timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .unwrap();
    let mut subscription = client
        .market_overview
        .create_subscription(MarketOverviewCreateSubscriptionOptions {
            symbols: Some(vec!["BTC-USDT".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut saw_live_price = false;
    while !saw_live_price {
        let rows = tokio::time::timeout(Duration::from_secs(2), subscription.updates().recv())
            .await
            .expect("overview update deadline")
            .expect("overview stream open");
        assert!(
            rows.iter().all(|row| row.symbol_id == 1),
            "unrelated market leaked into a filtered subscription: {rows:?}"
        );
        saw_live_price = rows
            .iter()
            .any(|row| row.last_price.as_ref().is_some_and(|p| p.format() == "5"));
    }
    subscription.close();
}

// Trigger modify and resume no longer need a symbol: the server targets the
// trigger's own market when `symbol_id` is 0.
#[tokio::test]
async fn l2_trigger_modify_and_resume_send_without_a_symbol() {
    use polyester::models::ModifyTriggerParams;
    use polyester::proto::triggers::v1::{ModifyTriggerRequest, ResumeTriggerRequest};

    let seen = Arc::new(std::sync::Mutex::new(Vec::<(String, u32)>::new()));
    let sink = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        let symbol_id = match req.path.as_str() {
            "/triggers.v1.TriggersService/ModifyTrigger" => {
                ModifyTriggerRequest::decode_from_slice(&req.body).map(|r| r.symbol_id)
            }
            "/triggers.v1.TriggersService/ResumeTrigger" => {
                ResumeTriggerRequest::decode_from_slice(&req.body).map(|r| r.symbol_id)
            }
            _ => return hardening_support::HttpScript::NotFound,
        };
        sink.lock()
            .unwrap()
            .push((req.path.clone(), symbol_id.expect("request decodes")));
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = api_key_client(&http);

    let _ = client.triggers.resume_by_id("1", None, None).await;
    let _ = client
        .triggers
        .modify(ModifyTriggerParams {
            trigger_id: "1".into(),
            subaccount_id: None,
            symbol: None,
            symbol_id: None,
            trigger_price: None,
            activation_price: None,
            trailing_distance_ticks: None,
            trailing_distance_bps: None,
            max_slippage_ticks: None,
            max_slippage_bps: Some(9_999),
        })
        .await;
    assert_eq!(
        *seen.lock().unwrap(),
        [
            ("/triggers.v1.TriggersService/ResumeTrigger".to_owned(), 0),
            ("/triggers.v1.TriggersService/ModifyTrigger".to_owned(), 0),
        ]
    );
}

fn encoded_body(
    status: u16,
    content_type: &str,
    encoding: &str,
    body: &str,
) -> hardening_support::HttpScript {
    let body = connectrpc::CompressionRegistry::default()
        .compress(encoding, body.as_bytes())
        .expect("supported encoding")
        .to_vec();
    hardening_support::HttpScript::Raw {
        status,
        headers: vec![
            ("Content-Type".into(), content_type.into()),
            ("Content-Encoding".into(), encoding.into()),
            ("Content-Length".into(), body.len().to_string()),
        ],
        body,
    }
}

// A Content-Encoding (gzip, or an explicit identity) must not hide a bare
// HTTP status or a gateway code, and must not turn a Connect error body into
// a bare one.
#[tokio::test]
async fn l2_encoded_failed_responses_keep_their_classification() {
    // A bare 501 proves nothing about a mutation, compressed or not.
    for script in [
        (|| encoded_body(501, "text/plain", "gzip", "Not Implemented"))
            as fn() -> hardening_support::HttpScript,
        || encoded_body(501, "text/plain", "identity", "Not Implemented"),
        || encoded_body(501, "application/json", "gzip", "{}"),
    ] {
        let err = auth_me_error(script).await;
        assert!(matches!(err, Error::Server(_)), "{err:?}");
        assert!(err.mutation_outcome_unknown(), "{err:?}");
    }
    let err = auth_me_error(|| {
        encoded_body(
            501,
            "application/json",
            "gzip",
            r#"{"code":"unimplemented"}"#,
        )
    })
    .await;
    assert!(matches!(err, Error::NotImplemented { .. }), "{err:?}");
    for script in [
        (|| encoded_body(404, "text/plain", "identity", "404 page not found"))
            as fn() -> hardening_support::HttpScript,
        || encoded_body(404, "text/plain", "gzip", "404 page not found"),
    ] {
        let err = auth_me_error(script).await;
        assert!(matches!(err, Error::RouteNotFound { .. }), "{err:?}");
    }
    // A compressed JSON body that names no code is no more bare than a plain one.
    let err = auth_me_error(|| encoded_body(404, "application/json", "gzip", "{}")).await;
    assert!(matches!(err, Error::Server(_)), "{err:?}");
    // A compressed Connect not_found stays a resource error.
    let err = auth_me_error(|| {
        encoded_body(
            404,
            "application/json",
            "gzip",
            r#"{"code":"not_found","message":"no such order"}"#,
        )
    })
    .await;
    assert!(
        !err.is_not_implemented() && err.to_string().contains("no such order"),
        "{err:?}"
    );
    // A compressed gateway rejection keeps its code.
    let err = auth_me_error(|| {
        encoded_body(
            401,
            "application/json",
            "gzip",
            r#"{"code":"TIMESTAMP_SKEW"}"#,
        )
    })
    .await;
    assert!(
        matches!(err, Error::TimestampSkew { status: 401, .. }),
        "{err:?}"
    );
}

const CHANGELOG_LIST_POSTS_PATH: &str = "/changelog.v1.ChangelogService/ListPosts";
const CHANGELOG_GET_POST_PATH: &str = "/changelog.v1.ChangelogService/GetPost";
const CHANGELOG_LIST_CHANGES_PATH: &str = "/changelog.v1.ChangelogService/ListChanges";

/// A client with no API key: the changelog is public.
fn public_changelog_client(server: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap()
}

#[tokio::test]
async fn l2_changelog_posts_stream_follows_page_tokens_without_credentials() {
    use futures_util::TryStreamExt;
    use polyester::proto::changelog::v1::{ListPostsRequest, ListPostsResponse, PostSummary};
    use std::sync::Mutex;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path != CHANGELOG_LIST_POSTS_PATH {
            return hardening_support::HttpScript::NotFound;
        }
        let req = ListPostsRequest::decode_from_slice(&request.body).unwrap();
        seen_handler
            .lock()
            .unwrap()
            .push((req.page_token.clone(), req.limit));
        let post = |slug: &str| PostSummary {
            slug: slug.into(),
            breaking_change_count: 1,
            ..Default::default()
        };
        let (posts, next) = match req.page_token.as_str() {
            "" => (vec![post("2026-11-02-2"), post("2026-11-02")], "p2"),
            "p2" => (vec![post("2026-10-28")], ""),
            other => panic!("unexpected page token {other}"),
        };
        connect_proto_ok(&ListPostsResponse {
            posts,
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    let client = public_changelog_client(&server);

    let first = client
        .changelog
        .list_posts(polyester::services::ListChangelogPostsOptions {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first.posts.len(), 2);
    assert_eq!(first.next_page_token, "p2");

    let all: Vec<_> = client
        .changelog
        .posts(polyester::services::ListChangelogPostsOptions {
            limit: Some(2),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    let slugs: Vec<_> = all.iter().map(|p| p.slug.as_str()).collect();
    assert_eq!(slugs, ["2026-11-02-2", "2026-11-02", "2026-10-28"]);
    assert_eq!(all[0].breaking_change_count, 1);
    assert_eq!(
        *seen.lock().unwrap(),
        [(String::new(), 2), (String::new(), 2), ("p2".to_owned(), 2)]
    );
}

#[tokio::test]
async fn l2_changelog_posts_stream_fails_on_a_repeated_page_token() {
    use futures_util::TryStreamExt;
    use polyester::proto::changelog::v1::{ListPostsResponse, PostSummary};

    let server = MockHttpServer::spawn(|_| {
        connect_proto_ok(&ListPostsResponse {
            posts: vec![PostSummary::default()],
            next_page_token: "same".into(),
            ..Default::default()
        })
    })
    .await;
    let client = public_changelog_client(&server);
    let err = client
        .changelog
        .posts(polyester::services::ListChangelogPostsOptions {
            page_token: Some("same".into()),
            ..Default::default()
        })
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        matches!(err, polyester::Error::ResponseContract { .. }),
        "{err}"
    );
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn l2_changelog_posts_stream_fails_on_a_page_token_cycle() {
    use futures_util::TryStreamExt;
    use polyester::proto::changelog::v1::{ListPostsRequest, ListPostsResponse, PostSummary};
    use std::sync::Mutex;

    // (starting token, next tokens the server returns in order, tokens the
    // SDK must send before it stops)
    let cases: [(Option<&str>, &[&str], &[&str]); 3] = [
        // "" -> A -> B -> A: A recurs after two pages.
        (None, &["A", "B", "A"], &["", "A", "B"]),
        // "" -> A -> B -> C -> B.
        (None, &["A", "B", "C", "B"], &["", "A", "B", "C"]),
        // The caller's starting token counts: S -> A -> S.
        (Some("S"), &["A", "S"], &["S", "A"]),
    ];
    for (start, cycle, expected) in cases {
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let sent_handler = sent.clone();
        let cycle: Vec<String> = cycle.iter().map(|t| (*t).to_owned()).collect();
        let server = MockHttpServer::spawn(move |request| {
            let req = ListPostsRequest::decode_from_slice(&request.body).unwrap();
            let mut sent = sent_handler.lock().unwrap();
            sent.push(req.page_token.clone());
            let next = cycle[(sent.len() - 1) % cycle.len()].clone();
            connect_proto_ok(&ListPostsResponse {
                posts: vec![PostSummary::default()],
                next_page_token: next,
                ..Default::default()
            })
        })
        .await;
        let client = public_changelog_client(&server);
        let err = client
            .changelog
            .posts(polyester::services::ListChangelogPostsOptions {
                page_token: start.map(str::to_owned),
                ..Default::default()
            })
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(err, polyester::Error::ResponseContract { .. }),
            "{err}"
        );
        assert_eq!(*sent.lock().unwrap(), expected, "start {start:?}");
    }
}

#[tokio::test]
async fn l2_changelog_since_helpers_page_filters_and_tolerate_unknown_enums() {
    use buffa::EnumValue;
    use polyester::proto::changelog::v1::{
        Audience, Change, ChangeCategory, ChangeEntry, ListChangesRequest, ListChangesResponse,
        Surface,
    };
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;

    let seen = Arc::new(Mutex::new(Vec::<ListChangesRequest>::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path != CHANGELOG_LIST_CHANGES_PATH {
            return hardening_support::HttpScript::NotFound;
        }
        let req = ListChangesRequest::decode_from_slice(&request.body).unwrap();
        let page_token = req.page_token.clone();
        seen_handler.lock().unwrap().push(req);
        let entry = |id: u64, category: EnumValue<ChangeCategory>| ChangeEntry {
            change: Change {
                id,
                title: format!("change {id}"),
                category,
                action_required: true,
                audiences: vec![Audience::DEVELOPERS.into(), EnumValue::Unknown(77)],
                surfaces: vec![EnumValue::Unknown(88), Surface::API.into()],
                ..Default::default()
            }
            .into(),
            post_slug: "2026-11-02".into(),
            section_heading: "API".into(),
            ..Default::default()
        };
        let (changes, next) = match page_token.as_str() {
            "" => (vec![entry(1, ChangeCategory::BREAKING_CHANGE.into())], "c2"),
            "c2" => (vec![entry(58, EnumValue::Unknown(9))], ""),
            other => panic!("unexpected page token {other}"),
        };
        connect_proto_ok(&ListChangesResponse {
            changes,
            next_page_token: next.into(),
            ..Default::default()
        })
    })
    .await;
    let client = public_changelog_client(&server);
    let since = UNIX_EPOCH + Duration::from_secs(1_767_225_600);

    let breaking = client
        .changelog
        .breaking_changes_since(since)
        .await
        .unwrap();
    assert_eq!(breaking.len(), 2);
    assert_eq!(breaking[0].change.id, "2");
    assert_eq!(breaking[0].change.category, "breaking_change");
    assert_eq!(breaking[0].change.audiences, ["developers", "unknown(77)"]);
    assert_eq!(breaking[0].change.surfaces, ["unknown(88)", "api"]);
    assert_eq!(breaking[0].section_heading, "API");
    assert_eq!(breaking[1].change.id, "21");
    assert_eq!(breaking[1].change.category, "unknown(9)");

    let action = client.changelog.action_required_since(since).await.unwrap();
    assert_eq!(action.len(), 2);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 4);
    for req in seen.iter() {
        assert_eq!(req.limit, 500);
    }
    for req in &seen[..2] {
        assert_eq!(
            req.categories,
            [EnumValue::from(ChangeCategory::BREAKING_CHANGE)]
        );
        assert_eq!(req.action_required, None);
        assert_eq!(req.start_time.as_option().unwrap().seconds, 1_767_225_600);
    }
    for req in &seen[2..] {
        assert!(req.categories.is_empty());
        assert_eq!(req.action_required, Some(true));
        assert_eq!(req.start_time.as_option().unwrap().seconds, 1_767_225_600);
    }
    assert_eq!(seen[1].page_token, "c2");
    assert_eq!(seen[3].page_token, "c2");
}

#[tokio::test]
async fn l2_changelog_get_post_unknown_slug_maps_to_not_found() {
    use polyester::proto::changelog::v1::GetPostRequest;

    let server = MockHttpServer::spawn(|request| {
        if request.path != CHANGELOG_GET_POST_PATH {
            return hardening_support::HttpScript::NotFound;
        }
        let req = GetPostRequest::decode_from_slice(&request.body).unwrap();
        assert_eq!(req.slug, "2020-01-01");
        hardening_support::HttpScript::Json {
            status: 404,
            body: br#"{"code":"not_found","message":"changelog post not found"}"#.to_vec(),
        }
    })
    .await;
    let client = public_changelog_client(&server);
    let err = client.changelog.get_post("2020-01-01").await.unwrap_err();
    match err {
        Error::Api { code, message, .. } => {
            assert_eq!(code, "NotFound");
            assert!(message.contains("changelog post not found"), "{message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

// REST answers a member call that targets the sub-account's owner with a
// 412 problem; it maps like the ConnectRPC `failed_precondition` answer.
#[tokio::test]
async fn l2_rest_owner_role_lock_maps_to_the_typed_error() {
    let err = auth_me_error(|| {
        json_body(
            412,
            "application/problem+json",
            r#"{"status":412,"code":"AUTH_SUBACCOUNT_OWNER_ROLE_LOCKED","title":"Subaccount Owner Role Locked"}"#,
        )
    })
    .await;
    assert!(err.is_subaccount_owner_role_locked(), "{err:?}");
    assert_eq!(
        err.auth_error_code(),
        Some(polyester::errors::auth_codes::SUBACCOUNT_OWNER_ROLE_LOCKED)
    );
    assert!(!err.is_retryable(), "{err:?}");
}

const GET_TRIGGER_PATH: &str = "/triggers.v1.TriggersService/GetTrigger";
const LIST_TRIGGERS_PATH: &str = "/triggers.v1.TriggersService/ListTriggers";
const BATCH_STATUS_PATH: &str = "/orders.v1.OrdersReadService/GetBatchReplaceStatus";

fn catching_up(rest: bool) -> hardening_support::HttpScript {
    if rest {
        json_body(
            503,
            "application/problem+json",
            r#"{"status":503,"code":"UNAVAILABLE","detail":"Triggers are catching up with recent activity; retry the request."}"#,
        )
    } else {
        json_body(
            503,
            "application/json",
            r#"{"code":"unavailable","message":"Triggers are catching up with recent activity; retry the request."}"#,
        )
    }
}

// After a trigger change, trigger reads answer `unavailable` until the change
// is readable. The SDK retries them with the same request.
#[tokio::test]
async fn l2_trigger_reads_retry_unavailable_with_the_same_request() {
    use polyester::proto::triggers::v1::{
        GetTriggerRequest, GetTriggerResponse, ListTriggerEventsRequest, ListTriggerEventsResponse,
        ListTriggersRequest, ListTriggersResponse, Trigger,
    };

    for rest in [0, 1, 2] {
        let bodies = Arc::new(std::sync::Mutex::new(Vec::<(String, Vec<u8>)>::new()));
        let seen = bodies.clone();
        let http = MockHttpServer::spawn(move |request| {
            let mut seen = seen.lock().unwrap();
            seen.push((request.path.clone(), request.body.clone()));
            let attempts = seen.iter().filter(|(p, _)| *p == request.path).count();
            if attempts == 1 {
                return if rest == 2 {
                    json_body(504, "text/html", "<html>Gateway Timeout</html>")
                } else {
                    catching_up(rest == 1)
                };
            }
            match request.path.as_str() {
                GET_TRIGGER_PATH => connect_proto_ok(&GetTriggerResponse {
                    trigger: Trigger {
                        trigger_id: 77,
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }),
                LIST_TRIGGERS_PATH => connect_proto_ok(&ListTriggersResponse {
                    triggers: vec![Trigger {
                        trigger_id: 77,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                "/triggers.v1.TriggersService/ListTriggerEvents" => {
                    connect_proto_ok(&ListTriggerEventsResponse::default())
                }
                _ => hardening_support::HttpScript::NotFound,
            }
        })
        .await;
        let client = api_key_client(&http);
        let trigger = client
            .triggers
            .get(GetTriggerRequest {
                trigger_id: 77,
                ..Default::default()
            })
            .await
            .expect("the retry succeeds")
            .expect("trigger");
        assert_eq!(trigger.trigger_id, polyester::codecs::format_uint64_id(77));
        let list = client
            .triggers
            .list(ListTriggersRequest {
                limit: 5,
                ..Default::default()
            })
            .await
            .expect("the retried list succeeds");
        assert_eq!(list.triggers.len(), 1);

        let events = client
            .triggers
            .list_events(ListTriggerEventsRequest {
                trigger_id: 77,
                subaccount_id: Some(12),
                limit: 17,
                page_token: "next-page".into(),
                event_type: polyester::proto::triggers::v1::TriggerEventType::EventFired.into(),
                ..Default::default()
            })
            .await
            .expect("events retry succeeds");
        assert!(events.events.is_empty());
        let seen = bodies.lock().unwrap();
        for path in [
            GET_TRIGGER_PATH,
            LIST_TRIGGERS_PATH,
            "/triggers.v1.TriggersService/ListTriggerEvents",
        ] {
            let attempts: Vec<_> = seen.iter().filter(|(p, _)| p == path).collect();
            assert_eq!(attempts.len(), 2, "{path} rest={rest}");
            assert!(
                attempts.iter().all(|(_, body)| *body == attempts[0].1),
                "{path}: every retry sends the same request"
            );
        }
    }
}

#[tokio::test]
async fn l2_trigger_reads_give_up_after_about_two_seconds() {
    use polyester::proto::triggers::v1::GetTriggerRequest;

    let http = MockHttpServer::spawn(|_| catching_up(false)).await;
    let client = api_key_client(&http);
    let started = Instant::now();
    let err = client
        .triggers
        .get(GetTriggerRequest {
            trigger_id: 77,
            ..Default::default()
        })
        .await
        .expect_err("still unavailable");
    let elapsed = started.elapsed();
    assert!(
        matches!(err, Error::Server(_)) && !err.is_retryable(),
        "{err:?}"
    );
    // 25, 50, 100, 200, 400, 500, 500 ms of backoff: at most eight attempts
    // (fewer when a slow host stretches the requests). No retry starts
    // past the 2 s budget, and the last delay is at most 500 ms, so it stops
    // between 1.5 s and 2 s plus one request.
    let attempts = http.requests.load(Ordering::SeqCst);
    assert!((2..=8).contains(&attempts), "{attempts} attempts");
    assert!(
        elapsed >= Duration::from_millis(1_500) && elapsed < Duration::from_secs(10),
        "{elapsed:?}"
    );
}

#[tokio::test]
async fn l2_trigger_reads_do_not_retry_other_errors() {
    use polyester::proto::triggers::v1::GetTriggerRequest;

    let http = MockHttpServer::spawn(|_| {
        json_body(
            500,
            "application/json",
            r#"{"code":"internal","message":"boom"}"#,
        )
    })
    .await;
    let err = api_key_client(&http)
        .triggers
        .get(GetTriggerRequest {
            trigger_id: 77,
            ..Default::default()
        })
        .await
        .expect_err("internal");
    assert!(matches!(err, Error::Server(_)), "{err:?}");
    assert_eq!(http.requests.load(Ordering::SeqCst), 1);
}

fn batch_status(
    phase: polyester::proto::orders::v1::BatchReplacePhase,
    code: &str,
) -> hardening_support::HttpScript {
    use polyester::proto::orders::v1::{
        BatchReplaceAdmissionStatus, BatchReplaceStatusItem, GetBatchReplaceStatusResponse,
    };
    let accepted_ts_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    connect_proto_ok(&GetBatchReplaceStatusResponse {
        batch_request_id: 4242,
        admission_status: BatchReplaceAdmissionStatus::Admitted.into(),
        items: vec![BatchReplaceStatusItem {
            item_index: 0,
            phase: phase.into(),
            old_order_id: 11,
            code: code.into(),
            ..Default::default()
        }],
        accepted_count: 1,
        rejected_count: 0,
        accepted_ts_ns,
        ..Default::default()
    })
}

// An admitted item refused when applied leaves `admitted` for `rejected`
// with its code; wait_batch_replace polls until then.
#[tokio::test]
async fn l2_wait_batch_replace_polls_until_no_item_is_admitted() {
    use polyester::proto::orders::v1::BatchReplacePhase;

    for gateway in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let http = MockHttpServer::spawn(move |request| {
            assert_eq!(request.path, BATCH_STATUS_PATH);
            match counter.fetch_add(1, Ordering::SeqCst) {
                // Right after admission the status is not readable yet.
                0 => json_body(
                    404,
                    "application/json",
                    r#"{"code":"not_found","message":"batch replace status not found"}"#,
                ),
                1 => batch_status(BatchReplacePhase::Admitted, ""),
                2 => {
                    if gateway {
                        json_body(504, "text/html", "Gateway Timeout")
                    } else {
                        catching_up(false)
                    }
                }
                _ => batch_status(BatchReplacePhase::Rejected, "POST_ONLY_CROSS"),
            }
        })
        .await;
        let status = api_key_client(&http)
            .orders
            .wait_batch_replace(&polyester::codecs::format_uint64_id(4242), None, None)
            .await
            .expect("settled");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let item = &status.items[0];
        assert_eq!(item.phase, "rejected");
        assert_eq!(
            item.error_code().as_deref(),
            Some("ERROR_CODE_POST_ONLY_CROSS")
        );
        assert_eq!(item.replacement_order_id, "");
    }
}

#[tokio::test]
async fn l2_wait_batch_replace_stops_at_its_deadline_and_surfaces_errors() {
    use polyester::proto::orders::v1::BatchReplacePhase;

    let http = MockHttpServer::spawn(|_| batch_status(BatchReplacePhase::Admitted, "")).await;
    let err = api_key_client(&http)
        .orders
        .wait_batch_replace("1", None, Some(Duration::from_millis(300)))
        .await
        .expect_err("still admitted");
    assert!(
        matches!(&err, Error::Transport(message) if message.contains("reconcile")),
        "{err:?}"
    );

    // A status that never appears ends at the deadline too.
    let http = MockHttpServer::spawn(|_| {
        json_body(
            404,
            "application/json",
            r#"{"code":"not_found","message":"batch replace status not found"}"#,
        )
    })
    .await;
    let err = api_key_client(&http)
        .orders
        .wait_batch_replace("1", None, Some(Duration::from_millis(300)))
        .await
        .expect_err("never found");
    assert!(
        matches!(&err, Error::Transport(message) if message.contains("reconcile")),
        "{err:?}"
    );

    // Other errors return at once.
    let http = MockHttpServer::spawn(|_| {
        json_body(
            403,
            "application/json",
            r#"{"code":"permission_denied","message":"no"}"#,
        )
    })
    .await;
    let err = api_key_client(&http)
        .orders
        .wait_batch_replace("1", None, None)
        .await
        .expect_err("denied");
    assert!(!matches!(err, Error::Transport(_)), "{err:?}");
    assert_eq!(http.requests.load(Ordering::SeqCst), 1);
}

// A definite REST refusal returns at once with its typed reason. Real
// unavailability still retries: l2_trigger_reads_retry_unavailable_* cover it.
#[tokio::test]
async fn l2_trigger_reads_return_definite_rest_refusals_at_once() {
    use polyester::proto::triggers::v1::GetTriggerRequest;
    const PROBLEM: &str = "application/problem+json";
    for (status, body, code) in [
        (
            401,
            r#"{"code":"IP_NOT_ALLOWED","detail":"x"}"#,
            "IP_NOT_ALLOWED",
        ),
        (
            403,
            r#"{"code":"PERMISSION_DENIED","detail":"x"}"#,
            "PERMISSION_DENIED",
        ),
        (
            400,
            r#"{"code":"BAD_PRICE","detail":"x"}"#,
            "ERROR_CODE_BAD_PRICE",
        ),
        (
            429,
            r#"{"code":"RATE_LIMIT_EXCEEDED","detail":"x"}"#,
            "ERROR_CODE_RATE_LIMIT_EXCEEDED",
        ),
        (
            503,
            r#"{"code":"ACCOUNT_ROUTING_INVALID","detail":"x"}"#,
            "ERROR_CODE_ACCOUNT_ROUTING_INVALID",
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let http = MockHttpServer::spawn(move |request| {
            if request.path == "/triggers.v1.TriggersService/GetTrigger" {
                counted.fetch_add(1, Ordering::SeqCst);
                json_body(status, PROBLEM, body)
            } else {
                hardening_support::HttpScript::NotFound
            }
        })
        .await;
        let err = api_key_client(&http)
            .triggers
            .get(GetTriggerRequest {
                trigger_id: 9,
                ..Default::default()
            })
            .await
            .expect_err("refused");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{status} {body}: {err:?}");
        assert!(format!("{err:?}").contains(code), "{err:?}");
    }
}

// A network failure across the whole window reports like any wait that proves
// nothing: reconcile through the orders, and don't treat the batch as refused.
#[tokio::test]
async fn l2_wait_batch_replace_reports_a_transport_failure_during_its_window() {
    let refused = hardening_support::RefusedPort::bind();
    let client = Client::new(Config {
        api_url: refused.http_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");

    // One read shows the failure really is the network, not an answer.
    let direct = client
        .orders
        .get_batch_replace_status("1", None)
        .await
        .expect_err("nothing listens");
    assert!(
        matches!(&direct, Error::Server(message) if message.contains("HTTP request failed")),
        "{direct:?}"
    );

    let started = Instant::now();
    let err = client
        .orders
        .wait_batch_replace("1", None, Some(Duration::from_millis(300)))
        .await
        .expect_err("nothing listens");
    assert!(
        matches!(&err, Error::Transport(message) if message.contains("reconcile")),
        "{err:?}"
    );
    assert!(err.mutation_outcome_unknown(), "{err:?}");
    assert!(!err.is_retryable(), "{err:?}");
    // It polled through the failure rather than returning on the first one.
    assert!(started.elapsed() >= Duration::from_millis(100), "{err:?}");
}

// A test of a transport failure dials an address nothing serves, and it must
// stay that way for the whole test: tests run in parallel, and a listener
// another test spawns must not take its port and answer its requests.
#[tokio::test]
async fn l2_refused_port_stays_refused_while_other_tests_listen() {
    let refused = hardening_support::RefusedPort::bind();
    let addr = refused.addr();
    // A concurrent test's listener can't take the port, by number or by chance.
    assert!(
        std::net::TcpListener::bind(addr).is_err(),
        "another listener took {addr}"
    );
    let others: Vec<_> = (0..256)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind"))
        .collect();
    assert!(
        others
            .iter()
            .all(|listener| listener.local_addr().expect("addr") != addr),
        "another listener took {addr}"
    );
    drop(others);
    let err = tokio::net::TcpStream::connect(addr)
        .await
        .expect_err("nothing serves the port");
    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused, "{err}");
}

// A ledger refusal keeps its stable code, known and final.
#[tokio::test]
async fn l2_balances_read_reports_the_ledger_error_code() {
    use base64::Engine as _;
    use polyester::proto::ledger::read::v1::{ErrorCode, ErrorDetail};
    let detail = ErrorDetail {
        code: ErrorCode::InvalidSubaccountId.into(),
        ..Default::default()
    };
    let value = base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_bytes());
    let body = json!({
        "code": "invalid_argument",
        "message": "subaccount_id is not valid",
        "details": [{"type": "ledger.read.v1.ErrorDetail", "value": value}],
    })
    .to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let http = MockHttpServer::spawn(move |request| {
        if request
            .path
            .starts_with("/ledger.read.v1.LedgerReadService/")
        {
            counted.fetch_add(1, Ordering::SeqCst);
            json_body(400, "application/json", &body)
        } else {
            hardening_support::HttpScript::NotFound
        }
    })
    .await;
    let err = api_key_client(&http)
        .balances
        .list(Default::default())
        .await
        .expect_err("refused");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "ERROR_CODE_INVALID_SUBACCOUNT_ID"),
        "{err:?}"
    );
    assert!(!err.is_retryable(), "{err:?}");
    assert!(!err.mutation_outcome_unknown(), "{err:?}");
}

fn connect_auth_error(
    connect_code: &str,
    code: polyester::proto::auth::v1::AuthErrorCode,
    message: &str,
) -> String {
    use base64::Engine as _;
    use polyester::proto::auth::v1::AuthErrorDetail;
    let detail = AuthErrorDetail {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    };
    let value = base64::engine::general_purpose::STANDARD_NO_PAD.encode(detail.encode_to_bytes());
    json!({
        "code": connect_code,
        "message": message,
        "details": [{"type": "auth.v1.AuthErrorDetail", "value": value}],
    })
    .to_string()
}

// Order reads, order and trigger writes and balance reads answer a refused
// API key with a ConnectRPC error whose `auth.v1.AuthErrorDetail` names the
// reason; the REST problem answers the same reason as its `code`.
#[tokio::test]
async fn l2_api_key_refusal_reasons_read_the_same_on_connect_and_rest() {
    use polyester::proto::auth::v1::AuthErrorCode;

    for (code, rest_code, retryable) in [
        (
            AuthErrorCode::AUTH_SIGNATURE_INVALID,
            "SIGNATURE_INVALID",
            false,
        ),
        (AuthErrorCode::AUTH_TIMESTAMP_SKEW, "TIMESTAMP_SKEW", true),
        (
            AuthErrorCode::AUTH_MISSING_HEADERS,
            "MISSING_HEADERS",
            false,
        ),
        (AuthErrorCode::AUTH_KEY_EXPIRED, "KEY_EXPIRED", false),
        (AuthErrorCode::AUTH_KEY_DISABLED, "KEY_DISABLED", false),
        (AuthErrorCode::AUTH_IP_NOT_ALLOWED, "IP_NOT_ALLOWED", false),
        (
            AuthErrorCode::AUTH_API_KEY_NOT_ACCEPTED,
            "API_KEY_NOT_ACCEPTED",
            false,
        ),
    ] {
        let connect = connect_auth_error("unauthenticated", code, "refused");
        let rest = json!({"status": 401, "code": rest_code, "title": "Unauthorized"}).to_string();
        for (content_type, body) in [
            ("application/json", connect),
            ("application/problem+json", rest),
        ] {
            let http = MockHttpServer::spawn(move |_| json_body(401, content_type, &body)).await;
            let err = api_key_client(&http)
                .orders
                .list_open(None)
                .await
                .expect_err("refused");
            assert_eq!(err.auth_code(), Some(code), "{content_type}: {err:?}");
            assert_eq!(
                err.auth_error_code(),
                Some(rest_code),
                "{content_type}: {err:?}"
            );
            assert_eq!(err.is_retryable(), retryable, "{content_type}: {err:?}");
            if retryable {
                assert!(
                    matches!(err, Error::TimestampSkew { status: 401, .. }),
                    "{err:?}"
                );
            } else {
                assert!(
                    matches!(&err, Error::ApiKeyRejected { status: 401, .. }),
                    "{err:?}"
                );
            }
        }
    }

    // No credentials, or a missing or expired session: REST `UNAUTHENTICATED`.
    let http = MockHttpServer::spawn(|_| {
        json_body(
            401,
            "application/problem+json",
            r#"{"status":401,"code":"UNAUTHENTICATED"}"#,
        )
    })
    .await;
    let err = api_key_client(&http)
        .orders
        .list_open(None)
        .await
        .expect_err("refused");
    assert_eq!(
        err.auth_code(),
        Some(AuthErrorCode::AUTH_AUTHENTICATION_REQUIRED),
        "{err:?}"
    );
}

// Every session-only method refuses an account API key with
// AUTH_SESSION_KIND_NOT_ALLOWED: HTTP 403, Connect `permission_denied` with an
// AuthErrorDetail, or a REST problem with the code. Both read as one error.
#[tokio::test]
async fn l2_session_only_refusal_is_session_required_on_connect_and_rest() {
    use polyester::proto::auth::v1::AuthErrorCode;

    let connect = connect_auth_error(
        "permission_denied",
        AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED,
        "interactive session required",
    );
    let rest = r#"{"type":"https://polyester.dev/problems/auth-session-kind-not-allowed","title":"Interactive Session Required","status":403,"detail":"interactive session required","code":"AUTH_SESSION_KIND_NOT_ALLOWED"}"#.to_owned();
    for (content_type, body) in [
        ("application/json", connect),
        ("application/problem+json", rest),
    ] {
        let http = MockHttpServer::spawn(move |_| json_body(403, content_type, &body)).await;
        let err = api_key_client(&http)
            .orders
            .list_open(None)
            .await
            .expect_err("refused");
        assert!(
            matches!(&err, Error::SessionRequired { message, .. } if message == "interactive session required"),
            "{content_type}: {err:?}"
        );
        assert!(err.is_session_required());
        assert_eq!(
            err.auth_code(),
            Some(AuthErrorCode::AUTH_SESSION_KIND_NOT_ALLOWED)
        );
        assert!(!err.is_retryable());
    }
}

// Cancel-all cancels open orders, never triggers. stop_all_trading pages
// through the live triggers, cancels each, and only then cancels all orders.
#[tokio::test]
async fn l2_stop_all_trading_cancels_live_triggers_before_the_orders() {
    use polyester::models::StopAllTradingScope;
    use polyester::proto::orders::v1::{CancelAllOrdersRequest, CancelAllOrdersResponse};
    use polyester::proto::triggers::v1::{
        CancelTriggerRequest, CancelTriggerResponse, ListTriggersRequest, ListTriggersResponse,
        Trigger, TriggerStatus,
    };

    const CANCEL_TRIGGER: &str = "/triggers.v1.TriggersService/CancelTrigger";
    const CANCEL_ALL: &str = "/orders.v1.OrdersService/CancelAllOrders";
    let calls = Arc::new(std::sync::Mutex::new(Vec::<(String, Vec<u8>)>::new()));
    let seen = calls.clone();
    let http = MockHttpServer::spawn(move |request| {
        seen.lock()
            .unwrap()
            .push((request.path.clone(), request.body.clone()));
        let trigger = |id| Trigger {
            trigger_id: id,
            status: TriggerStatus::STATUS_ARMED.into(),
            ..Default::default()
        };
        match request.path.as_str() {
            LIST_TRIGGERS_PATH => {
                let req = ListTriggersRequest::decode_from_slice(&request.body).unwrap();
                if req.page_token.is_empty() {
                    connect_proto_ok(&ListTriggersResponse {
                        triggers: vec![trigger(11), trigger(12)],
                        next_page_token: "page-2".into(),
                        ..Default::default()
                    })
                } else {
                    connect_proto_ok(&ListTriggersResponse {
                        triggers: vec![trigger(13)],
                        ..Default::default()
                    })
                }
            }
            CANCEL_TRIGGER => {
                let req = CancelTriggerRequest::decode_from_slice(&request.body).unwrap();
                if req.trigger_id == 12 {
                    return json_body(
                        400,
                        "application/json",
                        r#"{"code":"failed_precondition","message":"trigger already ended"}"#,
                    );
                }
                connect_proto_ok(&CancelTriggerResponse {
                    trigger_id: req.trigger_id,
                    status: TriggerStatus::STATUS_CANCELED.into(),
                    ..Default::default()
                })
            }
            CANCEL_ALL => connect_proto_ok(&CancelAllOrdersResponse {
                status: polyester::proto::orders::v1::cancel_all_orders_response::Status::SUBMITTED
                    .into(),
                matched_orders: 2,
                submitted_cancels: 2,
                ..Default::default()
            }),
            _ => hardening_support::HttpScript::NotFound,
        }
    })
    .await;
    let client = api_key_client(&http);
    let result = client
        .stop_all_trading(StopAllTradingScope {
            request_id: Some("stop-all-1".into()),
            ..Default::default()
        })
        .await
        .expect("stop all trading");

    let id = polyester::codecs::format_uint64_id;
    assert_eq!(result.canceled_trigger_ids, vec![id(11), id(13)]);
    assert_eq!(result.trigger_failures.len(), 1);
    assert_eq!(result.trigger_failures[0].trigger_id, id(12));
    assert_eq!(result.orders.matched_orders, 2);

    let calls = calls.lock().unwrap();
    let paths: Vec<&str> = calls.iter().map(|(path, _)| path.as_str()).collect();
    assert_eq!(
        paths,
        [
            LIST_TRIGGERS_PATH,
            LIST_TRIGGERS_PATH,
            CANCEL_TRIGGER,
            CANCEL_TRIGGER,
            CANCEL_TRIGGER,
            CANCEL_ALL
        ],
        "every trigger is canceled before the orders"
    );
    for (n, (_, body)) in calls.iter().take(2).enumerate() {
        let req = ListTriggersRequest::decode_from_slice(body).unwrap();
        let statuses: Vec<_> = req.status.iter().map(|s| s.to_i32()).collect();
        assert_eq!(
            statuses,
            [
                TriggerStatus::STATUS_CREATED as i32,
                TriggerStatus::STATUS_ARMED as i32,
                TriggerStatus::STATUS_RUNNING as i32,
                TriggerStatus::STATUS_PAUSED as i32
            ]
        );
        assert_eq!(req.limit, 1000);
        assert_eq!(req.page_token, if n == 0 { "" } else { "page-2" });
    }
    let cancel_all = CancelAllOrdersRequest::decode_from_slice(&calls[5].1).unwrap();
    assert_eq!(cancel_all.request_id, "stop-all-1");
}

// A page token seen earlier in the run, not only the one just sent, is a
// pagination cycle: stop_all_trading stops listing and cancels nothing.
#[tokio::test]
async fn l2_stop_all_trading_refuses_a_page_token_cycle_before_canceling() {
    use polyester::models::StopAllTradingScope;
    use polyester::proto::triggers::v1::{ListTriggersResponse, Trigger, TriggerStatus};

    for (name, cycle) in [
        ("A,A", vec!["A", "A"]),
        ("A,B,A", vec!["A", "B", "A"]),
        ("A,B,C,A", vec!["A", "B", "C", "A"]),
        ("A,B,C,D,B", vec!["A", "B", "C", "D", "B"]),
    ] {
        // The server keeps answering the cycle; a twelfth listing fails so a
        // missed cycle cannot hang the test.
        let first = cycle
            .iter()
            .position(|t| *t == cycle[cycle.len() - 1])
            .unwrap();
        let mut tokens: Vec<String> = cycle.iter().map(|t| (*t).to_owned()).collect();
        while tokens.len() < 24 {
            tokens.extend(cycle[first + 1..].iter().map(|t| (*t).to_owned()));
        }
        let paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = paths.clone();
        let http = MockHttpServer::spawn(move |request| {
            let mut paths = seen.lock().unwrap();
            paths.push(request.path.clone());
            if request.path != LIST_TRIGGERS_PATH {
                return hardening_support::HttpScript::NotFound;
            }
            let listed = paths.len();
            if listed > 11 {
                return json_body(
                    409,
                    "application/json",
                    r#"{"code":"aborted","message":"loop"}"#,
                );
            }
            connect_proto_ok(&ListTriggersResponse {
                triggers: vec![Trigger {
                    trigger_id: listed as u64,
                    status: TriggerStatus::STATUS_ARMED.into(),
                    ..Default::default()
                }],
                next_page_token: tokens[listed - 1].clone(),
                ..Default::default()
            })
        })
        .await;
        let client = api_key_client(&http);
        let result = client
            .stop_all_trading(StopAllTradingScope::default())
            .await;
        assert!(
            matches!(result, Err(Error::ResponseContract { ref message, .. }) if message.contains("repeats an earlier page_token")),
            "{name}: {result:?}"
        );
        let paths = paths.lock().unwrap();
        assert_eq!(
            *paths,
            vec![LIST_TRIGGERS_PATH.to_owned(); cycle.len()],
            "{name}: a cancel began, or the cycle was read past its first repeat"
        );
    }
}

// A JSON-wire response naming an enum value newer than this SDK reads it as
// unknown instead of rejecting the whole response (changelog sections and
// change surfaces gain product areas over time).
#[tokio::test]
async fn l2_json_wire_reads_unknown_enum_names_instead_of_failing() {
    let http = MockHttpServer::spawn(|request| match request.path.as_str() {
        "/changelog.v1.ChangelogService/GetPost" => json_body(
            200,
            "application/json",
            r#"{"post":{"summary":{"slug":"2026-11-04","changeCount":2},"sections":[
                {"heading":"Market data","surface":"MARKET_DATA","changes":[{"id":"1","surfaces":["MARKET_DATA","FUTURE_SURFACE"]}]},
                {"heading":"Future","surface":"FUTURE_SURFACE","changes":[{"id":"2","category":"FUTURE_CATEGORY","surfaces":["FUTURE_SURFACE"]}]}]}}"#,
        ),
        _ => hardening_support::HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        hydrate_catalogs: false,
        wire_format: polyester::transport::WireFormat::Json,
        ..Default::default()
    })
    .expect("client");
    let post = client
        .changelog
        .get_post("2026-11-04")
        .await
        .expect("an unknown enum name must not reject the response");
    assert_eq!(post.sections.len(), 2);
    assert_eq!(post.sections[0].surface, "market_data");
    assert_eq!(post.sections[0].changes[0].surfaces, vec!["market_data"]);
    // A name this SDK does not know reads as unset: no product-area label.
    assert_eq!(post.sections[1].surface, "");
    assert!(post.sections[1].changes[0].surfaces.is_empty());
    assert_eq!(post.sections[1].changes[0].category, "");
}

// The same holds for trigger enums: an event type or refusal reason newer
// than this SDK reads as unset over JSON, and the known ones keep their labels.
#[tokio::test]
async fn l2_json_wire_reads_unknown_trigger_enum_names() {
    use polyester::proto::triggers::v1::ListTriggerEventsRequest;

    let http = MockHttpServer::spawn(|request| match request.path.as_str() {
        "/triggers.v1.TriggersService/ListTriggerEvents" => json_body(
            200,
            "application/json",
            r#"{"events":[
                {"triggerId":"7","triggerType":"FUTURE_TYPE","eventType":"EVENT_FUTURE","childSeq":1,"fireRefusalReason":"TRIGGER_FAILURE_REASON_FUTURE"},
                {"triggerId":"7","triggerType":"TAKE_PROFIT","eventType":"EVENT_FIRE_REFUSED","childSeq":2,"firePriceTicks":"104000000000","fireRefusalReason":"TRIGGER_FAILURE_REASON_ACCOUNT_REPAIRING"}],
              "nextPageToken":""}"#,
        ),
        _ => hardening_support::HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: polyester::transport::WireFormat::Json,
        ..Default::default()
    })
    .expect("client");
    let events = client
        .triggers
        .list_events(ListTriggerEventsRequest {
            trigger_id: 7,
            ..Default::default()
        })
        .await
        .expect("unknown trigger enum names must not reject the response");
    assert_eq!(events.events.len(), 2);
    let (future, refused) = (&events.events[0], &events.events[1]);
    assert_eq!(
        (future.event_type.as_str(), future.trigger_type.as_str()),
        ("", "")
    );
    assert_eq!(future.fire_refusal_reason, "");
    assert_eq!(refused.event_type, "fire_refused");
    assert_eq!(refused.trigger_type, "take_profit");
    assert_eq!(refused.fire_refusal_reason, "account_repairing");
    assert_eq!(refused.fire_price.as_ref().unwrap().format(), "104");
}

// GetSubaccount returns the sub-account's balances, in the shape of the
// balances list, only when `include_balances` asks for them.
#[tokio::test]
async fn l2_subaccount_get_requests_and_parses_balances_only_when_asked() {
    use polyester::proto::auth::v1::{GetSubaccountRequest, GetSubaccountResponse, Subaccount};
    use polyester::proto::ledger::read::v1::{
        AssetBalance as ProtoAssetBalance, GetBalancesResponse,
    };
    use polyester::proto::polyester::r#type::v1::U128;
    use polyester::services::GetSubaccountOpts;

    const ONE_POINT_FIVE_E18: u64 = 1_500_000_000_000_000_000;
    let seen = Arc::new(std::sync::Mutex::new(Vec::<bool>::new()));
    let sink = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path != "/auth.v1.SubaccountViewService/GetSubaccount" {
            return hardening_support::HttpScript::NotFound;
        }
        let request = GetSubaccountRequest::decode_from_slice(&req.body).expect("request decodes");
        sink.lock().unwrap().push(request.include_balances);
        let mut response = GetSubaccountResponse {
            subaccount: Subaccount {
                id: 42,
                label: "desk".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        if request.include_balances {
            response.balances = GetBalancesResponse {
                balances: vec![ProtoAssetBalance {
                    asset_id: 1,
                    trading: U128 {
                        hi: 0,
                        lo: ONE_POINT_FIVE_E18,
                        ..Default::default()
                    }
                    .into(),
                    available: U128 {
                        hi: 0,
                        lo: ONE_POINT_FIVE_E18,
                        ..Default::default()
                    }
                    .into(),
                    trading_revision: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .into();
        }
        connect_proto_ok(&response)
    })
    .await;
    let client = api_key_client(&http);

    let with = client
        .sub_accounts
        .get(
            &polyester::codecs::scalars::format_id(42),
            GetSubaccountOpts {
                include_balances: true,
                ..Default::default()
            },
        )
        .await
        .expect("get with balances");
    let balances = with.balances.expect("balances were requested");
    assert_eq!(balances.balances.len(), 1);
    let row = &balances.balances[0];
    assert_eq!(row.asset_id, 1);
    assert_eq!(format_ledger_u128(&row.trading, 18).unwrap(), "1.5");
    assert_eq!(format_ledger_u128(&row.available, 18).unwrap(), "1.5");
    assert_eq!((row.funding.as_str(), row.reserved.as_str()), ("0", "0"));
    assert_eq!((row.trading_revision, row.funding_revision), (3, 0));
    assert_eq!(with.subaccount.unwrap().label, "desk");

    let without = client
        .sub_accounts
        .get(
            &polyester::codecs::scalars::format_id(42),
            GetSubaccountOpts::default(),
        )
        .await
        .expect("get without balances");
    assert!(without.balances.is_none());
    assert_eq!(*seen.lock().unwrap(), [true, false]);
}

#[tokio::test]
async fn l2_execution_pages_pin_the_returned_generation() {
    use polyester::models::OrderKey;
    use polyester::proto::orders::v1::{
        GetOrderRequest, GetOrderResponse, Order, OrderStatus, UserTrade,
    };
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path != "/orders.v1.OrdersReadService/GetOrder" {
            return hardening_support::HttpScript::NotFound;
        }
        let request = GetOrderRequest::decode_from_slice(&req.body).unwrap();
        let mut seen = captured.lock().unwrap();
        seen.push(request);
        let first = seen.len() == 1;
        connect_proto_ok(&GetOrderResponse {
            order: Order {
                order_id: 9,
                symbol_id: 1,
                cum_qty_scaled: 100,
                inherited_cum_qty_scaled: 40,
                status: OrderStatus::Filled.into(),
                ..Default::default()
            }
            .into(),
            trades: vec![UserTrade {
                order_id: 9,
                symbol_id: 1,
                qty_scaled: if first { 40 } else { 60 },
                ..Default::default()
            }],
            next_page_token: if first {
                "older-executions".into()
            } else {
                String::new()
            },
            ..Default::default()
        })
    })
    .await;
    let result = api_key_client(&http)
        .orders
        .wait_for_order_trades_complete(
            OrderKey::ClientOrderId("reused-client-id".into()),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(result.trades.len(), 2);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    use polyester::proto::orders::v1::__buffa::oneof::get_order_request::Key;
    assert!(matches!(&seen[0].key,Some(Key::ClientOrderId(value)) if value=="reused-client-id"));
    assert!(
        matches!(seen[1].key, Some(Key::OrderId(9))),
        "cursor not pinned: {:?}",
        seen[1]
    );
    assert_eq!(seen[1].page_token, "older-executions");
}

#[tokio::test]
async fn l2_early_auth_problem_metadata_and_trigger_not_found() {
    for (code, expected_type) in [
        (
            "UNAUTHENTICATED",
            "https://polyester.dev/problems/unauthenticated",
        ),
        (
            "AUTH_AUTHENTICATION_REQUIRED",
            "https://polyester.dev/problems/auth-authentication-required",
        ),
        (
            "AUTH_SESSION_KIND_NOT_ALLOWED",
            "https://polyester.dev/problems/auth-session-kind-not-allowed",
        ),
    ] {
        for supplied_type in [
            None,
            Some("https://polyester.dev/problems/AUTH_SESSION_KIND_NOT_ALLOWED"),
        ] {
            let mut body = serde_json::json!({"code":code,"status":401,"detail":""});
            if let Some(value) = supplied_type {
                body["type"] = value.into();
            }
            let body = body.to_string();
            let http =
                MockHttpServer::spawn(move |_| json_body(401, "application/problem+json", &body))
                    .await;
            let err = api_key_client(&http)
                .auth
                .me()
                .await
                .expect_err("early auth refused");
            let problem = err.http_problem().expect("problem metadata");
            assert_eq!(problem.code, code);
            assert_eq!(problem.status, 401);
            assert_eq!(problem.detail, "");
            assert_eq!(problem.problem_type, expected_type);
            assert!(!err.is_retryable());
        }
    }
    let http = MockHttpServer::spawn(|_| {
        json_body(
            404,
            "application/json",
            r#"{"code":"not_found","message":"NOT_FOUND"}"#,
        )
    })
    .await;
    let client = api_key_client(&http);
    let get = client
        .triggers
        .get(polyester::proto::triggers::v1::GetTriggerRequest {
            trigger_id: 77,
            ..Default::default()
        })
        .await
        .expect_err("unknown trigger");
    let events = client
        .triggers
        .list_events(polyester::proto::triggers::v1::ListTriggerEventsRequest {
            trigger_id: 77,
            ..Default::default()
        })
        .await
        .expect_err("unknown trigger events");
    assert!(matches!(&get,Error::Api{code,..} if code=="NotFound"));
    assert!(matches!(&events,Error::Api{code,..} if code=="NotFound"));
    assert_eq!(
        http.requests.load(Ordering::SeqCst),
        2,
        "not-found is not retried"
    );
}

#[tokio::test]
async fn l2_rest_order_unavailable_classification() {
    for (code, status) in [
        ("RATE_LIMIT_POLICY_UNAVAILABLE", 503),
        ("POLICY_UNAVAILABLE", 503),
        ("FEE_SCHEDULE_UNAVAILABLE", 503),
        ("UNAVAILABLE", 503),
        ("PAIR_BACKLOGGED", 503),
        ("ORDER_MODIFY_IN_PROGRESS", 503),
        ("ORDER_PLACEMENT_PENDING", 503),
        ("SETTLEMENT_BACKLOGGED", 503),
        ("TRIGGER_CONTROLS_BUSY", 503),
        ("TRIGGER_CONTROL_IN_PROGRESS", 503),
        ("ACCOUNT_ROUTING_INVALID", 503),
        ("UPSTREAM_ERROR", 502),
        ("SERVICE_UNAVAILABLE", 503),
        ("ACCOUNT_SHARD_UNAVAILABLE", 503),
        ("CAPITAL_VIEW_UNAVAILABLE", 503),
        ("CHAIN_METADATA_UNAVAILABLE", 503),
        ("DESTINATION_VALIDATION_UNAVAILABLE", 503),
        ("FEE_UNAVAILABLE", 503),
        ("STEP_UP_UNAVAILABLE", 503),
        ("SUPPLY_UNAVAILABLE", 503),
    ] {
        let body = json!({"code": code, "status": status, "detail": "retry later"}).to_string();
        let http =
            MockHttpServer::spawn(move |_| json_body(status, "application/problem+json", &body))
                .await;
        let err = api_key_client(&http)
            .auth
            .me()
            .await
            .expect_err("unavailable");
        let known = matches!(
            code,
            "RATE_LIMIT_POLICY_UNAVAILABLE"
                | "POLICY_UNAVAILABLE"
                | "FEE_SCHEDULE_UNAVAILABLE"
                | "PAIR_BACKLOGGED"
                | "ORDER_MODIFY_IN_PROGRESS"
                | "ORDER_PLACEMENT_PENDING"
                | "SETTLEMENT_BACKLOGGED"
                | "TRIGGER_CONTROLS_BUSY"
                | "TRIGGER_CONTROL_IN_PROGRESS"
        );
        assert!(
            matches!(
                err,
                Error::Server(_) | Error::AdmissionUnavailable { .. } | Error::Api { .. }
            ),
            "{code}: {err:?}"
        );
        assert!(!matches!(err, Error::Auth(_)));
        assert_eq!(err.is_retryable(), known, "{code}: {err:?}");
        assert_eq!(
            err.mutation_outcome_unknown(),
            !known && code != "ACCOUNT_ROUTING_INVALID",
            "{code}: {err:?}"
        );
    }
}

#[tokio::test]
async fn l2_rest_outcome_unknown_matches_connect() {
    use buffa::Enumeration;
    use polyester::proto::orders::v1::ErrorCode;
    for code in ErrorCode::values()
        .iter()
        .filter(|c| c.proto_name().ends_with("_OUTCOME_UNKNOWN"))
    {
        let name = code.proto_name();
        for status in [504, 503] {
            let body = json!({"code": name.trim_start_matches("ERROR_CODE_"), "detail": "read back first"}).to_string();
            let http = MockHttpServer::spawn(move |_| {
                json_body(status, "application/problem+json", &body)
            })
            .await;
            let err = api_key_client(&http)
                .auth
                .me()
                .await
                .expect_err("unknown outcome");
            assert!(
                matches!(&err, Error::Api { code, .. } if code == name),
                "{name}: {err:?}"
            );
            assert!(!err.is_retryable(), "{err:?}");
            assert!(err.mutation_outcome_unknown(), "{err:?}");
            assert_eq!(err.to_string(), "deadline_exceeded: read back first");
            assert_eq!(http.requests.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn l2_unrecognised_rest_504_remains_ambiguous_transport_failure() {
    for (content_type, body) in [
        ("text/html", "<html>Gateway Timeout</html>"),
        (
            "application/problem+json",
            r#"{"code":"UNKNOWN_GATEWAY_FAILURE"}"#,
        ),
    ] {
        let http = MockHttpServer::spawn(move |_| json_body(504, content_type, body)).await;
        let err = api_key_client(&http)
            .auth
            .me()
            .await
            .expect_err("gateway timeout");
        assert!(matches!(err, Error::GatewayTimeout { .. }), "{err:?}");
        assert!(!err.is_retryable());
        assert!(err.mutation_outcome_unknown());
        assert_eq!(http.requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn l2_order_create_never_resends_unknown_outcome() {
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};
    for client_order_id in [None, Some("reconcile-first")] {
        for (status, content_type, body) in [
            (
                504,
                "application/problem+json",
                r#"{"code":"ORDER_CREATE_OUTCOME_UNKNOWN"}"#,
            ),
            (504, "text/html", "<html>Gateway Timeout</html>"),
            (502, "text/plain", ""),
            (503, "text/html", "<html>Unavailable</html>"),
            (504, "application/json", r#"{"code":"deadline_exceeded"}"#),
            (503, "application/json", r#"{"code":"unavailable"}"#),
            (
                503,
                "application/json",
                r#"{"code":"unavailable","details":[{"type":"orders.v1.ErrorDetail","value":"CK8C"}]}"#,
            ),
            (
                504,
                "application/json",
                r#"{"code":"deadline_exceeded","details":[{"type":"orders.v1.ErrorDetail","value":"CK8C"}]}"#,
            ),
        ] {
            let http = MockHttpServer::spawn(move |request| {
                assert_eq!(request.path, "/orders.v1.OrdersService/CreateOrder");
                json_body(status, content_type, body)
            })
            .await;
            let client = api_key_client(&http);
            client
                .catalogs
                .hydrate_spot_config_json(json!({"pairs": [{
                    "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8
                }]}))
                .unwrap();
            let mut params =
                CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Market);
            params.quantity = Some(
                Quantity::from_scaled(
                    1,
                    Some(8),
                    QuantityDomain::OrderBase,
                    Some("BTC-USDT".into()),
                    Some(1),
                )
                .unwrap(),
            );
            params.client_order_id = client_order_id.map(str::to_owned);
            let err = client
                .orders
                .create(params)
                .await
                .expect_err("unknown outcome");
            assert!(err.mutation_outcome_unknown(), "{err:?}");
            assert_eq!(http.requests.load(Ordering::SeqCst), 1);
            assert!(!err.is_retryable());
            if body.contains("OUTCOME_UNKNOWN") || body.contains("details") {
                assert!(matches!(err, Error::Api { .. }), "{err:?}");
                assert!(!err.is_retryable());
            }
        }
    }
}

#[tokio::test]
async fn l2_transfer_paging_preserves_opaque_token_scope_and_equal_timestamps() {
    use polyester::proto::ledger::read::v1::{
        ListTransfersRequest, ListTransfersResponse, TransferRow,
    };
    use polyester::transport::WireFormat;
    use std::sync::Mutex;
    for json_wire in [false, true] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let server = MockHttpServer::spawn(move |req| {
            assert!(req.path.ends_with("/ListTransfers"));
            let request: ListTransfersRequest = if json_wire {
                serde_json::from_slice(&req.body).unwrap()
            } else {
                ListTransfersRequest::decode_from_slice(&req.body).unwrap()
            };
            let mut seen = observed.lock().unwrap();
            seen.push(request);
            let index = seen.len();
            let response = ListTransfersResponse {
                transfers: vec![TransferRow {
                    ts_us: 999,
                    transfer_id: index.to_string(),
                    ..Default::default()
                }],
                next_page_token: if index == 1 {
                    "eyJ2IjoxfQ==".into()
                } else {
                    String::new()
                },
                ..Default::default()
            };
            if json_wire {
                hardening_support::HttpScript::Json {
                    status: 200,
                    body: serde_json::to_vec(&response).unwrap(),
                }
            } else {
                connect_proto_ok(&response)
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: server.base_url(),
            api_key_id: Some("scope-test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            wire_format: if json_wire {
                WireFormat::Json
            } else {
                WireFormat::Binary
            },
            ..Default::default()
        })
        .unwrap();
        let mut request = ListTransfersRequest {
            limit: 1,
            reversed: true,
            subaccount_id: Some(42),
            ts_min_us: 100,
            ..Default::default()
        };
        let first = client.transfers.list(request.clone()).await.unwrap();
        assert_eq!(first.next_page_token, "eyJ2IjoxfQ==");
        request.page_token = first.next_page_token;
        // The balances facade shares this decoder and accepts the same wire request.
        let second = client.balances.list_transfers(request).await.unwrap();
        assert!(second.next_page_token.is_empty());
        assert_eq!(first.transfers[0].timestamp, 999);
        assert_eq!(second.transfers[0].timestamp, 999);
        assert_ne!(
            first.transfers[0].transfer_id,
            second.transfers[0].transfer_id
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].page_token, "");
        assert_eq!(seen[1].page_token, "eyJ2IjoxfQ==");
        let mut next = seen[1].clone();
        next.page_token.clear();
        assert_eq!(seen[0], next);
    }
}

#[tokio::test]
async fn l2_equity_history_keeps_wire_btc_closes_in_both_codecs() {
    use polyester::proto::ledger::read::v1::{
        EquitySeries, GetEquityHistorySeriesRequest, GetEquityHistorySeriesResponse,
    };
    use polyester::transport::WireFormat;
    for json_wire in [false, true] {
        let server = MockHttpServer::spawn(move |req| {
            assert!(req.path.ends_with("/GetEquityHistorySeries"));
            let response = GetEquityHistorySeriesResponse {
                points: 2,
                start_ts_sec: 100,
                end_ts_sec: 400,
                bucket: "5m".into(),
                btc_prices_q: vec![60000000000001, 61000000000009],
                series: vec![EquitySeries {
                    equity_q: vec![12345, -6789],
                    ..Default::default()
                }],
                ..Default::default()
            };
            if json_wire {
                hardening_support::HttpScript::Json {
                    status: 200,
                    body: serde_json::to_vec(&response).unwrap(),
                }
            } else {
                connect_proto_ok(&response)
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: server.base_url(),
            api_key_id: Some("read-test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            wire_format: if json_wire {
                WireFormat::Json
            } else {
                WireFormat::Binary
            },
            ..Default::default()
        })
        .unwrap();
        let result = client
            .balances
            .get_equity_history(GetEquityHistorySeriesRequest::default())
            .await
            .unwrap();
        assert_eq!(server.requests.load(Ordering::SeqCst), 1);
        assert_eq!(result.btc_prices_q, vec![60000000000001, 61000000000009]);
        assert_eq!(result.series[0].equity_q, vec![12345, -6789]);
        assert_eq!(
            (result.start_ts_sec, result.end_ts_sec, result.points),
            (100, 400, 2)
        );
    }
}

#[tokio::test]
async fn l2_heatmap_paging_keeps_filters_without_a_new_window() {
    use buffa_types::google::protobuf::Timestamp;
    use polyester::proto::marketdata::v1::{
        GetOrderbookHeatmapRequest, GetOrderbookHeatmapResponse, HeatmapTimeRange,
    };
    use polyester::services::GetHeatmapOptions;
    use polyester::transport::WireFormat;
    use std::sync::Mutex;
    for json_wire in [false, true] {
        for explicit in [false, true] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let observed = seen.clone();
            let server = MockHttpServer::spawn(move |req| {
                assert!(req.path.ends_with("/GetOrderbookHeatmap"));
                let request: GetOrderbookHeatmapRequest = if json_wire {
                    serde_json::from_slice(&req.body).unwrap()
                } else {
                    GetOrderbookHeatmapRequest::decode_from_slice(&req.body).unwrap()
                };
                let mut seen = observed.lock().unwrap();
                seen.push(request);
                let response = GetOrderbookHeatmapResponse {
                    symbol_id: 1,
                    last_persisted_ts_sec: 100,
                    next_page_token: if seen.len() == 1 {
                        "eyJoZWF0bWFwIjoxfQ==".into()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                };
                if json_wire {
                    hardening_support::HttpScript::Json {
                        status: 200,
                        body: serde_json::to_vec(&response).unwrap(),
                    }
                } else {
                    connect_proto_ok(&response)
                }
            })
            .await;
            let client = Client::new(Config {
                api_url: server.base_url(),
                hydrate_catalogs: false,
                wire_format: if json_wire {
                    WireFormat::Json
                } else {
                    WireFormat::Binary
                },
                ..Default::default()
            })
            .unwrap();
            client
                .catalogs
                .hydrate_spot_config_json(
                    json!({"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":8}]}),
                )
                .unwrap();
            let range = explicit.then(|| HeatmapTimeRange {
                start_time: Timestamp {
                    seconds: 100,
                    ..Default::default()
                }
                .into(),
                end_time: Timestamp {
                    seconds: 400,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            });
            let first = if explicit {
                client
                    .heatmap
                    .get_with_options(
                        "BTC-USDT",
                        "1m",
                        50,
                        1,
                        "peak",
                        GetHeatmapOptions {
                            time_range: range.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
            } else {
                client
                    .heatmap
                    .get("BTC-USDT", "1m", 50, 1, "peak")
                    .await
                    .unwrap()
            };
            let token = first.raw["nextPageToken"]
                .as_str()
                .expect("nonempty token returned")
                .to_owned();
            assert_eq!(token, "eyJoZWF0bWFwIjoxfQ==");
            let second = client
                .heatmap
                .get_with_options(
                    "BTC-USDT",
                    "1m",
                    50,
                    1,
                    "peak",
                    GetHeatmapOptions {
                        time_range: range,
                        page_token: Some(token.clone()),
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                second
                    .raw
                    .get("nextPageToken")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                ""
            );
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            let (mut a, mut b) = (seen[0].clone(), seen[1].clone());
            assert_eq!(a.page_token, "");
            assert_eq!(b.page_token, token);
            assert!(a.time_range.is_set());
            if explicit {
                assert_eq!(a.time_range, b.time_range)
            } else {
                assert!(!b.time_range.is_set(), "continuation invented a window")
            }
            a.time_range = Default::default();
            b.time_range = Default::default();
            b.page_token.clear();
            assert_eq!(a, b);
            assert_eq!(a.symbol_id, 1);
            assert_eq!(a.limit, 1);
        }
    }
}

#[tokio::test]
async fn l2_market_overview_follows_token_with_unchanged_filters() {
    use polyester::proto::marketoverview::v1::{
        ListMarketOverviewRequest, ListMarketOverviewResponse, MarketOverview,
    };
    use polyester::services::ListMarketOverviewOptions;
    use polyester::transport::WireFormat;
    use std::sync::Mutex;
    for json_wire in [false, true] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let server = MockHttpServer::spawn(move |req| {
            assert!(req.path.ends_with("/ListMarketOverview"));
            let request: ListMarketOverviewRequest = if json_wire {
                serde_json::from_slice(&req.body).unwrap()
            } else {
                ListMarketOverviewRequest::decode_from_slice(&req.body).unwrap()
            };
            let mut seen = observed.lock().unwrap();
            seen.push(request);
            let response = ListMarketOverviewResponse {
                markets: vec![MarketOverview {
                    symbol_id: seen.len() as u32,
                    ..Default::default()
                }],
                next_page_token: if seen.len() == 1 {
                    "opaque-overview".into()
                } else {
                    String::new()
                },
                ..Default::default()
            };
            if json_wire {
                hardening_support::HttpScript::Json {
                    status: 200,
                    body: serde_json::to_vec(&response).unwrap(),
                }
            } else {
                connect_proto_ok(&response)
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: server.base_url(),
            hydrate_catalogs: false,
            wire_format: if json_wire {
                WireFormat::Json
            } else {
                WireFormat::Binary
            },
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(
                json!({"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":8}]}),
            )
            .unwrap();
        let mut opts = ListMarketOverviewOptions {
            symbols: Some(vec!["BTC-USDT".into()]),
            limit: Some(1),
            include_sparklines: true,
            ..Default::default()
        };
        let first = client.market_overview.list(opts.clone()).await.unwrap();
        assert_eq!(first.markets.len(), 1);
        assert_eq!(first.next_page_token, "opaque-overview");
        opts.page_token = Some(first.next_page_token);
        let second = client.market_overview.list(opts).await.unwrap();
        assert_eq!(second.markets.len(), 1);
        assert!(second.next_page_token.is_empty());
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].page_token, "");
        assert_eq!(seen[1].page_token, "opaque-overview");
        let mut next = seen[1].clone();
        next.page_token.clear();
        assert_eq!(seen[0], next);
        assert_eq!(next.symbol_id, vec![1]);
        assert!(next.include_sparklines);
    }
}

// Each case goes through the public helper and a capture server; both refusal and
// supported wire values are checked, so a missing scenario cannot pass silently.
#[tokio::test]
async fn poly_8539_trigger_option_applicability() {
    use polyester::Price;
    use polyester::models::{CreateOrderType, CreateSide, CreateTriggerParams, CreateTriggerType};
    use polyester::proto::triggers::v1::{
        CreateTriggerRequest, CreateTriggerResponse, trigger_intent,
    };
    use std::sync::Mutex;

    let captured = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&captured);
    let http = MockHttpServer::spawn(move |request| {
        capture
            .lock()
            .unwrap()
            .push(CreateTriggerRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&CreateTriggerResponse {
            trigger_id: 1,
            client_trigger_id: "capture-option".into(),
            ..Default::default()
        })
    })
    .await;
    let client = api_key_client(&http);
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol":"BTC-USDT", "symbol_id":1, "base_quantity_scale":8}]
        }))
        .unwrap();
    let kinds = [
        CreateTriggerType::StopLoss,
        CreateTriggerType::TakeProfit,
        CreateTriggerType::TrailingStop,
        CreateTriggerType::Twap,
        CreateTriggerType::Ladder,
    ];
    let fields = [
        "trailing_distance_ticks",
        "trailing_distance_bps",
        "activation_price",
        "twap_duration_ms",
        "twap_slice_interval_ms",
        "ladder_price_min",
        "ladder_price_max",
        "ladder_levels",
        "ladder_distribution",
        "trigger_price",
        "trigger_price_source",
    ];
    let mut failures = Vec::new();
    let mut executed = 0;
    for kind in kinds {
        for field in fields {
            let price = || Price::from_ticks(90_000_000_000, Some("BTC-USDT".into())).unwrap();
            let qty = Quantity::from_scaled(
                1_000_000,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .unwrap();
            // A ladder places limit orders only (POLY-9423).
            let order_type = if kind == CreateTriggerType::Ladder {
                CreateOrderType::Limit
            } else {
                CreateOrderType::Market
            };
            let mut params =
                CreateTriggerParams::new("BTC-USDT", kind, CreateSide::Sell, order_type, qty);
            params.client_trigger_id = "capture-option".into();
            match kind {
                CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit => {
                    params.trigger_price = Some(price())
                }
                CreateTriggerType::TrailingStop => params.trailing_distance_bps = Some(50),
                CreateTriggerType::Twap => {
                    params.twap_duration_ms = Some(60000);
                    params.twap_slice_interval_ms = Some(10000);
                }
                CreateTriggerType::Ladder => {
                    params.ladder_price_min = Some(price());
                    params.ladder_price_max =
                        Some(Price::from_ticks(110_000_000_000, Some("BTC-USDT".into())).unwrap());
                    params.ladder_levels = Some(4);
                }
            }
            let allowed = match field {
                "trailing_distance_ticks" => {
                    params.trailing_distance_bps = None;
                    params.trailing_distance_ticks = Some(5);
                    kind == CreateTriggerType::TrailingStop
                }
                "trailing_distance_bps" => {
                    params.trailing_distance_bps = Some(50);
                    kind == CreateTriggerType::TrailingStop
                }
                "activation_price" => {
                    params.activation_price = Some(price());
                    kind == CreateTriggerType::TrailingStop
                }
                "twap_duration_ms" => {
                    params.twap_duration_ms = Some(60000);
                    kind == CreateTriggerType::Twap
                }
                "twap_slice_interval_ms" => {
                    params.twap_slice_interval_ms = Some(10000);
                    kind == CreateTriggerType::Twap
                }
                "ladder_price_min" => {
                    params.ladder_price_min = Some(price());
                    kind == CreateTriggerType::Ladder
                }
                "ladder_price_max" => {
                    params.ladder_price_max =
                        Some(Price::from_ticks(110_000_000_000, Some("BTC-USDT".into())).unwrap());
                    kind == CreateTriggerType::Ladder
                }
                "ladder_levels" => {
                    params.ladder_levels = Some(4);
                    kind == CreateTriggerType::Ladder
                }
                "ladder_distribution" => {
                    params.ladder_distribution = Some("linear".into());
                    kind == CreateTriggerType::Ladder
                }
                "trigger_price" => {
                    params.trigger_price = Some(price());
                    matches!(
                        kind,
                        CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit
                    )
                }
                "trigger_price_source" => {
                    params.trigger_price_source = Some("index".into());
                    !matches!(kind, CreateTriggerType::Twap | CreateTriggerType::Ladder)
                }
                _ => unreachable!(),
            };
            captured.lock().unwrap().clear();
            let result = client.triggers.create(params).await;
            let requests = captured.lock().unwrap();
            executed += 1;
            if !allowed {
                if !matches!(&result, Err(Error::Validation(message)) if message.contains(field))
                    || !requests.is_empty()
                {
                    failures.push(format!(
                        "{kind:?}/{field}: result={result:?}, requests={}",
                        requests.len()
                    ));
                }
                continue;
            }
            assert!(result.is_ok(), "{kind:?}/{field}: {result:?}");
            assert_eq!(requests.len(), 1, "valid {kind:?}/{field} must send once");
            match requests[0].trigger.strategy.as_ref().unwrap() {
                trigger_intent::Strategy::TrailingStop(s) => match field {
                    "trailing_distance_ticks" => assert!(matches!(s.trailing_distance,Some(polyester::proto::triggers::v1::trailing_stop_trigger::TrailingDistance::TrailingDistanceTicks(5)))),
                    "trailing_distance_bps" => assert!(matches!(s.trailing_distance,Some(polyester::proto::triggers::v1::trailing_stop_trigger::TrailingDistance::TrailingDistanceBps(50)))),
                    "activation_price" => assert_eq!(s.activation_price_ticks,90_000_000_000),
                    "trigger_price_source" => assert_eq!(s.trigger_price_source,polyester::proto::orders::v1::TriggerPriceSource::IndexPrice),
                    _=>panic!("unexpected trailing control"),
                },
                trigger_intent::Strategy::Twap(s) => match field {"twap_duration_ms"=>assert_eq!(s.duration_ms,60000),"twap_slice_interval_ms"=>assert_eq!(s.slice_interval_ms,10000),_=>panic!("unexpected twap control")},
                trigger_intent::Strategy::Ladder(s) => match field {"ladder_price_min"=>assert_eq!(s.price_min_ticks,90_000_000_000),"ladder_price_max"=>assert_eq!(s.price_max_ticks,110_000_000_000),"ladder_levels"|"ladder_distribution"=>assert_eq!(s.levels,4),_=>panic!("unexpected ladder control")},
                trigger_intent::Strategy::StopLoss(s)|trigger_intent::Strategy::TakeProfit(s) => match field {"trigger_price"=>assert_eq!(s.trigger_price_ticks,90_000_000_000),"trigger_price_source"=>assert_eq!(s.trigger_price_source,polyester::proto::orders::v1::TriggerPriceSource::IndexPrice),_=>panic!("unexpected conditional control")},
            }
        }
    }
    assert_eq!(executed, 55);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn poly_8539_params(
    kind: polyester::models::CreateTriggerType,
    order_type: polyester::models::CreateOrderType,
) -> polyester::models::CreateTriggerParams {
    use polyester::Price;
    use polyester::models::{CreateSide, CreateTriggerParams, CreateTriggerType};
    let price = |ticks| Price::from_ticks(ticks, Some("BTC-USDT".into())).unwrap();
    let qty = Quantity::from_scaled(
        1_000_000,
        Some(8),
        QuantityDomain::OrderBase,
        Some("BTC-USDT".into()),
        Some(1),
    )
    .unwrap();
    let mut p = CreateTriggerParams::new("BTC-USDT", kind, CreateSide::Sell, order_type, qty);
    p.client_trigger_id = "capture-execution".into();
    match kind {
        CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit => {
            p.trigger_price = Some(price(100_000_000_000))
        }
        CreateTriggerType::TrailingStop => p.trailing_distance_bps = Some(50),
        CreateTriggerType::Twap => {
            p.twap_duration_ms = Some(60000);
            p.twap_slice_interval_ms = Some(10000);
        }
        CreateTriggerType::Ladder => {
            p.ladder_price_min = Some(price(90_000_000_000));
            p.ladder_price_max = Some(price(110_000_000_000));
            p.ladder_levels = Some(4);
        }
    }
    p
}

#[tokio::test]
async fn poly_8539_trigger_execution_applicability() {
    use polyester::Price;
    use polyester::models::{CreateOrderType, CreateTimeInForce, CreateTriggerType};
    use polyester::proto::triggers::v1::{
        CreateTriggerRequest, CreateTriggerResponse, conditional_child_execution,
        trailing_stop_trigger, trigger_intent, trigger_market_ioc, twap_market_ioc, twap_trigger,
    };
    use std::sync::Mutex;
    let captured = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&captured);
    let http = MockHttpServer::spawn(move |request| {
        capture
            .lock()
            .unwrap()
            .push(CreateTriggerRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&CreateTriggerResponse {
            trigger_id: 1,
            client_trigger_id: "capture-option".into(),
            ..Default::default()
        })
    })
    .await;
    let client = api_key_client(&http);
    client
        .catalogs
        .hydrate_spot_config_json(
            json!({"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":8}]}),
        )
        .unwrap();
    let mut failures = Vec::new();
    let mut executed = 0;
    for kind in [
        CreateTriggerType::StopLoss,
        CreateTriggerType::TakeProfit,
        CreateTriggerType::TrailingStop,
        CreateTriggerType::Twap,
        CreateTriggerType::Ladder,
    ] {
        for order_type in [CreateOrderType::Market, CreateOrderType::Limit] {
            if kind == CreateTriggerType::TrailingStop && order_type == CreateOrderType::Limit {
                continue;
            }
            // A market ladder is refused whatever its options (POLY-9423,
            // trigger_reads_and_filters); its limit options are checked here.
            if kind == CreateTriggerType::Ladder && order_type == CreateOrderType::Market {
                continue;
            }
            for field in [
                "post_only",
                "limit_price",
                "time_in_force",
                "max_slippage_ticks",
                "max_slippage_bps",
            ] {
                let mut p = poly_8539_params(kind, order_type);
                let price = || Price::from_ticks(100_000_000_000, Some("BTC-USDT".into())).unwrap();
                if order_type == CreateOrderType::Limit && kind != CreateTriggerType::Ladder {
                    p.limit_price = Some(price());
                }
                let allowed = match field {
                    "post_only" => {
                        p.post_only = true;
                        kind == CreateTriggerType::Ladder
                            || (order_type == CreateOrderType::Limit
                                && matches!(
                                    kind,
                                    CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit
                                ))
                    }
                    "limit_price" => {
                        p.limit_price = Some(price());
                        order_type == CreateOrderType::Limit && kind != CreateTriggerType::Ladder
                    }
                    "time_in_force" => {
                        p.time_in_force = Some(CreateTimeInForce::Fok);
                        order_type == CreateOrderType::Limit
                            && matches!(
                                kind,
                                CreateTriggerType::StopLoss | CreateTriggerType::TakeProfit
                            )
                    }
                    "max_slippage_ticks" => {
                        p.max_slippage_ticks = Some(5);
                        kind == CreateTriggerType::TrailingStop
                            || (kind != CreateTriggerType::Ladder
                                && order_type == CreateOrderType::Market)
                    }
                    "max_slippage_bps" => {
                        p.max_slippage_bps = Some(5);
                        kind == CreateTriggerType::TrailingStop
                            || (kind != CreateTriggerType::Ladder
                                && order_type == CreateOrderType::Market)
                    }
                    _ => unreachable!(),
                };
                captured.lock().unwrap().clear();
                let result = client.triggers.create(p).await;
                executed += 1;
                let requests = captured.lock().unwrap();
                if !allowed {
                    if !matches!(&result,Err(Error::Validation(message)) if message.contains(field))
                        || !requests.is_empty()
                    {
                        failures.push(format!(
                            "{kind:?}/{order_type:?}/{field}: {result:?}, {} requests",
                            requests.len()
                        ));
                    }
                    continue;
                }
                assert!(result.is_ok(), "{kind:?}/{field}: {result:?}");
                assert_eq!(requests.len(), 1);
                match requests[0].trigger.strategy.as_ref().unwrap() {
                    trigger_intent::Strategy::Ladder(s) => assert!(s.post_only),
                    trigger_intent::Strategy::TrailingStop(s) => match field {
                        "max_slippage_ticks" => assert!(matches!(
                            s.max_slippage,
                            Some(trailing_stop_trigger::MaxSlippage::MaxSlippageTicks(5))
                        )),
                        "max_slippage_bps" => assert!(matches!(
                            s.max_slippage,
                            Some(trailing_stop_trigger::MaxSlippage::MaxSlippageBps(5))
                        )),
                        _ => panic!("unexpected trailing control"),
                    },
                    trigger_intent::Strategy::Twap(s) => match s.execution.as_ref().unwrap() {
                        twap_trigger::Execution::LimitGtc(child) => {
                            assert_eq!(child.price_ticks, 100_000_000_000)
                        }
                        twap_trigger::Execution::MarketIoc(child) => match field {
                            "max_slippage_ticks" => assert!(matches!(
                                child.max_slippage,
                                Some(twap_market_ioc::MaxSlippage::MaxSlippageTicks(5))
                            )),
                            "max_slippage_bps" => assert!(matches!(
                                child.max_slippage,
                                Some(twap_market_ioc::MaxSlippage::MaxSlippageBps(5))
                            )),
                            _ => panic!("unexpected twap control"),
                        },
                    },
                    trigger_intent::Strategy::StopLoss(s)
                    | trigger_intent::Strategy::TakeProfit(s) => {
                        match s.child.execution.as_ref().unwrap() {
                            conditional_child_execution::Execution::LimitGtc(child) => {
                                assert_eq!(child.price_ticks, 100_000_000_000);
                                if field == "post_only" {
                                    assert!(child.post_only);
                                }
                            }
                            conditional_child_execution::Execution::LimitFok(child) => {
                                assert_eq!(field, "time_in_force");
                                assert_eq!(child.price_ticks, 100_000_000_000);
                            }
                            conditional_child_execution::Execution::MarketIoc(child) => match field
                            {
                                "max_slippage_ticks" => assert!(matches!(
                                    child.max_slippage,
                                    Some(trigger_market_ioc::MaxSlippage::MaxSlippageTicks(5))
                                )),
                                "max_slippage_bps" => assert!(matches!(
                                    child.max_slippage,
                                    Some(trigger_market_ioc::MaxSlippage::MaxSlippageBps(5))
                                )),
                                _ => panic!("unexpected market control"),
                            },
                            _ => panic!("unexpected conditional control"),
                        }
                    }
                }
            }
        }
    }
    // 9 kind/order-type pairs (a market ladder is refused outright) x 5 fields.
    assert_eq!(executed, 40);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn poly_8539_trailing_side_execution_and_buy_maximum() {
    use polyester::Price;
    use polyester::models::{CreateOrderType, CreateSide, CreateTriggerType};
    use polyester::proto::triggers::v1::{
        CreateTriggerRequest, CreateTriggerResponse, trigger_intent,
    };
    use std::sync::Mutex;

    let captured = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&captured);
    let http = MockHttpServer::spawn(move |request| {
        capture
            .lock()
            .unwrap()
            .push(CreateTriggerRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&CreateTriggerResponse {
            trigger_id: 1,
            client_trigger_id: "capture-execution".into(),
            ..Default::default()
        })
    })
    .await;
    let client = api_key_client(&http);
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol":"BTC-USDT", "symbol_id":1, "base_quantity_scale":8}]
        }))
        .unwrap();
    let mut executed = 0;
    for (side, order_type) in [
        (CreateSide::Sell, CreateOrderType::Limit),
        (CreateSide::Buy, CreateOrderType::Limit),
        (CreateSide::Buy, CreateOrderType::Market),
    ] {
        let mut p = poly_8539_params(CreateTriggerType::TrailingStop, order_type);
        p.side = side;
        if side == CreateSide::Buy {
            p.limit_price =
                Some(Price::from_ticks(100_000_000_000, Some("BTC-USDT".into())).unwrap());
        }
        captured.lock().unwrap().clear();
        let result = client.triggers.create(p).await;
        let requests = captured.lock().unwrap();
        executed += 1;
        if side == CreateSide::Sell {
            assert!(
                matches!(result, Err(Error::Validation(ref message)) if message.contains("order_type=limit")),
                "{result:?}"
            );
            assert_eq!(requests.len(), 0);
        } else {
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(requests.len(), 1);
            let Some(trigger_intent::Strategy::TrailingStop(trailing)) =
                &requests[0].trigger.strategy
            else {
                panic!("missing trailing strategy");
            };
            assert_eq!(trailing.limit_price_ticks, 100_000_000_000);
        }
    }
    assert_eq!(executed, 3);
}

#[tokio::test]
async fn create_quantity_uses_catalog_scale_on_wire_and_result() {
    use polyester::Price;
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};
    use polyester::proto::orders::v1::{CreateOrderRequest, CreateOrderResponse};
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, "/orders.v1.OrdersService/CreateOrder");
        let req = CreateOrderRequest::decode_from_slice(&request.body).unwrap();
        let intent = req.order.as_option().unwrap();
        assert_eq!(intent.symbol_id, 1);
        assert_eq!(
            intent.sizing,
            Some(polyester::proto::orders::v1::order_intent::Sizing::BaseQtyScaled(16000))
        );
        observed.fetch_add(1, Ordering::SeqCst);
        connect_proto_ok(&CreateOrderResponse {
            order_id: 42,
            resolved_base_qty_scaled: 16000,
            ..Default::default()
        })
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scale-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [{
            "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8
        }]}))
        .unwrap();
    for scale in [None, Some(8), Some(6)] {
        let mut params =
            CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
        params.quantity = Some(
            Quantity::from_scaled(16000, scale, QuantityDomain::OrderBase, None, None).unwrap(),
        );
        params.price = Some(Price::from_decimal_str("100", None).unwrap());
        let before = calls.load(Ordering::SeqCst);
        let result = client.orders.create(params).await;
        if scale == Some(6) {
            assert!(result.unwrap_err().to_string().contains("scale mismatch"));
            assert_eq!(calls.load(Ordering::SeqCst), before);
        } else {
            let qty = result.unwrap().resolved_base_qty.unwrap();
            assert_eq!(qty.scale(), Some(8));
            assert_eq!(qty.format(None).unwrap(), "0.00016");
        }
    }
}

// Ticket regressions use explicit protocol messages and channels as barriers.
async fn ticket_ws(
    setup_frames: Vec<Vec<u8>>,
    expected_symbol_id: Option<u32>,
) -> (
    String,
    tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    tokio::task::JoinHandle<()>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let task = tokio::spawn(async move {
        let socket = hardening_support::accept_skipping_port_probes(&listener).await;
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            |_: &_, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    "centrifuge-protobuf".parse().unwrap(),
                );
                Ok(response)
            },
        )
        .await
        .unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(WsMessage::Binary(centrifugo_ok_reply(1).into()))
            .await
            .unwrap();
        let subscribe = ws.next().await.unwrap().unwrap();
        if let Some(id) = expected_symbol_id {
            let expected = format!("public:spot:orderbook:deltas:depth:50:{id}:proto");
            let command = subscribe.into_data();
            assert!(
                command
                    .windows(expected.len())
                    .any(|bytes| bytes == expected.as_bytes()),
                "subscription channel identity mismatch"
            );
        }
        for frame in setup_frames {
            ws.send(WsMessage::Binary(frame.into())).await.unwrap();
        }
        loop {
            tokio::select! {
                frame = rx.recv() => {
                    let Some(frame) = frame else { break };
                    if ws.send(WsMessage::Binary(frame.into())).await.is_err() { break; }
                }
                msg = ws.next() => {
                    if !matches!(msg, Some(Ok(WsMessage::Binary(_) | WsMessage::Pong(_)))) { break; }
                }
            }
        }
    });
    (url, tx, task)
}

#[tokio::test]
async fn ticket_8475_setup_preserves_publication_order() {
    use hardening_support::centrifugo_publication as publication;
    let ack = centrifugo_ok_reply(2);
    let first = publication(b"first");
    assert_eq!(
        hex::encode([ack.clone(), first.clone()].concat()),
        "0208020b2209220722056669727374"
    );
    for frames in [
        vec![[ack.clone(), first.clone()].concat()],
        vec![[first.clone(), ack.clone()].concat()],
        vec![first.clone(), ack.clone()],
        vec![ack.clone(), first.clone()],
        vec![[first.clone(), ack.clone(), publication(b"second")].concat()],
    ] {
        let multiple = frames[0].len() > ack.len() + first.len();
        let mut frames = frames;
        frames.push(publication(b"control"));
        let (url, _tx, task) = ticket_ws(frames, None).await;
        let rt = RealtimeClient::new(url, "", None, None);
        let mut sub = rt.subscribe_raw(PUBLIC_CHANNEL).await.unwrap();
        let mut received = Vec::new();
        loop {
            let item = tokio::time::timeout(Duration::from_secs(10), sub.recv())
                .await
                .unwrap()
                .unwrap();
            let done = item == b"control";
            received.push(item);
            if done {
                break;
            }
        }
        let mut expected = vec![b"first".to_vec()];
        if multiple {
            expected.push(b"second".to_vec());
        }
        expected.push(b"control".to_vec());
        assert_eq!(received, expected);
        sub.close();
        task.await.unwrap();
    }
}

fn ticket_book_client(http_url: String, ws_url: String) -> Client {
    let client = Client::new(Config {
        api_url: http_url,
        ws_url,
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [
            {"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8},
            {"symbol": "ETH-USDT", "symbol_id": 2, "base_quantity_scale": 6}
        ]}))
        .unwrap();
    client
}

#[tokio::test]
async fn ticket_8510_zero_sequence_and_availability_are_independent() {
    use polyester::proto::orderbook::v1::GetOrderBookResponse;
    for seq in [0, 1] {
        for available in [false, true] {
            let http = MockHttpServer::spawn(move |_| {
                connect_proto_ok(&GetOrderBookResponse {
                    symbol_id: 1,
                    book_seq: seq,
                    available,
                    ..Default::default()
                })
            })
            .await;
            let (url, _tx, task) = ticket_ws(vec![centrifugo_ok_reply(2)], None).await;
            let client = ticket_book_client(http.base_url(), url);
            let unary = client.orderbook.get("BTC-USDT", Some(50)).await.unwrap();
            assert_eq!(unary.book_seq, seq.to_string());
            assert_eq!(unary.available, available);
            let mut sub = client
                .orderbook
                .create_subscription(polyester::services::CreateSubscriptionOptions {
                    symbol: "BTC-USDT".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let view = tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(view.book_seq, seq.to_string());
            assert_eq!(view.available, available);
            assert!(sub.err().is_none());
            sub.close();
            task.await.unwrap();
        }
    }
}

#[tokio::test]
async fn ticket_8512_conflicting_selectors_rejected_before_io() {
    let client = ticket_book_client("http://127.0.0.1:1".into(), "ws://127.0.0.1:1".into());
    let result = client
        .orderbook
        .create_subscription(polyester::services::CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            symbol_id: Some(2),
            ..Default::default()
        })
        .await;
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("conflicting selectors accepted"),
    };
    assert!(matches!(err, Error::Validation(_)), "{err}");
    assert!(
        err.to_string().contains("symbol") && err.to_string().contains("symbol_id"),
        "{err}"
    );
}

#[tokio::test]
async fn ticket_8512_selector_controls_keep_reset_identity_and_scale() {
    use hardening_support::centrifugo_publication;
    use polyester::proto::orderbook::v1::{
        GetOrderBookRequest, GetOrderBookResponse, OrderBookDelta, PriceLevel,
    };
    for (symbol, id, supplied_id, quantity) in [
        ("BTC-USDT", 1, None, "0.01"),
        ("ETH-USDT", 2, None, "1"),
        ("ETH-USDT", 2, Some(2), "1"),
    ] {
        let http = MockHttpServer::spawn(move |req| {
            assert_eq!(
                GetOrderBookRequest::decode_from_slice(&req.body)
                    .unwrap()
                    .symbol_id,
                id
            );
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: id,
                book_seq: 1,
                available: true,
                ..Default::default()
            })
        })
        .await;
        let (url, tx, task) = ticket_ws(vec![centrifugo_ok_reply(2)], Some(id)).await;
        let client = ticket_book_client(http.base_url(), url);
        let mut sub = client
            .orderbook
            .create_subscription(polyester::services::CreateSubscriptionOptions {
                symbol: symbol.into(),
                symbol_id: supplied_id,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(sub.updates().recv().await.unwrap().symbol, symbol);
        tx.send(centrifugo_publication(
            &OrderBookDelta {
                symbol_id: id,
                book_seq_start: 0,
                book_seq_end: 2,
                reset: true,
                bids: vec![PriceLevel {
                    price_ticks: 1_000_000_000,
                    qty_scaled: 1_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec(),
        ))
        .unwrap();
        let reset = tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reset.symbol, symbol);
        assert_eq!(reset.book_seq, "2");
        assert_eq!(
            reset.bids[0].qty.as_ref().unwrap().format(None).unwrap(),
            quantity
        );
        sub.close();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn ticket_8511_refresh_exhaustion_closes_receiver_with_typed_error() {
    use hardening_support::HttpScript;
    use polyester::proto::orderbook::v1::GetOrderBookResponse;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let http = MockHttpServer::spawn(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 1,
                // An unavailable book is requested again; every later
                // snapshot request fails.
                available: false,
                ..Default::default()
            })
        } else {
            HttpScript::Json {
                status: 503,
                body: br#"{"code":"unavailable","message":"snapshot unavailable"}"#.to_vec(),
            }
        }
    })
    .await;
    let (url, _tx, task) = ticket_ws(vec![centrifugo_ok_reply(2)], None).await;
    let client = ticket_book_client(http.base_url(), url);
    let mut sub = client
        .orderbook
        .create_subscription(polyester::services::CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(sub.updates().recv().await.unwrap().book_seq, "1");
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reported = errors.clone();
    sub.set_on_error(move |err| reported.lock().unwrap().push(err));
    assert!(
        tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
            .await
            .expect("terminal receiver must close")
            .is_none()
    );
    assert!(
        calls.load(Ordering::SeqCst) >= 4,
        "refresh exhaustion must occur"
    );
    let err = sub.err().expect("typed terminal error");
    assert!(!errors.lock().unwrap().is_empty());
    assert_eq!(
        err.to_string(),
        errors.lock().unwrap().last().unwrap().to_string()
    );
    sub.close();
    task.await.unwrap();
}

#[tokio::test]
async fn ticket_8510_zero_sequence_keeps_ordering() {
    use hardening_support::centrifugo_publication;
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    for mode in ["gap", "contiguous", "reset"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let http = MockHttpServer::spawn(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 0,
                available: true,
                ..Default::default()
            })
        })
        .await;
        let (url, tx, task) = ticket_ws(vec![centrifugo_ok_reply(2)], None).await;
        let client = ticket_book_client(http.base_url(), url);
        let mut sub = client
            .orderbook
            .create_subscription(polyester::services::CreateSubscriptionOptions {
                symbol: "BTC-USDT".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(sub.updates().recv().await.unwrap().book_seq, "0");
        let seq = if mode == "contiguous" { 1 } else { 5 };
        tx.send(centrifugo_publication(
            &OrderBookDelta {
                // A contiguous update starts at the snapshot's sequence; a
                // reset at 0.
                book_seq_start: if mode == "gap" { 4 } else { 0 },
                book_seq_end: seq,
                reset: mode == "reset",
                bids: vec![PriceLevel {
                    price_ticks: 100,
                    qty_scaled: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec(),
        ))
        .unwrap();
        let view = tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
            .await
            .unwrap()
            .unwrap();
        if mode == "gap" {
            // Never applied, and no snapshot is fetched: the book waits for
            // the next reset and says it is not current.
            assert_eq!(view.book_seq, "0", "a gap must never apply");
            assert!(view.bids.is_empty());
            assert!(!view.available);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(view.book_seq, seq.to_string());
            assert_eq!(view.bids.len(), 1);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        sub.close();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn ticket_8512_unknown_matching_selectors_require_catalogs() {
    let client = Client::new(Config {
        api_url: "http://127.0.0.1:1".into(),
        ws_url: "ws://127.0.0.1:1".into(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    let result = client
        .orderbook
        .create_subscription(polyester::services::CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            symbol_id: Some(1),
            ..Default::default()
        })
        .await;
    let Err(polyester::Error::Validation(message)) = result else {
        panic!("expected hydration validation")
    };
    assert!(message.contains("wait_for_catalogs"), "{message}");
}

#[tokio::test]
async fn ticket_8475_setup_overflow_keeps_typed_error() {
    use hardening_support::centrifugo_publication;
    let mut frames = vec![centrifugo_publication(b"early"); 201];
    frames.push(centrifugo_ok_reply(2));
    let (url, _tx, task) = ticket_ws(frames, None).await;
    let rt = RealtimeClient::new(url, "", None, None);
    let result = rt.subscribe_raw(PUBLIC_CHANNEL).await;
    assert!(
        matches!(result, Err(polyester::Error::QueueOverflow(_))),
        "setup overflow lost its type"
    );
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn ticket_8511_zero_sequence_disconnect_then_503_releases_receiver() {
    use futures_util::{SinkExt, StreamExt};
    use hardening_support::HttpScript;
    use polyester::proto::orderbook::v1::GetOrderBookResponse;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let http = MockHttpServer::spawn(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 0,
                available: true,
                ..Default::default()
            })
        } else {
            HttpScript::Json {
                status: 503,
                body: br#"{"code":"unavailable","message":"snapshot unavailable"}"#.to_vec(),
            }
        }
    })
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (disconnect, disconnected) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut disconnected = Some(disconnected);
        for connection in 0..2 {
            let socket = hardening_support::accept_skipping_port_probes(&listener).await;
            let mut ws = tokio_tungstenite::accept_hdr_async(socket,
                |_: &_, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    response.headers_mut().insert("sec-websocket-protocol", "centrifuge-protobuf".parse().unwrap());
                    Ok(response)
                }
            ).await.unwrap();
            for id in 1..=2 {
                ws.next().await.unwrap().unwrap();
                ws.send(WsMessage::Binary(centrifugo_ok_reply(id).into()))
                    .await
                    .unwrap();
            }
            if connection == 0 {
                disconnected.take().unwrap().await.unwrap();
                ws.close(None).await.unwrap();
            } else {
                while let Some(Ok(message)) = ws.next().await {
                    if matches!(message, WsMessage::Close(_)) {
                        break;
                    }
                }
            }
        }
    });
    let client = ticket_book_client(http.base_url(), url);
    let mut sub = client
        .orderbook
        .create_subscription(polyester::services::CreateSubscriptionOptions {
            symbol: "BTC-USDT".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(sub.updates().recv().await.unwrap().book_seq, "0");
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reported = errors.clone();
    sub.set_on_error(move |err| reported.lock().unwrap().push(err));
    // The disconnect cannot happen before the initial snapshot has been delivered.
    disconnect.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), sub.updates().recv())
            .await
            .expect("terminal receiver must close")
            .is_none()
    );
    assert!(
        calls.load(Ordering::SeqCst) >= 3,
        "reconnect snapshot retries must occur"
    );
    let error = sub.err().expect("typed terminal snapshot error");
    assert!(matches!(error, polyester::Error::Server(_)), "{error:?}");
    assert_eq!(
        errors.lock().unwrap().last().unwrap().to_string(),
        error.to_string()
    );
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("replacement socket must be released")
        .unwrap();
    sub.close();
}

#[tokio::test]
async fn l2_admission_error_classification() {
    use base64::Engine;
    use buffa::{Enumeration, Message};
    use polyester::proto::orders::v1::{ErrorCode, ErrorDetail};
    let mut failures = Vec::new();
    for (name, status, retryable) in [
        ("OVERLOADED", 503, true),
        ("SYSTEM_RATE_LIMITED", 429, true),
        ("ORDER_SEQUENCE_EXHAUSTED", 429, false),
    ] {
        for shape in ["rest", "connect", "connect_detail"] {
            let mut body = json!({"code":name,"detail":"admission refused"});
            let http_status = if shape == "rest" { status } else { 429 };
            if shape != "rest" {
                body = json!({"code":"resource_exhausted","message":name});
            }
            if shape == "connect_detail" {
                let detail = ErrorDetail {
                    code: ErrorCode::from_proto_name(&format!("ERROR_CODE_{name}"))
                        .unwrap()
                        .into(),
                    ..Default::default()
                };
                body["details"] = json!([{"type":"orders.v1.ErrorDetail", "value":base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec())}]);
            }
            let body = body.to_string();
            let http =
                MockHttpServer::spawn(move |_| json_body(http_status, "application/json", &body))
                    .await;
            let err = api_key_client(&http)
                .auth
                .me()
                .await
                .expect_err("refused admission");
            let actual_code = match &err {
                Error::Api { code, .. } => Some(code.as_str()),
                Error::RateLimit { code, .. } => code.as_deref(),
                _ => None,
            };
            let expected_code = format!("ERROR_CODE_{name}");
            if actual_code != Some(expected_code.as_str()) {
                failures.push(format!("{name}/{shape} code={actual_code:?}: {err:?}"));
            }
            if err.is_retryable() != retryable || err.mutation_outcome_unknown() {
                failures.push(format!("{name}/{shape}: {err:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn l2_future_outcome_code_classification() {
    for name in [
        "ORDER_CREATE_OUTCOME_UNKNOWN",
        "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN",
        "TWAP_CREATE_OUTCOME_UNKNOWN",
        "ERROR_CODE_TWAP_CREATE_OUTCOME_UNKNOWN",
    ] {
        for status in [503, 504] {
            for connect in [false, true] {
                let body=if connect { json!({"code":if status==503 {"unavailable"} else {"deadline_exceeded"},"message":name}) } else {json!({"code":name,"detail":"read back first"})}.to_string();
                let http =
                    MockHttpServer::spawn(move |_| json_body(status, "application/json", &body))
                        .await;
                let err = api_key_client(&http)
                    .auth
                    .me()
                    .await
                    .expect_err("unknown outcome");
                let expected = if name == "ORDER_CREATE_OUTCOME_UNKNOWN" {
                    "ERROR_CODE_ORDER_CREATE_OUTCOME_UNKNOWN"
                } else {
                    name
                };
                assert!(
                    matches!(&err, Error::Api {code, ..} if code == expected),
                    "{name}/{status}/{connect}: {err:?}"
                );
                assert!(!err.is_retryable(), "{name}/{status}/{connect}: {err:?}");
                assert!(err.mutation_outcome_unknown(), "{err:?}");
                if !connect {
                    assert_eq!(err.http_problem().unwrap().code, name);
                }
            }
        }
    }
}

#[tokio::test]
async fn l2_rest_admission_keeps_retry_delay() {
    for code in [
        "SYSTEM_RATE_LIMITED",
        "RATE_LIMIT_EXCEEDED",
        "PRICE_LEVELS_EXHAUSTED",
    ] {
        let http = MockHttpServer::spawn(move |_| {
            let mut response = json_body(
                429,
                "application/problem+json",
                &json!({"code":code}).to_string(),
            );
            if let hardening_support::HttpScript::Raw { headers, .. } = &mut response {
                headers.push(("Retry-After".into(), "1.25".into()));
            }
            response
        })
        .await;
        let error = api_key_client(&http)
            .auth
            .me()
            .await
            .expect_err("rate limited");
        assert_eq!(error.retry_after(), Some(1.25), "{error:?}");
    }
}

#[tokio::test]
async fn l2_snapshot_refresh_retries_gateway_504() {
    use polyester::proto::orders::v1::BatchReplacePhase;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let http = MockHttpServer::spawn(move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            json_body(504, "text/html", "Gateway Timeout")
        } else {
            batch_status(BatchReplacePhase::Rejected, "POST_ONLY_CROSS")
        }
    })
    .await;
    let client = Arc::new(api_key_client(&http));
    let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
        client: RealtimeClient::new("ws://example.invalid", "", None, None),
        channel: "public:test".into(),
        decode: Arc::new(|_b| Ok(1u8)),
        fetch_snapshot: Arc::new(move || {
            let client = client.clone();
            async move { client.orders.get_batch_replace_status("4242", None).await }.boxed()
        }),
        read_publication: Arc::new(|p| vec![p]),
        apply_snapshot: Arc::new(|_s, _p| {}),
        apply_live_publications: Arc::new(|_p| {}),
        max_buffered: 8,
        on_reconnect: None,
        on_snapshot_refresh: None,
        on_error: None,
    });
    stream.request_refresh();
    wait_until(
        || stream.is_ready() || stream.is_disposed(),
        Duration::from_secs(2),
    )
    .await;
    assert!(stream.is_ready(), "{:?}", stream.err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    stream.close();
}

#[tokio::test]
async fn l2_retry_contract_catalog() {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/retry_contract.json")).unwrap();
    let mut failures = Vec::new();
    for case in catalog["bad_requests"].as_array().unwrap() {
        let code = case["code"].as_str().unwrap();
        let body = json!({"code":code,"detail":"refused"}).to_string();
        let http =
            MockHttpServer::spawn(move |_| json_body(400, "application/problem+json", &body)).await;
        let e = api_key_client(&http).auth.me().await.unwrap_err();
        if e.is_retryable() != case["retryable"].as_bool().unwrap() || e.mutation_outcome_unknown()
        {
            failures.push(format!("400 {code}: {e:?}"));
        }
    }
    for case in catalog["resources"].as_array().unwrap() {
        let code = case["code"].as_str().unwrap();
        assert_eq!(
            case["retryable"].as_bool().unwrap(),
            case["guidance"]
                .as_str()
                .unwrap()
                .to_ascii_lowercase()
                .contains("retry")
                || code == "RATE_LIMIT_EXCEEDED"
        );
        for shape in ["rest", "message", "detail", "hint"] {
            let status = case["status"].as_u64().unwrap() as u16;
            let mut body = if shape == "rest" {
                json!({"code":code,"detail":"refused"})
            } else {
                json!({"code":"resource_exhausted","message":code})
            };
            if shape == "detail" || shape == "hint" {
                use base64::Engine;
                use buffa::Enumeration;
                let mut detail = polyester::proto::orders::v1::ErrorDetail {
                    code: polyester::proto::orders::v1::ErrorCode::from_proto_name(&format!(
                        "ERROR_CODE_{code}"
                    ))
                    .unwrap()
                    .into(),
                    ..Default::default()
                };
                if shape == "hint" {
                    detail.rate_limit = Some(
                        polyester::proto::polyester::ratelimit::v1::RateLimitDetail {
                            retry_after_ms: Some(1250),
                            ..Default::default()
                        },
                    )
                    .into();
                }
                body["details"] = json!([{"type":"orders.v1.ErrorDetail","value":base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec())}]);
            }
            let body = body.to_string();
            let http = MockHttpServer::spawn(move |_| {
                let mut script = json_body(status, "application/json", &body);
                if let hardening_support::HttpScript::Raw {
                    ref mut headers, ..
                } = script
                {
                    headers.push(("Retry-After".into(), "1.25".into()));
                }
                script
            })
            .await;
            let e = api_key_client(&http).auth.me().await.unwrap_err();
            if case["retryable"].as_bool().unwrap()
                && !matches!(&e, Error::RateLimit{retry_after:Some(d),..} if *d==1.25)
            {
                failures.push(format!("delay {code}/{shape}: {e:?}"));
            }
            if e.is_retryable() != case["retryable"].as_bool().unwrap()
                || e.mutation_outcome_unknown()
            {
                failures.push(format!("{code}/{shape}: {e:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn l2_retry_contract_unavailable_refusal() {
    use base64::Engine;
    use buffa::Enumeration;
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/retry_contract.json")).unwrap();
    let mut failures = Vec::new();
    for case in catalog["unavailable"].as_array().unwrap() {
        let name = case["code"].as_str().unwrap();
        for shape in ["rest", "message", "detail", "rest502"] {
            let mut body = json!({"code":name,"detail":"not admitted"});
            if shape == "message" {
                body = json!({"code":"unavailable","message":name,"details":[]});
            }
            if shape == "detail" {
                let detail = polyester::proto::orders::v1::ErrorDetail {
                    code: polyester::proto::orders::v1::ErrorCode::from_proto_name(&format!(
                        "ERROR_CODE_{name}"
                    ))
                    .unwrap()
                    .into(),
                    ..Default::default()
                };
                body = json!({"code":"unavailable","message":"not admitted","details":[{"type":"orders.v1.ErrorDetail","value":base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec())}]});
            }
            let body = body.to_string();
            let status = if shape == "rest502" { 502 } else { 503 };
            let http =
                MockHttpServer::spawn(move |_| json_body(status, "application/json", &body)).await;
            let e = api_key_client(&http).auth.me().await.unwrap_err();
            let unknown = shape == "rest502" || (name == "UNAVAILABLE" && shape != "detail");
            if e.is_retryable() != (case["retryable"].as_bool().unwrap() && !unknown)
                || e.mutation_outcome_unknown() != unknown
            {
                failures.push(format!("{name}/{shape}: {e:?}"));
            }
            if !unknown
                && !matches!(&e,Error::AdmissionUnavailable{code,..}|Error::Api{code,..} if code==&format!("ERROR_CODE_{name}"))
            {
                failures.push(format!("code {name}/{shape}: {e:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn l2_retry_contract_ambiguous() {
    for (status, kind, body) in [
        (504, "application/json", r#"{"code":"deadline_exceeded"}"#),
        (503, "application/json", r#"{"code":"unavailable"}"#),
        (502, "text/plain", ""),
        (503, "text/html", "<html>unavailable</html>"),
    ] {
        let http = MockHttpServer::spawn(move |_| json_body(status, kind, body)).await;
        let e = api_key_client(&http).auth.me().await.unwrap_err();
        assert!(
            e.mutation_outcome_unknown() && !e.is_retryable(),
            "{status}: {e:?}"
        );
    }
}

#[tokio::test]
async fn l2_retry_contract_read_recovery() {
    use polyester::proto::orders::v1::BatchReplacePhase;
    for (status, kind, body) in [
        (0, "timeout", "timeout"),
        (502, "text/plain", ""),
        (503, "text/html", "<html>Unavailable</html>"),
        (504, "application/json", r#"{"code":"deadline_exceeded"}"#),
        (503, "application/json", r#"{"code":"unavailable"}"#),
    ] {
        for operation in ["trigger", "wait", "snapshot"] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = calls.clone();
            let http = MockHttpServer::spawn(move |_| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    if status == 0 {
                        hardening_support::HttpScript::NeverRespond {
                            stall: Duration::from_secs(1),
                        }
                    } else {
                        json_body(status, kind, body)
                    }
                } else if operation == "trigger" {
                    connect_proto_ok(&polyester::proto::triggers::v1::GetTriggerResponse {
                        trigger: Some(polyester::proto::triggers::v1::Trigger {
                            trigger_id: 9,
                            ..Default::default()
                        })
                        .into(),
                        ..Default::default()
                    })
                } else {
                    batch_status(BatchReplacePhase::Rejected, "POST_ONLY_CROSS")
                }
            })
            .await;
            let client = Arc::new(
                Client::new(Config {
                    api_url: http.base_url(),
                    api_key_id: Some("ak_test".into()),
                    api_private_key: Some(TEST_KEY.into()),
                    hydrate_catalogs: false,
                    timeout: if status == 0 {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_secs(10)
                    },
                    ..Default::default()
                })
                .unwrap(),
            );
            match operation {
                "trigger" => {
                    client
                        .triggers
                        .get(polyester::proto::triggers::v1::GetTriggerRequest {
                            trigger_id: 9,
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                "wait" => {
                    client
                        .orders
                        .wait_batch_replace("4242", None, Some(Duration::from_secs(2)))
                        .await
                        .unwrap();
                }
                _ => {
                    let stream = SnapshotThenStream::new(SnapshotThenStreamConfig {
                        client: RealtimeClient::new("ws://example.invalid", "", None, None),
                        channel: "public:test".into(),
                        decode: Arc::new(|_| Ok(1u8)),
                        fetch_snapshot: Arc::new(move || {
                            let client = client.clone();
                            async move {client.orders.get_batch_replace_status("4242",None).await}.boxed()
                        }),
                        read_publication: Arc::new(|p| vec![p]),
                        apply_snapshot: Arc::new(|_, _| {}),
                        apply_live_publications: Arc::new(|_| {}),
                        max_buffered: 8,
                        on_reconnect: None,
                        on_snapshot_refresh: None,
                        on_error: None,
                    });
                    stream.request_refresh();
                    wait_until(
                        || stream.is_ready() || stream.is_disposed(),
                        Duration::from_secs(2),
                    )
                    .await;
                    assert!(stream.is_ready(), "{:?}", stream.err());
                    stream.close();
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2, "{status}/{operation}");
        }
    }
}

#[tokio::test]
async fn l2_retry_contract_client_timeout() {
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};
    // The scenario needs the request on the wire before the client gives up. A
    // slow host can time out first, so try again with longer, and fail if the
    // request never arrives. A resend would make the count 2.
    let mut timeout = Duration::from_millis(50);
    for _ in 0..5 {
        let http = MockHttpServer::spawn(|_| hardening_support::HttpScript::NeverRespond {
            stall: Duration::from_secs(1),
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            hydrate_catalogs: false,
            timeout,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(
                json!({"pairs":[{"symbol":"BTC-USDT","symbol_id":1,"base_quantity_scale":8}]}),
            )
            .unwrap();
        let mut params =
            CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Market);
        params.quantity = Some(
            Quantity::from_scaled(
                1,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .unwrap(),
        );
        let e = client.orders.create(params).await.unwrap_err();
        assert!(e.mutation_outcome_unknown() && !e.is_retryable(), "{e:?}");
        let arrived = hardening_support::eventually(
            || http.requests.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(1),
        )
        .await;
        if arrived {
            assert_eq!(http.requests.load(Ordering::SeqCst), 1);
            return;
        }
        timeout *= 2;
    }
    panic!("the request never reached the mock before the client timed out");
}

#[tokio::test]
async fn l2_retry_contract_unavailable_message_not_proof() {
    let http = MockHttpServer::spawn(|_| {
        json_body(
            503,
            "application/json",
            r#"{"code":"unavailable","message":"UNAVAILABLE"}"#,
        )
    })
    .await;
    let e = api_key_client(&http).auth.me().await.unwrap_err();
    assert!(e.mutation_outcome_unknown() && !e.is_retryable(), "{e:?}");
}

#[tokio::test]
async fn l2_retry_contract_unknown_precedes_backoff() {
    use base64::Engine;
    let detail = polyester::proto::orders::v1::ErrorDetail {
        code: polyester::proto::orders::v1::ErrorCode::OrderCreateOutcomeUnknown.into(),
        rate_limit: Some(
            polyester::proto::polyester::ratelimit::v1::RateLimitDetail {
                retry_after_ms: Some(1250),
                ..Default::default()
            },
        )
        .into(),
        ..Default::default()
    };
    let body=json!({"code":"unavailable","message":"read back","details":[{"type":"orders.v1.ErrorDetail","value":base64::engine::general_purpose::STANDARD.encode(detail.encode_to_vec())}]}).to_string();
    let http = MockHttpServer::spawn(move |_| json_body(503, "application/json", &body)).await;
    let e = api_key_client(&http).auth.me().await.unwrap_err();
    assert!(e.mutation_outcome_unknown() && !e.is_retryable(), "{e:?}");
}

#[tokio::test]
async fn l2_retry_contract_future_resource_code() {
    let http = MockHttpServer::spawn(|_| {
        json_body(
            429,
            "application/json",
            r#"{"code":"resource_exhausted","message":"FUTURE_PERMANENT_CAPACITY"}"#,
        )
    })
    .await;
    let e = api_key_client(&http).auth.me().await.unwrap_err();
    assert!(!e.is_retryable() && !e.mutation_outcome_unknown(), "{e:?}");
    assert!(matches!(e,Error::Api{code,..} if code=="FUTURE_PERMANENT_CAPACITY"));
}

#[tokio::test]
async fn l2_retry_contract_standalone_backoff_never_hides_unknown() {
    use base64::Engine;
    let rate = polyester::proto::polyester::ratelimit::v1::RateLimitDetail {
        retry_after_ms: Some(1250),
        ..Default::default()
    };
    let order = polyester::proto::orders::v1::ErrorDetail {
        code: polyester::proto::orders::v1::ErrorCode::OrderCreateOutcomeUnknown.into(),
        ..Default::default()
    };
    for first in [true, false] {
        let mut details = vec![
            json!({"type":"polyester.ratelimit.v1.RateLimitDetail","value":base64::engine::general_purpose::STANDARD.encode(rate.encode_to_vec())}),
            json!({"type":"orders.v1.ErrorDetail","value":base64::engine::general_purpose::STANDARD.encode(order.encode_to_vec())}),
        ];
        if !first {
            details.reverse();
        }
        let body =
            json!({"code":"unavailable","message":"read back","details":details}).to_string();
        let http = MockHttpServer::spawn(move |_| json_body(503, "application/json", &body)).await;
        let e = api_key_client(&http).auth.me().await.unwrap_err();
        assert!(!e.is_retryable() && e.mutation_outcome_unknown(), "{e:?}");
    }
}

#[tokio::test]
async fn l2_retry_contract_code_only_unknown_precedes_standalone_backoff() {
    use base64::Engine;
    let rate = polyester::proto::polyester::ratelimit::v1::RateLimitDetail {
        retry_after_ms: Some(1250),
        ..Default::default()
    };
    let body=json!({"code":"unavailable","message":"ORDER_CREATE_OUTCOME_UNKNOWN","details":[{"type":"polyester.ratelimit.v1.RateLimitDetail","value":base64::engine::general_purpose::STANDARD.encode(rate.encode_to_vec())}]}).to_string();
    let http = MockHttpServer::spawn(move |_| json_body(503, "application/json", &body)).await;
    let e = api_key_client(&http).auth.me().await.unwrap_err();
    assert!(e.mutation_outcome_unknown() && !e.is_retryable(), "{e:?}");
}

#[tokio::test]
async fn l2_orderbook_coarse_buckets_preserve_zero_bid_and_round_asks_up() {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    use polyester::services::CreateSubscriptionOptions;

    for (symbol, bid, ask, coarse, fine) in [
        ("DOGE-USDT", 71_600_000_i64, 71_700_000_i64, "0.1", "0.001"),
        ("LTC-USDT", 51_750_000_000, 51_760_000_000, "100", "0.1"),
        ("BNB-USDT", 650_100_000_000, 650_200_000_000, "100", "1"),
    ] {
        for live_delta in [false, true] {
            // Exercise snapshot rendering and live updates independently: a buffered
            // publication may otherwise replace the initial view before it is read.
            let ws = if live_delta {
                let delta = OrderBookDelta {
                    symbol_id: 1,
                    book_seq_start: 1,
                    book_seq_end: 2,
                    bids: vec![PriceLevel {
                        price_ticks: bid,
                        qty_scaled: 5,
                        ..Default::default()
                    }],
                    asks: vec![PriceLevel {
                        price_ticks: ask,
                        qty_scaled: 6,
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode_to_vec();
                MockWsServer::spawn_centrifugo_publication_after_handshake(Arc::default(), delta)
                    .await
            } else {
                MockWsServer::spawn_centrifugo_public(Arc::default()).await
            };
            let http = MockHttpServer::spawn(move |request| {
                if request.path == "/orderbook.v1.OrderbookService/GetOrderBook" {
                    connect_proto_ok(&GetOrderBookResponse {
                        symbol_id: 1,
                        book_seq: 1,
                        available: true,
                        bids: if live_delta {
                            vec![]
                        } else {
                            vec![PriceLevel {
                                price_ticks: bid,
                                qty_scaled: 3,
                                ..Default::default()
                            }]
                        },
                        asks: if live_delta {
                            vec![]
                        } else {
                            vec![PriceLevel {
                                price_ticks: ask,
                                qty_scaled: 4,
                                ..Default::default()
                            }]
                        },
                        ..Default::default()
                    })
                } else {
                    hardening_support::HttpScript::NotFound
                }
            })
            .await;
            let client = Client::new(Config {
                api_url: http.base_url(),
                ws_url: ws.ws_url(),
                hydrate_catalogs: false,
                ..Default::default()
            })
            .unwrap();
            client
                .catalogs
                .hydrate_spot_config_json(json!({
                    "pairs": [{"symbol": symbol, "symbol_id": 1, "base_quantity_scale": 8}]
                }))
                .unwrap();
            let mut subscription = client
                .orderbook
                .create_subscription(CreateSubscriptionOptions {
                    symbol: symbol.into(),
                    symbol_id: Some(1),
                    depth: Some(50),
                    bucket: Some(coarse.into()),
                })
                .await
                .unwrap();
            for (index, bucket) in [coarse, fine, coarse, ""].into_iter().enumerate() {
                if index > 0 {
                    subscription.set_bucket(bucket).unwrap();
                }
                let mut view =
                    tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
                        .await
                        .expect("bucket must emit a view")
                        .expect("stream must remain open");
                if live_delta && index == 0 && view.book_seq == "1" {
                    assert!(view.bids.is_empty() && view.asks.is_empty());
                    view =
                        tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
                            .await
                            .expect("delta must emit a view")
                            .expect("stream must remain open");
                }
                assert_eq!(view.book_seq, if live_delta { "2" } else { "1" });
                let ticks = polyester::orderbook::parse_bucket_ticks(bucket)
                    .unwrap()
                    .max(1);
                assert_eq!(view.bids.len(), 1);
                assert_eq!(view.asks.len(), 1);
                assert_eq!(
                    view.bids[0].price.as_ref().unwrap().as_ticks(),
                    bid / ticks * ticks
                );
                assert_eq!(
                    view.asks[0].price.as_ref().unwrap().as_ticks(),
                    (ask + ticks - 1) / ticks * ticks
                );
                assert_eq!(
                    view.bids[0].qty.as_ref().unwrap().as_scaled(),
                    if live_delta { 5 } else { 3 }
                );
                assert_eq!(
                    view.asks[0].qty.as_ref().unwrap().as_scaled(),
                    if live_delta { 6 } else { 4 }
                );
            }
            assert!(subscription.err().is_none());
            subscription.close();
        }
    }
}

/// POLY-9098: a snapshot applied with a buffered delta is one book state, so it
/// must yield one view. A second, identical view stayed queued, and the next
/// bucket change read that stale view instead of its own.
#[tokio::test]
async fn l2_orderbook_snapshot_with_buffered_delta_emits_one_view() {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    use polyester::services::CreateSubscriptionOptions;

    let level = |price_ticks, qty_scaled| PriceLevel {
        price_ticks,
        qty_scaled,
        ..Default::default()
    };
    for buffered in [1_u64, 3] {
        // Deltas 2..=1+buffered, published right after the subscribe reply, reach the
        // client while the snapshot read (sequence 1) is held back, so all are buffered.
        let deltas: Vec<Vec<u8>> = (2..2 + buffered)
            .map(|seq| {
                OrderBookDelta {
                    symbol_id: 1,
                    book_seq_start: seq - 1,
                    book_seq_end: seq,
                    bids: vec![level(71_600_000, seq as i64)],
                    ..Default::default()
                }
                .encode_to_vec()
            })
            .collect();
        let ws =
            MockWsServer::spawn_centrifugo_publications_after_handshake(Arc::default(), deltas)
                .await;
        let http = MockHttpServer::spawn(move |request| {
            assert_eq!(request.path, "/orderbook.v1.OrderbookService/GetOrderBook");
            hardening_support::HttpScript::RawAfter {
                delay: Duration::from_millis(300),
                status: 200,
                headers: vec![("Content-Type".into(), "application/proto".into())],
                body: GetOrderBookResponse {
                    symbol_id: 1,
                    book_seq: 1,
                    available: true,
                    bids: vec![level(71_600_000, 1)],
                    asks: vec![level(71_700_000, 4)],
                    ..Default::default()
                }
                .encode_to_vec(),
            }
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            ws_url: ws.ws_url(),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(json!({
                "pairs": [{"symbol": "DOGE-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
            }))
            .unwrap();
        let mut subscription = client
            .orderbook
            .create_subscription(CreateSubscriptionOptions {
                symbol: "DOGE-USDT".into(),
                symbol_id: Some(1),
                depth: Some(50),
                bucket: Some("0.1".into()),
            })
            .await
            .unwrap();

        let last_seq = (1 + buffered).to_string();
        let first = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
            .await
            .expect("the snapshot must emit a view")
            .expect("stream must remain open");
        assert_eq!(first.book_seq, last_seq, "buffered deltas {buffered}");
        assert_eq!(
            first.bids[0].qty.as_ref().unwrap().as_scaled(),
            1 + buffered as i64
        );
        let extra = tokio::time::timeout(Duration::from_millis(300), subscription.updates().recv())
            .await
            .ok()
            .flatten();
        assert!(
            extra.is_none(),
            "{buffered} buffered deltas: one book state emitted a second view {extra:?}"
        );

        // The next bucket change reads its own view, not a stale one.
        subscription.set_bucket("0.001").unwrap();
        let fine = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
            .await
            .expect("bucket must emit a view")
            .expect("stream must remain open");
        assert_eq!(
            fine.bids[0].price.as_ref().unwrap().as_ticks(),
            71_000_000,
            "bid bucket price after the bucket change"
        );
        assert!(subscription.err().is_none());
        subscription.close();
    }
}

/// POLY-8563: a stalled publisher must be distinguishable from a quiet market.
/// The server republishes every book about every 30 s, so the time since the
/// stream's last publication tells them apart; the managed book reports it.
#[tokio::test]
async fn l2_orderbook_reports_time_since_last_publication() {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    use polyester::services::CreateSubscriptionOptions;

    let level = |price_ticks, qty_scaled| PriceLevel {
        price_ticks,
        qty_scaled,
        ..Default::default()
    };
    for publishes in [false, true] {
        let ws = if publishes {
            let delta = OrderBookDelta {
                symbol_id: 1,
                book_seq_start: 1,
                book_seq_end: 2,
                bids: vec![level(71_600_000, 5)],
                ..Default::default()
            }
            .encode_to_vec();
            // Well after the start, so the publication's age and the
            // subscription's age differ.
            MockWsServer::spawn_centrifugo_publications_after_delay(
                Arc::default(),
                vec![delta],
                Duration::from_millis(400),
            )
            .await
        } else {
            MockWsServer::spawn_centrifugo_public(Arc::default()).await
        };
        let http = MockHttpServer::spawn(move |_| {
            connect_proto_ok(&GetOrderBookResponse {
                symbol_id: 1,
                book_seq: 1,
                available: true,
                bids: vec![level(71_600_000, 3)],
                asks: vec![level(71_700_000, 4)],
                ..Default::default()
            })
        })
        .await;
        let client = Client::new(Config {
            api_url: http.base_url(),
            ws_url: ws.ws_url(),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        client
            .catalogs
            .hydrate_spot_config_json(json!({
                "pairs": [{"symbol": "DOGE-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
            }))
            .unwrap();
        let created = Instant::now();
        let mut subscription = client
            .orderbook
            .create_subscription(CreateSubscriptionOptions {
                symbol: "DOGE-USDT".into(),
                symbol_id: Some(1),
                depth: Some(50),
                bucket: None,
            })
            .await
            .unwrap();
        let want_seq = if publishes { "2" } else { "1" };
        loop {
            let view = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
                .await
                .expect("a view")
                .expect("stream must remain open");
            if view.book_seq == want_seq {
                break;
            }
        }
        if publishes {
            // The publication just arrived, 400 ms after the start.
            let age = subscription.since_last_publication();
            assert!(
                age < Duration::from_millis(200),
                "age just after a publication {age:?}"
            );
        }

        // Silence makes it grow; a bucket change is not a publication.
        tokio::time::sleep(Duration::from_millis(300)).await;
        subscription.set_bucket("0.1").unwrap();
        tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
            .await
            .expect("a view for the bucket change")
            .expect("stream must remain open");
        let age = subscription.since_last_publication();
        assert!(
            age >= Duration::from_millis(300),
            "publishes={publishes}: age {age:?}"
        );
        // With no publication at all, the age runs from the subscription's start.
        assert!(
            age <= created.elapsed(),
            "publishes={publishes}: age {age:?}"
        );
        subscription.close();
    }
}

/// Subscribe to a book whose stream sends one delta at price zero on `side`.
///
/// The delta is published right after the subscribe reply, so it reaches the
/// client before or after the first snapshot read answers, depending on
/// timing; `first_snapshot_delay` holds that answer back to choose. A refused
/// delta leaves the book out of sync until the next reset, so no second
/// snapshot is read.
async fn raw_zero_delta_subscription(
    side: &str,
    bucket: &str,
    quantity: i64,
    depth: u32,
    first_snapshot_delay: Duration,
) {
    use polyester::proto::orderbook::v1::{GetOrderBookResponse, OrderBookDelta, PriceLevel};
    use polyester::services::CreateSubscriptionOptions;

    let level = PriceLevel {
        qty_scaled: quantity,
        ..Default::default()
    };
    let delta = OrderBookDelta {
        symbol_id: 1,
        book_seq_start: 1,
        book_seq_end: 2,
        bids: if side == "bids" {
            vec![level.clone()]
        } else {
            vec![]
        },
        asks: if side == "asks" { vec![level] } else { vec![] },
        ..Default::default()
    }
    .encode_to_vec();
    let ws =
        MockWsServer::spawn_centrifugo_publication_after_handshake(Arc::default(), delta).await;
    let reads = Arc::new(AtomicUsize::new(0));
    let http = MockHttpServer::spawn(move |request| {
        assert_eq!(request.path, "/orderbook.v1.OrderbookService/GetOrderBook");
        let first = reads.fetch_add(1, Ordering::SeqCst) == 0;
        let (book_seq, bid_qty, ask_qty) = if first { (1, 3, 4) } else { (2, 5, 6) };
        let body = GetOrderBookResponse {
            symbol_id: 1,
            book_seq,
            available: true,
            bids: vec![PriceLevel {
                price_ticks: 71_600_000,
                qty_scaled: bid_qty,
                ..Default::default()
            }],
            asks: vec![PriceLevel {
                price_ticks: 71_700_000,
                qty_scaled: ask_qty,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        if !first || first_snapshot_delay.is_zero() {
            return hardening_support::HttpScript::Raw {
                status: 200,
                headers: vec![("Content-Type".into(), "application/proto".into())],
                body,
            };
        }
        // The answer completes only after the delay.
        hardening_support::HttpScript::SlowDrip {
            status: 200,
            headers: vec![("Content-Type".into(), "application/proto".into())],
            chunks: vec![body[..1].to_vec(), body[1..].to_vec()],
            inter_chunk_delay: first_snapshot_delay,
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        ws_url: ws.ws_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol": "DOGE-USDT", "symbol_id": 1, "base_quantity_scale": 8}]
        }))
        .unwrap();
    let result = client
        .orderbook
        .create_subscription(CreateSubscriptionOptions {
            symbol: "DOGE-USDT".into(),
            symbol_id: Some(1),
            depth: Some(depth),
            bucket: Some(bucket.into()),
        })
        .await;
    if quantity > 0 {
        // Refused like any malformed delta: never shown, and the stream stays
        // open. The book keeps its snapshot levels but is no longer current:
        // it waits for the publisher's next reset, and fetches no snapshot.
        // The snapshot has a view of its own only when it was shown before
        // the delta arrived; a delta buffered behind it is refused before it
        // is shown, so the out-of-sync book is the first view.
        let mut subscription = result.unwrap();
        let mut views = 0;
        loop {
            let view = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
                .await
                .unwrap_or_else(|_| panic!("no view after the refused delta ({views} views)"))
                .expect("a raw zero price must not close the stream");
            views += 1;
            assert_eq!(view.bids.len(), 1, "corrupt delta was emitted: {view:?}");
            assert_eq!(view.asks.len(), 1, "corrupt delta was emitted: {view:?}");
            let quantities = (
                view.bids[0].qty.as_ref().unwrap().as_scaled(),
                view.asks[0].qty.as_ref().unwrap().as_scaled(),
            );
            assert_eq!(view.book_seq, "1", "corrupt delta was emitted: {view:?}");
            assert_eq!(quantities, (3, 4), "{view:?}");
            if !view.available {
                break;
            }
            assert_eq!(views, 1, "only the snapshot view is current: {view:?}");
        }
        assert!(subscription.err().is_none(), "{:?}", subscription.err());
        assert_eq!(http.requests.load(Ordering::SeqCst), 1);
        subscription.close();
    } else {
        let mut subscription = result.unwrap();
        let mut view = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
            .await
            .expect("snapshot or delta view")
            .expect("stream open");
        if view.book_seq == "1" {
            view = tokio::time::timeout(Duration::from_secs(3), subscription.updates().recv())
                .await
                .expect("removal delta must be applied")
                .expect("stream open");
        }
        assert_eq!(view.book_seq, "2");
        let ticks = polyester::orderbook::parse_bucket_ticks(bucket)
            .unwrap()
            .max(1);
        assert_eq!(view.bids.len(), 1);
        assert_eq!(view.asks.len(), 1);
        assert_eq!(
            view.bids[0].price.as_ref().unwrap().as_ticks(),
            71_600_000 / ticks * ticks
        );
        assert_eq!(
            view.asks[0].price.as_ref().unwrap().as_ticks(),
            (71_700_000 + ticks - 1) / ticks * ticks
        );
        assert_eq!(view.bids[0].qty.as_ref().unwrap().as_scaled(), 3);
        assert_eq!(view.asks[0].qty.as_ref().unwrap().as_scaled(), 4);
        assert!(subscription.err().is_none());
        assert_eq!(http.requests.load(Ordering::SeqCst), 1);
        subscription.close();
    }
    assert_eq!(ws.connects.load(Ordering::SeqCst), 1);
}

/// Delays for the first snapshot answer: none, a few drawn from one seed so the
/// delta lands before, during or after that read, and one long enough that it
/// always arrives first. Set `POLYESTER_INTERLEAVING_SEED` to replay a run.
fn first_snapshot_delays() -> Vec<Duration> {
    use rand_core::RngCore;
    let seed = std::env::var("POLYESTER_INTERLEAVING_SEED")
        .ok()
        .map(|raw| {
            raw.parse::<u64>()
                .expect("POLYESTER_INTERLEAVING_SEED is a u64")
        })
        .unwrap_or_else(|| rand_core::OsRng.next_u64());
    println!("POLYESTER_INTERLEAVING_SEED={seed}");
    let mut state = seed;
    let mut drawn = || {
        // SplitMix64.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Duration::from_micros((z ^ (z >> 31)) % 40_000)
    };
    let mut delays = vec![Duration::ZERO];
    delays.extend((0..3).map(|_| drawn()));
    delays.push(Duration::from_millis(250));
    delays
}

#[tokio::test]
async fn l2_raw_zero_bid_delta_is_refused_and_waits_for_a_reset() {
    for delay in first_snapshot_delays() {
        for bucket in ["", "0.001", "0.1"] {
            raw_zero_delta_subscription("bids", bucket, 9, 50, delay).await;
        }
    }
}

#[tokio::test]
async fn l2_raw_zero_ask_delta_is_refused_and_waits_for_a_reset() {
    for delay in first_snapshot_delays() {
        for bucket in ["", "0.001", "0.1"] {
            raw_zero_delta_subscription("asks", bucket, 9, 50, delay).await;
        }
    }
}

#[tokio::test]
async fn l2_raw_zero_price_removal_deltas_are_harmless() {
    for delay in first_snapshot_delays() {
        for side in ["bids", "asks"] {
            for bucket in ["", "0.001", "0.1"] {
                raw_zero_delta_subscription(side, bucket, 0, 50, delay).await;
            }
        }
    }
}

#[tokio::test]
async fn l2_raw_zero_bid_outside_the_requested_depth_is_refused() {
    // A zero bid sorts last, so depth 1 alone would never render it.
    for delay in first_snapshot_delays() {
        for bucket in ["", "0.001", "0.1"] {
            raw_zero_delta_subscription("bids", bucket, 9, 1, delay).await;
        }
    }
}

#[tokio::test]
async fn batch_create_refuses_item_subaccount_instead_of_using_default() {
    use polyester::Price;
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};
    use polyester::proto::orders::v1::{BatchCreateOrdersRequest, BatchCreateOrdersResponse};
    use std::sync::Mutex;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path == "/orders.v1.OrdersService/BatchCreateOrders" {
            captured
                .lock()
                .unwrap()
                .push(BatchCreateOrdersRequest::decode_from_slice(&request.body).unwrap());
            return connect_proto_ok(&BatchCreateOrdersResponse::default());
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scope-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some(polyester::codecs::scalars::format_id(42)),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [{
            "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8
        }]}))
        .unwrap();
    let item = |subaccount_id| {
        let mut params =
            CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
        params.quantity = Some(
            Quantity::from_scaled(
                1,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .unwrap(),
        );
        params.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).unwrap());
        params.subaccount_id = subaccount_id;
        params
    };
    for (top, expected) in [(None, 42), (Some(44), 44)] {
        let before = seen.lock().unwrap().len();
        client
            .orders
            .batch_create(vec![item(None)], top, None)
            .await
            .unwrap();
        let requests = seen.lock().unwrap();
        assert_eq!(
            requests.len(),
            before + 1,
            "control never reached transport"
        );
        assert_eq!(requests.last().unwrap().subaccount_id, Some(expected));
    }
    for top in [None, Some(44)] {
        for sub in [44, 42, 0] {
            let before = seen.lock().unwrap().len();
            let result = client
                .orders
                .batch_create(vec![item(None), item(Some(sub))], top, None)
                .await;
            assert!(
                matches!(result, Err(Error::Validation(ref message)) if message.contains("batch-level subaccount_id")),
                "item override must be refused, got {result:?}"
            );
            assert_eq!(
                seen.lock().unwrap().len(),
                before,
                "item override reached transport"
            );
        }
    }
}

// A batch item's quantity scale is checked against the market its symbol
// resolves to. A padded symbol used to resolve the market (trimmed) but miss
// the scale lookup (untrimmed), so an unbound scaled Quantity was checked
// against its own scale: 1 BTC at scale 6 or 10 went out as 0.01 or 100 BTC
// on a scale-8 market. Every order and trigger write now resolves the market
// once and refuses a mismatch before sending anything.
#[tokio::test]
async fn order_writes_check_quantity_scale_against_the_resolved_market() {
    use polyester::Price;
    use polyester::models::{
        CreateOrderParams, CreateOrderType, CreateSide, CreateTriggerParams, CreateTriggerType,
        ModifyOrderParams, OrderKey, PreviewOrderParams,
    };
    use polyester::proto::orders::v1::{
        BatchCreateOrdersRequest, BatchCreateOrdersResponse, CreateOrderRequest,
        ModifyOrderRequest, OrderIntent, PreviewOrderRequest, order_intent,
    };
    use polyester::proto::triggers::v1::CreateTriggerRequest;
    use std::sync::Mutex;

    // (path, symbol_id, base quantity sent) of every write that reached the wire.
    let seen = Arc::new(Mutex::new(Vec::<(String, u32, i64)>::new()));
    let captured = seen.clone();
    let intent_qty = |intent: &OrderIntent| match intent.sizing {
        Some(order_intent::Sizing::BaseQtyScaled(qty)) => qty,
        _ => -1,
    };
    let server = MockHttpServer::spawn(move |request| {
        let path = request
            .path
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let body = &request.body;
        let sent: Vec<(u32, i64)> = match path.as_str() {
            "BatchCreateOrders" => BatchCreateOrdersRequest::decode_from_slice(body)
                .unwrap()
                .items
                .iter()
                .map(|item| (item.symbol_id, intent_qty(item)))
                .collect(),
            "CreateOrder" => {
                let req = CreateOrderRequest::decode_from_slice(body).unwrap();
                vec![(req.order.symbol_id, intent_qty(&req.order))]
            }
            "PreviewOrder" => {
                let req = PreviewOrderRequest::decode_from_slice(body).unwrap();
                vec![(req.order.symbol_id, intent_qty(&req.order))]
            }
            "ModifyOrder" => {
                let req = ModifyOrderRequest::decode_from_slice(body).unwrap();
                vec![(req.symbol_id, req.new_qty_scaled.unwrap_or(-1))]
            }
            "CreateTrigger" => {
                let req = CreateTriggerRequest::decode_from_slice(body).unwrap();
                vec![(req.trigger.symbol_id, req.trigger.qty_scaled)]
            }
            _ => return hardening_support::HttpScript::NotFound,
        };
        let mut log = captured.lock().unwrap();
        for (symbol_id, qty) in sent {
            log.push((path.clone(), symbol_id, qty));
        }
        connect_proto_ok(&BatchCreateOrdersResponse::default())
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scale-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [{
            "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8, "quote_quantity_scale": 6
        }]}))
        .unwrap();
    // 1 BTC written at `scale`, bound to no market.
    let one_btc = |scale: u32| {
        Quantity::from_scaled(
            10_i64.pow(scale),
            Some(scale),
            QuantityDomain::OrderBase,
            None,
            None,
        )
        .unwrap()
    };
    let bound_one_btc = || {
        Quantity::from_scaled(
            100_000_000,
            Some(8),
            QuantityDomain::OrderBase,
            Some("BTC-USDT".into()),
            Some(1),
        )
        .unwrap()
    };
    fn order(symbol: &str, quantity: Quantity) -> CreateOrderParams {
        let mut params = CreateOrderParams::new(symbol, CreateSide::Buy, CreateOrderType::Limit);
        params.quantity = Some(quantity);
        params.price = Some(Price::from_ticks(1, None).unwrap());
        params
    }
    fn preview(symbol: &str, quantity: Quantity) -> PreviewOrderParams {
        let mut params = PreviewOrderParams::new(symbol, CreateSide::Buy, CreateOrderType::Limit);
        params.quantity = Some(quantity);
        params.price = Some(Price::from_ticks(1, None).unwrap());
        params
    }
    fn modify(symbol: &str, quantity: Quantity) -> ModifyOrderParams {
        ModifyOrderParams {
            symbol: symbol.into(),
            key: OrderKey::OrderId(polyester::codecs::scalars::format_id(9)),
            subaccount_id: None,
            request_id: None,
            new_price: None,
            new_qty: Some(quantity),
            new_attached_risk: None,
            behavior: None,
            new_client_order_id: None,
        }
    }
    fn trigger(symbol: &str, quantity: Quantity) -> CreateTriggerParams {
        let mut params = CreateTriggerParams::new(
            symbol,
            CreateTriggerType::StopLoss,
            CreateSide::Sell,
            CreateOrderType::Market,
            quantity,
        );
        params.trigger_price = Some(Price::from_ticks(1, None).unwrap());
        params.client_trigger_id = "scale-trigger".into();
        params
    }
    // Every write path with one symbol and quantity; the batch puts a
    // correct item first, so a refusal must stop the whole batch.
    async fn write(
        client: &Client,
        call: &str,
        symbol: &str,
        quantity: Quantity,
        good: Quantity,
    ) -> polyester::Result<()> {
        match call {
            "batch_create" => client
                .orders
                .batch_create(
                    vec![order("BTC-USDT", good), order(symbol, quantity)],
                    None,
                    None,
                )
                .await
                .map(drop),
            "create" => client
                .orders
                .create(order(symbol, quantity))
                .await
                .map(drop),
            "preview" => client
                .orders
                .preview(preview(symbol, quantity))
                .await
                .map(drop),
            "modify" => client
                .orders
                .modify(modify(symbol, quantity))
                .await
                .map(drop),
            "trigger" => client
                .triggers
                .create(trigger(symbol, quantity))
                .await
                .map(drop),
            _ => unreachable!(),
        }
    }
    let calls = ["batch_create", "create", "preview", "modify", "trigger"];
    let mut failures = Vec::new();
    for call in calls {
        for symbol in [" BTC-USDT ", "BTC-USDT ", " BTC-USDT", "BTC-USDT"] {
            for scale in [6, 10] {
                let before = seen.lock().unwrap().len();
                let result = write(&client, call, symbol, one_btc(scale), one_btc(8)).await;
                let sent = seen.lock().unwrap()[before..].to_vec();
                let refused = matches!(result, Err(Error::Validation(ref message)) if message.contains("scale mismatch"));
                if !refused || !sent.is_empty() {
                    failures.push(format!(
                        "{call} {symbol:?} at scale {scale}: got {result:?}, sent {sent:?}"
                    ));
                }
            }
        }
    }
    // A padded symbol names the same market: the market's scale, and a
    // Quantity bound to it, are accepted and sent unchanged.
    for call in calls {
        for symbol in [" BTC-USDT ", "BTC-USDT"] {
            for quantity in [one_btc(8), bound_one_btc()] {
                let before = seen.lock().unwrap().len();
                let _ = write(&client, call, symbol, quantity, one_btc(8)).await;
                let sent: Vec<(u32, i64)> = seen.lock().unwrap()[before..]
                    .iter()
                    .map(|(_, symbol_id, qty)| (*symbol_id, *qty))
                    .collect();
                let expected = if call == "batch_create" { 2 } else { 1 };
                if sent != vec![(1, 100_000_000); expected] {
                    failures.push(format!(
                        "{call} {symbol:?} sent {sent:?}, want 1 BTC on market 1"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// Each public order and trigger write, captured on the wire: the subaccount it
// was asked for, the configured default when it was asked for none, and no
// subaccount at all when neither exists.
type CapturedScopes = Arc<std::sync::Mutex<Vec<(String, Option<u64>)>>>;

fn plain(subaccount_id: u64) -> Option<u64> {
    (subaccount_id != 0).then_some(subaccount_id)
}

async fn scope_capture_client(default: Option<u64>) -> (MockHttpServer, Client, CapturedScopes) {
    use polyester::proto::auth::v1 as book;
    use polyester::proto::chain::deposit::v1 as deposit;
    use polyester::proto::chain::guard::v1 as guard;
    use polyester::proto::ledger::read::v1 as ledger;
    use polyester::proto::orders::v1 as orders;
    use polyester::proto::triggers::v1 as triggers;

    let seen = CapturedScopes::default();
    let captured = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        let body = &request.body;
        macro_rules! scope {
            ($request:ty) => {
                <$request>::decode_from_slice(body).unwrap().subaccount_id
            };
        }
        let rpc = request.path.rsplit('/').next().unwrap().to_owned();
        let subaccount_id = match rpc.as_str() {
            "CreateOrder" => scope!(orders::CreateOrderRequest),
            "PreviewOrder" => scope!(orders::PreviewOrderRequest),
            "ModifyOrder" => scope!(orders::ModifyOrderRequest),
            "CancelOrder" => scope!(orders::CancelOrderRequest),
            "CancelAllOrders" => scope!(orders::CancelAllOrdersRequest),
            "CancelAllAfter" => scope!(orders::CancelAllAfterRequest),
            "BatchCreateOrders" => scope!(orders::BatchCreateOrdersRequest),
            "BatchCancelOrders" => scope!(orders::BatchCancelOrdersRequest),
            "BatchReplaceOrders" => scope!(orders::BatchReplaceOrdersRequest),
            "CreateTrigger" => scope!(triggers::CreateTriggerRequest),
            "ModifyTrigger" => scope!(triggers::ModifyTriggerRequest),
            "CancelTrigger" => scope!(triggers::CancelTriggerRequest),
            "PauseTrigger" => scope!(triggers::PauseTriggerRequest),
            "ResumeTrigger" => scope!(triggers::ResumeTriggerRequest),
            "GetTrigger" => scope!(triggers::GetTriggerRequest),
            "ListTriggers" => scope!(triggers::ListTriggersRequest),
            "ListTriggerEvents" => scope!(triggers::ListTriggerEventsRequest),
            "CancelOco" => scope!(triggers::CancelOcoRequest),
            "GetBalances" => scope!(ledger::GetBalancesRequest),
            "GetBalanceHistory" => scope!(ledger::GetBalanceHistoryRequest),
            "GetEquityHistorySeries" => scope!(ledger::GetEquityHistorySeriesRequest),
            "ListTransfers" => scope!(ledger::ListTransfersRequest),
            // Plain `u64` fields: zero is what an unset field sends.
            "CreateDepositAddress" => plain(
                deposit::CreateDepositAddressRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "CreateAddressBookEntry" => plain(
                book::CreateAddressBookEntryRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "CreateAddressBookTag" => plain(
                book::CreateAddressBookTagRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "CopyAddressBookEntry" => plain(
                book::CopyAddressBookEntryRequest::decode_from_slice(body)
                    .unwrap()
                    .target_subaccount_id,
            ),
            "ListAddressBookEntries" => plain(
                book::ListAddressBookEntriesRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ListTransferCounterparties" => plain(
                book::ListTransferCounterpartiesRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ListTransferDestinations" => plain(
                book::ListTransferDestinationsRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ListInternalTransferWhitelistEntries" => plain(
                book::ListInternalTransferWhitelistEntriesRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "GetWithdrawWhitelistView" => plain(
                book::GetWithdrawWhitelistViewRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "GetAddressBookView" => plain(
                book::GetAddressBookViewRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ListSubaccountMembers" => plain(
                book::ListSubaccountMembersRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ListSubaccountActivity" => plain(
                book::ListSubaccountEventsRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "GetGuardSignerStatus" => plain(
                guard::GetGuardSignerStatusRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "CreateGuardSignerWallet" => plain(
                guard::CreateGuardSignerWalletRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "SignProtectedAction" => plain(
                guard::SignProtectedActionRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "BatchSignProtectedActions" => plain(
                guard::BatchSignProtectedActionsRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "RotateGuardSignerWallet" => plain(
                guard::RotateGuardSignerWalletRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            "ExportGuardSignerWallet" => plain(
                guard::ExportGuardSignerWalletRequest::decode_from_slice(body)
                    .unwrap()
                    .subaccount_id,
            ),
            _ => return hardening_support::HttpScript::NotFound,
        };
        captured.lock().unwrap().push((rpc.clone(), subaccount_id));
        if rpc == "ListTriggers" {
            return connect_proto_ok(&triggers::ListTriggersResponse {
                triggers: vec![triggers::Trigger {
                    trigger_id: 9,
                    status: triggers::TriggerStatus::STATUS_ARMED.into(),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        // The capture is the assertion; a refusal stops response decoding from masking it.
        json_body(
            400,
            "application/json",
            r#"{"code":"failed_precondition","message":"capture complete"}"#,
        )
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scope-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: default.map(polyester::codecs::scalars::format_id),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({"pairs": [{
            "symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 8
        }]}))
        .unwrap();
    (server, client, seen)
}

fn scope_quantity() -> Quantity {
    Quantity::from_scaled(
        1,
        Some(8),
        QuantityDomain::OrderBase,
        Some("BTC-USDT".into()),
        Some(1),
    )
    .unwrap()
}

fn scope_price() -> polyester::Price {
    polyester::Price::from_ticks(1, Some("BTC-USDT".into())).unwrap()
}

fn scope_order(subaccount_id: Option<u64>) -> polyester::models::CreateOrderParams {
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};
    let mut params = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
    params.quantity = Some(scope_quantity());
    params.price = Some(scope_price());
    params.subaccount_id = subaccount_id;
    params
}

#[tokio::test]
async fn order_and_trigger_writes_send_the_resolved_subaccount() {
    use polyester::models::{
        BatchCancelItem, BatchReplaceItem, CancelAllOpts, CancelOrderParams, CreateOrderType,
        CreateSide, CreateTriggerParams, CreateTriggerType, ModifyOrderParams, ModifyTriggerParams,
        OrderKey, PreviewOrderParams, ResumeTriggerParams, StopAllTradingScope,
    };

    for (default, sub, want) in [
        (Some(42), None, Some(42)),
        (Some(43), None, Some(43)),
        (Some(42), Some(44), Some(44)),
        (Some(42), Some(42), Some(42)),
        (None, Some(44), Some(44)),
        (None, None, None),
    ] {
        let (_server, client, seen) = scope_capture_client(default).await;
        macro_rules! captures {
            ($label:literal, $rpcs:expr, $call:expr) => {{
                let _ = $call.await;
                let got = std::mem::take(&mut *seen.lock().unwrap());
                let expected: Vec<(String, Option<u64>)> = $rpcs
                    .iter()
                    .map(|rpc: &&str| (rpc.to_string(), want))
                    .collect();
                assert_eq!(got, expected, "{} default={default:?} sub={sub:?}", $label);
            }};
        }
        let id = || OrderKey::OrderId("9".into());

        captures!(
            "orders.create",
            ["CreateOrder"],
            client.orders.create(scope_order(sub))
        );
        captures!(
            "orders.preview",
            ["PreviewOrder"],
            client.orders.preview({
                let mut params =
                    PreviewOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
                params.quantity = Some(scope_quantity());
                params.price = Some(scope_price());
                params.subaccount_id = sub;
                params
            })
        );
        captures!(
            "orders.batch_create",
            ["BatchCreateOrders"],
            client
                .orders
                .batch_create(vec![scope_order(None)], sub, None)
        );
        captures!(
            "orders.batch_cancel",
            ["BatchCancelOrders"],
            client.orders.batch_cancel(
                vec![BatchCancelItem {
                    key: id(),
                    symbol_id: None
                }],
                sub,
                None
            )
        );
        captures!(
            "orders.batch_replace",
            ["BatchReplaceOrders"],
            client.orders.batch_replace(
                vec![BatchReplaceItem {
                    key: id(),
                    new_price: Some(scope_price()),
                    new_qty: None,
                    new_attached_risk: None,
                    new_client_order_id: None,
                }],
                "BTC-USDT",
                sub,
                None
            )
        );
        captures!(
            "orders.modify",
            ["ModifyOrder"],
            client.orders.modify(ModifyOrderParams {
                symbol: "BTC-USDT".into(),
                key: id(),
                subaccount_id: sub,
                request_id: None,
                new_price: Some(scope_price()),
                new_qty: None,
                new_attached_risk: None,
                behavior: None,
                new_client_order_id: None,
            })
        );
        captures!(
            "orders.cancel_with",
            ["CancelOrder"],
            client.orders.cancel_with(CancelOrderParams {
                key: id(),
                symbol: None,
                symbol_id: None,
                subaccount_id: sub
            })
        );
        captures!(
            "orders.cancel_by_order_id",
            ["CancelOrder"],
            client.orders.cancel_by_order_id("9", sub)
        );
        captures!(
            "orders.cancel_by_client_order_id",
            ["CancelOrder"],
            client
                .orders
                .cancel_by_client_order_id("client-9", None, sub)
        );
        captures!(
            "orders.cancel_all",
            ["CancelAllOrders"],
            client.orders.cancel_all(None, false, sub)
        );
        captures!(
            "orders.cancel_all_with",
            ["CancelAllOrders"],
            client.orders.cancel_all_with(CancelAllOpts {
                subaccount_id: sub,
                ..Default::default()
            })
        );
        captures!(
            "orders.cancel_all_after",
            ["CancelAllAfter"],
            client.orders.cancel_all_after(15, None, sub, None)
        );
        captures!(
            "triggers.create",
            ["CreateTrigger"],
            client.triggers.create({
                let mut params = CreateTriggerParams::new(
                    "BTC-USDT",
                    CreateTriggerType::StopLoss,
                    CreateSide::Sell,
                    CreateOrderType::Market,
                    scope_quantity(),
                );
                params.trigger_price = Some(scope_price());
                params.subaccount_id = sub;
                params.client_trigger_id = "scope-trigger".into();
                params
            })
        );
        captures!(
            "triggers.modify",
            ["ModifyTrigger"],
            client.triggers.modify(ModifyTriggerParams {
                trigger_id: "9".into(),
                subaccount_id: sub,
                symbol: None,
                symbol_id: None,
                trigger_price: None,
                activation_price: None,
                trailing_distance_ticks: None,
                trailing_distance_bps: None,
                max_slippage_ticks: None,
                max_slippage_bps: Some(100),
            })
        );
        captures!(
            "triggers.cancel_by_id",
            ["CancelTrigger"],
            client.triggers.cancel_by_id("9", sub)
        );
        captures!(
            "triggers.pause_by_id",
            ["PauseTrigger"],
            client.triggers.pause_by_id("9", sub)
        );
        captures!(
            "triggers.resume_by_id",
            ["ResumeTrigger"],
            client.triggers.resume_by_id("9", None, sub)
        );
        captures!(
            "triggers.resume_with",
            ["ResumeTrigger"],
            client.triggers.resume_with(ResumeTriggerParams {
                trigger_id: "9".into(),
                subaccount_id: sub,
                symbol: None,
                symbol_id: None,
            })
        );
        captures!(
            "triggers.get_by_id",
            ["GetTrigger"],
            client.triggers.get_by_id("9", sub)
        );
        captures!(
            "stop_all_trading",
            ["ListTriggers", "CancelTrigger", "CancelAllOrders"],
            client.stop_all_trading(StopAllTradingScope {
                subaccount_id: sub,
                ..Default::default()
            })
        );
    }
}

// A raw request that leaves its sub-account unset (`None`, or 0 for a plain
// `u64` field) is sent for the configured default sub-account, as the typed
// helpers are; a sub-account the caller set is sent unchanged, and with no
// default nothing is added. A copy's target is a destination, not the scope
// the call acts in, so it is still sent as built.
#[tokio::test]
async fn raw_requests_use_the_default_subaccount_when_unset() {
    use polyester::proto::auth::v1::{
        CopyAddressBookEntryRequest, CreateAddressBookEntryRequest, CreateAddressBookTagRequest,
        GetAddressBookViewRequest, GetWithdrawWhitelistViewRequest, ListAddressBookEntriesRequest,
        ListInternalTransferWhitelistEntriesRequest, ListSubaccountEventsRequest,
        ListSubaccountMembersRequest, ListTransferCounterpartiesRequest,
        ListTransferDestinationsRequest,
    };
    use polyester::proto::chain::deposit::v1::CreateDepositAddressRequest;
    use polyester::proto::chain::guard::v1::{
        BatchSignProtectedActionsRequest, CreateGuardSignerWalletRequest,
        ExportGuardSignerWalletRequest, GetGuardSignerStatusRequest,
        RotateGuardSignerWalletRequest, SignProtectedActionRequest,
    };
    use polyester::proto::ledger::read::v1::{
        GetBalanceHistoryRequest, GetBalancesRequest, GetEquityHistorySeriesRequest,
        ListTransfersRequest,
    };
    use polyester::proto::orders::v1::CancelOrderRequest;
    use polyester::proto::triggers::v1::{
        CancelOcoRequest, CancelTriggerRequest, GetTriggerRequest, ListTriggerEventsRequest,
        ListTriggersRequest, PauseTriggerRequest, ResumeTriggerRequest,
    };

    for (default, sub, want) in [
        (Some(42), None, Some(42)),
        (Some(42), Some(44), Some(44)),
        (Some(42), Some(42), Some(42)),
        (None, Some(44), Some(44)),
        (None, None, None),
    ] {
        let (_server, client, seen) = scope_capture_client(default).await;
        let subaccount_id = sub;
        let plain_id = sub.unwrap_or(0);
        let _ = client
            .balances
            .list(GetBalancesRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .balances
            .get_balance_history(GetBalanceHistoryRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .balances
            .get_equity_history(GetEquityHistorySeriesRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .balances
            .list_transfers(ListTransfersRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .transfers
            .list(ListTransfersRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .orders
            .cancel(CancelOrderRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .list(ListTriggersRequest {
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .get(GetTriggerRequest {
                trigger_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .list_events(ListTriggerEventsRequest {
                trigger_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .cancel(CancelTriggerRequest {
                trigger_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .cancel_oco(CancelOcoRequest {
                order_list_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .pause(PauseTriggerRequest {
                trigger_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .triggers
            .resume(ResumeTriggerRequest {
                trigger_id: 9,
                subaccount_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .deposit
            .create_address(CreateDepositAddressRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .list_entries(ListAddressBookEntriesRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .list_transfer_counterparties(ListTransferCounterpartiesRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .list_transfer_destinations(ListTransferDestinationsRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .list_internal_transfer_whitelist_entries(ListInternalTransferWhitelistEntriesRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .get_withdraw_whitelist_view(GetWithdrawWhitelistViewRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .get_view(GetAddressBookViewRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .create_entry(CreateAddressBookEntryRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .address_book
            .create_tag(CreateAddressBookTagRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .get_status(GetGuardSignerStatusRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .create_wallet(CreateGuardSignerWalletRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .sign_protected_action(SignProtectedActionRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .batch_sign_protected_actions(BatchSignProtectedActionsRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .rotate_wallet(RotateGuardSignerWalletRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .guard_signer
            .export_wallet(ExportGuardSignerWalletRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .sub_accounts
            .list_members(ListSubaccountMembersRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let _ = client
            .sub_accounts
            .list_activity(ListSubaccountEventsRequest {
                subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let got = std::mem::take(&mut *seen.lock().unwrap());
        let expected: Vec<(String, Option<u64>)> = [
            "GetBalances",
            "GetBalanceHistory",
            "GetEquityHistorySeries",
            "ListTransfers",
            "ListTransfers",
            "CancelOrder",
            "ListTriggers",
            "GetTrigger",
            "ListTriggerEvents",
            "CancelTrigger",
            "CancelOco",
            "PauseTrigger",
            "ResumeTrigger",
            "CreateDepositAddress",
            "ListAddressBookEntries",
            "ListTransferCounterparties",
            "ListTransferDestinations",
            "ListInternalTransferWhitelistEntries",
            "GetWithdrawWhitelistView",
            "GetAddressBookView",
            "CreateAddressBookEntry",
            "CreateAddressBookTag",
            "GetGuardSignerStatus",
            "CreateGuardSignerWallet",
            "SignProtectedAction",
            "BatchSignProtectedActions",
            "RotateGuardSignerWallet",
            "ExportGuardSignerWallet",
            "ListSubaccountMembers",
            "ListSubaccountActivity",
        ]
        .map(|rpc| (rpc.to_owned(), want))
        .to_vec();
        let wrong: Vec<_> = got
            .iter()
            .zip(&expected)
            .filter(|(got, want)| got != want)
            .collect();
        assert!(
            got.len() == expected.len() && wrong.is_empty(),
            "default={default:?} sub={sub:?}: {} requests, wrong {wrong:?}",
            got.len()
        );

        // The copy's target is sent exactly as built.
        let _ = client
            .address_book
            .copy_entry(CopyAddressBookEntryRequest {
                target_subaccount_id: plain_id,
                ..Default::default()
            })
            .await;
        let got = std::mem::take(&mut *seen.lock().unwrap());
        assert_eq!(got, vec![("CopyAddressBookEntry".to_owned(), sub)]);
    }
}

// Reads take the public IDs other reads return: `sub_accounts.get` takes a
// sub-account's public ID (base58, or decimal), and `triggers.list_events_by_id`
// a trigger's, with the configured default sub-account as the typed helpers
// apply it. An ID that does not parse, or names 0, is refused before sending.
#[tokio::test]
async fn subaccount_and_trigger_event_reads_take_public_ids() {
    use polyester::codecs::scalars::format_id;
    use polyester::models::ListTriggerEventsOpts;
    use polyester::proto::auth::v1::GetSubaccountRequest;
    use polyester::proto::triggers::v1::{ListTriggerEventsRequest, TriggerEventType};
    use polyester::services::GetSubaccountOpts;

    const SUB: u64 = 98_765_432_109_876_543;
    const TRIGGER: u64 = 13_835_058_055_282_163_729;
    type Seen = Arc<std::sync::Mutex<Vec<(String, u64, Option<u64>, i32, u32, String)>>>;
    let seen = Seen::default();
    let captured = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        let rpc = request.path.rsplit('/').next().unwrap().to_owned();
        let row = match rpc.as_str() {
            "GetSubaccount" => {
                let req = GetSubaccountRequest::decode_from_slice(&request.body).unwrap();
                (rpc, req.subaccount_id, None, 0, 0, String::new())
            }
            "ListTriggerEvents" => {
                let req = ListTriggerEventsRequest::decode_from_slice(&request.body).unwrap();
                (
                    rpc,
                    req.trigger_id,
                    req.subaccount_id,
                    req.event_type.to_i32(),
                    req.limit,
                    req.page_token,
                )
            }
            _ => return hardening_support::HttpScript::NotFound,
        };
        captured.lock().unwrap().push(row);
        json_body(
            400,
            "application/json",
            r#"{"code":"failed_precondition","message":"capture complete"}"#,
        )
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("public-ids".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some(format_id(42)),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();

    for id in [
        format_id(SUB),
        SUB.to_string(),
        format!(" {} ", format_id(SUB)),
    ] {
        let _ = client
            .sub_accounts
            .get(&id, GetSubaccountOpts::default())
            .await;
    }
    let _ = client
        .triggers
        .list_events_by_id(&format_id(TRIGGER), ListTriggerEventsOpts::default())
        .await;
    let _ = client
        .triggers
        .list_events_by_id(
            &TRIGGER.to_string(),
            ListTriggerEventsOpts {
                subaccount_id: Some(44),
                limit: Some(25),
                event_type: Some("fired".into()),
                page_token: Some("next".into()),
            },
        )
        .await;
    let fired = TriggerEventType::EVENT_FIRED as i32;
    assert_eq!(
        std::mem::take(&mut *seen.lock().unwrap()),
        vec![
            ("GetSubaccount".to_owned(), SUB, None, 0, 0, String::new()),
            ("GetSubaccount".to_owned(), SUB, None, 0, 0, String::new()),
            ("GetSubaccount".to_owned(), SUB, None, 0, 0, String::new()),
            (
                "ListTriggerEvents".to_owned(),
                TRIGGER,
                Some(42),
                0,
                0,
                String::new()
            ),
            (
                "ListTriggerEvents".to_owned(),
                TRIGGER,
                Some(44),
                fired,
                25,
                "next".to_owned()
            ),
        ]
    );

    let refused = [
        client
            .sub_accounts
            .get("1", GetSubaccountOpts::default())
            .await
            .map(|_| ()),
        client
            .sub_accounts
            .get("not-base58!", GetSubaccountOpts::default())
            .await
            .map(|_| ()),
        client
            .triggers
            .list_events_by_id("", ListTriggerEventsOpts::default())
            .await
            .map(|_| ()),
        client
            .triggers
            .list_events_by_id(
                &format_id(TRIGGER),
                ListTriggerEventsOpts {
                    event_type: Some("exploded".into()),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ()),
    ];
    for result in refused {
        let err = result.expect_err("refused before sending");
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
    }
    assert!(seen.lock().unwrap().is_empty(), "nothing else was sent");
}

// A quantity scale of 0 is a real scale. The published config omits a zero
// `quantityScale`, so BONK and SHIB arrive without one; their supplies are
// whole units. Scale 6, 8 and 18 assets are controls.
#[test]
fn zero_scale_zipped_supply_reads_whole_units() {
    use polyester::catalogs::Manager;
    use polyester::codecs::decode::{
        deposit_withdraw_config_from_proto, zipped_asset_supply_batch_from_bytes,
    };
    use polyester::codecs::format_ledger_u64;
    use polyester::proto::chain::zipper::v1::{
        GetDepositWithdrawConfigResponse, ZippedAssetSupplyBatch, ZippedAssetSupplyUpdate,
    };

    let config: GetDepositWithdrawConfigResponse = serde_json::from_value(json!({
        "chains": [{"chainId": 1, "code": "ethereum", "name": "Ethereum"}],
        "assets": [
            {"asset": "BONK", "ledgerId": 45, "variants": [{"zippedAssetId": 245, "chainId": 1}]},
            {"asset": "SHIB", "ledgerId": 47, "variants": [{"zippedAssetId": 247, "chainId": 1}]},
            {"asset": "USDT", "ledgerId": 11, "quantityScale": 6,
             "variants": [{"zippedAssetId": 3, "chainId": 1}]},
            {"asset": "BTC", "ledgerId": 12, "quantityScale": 8,
             "variants": [{"zippedAssetId": 1, "chainId": 1}]},
            {"asset": "ETH", "ledgerId": 13, "quantityScale": 18,
             "variants": [{"zippedAssetId": 13, "chainId": 1}]}
        ]
    }))
    .unwrap();
    let catalogs = Manager::new();
    catalogs
        .hydrate_zipper_config(&deposit_withdraw_config_from_proto(&config))
        .unwrap();
    assert_eq!(catalogs.quantity_scale_for_zipped_asset_id(245), Some(0));

    let updates = [
        (245, 3_582_802, "3582802"),
        (247, 2_232_802, "2232802"),
        (3, 1_500_000, "1.5"),
        (1, 150_000_000, "1.5"),
        (13, 1_500_000_000_000_000_000, "1.5"),
    ];
    let batch = ZippedAssetSupplyBatch {
        updates: updates
            .iter()
            .map(|(id, q, _)| ZippedAssetSupplyUpdate {
                zipped_asset_id: *id,
                supply_q: *q,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let decoded = zipped_asset_supply_batch_from_bytes(&batch.encode_to_vec(), |id| {
        catalogs.quantity_scale_for_zipped_asset_id(id)
    })
    .unwrap();
    let got: Vec<(u32, String)> = decoded
        .updates
        .into_iter()
        .map(|u| (u.zipped_asset_id, u.supply))
        .collect();
    let want: Vec<(u32, String)> = updates
        .iter()
        .map(|(id, _, s)| (*id, (*s).to_owned()))
        .collect();
    assert_eq!(got, want);

    let unknown = ZippedAssetSupplyBatch {
        updates: vec![ZippedAssetSupplyUpdate {
            zipped_asset_id: 999,
            supply_q: 3_582_802,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        zipped_asset_supply_batch_from_bytes(&unknown.encode_to_vec(), |id| {
            catalogs.quantity_scale_for_zipped_asset_id(id)
        })
        .is_err(),
        "an asset missing from the catalog must not decode with a guessed scale"
    );

    assert_eq!(format_ledger_u64(3_582_802, 0).unwrap(), "3582802");
    assert_eq!(format_ledger_u128("3582802", 0).unwrap(), "3582802");
    assert_eq!(
        format_ledger_u128("340282366920938463463374607431768211455", 0).unwrap(),
        "340282366920938463463374607431768211455"
    );
    assert_eq!(format_ledger_u128("1500000", 6).unwrap(), "1.5");
    assert_eq!(format_ledger_u128("150000000", 8).unwrap(), "1.5");
    assert_eq!(
        format_ledger_u128("1500000000000000000", 18).unwrap(),
        "1.5"
    );
}

// Guard approvals keep the exact signed nonce space and deadline, in both
// codecs, including values above 2^53, and rotation keeps its optional approval.
#[tokio::test]
async fn guard_signer_approvals_keep_nonce_and_deadline_in_both_codecs() {
    use polyester::models::GuardApproval;
    use polyester::proto::chain::guard::v1::{
        BatchSignProtectedActionsRequest, BatchSignProtectedActionsResponse,
        GuardApproval as ProtoApproval, RotateGuardSignerWalletRequest,
        RotateGuardSignerWalletResponse, SignProtectedActionRequest, SignProtectedActionResponse,
    };
    use polyester::transport::WireFormat;

    const NONCE_SPACE: u64 = u64::MAX;
    const DEADLINE_UNIX: u64 = 9_007_199_254_740_993;
    fn approval(sig: u8) -> ProtoApproval {
        ProtoApproval {
            nonce_space: NONCE_SPACE,
            deadline_unix: DEADLINE_UNIX,
            signature: vec![0xab, sig],
            ..Default::default()
        }
    }
    fn expected(sig: &str) -> GuardApproval {
        GuardApproval {
            approval_id: String::new(),
            signature: sig.into(),
            nonce_space: NONCE_SPACE,
            deadline_unix: DEADLINE_UNIX,
        }
    }

    for json_wire in [false, true] {
        for rotate_approval in [true, false] {
            let server = MockHttpServer::spawn(move |req| {
                let reply = |body: Vec<u8>, json: Vec<u8>| {
                    if json_wire {
                        hardening_support::HttpScript::Json {
                            status: 200,
                            body: json,
                        }
                    } else {
                        hardening_support::HttpScript::Raw {
                            status: 200,
                            headers: vec![("Content-Type".into(), "application/proto".into())],
                            body,
                        }
                    }
                };
                macro_rules! respond {
                    ($msg:expr) => {{
                        let msg = $msg;
                        reply(msg.encode_to_vec(), serde_json::to_vec(&msg).unwrap())
                    }};
                }
                if req.path.ends_with("/SignProtectedAction") {
                    respond!(SignProtectedActionResponse {
                        approval: approval(0x01).into(),
                        ..Default::default()
                    })
                } else if req.path.ends_with("/BatchSignProtectedActions") {
                    respond!(BatchSignProtectedActionsResponse {
                        approvals: vec![
                            approval(0x02),
                            ProtoApproval {
                                signature: vec![0xab, 0x03],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    })
                } else if req.path.ends_with("/RotateGuardSignerWallet") {
                    respond!(RotateGuardSignerWalletResponse {
                        new_signer_address: "0x2222222222222222222222222222222222222222".into(),
                        approval: if rotate_approval {
                            approval(0x04).into()
                        } else {
                            Default::default()
                        },
                        ..Default::default()
                    })
                } else {
                    panic!("unexpected path {}", req.path)
                }
            })
            .await;
            let client = Client::new(Config {
                api_url: server.base_url(),
                api_key_id: Some("guard-test".into()),
                api_private_key: Some(TEST_KEY.into()),
                hydrate_catalogs: false,
                wire_format: if json_wire {
                    WireFormat::Json
                } else {
                    WireFormat::Binary
                },
                ..Default::default()
            })
            .unwrap();

            let single = client
                .guard_signer
                .sign_protected_action(SignProtectedActionRequest::default())
                .await
                .unwrap();
            assert_eq!(single, Some(expected("ab01")), "json={json_wire}");

            let batch = client
                .guard_signer
                .batch_sign_protected_actions(BatchSignProtectedActionsRequest::default())
                .await
                .unwrap();
            assert_eq!(
                batch.approvals,
                vec![
                    expected("ab02"),
                    GuardApproval {
                        approval_id: String::new(),
                        signature: "ab03".into(),
                        nonce_space: 0,
                        deadline_unix: 0,
                    },
                ],
                "json={json_wire}"
            );

            let rotated = client
                .guard_signer
                .rotate_wallet(RotateGuardSignerWalletRequest::default())
                .await
                .unwrap();
            assert_eq!(
                rotated.signer_wallet,
                "0x2222222222222222222222222222222222222222"
            );
            assert_eq!(
                rotated.approval,
                rotate_approval.then(|| expected("ab04")),
                "json={json_wire} rotate_approval={rotate_approval}"
            );
        }
    }
}

// Realtime policy updates keep the market scope on both policy types, and the
// order caps and trading halt on sub-account policies.
#[test]
fn realtime_policy_updates_keep_scope_caps_and_halt() {
    use polyester::codecs::decode::{api_policy_from_bytes, subaccount_policy_from_bytes};
    use polyester::proto::auth::v1::{
        ApiPolicyView, SpotMarketRule as ProtoRule, SubaccountPolicyView, market_scope,
    };

    let rules = || {
        vec![
            ProtoRule {
                symbol_id: 1,
                ..Default::default()
            },
            ProtoRule {
                symbol_id: 2,
                ..Default::default()
            },
        ]
    };

    // ALL with a populated display list, caps set and trading halted.
    let all = SubaccountPolicyView {
        id: 7,
        spot_markets: rules(),
        spot_market_scope: market_scope::Value::ALL.into(),
        max_order_notional: 9_007_199_254_740_993,
        max_open_orders: 250,
        trading_halted: true,
        ..Default::default()
    };
    let got = subaccount_policy_from_bytes(&all.encode_to_vec()).unwrap();
    assert_eq!(got.spot_market_scope, "ALL");
    assert_eq!(got.spot_markets.len(), 2);
    assert_eq!(got.max_order_notional, 9_007_199_254_740_993);
    assert_eq!(got.max_open_orders, 250);
    assert!(got.trading_halted);

    let allowlist = SubaccountPolicyView {
        id: 8,
        spot_markets: rules(),
        spot_market_scope: market_scope::Value::ALLOWLIST.into(),
        ..Default::default()
    };
    let got = subaccount_policy_from_bytes(&allowlist.encode_to_vec()).unwrap();
    assert_eq!(got.spot_market_scope, "ALLOWLIST");
    assert_eq!(got.max_order_notional, 0, "0 means no explicit cap");
    assert_eq!(got.max_open_orders, 0, "0 means no explicit cap");
    assert!(!got.trading_halted);

    // Only an id: every scope, cap and halt field is at its default.
    let defaults = SubaccountPolicyView {
        id: 10,
        ..Default::default()
    };
    let defaults = subaccount_policy_from_bytes(&defaults.encode_to_vec()).unwrap();
    assert_eq!(defaults.spot_market_scope, "");
    assert_eq!(defaults.max_order_notional, 0);
    assert_eq!(defaults.max_open_orders, 0);
    assert!(!defaults.trading_halted);

    for (scope, label) in [
        (market_scope::Value::ALL, "ALL"),
        (market_scope::Value::ALLOWLIST, "ALLOWLIST"),
        (market_scope::Value::UNSPECIFIED, ""),
    ] {
        let view = ApiPolicyView {
            id: 9,
            spot_markets: rules(),
            spot_market_scope: scope.into(),
            ..Default::default()
        };
        let got = api_policy_from_bytes(&view.encode_to_vec()).unwrap();
        assert_eq!(got.spot_market_scope, label);
        assert_eq!(got.spot_markets.len(), 2);
    }

    // A scope newer than this SDK stays visible instead of reading as unspecified.
    let newer = ApiPolicyView {
        id: 11,
        spot_market_scope: buffa::EnumValue::Unknown(7),
        ..Default::default()
    };
    let got = api_policy_from_bytes(&newer.encode_to_vec()).unwrap();
    assert_eq!(got.spot_market_scope, "UNKNOWN(7)");
}

// Batch cancel and replace accept the 50 items the public contract allows and
// refuse 51 before sending; batch create keeps its separate limit of 20.
#[tokio::test]
async fn l2_batch_cancel_and_replace_send_up_to_fifty_items() {
    use polyester::models::{BatchCancelItem, BatchReplaceItem, OrderKey};
    use polyester::proto::orders::v1::{BatchCancelOrdersRequest, BatchReplaceOrdersRequest};
    use polyester::types::Price;

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let seen = Arc::new(std::sync::Mutex::new(Vec::<(String, usize)>::new()));
    let sink = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        let items = match req.path.as_str() {
            path if path == SPOT_CONFIG_PATH => return connect_proto_ok(&spot),
            path if path == ZIPPER_CONFIG_PATH => return connect_proto_ok(&zipper),
            "/orders.v1.OrdersService/BatchCancelOrders" => {
                BatchCancelOrdersRequest::decode_from_slice(&req.body).map(|r| r.items.len())
            }
            "/orders.v1.OrdersService/BatchReplaceOrders" => {
                BatchReplaceOrdersRequest::decode_from_slice(&req.body).map(|r| r.items.len())
            }
            _ => return hardening_support::HttpScript::NotFound,
        };
        sink.lock()
            .unwrap()
            .push((req.path.clone(), items.expect("request decodes")));
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("catalogs");

    let cancel = |n: usize| {
        (0..n)
            .map(|i| BatchCancelItem {
                key: OrderKey::OrderId((i + 1).to_string()),
                symbol_id: None,
            })
            .collect::<Vec<_>>()
    };
    let replace = |n: usize| {
        (0..n)
            .map(|i| BatchReplaceItem {
                key: OrderKey::OrderId((i + 1).to_string()),
                new_price: Some(Price::from_ticks(1_000_000_000, None).expect("price")),
                new_qty: None,
                new_attached_risk: None,
                new_client_order_id: None,
            })
            .collect::<Vec<_>>()
    };
    for n in [20, 21, 50] {
        let _ = client.orders.batch_cancel(cancel(n), None, None).await;
        let _ = client
            .orders
            .batch_replace(replace(n), "BTC-USDT", None, None)
            .await;
    }
    let err = client
        .orders
        .batch_cancel(cancel(51), None, None)
        .await
        .expect_err("51 cancel items exceed the contract");
    assert!(
        err.to_string()
            .contains("batch_cancel accepts at most 50 items; received 51"),
        "{err}"
    );
    let err = client
        .orders
        .batch_replace(replace(51), "BTC-USDT", None, None)
        .await
        .expect_err("51 replace items exceed the contract");
    assert!(
        err.to_string()
            .contains("batch_replace accepts at most 50 items; received 51"),
        "{err}"
    );
    let cancel_path = "/orders.v1.OrdersService/BatchCancelOrders".to_owned();
    let replace_path = "/orders.v1.OrdersService/BatchReplaceOrders".to_owned();
    assert_eq!(
        *seen.lock().unwrap(),
        [
            (cancel_path.clone(), 20),
            (replace_path.clone(), 20),
            (cancel_path.clone(), 21),
            (replace_path.clone(), 21),
            (cancel_path, 50),
            (replace_path, 50),
        ]
    );
}

// A batch replace item's attached risk reaches the wire; it is not dropped.
#[tokio::test]
async fn l2_batch_replace_sends_new_attached_risk() {
    use polyester::models::{AttachedRisk, BatchReplaceItem, OrderKey, RiskLeg};
    use polyester::proto::orders::v1::{BatchReplaceOrdersRequest, risk_policy};
    use polyester::types::Price;

    let spot = spot_config_fixture();
    let zipper = zipper_config_fixture();
    let seen = Arc::new(std::sync::Mutex::new(
        Vec::<BatchReplaceOrdersRequest>::new(),
    ));
    let sink = seen.clone();
    let http = MockHttpServer::spawn(move |req| {
        if req.path == SPOT_CONFIG_PATH {
            return connect_proto_ok(&spot);
        }
        if req.path == ZIPPER_CONFIG_PATH {
            return connect_proto_ok(&zipper);
        }
        if req.path == "/orders.v1.OrdersService/BatchReplaceOrders" {
            sink.lock()
                .unwrap()
                .push(BatchReplaceOrdersRequest::decode_from_slice(&req.body).expect("decodes"));
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: true,
        ..Default::default()
    })
    .expect("client");
    client.wait_for_catalogs().await.expect("catalogs");

    let stop = Price::from_ticks(30_000_000_000_000, Some("BTC-USDT".into())).expect("price");
    let _ = client
        .orders
        .batch_replace(
            vec![BatchReplaceItem {
                key: OrderKey::OrderId("9".into()),
                new_price: None,
                new_qty: None,
                new_attached_risk: Some(AttachedRisk {
                    stop_loss: Some(RiskLeg::market(stop)),
                    ..Default::default()
                }),
                new_client_order_id: None,
            }],
            "BTC-USDT",
            None,
            None,
        )
        .await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let risk = seen[0].items[0]
        .new_attached_risk
        .as_option()
        .expect("attached risk sent");
    let Some(risk_policy::StopLeg::StopLoss(stop_loss)) = risk.stop_leg.as_ref() else {
        panic!("stop loss sent: {risk:?}");
    };
    assert_eq!(stop_loss.trigger_price_ticks, 30_000_000_000_000);
}

// Sub-account 0 is not a sub-account: on the wire 0 means unset (the main
// account). An explicit Some(0) or 0 is refused before any request is sent.
#[tokio::test]
async fn explicit_subaccount_zero_is_refused_before_sending() {
    use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide, CreateTimeInForce};
    use polyester::proto::orders::v1::{CreateOrderRequest, CreateOrderResponse};
    use polyester::services::GetSubaccountOpts;
    use polyester::{Price, Quantity};
    use std::sync::Mutex;

    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_handler = observed.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path == "/orders.v1.OrdersService/CreateOrder" {
            let decoded = CreateOrderRequest::decode_from_slice(&request.body).unwrap();
            observed_handler.lock().unwrap().push(decoded.subaccount_id);
            return connect_proto_ok(&CreateOrderResponse {
                order_id: 9,
                client_order_id: "zero-scope".into(),
                ..Default::default()
            });
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scope-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: Some("5".into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    let order = |subaccount_id: Option<u64>| {
        let mut p = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
        p.quantity = Some(
            Quantity::from_scaled(
                1,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .unwrap(),
        );
        p.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).unwrap());
        p.time_in_force = Some(CreateTimeInForce::Gtc);
        p.client_order_id = Some("zero-scope".into());
        p.subaccount_id = subaccount_id;
        p
    };

    // Control: a real sub-account ID is sent.
    client.orders.create(order(Some(42))).await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![Some(42)]);
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);

    let refused = [
        client.orders.create(order(Some(0))).await.map(|_| ()),
        client.orders.list_open(Some(0)).await.map(|_| ()),
        client.api_keys.list(Some(0)).await.map(|_| ()),
        client
            .sub_accounts
            .get("1", GetSubaccountOpts::default())
            .await
            .map(|_| ()),
    ];
    for result in refused {
        let err = result.expect_err("sub-account 0 must be refused");
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
        assert!(err.to_string().contains("not 0"), "{err}");
    }
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

// Request-object wrappers forward the caller's request; an explicit
// subaccount_id of Some(0) is refused before sending, None still means none.
async fn zero_scope_request_client() -> (MockHttpServer, Client) {
    use polyester::proto::ledger::read::v1::GetBalancesResponse;
    let server = MockHttpServer::spawn(|request| {
        if request.path == GET_BALANCES_PATH {
            return connect_proto_ok(&GetBalancesResponse::default());
        }
        hardening_support::HttpScript::NotFound
    })
    .await;
    let client = Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("scope-test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    (server, client)
}

#[tokio::test]
async fn request_object_without_subaccount_is_still_sent() {
    use polyester::proto::ledger::read::v1::GetBalancesRequest;
    let (server, client) = zero_scope_request_client().await;
    client
        .balances
        .list(GetBalancesRequest::default())
        .await
        .unwrap();
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

macro_rules! request_object_refuses_subaccount_zero {
    ($($name:ident: |$client:ident| $call:expr;)+) => {$(
        #[tokio::test]
        async fn $name() {
            let (server, $client) = zero_scope_request_client().await;
            let err = $call
                .await
                .map(|_| ())
                .expect_err("sub-account 0 must be refused");
            assert!(matches!(err, Error::Validation(_)), "{err:?}");
            assert!(err.to_string().contains("not 0"), "{err}");
            assert_eq!(server.requests.load(Ordering::SeqCst), 0);
        }
    )+};
}

request_object_refuses_subaccount_zero! {
    balances_list_refuses_subaccount_zero: |client| client.balances.list(
        polyester::proto::ledger::read::v1::GetBalancesRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    balances_get_balance_history_refuses_subaccount_zero: |client| client.balances.get_balance_history(
        polyester::proto::ledger::read::v1::GetBalanceHistoryRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    balances_get_equity_history_refuses_subaccount_zero: |client| client.balances.get_equity_history(
        polyester::proto::ledger::read::v1::GetEquityHistorySeriesRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    balances_list_transfers_refuses_subaccount_zero: |client| client.balances.list_transfers(
        polyester::proto::ledger::read::v1::ListTransfersRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    transfers_list_refuses_subaccount_zero: |client| client.transfers.list(
        polyester::proto::ledger::read::v1::ListTransfersRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    orders_cancel_refuses_subaccount_zero: |client| client.orders.cancel(
        polyester::proto::orders::v1::CancelOrderRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_list_refuses_subaccount_zero: |client| client.triggers.list(
        polyester::proto::triggers::v1::ListTriggersRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_get_refuses_subaccount_zero: |client| client.triggers.get(
        polyester::proto::triggers::v1::GetTriggerRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_cancel_refuses_subaccount_zero: |client| client.triggers.cancel(
        polyester::proto::triggers::v1::CancelTriggerRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_pause_refuses_subaccount_zero: |client| client.triggers.pause(
        polyester::proto::triggers::v1::PauseTriggerRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_resume_refuses_subaccount_zero: |client| client.triggers.resume(
        polyester::proto::triggers::v1::ResumeTriggerRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
    triggers_list_events_refuses_subaccount_zero: |client| client.triggers.list_events(
        polyester::proto::triggers::v1::ListTriggerEventsRequest {
            subaccount_id: Some(0),
            ..Default::default()
        },
    );
}

// A sub-account or account ID that decodes to 0 is refused before anything is
// sent. On the wire 0 is unset: sub-account 0 is the main account, and account
// 0 names no account. "1" is the public ID of 0, and "0" and "00" read as 0.
const ZERO_ID_STRINGS: [&str; 4] = ["0", "00", "1", " 1 "];

async fn zero_id_client(
    default_sub_account_id: Option<&str>,
    default_account_id: Option<&str>,
) -> (Client, MockHttpServer) {
    let server = MockHttpServer::spawn(|_| hardening_support::HttpScript::NotFound).await;
    let ws_url = format!(
        "ws{}/connection/websocket",
        server.base_url().trim_start_matches("http")
    );
    let client = Client::new(Config {
        api_url: server.base_url(),
        ws_url,
        api_key_id: Some("zero-ids".into()),
        api_private_key: Some(TEST_KEY.into()),
        default_sub_account_id: default_sub_account_id.map(str::to_owned),
        default_account_id: Some(default_account_id.unwrap_or("8").to_owned()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8
            }]
        }))
        .unwrap();
    (client, server)
}

/// Records why `result` is not a `label` validation error sent nowhere.
fn check_zero_id_refused(
    failures: &mut Vec<String>,
    name: &str,
    label: &str,
    result: Result<(), Error>,
    server: &MockHttpServer,
) {
    let sent = server.requests.swap(0, Ordering::SeqCst);
    let refused = matches!(&result, Err(Error::Validation(message)) if message.contains(label));
    if !refused || sent != 0 {
        failures.push(format!("{name}: {result:?} after {sent} requests"));
    }
}

fn assert_no_zero_id_failures(failures: Vec<String>) {
    assert!(
        failures.is_empty(),
        "{} sent or not refused:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn zero_default_subaccount_is_refused_before_sending() {
    let mut failures = Vec::new();
    use polyester::models::{
        BatchCancelItem, CancelAllOpts, CreateOrderParams, CreateOrderType, CreateSide,
        CreateTimeInForce, ListTriggersOpts, OrderKey,
    };
    use polyester::{Price, Quantity};

    let order = || {
        let mut p = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Limit);
        p.quantity = Some(
            Quantity::from_scaled(
                1,
                Some(8),
                QuantityDomain::OrderBase,
                Some("BTC-USDT".into()),
                Some(1),
            )
            .unwrap(),
        );
        p.price = Some(Price::from_ticks(1, Some("BTC-USDT".into())).unwrap());
        p.time_in_force = Some(CreateTimeInForce::Gtc);
        p.client_order_id = Some("zero-default".into());
        p
    };
    let id = "j";
    for zero in ZERO_ID_STRINGS {
        let (client, server) = zero_id_client(Some(zero), None).await;
        let calls: Vec<(&str, Result<(), Error>)> = vec![
            (
                "orders.create",
                client.orders.create(order()).await.map(drop),
            ),
            (
                "orders.list_open",
                client.orders.list_open(None).await.map(drop),
            ),
            (
                "orders.list_history",
                client.orders.list_history(None, None).await.map(drop),
            ),
            (
                "orders.get_batch_replace_status",
                client
                    .orders
                    .get_batch_replace_status(id, None)
                    .await
                    .map(drop),
            ),
            (
                "orders.cancel_all_after",
                client
                    .orders
                    .cancel_all_after(10, None, None, None)
                    .await
                    .map(drop),
            ),
            (
                "orders.cancel_all_with",
                client
                    .orders
                    .cancel_all_with(CancelAllOpts {
                        dry_run: true,
                        ..Default::default()
                    })
                    .await
                    .map(drop),
            ),
            (
                "orders.batch_cancel",
                client
                    .orders
                    .batch_cancel(
                        vec![BatchCancelItem {
                            key: OrderKey::OrderId(id.into()),
                            symbol_id: Some(1),
                        }],
                        None,
                        None,
                    )
                    .await
                    .map(drop),
            ),
            ("api_keys.list", client.api_keys.list(None).await.map(drop)),
            (
                "triggers.list_with",
                client
                    .triggers
                    .list_with(ListTriggersOpts::default())
                    .await
                    .map(drop),
            ),
            (
                "triggers.get_by_id",
                client.triggers.get_by_id(id, None).await.map(drop),
            ),
            (
                "triggers.cancel_by_id",
                client.triggers.cancel_by_id(id, None).await.map(drop),
            ),
            (
                "triggers.pause_by_id",
                client.triggers.pause_by_id(id, None).await.map(drop),
            ),
            (
                "fees.get_spot_fee_rates",
                client
                    .fees
                    .get_spot_fee_rates(None, Vec::new())
                    .await
                    .map(drop),
            ),
            (
                "rate_limits.get_trading_rate_limits",
                client
                    .rate_limits
                    .get_trading_rate_limits(None)
                    .await
                    .map(drop),
            ),
        ];
        for (name, result) in calls {
            check_zero_id_refused(
                &mut failures,
                &format!("{name} default {zero:?}"),
                "subaccount_id",
                result,
                &server,
            );
        }
    }
    assert_no_zero_id_failures(failures);
}

#[tokio::test]
async fn omitted_and_non_zero_default_subaccounts_still_send() {
    for default in [None, Some(""), Some("j")] {
        let (client, server) = zero_id_client(default, None).await;
        let result = client.orders.list_open(None).await;
        assert!(
            !matches!(result, Err(Error::Validation(_))),
            "{default:?}: {result:?}"
        );
        assert_eq!(server.requests.load(Ordering::SeqCst), 1, "{default:?}");
    }
}

macro_rules! zero_account_subscribes {
    ($client:expr, $account:expr) => {
        vec![
            (
                "api_keys.subscribe",
                $client.api_keys.subscribe($account).await.map(drop),
            ),
            (
                "balances.subscribe",
                $client.balances.subscribe($account).await.map(drop),
            ),
            (
                "orders.subscribe",
                $client.orders.subscribe($account).await.map(drop),
            ),
            (
                "trades.subscribe",
                $client.trades.subscribe($account).await.map(drop),
            ),
            (
                "transfers.subscribe",
                $client.transfers.subscribe($account).await.map(drop),
            ),
            (
                "triggers.subscribe",
                $client.triggers.subscribe($account).await.map(drop),
            ),
            (
                "triggers.subscribe_events",
                $client.triggers.subscribe_events($account).await.map(drop),
            ),
            (
                "policies.subscribe",
                $client.policies.subscribe($account).await.map(drop),
            ),
            (
                "policies.subscribe_subaccount_policies",
                $client
                    .policies
                    .subscribe_subaccount_policies($account)
                    .await
                    .map(drop),
            ),
            (
                "policies.subscribe_api_policies",
                $client
                    .policies
                    .subscribe_api_policies($account)
                    .await
                    .map(drop),
            ),
            (
                "sub_accounts.subscribe",
                $client.sub_accounts.subscribe($account).await.map(drop),
            ),
            (
                "sub_accounts.subscribe_api_keys",
                $client
                    .sub_accounts
                    .subscribe_api_keys($account)
                    .await
                    .map(drop),
            ),
            (
                "address_book.subscribe",
                $client.address_book.subscribe($account).await.map(drop),
            ),
        ]
    };
}

#[tokio::test]
async fn zero_account_id_is_refused_before_subscribing() {
    let mut failures = Vec::new();
    for zero in ZERO_ID_STRINGS {
        let (client, server) = zero_id_client(None, None).await;
        let mut calls = zero_account_subscribes!(client, Some(zero));
        calls.push((
            "lifecycle.subscribe_open_flows",
            client
                .lifecycle
                .subscribe_open_flows(Some(zero))
                .await
                .map(drop),
        ));
        calls.push((
            "realtime.subscribe_private_orders",
            client
                .realtime
                .subscribe_private_orders(zero)
                .await
                .map(drop),
        ));
        for (name, result) in calls {
            check_zero_id_refused(
                &mut failures,
                &format!("{name} {zero:?}"),
                "account_id",
                result,
                &server,
            );
        }

        let (client, server) = zero_id_client(None, Some(zero)).await;
        for (name, result) in zero_account_subscribes!(client, None) {
            check_zero_id_refused(
                &mut failures,
                &format!("{name} default {zero:?}"),
                "account_id",
                result,
                &server,
            );
        }
    }
    assert_no_zero_id_failures(failures);
}

#[tokio::test]
async fn zero_internal_transfer_destination_is_refused_before_sending() {
    let mut failures = Vec::new();
    use polyester::AssetAmount;
    use polyester::models::CreateInternalTransferParams;

    let amount = AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
    let (client, server) = zero_id_client(None, None).await;
    for zero in ZERO_ID_STRINGS {
        for (label, account, sub) in [
            ("destination_account_id", Some(zero.to_owned()), None),
            ("destination_subaccount_id", None, Some(zero.to_owned())),
        ] {
            let result = client
                .internal_transfers
                .create(CreateInternalTransferParams {
                    asset_id: 7,
                    quantity: amount.clone(),
                    idempotency_key: "zero-destination".into(),
                    subaccount_id: None,
                    destination_account_id: account,
                    destination_subaccount_id: sub,
                    destination_smart_account_address: None,
                    quantity_scale: Some(18),
                })
                .await
                .map(drop);
            check_zero_id_refused(
                &mut failures,
                &format!("internal_transfers.create {label} {zero:?}"),
                label,
                result,
                &server,
            );
        }
    }
    assert_no_zero_id_failures(failures);
}

// A refusal reads the same `ERROR_CODE_` code over REST and ConnectRPC: the REST
// problem names it without the prefix, the typed detail by its enum value.
// Transfer, withdraw and ledger codes read like order codes either way.
#[tokio::test]
async fn l2_refusal_code_is_the_same_over_rest_and_connect() {
    use base64::Engine as _;
    use buffa::Enumeration as _;
    use polyester::proto::chain::withdraw::v1 as withdraw;
    use polyester::proto::ledger::read::v1 as ledger;
    use polyester::proto::orders::v1 as orders;
    use polyester::proto::transfer::v1 as transfer;

    let full = |name: &str| format!("ERROR_CODE_{name}");
    let cases: Vec<(&str, &str, Vec<u8>)> = vec![
        (
            "SAME_SOURCE_DESTINATION",
            "transfer.v1.ErrorDetail",
            transfer::ErrorDetail {
                code: transfer::ErrorCode::from_proto_name(&full("SAME_SOURCE_DESTINATION"))
                    .unwrap()
                    .into(),
                ..Default::default()
            }
            .encode_to_bytes()
            .to_vec(),
        ),
        (
            "INSUFFICIENT_FUNDS",
            "transfer.v1.ErrorDetail",
            transfer::ErrorDetail {
                code: transfer::ErrorCode::from_proto_name(&full("INSUFFICIENT_FUNDS"))
                    .unwrap()
                    .into(),
                ..Default::default()
            }
            .encode_to_bytes()
            .to_vec(),
        ),
        (
            "AMOUNT_BELOW_MINIMUM",
            "chain.withdraw.v1.ErrorDetail",
            withdraw::ErrorDetail {
                code: withdraw::ErrorCode::from_proto_name(&full("AMOUNT_BELOW_MINIMUM"))
                    .unwrap()
                    .into(),
                ..Default::default()
            }
            .encode_to_bytes()
            .to_vec(),
        ),
        (
            "INVALID_SUBACCOUNT_ID",
            "ledger.read.v1.ErrorDetail",
            ledger::ErrorDetail {
                code: ledger::ErrorCode::from_proto_name(&full("INVALID_SUBACCOUNT_ID"))
                    .unwrap()
                    .into(),
                ..Default::default()
            }
            .encode_to_bytes()
            .to_vec(),
        ),
        (
            "QTY_STEP_SIZE",
            "orders.v1.ErrorDetail",
            orders::ErrorDetail {
                code: orders::ErrorCode::from_proto_name(&full("QTY_STEP_SIZE"))
                    .unwrap()
                    .into(),
                ..Default::default()
            }
            .encode_to_bytes()
            .to_vec(),
        ),
    ];
    for (name, detail_type, detail) in cases {
        let value = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&detail);
        let connect = json!({
            "code": "invalid_argument",
            "message": "refused",
            "details": [{"type": detail_type, "value": value}],
        })
        .to_string();
        let rest = json!({"status": 400, "code": name, "detail": "refused"}).to_string();
        for (content_type, body) in [
            ("application/json", connect),
            ("application/problem+json", rest),
        ] {
            let http = MockHttpServer::spawn(move |_| json_body(400, content_type, &body)).await;
            let err = api_key_client(&http)
                .orders
                .list_open(None)
                .await
                .expect_err("refused");
            match &err {
                Error::Api { code, .. } => {
                    assert_eq!(code, &full(name), "{content_type}: {err:?}")
                }
                other => panic!("{content_type}: {other:?}"),
            }
        }
    }
}

// `INVALID_REQUEST` is an auth, transfer and withdraw code; REST can't say which
// service refused, so it stays as sent and still reads as an auth code.
#[tokio::test]
async fn l2_rest_code_the_auth_service_also_names_stays_as_sent() {
    use polyester::proto::auth::v1::AuthErrorCode;
    let rest = json!({"status": 400, "code": "INVALID_REQUEST", "detail": "refused"}).to_string();
    let http =
        MockHttpServer::spawn(move |_| json_body(400, "application/problem+json", &rest)).await;
    let err = api_key_client(&http)
        .orders
        .list_open(None)
        .await
        .expect_err("refused");
    assert!(
        matches!(&err, Error::Api { code, .. } if code == "INVALID_REQUEST"),
        "{err:?}"
    );
    assert_eq!(err.auth_code(), Some(AuthErrorCode::AUTH_INVALID_REQUEST));
}
