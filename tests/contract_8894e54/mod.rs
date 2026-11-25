//! Public contract 8894e54 (exchange release 0.93.7), through the public
//! services against a local server: a refused batch replacement read through
//! `orders.get` carries no order terms, the transfer reason filter, and
//! `GetInternalTransferBatch`'s refusal of a key without
//! `READ_INTERNAL_TRANSFERS`.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use base64::Engine as _;
use buffa::Message as _;
use polyester::codecs::scalars::format_id;
use polyester::models::{OrderKey, SubAccountScope};
use polyester::proto::ledger::read::v1::{ListTransfersRequest, ListTransfersResponse};
use polyester::proto::ledger::v1::TransferCode;
use polyester::proto::orders::v1::GetOrderResponse;
use polyester::proto::transfer::v1::{ErrorCode as TransferErrorCode, ErrorDetail};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

const SUCCESSOR_ID: u64 = 2_001_504_781_434_687_193;
const BATCH_ID: u64 = 2_001_504_781_434_687_001;

/// `GetOrder` of a refused batch replacement's successor within 60 seconds of
/// the batch's acceptance, as the API sends it: `REJECTED` at version 1 with
/// its batch and its own lineage, no terminal time and no order terms (no
/// symbol, side, type, time in force, self-trade prevention mode or fee
/// asset), its quantities and prices 0 placeholders.
fn refused_replacement_reply() -> Value {
    json!({
        "order": {
            "orderId": SUCCESSOR_ID.to_string(),
            "status": "REJECTED",
            "origQtyScaled": "0",
            "cumQtyScaled": "0",
            "inheritedCumQtyScaled": "0",
            "leavesQtyScaled": "0",
            "priceTicks": "0",
            "avgPriceTicks": "0",
            "createdTsNs": "1763600000123456789",
            "rejectReason": "ERROR_CODE_MODIFY_APPLY_EXPIRED",
            "version": 1,
            "batchRequestId": BATCH_ID.to_string(),
            "lineage": {"id": SUCCESSOR_ID.to_string(), "generation": 1}
        },
        "executionsComplete": true
    })
}

async fn client(wire: WireFormat, server: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: wire,
        ..Default::default()
    })
    .expect("client")
}

/// `orders.get` decodes a refused batch replacement's successor, which
/// carries no order terms, without error on both wires: its pair, side,
/// type, time in force, self-trade prevention mode and fee asset are `None`,
/// and its placeholder quantities name no pair, so the loaded catalogs give
/// them no scale.
#[tokio::test]
async fn get_order_decodes_a_refused_batch_replacement_without_terms() {
    let reply = refused_replacement_reply();
    let binary = serde_json::from_value::<GetOrderResponse>(reply.clone())
        .expect("the fixed reply is a GetOrderResponse")
        .encode_to_vec();
    for wire in [WireFormat::Binary, WireFormat::Json] {
        let (reply, binary) = (reply.clone(), binary.clone());
        let server = MockHttpServer::spawn(move |request| {
            assert_eq!(request.path, "/orders.v1.OrdersReadService/GetOrder");
            if wire == WireFormat::Json {
                HttpScript::Json {
                    status: 200,
                    body: serde_json::to_vec(&reply).unwrap(),
                }
            } else {
                HttpScript::Raw {
                    status: 200,
                    headers: vec![("Content-Type".into(), "application/proto".into())],
                    body: binary.clone(),
                }
            }
        })
        .await;
        let client = client(wire, &server).await;
        client
            .catalogs
            .hydrate_spot_config_json(json!({
                "pairs": [{
                    "symbol": "BTC-USDT",
                    "symbol_id": 1,
                    "base_quantity_scale": 8,
                    "quote_quantity_scale": 6
                }]
            }))
            .expect("catalog");
        let order_id = format_id(SUCCESSOR_ID);
        let read = client
            .orders
            .get(
                OrderKey::OrderId(order_id.clone()),
                SubAccountScope::Default,
            )
            .await
            .unwrap_or_else(|err| panic!("{wire:?}: {err}"));
        let order = read.order.expect("order");
        assert!(read.executions_complete, "{wire:?}");
        assert_eq!(order.order_id, order_id, "{wire:?}");
        assert_eq!(order.status, "rejected", "{wire:?}");
        assert_eq!(
            order.reject_reason.as_deref(),
            Some("modify_apply_expired"),
            "{wire:?}"
        );
        assert_eq!(order.version, 1, "{wire:?}");
        assert_eq!(order.batch_request_id, format_id(BATCH_ID), "{wire:?}");
        assert_eq!(order.created_ts_ns, "1763600000123456789", "{wire:?}");
        assert_eq!(order.terminal_ts_ns, "", "{wire:?}");
        let lineage = order.lineage.as_ref().expect("lineage");
        assert_eq!(
            (lineage.id.as_str(), lineage.generation),
            (order_id.as_str(), 1)
        );
        assert_eq!(
            (
                order.symbol_id,
                order.side.as_deref(),
                order.order_type.as_deref(),
                order.tif.as_deref(),
                order.self_trade_prevention_mode.as_deref(),
                order.fee_asset.as_deref(),
            ),
            (None, None, None, None, None, None),
            "{wire:?}: the six order terms"
        );
        // The wire's "0" placeholders, as the model reads them: prices of 0
        // are absent, and the original, cumulative, remaining, inherited,
        // display and hidden quantities are zero quantities that name no
        // pair.
        for (name, qty) in [
            ("orig_qty", &order.orig_qty),
            ("cum_qty", &order.cum_qty),
            ("leaves_qty", &order.leaves_qty),
            ("inherited_cum_qty", &order.inherited_cum_qty),
            ("display_qty", &order.display_qty),
            ("hidden_qty", &order.hidden_qty),
        ] {
            let qty = qty
                .as_ref()
                .unwrap_or_else(|| panic!("{wire:?}: {name} missing"));
            assert_eq!(
                (qty.as_scaled(), qty.scale(), qty.symbol_id()),
                (0, None, None),
                "{wire:?}: {name} is a 0 placeholder naming no pair"
            );
        }
        assert!(order.price.is_none(), "{wire:?}: price {:?}", order.price);
        assert!(
            order.avg_px.is_none(),
            "{wire:?}: avg_px {:?}",
            order.avg_px
        );
        assert_eq!(
            (
                order.avg_price_e18.as_str(),
                order.cum_notional.as_str(),
                order.fee_total.is_none()
            ),
            ("0", "0", true),
            "{wire:?}"
        );
    }
}

/// An order that carries its terms still reads every one of them, and its
/// quantities take the pair's scale from the catalogs.
#[tokio::test]
async fn get_order_with_its_terms_still_reads_them() {
    let reply = json!({
        "order": {
            "orderId": "7",
            "symbolId": 1,
            "side": "BUY",
            "orderType": "LIMIT",
            "timeInForce": "GTC",
            "selfTradePreventionMode": "EXPIRE_TAKER",
            "feeAsset": "QUOTE",
            "status": "WORKING",
            "origQtyScaled": "100",
            "leavesQtyScaled": "100"
        }
    });
    let body = serde_json::from_value::<GetOrderResponse>(reply)
        .unwrap()
        .encode_to_vec();
    let server = MockHttpServer::spawn(move |_| HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), "application/proto".into())],
        body: body.clone(),
    })
    .await;
    let client = client(WireFormat::Binary, &server).await;
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 8,
                "quote_quantity_scale": 6
            }]
        }))
        .expect("catalog");
    let order = client
        .orders
        .get(OrderKey::OrderId(format_id(7)), SubAccountScope::Default)
        .await
        .unwrap()
        .order
        .expect("order");
    assert_eq!(
        (
            order.symbol_id,
            order.side.as_deref(),
            order.order_type.as_deref(),
            order.tif.as_deref(),
            order.self_trade_prevention_mode.as_deref(),
            order.fee_asset.as_deref(),
        ),
        (
            Some(1),
            Some("buy"),
            Some("limit"),
            Some("gtc"),
            Some("expire_taker"),
            Some("quote")
        )
    );
    assert_eq!(order.orig_qty.as_ref().and_then(|q| q.scale()), Some(8));
}

/// Both transfer list wrappers send the reason filter with the other
/// filters on both wires, and continue a filtered listing with the same
/// reason; a listing without one sends none.
#[tokio::test]
async fn transfer_lists_send_the_transfer_code() {
    for wire in [WireFormat::Binary, WireFormat::Json] {
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let log = bodies.clone();
        let server = MockHttpServer::spawn(move |request| {
            assert!(request.path.ends_with("/ListTransfers"));
            log.lock().unwrap().push(request.body.clone());
            let response = ListTransfersResponse {
                next_page_token: "next".into(),
                ..Default::default()
            };
            if wire == WireFormat::Json {
                HttpScript::Json {
                    status: 200,
                    body: serde_json::to_vec(&response).unwrap(),
                }
            } else {
                connect_proto_ok(&response)
            }
        })
        .await;
        let client = client(wire, &server).await;
        let since = 1_700_000_000_000_000;
        let filtered = ListTransfersRequest {
            limit: 10,
            reversed: true,
            ts_min_us: since,
            transfer_code: TransferCode::DEPOSIT.into(),
            ..Default::default()
        };
        let first = client
            .transfers
            .list(filtered.clone())
            .await
            .unwrap_or_else(|err| panic!("{wire:?}: {err}"));
        client
            .balances
            .list_transfers(ListTransfersRequest {
                page_token: first.next_page_token.clone(),
                ..filtered.clone()
            })
            .await
            .unwrap_or_else(|err| panic!("{wire:?}: {err}"));
        client
            .transfers
            .list(ListTransfersRequest {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap_or_else(|err| panic!("{wire:?}: {err}"));
        let bodies = bodies.lock().unwrap().clone();
        assert_eq!(bodies.len(), 3, "{wire:?}");
        let sent: Vec<ListTransfersRequest> = bodies
            .iter()
            .map(|body| {
                if wire == WireFormat::Json {
                    serde_json::from_slice(body).unwrap()
                } else {
                    ListTransfersRequest::decode_from_slice(body).unwrap()
                }
            })
            .collect();
        for (request, token) in [(&sent[0], ""), (&sent[1], "next")] {
            assert_eq!(
                request.transfer_code.to_i32(),
                1000,
                "{wire:?}: DEPOSIT is sent"
            );
            assert_eq!(
                (
                    request.page_token.as_str(),
                    request.ts_min_us,
                    request.reversed,
                    request.limit
                ),
                (token, since, true, 10),
                "{wire:?}"
            );
        }
        assert_eq!(sent[2].transfer_code.to_i32(), 0, "{wire:?}: no reason");
        if wire == WireFormat::Json {
            let named = |body: &[u8]| {
                serde_json::from_slice::<Value>(body)
                    .unwrap()
                    .get("transferCode")
                    .cloned()
            };
            assert_eq!(named(&bodies[0]), Some(json!("DEPOSIT")));
            assert_eq!(named(&bodies[2]), None);
        }
    }
}

/// A negative transfer code is refused before sending by both wrappers.
#[tokio::test]
async fn transfer_lists_refuse_a_negative_transfer_code_before_sending() {
    let server = MockHttpServer::spawn(|_| panic!("nothing is sent")).await;
    let client = client(WireFormat::Binary, &server).await;
    for code in [-1, i32::MIN] {
        let request = ListTransfersRequest {
            transfer_code: code.into(),
            ..Default::default()
        };
        for (name, result) in [
            (
                "transfers.list",
                client.transfers.list(request.clone()).await,
            ),
            (
                "balances.list_transfers",
                client.balances.list_transfers(request.clone()).await,
            ),
        ] {
            match result {
                Err(Error::Validation(message)) => {
                    assert!(message.contains(name), "{message}");
                    assert!(message.contains("transfer_code"), "{message}");
                }
                other => panic!("{name} {code}: {other:?}"),
            }
        }
    }
    assert_eq!(server.requests.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// `GetInternalTransferBatch` refuses an API key whose policy does not grant
/// `READ_INTERNAL_TRANSFERS` with `permission_denied`, naming the action to
/// grant, rather than `not_found`: it is a final `Error::PermissionDenied`
/// carrying the `ERROR_CODE_PERMISSION_DENIED` code and that message.
#[tokio::test]
async fn get_batch_refuses_a_key_without_the_read_grant_as_permission_denied() {
    const MESSAGE: &str = "API key policy does not grant READ_INTERNAL_TRANSFERS";
    let detail = ErrorDetail {
        code: TransferErrorCode::ERROR_CODE_PERMISSION_DENIED.into(),
        ..Default::default()
    };
    let body = serde_json::to_vec(&json!({
        "code": "permission_denied",
        "message": MESSAGE,
        "details": [{
            "type": "transfer.v1.ErrorDetail",
            "value": base64::engine::general_purpose::STANDARD_NO_PAD
                .encode(detail.encode_to_vec()),
        }],
    }))
    .unwrap();
    let paths: Arc<Mutex<Vec<String>>> = Arc::default();
    let log = paths.clone();
    let server = MockHttpServer::spawn(move |request| {
        log.lock().unwrap().push(request.path.clone());
        HttpScript::Json {
            status: 403,
            body: body.clone(),
        }
    })
    .await;
    let client = client(WireFormat::Binary, &server).await;
    let err = client
        .internal_transfers
        .get_batch("12345")
        .await
        .expect_err("refused");
    assert_eq!(
        *paths.lock().unwrap(),
        ["/transfer.v1.InternalTransferService/GetInternalTransferBatch"]
    );
    match &err {
        Error::PermissionDenied {
            code,
            message,
            status,
            endpoint,
            ..
        } => {
            assert_eq!(code, "ERROR_CODE_PERMISSION_DENIED");
            assert_eq!(message, MESSAGE);
            assert_eq!(*status, 403);
            assert_eq!(
                endpoint,
                "/transfer.v1.InternalTransferService/GetInternalTransferBatch"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(!err.is_retryable());
    assert!(!err.mutation_outcome_unknown());
}
