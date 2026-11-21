//! `orders.cancel_all_with` refuses a market filter that would widen the
//! cancel to every market, through the public `orders` service against a local
//! server, in binary and JSON. A blank `symbol`, or a blank entry in
//! `symbols`, used to be dropped and so read as every market; like
//! `triggers.cancel_all`, each is refused and nothing is sent. Leaving the
//! filters unset is still the intentional cancel of every market.

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa::Message as _;
use polyester::models::{CancelAllOpts, StopAllTradingScope, SubAccountScope};
use polyester::proto::orders::v1::{
    CancelAllOrdersRequest, CancelAllOrdersResponse, cancel_all_orders_response,
};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const CANCEL_ALL: &str = "/orders.v1.OrdersService/CancelAllOrders";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];

struct Server {
    client: Client,
    http: MockHttpServer,
    cancel_alls: Arc<Mutex<Vec<CancelAllOrdersRequest>>>,
}

impl Server {
    fn sent(&self) -> usize {
        self.http.requests.load(Ordering::SeqCst)
    }
}

async fn server(wire: WireFormat) -> Server {
    let cancel_alls = Arc::new(Mutex::new(Vec::new()));
    let seen = cancel_alls.clone();
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        CANCEL_ALL => {
            let sent: CancelAllOrdersRequest = match wire {
                WireFormat::Binary => {
                    CancelAllOrdersRequest::decode_from_slice(&req.body).expect("binary request")
                }
                WireFormat::Json => serde_json::from_slice(&req.body).expect("JSON request"),
            };
            seen.lock().unwrap().push(sent);
            let reply = CancelAllOrdersResponse {
                status: cancel_all_orders_response::Status::DRY_RUN.into(),
                ..Default::default()
            };
            let (content_type, body) = match wire {
                WireFormat::Binary => ("application/proto", reply.encode_to_vec()),
                WireFormat::Json => ("application/json", serde_json::to_vec(&reply).unwrap()),
            };
            HttpScript::Raw {
                status: 200,
                headers: vec![("Content-Type".into(), content_type.into())],
                body,
            }
        }
        _ => HttpScript::NotFound,
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        wire_format: wire,
        ..Default::default()
    })
    .expect("client");
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{
                "symbol": "BTC-USDT",
                "symbol_id": 1,
                "base_quantity_scale": 5,
                "quote_quantity_scale": 6
            }]
        }))
        .expect("catalog");
    Server {
        client,
        http,
        cancel_alls,
    }
}

/// Each refused cancel-all, with a fragment of the error it must raise.
fn refused_cases() -> Vec<(CancelAllOpts, &'static str)> {
    let opts = |f: fn(&mut CancelAllOpts)| {
        let mut opts = CancelAllOpts {
            dry_run: true,
            ..Default::default()
        };
        f(&mut opts);
        opts
    };
    vec![
        (
            opts(|o| o.symbol = Some(String::new())),
            "symbol must not be blank",
        ),
        (
            opts(|o| o.symbol = Some(" \t".into())),
            "symbol must not be blank",
        ),
        (
            opts(|o| o.symbols = vec!["".into()]),
            "symbols[0] must not be blank",
        ),
        (
            opts(|o| o.symbols = vec!["BTC-USDT".into(), "  ".into()]),
            "symbols[1] must not be blank",
        ),
        (
            opts(|o| {
                o.symbol = Some(" ".into());
                o.symbol_ids = vec![1];
            }),
            "symbol must not be blank",
        ),
        (
            opts(|o| {
                o.symbol = Some("BTC-USDT".into());
                o.symbol_id = Some(1);
            }),
            "only one of symbol, symbols, or symbol_ids",
        ),
        (
            opts(|o| {
                o.symbols = vec!["BTC-USDT".into()];
                o.symbol_ids = vec![1];
            }),
            "only one of symbol, symbols, or symbol_ids",
        ),
        (opts(|o| o.request_id = Some(String::new())), "request_id"),
        (opts(|o| o.request_id = Some("   ".into())), "request_id"),
        (
            opts(|o| o.request_id = Some(" retry-1".into())),
            "request_id",
        ),
        (
            opts(|o| o.request_id = Some("retry-1 ".into())),
            "request_id",
        ),
        (
            opts(|o| o.request_id = Some("not valid!".into())),
            "request_id",
        ),
    ]
}

/// A blank or malformed filter, or a request ID the SDK would have to trim or
/// replace, is refused before anything is sent.
#[tokio::test]
async fn cancel_all_refuses_blank_filters_before_sending() {
    for wire in WIRES {
        for (opts, want) in refused_cases() {
            let s = server(wire).await;
            let label = format!("{wire:?} {opts:?}");
            let err = s
                .client
                .orders
                .cancel_all_with(opts)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{label}: {err}");
            assert!(err.to_string().contains(want), "{label}: {err}");
            assert_eq!(s.sent(), 0, "{label}: a refused cancel must send nothing");
        }

        // The positional form refuses a blank symbol the same way.
        let s = server(wire).await;
        let err = s
            .client
            .orders
            .cancel_all(Some(" "), true, SubAccountScope::Default)
            .await
            .expect_err("refused");
        assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
        assert_eq!(s.sent(), 0, "{wire:?}: a refused cancel must send nothing");
    }
}

/// Leaving every market filter unset still cancels on every market, and a
/// request ID the caller supplies is sent exactly as given.
#[tokio::test]
async fn cancel_all_without_filters_still_sends() {
    for wire in WIRES {
        let s = server(wire).await;
        s.client
            .orders
            .cancel_all(None, true, SubAccountScope::Default)
            .await
            .expect("no filter");
        s.client
            .orders
            .cancel_all_with(CancelAllOpts {
                dry_run: true,
                request_id: Some("ui/cancel-all:7f3a".into()),
                ..Default::default()
            })
            .await
            .expect("no filter, own request ID");
        let sent = s.cancel_alls.lock().unwrap().clone();
        assert_eq!(sent.len(), 2, "{wire:?}");
        assert!(
            sent.iter().all(|r| r.symbol_ids.is_empty()),
            "{wire:?}: every market"
        );
        assert!(
            sent[0].request_id.starts_with("cancel-all-"),
            "{wire:?}: {}",
            sent[0].request_id
        );
        assert_eq!(sent[1].request_id, "ui/cancel-all:7f3a", "{wire:?}");
    }
}

/// `stop_all_trading` refuses a blank symbol, or a request ID its cancel-all
/// would refuse, before it lists or cancels any trigger.
#[tokio::test]
async fn stop_all_trading_refuses_blank_filters_before_sending() {
    for scope in [
        StopAllTradingScope {
            symbol: Some(" ".into()),
            ..Default::default()
        },
        StopAllTradingScope {
            request_id: Some("stop-1 ".into()),
            ..Default::default()
        },
    ] {
        let s = server(WireFormat::Binary).await;
        let label = format!("{scope:?}");
        let err = s.client.stop_all_trading(scope).await.expect_err("refused");
        assert!(matches!(err, Error::Validation(_)), "{label}: {err}");
        assert_eq!(s.sent(), 0, "{label}: nothing may be sent");
    }
}
