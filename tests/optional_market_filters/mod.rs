//! Calls taking an optional market filter refuse a blank market name, the
//! trigger cancel-all sends a supplied request ID as given, and candle calls
//! refuse a timeframe that is not a documented label or enum name: each
//! through the public services against a local server whose catalog load
//! never ends, so a call that waited for the catalogs would time out instead
//! of answering with the validation error. Nothing may reach the server.
//!
//! A blank market name used to be dropped and so read as every market: the
//! dead-man switch armed for every market, and with `include_triggers` its
//! expiry stopped every trigger on every market.

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa::Message as _;
use polyester::models::{
    CancelAllAfterOpts, CancelAllTriggersOpts, CancelOrderParams, GetCandlesOpts,
    GetIndexPriceCandlesOpts, ListAlertsOptions, ListTriggersOpts, ListUserTradesOpts,
    ModifyTriggerParams, OrderKey, ProfitAndLossWindow, ResumeTriggerParams, SubAccountScope,
};
use polyester::proto::orders::v1::{
    CancelAllAfterRequest, CancelAllAfterResponse, cancel_all_after_response,
};
use polyester::proto::triggers::v1::{
    CancelAllTriggersRequest, CancelAllTriggersResponse, cancel_all_triggers_response,
};
use polyester::services::ProfitAndLossOptions;
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const ZIPPER_CONFIG: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";
const CANCEL_ALL_AFTER: &str = "/orders.v1.OrdersService/CancelAllAfter";
const CANCEL_ALL_TRIGGERS: &str = "/triggers.v1.TriggersService/CancelAllTriggers";
const BLANKS: [&str; 3] = ["", "   ", " \t"];

/// A request the server read: its path and body.
type Sent = (String, Vec<u8>);

struct Server {
    client: Client,
    _http: MockHttpServer,
    /// Requests other than the catalog loads.
    sent: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Sent>>>,
}

impl Server {
    fn sent(&self) -> usize {
        self.sent.load(Ordering::SeqCst)
    }

    fn bodies(&self, path: &str) -> Vec<Vec<u8>> {
        self.bodies
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, body)| body.clone())
            .collect()
    }
}

/// A binary-wire client. With `pending` its catalog load never ends; without
/// it the client loads no catalogs.
async fn server(pending: bool) -> Server {
    let sent = Arc::new(AtomicUsize::new(0));
    let counted = sent.clone();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = bodies.clone();
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        SPOT_CONFIG | ZIPPER_CONFIG => HttpScript::NeverRespond {
            stall: Duration::from_secs(60),
        },
        path => {
            counted.fetch_add(1, Ordering::SeqCst);
            seen.lock()
                .unwrap()
                .push((path.to_owned(), req.body.clone()));
            let body = match path {
                CANCEL_ALL_AFTER => CancelAllAfterResponse {
                    status: cancel_all_after_response::Status::ARMED.into(),
                    effective_timeout_sec: 30,
                    ..Default::default()
                }
                .encode_to_vec(),
                CANCEL_ALL_TRIGGERS => CancelAllTriggersResponse {
                    status: cancel_all_triggers_response::Status::SUBMITTED.into(),
                    matched_triggers: 1,
                    ..Default::default()
                }
                .encode_to_vec(),
                _ => return HttpScript::NotFound,
            };
            HttpScript::Raw {
                status: 200,
                headers: vec![("Content-Type".into(), "application/proto".into())],
                body,
            }
        }
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: pending,
        wire_format: WireFormat::Binary,
        ..Default::default()
    })
    .expect("client");
    Server {
        client,
        _http: http,
        sent,
        bodies,
    }
}

type Call = for<'a> fn(&'a Client, String) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

fn optional_market_calls() -> Vec<(&'static str, Call)> {
    vec![
        ("orders.cancel_all_after_with", |c, s| {
            Box::pin(async move {
                c.orders
                    .cancel_all_after_with(CancelAllAfterOpts {
                        timeout_sec: 30,
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("orders.cancel_all_after_with include_triggers", |c, s| {
            Box::pin(async move {
                c.orders
                    .cancel_all_after_with(CancelAllAfterOpts {
                        timeout_sec: 30,
                        symbol: Some(s),
                        include_triggers: true,
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("orders.cancel_with", |c, s| {
            Box::pin(async move {
                c.orders
                    .cancel_with(CancelOrderParams {
                        key: OrderKey::ClientOrderId("c-1".into()),
                        symbol: Some(s),
                        symbol_id: None,
                        subaccount_id: SubAccountScope::Default,
                    })
                    .await
                    .map(drop)
            })
        }),
        ("trades.list_with", |c, s| {
            Box::pin(async move {
                c.trades
                    .list_with(ListUserTradesOpts {
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("pnl.get", |c, s| {
            Box::pin(async move {
                let mut opts = ProfitAndLossOptions::new(ProfitAndLossWindow::Days7);
                opts.symbol = Some(s);
                c.pnl.get(opts).await.map(drop)
            })
        }),
        ("alerts.list", |c, s| {
            Box::pin(async move {
                let mut opts = ListAlertsOptions::default();
                opts.symbol = Some(s);
                c.alerts.list(opts).await.map(drop)
            })
        }),
        ("triggers.list_with", |c, s| {
            Box::pin(async move {
                c.triggers
                    .list_with(ListTriggersOpts {
                        symbol: Some(s),
                        ..Default::default()
                    })
                    .await
                    .map(drop)
            })
        }),
        ("triggers.resume_with", |c, s| {
            Box::pin(async move {
                c.triggers
                    .resume_with(ResumeTriggerParams {
                        trigger_id: "9".into(),
                        subaccount_id: SubAccountScope::Default,
                        symbol: Some(s),
                        symbol_id: None,
                    })
                    .await
                    .map(drop)
            })
        }),
        ("triggers.modify", |c, s| {
            Box::pin(async move {
                c.triggers
                    .modify(ModifyTriggerParams {
                        trigger_id: "9".into(),
                        subaccount_id: SubAccountScope::Default,
                        symbol: Some(s),
                        symbol_id: None,
                        trigger_price: None,
                        activation_price: None,
                        trailing_distance_ticks: None,
                        trailing_distance_bps: None,
                        max_slippage_ticks: None,
                        max_slippage_bps: Some(50),
                    })
                    .await
                    .map(drop)
            })
        }),
    ]
}

#[tokio::test]
async fn optional_market_filters_refuse_a_blank_symbol_before_sending() {
    for (name, call) in optional_market_calls() {
        for blank in BLANKS {
            let s = server(true).await;
            let label = format!("{name} {blank:?}");
            let err = tokio::time::timeout(Duration::from_secs(5), call(&s.client, blank.into()))
                .await
                .unwrap_or_else(|_| panic!("{label}: waited for the catalogs"))
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{label}: {err}");
            assert!(
                err.to_string().contains("must not be blank"),
                "{label}: {err}"
            );
            assert_eq!(s.sent(), 0, "{label}: nothing may be sent");
        }
    }
}

/// Leaving the market out still arms the dead-man switch for every market.
#[tokio::test]
async fn cancel_all_after_without_a_symbol_still_arms_every_market() {
    let s = server(false).await;
    for include_triggers in [false, true] {
        s.client
            .orders
            .cancel_all_after_with(CancelAllAfterOpts {
                timeout_sec: 30,
                include_triggers,
                ..Default::default()
            })
            .await
            .expect("every market");
    }
    let sent: Vec<CancelAllAfterRequest> = s
        .bodies(CANCEL_ALL_AFTER)
        .iter()
        .map(|body| CancelAllAfterRequest::decode_from_slice(body).expect("request"))
        .collect();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|r| r.symbol_id == 0), "every market");
    assert!(!sent[0].include_triggers && sent[1].include_triggers);
}

/// Already correct: a supplied request ID is sent exactly as given, or
/// refused; an empty or padded one is never trimmed or replaced. An omitted
/// one is generated anew for each call.
#[tokio::test]
async fn triggers_cancel_all_sends_a_supplied_request_id_as_given() {
    for request_id in ["", "   ", " abc ", "abc ", "\tabc"] {
        let s = server(false).await;
        let err = s
            .client
            .triggers
            .cancel_all(CancelAllTriggersOpts {
                request_id: Some(request_id.into()),
                ..Default::default()
            })
            .await
            .expect_err("refused");
        assert!(matches!(err, Error::Validation(_)), "{request_id:?}: {err}");
        assert!(
            err.to_string().contains("request_id"),
            "{request_id:?}: {err}"
        );
        assert_eq!(s.sent(), 0, "{request_id:?}: nothing may be sent");
    }

    let s = server(false).await;
    for _ in 0..2 {
        for request_id in [Some("abc".to_owned()), None] {
            s.client
                .triggers
                .cancel_all(CancelAllTriggersOpts {
                    request_id,
                    ..Default::default()
                })
                .await
                .expect("sent");
        }
    }
    let keys: Vec<String> = s
        .bodies(CANCEL_ALL_TRIGGERS)
        .iter()
        .map(|body| {
            CancelAllTriggersRequest::decode_from_slice(body)
                .expect("request")
                .request_id
        })
        .collect();
    assert_eq!(keys.len(), 4);
    assert_eq!((keys[0].as_str(), keys[2].as_str()), ("abc", "abc"));
    assert_ne!(
        keys[1], keys[3],
        "an omitted request ID is generated per call"
    );
    assert!(keys[1].starts_with("cancel-all-triggers-"), "{}", keys[1]);
}

/// A timeframe that is not a documented label or enum name exactly as
/// written is refused before the catalog wait, any request or any
/// subscription.
#[tokio::test]
async fn candle_calls_refuse_an_unknown_timeframe_before_sending() {
    for timeframe in [
        "",
        "1M",
        "buy",
        "sell",
        "min_1",
        "TIMEFRAME_UNSPECIFIED",
        " 1m",
    ] {
        let s = server(true).await;
        let md = &s.client.market_data;
        let opts = || GetCandlesOpts {
            symbol: Some("BTC-USDT".into()),
            timeframe: timeframe.into(),
            ..Default::default()
        };
        let errs = [
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_candles("BTC-USDT", timeframe, None),
            )
            .await
            .map(|r| r.map(drop)),
            tokio::time::timeout(Duration::from_secs(5), md.get_candles_with(opts()))
                .await
                .map(|r| r.map(drop)),
            tokio::time::timeout(Duration::from_secs(5), md.get_candles_columns(opts()))
                .await
                .map(|r| r.map(drop)),
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_current_candle("BTC-USDT", timeframe),
            )
            .await
            .map(|r| r.map(drop)),
            tokio::time::timeout(
                Duration::from_secs(5),
                md.subscribe_candles("BTC-USDT", timeframe),
            )
            .await
            .map(|r| r.map(drop)),
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_index_price_candles("BTC-USDT", timeframe, None),
            )
            .await
            .map(|r| r.map(drop)),
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_index_price_candles_with(GetIndexPriceCandlesOpts {
                    symbol: Some("BTC-USDT".into()),
                    timeframe: timeframe.into(),
                    ..Default::default()
                }),
            )
            .await
            .map(|r| r.map(drop)),
        ];
        for (i, err) in errs.into_iter().enumerate() {
            let err = err
                .unwrap_or_else(|_| panic!("{timeframe:?} call {i}: waited for the catalogs"))
                .expect_err("refused");
            assert!(
                matches!(err, Error::Validation(_)),
                "{timeframe:?} call {i}: {err}"
            );
        }
        assert_eq!(s.sent(), 0, "{timeframe:?}: nothing may be sent");
    }
}

/// One second has no index price candles: the index price candle calls
/// refuse it, in either spelling, before the catalog wait or any request.
#[tokio::test]
async fn index_price_candles_refuse_one_second_before_sending() {
    for timeframe in ["1s", "SEC_1"] {
        let s = server(true).await;
        let md = &s.client.market_data;
        let errs = [
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_index_price_candles("BTC-USDT", timeframe, None),
            )
            .await,
            tokio::time::timeout(
                Duration::from_secs(5),
                md.get_index_price_candles_with(GetIndexPriceCandlesOpts {
                    symbol: Some("BTC-USDT".into()),
                    timeframe: timeframe.into(),
                    ..Default::default()
                }),
            )
            .await,
        ];
        for (i, err) in errs.into_iter().enumerate() {
            let err = err
                .unwrap_or_else(|_| panic!("{timeframe:?} call {i}: waited for the catalogs"))
                .expect_err("refused");
            assert!(
                matches!(&err, Error::Validation(msg) if msg.contains("one second")),
                "{timeframe:?} call {i}: {err}"
            );
        }
        assert_eq!(s.sent(), 0, "{timeframe:?}: nothing may be sent");
    }
}
