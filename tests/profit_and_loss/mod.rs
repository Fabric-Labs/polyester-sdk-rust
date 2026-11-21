//! Profit and loss through the public `pnl` service against a local server,
//! in binary and JSON: what each read sends (account, window, product,
//! market, days, paging), what it returns, and that a refused read sends
//! nothing.

use super::hardening_support::{HttpScript, MockHttpServer};
use buffa_types::google::protobuf::Timestamp;
use futures_util::TryStreamExt;
use polyester::models::{ProfitAndLossProduct, ProfitAndLossWindow, SubAccountScope};
use polyester::proto::pnl::v1::{
    DailyProfitAndLoss, GetProfitAndLossRequest, GetProfitAndLossResponse,
    GetTradingStatisticsRequest, GetTradingStatisticsResponse, HoldingProfitAndLoss,
    ListDailyProfitAndLossRequest, ListDailyProfitAndLossResponse, ProfitAndLossNotice,
    ProfitAndLossProduct as ProtoProduct, ProfitAndLossWindow as ProtoWindow,
};
use polyester::services::{ListDailyProfitAndLossOptions, ProfitAndLossOptions};
use polyester::transport::WireFormat;
use polyester::{Client, Config, Error};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const GET: &str = "/pnl.v1.ProfitAndLossService/GetProfitAndLoss";
const STATISTICS: &str = "/pnl.v1.ProfitAndLossService/GetTradingStatistics";
const DAILY: &str = "/pnl.v1.ProfitAndLossService/ListDailyProfitAndLoss";
const WIRES: [WireFormat; 2] = [WireFormat::Binary, WireFormat::Json];
/// 2026-11-01T00:00:00Z.
const NOV_1: u64 = 1_793_491_200;
const DAY: u64 = 86_400;

fn answer<M: buffa::Message + serde::Serialize>(wire: WireFormat, msg: &M) -> HttpScript {
    let (content_type, body) = match wire {
        WireFormat::Binary => ("application/proto", msg.encode_to_vec()),
        WireFormat::Json => ("application/json", serde_json::to_vec(msg).unwrap()),
    };
    HttpScript::Raw {
        status: 200,
        headers: vec![("Content-Type".into(), content_type.into())],
        body,
    }
}

fn request<M: buffa::Message + serde::de::DeserializeOwned + Default>(
    wire: WireFormat,
    body: &[u8],
) -> M {
    match wire {
        WireFormat::Binary => M::decode_from_slice(body).expect("binary request"),
        WireFormat::Json => serde_json::from_slice(body).expect("JSON request"),
    }
}

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn ts(seconds: u64) -> buffa::MessageField<Timestamp> {
    Timestamp {
        seconds: seconds as i64,
        ..Default::default()
    }
    .into()
}

fn day(seconds: u64) -> DailyProfitAndLoss {
    DailyProfitAndLoss {
        day: ts(seconds),
        realized_pnl_usd: "0".into(),
        fees_usd: "0".into(),
        volume_usd: "0".into(),
        ..Default::default()
    }
}

#[derive(Default)]
struct Sent {
    gets: Vec<GetProfitAndLossRequest>,
    statistics: Vec<GetTradingStatisticsRequest>,
    daily: Vec<ListDailyProfitAndLossRequest>,
}

struct Server {
    client: Client,
    http: MockHttpServer,
    sent: Arc<Mutex<Sent>>,
}

async fn server(wire: WireFormat) -> Server {
    server_with_default(wire, None).await
}

async fn server_with_default(wire: WireFormat, default_sub_account_id: Option<String>) -> Server {
    let sent = Arc::new(Mutex::new(Sent::default()));
    let log = sent.clone();
    let http = MockHttpServer::spawn(move |req| match req.path.as_str() {
        GET => {
            log.lock().unwrap().gets.push(request(wire, &req.body));
            answer(
                wire,
                &GetProfitAndLossResponse {
                    window_start: ts(NOV_1),
                    pnl_usd: None,
                    realized_pnl_usd: "12.5".into(),
                    unrealized_pnl_usd: Some("-0.000000000000000001".into()),
                    fees_usd: "-0.25".into(),
                    volume_usd: "1000".into(),
                    holdings: vec![HoldingProfitAndLoss {
                        asset_id: 2,
                        position_quantity: "1.5".into(),
                        untracked_quantity: "0.5".into(),
                        cost_usd: "90000".into(),
                        entry_price_usd: Some("90000".into()),
                        ..Default::default()
                    }],
                    notices: vec![
                        ProfitAndLossNotice::PROFIT_AND_LOSS_NOTICE_PRICE_UNAVAILABLE.into(),
                    ],
                    ..Default::default()
                },
            )
        }
        STATISTICS => {
            log.lock()
                .unwrap()
                .statistics
                .push(request(wire, &req.body));
            answer(
                wire,
                &GetTradingStatisticsResponse {
                    trade_count: 7,
                    volume_usd: "1500".into(),
                    fees_usd: "1.5".into(),
                    realized_pnl_usd: "3".into(),
                    return_pct: Some("9.95".into()),
                    max_drawdown_pct: Some("0".into()),
                    return_days: 2,
                    notices: vec![
                        ProfitAndLossNotice::PROFIT_AND_LOSS_NOTICE_SHARPE_INSUFFICIENT_DAYS.into(),
                    ],
                    ..Default::default()
                },
            )
        }
        DAILY => {
            let sent: ListDailyProfitAndLossRequest = request(wire, &req.body);
            // Two pages of two days: the first page continues with "p2".
            let page = if sent.page_token.is_empty() {
                ListDailyProfitAndLossResponse {
                    days: vec![day(NOV_1), day(NOV_1 + DAY)],
                    next_page_token: "p2".into(),
                    ..Default::default()
                }
            } else {
                ListDailyProfitAndLossResponse {
                    days: vec![day(NOV_1 + 2 * DAY), day(NOV_1 + 3 * DAY)],
                    as_of: ts(NOV_1 + 4 * DAY),
                    ..Default::default()
                }
            };
            log.lock().unwrap().daily.push(sent);
            answer(wire, &page)
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
        default_sub_account_id,
        ..Default::default()
    })
    .expect("client");
    client
        .catalogs
        .hydrate_spot_config_json(json!({
            "pairs": [{"symbol": "BTC-USDT", "symbol_id": 1, "base_quantity_scale": 5}]
        }))
        .expect("catalog");
    Server { client, http, sent }
}

/// `get` sends the window, product, market and sub-account, and returns the
/// figures exactly, an absent one as `None`.
#[tokio::test]
async fn get_sends_its_selection_and_returns_exact_figures() {
    for wire in WIRES {
        let s = server(wire).await;
        let mut opts = ProfitAndLossOptions::new(ProfitAndLossWindow::Days7);
        opts.subaccount_id = SubAccountScope::Id(42);
        opts.product = Some(ProfitAndLossProduct::Spot);
        opts.symbol = Some("BTC-USDT".into());
        let pnl = s.client.pnl.get(opts).await.expect("get");
        assert_eq!(pnl.window_start.unwrap().seconds, NOV_1 as i64, "{wire:?}");
        assert_eq!(pnl.pnl_usd, None, "{wire:?}");
        assert_eq!(pnl.realized_pnl_usd, "12.5", "{wire:?}");
        assert_eq!(
            pnl.unrealized_pnl_usd.as_deref(),
            Some("-0.000000000000000001"),
            "{wire:?}"
        );
        assert_eq!(pnl.holdings[0].untracked_quantity, "0.5", "{wire:?}");
        assert_eq!(pnl.holdings[0].mark_price_usd, None, "{wire:?}");
        assert_eq!(pnl.notices, vec!["price_unavailable"], "{wire:?}");

        // Every product and market of the default account.
        s.client
            .pnl
            .get(ProfitAndLossOptions::new(ProfitAndLossWindow::Lifetime))
            .await
            .expect("get lifetime");

        let sent = &s.sent.lock().unwrap().gets;
        assert_eq!(sent[0].subaccount_id, Some(42), "{wire:?}");
        assert_eq!(sent[0].window, ProtoWindow::PROFIT_AND_LOSS_WINDOW_DAYS_7);
        assert_eq!(sent[0].product, ProtoProduct::PROFIT_AND_LOSS_PRODUCT_SPOT);
        assert_eq!(sent[0].symbol_id, 1, "{wire:?}");
        assert_eq!(sent[1].subaccount_id, None, "{wire:?}");
        assert_eq!(sent[1].window, ProtoWindow::PROFIT_AND_LOSS_WINDOW_LIFETIME);
        assert_eq!(
            sent[1].product,
            ProtoProduct::PROFIT_AND_LOSS_PRODUCT_UNSPECIFIED
        );
        assert_eq!(sent[1].symbol_id, 0, "{wire:?}");
    }
}

#[tokio::test]
async fn trading_statistics_send_the_window_and_return_the_return_figures() {
    for wire in WIRES {
        let s = server(wire).await;
        let mut opts = ProfitAndLossOptions::new(ProfitAndLossWindow::Days30);
        opts.symbol_id = Some(3);
        let stats = s
            .client
            .pnl
            .get_trading_statistics(opts)
            .await
            .expect("statistics");
        assert_eq!(stats.trade_count, 7, "{wire:?}");
        assert_eq!(stats.return_pct.as_deref(), Some("9.95"), "{wire:?}");
        assert_eq!(stats.max_drawdown_pct.as_deref(), Some("0"), "{wire:?}");
        assert_eq!(stats.sharpe, None, "{wire:?}");
        assert_eq!(stats.return_days, 2, "{wire:?}");
        assert_eq!(stats.notices, vec!["sharpe_insufficient_days"], "{wire:?}");
        let sent = &s.sent.lock().unwrap().statistics;
        assert_eq!(sent[0].window, ProtoWindow::PROFIT_AND_LOSS_WINDOW_DAYS_30);
        assert_eq!(sent[0].symbol_id, 3, "{wire:?}");
    }
}

/// `list_daily` sends the days, page size and token; `daily` follows every
/// page's token with the same days and returns every day oldest first.
#[tokio::test]
async fn daily_pages_follow_the_token_with_the_same_days() {
    for wire in WIRES {
        let s = server(wire).await;
        let opts = ListDailyProfitAndLossOptions {
            subaccount_id: SubAccountScope::Id(9),
            start_time: Some(at(NOV_1)),
            end_time: Some(at(NOV_1 + 30 * DAY)),
            limit: 2,
            page_token: None,
        };
        let first = s.client.pnl.list_daily(opts.clone()).await.expect("page");
        assert_eq!(first.days.len(), 2, "{wire:?}");
        assert_eq!(first.next_page_token, "p2", "{wire:?}");

        let days: Vec<_> = s.client.pnl.daily(opts).try_collect().await.expect("days");
        assert_eq!(
            days.iter()
                .map(|d| d.day.as_ref().unwrap().seconds as u64)
                .collect::<Vec<_>>(),
            vec![NOV_1, NOV_1 + DAY, NOV_1 + 2 * DAY, NOV_1 + 3 * DAY],
            "{wire:?}"
        );

        let sent = &s.sent.lock().unwrap().daily;
        assert_eq!(sent.len(), 3, "{wire:?}");
        for req in sent.iter() {
            assert_eq!(req.subaccount_id, Some(9), "{wire:?}");
            assert_eq!(req.start_time.as_option().unwrap().seconds, NOV_1 as i64);
            assert_eq!(
                req.end_time.as_option().unwrap().seconds,
                (NOV_1 + 30 * DAY) as i64
            );
            assert_eq!(req.limit, 2, "{wire:?}");
        }
        assert_eq!(
            sent.iter()
                .map(|r| r.page_token.as_str())
                .collect::<Vec<_>>(),
            vec!["", "", "p2"],
            "{wire:?}"
        );
    }
}

/// A read the SDK can refuse is refused with `Error::Validation` before
/// anything is sent.
#[tokio::test]
async fn invalid_reads_are_refused_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        let days = |start: Option<u64>, end: Option<u64>| ListDailyProfitAndLossOptions {
            start_time: start.map(at),
            end_time: end.map(at),
            ..Default::default()
        };
        for (opts, want) in [
            (days(None, Some(NOV_1)), "start_time is required"),
            (days(Some(NOV_1), None), "end_time is required"),
            (
                days(Some(NOV_1 + 1), Some(NOV_1 + DAY)),
                "start_time must be 00:00 UTC of a day",
            ),
            (
                days(Some(NOV_1), Some(NOV_1 + DAY + 3_600)),
                "end_time must be 00:00 UTC of a day",
            ),
            (
                ListDailyProfitAndLossOptions {
                    end_time: Some(at(NOV_1 + DAY) + Duration::from_nanos(1)),
                    ..days(Some(NOV_1), None)
                },
                "end_time must be 00:00 UTC of a day",
            ),
            (days(Some(NOV_1), Some(NOV_1)), "after start_time"),
            (days(Some(NOV_1 + DAY), Some(NOV_1)), "after start_time"),
            (
                days(Some(NOV_1), Some(NOV_1 + 367 * DAY)),
                "at most 366 days",
            ),
            (
                ListDailyProfitAndLossOptions {
                    limit: 367,
                    ..days(Some(NOV_1), Some(NOV_1 + DAY))
                },
                "above the maximum 366",
            ),
            (
                ListDailyProfitAndLossOptions {
                    page_token: Some("t".repeat(513)),
                    ..days(Some(NOV_1), Some(NOV_1 + DAY))
                },
                "page_token",
            ),
            (
                ListDailyProfitAndLossOptions {
                    subaccount_id: SubAccountScope::Id(0),
                    ..days(Some(NOV_1), Some(NOV_1 + DAY))
                },
                "subaccount",
            ),
        ] {
            let err = s.client.pnl.list_daily(opts).await.expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            assert!(err.to_string().contains(want), "{wire:?}: {want}: {err}");
        }
        // 9999-12-31T00:00:00Z, the last 00:00 UTC a protobuf timestamp
        // holds, ends a valid range; a bound past the timestamp range is
        // refused, with no overflow.
        const LAST_MIDNIGHT: u64 = 253_402_214_400;
        for (opts, want) in [
            (
                days(Some(LAST_MIDNIGHT + DAY), Some(LAST_MIDNIGHT + 2 * DAY)),
                "start_time is outside the timestamp range",
            ),
            (
                days(Some(LAST_MIDNIGHT), Some(LAST_MIDNIGHT + DAY)),
                "end_time is outside the timestamp range",
            ),
        ] {
            let err = s.client.pnl.list_daily(opts).await.expect_err("refused");
            assert!(err.to_string().contains(want), "{wire:?}: {want}: {err}");
        }
        let before = s.http.requests.load(Ordering::SeqCst);
        s.client
            .pnl
            .list_daily(days(Some(LAST_MIDNIGHT - 366 * DAY), Some(LAST_MIDNIGHT)))
            .await
            .expect("a range ending at the last timestamp day");
        assert_eq!(s.http.requests.load(Ordering::SeqCst), before + 1);
        // The longest range is accepted locally.
        let max = days(Some(NOV_1), Some(NOV_1 + 366 * DAY));
        let before = s.http.requests.load(Ordering::SeqCst);
        s.client.pnl.list_daily(max).await.expect("366 days");
        assert_eq!(s.http.requests.load(Ordering::SeqCst), before + 1);

        let mut both = ProfitAndLossOptions::new(ProfitAndLossWindow::Today);
        both.symbol = Some("BTC-USDT".into());
        both.symbol_id = Some(1);
        let mut zero = ProfitAndLossOptions::new(ProfitAndLossWindow::Today);
        zero.symbol_id = Some(0);
        let mut unknown = ProfitAndLossOptions::new(ProfitAndLossWindow::Today);
        unknown.symbol = Some("DOGE-USDT".into());
        for opts in [both, zero, unknown] {
            let err = s.client.pnl.get(opts.clone()).await.expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            let err = s
                .client
                .pnl
                .get_trading_statistics(opts)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
        }
        assert_eq!(
            s.http.requests.load(Ordering::SeqCst),
            before + 1,
            "{wire:?}: a refused read must send nothing"
        );
    }
}

/// Profit and loss has no main-account-only read: `Main` names no
/// sub-account (the caller's whole account), even with a default sub-account
/// configured; `Default` sends the configured default; `Id` sends that
/// sub-account. All three reads send the same selection.
#[tokio::test]
async fn account_selectors_send_the_documented_subaccount() {
    for wire in WIRES {
        let s = server_with_default(wire, Some(polyester::codecs::format_uint64_id(77))).await;
        let days = |scope| ListDailyProfitAndLossOptions {
            subaccount_id: scope,
            start_time: Some(at(NOV_1)),
            end_time: Some(at(NOV_1 + DAY)),
            ..Default::default()
        };
        for scope in [
            SubAccountScope::Main,
            SubAccountScope::Default,
            SubAccountScope::Id(5),
        ] {
            let mut opts = ProfitAndLossOptions::new(ProfitAndLossWindow::Today);
            opts.subaccount_id = scope;
            s.client.pnl.get(opts.clone()).await.expect("get");
            s.client
                .pnl
                .get_trading_statistics(opts)
                .await
                .expect("statistics");
            s.client.pnl.list_daily(days(scope)).await.expect("daily");
        }
        let sent = s.sent.lock().unwrap();
        let want = [None, Some(77), Some(5)];
        assert_eq!(
            sent.gets
                .iter()
                .map(|r| r.subaccount_id)
                .collect::<Vec<_>>(),
            want,
            "{wire:?}"
        );
        assert_eq!(
            sent.statistics
                .iter()
                .map(|r| r.subaccount_id)
                .collect::<Vec<_>>(),
            want,
            "{wire:?}"
        );
        assert_eq!(
            sent.daily
                .iter()
                .map(|r| r.subaccount_id)
                .collect::<Vec<_>>(),
            want,
            "{wire:?}"
        );
    }
}

/// A blank market name is refused through both market-filtered reads, alone
/// or beside a symbol ID, before anything is sent: it never widens the read
/// to every market.
#[tokio::test]
async fn blank_market_names_are_refused_before_sending() {
    for wire in WIRES {
        let s = server(wire).await;
        for (symbol, symbol_id, want) in [
            ("", None, "symbol must not be blank"),
            ("   ", None, "symbol must not be blank"),
            (" ", Some(1), "symbol must not be blank"),
            ("BTC-USDT", Some(1), "symbol or symbol_id, not both"),
        ] {
            let mut opts = ProfitAndLossOptions::new(ProfitAndLossWindow::Days7);
            opts.symbol = Some(symbol.into());
            opts.symbol_id = symbol_id;
            let err = s.client.pnl.get(opts.clone()).await.expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            assert!(err.to_string().contains(want), "{wire:?}: {want}: {err}");
            let err = s
                .client
                .pnl
                .get_trading_statistics(opts)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::Validation(_)), "{wire:?}: {err}");
            assert!(err.to_string().contains(want), "{wire:?}: {want}: {err}");
        }
        assert_eq!(
            s.http.requests.load(Ordering::SeqCst),
            0,
            "{wire:?}: a refused read must send nothing"
        );
    }
}
