//! `market_overview.list` sends the contract's sort key, direction and
//! sparkline intervals (`ListMarketOverviewRequest` fields 4, 5 and 7). Unset,
//! nothing is sent and the server keeps its defaults. A page token is bound
//! to the sort it was issued for, so the next page is asked with the same one.

use super::hardening_support::{MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::{
    MarketOverviewOrderBy, MarketOverviewSort, MarketOverviewSparklineInterval,
};
use polyester::proto::marketoverview::v1::{
    ListMarketOverviewRequest, ListMarketOverviewResponse, MarketOrderBy, SortDirection,
    SparklineInterval,
};
use polyester::services::ListMarketOverviewOptions;
use polyester::{Client, Config};
use std::sync::{Arc, Mutex};

type Sent = Arc<Mutex<Vec<ListMarketOverviewRequest>>>;

async fn server() -> (Client, Sent) {
    let sent: Sent = Arc::default();
    let log = sent.clone();
    let http = MockHttpServer::spawn(move |request| {
        log.lock()
            .unwrap()
            .push(ListMarketOverviewRequest::decode_from_slice(&request.body).unwrap());
        connect_proto_ok(&ListMarketOverviewResponse {
            next_page_token: "page-2".into(),
            ..Default::default()
        })
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();
    // The server lives as long as the test: leak it with the client.
    std::mem::forget(http);
    (client, sent)
}

#[tokio::test]
async fn order_by_sort_and_sparkline_intervals_are_sent_as_fields_4_5_and_7() {
    let cases = [
        (
            MarketOverviewOrderBy::Change24hBps,
            MarketOrderBy::ORDER_BY_CHANGE_24H_BPS,
        ),
        (
            MarketOverviewOrderBy::Volume24hUsd,
            MarketOrderBy::ORDER_BY_VOLUME_24H_USD,
        ),
        (
            MarketOverviewOrderBy::LastPrice,
            MarketOrderBy::ORDER_BY_LAST_PRICE,
        ),
        (
            MarketOverviewOrderBy::DateAdded,
            MarketOrderBy::ORDER_BY_DATE_ADDED,
        ),
    ];
    for (order_by, want) in cases {
        for (sort, want_sort) in [
            (MarketOverviewSort::Asc, SortDirection::SORT_ASC),
            (MarketOverviewSort::Desc, SortDirection::SORT_DESC),
        ] {
            let (client, sent) = server().await;
            client
                .market_overview
                .list(ListMarketOverviewOptions {
                    order_by: Some(order_by),
                    sort: Some(sort),
                    ..Default::default()
                })
                .await
                .unwrap();
            let request = sent.lock().unwrap()[0].clone();
            assert_eq!(request.order_by.as_known(), Some(want), "{order_by:?}");
            assert_eq!(request.sort.as_known(), Some(want_sort), "{sort:?}");
        }
    }
    let (client, sent) = server().await;
    client
        .market_overview
        .list(ListMarketOverviewOptions {
            include_sparklines: true,
            sparkline_intervals: vec![
                MarketOverviewSparklineInterval::OneHour,
                MarketOverviewSparklineInterval::TwentyFourHours,
                MarketOverviewSparklineInterval::OneWeek,
                MarketOverviewSparklineInterval::OneMonth,
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    let request = sent.lock().unwrap()[0].clone();
    let intervals: Vec<_> = request
        .sparkline_intervals
        .iter()
        .map(|interval| interval.as_known())
        .collect();
    assert_eq!(
        intervals,
        vec![
            Some(SparklineInterval::SPARKLINE_1H),
            Some(SparklineInterval::SPARKLINE_24H),
            Some(SparklineInterval::SPARKLINE_1W),
            Some(SparklineInterval::SPARKLINE_1M),
        ]
    );
}

#[tokio::test]
async fn unset_options_send_nothing_and_pages_keep_their_sort() {
    let (client, sent) = server().await;
    client
        .market_overview
        .list(ListMarketOverviewOptions::default())
        .await
        .unwrap();
    assert_eq!(
        sent.lock().unwrap()[0],
        ListMarketOverviewRequest::default()
    );

    let mut opts = ListMarketOverviewOptions {
        limit: Some(10),
        order_by: Some(MarketOverviewOrderBy::DateAdded),
        sort: Some(MarketOverviewSort::Asc),
        ..Default::default()
    };
    let first = client.market_overview.list(opts.clone()).await.unwrap();
    opts.page_token = Some(first.next_page_token);
    client.market_overview.list(opts).await.unwrap();
    let second = sent.lock().unwrap()[2].clone();
    assert_eq!(second.page_token, "page-2");
    assert_eq!(
        second.order_by.as_known(),
        Some(MarketOrderBy::ORDER_BY_DATE_ADDED)
    );
    assert_eq!(second.sort.as_known(), Some(SortDirection::SORT_ASC));
}

#[test]
fn labels_match_the_contract_and_the_sparkline_reads() {
    assert_eq!(
        MarketOverviewOrderBy::Change24hBps.as_str(),
        "change_24h_bps"
    );
    assert_eq!(
        MarketOverviewOrderBy::Volume24hUsd.as_str(),
        "volume_24h_usd"
    );
    assert_eq!(MarketOverviewOrderBy::LastPrice.as_str(), "last_price");
    assert_eq!(MarketOverviewOrderBy::DateAdded.as_str(), "date_added");
    assert_eq!(MarketOverviewSort::Asc.as_str(), "asc");
    assert_eq!(MarketOverviewSort::Desc.as_str(), "desc");
    // As `MarketOverviewSparkline::interval` reads them.
    assert_eq!(
        MarketOverviewSparklineInterval::OneHour.as_str(),
        "SPARKLINE_1H"
    );
    assert_eq!(
        MarketOverviewSparklineInterval::TwentyFourHours.as_str(),
        "SPARKLINE_24H"
    );
    assert_eq!(
        MarketOverviewSparklineInterval::OneWeek.as_str(),
        "SPARKLINE_1W"
    );
    assert_eq!(
        MarketOverviewSparklineInterval::OneMonth.as_str(),
        "SPARKLINE_1M"
    );
}
