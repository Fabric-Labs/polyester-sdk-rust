//! The typed `SubAccountScope` selector: `Default` takes the configured default
//! sub-account, `Main` is always the main account, and `Id` names a
//! sub-account (0 refused). Helpers that build a request and send it through
//! a raw-request method must not apply the default a second time, or `Main`
//! would reach the default sub-account.

use super::*;
use polyester::models::{
    ListTriggerEventsOpts, ListTriggersOpts, StopAllTradingScope, SubAccountScope,
};

/// Every typed helper with a sub-account parameter, called with `sub`, on a
/// client whose default sub-account is 42.
async fn call_typed_helpers(client: &Client, sub: SubAccountScope) {
    let id = polyester::codecs::format_uint64_id(9);
    let _ = client.orders.cancel_by_order_id(&id, sub).await;
    let _ = client.orders.cancel_all(None, false, sub).await;
    let _ = client
        .triggers
        .list_with(ListTriggersOpts {
            subaccount_id: sub,
            ..Default::default()
        })
        .await;
    let _ = client.triggers.get_by_id(&id, sub).await;
    let _ = client.triggers.cancel_by_id(&id, sub).await;
    let _ = client.triggers.cancel_oco_by_id(&id, sub).await;
    let _ = client.triggers.pause_by_id(&id, sub).await;
    let _ = client
        .triggers
        .resume_by_id(&id, Some("BTC-USDT"), sub)
        .await;
    let _ = client
        .triggers
        .list_events_by_id(
            &id,
            ListTriggerEventsOpts {
                subaccount_id: sub,
                ..Default::default()
            },
        )
        .await;
    let _ = client.deposit.list_addresses(sub).await;
}

const RPCS: [&str; 10] = [
    "CancelOrder",
    "CancelAllOrders",
    "ListTriggers",
    "GetTrigger",
    "CancelTrigger",
    "CancelOco",
    "PauseTrigger",
    "ResumeTrigger",
    "ListTriggerEvents",
    "ListDepositAddresses",
];

#[tokio::test]
async fn typed_helpers_send_the_selected_subaccount() {
    for (default, sub, want) in [
        (Some(42), SubAccountScope::Default, Some(42)),
        (Some(42), SubAccountScope::Main, None),
        (Some(42), SubAccountScope::Id(44), Some(44)),
        (Some(42), 44.into(), Some(44)),
        (None, SubAccountScope::Default, None),
        (None, SubAccountScope::Main, None),
    ] {
        let (_server, client, seen) = scope_capture_client(default).await;
        call_typed_helpers(&client, sub).await;
        let got = std::mem::take(&mut *seen.lock().unwrap());
        let expected: Vec<(String, Option<u64>)> = RPCS.map(|rpc| (rpc.to_owned(), want)).to_vec();
        assert_eq!(got, expected, "default={default:?} sub={sub:?}");
    }
}

#[tokio::test]
async fn stop_all_trading_on_the_main_account_never_reaches_the_default() {
    let (_server, client, seen) = scope_capture_client(Some(42)).await;
    let _ = client
        .stop_all_trading(StopAllTradingScope {
            subaccount_id: SubAccountScope::Main,
            ..Default::default()
        })
        .await;
    let got = std::mem::take(&mut *seen.lock().unwrap());
    assert!(!got.is_empty());
    assert!(
        got.iter().all(|(_, sub)| sub.is_none()),
        "a request reached a sub-account: {got:?}"
    );
}

#[tokio::test]
async fn subaccount_id_zero_is_refused_before_sending() {
    let (_server, client, seen) = scope_capture_client(Some(42)).await;
    call_typed_helpers(&client, SubAccountScope::Id(0)).await;
    assert!(
        seen.lock().unwrap().is_empty(),
        "sub-account 0 reached the server"
    );
    let err = client
        .deposit
        .list_addresses(0.into())
        .await
        .expect_err("0 is refused");
    assert!(matches!(err, Error::Validation(_)), "{err:?}");
}
