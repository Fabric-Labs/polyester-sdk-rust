//! Credentialed confirmation of the public fee and trigger-read contract.
use crate::support::require_live_client;
use polyester::errors::Error;
use polyester::proto::triggers::v1::{GetTriggerRequest, ListTriggerEventsRequest};

#[tokio::test]
async fn contract_fees_and_unknown_trigger() {
    let Some(client) = require_live_client() else {
        return;
    };
    let fees = client
        .fees
        .get_spot_fee_rates(None, vec![])
        .await
        .expect("fee rates");
    assert!(!fees.fee_rates.is_empty(), "fee scenario must occur");
    for rate in &fees.fee_rates {
        assert!(!rate.reserve_fee_rate_percent.is_empty());
        assert!(!rate.post_only_reserve_fee_rate_percent.is_empty());
        assert!(
            rate.breakdown.is_some(),
            "devnet must return the new breakdown"
        );
    }
    println!(
        "reserve fields and breakdown returned for {} markets",
        fees.fee_rates.len()
    );
    let trigger_id = u64::MAX - 101;
    let get = client
        .triggers
        .get(GetTriggerRequest {
            trigger_id,
            ..Default::default()
        })
        .await
        .unwrap_err();
    let events = client
        .triggers
        .list_events(ListTriggerEventsRequest {
            trigger_id,
            ..Default::default()
        })
        .await
        .unwrap_err();
    for error in [get, events] {
        assert!(
            matches!(&error, Error::Api { code, .. } if code == "NotFound"),
            "expected not_found, got {error}"
        );
    }
    println!("GetTrigger and ListTriggerEvents both returned not_found");
}
