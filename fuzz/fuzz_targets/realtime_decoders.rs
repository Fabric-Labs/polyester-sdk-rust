#![no_main]

use libfuzzer_sys::fuzz_target;
use polyester::codecs::decode::{
    account_identity_from_bytes, address_book_invalidation_from_bytes, api_key_from_bytes,
    api_policy_from_bytes, asset_balance_from_bytes, candle_point_from_bytes,
    flow_detail_from_bytes, flow_summary_from_bytes, heatmap_live_bucket_from_bytes,
    ledger_transfer_from_bytes, market_overview_batch_from_bytes, market_trade_from_bytes,
    order_from_bytes, orderbook_delta_from_bytes, subaccount_from_bytes,
    subaccount_policy_from_bytes, trigger_event_from_bytes, trigger_from_bytes,
    user_trade_from_bytes, zipped_asset_supply_batch_from_bytes,
};

fuzz_target!(|data: &[u8]| {
    let _ = order_from_bytes(data);
    let _ = user_trade_from_bytes(data);
    let _ = asset_balance_from_bytes(data);
    let _ = ledger_transfer_from_bytes(data);
    let _ = trigger_from_bytes(data);
    let _ = trigger_event_from_bytes(data);
    let _ = market_trade_from_bytes(8)(data);
    let _ = orderbook_delta_from_bytes(data);
    let _ = flow_summary_from_bytes(data);
    let _ = flow_detail_from_bytes(data);
    let _ = account_identity_from_bytes(data);
    let _ = candle_point_from_bytes(1, "1m".to_owned(), 8)(data);
    let _ = heatmap_live_bucket_from_bytes(data);
    let _ = market_overview_batch_from_bytes(data);
    let _ = zipped_asset_supply_batch_from_bytes(data, |_| Some(18));
    let _ = api_key_from_bytes(data);
    let _ = subaccount_from_bytes(data);
    let _ = subaccount_policy_from_bytes(data);
    let _ = api_policy_from_bytes(data);
    let _ = address_book_invalidation_from_bytes(data);
});
