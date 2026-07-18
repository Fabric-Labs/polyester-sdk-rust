//! Realtime protobuf publication decoders (Go `codecs/decode/realtime.go` parity).

use buffa::Message;

use super::{
    account_identity_from_proto, address_book_invalidation_from_proto, api_key_from_proto,
    asset_balance_from_proto, candle_point_from_proto, flow_summary_message_from_proto,
    market_overview_batch_from_proto, market_trade_from_proto, order_from_proto,
    subaccount_from_proto, subaccount_policy_from_proto, transfer_row_from_proto,
    trigger_event_from_proto, trigger_from_proto, user_trade_from_proto,
    zipped_asset_supply_batch_from_proto,
};
use crate::errors::{Error, Result};
use crate::models::{
    AccountIdentity, AddressBookViewInvalidation, ApiData, ApiKeySummary, AssetBalance, Candle,
    LedgerTransfer, LifecycleFlowSummary, MarketOverviewList, MarketTrade, Order,
    OrderBookDeltaUpdate, PriceQtyPair, SubAccount, SubaccountPolicy, Trigger, TriggerEvent,
    UserTrade, ZippedAssetSupplyBatch,
};
use crate::proto::auth::v1::{
    AccountIdentity as ProtoAccountIdentity, AddressBookViewInvalidated, ApiKey as ProtoApiKey,
    Subaccount, SubaccountPolicyView,
};
use crate::proto::chain::lifecycle::v1::{FlowDetailView, FlowSummaryView};
use crate::proto::chain::zipper::v1::ZippedAssetSupplyBatch as ProtoZippedAssetSupplyBatch;
use crate::proto::ledger::read::v1::{AssetBalance as ProtoAssetBalance, TransferRow};
use crate::proto::marketdata::v1::{
    CandlePoint, HeatmapLiveBucket, MarketTrade as ProtoMarketTrade,
};
use crate::proto::marketoverview::v1::MarketOverviewBatch;
use crate::proto::orderbook::v1::OrderBookDelta;
use crate::proto::orders::v1::{Order as ProtoOrder, UserTrade as ProtoUserTrade};
use crate::proto::triggers::v1::{Trigger as ProtoTrigger, TriggerEvent as ProtoTriggerEvent};

fn decode_proto<M: Message + Default>(payload: &[u8]) -> Result<M> {
    M::decode_from_slice(payload).map_err(|e| Error::realtime(format!("proto decode: {e}")))
}

pub fn order_from_bytes(payload: &[u8]) -> Result<Order> {
    let msg = decode_proto::<ProtoOrder>(payload)?;
    Ok(order_from_proto(&msg))
}

pub fn user_trade_from_bytes(payload: &[u8]) -> Result<UserTrade> {
    let msg = decode_proto::<ProtoUserTrade>(payload)?;
    Ok(user_trade_from_proto(&msg))
}

pub fn asset_balance_from_bytes(payload: &[u8]) -> Result<AssetBalance> {
    let msg = decode_proto::<ProtoAssetBalance>(payload)?;
    Ok(asset_balance_from_proto(&msg))
}

pub fn ledger_transfer_from_bytes(payload: &[u8]) -> Result<LedgerTransfer> {
    let msg = decode_proto::<TransferRow>(payload)?;
    Ok(transfer_row_from_proto(&msg))
}

pub fn trigger_from_bytes(payload: &[u8]) -> Result<Trigger> {
    let msg = decode_proto::<ProtoTrigger>(payload)?;
    Ok(trigger_from_proto(&msg))
}

pub fn trigger_event_from_bytes(payload: &[u8]) -> Result<TriggerEvent> {
    let msg = decode_proto::<ProtoTriggerEvent>(payload)?;
    Ok(trigger_event_from_proto(&msg))
}

pub fn market_trade_from_bytes(payload: &[u8]) -> Result<MarketTrade> {
    let msg = decode_proto::<ProtoMarketTrade>(payload)?;
    Ok(market_trade_from_proto(&msg))
}

pub fn orderbook_delta_from_bytes(payload: &[u8]) -> Result<OrderBookDeltaUpdate> {
    let msg = decode_proto::<OrderBookDelta>(payload)?;
    Ok(OrderBookDeltaUpdate {
        symbol_id: msg.symbol_id,
        book_seq_start: msg.book_seq_start.to_string(),
        book_seq_end: msg.book_seq_end.to_string(),
        reset: msg.reset,
        bids: msg
            .bids
            .iter()
            .map(|l| PriceQtyPair {
                price_ticks: l.price_ticks,
                qty_scaled: l.qty_scaled,
            })
            .collect(),
        asks: msg
            .asks
            .iter()
            .map(|l| PriceQtyPair {
                price_ticks: l.price_ticks,
                qty_scaled: l.qty_scaled,
            })
            .collect(),
    })
}

pub fn flow_summary_from_bytes(payload: &[u8]) -> Result<LifecycleFlowSummary> {
    let msg = decode_proto::<FlowSummaryView>(payload)?;
    Ok(flow_summary_message_from_proto(&msg))
}

pub fn flow_detail_from_bytes(payload: &[u8]) -> Result<LifecycleFlowSummary> {
    let msg = decode_proto::<FlowDetailView>(payload)?;
    Ok(msg
        .summary
        .as_option()
        .map(flow_summary_message_from_proto)
        .unwrap_or_default())
}

pub fn account_identity_from_bytes(payload: &[u8]) -> Result<AccountIdentity> {
    let msg = decode_proto::<ProtoAccountIdentity>(payload)?;
    Ok(account_identity_from_proto(&msg))
}

pub fn candle_point_from_bytes(
    symbol_id: u32,
    timeframe: String,
    volume_scale: u32,
) -> impl Fn(&[u8]) -> Result<Candle> + Send + Sync + 'static {
    move |payload: &[u8]| {
        let point = decode_proto::<CandlePoint>(payload)?;
        Ok(candle_point_from_proto(
            &point,
            volume_scale,
            symbol_id,
            &timeframe,
        ))
    }
}

pub fn heatmap_live_bucket_from_bytes(payload: &[u8]) -> Result<ApiData> {
    let msg = decode_proto::<HeatmapLiveBucket>(payload)?;
    Ok(super::api_data_from_proto(&msg))
}

pub fn market_overview_batch_from_bytes(payload: &[u8]) -> Result<MarketOverviewList> {
    let msg = decode_proto::<MarketOverviewBatch>(payload)?;
    Ok(market_overview_batch_from_proto(&msg))
}

pub fn zipped_asset_supply_batch_from_bytes(
    payload: &[u8],
    scale_fn: impl Fn(u32) -> u32,
) -> Result<ZippedAssetSupplyBatch> {
    let msg = decode_proto::<ProtoZippedAssetSupplyBatch>(payload)?;
    Ok(zipped_asset_supply_batch_from_proto(&msg, scale_fn))
}

pub fn api_key_from_bytes(payload: &[u8]) -> Result<ApiKeySummary> {
    let msg = decode_proto::<ProtoApiKey>(payload)?;
    Ok(api_key_from_proto(&msg))
}

pub fn subaccount_from_bytes(payload: &[u8]) -> Result<SubAccount> {
    let msg = decode_proto::<Subaccount>(payload)?;
    Ok(subaccount_from_proto(&msg))
}

pub fn subaccount_policy_from_bytes(payload: &[u8]) -> Result<SubaccountPolicy> {
    let msg = decode_proto::<SubaccountPolicyView>(payload)?;
    Ok(subaccount_policy_from_proto(&msg))
}

pub fn address_book_invalidation_from_bytes(payload: &[u8]) -> Result<AddressBookViewInvalidation> {
    let msg = decode_proto::<AddressBookViewInvalidated>(payload)?;
    Ok(address_book_invalidation_from_proto(&msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::scalars::format_uint64_id;
    use crate::proto::orders::v1::{OrderStatus, OrderType, Side, TimeInForce};

    #[test]
    fn order_from_bytes_round_trip() {
        let msg = ProtoOrder {
            order_id: 42,
            symbol_id: 3,
            client_order_id: "coid".into(),
            side: Side::Buy.into(),
            status: OrderStatus::Working.into(),
            order_type: OrderType::Limit.into(),
            time_in_force: TimeInForce::Gtc.into(),
            ..Default::default()
        };
        let bytes = msg.encode_to_vec();
        let order = order_from_bytes(&bytes).expect("decode");
        assert_eq!(order.order_id, format_uint64_id(42));
        assert_eq!(order.side, "buy");
        assert_eq!(order.status, "working");
    }

    #[test]
    fn market_trade_from_bytes_maps_side() {
        let msg = ProtoMarketTrade {
            symbol_id: 1,
            match_id: 99,
            is_buy: true,
            price_ticks: 100,
            qty_scaled: 5,
            ts_ns: 123,
            ..Default::default()
        };
        let bytes = msg.encode_to_vec();
        let trade = market_trade_from_bytes(&bytes).expect("decode");
        assert_eq!(trade.match_id, "99");
        assert_eq!(trade.side, "buy");
    }
}
