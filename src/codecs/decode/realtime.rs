//! Realtime protobuf publication decoders (Go `codecs/decode/realtime.go` parity).

use buffa::{Enumeration, Message};

use super::{
    account_identity_from_proto, address_book_invalidation_from_proto, api_key_from_proto,
    api_policy_from_proto, asset_balance_from_proto, candle_point_from_proto,
    flow_summary_message_from_proto, market_overview_batch_from_proto, market_trade_from_proto,
    order_from_proto, subaccount_from_proto, subaccount_policy_from_proto, transfer_row_from_proto,
    trigger_event_from_proto, trigger_from_proto, user_trade_from_proto,
    zipped_asset_supply_batch_from_proto,
};
use super::{decode_price_ticks, decode_qty_scaled};
use crate::codecs::scalars::format_uint64_id;
use crate::errors::{Error, Result};
use crate::models::{
    AccountIdentity, AddressBookViewInvalidation, ApiData, ApiKeySummary, ApiPolicy, AssetBalance,
    Candle, LedgerTransfer, LifecycleFlowSummary, MarketOverviewList, MarketTrade,
    OrderBookDeltaUpdate, PriceQtyPair, SubAccount, SubaccountPolicy, Trigger, TriggerEvent,
    UserTrade, ZippedAssetSupplyBatch,
};
use crate::models::{
    OrderCommandOutcome, OrderFill, OrderStreamEvent, OrderUnitEvent, OrdersStale, OrdersUnit,
};
use crate::proto::auth::v1::{
    AccountIdentity as ProtoAccountIdentity, AddressBookViewInvalidated, ApiKey as ProtoApiKey,
    ApiPolicyView, Subaccount, SubaccountPolicyView,
};
use crate::proto::chain::lifecycle::v1::{FlowDetailView, FlowSummaryView};
use crate::proto::chain::zipper::v1::ZippedAssetSupplyBatch as ProtoZippedAssetSupplyBatch;
use crate::proto::ledger::read::v1::{AssetBalance as ProtoAssetBalance, TransferRow};
use crate::proto::marketdata::v1::{
    CandlePoint, HeatmapLiveBucket, MarketTrade as ProtoMarketTrade,
};
use crate::proto::marketoverview::v1::MarketOverviewBatch;
use crate::proto::orderbook::v1::OrderBookDelta;
use crate::proto::orders::v1::{ErrorCode as OrderErrorCode, UserTrade as ProtoUserTrade};
use crate::proto::privatews::v1::{
    Unavailable as ProtoUnavailable, UnavailableReason, Unit as ProtoUnit, unit_event,
};
use crate::proto::triggers::v1::{Trigger as ProtoTrigger, TriggerEvent as ProtoTriggerEvent};

fn decode_proto<M: Message + Default>(payload: &[u8]) -> Result<M> {
    if payload.is_empty() {
        return Err(Error::realtime(
            "proto decode: empty publication payload".to_owned(),
        ));
    }
    if payload.len() > crate::realtime::MAX_REALTIME_MESSAGE_BYTES {
        return Err(Error::realtime(format!(
            "proto decode: publication exceeds {} bytes",
            crate::realtime::MAX_REALTIME_MESSAGE_BYTES
        )));
    }
    M::decode_from_slice(payload).map_err(|e| Error::realtime(format!("proto decode: {e}")))
}

pub fn user_trade_from_bytes(payload: &[u8]) -> Result<UserTrade> {
    let msg = decode_proto::<ProtoUserTrade>(payload)?;
    user_trade_from_proto(&msg)
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
    trigger_event_from_proto(&msg)
}

pub fn market_trade_from_bytes(
    quantity_scale: u32,
) -> impl Fn(&[u8]) -> Result<MarketTrade> + Send + Sync + 'static {
    move |payload: &[u8]| {
        let msg = decode_proto::<ProtoMarketTrade>(payload)?;
        market_trade_from_proto(&msg, quantity_scale)
    }
}

pub fn orderbook_delta_from_bytes(payload: &[u8]) -> Result<OrderBookDeltaUpdate> {
    let msg = decode_proto::<OrderBookDelta>(payload)?;
    Ok(OrderBookDeltaUpdate {
        symbol_id: msg.symbol_id,
        book_seq_start: msg.book_seq_start,
        book_seq_end: msg.book_seq_end,
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
    msg.summary
        .as_option()
        .map(flow_summary_message_from_proto)
        .ok_or_else(|| Error::realtime("proto decode: flow detail is missing summary".to_owned()))
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
        candle_point_from_proto(&point, volume_scale, symbol_id, &timeframe)
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
    scale_fn: impl Fn(u32) -> Option<u32>,
) -> Result<ZippedAssetSupplyBatch> {
    let msg = decode_proto::<ProtoZippedAssetSupplyBatch>(payload)?;
    zipped_asset_supply_batch_from_proto(&msg, scale_fn)
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

pub fn api_policy_from_bytes(payload: &[u8]) -> Result<ApiPolicy> {
    let msg = decode_proto::<ApiPolicyView>(payload)?;
    Ok(api_policy_from_proto(&msg))
}

pub fn address_book_invalidation_from_bytes(payload: &[u8]) -> Result<AddressBookViewInvalidation> {
    let msg = decode_proto::<AddressBookViewInvalidated>(payload)?;
    Ok(address_book_invalidation_from_proto(&msg))
}

/// Decode one private order stream unit with the same order decoder the
/// order reads use.
pub fn orders_unit_from_proto(msg: &ProtoUnit) -> Result<OrdersUnit> {
    Ok(OrdersUnit {
        orders: msg
            .orders
            .iter()
            .map(order_from_proto)
            .collect::<Result<Vec<_>>>()?,
        removed_order_ids: msg
            .removed_order_ids
            .iter()
            .map(|id| format_uint64_id(*id))
            .collect(),
        events: msg
            .events
            .iter()
            .filter_map(|event| event.kind.as_ref())
            .map(order_unit_event_from_proto)
            .collect(),
    })
}

/// Decode a private order stream `Unavailable` frame.
pub fn orders_stale_from_proto(msg: &ProtoUnavailable) -> OrdersStale {
    OrdersStale {
        stale: true,
        reason: match msg.reason.as_known() {
            Some(UnavailableReason::UNAVAILABLE_REASON_UNSPECIFIED) => String::new(),
            _ => lowercase_enum_label(msg.reason),
        },
        order_ids: msg
            .order_ids
            .iter()
            .map(|id| format_uint64_id(*id))
            .collect(),
    }
}

fn order_unit_event_from_proto(kind: &unit_event::Kind) -> OrderUnitEvent {
    match kind {
        unit_event::Kind::Order(event) => OrderUnitEvent::Order(OrderStreamEvent {
            order_id: format_uint64_id(event.order_id),
            version: event.version,
            kind: lowercase_enum_label(event.kind),
            reason_code: event.reason_code,
            replacement_order_id: optional_public_id(event.replacement_order_id),
            fill: event.fill.as_option().map(|fill| OrderFill {
                match_id: if fill.match_id == 0 {
                    String::new()
                } else {
                    fill.match_id.to_string()
                },
                ordinal: fill.ordinal,
                role: lowercase_enum_label(fill.role),
                qty: decode_qty_scaled(fill.qty_scaled, None, None, None),
                price: decode_price_ticks(fill.price_ticks, None),
            }),
        }),
        unit_event::Kind::Outcome(outcome) => OrderUnitEvent::Outcome(OrderCommandOutcome {
            kind: lowercase_enum_label(outcome.kind),
            order_id: optional_public_id(outcome.order_id),
            new_order_id: optional_public_id(outcome.new_order_id),
            client_order_id: outcome.client_order_id.clone(),
            reason: match outcome.reason.as_known() {
                Some(OrderErrorCode::ERROR_CODE_UNSPECIFIED) => String::new(),
                Some(code) => code.proto_name().to_owned(),
                None => format!("UNKNOWN({})", outcome.reason.to_i32()),
            },
        }),
    }
}

fn optional_public_id(id: u64) -> Option<String> {
    (id != 0).then(|| format_uint64_id(id))
}

fn lowercase_enum_label<E: Enumeration>(value: buffa::EnumValue<E>) -> String {
    value
        .as_known()
        .map(|known| known.proto_name().to_ascii_lowercase())
        .unwrap_or_else(|| format!("UNKNOWN({})", value.to_i32()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::orders::v1::Order as ProtoOrder;
    use crate::proto::orders::v1::{OrderStatus, OrderType, Side, TimeInForce};

    #[test]
    fn orders_stale_keeps_reason_and_order_ids() {
        let cases = [
            (
                UnavailableReason::UNAVAILABLE_REASON_UNSPECIFIED.into(),
                vec![],
                "",
                vec![],
            ),
            (
                UnavailableReason::VIEW_BEHIND.into(),
                vec![],
                "view_behind",
                vec![],
            ),
            (
                UnavailableReason::ORDER_UNAVAILABLE.into(),
                vec![5, 9],
                "order_unavailable",
                vec![format_uint64_id(5), format_uint64_id(9)],
            ),
            (buffa::EnumValue::from(77), vec![], "UNKNOWN(77)", vec![]),
        ];
        for (reason, ids, want_reason, want_ids) in cases {
            let stale = orders_stale_from_proto(&ProtoUnavailable {
                reason,
                order_ids: ids,
                ..Default::default()
            });
            assert_eq!(
                stale,
                OrdersStale {
                    stale: true,
                    reason: want_reason.into(),
                    order_ids: want_ids,
                }
            );
        }
    }

    #[test]
    fn orders_unit_decodes_orders_removals_and_events() {
        use crate::proto::orders::v1::ErrorCode;
        use crate::proto::privatews::v1::{
            CommandOutcome, Fill, OrderEvent, UnitEvent, command_outcome, fill, order_event,
        };

        let unit = ProtoUnit {
            next_cursor: "c2".into(),
            orders: vec![ProtoOrder {
                order_id: 42,
                symbol_id: 3,
                client_order_id: "coid".into(),
                side: Side::Buy.into(),
                status: OrderStatus::Working.into(),
                order_type: OrderType::Limit.into(),
                time_in_force: TimeInForce::Gtc.into(),
                price_ticks: 2_420_000_000_000,
                version: 2,
                ..Default::default()
            }],
            removed_order_ids: vec![7],
            events: vec![
                UnitEvent {
                    kind: Some(
                        OrderEvent {
                            order_id: 42,
                            version: 2,
                            kind: order_event::Kind::FILLED.into(),
                            fill: buffa::MessageField::some(Fill {
                                match_id: 900,
                                ordinal: 1,
                                role: fill::Role::MAKER.into(),
                                qty_scaled: 231_500,
                                price_ticks: 2_420_000_000_000,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }
                        .into(),
                    ),
                    ..Default::default()
                },
                UnitEvent {
                    kind: Some(
                        CommandOutcome {
                            kind: command_outcome::Kind::CANCEL_REJECTED.into(),
                            order_id: 8,
                            client_order_id: "late".into(),
                            reason: ErrorCode::ERROR_CODE_ORDER_UNKNOWN.into(),
                            ..Default::default()
                        }
                        .into(),
                    ),
                    ..Default::default()
                },
                // A future event kind is skipped.
                UnitEvent::default(),
            ],
            ..Default::default()
        };
        let decoded = orders_unit_from_proto(&unit).expect("decode");
        assert_eq!(decoded.orders.len(), 1);
        let order = &decoded.orders[0];
        assert_eq!(order, &order_from_proto(&unit.orders[0]).unwrap());
        assert_eq!(order.order_id, format_uint64_id(42));
        assert_eq!(order.side, "buy");
        assert_eq!(order.status, "working");
        assert_eq!(order.price.as_ref().unwrap().format(), "2420");
        assert_eq!(decoded.removed_order_ids, vec![format_uint64_id(7)]);
        assert_eq!(
            decoded.events,
            vec![
                OrderUnitEvent::Order(OrderStreamEvent {
                    order_id: format_uint64_id(42),
                    version: 2,
                    kind: "filled".into(),
                    reason_code: 0,
                    replacement_order_id: None,
                    fill: Some(OrderFill {
                        match_id: "900".into(),
                        ordinal: 1,
                        role: "maker".into(),
                        qty: decode_qty_scaled(231_500, None, None, None),
                        price: decode_price_ticks(2_420_000_000_000, None),
                    }),
                }),
                OrderUnitEvent::Outcome(OrderCommandOutcome {
                    kind: "cancel_rejected".into(),
                    order_id: Some(format_uint64_id(8)),
                    new_order_id: None,
                    client_order_id: "late".into(),
                    reason: "ERROR_CODE_ORDER_UNKNOWN".into(),
                }),
            ]
        );
    }

    #[test]
    fn api_policy_from_bytes_round_trip() {
        use crate::proto::auth::v1::ApiPolicyView;

        let msg = ApiPolicyView {
            id: 9,
            name: "bots".into(),
            description: "api key policy".into(),
            revision: 4,
            ..Default::default()
        };
        let bytes = msg.encode_to_vec();
        let policy = api_policy_from_bytes(&bytes).expect("decode");
        assert_eq!(policy.policy_id, format_uint64_id(9));
        assert_eq!(policy.name, "bots");
        assert_eq!(policy.revision, 4);
    }

    #[test]
    fn subaccount_policy_from_bytes_round_trip() {
        use crate::proto::auth::v1::SubaccountPolicyView;

        let msg = SubaccountPolicyView {
            id: 7,
            name: "trader".into(),
            revision: 3,
            ..Default::default()
        };
        let bytes = msg.encode_to_vec();
        let policy = subaccount_policy_from_bytes(&bytes).expect("decode");
        assert_eq!(policy.policy_id, format_uint64_id(7));
        assert_eq!(policy.name, "trader");
        assert_eq!(policy.revision, 3);
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
        let trade = market_trade_from_bytes(6)(&bytes).expect("decode");
        assert_eq!(trade.match_id, "99");
        assert_eq!(trade.side, "buy");
        assert_eq!(
            trade.qty.as_ref().unwrap().format(None).unwrap(),
            "0.000005"
        );
    }

    #[test]
    fn flow_detail_without_required_summary_fails_closed() {
        let msg = FlowDetailView {
            from_live_state: true,
            ..Default::default()
        };
        let error = flow_detail_from_bytes(&msg.encode_to_vec())
            .expect_err("missing flow summary must not become an empty success");
        assert!(error.to_string().contains("missing summary"));
    }
}
