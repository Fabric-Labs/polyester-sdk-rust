//! A reply that must name what it created or changed refuses ID 0, the proto
//! default of an absent field, with `Error::ResponseContract`; the same reply
//! with a real ID decodes.

use super::*;
use crate::errors::{Error, Result};
use crate::proto::auth::v1::{
    AddressBookEntry, AddressBookTag, CopyAddressBookEntryResponse, CreateAddressBookEntryResponse,
    CreateAddressBookTagResponse, UpdateAddressBookEntryResponse, UpdateAddressBookTagResponse,
};
use crate::proto::orders::v1::{
    BatchCreateAccepted, BatchCreateOrdersResponse, BatchCreateResultItem, CancelAllOrdersResponse,
    CancelledOrder, batch_create_result_item, cancel_all_orders_response,
};
use crate::proto::triggers::v1::{
    BatchCancelTriggersResponse, BatchCancelTriggersResultItem, CancelOcoResponse,
    CancelTriggerResponse, CreateOcoResponse, CreateTriggerResponse, ModifyTriggerResponse,
    PauseTriggerResponse, ResumeTriggerResponse, TriggerStatus, batch_cancel_triggers_result_item,
};

fn assert_contract<T: std::fmt::Debug>(name: &str, result: Result<T>) {
    match result {
        Err(Error::ResponseContract { .. }) => {}
        other => panic!("{name}: want Error::ResponseContract, got {other:?}"),
    }
}

fn armed() -> buffa::EnumValue<TriggerStatus> {
    TriggerStatus::StatusArmed.into()
}

fn batch_create(order_id: u64) -> BatchCreateOrdersResponse {
    BatchCreateOrdersResponse {
        results: vec![BatchCreateResultItem {
            client_order_id: "c1".into(),
            outcome: Some(batch_create_result_item::Outcome::Accepted(Box::new(
                BatchCreateAccepted {
                    order_id,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }],
        accepted_count: 1,
        ..Default::default()
    }
}

fn cancel_all(order_id: u64) -> CancelAllOrdersResponse {
    CancelAllOrdersResponse {
        status: cancel_all_orders_response::Status::Submitted.into(),
        matched_orders: 1,
        submitted_cancels: 1,
        total_cancelled: 1,
        cancelled_orders: vec![CancelledOrder {
            order_id,
            client_order_id: "c1".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn batch_cancel_triggers(trigger_id: u64) -> BatchCancelTriggersResponse {
    BatchCancelTriggersResponse {
        results: vec![BatchCancelTriggersResultItem {
            status: batch_cancel_triggers_result_item::Status::Accepted.into(),
            trigger_id,
            ..Default::default()
        }],
        accepted_count: 1,
        ..Default::default()
    }
}

fn entry(address_book_entry_id: u64) -> AddressBookEntry {
    AddressBookEntry {
        address_book_entry_id,
        ..Default::default()
    }
}

fn tag(tag_id: u64) -> AddressBookTag {
    AddressBookTag {
        tag_id,
        ..Default::default()
    }
}

fn create_oco(order_list_id: u64, take_profit: u64, stop_loss: u64) -> CreateOcoResponse {
    CreateOcoResponse {
        order_list_id,
        client_list_id: "oco-1".into(),
        take_profit_trigger_id: take_profit,
        stop_loss_trigger_id: stop_loss,
        ..Default::default()
    }
}

#[test]
fn reply_decoders_refuse_a_zero_required_id() {
    assert_contract(
        "CreateTrigger",
        trigger_mutation_from_create(&CreateTriggerResponse {
            client_trigger_id: "t1".into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "CancelTrigger",
        trigger_mutation_from_cancel(&CancelTriggerResponse {
            status: armed(),
            ..Default::default()
        }),
    );
    assert_contract(
        "PauseTrigger",
        trigger_mutation_from_pause(&PauseTriggerResponse {
            status: armed(),
            ..Default::default()
        }),
    );
    assert_contract(
        "ResumeTrigger",
        trigger_mutation_from_resume(&ResumeTriggerResponse {
            status: armed(),
            ..Default::default()
        }),
    );
    assert_contract(
        "ModifyTrigger",
        trigger_mutation_from_modify(&ModifyTriggerResponse {
            status: armed(),
            ..Default::default()
        }),
    );
    assert_contract(
        "CreateOco order_list_id",
        create_oco_from_proto(&create_oco(0, 2, 3)),
    );
    assert_contract(
        "CreateOco take_profit",
        create_oco_from_proto(&create_oco(1, 0, 3)),
    );
    assert_contract(
        "CreateOco stop_loss",
        create_oco_from_proto(&create_oco(1, 2, 0)),
    );
    assert_contract(
        "CancelOco",
        cancel_oco_from_proto(&CancelOcoResponse {
            status: TriggerStatus::StatusCanceled.into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "BatchCancelTriggers item",
        batch_cancel_triggers_from_proto(&batch_cancel_triggers(0)),
    );
    assert_contract(
        "BatchCreateOrders accepted item",
        batch_create_from_proto(&batch_create(0)),
    );
    assert_contract(
        "CancelAllOrders cancelled order",
        cancel_all_from_proto(&cancel_all(0)),
    );
    assert_contract(
        "CreateAddressBookEntry",
        entry_from_create_proto(&CreateAddressBookEntryResponse {
            entry: entry(0).into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "UpdateAddressBookEntry",
        entry_from_update_proto(&UpdateAddressBookEntryResponse {
            entry: entry(0).into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "CopyAddressBookEntry",
        entry_from_copy_proto(&CopyAddressBookEntryResponse {
            entry: entry(0).into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "CreateAddressBookTag",
        tag_from_create_proto(&CreateAddressBookTagResponse {
            tag: tag(0).into(),
            ..Default::default()
        }),
    );
    assert_contract(
        "UpdateAddressBookTag",
        tag_from_update_proto(&UpdateAddressBookTagResponse {
            tag: tag(0).into(),
            ..Default::default()
        }),
    );
}

#[test]
fn reply_decoders_keep_a_real_required_id() {
    let id = crate::codecs::scalars::format_uint64_id;
    assert_eq!(
        trigger_mutation_from_cancel(&CancelTriggerResponse {
            trigger_id: 7,
            status: armed(),
            ..Default::default()
        })
        .unwrap()
        .trigger_id,
        id(7)
    );
    assert_eq!(
        create_oco_from_proto(&create_oco(1, 2, 3))
            .unwrap()
            .order_list_id,
        id(1)
    );
    assert_eq!(
        batch_cancel_triggers_from_proto(&batch_cancel_triggers(7))
            .unwrap()
            .results[0]
            .trigger_id,
        id(7)
    );
    assert_eq!(
        batch_create_from_proto(&batch_create(9)).unwrap().results[0].order_id,
        id(9)
    );
    assert_eq!(
        cancel_all_from_proto(&cancel_all(9))
            .unwrap()
            .cancelled_orders[0]
            .order_id,
        id(9)
    );
    let created = entry_from_create_proto(&CreateAddressBookEntryResponse {
        entry: entry(4).into(),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(created.address_book_entry_id, id(4));
    let tagged = tag_from_create_proto(&CreateAddressBookTagResponse {
        tag: tag(5).into(),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(tagged.tag_id, id(5));
}
