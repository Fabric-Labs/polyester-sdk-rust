use polyester::models::CreateInternalTransferParams;
use polyester::types::{Quantity, QuantityDomain};

fn main() {
    let qty = Quantity::from_scaled(100, Some(8), QuantityDomain::OrderBase, None, None).unwrap();
    // Order Quantity cannot be used where AssetAmount is required.
    let _ = CreateInternalTransferParams {
        asset_id: 1,
        quantity: qty,
        idempotency_key: "x".into(),
        subaccount_id: None,
        destination_account_id: Some("1".into()),
        destination_subaccount_id: None,
        destination_smart_account_address: None,
        quantity_scale: None,
    };
}
