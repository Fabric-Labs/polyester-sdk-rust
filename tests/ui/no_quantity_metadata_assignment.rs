use polyester::types::{Quantity, QuantityDomain};

fn main() {
    let mut quantity =
        Quantity::from_scaled(5, Some(8), QuantityDomain::OrderBase, None, None).unwrap();

    quantity.scale = Some(6);
    quantity.domain = QuantityDomain::Asset;
    quantity.symbol = Some("ETH-USDT".into());
    quantity.symbol_id = Some(9);
}
