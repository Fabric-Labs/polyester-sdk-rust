use polyester::types::{Quantity, QuantityDomain};

#[allow(unreachable_code)]
fn main() {
    let _ = Quantity {
        scaled: loop {},
        scale: Some(8),
        domain: QuantityDomain::OrderBase,
        symbol: Some("BTC-USDT".into()),
        symbol_id: Some(7),
    };
}
