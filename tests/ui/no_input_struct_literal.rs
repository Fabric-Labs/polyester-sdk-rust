// Input structs that grow optional fields are built with constructors, so a
// new field is not a breaking change.
use polyester::models::{TrailingDistance, TrailingStop};

fn main() {
    #[allow(deprecated)]
    let _stop = TrailingStop {
        distance: Some(TrailingDistance::Bps(100)),
        activation_price: None,
        limit_price: None,
        trigger_price_source: None,
        order_type: None,
        max_slippage: None,
        state: None,
    };
}
