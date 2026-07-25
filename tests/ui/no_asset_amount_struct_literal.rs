use polyester::types::{AssetAmount, QuantityDomain};

fn main() {
    // Fields are private: struct literals must not bypass from_scaled invariants.
    let _ = AssetAmount {
        scaled: -5,
        scale: Some(18),
        domain: QuantityDomain::LedgerE18,
        asset_id: None,
    };
}
