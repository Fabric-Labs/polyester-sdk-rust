use polyester::types::Price;

#[allow(unreachable_code)]
fn main() {
    let _ = Price {
        ticks: loop {},
        symbol: Some("BTC-USDT".into()),
    };
}
