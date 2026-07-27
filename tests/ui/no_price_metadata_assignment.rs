use polyester::types::Price;

fn main() {
    let mut price = Price::from_ticks(42_500_000, Some("BTC-USDT".into())).unwrap();

    price.symbol = Some("ETH-USDT".into());
}
