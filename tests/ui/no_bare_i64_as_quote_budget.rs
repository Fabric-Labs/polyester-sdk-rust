use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};

fn main() {
    let mut params = CreateOrderParams::new("BTC-USDT", CreateSide::Buy, CreateOrderType::Market);
    params.max_quote_debit_scaled = Some(5_000_000_i64);
}
