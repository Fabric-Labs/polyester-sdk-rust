use polyester::models::{CreateOrderParams, CreateOrderType, CreateSide};

fn main() {
    let _params = CreateOrderParams {
        symbol: "BTC-USDT".into(),
        side: CreateSide::Buy,
        order_type: CreateOrderType::Market,
        quantity: None,
        max_quote_debit_scaled: Some(5_000_000_i64),
        price: None,
        time_in_force: None,
        client_order_id: None,
        subaccount_id: None,
        post_only: None,
        market_client_ref_price: None,
        fee_asset: None,
        self_trade_prevention: None,
        market_max_slippage: None,
        attached_risk: None,
    };
}
