use polyester::types::Quantity;

fn main() {
    // Floats are not accepted as Quantity.
    let _: Quantity = 0.01_f64;
}
