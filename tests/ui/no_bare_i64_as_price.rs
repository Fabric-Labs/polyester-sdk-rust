use polyester::types::Price;

fn main() {
    // Bare integers are not a Price — constructors are required.
    let _: Price = 1_500_000;
}
