use polyester::PriceTicks;

fn main() {
    // PriceTicks::new is crate-private so callers cannot bypass validation.
    let _ = PriceTicks::new(1);
}
