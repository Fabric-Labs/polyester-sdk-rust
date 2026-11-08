// Quick Swap is served by the TypeScript SDK only. This crate exposes neither
// its generated messages nor its generated Connect clients.
use polyester::connect::swap::quickswap::v1::QuickSwapServiceClient;
use polyester::proto::swap::quickswap::v1::GetQuickSwapResponse;

fn main() {
    let _ = std::mem::size_of::<GetQuickSwapResponse>();
    let _ = std::any::type_name::<QuickSwapServiceClient<()>>();
}
