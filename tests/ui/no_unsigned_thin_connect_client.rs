use polyester::{Client, Config};

fn main() {
    let client = Client::new(Config {
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap();

    // Service wrappers own request signing. Their raw generated clients are
    // intentionally not exposed as an apparently authenticated escape hatch.
    let _ = client.guard_signer.connect_client();
}
