//! Public live check: SDK pins match the Zipper catalog for that API host.

use polyester::{
    Client, Config, POLYESTER_DEVNET_ENVIRONMENT, POLYESTER_TESTNET_ENVIRONMENT,
    PolyesterEnvironment,
};

async fn assert_pins_match_catalog(environment: PolyesterEnvironment) {
    let client = match Client::new(Config {
        environment: Some(environment.clone()),
        hydrate_catalogs: false,
        ..Default::default()
    }) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("skip: live zipper catalog unavailable: {err}");
            return;
        }
    };
    let config = match client.zipper.get_deposit_withdraw_config().await {
        Ok(config) => config,
        Err(err) => {
            eprintln!("skip: live zipper catalog unavailable: {err}");
            return;
        }
    };
    let mut by_name = std::collections::HashMap::new();
    for item in config.contracts {
        by_name.insert(item.name, item.address);
    }
    let expected = [
        (
            "tradingGateway",
            environment.contracts.trading_gateway_address.as_str(),
        ),
        (
            "fundingAccount",
            environment.contracts.funding_account_address.as_str(),
        ),
        (
            "guardRegistry",
            environment.contracts.guard_registry_address.as_str(),
        ),
        (
            "zipperEndpoint",
            environment.contracts.zipper_endpoint_address.as_str(),
        ),
        (
            "EntryPoint",
            environment.account_abstraction.entry_point.address.as_str(),
        ),
    ];
    for (name, address) in expected {
        let got = by_name
            .get(name)
            .unwrap_or_else(|| panic!("{} catalog missing {name}", environment.name));
        assert!(
            got.eq_ignore_ascii_case(address),
            "{} {name}: catalog {got} != pin {address}",
            environment.name
        );
    }
}

#[tokio::test]
async fn devnet_pins_match_live_zipper_catalog() {
    assert_pins_match_catalog(POLYESTER_DEVNET_ENVIRONMENT.clone()).await;
}

#[tokio::test]
async fn testnet_pins_match_live_zipper_catalog() {
    assert_pins_match_catalog(POLYESTER_TESTNET_ENVIRONMENT.clone()).await;
}
