//! Pinned Polyester chain / account-abstraction environments.

/// EntryPoint configuration for ERC-4337.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPointConfig {
    pub address: &'static str,
    pub version: &'static str,
}

/// Safe / 4337 module deployment addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeDeploymentConfig {
    pub version: &'static str,
    pub safe_module_setup_address: &'static str,
    pub safe_4337_module_address: &'static str,
    pub safe_proxy_factory_address: &'static str,
    pub safe_singleton_address: &'static str,
    pub multi_send_address: &'static str,
    pub multi_send_call_only_address: Option<&'static str>,
}

/// Bundler / paymaster / EntryPoint / Safe deployment settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAbstractionEnvironment {
    pub bundler_url: &'static str,
    pub paymaster_url: &'static str,
    pub entry_point: EntryPointConfig,
    pub safe: SafeDeploymentConfig,
}

/// Polyester application contract addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractsEnvironment {
    pub trading_gateway_address: &'static str,
    pub funding_account_address: &'static str,
    pub guard_registry_address: &'static str,
    pub zipper_endpoint_address: &'static str,
}

/// On-chain / AA settings for Funding UserOps (not API-key Connect).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyesterChainEnvironment {
    pub name: &'static str,
    pub api_url: &'static str,
    pub websocket_url: &'static str,
    pub rpc_url: &'static str,
    pub chain_id: u64,
    pub account_abstraction: AccountAbstractionEnvironment,
    pub contracts: ContractsEnvironment,
}

/// Polyester testnet (chain 888168) environment.
pub const POLYESTER_TESTNET_ENVIRONMENT: PolyesterChainEnvironment = PolyesterChainEnvironment {
    name: "polyester-testnet",
    api_url: "https://api-devnet.polyester.ai",
    websocket_url: "wss://api-devnet.polyester.ai",
    rpc_url: "https://rpc.polyester.tech",
    chain_id: 888168,
    account_abstraction: AccountAbstractionEnvironment {
        bundler_url: "https://bundler.polyester.tech",
        paymaster_url: "https://paymaster.polyester.tech",
        entry_point: EntryPointConfig {
            address: "0x59a4B77766509c4507D79eFF8089474eC3daC174",
            version: "0.7",
        },
        safe: SafeDeploymentConfig {
            version: "1.4.1",
            safe_module_setup_address: "0x80791683D9C079A37Debc67EaDdbFcBC6f0FF2bB",
            safe_4337_module_address: "0x0713FF3d4c1b4f177833a372b1e3cb977540EA11",
            safe_proxy_factory_address: "0xF8F0F649Dd3bFa9095206691E9fb2356c26216dE",
            safe_singleton_address: "0x92abEa238FEA8908c397cE65366ea9278f0AeC7A",
            multi_send_address: "0x70C8a8CcB45a8E2589B0f019374fc923dA34E4c7",
            multi_send_call_only_address: Some("0x375C86a08DA98d1944D7B3c736307A72186CcAf1"),
        },
    },
    contracts: ContractsEnvironment {
        trading_gateway_address: "0xD3fecf5D39131e23b6B0f872cA0a21c8A5a30932",
        funding_account_address: "0xBfF4F6224BC10f233dDB1E61E770d9832aabC7c4",
        guard_registry_address: "0xd71F60FD6f784Cc0aD8c25441568C48705D95f64",
        zipper_endpoint_address: "0xae6B981BE9B73421eB1ba5372d1A4A937d63ffFB",
    },
};
