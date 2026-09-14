//! First-class Polyester network environments.
//!
//! Presets match `polyester-sdk-typescript` `src/environment.ts` plus zipper
//! catalog extras used by language-SDK chain helpers.

use crate::errors::{Error, Result};
use crate::transport::{validate_http_url, validate_ws_url};
use alloy_primitives::Address;
use std::str::FromStr;
use std::sync::LazyLock;

/// Process env that selects a named preset for [`crate::Client::from_env`].
pub const ENV_NAME_ENV: &str = "POLYESTER_ENV";

/// ERC-4337 EntryPoint pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPointConfig {
    pub address: String,
    pub version: String,
}

/// Safe / 4337 module deployment addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeDeploymentConfig {
    pub version: String,
    pub safe_module_setup_address: String,
    pub safe_4337_module_address: String,
    pub safe_proxy_factory_address: String,
    pub safe_singleton_address: String,
    pub multi_send_address: String,
    pub multi_send_call_only_address: Option<String>,
}

/// Bundler / paymaster / EntryPoint / Safe deployment settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAbstractionEnvironment {
    pub bundler_url: String,
    pub paymaster_url: String,
    pub entry_point: EntryPointConfig,
    pub safe: SafeDeploymentConfig,
}

/// Polyester application contract addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractsEnvironment {
    pub trading_gateway_address: String,
    pub funding_account_address: String,
    pub guard_registry_address: String,
    pub zipper_endpoint_address: String,
}

/// Complete SDK environment: API, realtime, RPC, AA, and contract pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyesterEnvironment {
    pub name: String,
    pub api_url: String,
    pub websocket_url: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub chain_name: String,
    pub explorer_url: String,
    pub account_abstraction: AccountAbstractionEnvironment,
    pub contracts: ContractsEnvironment,
}

/// Backward-compatible alias used by chain helpers.
pub type PolyesterChainEnvironment = PolyesterEnvironment;

/// Input to [`create_polyester_environment`].
#[derive(Debug, Clone)]
pub struct CreatePolyesterEnvironmentParams {
    pub name: String,
    pub api_url: String,
    pub websocket_url: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub chain_name: String,
    pub explorer_url: String,
    pub account_abstraction: AccountAbstractionEnvironment,
    pub contracts: ContractsEnvironment,
    pub allow_insecure: bool,
}

fn strip_trailing_slashes(value: &str) -> String {
    value.trim_end_matches('/').to_owned()
}

fn normalize_http_url(
    value: &str,
    label: &str,
    allow_search: bool,
    allow_insecure: bool,
) -> Result<String> {
    if value != value.trim() {
        return Err(Error::validation(format!(
            "{label} must not contain surrounding whitespace"
        )));
    }
    let parsed = validate_http_url(value, allow_insecure)?;
    if !allow_search && parsed.query().is_some() {
        return Err(Error::validation(format!(
            "{label} must not include query parameters"
        )));
    }
    if parsed.fragment().is_some() {
        return Err(Error::validation(format!(
            "{label} must not include a fragment"
        )));
    }
    Ok(strip_trailing_slashes(value))
}

fn normalize_ws_url(value: &str, label: &str, allow_insecure: bool) -> Result<String> {
    if value != value.trim() {
        return Err(Error::validation(format!(
            "{label} must not contain surrounding whitespace"
        )));
    }
    let parsed = validate_ws_url(value, allow_insecure)?;
    if parsed.fragment().is_some() {
        return Err(Error::validation(format!(
            "{label} must not include a fragment"
        )));
    }
    Ok(strip_trailing_slashes(value))
}

fn normalize_address(value: &str, label: &str) -> Result<String> {
    let addr = Address::from_str(value.trim())
        .map_err(|_| Error::validation(format!("{label} must be a valid address")))?;
    Ok(addr.to_checksum(None))
}

fn normalize_entry_point(entry_point: &EntryPointConfig) -> Result<EntryPointConfig> {
    if entry_point.version != "0.7" {
        return Err(Error::validation(
            "account_abstraction.entry_point.version must be 0.7".to_owned(),
        ));
    }
    Ok(EntryPointConfig {
        address: normalize_address(
            &entry_point.address,
            "account_abstraction.entry_point.address",
        )?,
        version: entry_point.version.clone(),
    })
}

fn normalize_safe(safe: &SafeDeploymentConfig) -> Result<SafeDeploymentConfig> {
    if safe.version != "1.4.1" && safe.version != "1.5.0" {
        return Err(Error::validation(
            "account_abstraction.safe.version must be either \"1.4.1\" or \"1.5.0\"".to_owned(),
        ));
    }
    Ok(SafeDeploymentConfig {
        version: safe.version.clone(),
        safe_module_setup_address: normalize_address(
            &safe.safe_module_setup_address,
            "account_abstraction.safe.safe_module_setup_address",
        )?,
        safe_4337_module_address: normalize_address(
            &safe.safe_4337_module_address,
            "account_abstraction.safe.safe_4337_module_address",
        )?,
        safe_proxy_factory_address: normalize_address(
            &safe.safe_proxy_factory_address,
            "account_abstraction.safe.safe_proxy_factory_address",
        )?,
        safe_singleton_address: normalize_address(
            &safe.safe_singleton_address,
            "account_abstraction.safe.safe_singleton_address",
        )?,
        multi_send_address: normalize_address(
            &safe.multi_send_address,
            "account_abstraction.safe.multi_send_address",
        )?,
        multi_send_call_only_address: match &safe.multi_send_call_only_address {
            Some(value) => Some(normalize_address(
                value,
                "account_abstraction.safe.multi_send_call_only_address",
            )?),
            None => None,
        },
    })
}

/// Validate and freeze a complete environment (presets and custom / VPC URLs).
pub fn create_polyester_environment(
    params: CreatePolyesterEnvironmentParams,
) -> Result<PolyesterEnvironment> {
    if params.name.trim().is_empty() {
        return Err(Error::validation(
            "name must be a non-empty string".to_owned(),
        ));
    }
    if params.chain_id == 0 {
        return Err(Error::validation(
            "chain_id must be a positive integer".to_owned(),
        ));
    }
    let api_url = normalize_http_url(&params.api_url, "api_url", false, params.allow_insecure)?;
    let websocket_url = normalize_ws_url(
        &params.websocket_url,
        "websocket_url",
        params.allow_insecure,
    )?;
    let rpc_url = normalize_http_url(&params.rpc_url, "rpc_url", true, params.allow_insecure)?;
    let bundler_url = normalize_http_url(
        &params.account_abstraction.bundler_url,
        "bundler_url",
        true,
        params.allow_insecure,
    )?;
    let paymaster_url = normalize_http_url(
        &params.account_abstraction.paymaster_url,
        "paymaster_url",
        true,
        params.allow_insecure,
    )?;
    Ok(PolyesterEnvironment {
        name: params.name.trim().to_owned(),
        api_url,
        websocket_url,
        rpc_url,
        chain_id: params.chain_id,
        chain_name: params.chain_name.trim().to_owned(),
        explorer_url: params.explorer_url.trim().to_owned(),
        account_abstraction: AccountAbstractionEnvironment {
            bundler_url,
            paymaster_url,
            entry_point: normalize_entry_point(&params.account_abstraction.entry_point)?,
            safe: normalize_safe(&params.account_abstraction.safe)?,
        },
        contracts: ContractsEnvironment {
            trading_gateway_address: normalize_address(
                &params.contracts.trading_gateway_address,
                "contracts.trading_gateway_address",
            )?,
            funding_account_address: normalize_address(
                &params.contracts.funding_account_address,
                "contracts.funding_account_address",
            )?,
            guard_registry_address: normalize_address(
                &params.contracts.guard_registry_address,
                "contracts.guard_registry_address",
            )?,
            zipper_endpoint_address: normalize_address(
                &params.contracts.zipper_endpoint_address,
                "contracts.zipper_endpoint_address",
            )?,
        },
    })
}

/// Re-validate an environment supplied to a client constructor.
pub fn parse_polyester_environment(
    environment: &PolyesterEnvironment,
) -> Result<PolyesterEnvironment> {
    create_polyester_environment(CreatePolyesterEnvironmentParams {
        name: environment.name.clone(),
        api_url: environment.api_url.clone(),
        websocket_url: environment.websocket_url.clone(),
        rpc_url: environment.rpc_url.clone(),
        chain_id: environment.chain_id,
        chain_name: environment.chain_name.clone(),
        explorer_url: environment.explorer_url.clone(),
        account_abstraction: environment.account_abstraction.clone(),
        contracts: environment.contracts.clone(),
        allow_insecure: false,
    })
}

impl PolyesterEnvironment {
    /// Copy this environment with endpoint overrides (market-maker / VPC case).
    pub fn with_urls(
        &self,
        api_url: Option<&str>,
        websocket_url: Option<&str>,
        rpc_url: Option<&str>,
    ) -> Result<Self> {
        create_polyester_environment(CreatePolyesterEnvironmentParams {
            name: self.name.clone(),
            api_url: api_url.unwrap_or(&self.api_url).to_owned(),
            websocket_url: websocket_url.unwrap_or(&self.websocket_url).to_owned(),
            rpc_url: rpc_url.unwrap_or(&self.rpc_url).to_owned(),
            chain_id: self.chain_id,
            chain_name: self.chain_name.clone(),
            explorer_url: self.explorer_url.clone(),
            account_abstraction: self.account_abstraction.clone(),
            contracts: self.contracts.clone(),
            allow_insecure: false,
        })
    }
}

/// Resolve `devnet` / `testnet` (and `polyester-*` aliases) to a preset.
pub fn environment_from_name(name: &str) -> Result<PolyesterEnvironment> {
    match name.trim().to_ascii_lowercase().as_str() {
        "devnet" | "polyester-devnet" => Ok(POLYESTER_DEVNET_ENVIRONMENT.clone()),
        "testnet" | "polyester-testnet" => Ok(POLYESTER_TESTNET_ENVIRONMENT.clone()),
        _ => Err(Error::validation(format!(
            "unknown environment {name:?}; expected \"devnet\" or \"testnet\""
        ))),
    }
}

fn must_env(params: CreatePolyesterEnvironmentParams) -> PolyesterEnvironment {
    create_polyester_environment(params).expect("bundled environment pins must be valid")
}

/// Polyester devnet (chain 888168). Client / chain-helper default.
pub static POLYESTER_DEVNET_ENVIRONMENT: LazyLock<PolyesterEnvironment> = LazyLock::new(|| {
    must_env(CreatePolyesterEnvironmentParams {
        name: "polyester-devnet".into(),
        api_url: "https://api-devnet.polyester.ai".into(),
        websocket_url: "wss://api-devnet.polyester.ai".into(),
        rpc_url: "https://rpc.polyester.tech".into(),
        chain_id: 888168,
        chain_name: "Polyester Chain Devnet".into(),
        explorer_url: "https://devnet.polyesterscan.com".into(),
        account_abstraction: AccountAbstractionEnvironment {
            bundler_url: "https://bundler.polyester.tech".into(),
            paymaster_url: "https://paymaster.polyester.tech".into(),
            entry_point: EntryPointConfig {
                address: "0x59a4B77766509c4507D79eFF8089474eC3daC174".into(),
                version: "0.7".into(),
            },
            safe: SafeDeploymentConfig {
                version: "1.4.1".into(),
                safe_module_setup_address: "0x80791683D9C079A37Debc67EaDdbFcBC6f0FF2bB".into(),
                safe_4337_module_address: "0x0713FF3d4c1b4f177833a372b1e3cb977540EA11".into(),
                safe_proxy_factory_address: "0xF8F0F649Dd3bFa9095206691E9fb2356c26216dE".into(),
                safe_singleton_address: "0x92abEa238FEA8908c397cE65366ea9278f0AeC7A".into(),
                multi_send_address: "0x70C8a8CcB45a8E2589B0f019374fc923dA34E4c7".into(),
                multi_send_call_only_address: Some(
                    "0x375C86a08DA98d1944D7B3c736307A72186CcAf1".into(),
                ),
            },
        },
        contracts: ContractsEnvironment {
            trading_gateway_address: "0xD3fecf5D39131e23b6B0f872cA0a21c8A5a30932".into(),
            funding_account_address: "0xBfF4F6224BC10f233dDB1E61E770d9832aabC7c4".into(),
            guard_registry_address: "0xd71F60FD6f784Cc0aD8c25441568C48705D95f64".into(),
            zipper_endpoint_address: "0xae6B981BE9B73421eB1ba5372d1A4A937d63ffFB".into(),
        },
        allow_insecure: false,
    })
});

/// Public Polyester testnet (chain 888169).
pub static POLYESTER_TESTNET_ENVIRONMENT: LazyLock<PolyesterEnvironment> = LazyLock::new(|| {
    must_env(CreatePolyesterEnvironmentParams {
        name: "polyester-testnet".into(),
        api_url: "https://api-testnet.polyester.com".into(),
        websocket_url: "wss://api-testnet.polyester.com".into(),
        rpc_url: "https://rpc.polyester.live".into(),
        chain_id: 888169,
        chain_name: "Polyester Chain Testnet".into(),
        explorer_url: "https://testnet.polyesterscan.com".into(),
        account_abstraction: AccountAbstractionEnvironment {
            bundler_url: "https://bundler.polyester.live".into(),
            paymaster_url: "https://paymaster.polyester.live".into(),
            entry_point: EntryPointConfig {
                address: "0x35c524a72ffb4D348d616cDD340D176c8f3C8B2C".into(),
                version: "0.7".into(),
            },
            safe: SafeDeploymentConfig {
                version: "1.4.1".into(),
                safe_module_setup_address: "0xdA9510c95Ab50EAd5A3DD28FA6BACce497dCF1fB".into(),
                safe_4337_module_address: "0xE278E4BCb71b095f7dAaa1bcEc1950696Fc40C74".into(),
                safe_proxy_factory_address: "0x2b8250158D58dD6D5e89313fa940586C9054A547".into(),
                safe_singleton_address: "0x6f00AB12B6A8aFf400F14f4Cd738549f0F53390d".into(),
                multi_send_address: "0xA38fEFA19ff5d8E3d988b2a0e6C8A2ae099fd97D".into(),
                multi_send_call_only_address: Some(
                    "0xE99b6c6d550B322347EeE11f4e8643377D8475A8".into(),
                ),
            },
        },
        contracts: ContractsEnvironment {
            trading_gateway_address: "0x20ef1BCeE69D73Ce1649E688dAA9A7AcF441f0EE".into(),
            funding_account_address: "0x57D15F393772041b4107943CAFa8A70b620212D1".into(),
            guard_registry_address: "0xB0E23DDa102c5d37AcBA583cf84E5aC214e7521C".into(),
            zipper_endpoint_address: "0xD439270f881b56727EaaB878CE4e80eB08A25BEB".into(),
        },
        allow_insecure: false,
    })
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn devnet_preset_matches_typescript() {
        let env = &*POLYESTER_DEVNET_ENVIRONMENT;
        assert_eq!(env.name, "polyester-devnet");
        assert_eq!(env.api_url, "https://api-devnet.polyester.ai");
        assert_eq!(env.chain_id, 888168);
        assert_eq!(
            env.contracts.trading_gateway_address,
            "0xD3fecf5D39131e23b6B0f872cA0a21c8A5a30932"
        );
    }

    #[test]
    fn testnet_preset_matches_typescript() {
        let env = &*POLYESTER_TESTNET_ENVIRONMENT;
        assert_eq!(env.name, "polyester-testnet");
        assert_eq!(env.api_url, "https://api-testnet.polyester.com");
        assert_eq!(env.chain_id, 888169);
        assert_eq!(
            env.contracts.trading_gateway_address,
            "0x20ef1BCeE69D73Ce1649E688dAA9A7AcF441f0EE"
        );
        assert_eq!(
            env.contracts.zipper_endpoint_address,
            "0xD439270f881b56727EaaB878CE4e80eB08A25BEB"
        );
    }

    #[test]
    fn with_urls_keeps_chain_pins() {
        let custom = POLYESTER_TESTNET_ENVIRONMENT
            .with_urls(
                Some("https://mm.internal.example"),
                Some("wss://mm.internal.example"),
                None,
            )
            .expect("custom");
        assert_eq!(custom.api_url, "https://mm.internal.example");
        assert_eq!(custom.chain_id, POLYESTER_TESTNET_ENVIRONMENT.chain_id);
        assert_eq!(custom.contracts, POLYESTER_TESTNET_ENVIRONMENT.contracts);
    }

    #[test]
    fn create_allows_loopback_http() {
        let custom = POLYESTER_DEVNET_ENVIRONMENT
            .with_urls(Some("http://127.0.0.1:8080"), None, None)
            .expect("loopback");
        assert_eq!(custom.api_url, "http://127.0.0.1:8080");
    }

    #[test]
    fn create_rejects_remote_plaintext() {
        let err = POLYESTER_DEVNET_ENVIRONMENT
            .with_urls(Some("http://api.example.test"), None, None)
            .expect_err("insecure");
        assert!(err.to_string().contains("TLS") || err.to_string().contains("secure"));
    }

    #[test]
    fn environment_from_name_resolves_presets() {
        assert_eq!(environment_from_name("devnet").unwrap().chain_id, 888168);
        assert!(environment_from_name("mainnet").is_err());
    }
}
