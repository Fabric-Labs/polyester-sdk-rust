//! Public contract e339360: the build version endpoint, smart account wallet
//! balances and the other additions this SDK maps.

mod alerts;

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::models::SubAccountScope;
use polyester::{Client, Config};
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const GET_VERSION_PATH: &str = "/system.v1.SystemService/GetVersion";
const GET_WALLET_BALANCES_PATH: &str = "/chain.withdraw.v1.WithdrawService/GetWalletBalances";

fn public_client(server: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: server.base_url(),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap()
}

fn keyed_client(server: &MockHttpServer) -> Client {
    Client::new(Config {
        api_url: server.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .unwrap()
}

/// `get_version` needs no API key and keeps every field a string: an
/// environment this SDK does not know and a post-release build version pass
/// through unchanged.
#[tokio::test]
async fn get_version_reads_the_build_without_credentials() {
    use polyester::proto::system::v1::GetVersionResponse;

    let server = MockHttpServer::spawn(|request| {
        if request.path != GET_VERSION_PATH {
            return HttpScript::NotFound;
        }
        connect_proto_ok(&GetVersionResponse {
            version: "0.92.9-12-g4e21a2f09670".into(),
            commit: "4e21a2f0967068fc944f5229eead01985df301b9".into(),
            environment: "staging-next".into(),
            ..Default::default()
        })
    })
    .await;
    let version = public_client(&server).get_version().await.unwrap();
    assert_eq!(version.version, "0.92.9-12-g4e21a2f09670");
    assert_eq!(version.commit, "4e21a2f0967068fc944f5229eead01985df301b9");
    assert_eq!(version.environment, "staging-next");
    assert_eq!(version.release(), "0.92.9");

    let release = polyester::models::ServerVersion {
        version: "0.10.0".into(),
        commit: String::new(),
        environment: "testnet".into(),
    };
    assert_eq!(release.release(), "0.10.0");
}

/// `get_time` (contract 0.93.5) needs no API key, is never signed, keeps the
/// server time exact, and refuses a reply without it.
#[tokio::test]
async fn get_time_reads_the_server_clock_unsigned() {
    use polyester::proto::system::v1::GetTimeResponse;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path != "/system.v1.SystemService/GetTime" {
            return HttpScript::NotFound;
        }
        let mut log = log.lock().unwrap();
        log.push(request.has_header("X-API-KEY-ID"));
        connect_proto_ok(&GetTimeResponse {
            server_time_ms: if log.len() < 3 { 1_763_337_600_123 } else { 0 },
            ..Default::default()
        })
    })
    .await;
    for client in [public_client(&server), keyed_client(&server)] {
        let time = client.get_time().await.unwrap();
        assert_eq!(time.server_time_ms, 1_763_337_600_123);
    }
    let err = public_client(&server).get_time().await.unwrap_err();
    assert!(
        matches!(err, polyester::Error::ResponseContract { .. }),
        "{err:?}"
    );
    assert_eq!(*seen.lock().unwrap(), [false, false, false]);
}

/// Wallet balances are signed reads of the account's smart account wallet:
/// the root account by default, or the named sub-account. Amounts keep all
/// 18 decimals.
#[tokio::test]
async fn get_wallet_balances_maps_ztokens_at_18_decimals() {
    use polyester::proto::chain::withdraw::v1::{
        GetWalletBalancesRequest, GetWalletBalancesResponse, WalletBalance,
    };
    use polyester::proto::polyester::r#type::v1::U128;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let server = MockHttpServer::spawn(move |request| {
        if request.path != GET_WALLET_BALANCES_PATH {
            return HttpScript::NotFound;
        }
        let req = GetWalletBalancesRequest::decode_from_slice(&request.body).unwrap();
        seen_handler.lock().unwrap().push(req.subaccount_id);
        connect_proto_ok(&GetWalletBalancesResponse {
            smart_account_address: "0x1111".into(),
            balances: vec![WalletBalance {
                zipped_asset_id: 12,
                unified_asset_id: 7,
                chain_id: 8453,
                ztoken_address: "0x2222".into(),
                // 2^64 + 1 wei-like units: needs both halves of the U128.
                amount_e18: U128 {
                    hi: 1,
                    lo: 1,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        })
    })
    .await;
    let client = keyed_client(&server);

    let root = client
        .withdraw
        .get_wallet_balances(SubAccountScope::Default)
        .await
        .unwrap();
    assert_eq!(root.smart_account_address, "0x1111");
    assert_eq!(root.balances.len(), 1);
    let balance = &root.balances[0];
    assert_eq!(balance.zipped_asset_id, 12);
    assert_eq!(balance.unified_asset_id, 7);
    assert_eq!(balance.chain_id, 8453);
    assert_eq!(balance.ztoken_address, "0x2222");
    assert_eq!(balance.amount_e18, "18446744073709551617");
    assert_eq!(balance.amount, "18.446744073709551617");

    client
        .withdraw
        .get_wallet_balances(SubAccountScope::Id(42))
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [0, 42]);
}

/// A balance without an amount breaks the response contract instead of
/// reading as zero.
#[tokio::test]
async fn get_wallet_balances_refuses_a_balance_without_amount() {
    use polyester::proto::chain::withdraw::v1::{GetWalletBalancesResponse, WalletBalance};

    let server = MockHttpServer::spawn(|_| {
        connect_proto_ok(&GetWalletBalancesResponse {
            smart_account_address: "0x1111".into(),
            balances: vec![WalletBalance {
                zipped_asset_id: 12,
                ..Default::default()
            }],
            ..Default::default()
        })
    })
    .await;
    let err = keyed_client(&server)
        .withdraw
        .get_wallet_balances(SubAccountScope::Default)
        .await
        .unwrap_err();
    assert!(
        matches!(err, polyester::Error::ResponseContract { .. }),
        "{err:?}"
    );
}

/// An asset whose balance could not be read is listed as unavailable, with
/// its reason, and the read is incomplete: its balance is unknown, not zero.
#[tokio::test]
async fn get_wallet_balances_reports_unreadable_assets() {
    use polyester::proto::chain::withdraw::v1::{
        GetWalletBalancesResponse, UnavailableWalletAsset, WalletAssetUnavailableReason,
    };

    let server = MockHttpServer::spawn(|_| {
        connect_proto_ok(&GetWalletBalancesResponse {
            smart_account_address: "0x1111".into(),
            unavailable_assets: vec![
                UnavailableWalletAsset {
                    zipped_asset_id: 12,
                    unified_asset_id: 3,
                    chain_id: 1,
                    ztoken_address: "0x2222".into(),
                    reason: WalletAssetUnavailableReason::READ_FAILED.into(),
                    ..Default::default()
                },
                UnavailableWalletAsset {
                    zipped_asset_id: 13,
                    reason: buffa::EnumValue::from(9),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    })
    .await;
    let wallet = keyed_client(&server)
        .withdraw
        .get_wallet_balances(SubAccountScope::Default)
        .await
        .unwrap();
    assert!(wallet.balances.is_empty());
    assert!(!wallet.is_complete());
    let reasons: Vec<_> = wallet
        .unavailable_assets
        .iter()
        .map(|a| (a.zipped_asset_id, a.reason.as_str()))
        .collect();
    assert_eq!(reasons, [(12, "read_failed"), (13, "unknown(9)")]);
    assert_eq!(wallet.unavailable_assets[0].ztoken_address, "0x2222");
}

/// The deposit and withdraw config refuses an available or stale route
/// supply without its observation time, rather than reporting it as current;
/// a missing supply carries no time and still decodes.
#[tokio::test]
async fn deposit_withdraw_config_refuses_supply_without_observation_time() {
    use polyester::proto::chain::zipper::v1::{
        AssetChainVariant, AssetConfig, GetDepositWithdrawConfigResponse, SupplyStatus,
    };

    let config = |status: SupplyStatus| GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        assets: vec![AssetConfig {
            asset: "USDT".into(),
            ledger_id: 7,
            quantity_scale: 6,
            variants: vec![AssetChainVariant {
                zipped_asset_id: 3,
                chain_id: 1,
                supply_q: 1_000_000,
                supply_status: status.into(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    for status in [SupplyStatus::AVAILABLE, SupplyStatus::STALE] {
        let body = config(status);
        let server = MockHttpServer::spawn(move |_| connect_proto_ok(&body)).await;
        let err = keyed_client(&server)
            .zipper
            .get_deposit_withdraw_config()
            .await
            .unwrap_err();
        assert!(
            matches!(err, polyester::Error::ResponseContract { .. }),
            "{status:?}: {err:?}"
        );
    }
    let body = config(SupplyStatus::MISSING);
    let server = MockHttpServer::spawn(move |_| connect_proto_ok(&body)).await;
    let cfg = keyed_client(&server)
        .zipper
        .get_deposit_withdraw_config()
        .await
        .unwrap();
    assert_eq!(cfg.assets[0].variants[0].supply, None);
}

/// A present market datapoint carries its value, source and observation
/// time, and a present unlimited-supply flag its source and time: one that
/// lacks any of them is refused. An absent datapoint stays unknown, and a
/// value of "0" is valid.
#[tokio::test]
async fn asset_market_datapoints_refuse_missing_parts() {
    use buffa_types::google::protobuf::Timestamp;
    use polyester::proto::marketdata::v1::{
        AssetDataSource, AssetInfo, AssetMarketFlag, AssetMarketStats, AssetMarketValue,
        ListAssetInfoResponse,
    };

    let at = || -> buffa::MessageField<Timestamp> {
        Timestamp {
            seconds: 1_794_700_000,
            ..Default::default()
        }
        .into()
    };
    let value = |v: &str| AssetMarketValue {
        value: v.into(),
        source: AssetDataSource::COINMARKETCAP.into(),
        as_of: at(),
        ..Default::default()
    };
    let reply = |stats: AssetMarketStats| ListAssetInfoResponse {
        asset_info: vec![AssetInfo {
            asset_id: 1,
            market_stats: stats.into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let read = |body: ListAssetInfoResponse| async move {
        let server = MockHttpServer::spawn(move |_| connect_proto_ok(&body)).await;
        public_client(&server)
            .market_data
            .list_asset_info(&[1])
            .await
    };

    let bad = [
        (
            "no value",
            AssetMarketValue {
                value: String::new(),
                ..value("1")
            },
        ),
        (
            "no source",
            AssetMarketValue {
                source: AssetDataSource::SOURCE_UNSPECIFIED.into(),
                ..value("1")
            },
        ),
        (
            "no as_of",
            AssetMarketValue {
                as_of: Default::default(),
                ..value("1")
            },
        ),
        ("signed", value("-1")),
        ("exponent", value("1e9")),
    ];
    for (label, datapoint) in bad {
        let err = read(reply(AssetMarketStats {
            circulating_supply: datapoint.into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
        assert!(
            matches!(err, polyester::Error::ResponseContract { .. }),
            "{label}: {err:?}"
        );
    }
    for (label, flag) in [
        (
            "flag without source",
            AssetMarketFlag {
                as_of: at(),
                ..Default::default()
            },
        ),
        (
            "flag without as_of",
            AssetMarketFlag {
                source: AssetDataSource::POLYESTER.into(),
                ..Default::default()
            },
        ),
    ] {
        let err = read(reply(AssetMarketStats {
            max_supply_unlimited: flag.into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
        assert!(
            matches!(err, polyester::Error::ResponseContract { .. }),
            "{label}: {err:?}"
        );
    }

    // Absent datapoints stay unknown; "0" is a valid value.
    let infos = read(reply(AssetMarketStats {
        total_supply: value("0").into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let stats = infos[0].market_stats.as_ref().expect("stats");
    assert_eq!(
        stats.total_supply.as_ref().map(|v| v.value.as_str()),
        Some("0")
    );
    assert!(stats.circulating_supply.is_none() && stats.max_supply_unlimited.is_none());
}
