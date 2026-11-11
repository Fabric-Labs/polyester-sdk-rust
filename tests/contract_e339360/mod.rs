//! Public contract e339360: the build version endpoint, smart account wallet
//! balances and the other additions this SDK maps.

mod alerts;

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
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

    let root = client.withdraw.get_wallet_balances(None).await.unwrap();
    assert_eq!(root.smart_account_address, "0x1111");
    assert_eq!(root.balances.len(), 1);
    let balance = &root.balances[0];
    assert_eq!(balance.zipped_asset_id, 12);
    assert_eq!(balance.unified_asset_id, 7);
    assert_eq!(balance.chain_id, 8453);
    assert_eq!(balance.ztoken_address, "0x2222");
    assert_eq!(balance.amount_e18, "18446744073709551617");
    assert_eq!(balance.amount, "18.446744073709551617");

    client.withdraw.get_wallet_balances(Some(42)).await.unwrap();
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
        .get_wallet_balances(None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, polyester::Error::ResponseContract { .. }),
        "{err:?}"
    );
}
