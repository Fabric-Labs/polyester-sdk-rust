//! Request fields the contract bounds, on a local server: a value outside the
//! rule is an `Error::Validation` before anything is sent, and the bounds
//! themselves are sent unchanged.

use super::hardening_support::{HttpScript, MockHttpServer, connect_proto_ok};
use buffa::Message as _;
use polyester::proto::chain::deposit::v1::{
    CreateDepositAddressRequest, CreateDepositAddressResponse,
};
use polyester::proto::marketdata::v1::{ListAssetInfoRequest, ListAssetInfoResponse};
use polyester::{Client, Config, Error};
use std::sync::{Arc, Mutex};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[derive(Debug, PartialEq)]
enum Sent {
    AssetInfo(Vec<u32>),
    DepositAddress(u32),
}

async fn server() -> (Client, MockHttpServer, Arc<Mutex<Vec<Sent>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let log = sent.clone();
    let http = MockHttpServer::spawn(move |req| {
        let (entry, answer) = match req.path.rsplit('/').next().unwrap_or_default() {
            "ListAssetInfo" => (
                Sent::AssetInfo(
                    ListAssetInfoRequest::decode_from_slice(&req.body)
                        .unwrap()
                        .asset_ids,
                ),
                connect_proto_ok(&ListAssetInfoResponse::default()),
            ),
            "CreateDepositAddress" => (
                Sent::DepositAddress(
                    CreateDepositAddressRequest::decode_from_slice(&req.body)
                        .unwrap()
                        .chain_id,
                ),
                connect_proto_ok(&CreateDepositAddressResponse::default()),
            ),
            _ => return HttpScript::NotFound,
        };
        log.lock().unwrap().push(entry);
        answer
    })
    .await;
    let client = Client::new(Config {
        api_url: http.base_url(),
        api_key_id: Some("ak_test".into()),
        api_private_key: Some(TEST_KEY.into()),
        hydrate_catalogs: false,
        ..Default::default()
    })
    .expect("client");
    (client, http, sent)
}

fn assert_refused(label: &str, want: &str, result: polyester::Result<impl std::fmt::Debug>) {
    match result {
        Err(Error::Validation(message)) => assert!(message.contains(want), "{label}: {message}"),
        other => panic!("{label}: expected a validation error naming {want}, got {other:?}"),
    }
}

/// `ListAssetInfoRequest.asset_ids`: `max_items: 1000` and `items: { uint32:
/// { gt: 0, lte: 2147483647 } }`. An ID of 0 or above 2147483647, or more
/// than 1000 IDs, is refused; the bounds, 1000 IDs and an empty list (every
/// asset) are sent.
#[tokio::test]
async fn asset_info_ids_outside_the_contract_range_are_never_sent() {
    let (c, _http, sent) = server().await;
    let thousand: Vec<u32> = (1..=1000).collect();
    let too_many: Vec<u32> = (1..=1001).collect();
    for ids in [
        &[0][..],
        &[2_147_483_648],
        &[0, 2_147_483_648],
        &[1, u32::MAX],
        &too_many,
    ] {
        assert_refused(
            &format!("{} ids from {:?}", ids.len(), ids.first()),
            "asset_ids",
            c.market_data.list_asset_info(ids).await,
        );
    }
    assert!(
        sent.lock().unwrap().is_empty(),
        "{:?}",
        sent.lock().unwrap()
    );

    c.market_data
        .list_asset_info(&[1, 2_147_483_647])
        .await
        .unwrap();
    c.market_data.list_asset_info(&[]).await.unwrap();
    c.market_data.list_asset_info(&thousand).await.unwrap();
    assert_eq!(
        *sent.lock().unwrap(),
        [
            Sent::AssetInfo(vec![1, 2_147_483_647]),
            Sent::AssetInfo(vec![]),
            Sent::AssetInfo(thousand),
        ]
    );
}

/// `CreateDepositAddressRequest.chain_id`: `uint32.gt = 0`, with no
/// ignore-if-zero, so an unset chain ID is refused; any positive one is sent.
#[tokio::test]
async fn deposit_address_without_a_chain_is_never_sent() {
    let (c, _http, sent) = server().await;
    assert_refused(
        "chain 0",
        "chain_id",
        c.deposit
            .create_address(CreateDepositAddressRequest::default())
            .await,
    );
    assert!(
        sent.lock().unwrap().is_empty(),
        "{:?}",
        sent.lock().unwrap()
    );

    for chain_id in [1, u32::MAX] {
        // The empty answer is the stub's; only what was sent matters here.
        let _ = c
            .deposit
            .create_address(CreateDepositAddressRequest {
                chain_id,
                ..Default::default()
            })
            .await;
    }
    assert_eq!(
        *sent.lock().unwrap(),
        [Sent::DepositAddress(1), Sent::DepositAddress(u32::MAX)]
    );
}
