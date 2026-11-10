//! POLY-9237: `ZipperService::get_deposit_withdraw_config` keeps each asset
//! variant's supply, `supply_q` at its asset's quantity scale, and the
//! catalogs hydrated at construction know it before any supply update.

use super::hardening_support::RefusedPort;
use buffa::Message as _;
use polyester::proto::chain::zipper::v1::{
    AssetChainVariant, AssetConfig as ZipperAsset, ChainConfig, GetDepositWithdrawConfigResponse,
};
use polyester::proto::marketdata::v1::{AssetConfig, GetSpotConfigResponse, PairConfig};
use polyester::transport::WireFormat;
use polyester::{Client, Config};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SPOT_CONFIG: &str = "/marketdata.v1.MarketDataService/GetSpotConfig";
const ZIPPER_CONFIG: &str = "/chain.zipper.v1.ZipperService/GetDepositWithdrawConfig";
const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// zipped_asset_id, supply_q and the decimal supply the SDK must report.
const SCALE_0: [(u32, u64, &str); 2] = [(11, 3_582_802, "3582802"), (12, 0, "0")];
const SCALE_8: [(u32, u64, &str); 2] = [(21, 9_708_173_223, "97.08173223"), (22, 0, "0")];

fn spot_config() -> GetSpotConfigResponse {
    GetSpotConfigResponse {
        assets: vec![AssetConfig {
            asset: "BTC".into(),
            quantity_scale: 8,
            market_data_volume_scale: 8,
            ..Default::default()
        }],
        pairs: vec![PairConfig {
            symbol_id: 1,
            symbol: "BTC-USDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            base_quantity_scale: 8,
            quote_quantity_scale: 6,
            reference_price_scale: 9,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn zipper_config() -> GetDepositWithdrawConfigResponse {
    let variants = |rows: [(u32, u64, &str); 2]| {
        rows.iter()
            .map(|(id, supply_q, _)| AssetChainVariant {
                zipped_asset_id: *id,
                chain_id: *id,
                network_fee: "0.0001".into(),
                deposit_min_amount: "0.001".into(),
                withdraw_min_amount: "0.002".into(),
                supply_q: *supply_q,
                ..Default::default()
            })
            .collect()
    };
    GetDepositWithdrawConfigResponse {
        ts_sec: 1,
        chains: [11u32, 12, 21, 22]
            .iter()
            .map(|id| ChainConfig {
                chain_id: *id,
                code: "chain".into(),
                name: "Chain".into(),
                ..Default::default()
            })
            .collect(),
        assets: vec![
            ZipperAsset {
                asset: "WHOLE".into(),
                ledger_id: 1,
                quantity_scale: 0,
                variants: variants(SCALE_0),
                ..Default::default()
            },
            ZipperAsset {
                asset: "BTC".into(),
                ledger_id: 2,
                quantity_scale: 8,
                variants: variants(SCALE_8),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

async fn serve(mut stream: TcpStream) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let Ok(n) = stream.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(index) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let path = head.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let json = head
        .to_ascii_lowercase()
        .contains("content-type: application/json");
    let (content_type, payload) = match (path.as_str(), json) {
        (SPOT_CONFIG, true) => (
            "application/json",
            serde_json::to_vec(&spot_config()).unwrap(),
        ),
        (SPOT_CONFIG, false) => ("application/proto", spot_config().encode_to_vec()),
        (ZIPPER_CONFIG, true) => (
            "application/json",
            serde_json::to_vec(&zipper_config()).unwrap(),
        ),
        (ZIPPER_CONFIG, false) => ("application/proto", zipper_config().encode_to_vec()),
        _ => return,
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

#[tokio::test]
async fn deposit_withdraw_config_keeps_variant_supply() {
    let want: Vec<(u32, &str)> = SCALE_0
        .iter()
        .chain(SCALE_8.iter())
        .map(|(id, _, supply)| (*id, *supply))
        .collect();
    let mut failures = Vec::new();
    for wire_format in [WireFormat::Json, WireFormat::Binary] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream));
            }
        });
        let refused = RefusedPort::bind();
        let client = Client::new(Config {
            api_url,
            ws_url: format!("ws://{}/connection/websocket", refused.addr()),
            api_key_id: Some("ak_test".into()),
            api_private_key: Some(TEST_KEY.into()),
            wire_format,
            timeout: Duration::from_secs(5),
            hydrate_catalogs: true,
            allow_insecure_http: true,
            ..Default::default()
        })
        .expect("client");

        let typed = client
            .zipper
            .get_deposit_withdraw_config()
            .await
            .unwrap_or_else(|err| panic!("{wire_format:?}: {err}"));
        let typed = serde_json::to_value(&typed).expect("encode");
        let mut seen = 0;
        for asset in typed["assets"].as_array().expect("assets") {
            for variant in asset["variants"].as_array().expect("variants") {
                seen += 1;
                let id = variant["zipped_asset_id"].as_u64().unwrap() as u32;
                let expected = want.iter().find(|(w, _)| *w == id).unwrap().1;
                if variant["supply"].as_str() != Some(expected) {
                    failures.push(format!(
                        "{wire_format:?}: typed variant {id} supply {}, want {expected}",
                        variant["supply"]
                    ));
                }
                assert_eq!(
                    variant["network_fee"].as_str(),
                    Some("0.0001"),
                    "{wire_format:?}"
                );
                assert_eq!(
                    variant["withdraw_min_amount"].as_str(),
                    Some("0.002"),
                    "{wire_format:?}"
                );
            }
        }
        assert_eq!(seen, want.len(), "{wire_format:?}");

        client.wait_for_catalogs().await.expect("hydrated");
        for (id, expected) in &want {
            let got = client.catalogs.supply_for_zipped_asset_id(*id);
            if got.as_deref() != Some(*expected) {
                failures.push(format!(
                    "{wire_format:?}: catalog variant {id} supply {got:?}, want {expected}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
