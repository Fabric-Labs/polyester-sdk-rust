use buffa::Message;
use polyester::codecs::decode::{equity_history_from_proto, transfers_list_from_proto};
use polyester::proto::ledger::read::v1::{
    EquitySeries, GetEquityHistorySeriesResponse, ListTransfersResponse, TransferRow,
};

#[test]
fn read_transfer_opaque_token() {
    for token in ["eyJ2IjoxfQ==", "000123", "0", ""] {
        let raw = ListTransfersResponse {
            transfers: vec![TransferRow {
                asset_id: 1,
                ts_us: 999,
                ..Default::default()
            }],
            next_page_token: token.into(),
            ..Default::default()
        };
        let msg = ListTransfersResponse::decode_from_slice(&raw.encode_to_vec()).unwrap();
        let json: ListTransfersResponse =
            serde_json::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(
            transfers_list_from_proto(&json),
            transfers_list_from_proto(&msg)
        );
        let result = transfers_list_from_proto(&msg);
        assert_eq!(result.transfers.len(), 1);
        assert_eq!(result.transfers[0].timestamp, 999);
        assert_eq!(result.next_page_token, token);
    }
}

#[test]
fn read_equity_btc_prices() {
    for prices in [vec![60000000000001, 61000000000009], vec![]] {
        let raw = GetEquityHistorySeriesResponse {
            start_ts_sec: 100,
            end_ts_sec: 400,
            bucket: "5m".into(),
            points: 2,
            series: vec![EquitySeries {
                equity_q: vec![12345, -6789],
                ..Default::default()
            }],
            btc_prices_q: prices.clone(),
            ..Default::default()
        };
        let msg = GetEquityHistorySeriesResponse::decode_from_slice(&raw.encode_to_vec()).unwrap();
        let json: GetEquityHistorySeriesResponse =
            serde_json::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(
            equity_history_from_proto(&json),
            equity_history_from_proto(&msg)
        );
        let result = equity_history_from_proto(&msg);
        assert_eq!(result.series[0].equity_q, vec![12345, -6789]);
        assert_eq!(result.btc_prices_q, prices);
        assert_eq!(result.points, 2);
        assert_eq!((result.start_ts_sec, result.end_ts_sec), (100, 400));
    }
}
