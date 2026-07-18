use super::ServiceContext;
use super::scope;
use super::unary;
use crate::codecs::decode::{
    create_deposit_address_from_proto, deposit_addresses_list_from_proto,
    deposit_withdraw_config_from_proto, withdraw_intent_from_proto,
    withdraw_intent_from_wallet_proto,
};
use crate::codecs::scalars::{LEDGER_SCALE, i128_to_u128, u128_to_proto};
use crate::connect::chain::deposit::v1::DepositAddressServiceClient;
use crate::connect::chain::withdraw::v1::WithdrawServiceClient;
use crate::connect::chain::zipper::v1::ZipperServiceClient;
use crate::errors::{Error, Result};
#[cfg(feature = "realtime")]
use crate::models::ZippedAssetSupplyBatch;
use crate::models::{
    CreateTradingWithdrawParams, CreateWalletTradingWithdrawParams, DepositAddress,
    DepositAddressesList, DepositWithdrawConfig, WithdrawIntentResult,
};
use crate::proto::chain::deposit::v1::{CreateDepositAddressRequest, ListDepositAddressesRequest};
use crate::proto::chain::withdraw::v1::{
    CreateTradingWithdrawRequest, CreateWalletTradingWithdrawRequest, TradingWithdrawAction,
    TradingWithdrawIntentPayload,
};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigRequest;
use crate::types::{AssetAmount, QuantityDomain, resolve_asset_amount_scaled};

struct EncodeWithdrawPayload<'a> {
    action: TradingWithdrawAction,
    asset_id: u32,
    amount: &'a AssetAmount,
    amount_scale: Option<u32>,
    idempotency_key: String,
    destination_chain_id: u64,
    destination_address: String,
    deadline_ts_sec: Option<u64>,
    nonce: Option<u128>,
}

#[derive(Clone)]
pub struct DepositService {
    ctx: ServiceContext,
}

impl DepositService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn list_addresses(&self) -> Result<DepositAddressesList> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let req = ListDepositAddressesRequest::default();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/ListDepositAddresses",
            req,
            |req, opts| client.list_deposit_addresses_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(deposit_addresses_list_from_proto(&resp))
    }

    pub async fn create_address(&self, req: CreateDepositAddressRequest) -> Result<DepositAddress> {
        let client = DepositAddressServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/CreateDepositAddress",
            req,
            |req, opts| client.create_deposit_address_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(create_deposit_address_from_proto(&resp))
    }
}

#[derive(Clone)]
pub struct WithdrawService {
    ctx: ServiceContext,
}

impl WithdrawService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub fn connect_client(&self) -> WithdrawServiceClient<crate::transport::SharedTransport> {
        WithdrawServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    fn encode_payload(opts: EncodeWithdrawPayload<'_>) -> Result<TradingWithdrawIntentPayload> {
        if opts.amount_scale.is_some_and(|s| s == 0) {
            return Err(Error::validation("amount_scale must be positive"));
        }
        let scale = opts.amount_scale.unwrap_or(LEDGER_SCALE);
        let scaled = resolve_asset_amount_scaled(
            opts.amount,
            scale,
            QuantityDomain::LedgerE18,
            Some(opts.asset_id),
        )?;
        let mut payload = TradingWithdrawIntentPayload {
            action: opts.action.into(),
            asset_id: opts.asset_id,
            destination_chain_id: opts.destination_chain_id,
            destination_address: opts.destination_address,
            idempotency_key: opts.idempotency_key,
            deadline_ts_sec: opts
                .deadline_ts_sec
                .unwrap_or_else(Self::default_deadline_ts_sec),
            ..Default::default()
        };
        *payload.amount_e18.get_or_insert_default() = i128_to_u128(scaled)?;
        *payload.nonce.get_or_insert_default() =
            u128_to_proto(opts.nonce.unwrap_or_else(Self::default_nonce));
        if payload
            .amount_e18
            .as_option()
            .is_none_or(|u| u.hi == 0 && u.lo == 0)
        {
            return Err(Error::validation("amount must be positive"));
        }
        Ok(payload)
    }

    fn default_deadline_ts_sec() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() + 5 * 60)
            .unwrap_or(0)
    }

    fn default_nonce() -> u128 {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(1);
        if n == 0 { 1 } else { n }
    }

    fn new_idempotency_key() -> String {
        format!(
            "wd-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }

    /// Withdraw from trading to funding. Amount must be an [`crate::types::AssetAmount`].
    pub async fn create_to_funding(
        &self,
        params: CreateTradingWithdrawParams,
    ) -> Result<WithdrawIntentResult> {
        if params.payload_signature.is_empty() {
            return Err(Error::validation(
                "payload_signature is required for trading withdraw",
            ));
        }
        let key = params
            .idempotency_key
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(Self::new_idempotency_key);
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: key,
            destination_chain_id: 0,
            destination_address: params.destination_address,
            deadline_ts_sec: params.deadline_ts_sec,
            nonce: params.nonce,
        })?;
        let mut req = CreateTradingWithdrawRequest {
            payload_signature: params.payload_signature,
            ..Default::default()
        };
        *req.payload.get_or_insert_default() = payload;
        self.create_trading_withdraw(req).await
    }

    /// Withdraw from trading to an external chain. Amount must be an [`AssetAmount`].
    pub async fn create_to_external_chain(
        &self,
        params: CreateTradingWithdrawParams,
        destination_chain_id: u64,
    ) -> Result<WithdrawIntentResult> {
        if params.payload_signature.is_empty() {
            return Err(Error::validation(
                "payload_signature is required for trading withdraw",
            ));
        }
        if params.destination_address.is_empty() {
            return Err(Error::validation(
                "destination_address is required for external-chain withdraw",
            ));
        }
        let key = params
            .idempotency_key
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(Self::new_idempotency_key);
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToExternalChain,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: key,
            destination_chain_id,
            destination_address: params.destination_address,
            deadline_ts_sec: params.deadline_ts_sec,
            nonce: params.nonce,
        })?;
        let mut req = CreateTradingWithdrawRequest {
            payload_signature: params.payload_signature,
            ..Default::default()
        };
        *req.payload.get_or_insert_default() = payload;
        self.create_trading_withdraw(req).await
    }

    async fn create_trading_withdraw(
        &self,
        req: CreateTradingWithdrawRequest,
    ) -> Result<WithdrawIntentResult> {
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.withdraw.v1.WithdrawService/CreateTradingWithdraw",
            req,
            |req, opts| client.create_trading_withdraw_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(withdraw_intent_from_proto(&resp))
    }

    /// Wallet-signed trading withdraw. Amount must be an [`AssetAmount`].
    pub async fn create_wallet_trading_withdraw(
        &self,
        params: CreateWalletTradingWithdrawParams,
    ) -> Result<WithdrawIntentResult> {
        if params.payload_signature.is_empty() {
            return Err(Error::validation(
                "payload_signature is required for trading withdraw",
            ));
        }
        let action = match params
            .action
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str()
        {
            "to_funding" => TradingWithdrawAction::ToFunding,
            "to_external_chain" => TradingWithdrawAction::ToExternalChain,
            _ => TradingWithdrawAction::ActionUnspecified,
        };
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: params.idempotency_key,
            destination_chain_id: params.destination_chain_id,
            destination_address: params.destination_address,
            deadline_ts_sec: params.deadline_ts_sec,
            nonce: params.nonce,
        })?;
        let mut req = CreateWalletTradingWithdrawRequest {
            signer_wallet: params.signer_wallet,
            payload_signature: params.payload_signature,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?
                .unwrap_or(0),
            ..Default::default()
        };
        *req.payload.get_or_insert_default() = payload;
        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.withdraw.v1.WithdrawService/CreateWalletTradingWithdraw",
            req,
            |req, opts| client.create_wallet_trading_withdraw_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(withdraw_intent_from_wallet_proto(&resp))
    }
}

#[derive(Clone)]
pub struct ZipperService {
    ctx: ServiceContext,
}

impl ZipperService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    pub async fn get_deposit_withdraw_config(&self) -> Result<DepositWithdrawConfig> {
        let client = ZipperServiceClient::new(
            self.ctx.factory.transport(false),
            self.ctx.factory.connect_config(false),
        );
        let resp = unary::await_public(
            client.get_deposit_withdraw_config(GetDepositWithdrawConfigRequest::default()),
        )
        .await?
        .into_owned();
        Ok(deposit_withdraw_config_from_proto(&resp))
    }

    /// Subscribe to zipped-asset supply updates (requires `realtime` feature).
    ///
    /// When `patch_catalog` is true, each batch updates
    /// [`crate::catalogs::Manager::patch_zipper_supply`] (`zipped_asset_id` → supply string).
    #[cfg(feature = "realtime")]
    pub async fn subscribe_zipped_asset_supply(
        &self,
        patch_catalog: bool,
    ) -> Result<crate::realtime::TypedSubscription<ZippedAssetSupplyBatch>> {
        let catalogs = self.ctx.catalogs.clone();
        self.ctx
            .realtime
            .subscribe_proto("public:chain:zipped-asset:supply:proto", move |payload| {
                let batch =
                    crate::codecs::decode::zipped_asset_supply_batch_from_bytes(payload, |id| {
                        catalogs.quantity_scale_for_zipped_asset_id(id)
                    })?;
                if patch_catalog {
                    catalogs.patch_zipper_supply(&batch.updates);
                }
                Ok(batch)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa::Message;

    fn encode(amount: &AssetAmount) -> Result<TradingWithdrawIntentPayload> {
        WithdrawService::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: 7,
            amount,
            amount_scale: Some(18),
            idempotency_key: "withdraw-equivalence".into(),
            destination_chain_id: 0,
            destination_address: String::new(),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: Some(42),
        })
    }

    #[test]
    fn decimal_and_scaled_withdraw_amount_encode_identically() {
        let decimal =
            AssetAmount::from_decimal_str("0.5", 18, QuantityDomain::LedgerE18, Some(7)).unwrap();
        let scaled = AssetAmount::from_scaled(
            500_000_000_000_000_000,
            Some(18),
            QuantityDomain::LedgerE18,
            Some(7),
        )
        .unwrap();

        assert_eq!(
            encode(&decimal).unwrap().encode_to_vec(),
            encode(&scaled).unwrap().encode_to_vec()
        );
    }

    #[test]
    fn withdraw_rejects_wrong_amount_domain() {
        let amount =
            AssetAmount::from_scaled(100, Some(18), QuantityDomain::Asset, Some(7)).unwrap();
        assert!(encode(&amount).is_err());
    }

    #[tokio::test]
    async fn withdraw_rejects_missing_signature_before_transport() {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let params = CreateTradingWithdrawParams {
            asset_id: 7,
            amount: AssetAmount::from_scaled(100, Some(18), QuantityDomain::LedgerE18, Some(7))
                .unwrap(),
            payload_signature: Vec::new(),
            destination_address: String::new(),
            idempotency_key: Some("missing-signature".into()),
            amount_scale: Some(18),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: Some(42),
        };
        let err = client.withdraw.create_to_funding(params).await.unwrap_err();
        assert!(err.to_string().contains("payload_signature"));
    }
}
