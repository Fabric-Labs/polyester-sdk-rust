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
use crate::models::ZippedAssetSupplyBatch;
use crate::models::{
    CreateApiKeyTradingWithdrawParams, CreateTradingWithdrawParams,
    CreateWalletTradingWithdrawParams, DepositAddress, DepositAddressesList, DepositWithdrawConfig,
    WithdrawIntentResult,
};
use crate::proto::chain::deposit::v1::{CreateDepositAddressRequest, ListDepositAddressesRequest};
use crate::proto::chain::withdraw::v1::{
    CreateTradingWithdrawRequest, CreateWalletTradingWithdrawRequest, TradingWithdrawAction,
    TradingWithdrawIntentPayload,
};
use crate::proto::chain::zipper::v1::GetDepositWithdrawConfigRequest;
use crate::types::{AssetAmount, QuantityDomain, resolve_asset_amount_scaled_with_input_scale};
use buffa::Message;
use rand_core::{OsRng, RngCore};

/// Generate a cryptographically random withdrawal idempotency key.
///
/// Generate this once per logical withdrawal, persist it with the signed
/// payload, and reuse it unchanged for every retry.
pub fn new_trading_withdraw_idempotency_key() -> Result<String> {
    let mut random = [0_u8; 16];
    OsRng
        .try_fill_bytes(&mut random)
        .map_err(|err| Error::transport(format!("secure randomness unavailable: {err}")))?;
    Ok(format!("wd-{}", hex::encode(random)))
}

/// Generate a cryptographically random, non-zero withdrawal nonce.
///
/// Generate and persist this alongside the idempotency key before signing the
/// payload. The SDK never changes it during submission or retry.
pub fn new_trading_withdraw_nonce() -> Result<u128> {
    for _ in 0..2 {
        let mut random = [0_u8; 16];
        OsRng
            .try_fill_bytes(&mut random)
            .map_err(|err| Error::transport(format!("secure randomness unavailable: {err}")))?;
        let nonce = u128::from_be_bytes(random);
        if nonce != 0 {
            return Ok(nonce);
        }
    }
    Err(Error::transport(
        "secure random source returned a zero withdrawal nonce twice",
    ))
}

struct EncodeWithdrawPayload<'a> {
    action: TradingWithdrawAction,
    asset_id: u32,
    amount: &'a AssetAmount,
    amount_scale: Option<u32>,
    idempotency_key: String,
    destination_chain_id: u64,
    destination_address: String,
    deadline_ts_sec: u64,
    nonce: u128,
}

/// Exact API-key signed withdraw request prepared for durable persistence and retry.
#[derive(Clone)]
pub struct PreparedTradingWithdraw {
    request: CreateTradingWithdrawRequest,
}

impl std::fmt::Debug for PreparedTradingWithdraw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedTradingWithdraw")
            .field("payload", &self.request.payload.as_option())
            .field("payload_signature", &"<redacted>")
            .finish()
    }
}

impl PreparedTradingWithdraw {
    /// Restore a prepared request previously persisted with [`Self::request_bytes`].
    pub fn from_request_bytes(bytes: &[u8]) -> Result<Self> {
        let request = CreateTradingWithdrawRequest::decode_from_slice(bytes)
            .map_err(|err| Error::validation(format!("invalid prepared withdraw bytes: {err}")))?;
        if request.payload.as_option().is_none() {
            return Err(Error::validation(
                "prepared withdraw request is missing payload",
            ));
        }
        if request.payload_signature.is_empty() {
            return Err(Error::validation(
                "prepared withdraw request is missing payload_signature",
            ));
        }
        Ok(Self { request })
    }

    pub fn payload(&self) -> &TradingWithdrawIntentPayload {
        self.request
            .payload
            .as_option()
            .expect("prepared withdraw always has a payload")
    }

    pub fn payload_signature(&self) -> &[u8] {
        &self.request.payload_signature
    }

    /// Exact deterministic protobuf bytes covered by `payload_signature`.
    pub fn deterministic_payload_bytes(&self) -> Vec<u8> {
        self.payload().encode_to_vec()
    }

    /// Canonical bytes to persist before first submission and reuse on retries.
    pub fn request_bytes(&self) -> Vec<u8> {
        self.request.encode_to_vec()
    }
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
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
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
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        );
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/chain.deposit.v1.DepositAddressService/CreateDepositAddress",
            req,
            |req, opts| client.create_deposit_address_with_options(req, opts),
        )
        .await?
        .into_owned();
        create_deposit_address_from_proto(&resp)
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

    pub(crate) fn connect_client(
        &self,
    ) -> WithdrawServiceClient<crate::transport::SharedTransport> {
        WithdrawServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    fn encode_payload(opts: EncodeWithdrawPayload<'_>) -> Result<TradingWithdrawIntentPayload> {
        if opts.idempotency_key.trim().is_empty() {
            return Err(Error::validation("idempotency_key is required"));
        }
        if opts.deadline_ts_sec == 0 {
            return Err(Error::validation("deadline_ts_sec must be non-zero"));
        }
        if opts.nonce == 0 {
            return Err(Error::validation("nonce must be non-zero"));
        }
        let scaled = resolve_asset_amount_scaled_with_input_scale(
            opts.amount,
            opts.amount_scale,
            LEDGER_SCALE,
            QuantityDomain::LedgerE18,
            Some(opts.asset_id),
        )?;
        let mut payload = TradingWithdrawIntentPayload {
            action: opts.action.into(),
            asset_id: opts.asset_id,
            destination_chain_id: opts.destination_chain_id,
            destination_address: opts.destination_address,
            idempotency_key: opts.idempotency_key,
            deadline_ts_sec: opts.deadline_ts_sec,
            ..Default::default()
        };
        *payload.amount_e18.get_or_insert_default() = i128_to_u128(scaled)?;
        *payload.nonce.get_or_insert_default() = u128_to_proto(opts.nonce);
        if payload
            .amount_e18
            .as_option()
            .is_none_or(|u| u.hi == 0 && u.lo == 0)
        {
            return Err(Error::validation("amount must be positive"));
        }
        Ok(payload)
    }

    fn default_deadline_ts_sec() -> Result<u64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::validation("system clock is before UNIX_EPOCH"))?
            .as_secs();
        now.checked_add(5 * 60)
            .ok_or_else(|| Error::validation("withdraw deadline overflow"))
    }

    fn prepare_api_key(
        &self,
        action: TradingWithdrawAction,
        params: CreateApiKeyTradingWithdrawParams,
        destination_chain_id: u64,
    ) -> Result<PreparedTradingWithdraw> {
        if action == TradingWithdrawAction::ToExternalChain
            && params.destination_address.trim().is_empty()
        {
            return Err(Error::validation(
                "destination_address is required for external-chain withdraw",
            ));
        }
        let deadline_ts_sec = match params.deadline_ts_sec {
            Some(deadline) => deadline,
            None => Self::default_deadline_ts_sec()?,
        };
        let nonce = match params.nonce {
            Some(nonce) => nonce,
            None => new_trading_withdraw_nonce()?,
        };
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: params.idempotency_key,
            destination_chain_id,
            destination_address: params.destination_address,
            deadline_ts_sec,
            nonce,
        })?;
        let payload_signature = self
            .ctx
            .factory
            .require_credentials()?
            .sign_payload(&payload.encode_to_vec());
        let mut request = CreateTradingWithdrawRequest {
            payload_signature,
            ..Default::default()
        };
        *request.payload.get_or_insert_default() = payload;
        Ok(PreparedTradingWithdraw { request })
    }

    /// Build and API-key sign a complete Trading-to-Funding payload.
    ///
    /// Persist the returned value before submission and reuse it unchanged
    /// after an outcome-unknown transport error.
    pub fn prepare_api_key_to_funding(
        &self,
        params: CreateApiKeyTradingWithdrawParams,
    ) -> Result<PreparedTradingWithdraw> {
        self.prepare_api_key(TradingWithdrawAction::ToFunding, params, 0)
    }

    /// Build and API-key sign a complete Trading-to-external-chain payload.
    pub fn prepare_api_key_to_external_chain(
        &self,
        params: CreateApiKeyTradingWithdrawParams,
        destination_chain_id: u64,
    ) -> Result<PreparedTradingWithdraw> {
        self.prepare_api_key(
            TradingWithdrawAction::ToExternalChain,
            params,
            destination_chain_id,
        )
    }

    /// Submit an already prepared request without rebuilding signed fields.
    pub async fn submit_prepared(
        &self,
        prepared: &PreparedTradingWithdraw,
    ) -> Result<WithdrawIntentResult> {
        self.create_trading_withdraw(prepared.request.clone()).await
    }

    /// Build, API-key sign, and submit a Trading-to-Funding payload unchanged.
    pub async fn create_api_key_to_funding(
        &self,
        params: CreateApiKeyTradingWithdrawParams,
    ) -> Result<WithdrawIntentResult> {
        let prepared = self.prepare_api_key_to_funding(params)?;
        self.submit_prepared(&prepared).await
    }

    /// Build, API-key sign, and submit a Trading-to-external-chain payload unchanged.
    pub async fn create_api_key_to_external_chain(
        &self,
        params: CreateApiKeyTradingWithdrawParams,
        destination_chain_id: u64,
    ) -> Result<WithdrawIntentResult> {
        let prepared = self.prepare_api_key_to_external_chain(params, destination_chain_id)?;
        self.submit_prepared(&prepared).await
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
        let deadline_ts_sec = params.deadline_ts_sec.ok_or_else(|| {
            Error::validation("deadline_ts_sec is required when payload_signature is precomputed")
        })?;
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: params.idempotency_key,
            destination_chain_id: 0,
            destination_address: params.destination_address,
            deadline_ts_sec,
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
        let deadline_ts_sec = params.deadline_ts_sec.ok_or_else(|| {
            Error::validation("deadline_ts_sec is required when payload_signature is precomputed")
        })?;
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToExternalChain,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: params.idempotency_key,
            destination_chain_id,
            destination_address: params.destination_address,
            deadline_ts_sec,
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
        withdraw_intent_from_proto(&resp)
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
            _ => {
                return Err(Error::validation(format!(
                    "unknown trading withdraw action: {}",
                    params.action
                )));
            }
        };
        let deadline_ts_sec = params.deadline_ts_sec.ok_or_else(|| {
            Error::validation("deadline_ts_sec is required when payload_signature is precomputed")
        })?;
        let payload = Self::encode_payload(EncodeWithdrawPayload {
            action,
            asset_id: params.asset_id,
            amount: &params.amount,
            amount_scale: params.amount_scale,
            idempotency_key: params.idempotency_key,
            destination_chain_id: params.destination_chain_id,
            destination_address: params.destination_address,
            deadline_ts_sec,
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
        withdraw_intent_from_wallet_proto(&resp)
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
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
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
            deadline_ts_sec: 1_800_000_000,
            nonce: 42,
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

    #[test]
    fn withdraw_rejects_missing_amount_scale_before_transport() {
        let amount = AssetAmount::from_scaled(1, None, QuantityDomain::LedgerE18, Some(7)).unwrap();
        let err = WithdrawService::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: 7,
            amount: &amount,
            amount_scale: None,
            idempotency_key: "missing-scale".into(),
            destination_chain_id: 0,
            destination_address: String::new(),
            deadline_ts_sec: 1_800_000_000,
            nonce: 42,
        })
        .expect_err("missing scale must not silently mean e18");
        assert!(err.to_string().contains("amount scale is required"));
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
            idempotency_key: "missing-signature".into(),
            amount_scale: Some(18),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: 42,
        };
        let err = client.withdraw.create_to_funding(params).await.unwrap_err();
        assert!(err.to_string().contains("payload_signature"));
    }

    #[test]
    fn withdraw_rejects_empty_idempotency_key() {
        let amount =
            AssetAmount::from_scaled(100, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
        let err = WithdrawService::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: 7,
            amount: &amount,
            amount_scale: Some(18),
            idempotency_key: "  ".into(),
            destination_chain_id: 0,
            destination_address: String::new(),
            deadline_ts_sec: 1_800_000_000,
            nonce: 42,
        })
        .unwrap_err();
        assert!(err.to_string().contains("idempotency_key"));
    }

    #[test]
    fn withdraw_rejects_zero_nonce() {
        let amount =
            AssetAmount::from_scaled(100, Some(18), QuantityDomain::LedgerE18, Some(7)).unwrap();
        let err = WithdrawService::encode_payload(EncodeWithdrawPayload {
            action: TradingWithdrawAction::ToFunding,
            asset_id: 7,
            amount: &amount,
            amount_scale: Some(18),
            idempotency_key: "stable-withdraw".into(),
            destination_chain_id: 0,
            destination_address: String::new(),
            deadline_ts_sec: 1_800_000_000,
            nonce: 0,
        })
        .unwrap_err();
        assert!(err.to_string().contains("nonce"));
    }

    #[test]
    fn withdrawal_generators_return_explicit_unique_values() {
        let first_key = new_trading_withdraw_idempotency_key().unwrap();
        let second_key = new_trading_withdraw_idempotency_key().unwrap();
        assert!(first_key.starts_with("wd-"));
        assert_eq!(first_key.len(), 35);
        assert_ne!(first_key, second_key);
        assert_ne!(new_trading_withdraw_nonce().unwrap(), 0);
    }

    fn signing_client(seed_hex: &str) -> crate::Client {
        crate::Client::new(crate::Config {
            api_key_id: Some("withdraw-test-key".into()),
            api_private_key: Some(seed_hex.into()),
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap()
    }

    fn api_key_params(amount: AssetAmount) -> CreateApiKeyTradingWithdrawParams {
        CreateApiKeyTradingWithdrawParams {
            asset_id: 7,
            amount,
            destination_address: String::new(),
            idempotency_key: "prepared-withdraw".into(),
            amount_scale: Some(2),
            deadline_ts_sec: Some(1_800_000_000),
            nonce: Some(42),
        }
    }

    #[test]
    fn prepared_api_key_withdraw_retains_deadline_rescales_e18_and_signs_exact_bytes() {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};

        let seed = [7_u8; 32];
        let client = signing_client(&hex::encode(seed));
        let amount =
            AssetAmount::from_scaled(125, Some(2), QuantityDomain::LedgerE18, Some(7)).unwrap();
        let prepared = client
            .withdraw
            .prepare_api_key_to_funding(api_key_params(amount))
            .unwrap();
        let payload = prepared.payload();

        assert_eq!(payload.deadline_ts_sec, 1_800_000_000);
        let amount = payload.amount_e18.as_option().unwrap();
        assert_eq!(
            (u128::from(amount.hi) << 64) | u128::from(amount.lo),
            1_250_000_000_000_000_000
        );
        let verifying_key = VerifyingKey::from(&ed25519_dalek::SigningKey::from_bytes(&seed));
        let signature = Signature::from_slice(prepared.payload_signature()).unwrap();
        verifying_key
            .verify(&prepared.deterministic_payload_bytes(), &signature)
            .unwrap();
        let restored =
            PreparedTradingWithdraw::from_request_bytes(&prepared.request_bytes()).unwrap();
        assert_eq!(restored.request_bytes(), prepared.request_bytes());

        let identical = client
            .withdraw
            .prepare_api_key_to_funding(api_key_params(
                AssetAmount::from_scaled(125, Some(2), QuantityDomain::LedgerE18, Some(7)).unwrap(),
            ))
            .unwrap();
        assert_eq!(
            prepared.deterministic_payload_bytes(),
            identical.deterministic_payload_bytes()
        );
        assert_eq!(prepared.payload_signature(), identical.payload_signature());
    }

    #[tokio::test]
    async fn precomputed_signature_path_rejects_missing_deadline() {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let err = client
            .withdraw
            .create_to_funding(CreateTradingWithdrawParams {
                asset_id: 7,
                amount: AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7))
                    .unwrap(),
                payload_signature: vec![1],
                destination_address: String::new(),
                idempotency_key: "missing-deadline".into(),
                amount_scale: Some(18),
                deadline_ts_sec: None,
                nonce: 42,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("deadline_ts_sec"));
    }

    #[tokio::test]
    async fn wallet_withdraw_rejects_unknown_action_as_validation() {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let err = client
            .withdraw
            .create_wallet_trading_withdraw(CreateWalletTradingWithdrawParams {
                action: "future_action".into(),
                asset_id: 7,
                amount: AssetAmount::from_scaled(1, Some(18), QuantityDomain::LedgerE18, Some(7))
                    .unwrap(),
                idempotency_key: "unknown-action".into(),
                payload_signature: vec![1],
                signer_wallet: "0x1".into(),
                destination_chain_id: 0,
                destination_address: String::new(),
                subaccount_id: None,
                amount_scale: Some(18),
                deadline_ts_sec: Some(1_800_000_000),
                nonce: 42,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("unknown trading withdraw action"));
    }
}
