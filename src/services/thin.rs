//! Thin service shells that expose generated Connect clients for full surface parity.

use super::ServiceContext;

macro_rules! thin_service {
    ($name:ident, $client:ty, $auth:expr) => {
        #[derive(Clone)]
        pub struct $name {
            pub(crate) ctx: ServiceContext,
        }
        impl $name {
            pub fn new(ctx: ServiceContext) -> Self {
                Self { ctx }
            }
            pub fn connect_client(&self) -> $client {
                <$client>::new(
                    self.ctx.factory.transport($auth),
                    self.ctx.factory.connect_config($auth),
                )
            }
        }
    };
}

thin_service!(
    ChainAnalyticsService,
    crate::connect::chain::analytics::v1::ChainAnalyticsServiceClient<
        crate::transport::SharedTransport,
    >,
    false
);
thin_service!(
    LifecycleService,
    crate::connect::chain::lifecycle::v1::LifecycleReadServiceClient<
        crate::transport::SharedTransport,
    >,
    true
);
thin_service!(
    HeatmapService,
    crate::connect::marketdata::v1::HeatmapServiceClient<crate::transport::SharedTransport>,
    false
);
thin_service!(
    PoliciesService,
    crate::connect::auth::v1::PolicyServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    SubAccountsService,
    crate::connect::auth::v1::SubaccountServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    ResolveService,
    crate::connect::auth::v1::ResolveServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    AddressBookService,
    crate::connect::auth::v1::AddressBookServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    SocialVerificationService,
    crate::connect::auth::v1::SocialVerificationServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    WhiteboardService,
    crate::connect::collab::v1::WhiteboardServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    PolychartService,
    crate::connect::polychart::v1::PolychartServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    LayoutService,
    crate::connect::layout::v1::LayoutServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    GuardSignerService,
    crate::connect::chain::guard::v1::GuardSignerServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    InternalTransfersService,
    crate::connect::transfer::v1::InternalTransferServiceClient<crate::transport::SharedTransport>,
    true
);
thin_service!(
    TransfersService,
    crate::connect::ledger::read::v1::LedgerReadServiceClient<crate::transport::SharedTransport>,
    true
);

impl HeatmapService {
    /// Historical orderbook heatmap (Go `HeatmapService.Get` → `ApiData`).
    pub async fn get(
        &self,
        symbol: &str,
        interval: &str,
        depth: u32,
        limit: u32,
        quantity_mode: &str,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        use crate::codecs::heatmap::{
            heatmap_depth_for_levels, resolve_heatmap_interval, resolve_heatmap_quantity_mode,
        };
        use crate::errors::Error;
        use crate::proto::marketdata::v1::{GetOrderbookHeatmapRequest, HeatmapTimeRange};
        use buffa_types::google::protobuf::Timestamp;
        use std::time::{SystemTime, UNIX_EPOCH};

        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        let interval_enum = resolve_heatmap_interval(interval)?;
        let qty_mode = resolve_heatmap_quantity_mode(quantity_mode)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let end = Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
            ..Default::default()
        };
        let start = Timestamp {
            seconds: end.seconds.saturating_sub(300),
            nanos: end.nanos,
            ..Default::default()
        };
        let req = GetOrderbookHeatmapRequest {
            symbol_id,
            interval: interval_enum.into(),
            depth: heatmap_depth_for_levels(depth).into(),
            time_range: HeatmapTimeRange {
                start_time: start.into(),
                end_time: end.into(),
                ..Default::default()
            }
            .into(),
            limit: if limit == 0 { 100 } else { limit },
            quantity_mode: qty_mode.into(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = super::unary::await_public(client.get_orderbook_heatmap(req))
            .await?
            .into_owned();
        Ok(api_data_from_proto(&resp))
    }

    /// Subscribe to live heatmap buckets (requires `realtime` feature + hydrated catalogs).
    pub async fn subscribe_live(
        &self,
        symbol: &str,
        interval: &str,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::ApiData>> {
        use crate::codecs::heatmap::heatmap_interval_channel_name;
        use crate::errors::Error;
        let symbol_id = self
            .ctx
            .catalogs
            .symbol_id_for_symbol(symbol)
            .ok_or_else(|| {
                Error::validation(format!(
                    "unknown symbol {symbol}; call hydrate_catalogs / get_spot_config first"
                ))
            })?;
        // Validate interval aliases.
        crate::codecs::heatmap::resolve_heatmap_interval(interval)?;
        let interval_name = heatmap_interval_channel_name(interval);
        let channel = format!("public:spot:market:heatmap:{interval_name}:{symbol_id}:proto");
        self.ctx
            .realtime
            .subscribe_proto(
                &channel,
                crate::codecs::decode::heatmap_live_bucket_from_bytes,
            )
            .await
    }
}

impl InternalTransfersService {
    /// Create an internal transfer. Quantity must be an [`crate::types::AssetAmount`].
    pub async fn create(
        &self,
        params: crate::models::CreateInternalTransferParams,
    ) -> crate::errors::Result<crate::models::InternalTransferResult> {
        use super::scope;
        use super::unary;
        use crate::codecs::decode::internal_transfer_from_proto;
        use crate::codecs::scalars::{LEDGER_SCALE, i128_to_u128, id_to_u64};
        use crate::errors::Error;
        use crate::proto::transfer::v1::{
            CreateInternalTransferRequest, create_internal_transfer_request::Destination,
        };
        use crate::types::{QuantityDomain, resolve_asset_amount_scaled};

        let has_account = params
            .destination_account_id
            .as_ref()
            .is_some_and(|s| !s.is_empty());
        let has_sub = params
            .destination_subaccount_id
            .as_ref()
            .is_some_and(|s| !s.is_empty());
        let has_smart = params
            .destination_smart_account_address
            .as_ref()
            .is_some_and(|s| !s.is_empty());
        if !has_account && !has_sub && !has_smart {
            return Err(Error::validation(
                "create requires destination_account_id, destination_subaccount_id, or destination_smart_account_address",
            ));
        }

        let scale = params.quantity_scale.unwrap_or(LEDGER_SCALE);
        let scaled = resolve_asset_amount_scaled(
            &params.quantity,
            scale,
            QuantityDomain::LedgerE18,
            Some(params.asset_id),
        )?;
        let mut req = CreateInternalTransferRequest {
            asset_id: params.asset_id,
            idempotency_key: params.idempotency_key,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?
                .unwrap_or(0),
            ..Default::default()
        };
        *req.amount_e18.get_or_insert_default() = i128_to_u128(scaled)?;
        if has_account {
            req.destination = Some(Destination::DestinationAccountId(id_to_u64(
                params.destination_account_id.as_deref().unwrap(),
                "destination_account_id",
            )?));
        } else if has_sub {
            req.destination = Some(Destination::DestinationSubaccountId(id_to_u64(
                params.destination_subaccount_id.as_deref().unwrap(),
                "destination_subaccount_id",
            )?));
        } else {
            req.destination = Some(Destination::DestinationSmartAccountAddress(
                params.destination_smart_account_address.unwrap_or_default(),
            ));
        }

        let client = self.connect_client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.InternalTransferService/CreateInternalTransfer",
            req,
            |req, opts| client.create_internal_transfer_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(internal_transfer_from_proto(&resp))
    }
}

impl PoliciesService {
    /// Subscribe to private subaccount policy updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::SubaccountPolicy>>
    {
        self.subscribe_subaccount_policies(account_id).await
    }

    /// Subscribe to private subaccount policy updates (requires `realtime` feature).
    pub async fn subscribe_subaccount_policies(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::SubaccountPolicy>>
    {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:subaccount-policies:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(
                &channel,
                crate::codecs::decode::subaccount_policy_from_bytes,
            )
            .await
    }

    /// Subscribe to private API-key policy updates (requires `realtime` feature).
    pub async fn subscribe_api_policies(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::ApiPolicy>> {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:api-policies:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::api_policy_from_bytes)
            .await
    }

    pub async fn list_subaccount_policies(
        &self,
        req: crate::proto::auth::v1::ListSubaccountPoliciesRequest,
    ) -> crate::errors::Result<crate::models::SubaccountPoliciesList> {
        use crate::codecs::decode::subaccount_policies_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/ListSubaccountPolicies",
                req,
                |req, opts| client.list_subaccount_policies_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(subaccount_policies_list_from_proto(&resp))
    }

    pub async fn list_api_policies(
        &self,
        req: crate::proto::auth::v1::ListApiPoliciesRequest,
    ) -> crate::errors::Result<crate::models::ApiPoliciesList> {
        use crate::codecs::decode::api_policies_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/ListApiPolicies",
                req,
                |req, opts| client.list_api_policies_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_policies_list_from_proto(&resp))
    }

    pub async fn get_subaccount_policy(
        &self,
        policy_id: &str,
    ) -> crate::errors::Result<Option<crate::models::SubaccountPolicy>> {
        use crate::codecs::decode::get_subaccount_policy_from_proto;
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::GetSubaccountPolicyRequest;
        let req = GetSubaccountPolicyRequest {
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/GetSubaccountPolicy",
                req,
                |req, opts| client.get_subaccount_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(get_subaccount_policy_from_proto(&resp))
    }

    pub async fn create_subaccount_policy(
        &self,
        req: crate::proto::auth::v1::CreateSubaccountPolicyRequest,
    ) -> crate::errors::Result<Option<crate::models::SubaccountPolicy>> {
        use crate::codecs::decode::create_subaccount_policy_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/CreateSubaccountPolicy",
                req,
                |req, opts| client.create_subaccount_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(create_subaccount_policy_from_proto(&resp))
    }

    pub async fn update_subaccount_policy(
        &self,
        req: crate::proto::auth::v1::UpdateSubaccountPolicyRequest,
    ) -> crate::errors::Result<Option<crate::models::SubaccountPolicy>> {
        use crate::codecs::decode::update_subaccount_policy_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/UpdateSubaccountPolicy",
                req,
                |req, opts| client.update_subaccount_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(update_subaccount_policy_from_proto(&resp))
    }

    pub async fn delete_subaccount_policy(&self, policy_id: &str) -> crate::errors::Result<()> {
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::DeleteSubaccountPolicyRequest;
        let req = DeleteSubaccountPolicyRequest {
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/DeleteSubaccountPolicy",
                req,
                |req, opts| client.delete_subaccount_policy_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn set_subaccount_policy(
        &self,
        subaccount_id: u64,
        policy_id: &str,
    ) -> crate::errors::Result<()> {
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::SetSubaccountPolicyRequest;
        let req = SetSubaccountPolicyRequest {
            subaccount_id,
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/SetSubaccountPolicy",
                req,
                |req, opts| client.set_subaccount_policy_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn get_api_policy(
        &self,
        policy_id: &str,
    ) -> crate::errors::Result<Option<crate::models::ApiPolicy>> {
        use crate::codecs::decode::get_api_policy_from_proto;
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::GetApiPolicyRequest;
        let req = GetApiPolicyRequest {
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/GetApiPolicy",
                req,
                |req, opts| client.get_api_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(get_api_policy_from_proto(&resp))
    }

    pub async fn create_api_policy(
        &self,
        req: crate::proto::auth::v1::CreateApiPolicyRequest,
    ) -> crate::errors::Result<Option<crate::models::ApiPolicy>> {
        use crate::codecs::decode::create_api_policy_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/CreateApiPolicy",
                req,
                |req, opts| client.create_api_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(create_api_policy_from_proto(&resp))
    }

    pub async fn update_api_policy(
        &self,
        req: crate::proto::auth::v1::UpdateApiPolicyRequest,
    ) -> crate::errors::Result<Option<crate::models::ApiPolicy>> {
        use crate::codecs::decode::update_api_policy_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/UpdateApiPolicy",
                req,
                |req, opts| client.update_api_policy_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(update_api_policy_from_proto(&resp))
    }

    pub async fn delete_api_policy(&self, policy_id: &str) -> crate::errors::Result<()> {
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::DeleteApiPolicyRequest;
        let req = DeleteApiPolicyRequest {
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/DeleteApiPolicy",
                req,
                |req, opts| client.delete_api_policy_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn set_api_key_policy(
        &self,
        key_id: &str,
        policy_id: &str,
    ) -> crate::errors::Result<()> {
        use crate::codecs::scalars::id_to_u64;
        use crate::proto::auth::v1::SetApiKeyPolicyRequest;
        let req = SetApiKeyPolicyRequest {
            key_id: key_id.to_owned(),
            policy_id: id_to_u64(policy_id, "policy_id")?,
            ..Default::default()
        };
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.PolicyService/SetApiKeyPolicy",
                req,
                |req, opts| client.set_api_key_policy_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetSubaccountOpts {
    pub include_api_keys: bool,
    pub include_members: bool,
    pub include_invites: bool,
    pub include_policy: bool,
    pub include_balances: bool,
    pub invites_direction: String,
}

/// Presence-aware patch fields for [`SubAccountsService::update`].
///
/// `None` omits the field from the update mask; `Some(value)` selects it
/// (including empty string clears when selected).
#[derive(Debug, Clone, Default)]
pub struct UpdateSubaccountParams {
    pub label: Option<String>,
    pub icon: Option<String>,
    pub color: Option<String>,
    pub status: Option<String>,
}

impl SubAccountsService {
    /// Subscribe to private subaccount updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::SubAccount>> {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:subaccounts:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::subaccount_from_bytes)
            .await
    }

    /// Subscribe to private API key updates for an account (requires `realtime` feature).
    pub async fn subscribe_api_keys(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::ApiKeySummary>>
    {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:api-keys:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::api_key_from_bytes)
            .await
    }

    pub async fn list(
        &self,
        req: crate::proto::auth::v1::ListSubaccountsRequest,
    ) -> crate::errors::Result<crate::models::SubAccountsList> {
        use crate::codecs::decode::subaccounts_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/ListSubaccounts",
                req,
                |req, opts| client.list_subaccounts_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(subaccounts_list_from_proto(&resp))
    }

    pub async fn get(
        &self,
        subaccount_id: u64,
        opts: GetSubaccountOpts,
    ) -> crate::errors::Result<crate::models::GetSubaccountResult> {
        use crate::codecs::decode::get_subaccount_from_proto;
        use crate::proto::auth::v1::GetSubaccountRequest;
        let req = GetSubaccountRequest {
            subaccount_id,
            include_api_keys: opts.include_api_keys,
            include_members: opts.include_members,
            include_invites: opts.include_invites,
            include_policy: opts.include_policy,
            include_balances: opts.include_balances,
            invites_direction: opts.invites_direction,
            ..Default::default()
        };
        let client = crate::connect::auth::v1::SubaccountViewServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountViewService/GetSubaccount",
                req,
                |req, opts| client.get_subaccount_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(get_subaccount_from_proto(&resp))
    }

    pub async fn create(
        &self,
        req: crate::proto::auth::v1::CreateSubaccountRequest,
    ) -> crate::errors::Result<crate::models::CreateSubaccountResult> {
        use crate::codecs::decode::create_subaccount_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/CreateSubaccount",
                req,
                |req, opts| client.create_subaccount_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(create_subaccount_from_proto(&resp))
    }

    pub async fn update(
        &self,
        subaccount_id: u64,
        expected_revision: u64,
        params: UpdateSubaccountParams,
    ) -> crate::errors::Result<Option<crate::models::SubAccount>> {
        use crate::codecs::decode::subaccount_from_proto;
        let req = build_update_subaccount_request(subaccount_id, expected_revision, params)?;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/UpdateSubaccount",
                req,
                |req, opts| client.update_subaccount_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(resp.subaccount.as_option().map(subaccount_from_proto))
    }

    /// Soft-delete by setting status to `"deleted"` (Go `Delete` parity).
    pub async fn delete(
        &self,
        subaccount_id: u64,
        expected_revision: u64,
    ) -> crate::errors::Result<Option<crate::models::SubAccount>> {
        self.update(
            subaccount_id,
            expected_revision,
            UpdateSubaccountParams {
                status: Some("deleted".into()),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn set_member_mfa_requirement(
        &self,
        req: crate::proto::auth::v1::SetSubaccountMemberMFARequirementRequest,
    ) -> crate::errors::Result<()> {
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/SetSubaccountMemberMFARequirement",
                req,
                |req, opts| client.set_subaccount_member_mfa_requirement_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn list_members(
        &self,
        req: crate::proto::auth::v1::ListSubaccountMembersRequest,
    ) -> crate::errors::Result<crate::models::SubAccountMembersList> {
        use crate::codecs::decode::subaccount_members_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/ListSubaccountMembers",
                req,
                |req, opts| client.list_subaccount_members_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(subaccount_members_list_from_proto(&resp))
    }

    pub async fn remove_member(
        &self,
        req: crate::proto::auth::v1::RemoveSubaccountMemberRequest,
    ) -> crate::errors::Result<()> {
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/RemoveSubaccountMember",
                req,
                |req, opts| client.remove_subaccount_member_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn update_member_role(
        &self,
        req: crate::proto::auth::v1::UpdateSubaccountMemberRoleRequest,
    ) -> crate::errors::Result<()> {
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/UpdateSubaccountMemberRole",
                req,
                |req, opts| client.update_subaccount_member_role_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn invite_member(
        &self,
        req: crate::proto::auth::v1::InviteSubaccountMemberRequest,
    ) -> crate::errors::Result<Option<crate::models::SubAccountInvite>> {
        use crate::codecs::decode::invite_subaccount_member_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/InviteSubaccountMember",
                req,
                |req, opts| client.invite_subaccount_member_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(invite_subaccount_member_from_proto(&resp))
    }

    pub async fn list_invites(
        &self,
        req: crate::proto::auth::v1::ListSubaccountInvitesRequest,
    ) -> crate::errors::Result<crate::models::SubAccountInvitesList> {
        use crate::codecs::decode::subaccount_invites_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/ListSubaccountInvites",
                req,
                |req, opts| client.list_subaccount_invites_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(subaccount_invites_list_from_proto(&resp))
    }

    pub async fn respond_invite(
        &self,
        req: crate::proto::auth::v1::RespondSubaccountInviteRequest,
    ) -> crate::errors::Result<Option<crate::models::SubAccountInvite>> {
        use crate::codecs::decode::respond_subaccount_invite_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountService/RespondSubaccountInvite",
                req,
                |req, opts| client.respond_subaccount_invite_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(respond_subaccount_invite_from_proto(&resp))
    }

    pub async fn list_activity(
        &self,
        req: crate::proto::auth::v1::ListSubaccountEventsRequest,
    ) -> crate::errors::Result<crate::models::SubAccountActivityList> {
        use crate::codecs::decode::subaccount_activity_list_from_proto;
        let client = crate::connect::auth::v1::SubaccountViewServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        );
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SubaccountViewService/ListSubaccountActivity",
                req,
                |req, opts| client.list_subaccount_activity_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(subaccount_activity_list_from_proto(&resp))
    }
}

impl ResolveService {
    pub async fn resolve_account(
        &self,
        req: crate::proto::auth::v1::ResolveAccountRequest,
    ) -> crate::errors::Result<crate::models::ResolvedAccountsList> {
        use crate::codecs::decode::resolved_accounts_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.ResolveService/ResolveAccount",
                req,
                |req, opts| client.resolve_account_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(resolved_accounts_from_proto(&resp))
    }
}

/// Presence-aware patch fields for [`AddressBookService::update_entry`].
///
/// `None` omits the field from the update mask; `Some(value)` selects it
/// (including empty string / empty `tag_ids` clears when selected).
#[derive(Debug, Clone, Default)]
pub struct UpdateAddressBookEntryParams {
    pub label: Option<String>,
    pub note: Option<String>,
    pub tag_ids: Option<Vec<u64>>,
}

/// Optional scalar fields for [`AddressBookService::update_tag`].
///
/// `None` omits the field; `Some(value)` sets it. Empty `color` clears color.
/// Empty `name` is rejected (names cannot be cleared to blank).
#[derive(Debug, Clone, Default)]
pub struct UpdateAddressBookTagParams {
    pub name: Option<String>,
    pub color: Option<String>,
}

/// Presence-aware patch fields for common durable-auth subaccount policy updates.
///
/// Covers the fields exercised by Go/Python SDK tests without a full policy surface.
/// `None` omits; `Some(value)` selects (including zero/false/empty clears).
/// Timestamps: `None` omits; `Some(None)` clears; `Some(Some(ts))` sets.
#[derive(Debug, Clone, Default)]
pub struct UpdateSubaccountPolicyParams {
    pub name: Option<String>,
    pub description: Option<String>,
    pub actions: Option<Vec<crate::proto::auth::v1::PolicyAction>>,
    pub spot_markets: Option<Vec<String>>,
    pub trading_halted: Option<bool>,
    pub global_notional_cap: Option<u64>,
    pub max_order_notional: Option<u64>,
    pub review_at: Option<Option<buffa_types::google::protobuf::Timestamp>>,
    pub expires_at: Option<Option<buffa_types::google::protobuf::Timestamp>>,
}

/// Presence-aware patch fields for common durable-auth API policy updates.
#[derive(Debug, Clone, Default)]
pub struct UpdateApiPolicyParams {
    pub name: Option<String>,
    pub description: Option<String>,
    pub actions: Option<Vec<crate::proto::auth::v1::PolicyAction>>,
    pub max_order_notional: Option<u64>,
}

impl AddressBookService {
    /// Subscribe to address-book view invalidations (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<
        crate::realtime::TypedSubscription<crate::models::AddressBookViewInvalidation>,
    > {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:address-books:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(
                &channel,
                crate::codecs::decode::address_book_invalidation_from_bytes,
            )
            .await
    }

    /// Alias for [`Self::subscribe`] (Go `SubscribeViewInvalidations` parity).
    pub async fn subscribe_view_invalidations(
        &self,
        root_account_public_id: Option<&str>,
    ) -> crate::errors::Result<
        crate::realtime::TypedSubscription<crate::models::AddressBookViewInvalidation>,
    > {
        self.subscribe(root_account_public_id).await
    }

    pub async fn list_books(
        &self,
        req: crate::proto::auth::v1::ListAddressBooksRequest,
    ) -> crate::errors::Result<crate::models::AddressBooksList> {
        use crate::codecs::decode::list_books_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/ListAddressBooks",
                req,
                |req, opts| client.list_address_books_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(list_books_from_proto(&resp))
    }

    pub async fn list_entries(
        &self,
        req: crate::proto::auth::v1::ListAddressBookEntriesRequest,
    ) -> crate::errors::Result<crate::models::AddressBookEntriesList> {
        use crate::codecs::decode::list_entries_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/ListAddressBookEntries",
                req,
                |req, opts| client.list_address_book_entries_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(list_entries_from_proto(&resp))
    }

    pub async fn create_entry(
        &self,
        req: crate::proto::auth::v1::CreateAddressBookEntryRequest,
    ) -> crate::errors::Result<crate::models::AddressBookEntry> {
        use crate::codecs::decode::entry_from_create_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/CreateAddressBookEntry",
                req,
                |req, opts| client.create_address_book_entry_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(entry_from_create_proto(&resp))
    }

    pub async fn update_entry(
        &self,
        entry_id: u64,
        expected_revision: u64,
        params: UpdateAddressBookEntryParams,
    ) -> crate::errors::Result<crate::models::AddressBookEntry> {
        use crate::codecs::decode::entry_from_update_proto;
        let req = build_update_address_book_entry_request(entry_id, expected_revision, params)?;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/UpdateAddressBookEntry",
                req,
                |req, opts| client.update_address_book_entry_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(entry_from_update_proto(&resp))
    }

    pub async fn delete_entry(
        &self,
        req: crate::proto::auth::v1::DeleteAddressBookEntryRequest,
    ) -> crate::errors::Result<()> {
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/DeleteAddressBookEntry",
                req,
                |req, opts| client.delete_address_book_entry_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn copy_entry(
        &self,
        req: crate::proto::auth::v1::CopyAddressBookEntryRequest,
    ) -> crate::errors::Result<crate::models::AddressBookEntry> {
        use crate::codecs::decode::entry_from_copy_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/CopyAddressBookEntry",
                req,
                |req, opts| client.copy_address_book_entry_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(entry_from_copy_proto(&resp))
    }

    pub async fn create_tag(
        &self,
        req: crate::proto::auth::v1::CreateAddressBookTagRequest,
    ) -> crate::errors::Result<crate::models::AddressBookTag> {
        use crate::codecs::decode::tag_from_create_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/CreateAddressBookTag",
                req,
                |req, opts| client.create_address_book_tag_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(tag_from_create_proto(&resp))
    }

    pub async fn update_tag(
        &self,
        tag_id: u64,
        params: UpdateAddressBookTagParams,
    ) -> crate::errors::Result<crate::models::AddressBookTag> {
        use crate::codecs::decode::tag_from_update_proto;
        let req = build_update_address_book_tag_request(tag_id, params)?;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/UpdateAddressBookTag",
                req,
                |req, opts| client.update_address_book_tag_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(tag_from_update_proto(&resp))
    }

    pub async fn delete_tag(
        &self,
        req: crate::proto::auth::v1::DeleteAddressBookTagRequest,
    ) -> crate::errors::Result<()> {
        let client = self.connect_client();
        let _ = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/DeleteAddressBookTag",
                req,
                |req, opts| client.delete_address_book_tag_with_options(req, opts),
            )
            .await?
        };
        Ok(())
    }

    pub async fn list_transfer_counterparties(
        &self,
        req: crate::proto::auth::v1::ListTransferCounterpartiesRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/ListTransferCounterparties",
                req,
                |req, opts| client.list_transfer_counterparties_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_transfer_destinations(
        &self,
        req: crate::proto::auth::v1::ListTransferDestinationsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/ListTransferDestinations",
                req,
                |req, opts| client.list_transfer_destinations_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_internal_transfer_whitelist_entries(
        &self,
        req: crate::proto::auth::v1::ListInternalTransferWhitelistEntriesRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/ListInternalTransferWhitelistEntries",
                req,
                |req, opts| client.list_internal_transfer_whitelist_entries_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_withdraw_whitelist_view(
        &self,
        req: crate::proto::auth::v1::GetWithdrawWhitelistViewRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/GetWithdrawWhitelistView",
                req,
                |req, opts| client.get_withdraw_whitelist_view_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_view(
        &self,
        req: crate::proto::auth::v1::GetAddressBookViewRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.AddressBookService/GetAddressBookView",
                req,
                |req, opts| client.get_address_book_view_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }
}

impl TransfersService {
    pub async fn list(
        &self,
        req: crate::proto::ledger::read::v1::ListTransfersRequest,
    ) -> crate::errors::Result<crate::models::TransfersList> {
        use crate::codecs::decode::transfers_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/ledger.read.v1.LedgerReadService/ListTransfers",
                req,
                |req, opts| client.list_transfers_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(transfers_list_from_proto(&resp))
    }

    /// Subscribe to private transfer updates (requires `realtime` feature).
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<crate::realtime::TypedSubscription<crate::models::LedgerTransfer>>
    {
        let account = super::scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:ledger:transfers:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::ledger_transfer_from_bytes)
            .await
    }
}

impl ChainAnalyticsService {
    pub async fn get_zipped_asset_supply(
        &self,
        zipped_asset_id: u32,
        range_key: &str,
        bucket: &str,
        start_ts_sec: u32,
        end_ts_sec: u32,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::analytics::resolve_analytics_range;
        use crate::codecs::decode::api_data_from_proto;
        use crate::proto::chain::analytics::v1::GetZippedAssetSupplyRequest;
        let req = GetZippedAssetSupplyRequest {
            zipped_asset_id,
            range: resolve_analytics_range(range_key)?.into(),
            bucket: bucket.to_owned(),
            start_ts_sec,
            end_ts_sec,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = super::unary::await_public(client.get_zipped_asset_supply(req))
            .await?
            .into_owned();
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_zipped_asset_supply_group(
        &self,
        group_id: &str,
        range_key: &str,
        bucket: &str,
        start_ts_sec: u32,
        end_ts_sec: u32,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::analytics::resolve_analytics_range;
        use crate::codecs::decode::api_data_from_proto;
        use crate::proto::chain::analytics::v1::GetZippedAssetSupplyGroupRequest;
        let req = GetZippedAssetSupplyGroupRequest {
            group_id: group_id.to_owned(),
            range: resolve_analytics_range(range_key)?.into(),
            bucket: bucket.to_owned(),
            start_ts_sec,
            end_ts_sec,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = super::unary::await_public(client.get_zipped_asset_supply_group(req))
            .await?
            .into_owned();
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_unified_asset_balances(
        &self,
        asset_id: u32,
        range_key: &str,
        bucket: &str,
        start_ts_sec: u32,
        end_ts_sec: u32,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::analytics::resolve_analytics_range;
        use crate::codecs::decode::api_data_from_proto;
        use crate::errors::Error;
        use crate::proto::chain::analytics::v1::GetUnifiedAssetBalancesRequest;
        if asset_id == 0 {
            return Err(Error::validation("asset_id must be positive"));
        }
        let req = GetUnifiedAssetBalancesRequest {
            asset_id,
            range: resolve_analytics_range(range_key)?.into(),
            bucket: bucket.to_owned(),
            start_ts_sec,
            end_ts_sec,
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = super::unary::await_public(client.get_unified_asset_balances(req))
            .await?
            .into_owned();
        Ok(api_data_from_proto(&resp))
    }
}

impl LifecycleService {
    pub async fn list_flows(
        &self,
        req: crate::proto::chain::lifecycle::v1::ListFlowsRequest,
    ) -> crate::errors::Result<crate::models::LifecycleFlowsList> {
        use crate::codecs::decode::flows_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.lifecycle.v1.LifecycleReadService/ListFlows",
                req,
                |req, opts| client.list_flows_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(flows_list_from_proto(&resp))
    }

    pub async fn get_flow(
        &self,
        flow_id: &str,
    ) -> crate::errors::Result<crate::models::LifecycleFlowSummary> {
        use crate::codecs::decode::flow_from_get_response;
        use crate::errors::Error;
        use crate::proto::chain::lifecycle::v1::GetFlowByIdRequest;
        if flow_id.trim().is_empty() {
            return Err(Error::validation("flow_id or intent_id is required"));
        }
        let req = GetFlowByIdRequest {
            flow_id: flow_id.to_owned(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.lifecycle.v1.LifecycleReadService/GetFlowById",
                req,
                |req, opts| client.get_flow_by_id_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(flow_from_get_response(&resp))
    }

    pub async fn list_flows_by_tx(
        &self,
        req: crate::proto::chain::lifecycle::v1::ListFlowsByTxRequest,
    ) -> crate::errors::Result<crate::models::LifecycleFlowsList> {
        use crate::codecs::decode::flows_by_tx_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.lifecycle.v1.LifecycleReadService/ListFlowsByTx",
                req,
                |req, opts| client.list_flows_by_tx_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(flows_by_tx_list_from_proto(&resp))
    }

    pub async fn get_flow_by_tx(
        &self,
        req: crate::proto::chain::lifecycle::v1::ListFlowsByTxRequest,
    ) -> crate::errors::Result<crate::models::LifecycleFlowSummary> {
        use crate::codecs::decode::flow_from_get_by_tx_response;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.lifecycle.v1.LifecycleReadService/ListFlowsByTx",
                req,
                |req, opts| client.list_flows_by_tx_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(flow_from_get_by_tx_response(&resp))
    }

    /// Subscribe to open lifecycle flow summaries.
    ///
    /// When `account_id` is `Some`, uses the private account channel; otherwise
    /// the public open-flows channel (Go `SubscribeOpenFlows` parity).
    pub async fn subscribe_open_flows(
        &self,
        account_id: Option<&str>,
    ) -> crate::errors::Result<
        crate::realtime::TypedSubscription<crate::models::LifecycleFlowSummary>,
    > {
        if let Some(account) = account_id.filter(|s| !s.trim().is_empty()) {
            let channel = format!("private:chain:lifecycle:flows:{account}:proto");
            self.ctx
                .realtime
                .subscribe_proto(&channel, crate::codecs::decode::flow_summary_from_bytes)
                .await
        } else {
            self.ctx
                .realtime
                .subscribe_proto(
                    "public:chain:lifecycle:flows:proto",
                    crate::codecs::decode::flow_summary_from_bytes,
                )
                .await
        }
    }

    /// Subscribe to a single flow's detail updates (requires `realtime` feature).
    pub async fn subscribe_flow_detail(
        &self,
        flow_id: &str,
    ) -> crate::errors::Result<
        crate::realtime::TypedSubscription<crate::models::LifecycleFlowSummary>,
    > {
        let channel = format!("public:chain:lifecycle:flow:{flow_id}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::flow_detail_from_bytes)
            .await
    }
}

impl GuardSignerService {
    pub async fn get_status(
        &self,
        req: crate::proto::chain::guard::v1::GetGuardSignerStatusRequest,
    ) -> crate::errors::Result<Option<crate::models::GuardSignerStatus>> {
        use crate::codecs::decode::status_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/GetGuardSignerStatus",
                req,
                |req, opts| client.get_guard_signer_status_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(status_from_proto(&resp))
    }

    pub async fn create_wallet(
        &self,
        req: crate::proto::chain::guard::v1::CreateGuardSignerWalletRequest,
    ) -> crate::errors::Result<crate::models::CreateGuardSignerWalletResult> {
        use crate::codecs::decode::create_wallet_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/CreateGuardSignerWallet",
                req,
                |req, opts| client.create_guard_signer_wallet_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(create_wallet_from_proto(&resp))
    }

    pub async fn sign_protected_action(
        &self,
        req: crate::proto::chain::guard::v1::SignProtectedActionRequest,
    ) -> crate::errors::Result<Option<crate::models::GuardApproval>> {
        use crate::codecs::decode::sign_protected_action_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/SignProtectedAction",
                req,
                |req, opts| client.sign_protected_action_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(sign_protected_action_from_proto(&resp))
    }

    pub async fn batch_sign_protected_actions(
        &self,
        req: crate::proto::chain::guard::v1::BatchSignProtectedActionsRequest,
    ) -> crate::errors::Result<crate::models::BatchSignProtectedActionsResult> {
        use crate::codecs::decode::batch_sign_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/BatchSignProtectedActions",
                req,
                |req, opts| client.batch_sign_protected_actions_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(batch_sign_from_proto(&resp))
    }

    pub async fn rotate_wallet(
        &self,
        req: crate::proto::chain::guard::v1::RotateGuardSignerWalletRequest,
    ) -> crate::errors::Result<crate::models::RotateGuardSignerWalletResult> {
        use crate::codecs::decode::rotate_wallet_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/RotateGuardSignerWallet",
                req,
                |req, opts| client.rotate_guard_signer_wallet_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(rotate_wallet_from_proto(&resp))
    }

    pub async fn export_wallet(
        &self,
        req: crate::proto::chain::guard::v1::ExportGuardSignerWalletRequest,
    ) -> crate::errors::Result<crate::models::ExportGuardSignerWalletResult> {
        use crate::codecs::decode::export_wallet_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/chain.guard.v1.GuardSignerService/ExportGuardSignerWallet",
                req,
                |req, opts| client.export_guard_signer_wallet_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(export_wallet_from_proto(&resp))
    }
}

impl SocialVerificationService {
    pub async fn start(
        &self,
        provider: &str,
        method: &str,
        handle: &str,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        use crate::proto::auth::v1::StartSocialVerificationRequest;
        let req = StartSocialVerificationRequest {
            provider: social_provider_enum(provider).into(),
            method: social_method_enum(method).into(),
            handle: handle.to_owned(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SocialVerificationService/StartSocialVerification",
                req,
                |req, opts| client.start_social_verification_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn mark_ready(
        &self,
        provider: &str,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        use crate::proto::auth::v1::SocialVerificationReadyRequest;
        let req = SocialVerificationReadyRequest {
            provider: social_provider_enum(provider).into(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SocialVerificationService/SocialVerificationReady",
                req,
                |req, opts| client.social_verification_ready_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get(&self, provider: &str) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        use crate::proto::auth::v1::GetSocialVerificationRequest;
        let req = GetSocialVerificationRequest {
            provider: social_provider_enum(provider).into(),
            ..Default::default()
        };
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.SocialVerificationService/GetSocialVerification",
                req,
                |req, opts| client.get_social_verification_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }
}

fn social_provider_enum(v: &str) -> crate::proto::auth::v1::SocialProvider {
    use crate::proto::auth::v1::SocialProvider;
    match v.trim().to_ascii_lowercase().as_str() {
        "twitter" => SocialProvider::TWITTER,
        "discord" => SocialProvider::DISCORD,
        _ => SocialProvider::PROVIDER_UNSPECIFIED,
    }
}

fn social_method_enum(v: &str) -> crate::proto::auth::v1::SocialVerificationMethod {
    use crate::proto::auth::v1::SocialVerificationMethod;
    match v.trim().to_ascii_lowercase().as_str() {
        "profile" => SocialVerificationMethod::METHOD_PROFILE,
        "channel" => SocialVerificationMethod::METHOD_CHANNEL,
        "dm" => SocialVerificationMethod::METHOD_DM,
        _ => SocialVerificationMethod::METHOD_UNSPECIFIED,
    }
}

impl LayoutService {
    pub async fn get_layouts(
        &self,
        req: crate::proto::layout::v1::GetLayoutsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/GetLayouts",
                req,
                |req, opts| client.get_layouts_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_layout(
        &self,
        req: crate::proto::layout::v1::GetLayoutRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/GetLayout",
                req,
                |req, opts| client.get_layout_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn upsert_layout(
        &self,
        req: crate::proto::layout::v1::UpsertLayoutRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/UpsertLayout",
                req,
                |req, opts| client.upsert_layout_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn delete_layout(
        &self,
        req: crate::proto::layout::v1::DeleteLayoutRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/DeleteLayout",
                req,
                |req, opts| client.delete_layout_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn resolve_layout_share_token(
        &self,
        req: crate::proto::layout::v1::ResolveLayoutShareTokenRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/ResolveLayoutShareToken",
                req,
                |req, opts| client.resolve_layout_share_token_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn create_layout_share_link(
        &self,
        req: crate::proto::layout::v1::CreateLayoutShareLinkRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/CreateLayoutShareLink",
                req,
                |req, opts| client.create_layout_share_link_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn revoke_layout_share_link(
        &self,
        req: crate::proto::layout::v1::RevokeLayoutShareLinkRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/RevokeLayoutShareLink",
                req,
                |req, opts| client.revoke_layout_share_link_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_owner_published_layouts(
        &self,
        req: crate::proto::layout::v1::ListOwnerPublishedLayoutsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/ListOwnerPublishedLayouts",
                req,
                |req, opts| client.list_owner_published_layouts_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn publish_layout(
        &self,
        req: crate::proto::layout::v1::PublishLayoutRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/PublishLayout",
                req,
                |req, opts| client.publish_layout_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn unpublish_layout(
        &self,
        req: crate::proto::layout::v1::UnpublishLayoutRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/UnpublishLayout",
                req,
                |req, opts| client.unpublish_layout_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_layout_template_versions(
        &self,
        req: crate::proto::layout::v1::ListLayoutTemplateVersionsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/ListLayoutTemplateVersions",
                req,
                |req, opts| client.list_layout_template_versions_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_layout_template_version(
        &self,
        req: crate::proto::layout::v1::GetLayoutTemplateVersionRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/GetLayoutTemplateVersion",
                req,
                |req, opts| client.get_layout_template_version_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn set_layout_template_subscription(
        &self,
        req: crate::proto::layout::v1::SetLayoutTemplateSubscriptionRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/SetLayoutTemplateSubscription",
                req,
                |req, opts| client.set_layout_template_subscription_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn delete_layout_template_subscription(
        &self,
        req: crate::proto::layout::v1::DeleteLayoutTemplateSubscriptionRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/DeleteLayoutTemplateSubscription",
                req,
                |req, opts| client.delete_layout_template_subscription_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_my_layout_template_subscriptions(
        &self,
        req: crate::proto::layout::v1::ListMyLayoutTemplateSubscriptionsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/layout.v1.LayoutService/ListMyLayoutTemplateSubscriptions",
                req,
                |req, opts| client.list_my_layout_template_subscriptions_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }
}

impl PolychartService {
    pub async fn get_market_layers(
        &self,
        req: crate::proto::polychart::v1::GetMarketLayersRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/GetMarketLayers",
                req,
                |req, opts| client.get_market_layers_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_inbox_market_layers(
        &self,
        req: crate::proto::polychart::v1::ListInboxMarketLayersRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/ListInboxMarketLayers",
                req,
                |req, opts| client.list_inbox_market_layers_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_layer_snapshot(
        &self,
        req: crate::proto::polychart::v1::GetLayerSnapshotRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/GetLayerSnapshot",
                req,
                |req, opts| client.get_layer_snapshot_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_layer_subscribe_tokens(
        &self,
        req: crate::proto::polychart::v1::GetLayerSubscribeTokensRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/GetLayerSubscribeTokens",
                req,
                |req, opts| client.get_layer_subscribe_tokens_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn resolve_layer_share_token(
        &self,
        req: crate::proto::polychart::v1::ResolveLayerShareTokenRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/ResolveLayerShareToken",
                req,
                |req, opts| client.resolve_layer_share_token_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn create_layer_share_link(
        &self,
        req: crate::proto::polychart::v1::CreateLayerShareLinkRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/CreateLayerShareLink",
                req,
                |req, opts| client.create_layer_share_link_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn revoke_layer_share_link(
        &self,
        req: crate::proto::polychart::v1::RevokeLayerShareLinkRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/RevokeLayerShareLink",
                req,
                |req, opts| client.revoke_layer_share_link_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_owner_published_layers(
        &self,
        req: crate::proto::polychart::v1::ListOwnerPublishedLayersRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/ListOwnerPublishedLayers",
                req,
                |req, opts| client.list_owner_published_layers_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn publish_layer(
        &self,
        req: crate::proto::polychart::v1::PublishLayerRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/PublishLayer",
                req,
                |req, opts| client.publish_layer_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn unpublish_layer(
        &self,
        req: crate::proto::polychart::v1::UnpublishLayerRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/UnpublishLayer",
                req,
                |req, opts| client.unpublish_layer_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn upsert_layer(
        &self,
        req: crate::proto::polychart::v1::UpsertLayerRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/UpsertLayer",
                req,
                |req, opts| client.upsert_layer_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn delete_layer(
        &self,
        req: crate::proto::polychart::v1::DeleteLayerRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/DeleteLayer",
                req,
                |req, opts| client.delete_layer_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn upsert_drawing(
        &self,
        req: crate::proto::polychart::v1::UpsertDrawingRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/UpsertDrawing",
                req,
                |req, opts| client.upsert_drawing_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn delete_drawing(
        &self,
        req: crate::proto::polychart::v1::DeleteDrawingRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/DeleteDrawing",
                req,
                |req, opts| client.delete_drawing_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn set_layer_subscriptions(
        &self,
        req: crate::proto::polychart::v1::SetLayerSubscriptionsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/polychart.v1.PolychartService/SetLayerSubscriptions",
                req,
                |req, opts| client.set_layer_subscriptions_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }
}

impl WhiteboardService {
    pub async fn create_board(
        &self,
        req: crate::proto::collab::v1::CreateBoardRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/CreateBoard",
                req,
                |req, opts| client.create_board_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn get_board(
        &self,
        req: crate::proto::collab::v1::GetBoardRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/GetBoard",
                req,
                |req, opts| client.get_board_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn list_boards(
        &self,
        req: crate::proto::collab::v1::ListBoardsRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/ListBoards",
                req,
                |req, opts| client.list_boards_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn update_board(
        &self,
        req: crate::proto::collab::v1::UpdateBoardRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/UpdateBoard",
                req,
                |req, opts| client.update_board_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn update_board_acl(
        &self,
        req: crate::proto::collab::v1::UpdateBoardAclRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/UpdateBoardAcl",
                req,
                |req, opts| client.update_board_acl_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn archive_board(
        &self,
        req: crate::proto::collab::v1::ArchiveBoardRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/ArchiveBoard",
                req,
                |req, opts| client.archive_board_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }

    pub async fn mint_join_token(
        &self,
        req: crate::proto::collab::v1::MintJoinTokenRequest,
    ) -> crate::errors::Result<crate::models::ApiData> {
        use crate::codecs::decode::api_data_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/collab.v1.WhiteboardService/MintJoinToken",
                req,
                |req, opts| client.mint_join_token_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_data_from_proto(&resp))
    }
}

/// Build a durable-auth `UpdateSubaccountRequest` from presence-aware params.
pub fn build_update_subaccount_request(
    subaccount_id: u64,
    expected_revision: u64,
    params: UpdateSubaccountParams,
) -> crate::errors::Result<crate::proto::auth::v1::UpdateSubaccountRequest> {
    use crate::errors::Error;
    use crate::proto::auth::v1::{SubaccountUpdateSpec, UpdateSubaccountRequest};
    use buffa_types::google::protobuf::FieldMask;

    if expected_revision == 0 {
        return Err(Error::validation(
            "expected_revision must be a positive revision from a prior read",
        ));
    }

    let mut spec = SubaccountUpdateSpec::default();
    let mut paths = Vec::new();

    if let Some(label) = params.label {
        paths.push("label".to_owned());
        spec.label = label;
    }
    if let Some(icon) = params.icon {
        paths.push("icon".to_owned());
        spec.icon = icon;
    }
    if let Some(color) = params.color {
        paths.push("color".to_owned());
        spec.color = color;
    }
    if let Some(status) = params.status {
        paths.push("status".to_owned());
        spec.status = status;
    }

    if paths.is_empty() {
        return Err(Error::validation(
            "update_mask must be non-empty; set at least one field on UpdateSubaccountParams",
        ));
    }

    Ok(UpdateSubaccountRequest {
        subaccount_id,
        subaccount: spec.into(),
        update_mask: FieldMask {
            paths,
            ..Default::default()
        }
        .into(),
        expected_revision,
        ..Default::default()
    })
}

/// Build a durable-auth `UpdateAddressBookEntryRequest` from presence-aware params.
pub fn build_update_address_book_entry_request(
    entry_id: u64,
    expected_revision: u64,
    params: UpdateAddressBookEntryParams,
) -> crate::errors::Result<crate::proto::auth::v1::UpdateAddressBookEntryRequest> {
    use crate::errors::Error;
    use crate::proto::auth::v1::{AddressBookEntryUpdateSpec, UpdateAddressBookEntryRequest};
    use buffa_types::google::protobuf::FieldMask;

    if expected_revision == 0 {
        return Err(Error::validation(
            "expected_revision must be a positive revision from a prior read",
        ));
    }

    let mut spec = AddressBookEntryUpdateSpec::default();
    let mut paths = Vec::new();

    if let Some(label) = params.label {
        paths.push("label".to_owned());
        spec.label = label;
    }
    if let Some(note) = params.note {
        paths.push("note".to_owned());
        spec.note = note;
    }
    if let Some(tag_ids) = params.tag_ids {
        paths.push("tag_ids".to_owned());
        spec.tag_ids = tag_ids;
    }

    if paths.is_empty() {
        return Err(Error::validation(
            "update_mask must be non-empty; set at least one field on UpdateAddressBookEntryParams",
        ));
    }

    Ok(UpdateAddressBookEntryRequest {
        address_book_entry_id: entry_id,
        entry: spec.into(),
        update_mask: FieldMask {
            paths,
            ..Default::default()
        }
        .into(),
        expected_revision,
        ..Default::default()
    })
}

/// Build an `UpdateAddressBookTagRequest` from optional scalar params.
pub fn build_update_address_book_tag_request(
    tag_id: u64,
    params: UpdateAddressBookTagParams,
) -> crate::errors::Result<crate::proto::auth::v1::UpdateAddressBookTagRequest> {
    use crate::errors::Error;
    use crate::proto::auth::v1::UpdateAddressBookTagRequest;

    if let Some(ref name) = params.name
        && name.is_empty()
    {
        return Err(Error::validation("name cannot be empty when set"));
    }

    Ok(UpdateAddressBookTagRequest {
        tag_id,
        name: params.name,
        color: params.color,
        ..Default::default()
    })
}

/// Build a durable-auth `UpdateSubaccountPolicyRequest` from pragmatic patch params.
pub fn build_update_subaccount_policy_request(
    policy_id: u64,
    expected_revision: u64,
    params: UpdateSubaccountPolicyParams,
) -> crate::errors::Result<crate::proto::auth::v1::UpdateSubaccountPolicyRequest> {
    use crate::errors::Error;
    use crate::proto::auth::v1::{
        SpotMarketRule, SubaccountPolicySpec, UpdateSubaccountPolicyRequest,
    };
    use buffa_types::google::protobuf::FieldMask;

    if expected_revision == 0 {
        return Err(Error::validation(
            "expected_revision must be a positive revision from a prior read",
        ));
    }

    let mut spec = SubaccountPolicySpec::default();
    let mut paths = Vec::new();

    if let Some(name) = params.name {
        paths.push("name".to_owned());
        spec.name = name;
    }
    if let Some(description) = params.description {
        paths.push("description".to_owned());
        spec.description = description;
    }
    if let Some(actions) = params.actions {
        paths.push("actions".to_owned());
        spec.actions = actions.into_iter().map(Into::into).collect();
    }
    if let Some(symbols) = params.spot_markets {
        paths.push("spot_markets".to_owned());
        spec.spot_markets = symbols
            .into_iter()
            .map(|symbol| SpotMarketRule {
                symbol,
                ..Default::default()
            })
            .collect();
    }
    if let Some(trading_halted) = params.trading_halted {
        paths.push("trading_halted".to_owned());
        spec.trading_halted = trading_halted;
    }
    if let Some(global_notional_cap) = params.global_notional_cap {
        paths.push("global_notional_cap".to_owned());
        spec.global_notional_cap = global_notional_cap;
    }
    if let Some(max_order_notional) = params.max_order_notional {
        paths.push("max_order_notional".to_owned());
        spec.max_order_notional = max_order_notional;
    }
    if let Some(review_at) = params.review_at {
        paths.push("review_at".to_owned());
        match review_at {
            Some(ts) => spec.review_at = ts.into(),
            None => spec.review_at = buffa::MessageField::none(),
        }
    }
    if let Some(expires_at) = params.expires_at {
        paths.push("expires_at".to_owned());
        match expires_at {
            Some(ts) => spec.expires_at = ts.into(),
            None => spec.expires_at = buffa::MessageField::none(),
        }
    }

    if paths.is_empty() {
        return Err(Error::validation(
            "update_mask must be non-empty; set at least one field on UpdateSubaccountPolicyParams",
        ));
    }

    Ok(UpdateSubaccountPolicyRequest {
        policy_id,
        policy: spec.into(),
        update_mask: FieldMask {
            paths,
            ..Default::default()
        }
        .into(),
        expected_revision,
        ..Default::default()
    })
}

/// Build a durable-auth `UpdateApiPolicyRequest` from pragmatic patch params.
pub fn build_update_api_policy_request(
    policy_id: u64,
    expected_revision: u64,
    params: UpdateApiPolicyParams,
) -> crate::errors::Result<crate::proto::auth::v1::UpdateApiPolicyRequest> {
    use crate::errors::Error;
    use crate::proto::auth::v1::{ApiPolicySpec, UpdateApiPolicyRequest};
    use buffa_types::google::protobuf::FieldMask;

    if expected_revision == 0 {
        return Err(Error::validation(
            "expected_revision must be a positive revision from a prior read",
        ));
    }

    let mut spec = ApiPolicySpec::default();
    let mut paths = Vec::new();

    if let Some(name) = params.name {
        paths.push("name".to_owned());
        spec.name = name;
    }
    if let Some(description) = params.description {
        paths.push("description".to_owned());
        spec.description = description;
    }
    if let Some(actions) = params.actions {
        paths.push("actions".to_owned());
        spec.actions = actions.into_iter().map(Into::into).collect();
    }
    if let Some(max_order_notional) = params.max_order_notional {
        paths.push("max_order_notional".to_owned());
        spec.max_order_notional = max_order_notional;
    }

    if paths.is_empty() {
        return Err(Error::validation(
            "update_mask must be non-empty; set at least one field on UpdateApiPolicyParams",
        ));
    }

    Ok(UpdateApiPolicyRequest {
        policy_id,
        policy: spec.into(),
        update_mask: FieldMask {
            paths,
            ..Default::default()
        }
        .into(),
        expected_revision,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CreateInternalTransferParams;
    use crate::types::{AssetAmount, QuantityDomain};

    #[tokio::test]
    async fn internal_transfer_requires_destination_before_transport() {
        let client = crate::Client::new(crate::Config {
            hydrate_catalogs: false,
            ..Default::default()
        })
        .unwrap();
        let params = CreateInternalTransferParams {
            asset_id: 7,
            quantity: AssetAmount::from_scaled(100, Some(18), QuantityDomain::LedgerE18, Some(7))
                .unwrap(),
            idempotency_key: "missing-destination".into(),
            subaccount_id: None,
            destination_account_id: None,
            destination_subaccount_id: None,
            destination_smart_account_address: None,
            quantity_scale: Some(18),
        };

        let err = client.internal_transfers.create(params).await.unwrap_err();
        assert!(err.to_string().contains("requires destination"));
    }

    #[test]
    fn update_subaccount_request_builds_nested_spec_and_mask() {
        let req = build_update_subaccount_request(
            42,
            3,
            UpdateSubaccountParams {
                label: Some(String::new()),
                icon: None,
                color: Some("#fff".into()),
                status: Some("disabled".into()),
            },
        )
        .unwrap();

        assert_eq!(req.subaccount_id, 42);
        assert_eq!(req.expected_revision, 3);
        assert_eq!(
            req.update_mask.as_option().unwrap().paths,
            vec!["label", "color", "status"]
        );
        let spec = req.subaccount.as_option().unwrap();
        assert_eq!(spec.label, "");
        assert_eq!(spec.color, "#fff");
        assert_eq!(spec.status, "disabled");
        assert!(spec.icon.is_empty());
    }

    #[test]
    fn delete_subaccount_request_masks_status_only() {
        let req = build_update_subaccount_request(
            1,
            2,
            UpdateSubaccountParams {
                status: Some("deleted".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(req.update_mask.as_option().unwrap().paths, vec!["status"]);
        assert_eq!(req.subaccount.as_option().unwrap().status, "deleted");
    }

    #[test]
    fn update_subaccount_request_rejects_zero_revision_and_empty_mask() {
        let err = build_update_subaccount_request(
            1,
            0,
            UpdateSubaccountParams {
                label: Some("x".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected_revision"));

        let err =
            build_update_subaccount_request(1, 1, UpdateSubaccountParams::default()).unwrap_err();
        assert!(err.to_string().contains("update_mask"));
    }

    #[test]
    fn update_address_book_entry_clears_label_note_and_tag_ids() {
        let req = build_update_address_book_entry_request(
            110,
            6,
            UpdateAddressBookEntryParams {
                label: Some(String::new()),
                note: Some(String::new()),
                tag_ids: Some(vec![]),
            },
        )
        .unwrap();

        assert_eq!(req.address_book_entry_id, 110);
        assert_eq!(req.expected_revision, 6);
        assert_eq!(
            req.update_mask.as_option().unwrap().paths,
            vec!["label", "note", "tag_ids"]
        );
        let entry = req.entry.as_option().unwrap();
        assert!(entry.label.is_empty());
        assert!(entry.note.is_empty());
        assert!(entry.tag_ids.is_empty());
    }

    #[test]
    fn update_address_book_entry_rejects_zero_revision_and_empty_mask() {
        let err = build_update_address_book_entry_request(
            1,
            0,
            UpdateAddressBookEntryParams {
                label: Some("x".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected_revision"));

        let err =
            build_update_address_book_entry_request(1, 1, UpdateAddressBookEntryParams::default())
                .unwrap_err();
        assert!(err.to_string().contains("update_mask"));
    }

    #[test]
    fn update_address_book_tag_clears_color_and_omits_name() {
        let req = build_update_address_book_tag_request(
            40,
            UpdateAddressBookTagParams {
                name: None,
                color: Some(String::new()),
            },
        )
        .unwrap();
        assert_eq!(req.tag_id, 40);
        assert!(req.name.is_none());
        assert_eq!(req.color.as_deref(), Some(""));
    }

    #[test]
    fn update_address_book_tag_sets_name_omits_color() {
        let req = build_update_address_book_tag_request(
            40,
            UpdateAddressBookTagParams {
                name: Some("friends".into()),
                color: None,
            },
        )
        .unwrap();
        assert_eq!(req.name.as_deref(), Some("friends"));
        assert!(req.color.is_none());
    }

    #[test]
    fn update_address_book_tag_rejects_empty_name() {
        let err = build_update_address_book_tag_request(
            1,
            UpdateAddressBookTagParams {
                name: Some(String::new()),
                color: None,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn update_subaccount_policy_false_zero_empty_and_clear_timestamps() {
        use crate::proto::auth::v1::PolicyAction;

        let req = build_update_subaccount_policy_request(
            50,
            8,
            UpdateSubaccountPolicyParams {
                name: Some(String::new()),
                actions: Some(vec![PolicyAction::READ_BALANCES, PolicyAction::READ_SPOT]),
                spot_markets: Some(vec![]),
                trading_halted: Some(false),
                global_notional_cap: Some(0),
                review_at: Some(None),
                expires_at: Some(None),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(req.policy_id, 50);
        assert_eq!(req.expected_revision, 8);
        assert_eq!(
            req.update_mask.as_option().unwrap().paths,
            vec![
                "name",
                "actions",
                "spot_markets",
                "trading_halted",
                "global_notional_cap",
                "review_at",
                "expires_at",
            ]
        );
        let policy = req.policy.as_option().unwrap();
        assert!(policy.name.is_empty());
        assert_eq!(policy.actions.len(), 2);
        assert!(policy.spot_markets.is_empty());
        assert!(!policy.trading_halted);
        assert_eq!(policy.global_notional_cap, 0);
        assert!(!policy.review_at.is_set());
        assert!(!policy.expires_at.is_set());
    }

    #[test]
    fn update_api_policy_one_field_omits_others() {
        let req = build_update_api_policy_request(
            30,
            2,
            UpdateApiPolicyParams {
                description: Some("only".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            req.update_mask.as_option().unwrap().paths,
            vec!["description"]
        );
        let policy = req.policy.as_option().unwrap();
        assert_eq!(policy.description, "only");
        assert!(policy.name.is_empty());
        assert_eq!(policy.max_order_notional, 0);
    }

    #[test]
    fn create_policy_uses_nested_policy_spec() {
        use crate::proto::auth::v1::{
            ApiPolicySpec, CreateApiPolicyRequest, CreateSubaccountPolicyRequest,
            SubaccountPolicySpec,
        };

        let sub = CreateSubaccountPolicyRequest {
            policy: SubaccountPolicySpec {
                name: "p".into(),
                description: String::new(),
                ..Default::default()
            }
            .into(),
            subaccount_id: Some(1),
            ..Default::default()
        };
        assert_eq!(sub.policy.as_option().unwrap().name, "p");
        assert!(sub.policy.as_option().unwrap().description.is_empty());

        let api = CreateApiPolicyRequest {
            policy: ApiPolicySpec {
                name: "k".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        assert_eq!(api.policy.as_option().unwrap().name, "k");
    }
}
