//! Thin service shells that expose generated Connect clients for full surface parity.

use super::ServiceContext;

macro_rules! thin_service {
    ($name:ident, $client:ty) => {
        #[derive(Clone)]
        pub struct $name {
            pub(crate) ctx: ServiceContext,
        }
        impl $name {
            pub fn new(ctx: ServiceContext) -> Self {
                Self { ctx }
            }
            pub(crate) fn connect_client(&self) -> $client {
                <$client>::new(
                    self.ctx.factory.transport(),
                    self.ctx.factory.connect_config(),
                )
            }
        }
    };
}

macro_rules! realtime_only_service {
    ($name:ident) => {
        #[derive(Clone)]
        pub struct $name {
            pub(crate) ctx: ServiceContext,
        }
        impl $name {
            pub fn new(ctx: ServiceContext) -> Self {
                Self { ctx }
            }
        }
    };
}

thin_service!(
    ChainAnalyticsService,
    crate::connect::chain::analytics::v1::ChainAnalyticsServiceClient<
        crate::transport::SharedTransport,
    >
);
thin_service!(
    LifecycleService,
    crate::connect::chain::lifecycle::v1::LifecycleReadServiceClient<
        crate::transport::SharedTransport,
    >
);
thin_service!(
    HeatmapService,
    crate::connect::marketdata::v1::HeatmapServiceClient<crate::transport::SharedTransport>
);
realtime_only_service!(PoliciesService);
thin_service!(
    SubAccountsService,
    crate::connect::auth::v1::SubaccountServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    AddressBookService,
    crate::connect::auth::v1::AddressBookServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    SocialVerificationService,
    crate::connect::auth::v1::SocialVerificationServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    WhiteboardService,
    crate::connect::collab::v1::WhiteboardServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    PolychartService,
    crate::connect::polychart::v1::PolychartServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    LayoutService,
    crate::connect::layout::v1::LayoutServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    GuardSignerService,
    crate::connect::chain::guard::v1::GuardSignerServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    InternalTransfersService,
    crate::connect::transfer::v1::InternalTransferServiceClient<crate::transport::SharedTransport>
);
thin_service!(
    TransfersService,
    crate::connect::ledger::read::v1::LedgerReadServiceClient<crate::transport::SharedTransport>
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

fn internal_transfer_amount_e18(
    quantity: &crate::types::AssetAmount,
    quantity_scale: Option<u32>,
    asset_id: u32,
) -> crate::errors::Result<crate::proto::polyester::r#type::v1::U128> {
    use crate::codecs::scalars::{LEDGER_SCALE, i128_to_u128};
    use crate::types::{QuantityDomain, resolve_asset_amount_scaled_with_input_scale};

    let scaled = resolve_asset_amount_scaled_with_input_scale(
        quantity,
        quantity_scale,
        LEDGER_SCALE,
        QuantityDomain::LedgerE18,
        Some(asset_id),
    )?;
    i128_to_u128(scaled)
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
        use crate::codecs::scalars::id_to_u64;
        use crate::errors::Error;
        use crate::proto::transfer::v1::{
            CreateInternalTransferRequest, create_internal_transfer_request::Destination,
        };

        let has_account = params
            .destination_account_id
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        let has_sub = params
            .destination_subaccount_id
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        let has_smart = params
            .destination_smart_account_address
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty());
        if usize::from(has_account) + usize::from(has_sub) + usize::from(has_smart) != 1 {
            return Err(Error::validation(
                "create requires exactly one of destination_account_id, destination_subaccount_id, or destination_smart_account_address",
            ));
        }
        if params.idempotency_key.trim().is_empty() {
            return Err(Error::validation(
                "create requires a non-empty idempotency_key reused across retries",
            ));
        }

        let amount_e18 =
            internal_transfer_amount_e18(&params.quantity, params.quantity_scale, params.asset_id)?;
        let mut req = CreateInternalTransferRequest {
            asset_id: params.asset_id,
            idempotency_key: params.idempotency_key,
            subaccount_id: scope::optional_subaccount(&self.ctx, params.subaccount_id)?
                .unwrap_or(0),
            ..Default::default()
        };
        *req.amount_e18.get_or_insert_default() = amount_e18;
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
        internal_transfer_from_proto(&resp)
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
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
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

    pub async fn list_activity(
        &self,
        req: crate::proto::auth::v1::ListSubaccountEventsRequest,
    ) -> crate::errors::Result<crate::models::SubAccountActivityList> {
        use crate::codecs::decode::subaccount_activity_list_from_proto;
        let client = crate::connect::auth::v1::SubaccountViewServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
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
        flow_from_get_response(&resp)
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
        flow_from_get_by_tx_response(&resp)
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

#[cfg(test)]
mod tests {
    use super::internal_transfer_amount_e18;
    use crate::models::CreateInternalTransferParams;
    use crate::types::{AssetAmount, QuantityDomain};

    #[test]
    fn internal_transfer_amount_is_always_exact_e18() {
        let amount =
            AssetAmount::from_scaled(125, Some(2), QuantityDomain::LedgerE18, Some(7)).unwrap();
        let wire = internal_transfer_amount_e18(&amount, Some(2), 7).unwrap();
        assert_eq!(
            (u128::from(wire.hi) << 64) | u128::from(wire.lo),
            1_250_000_000_000_000_000
        );

        let inexact =
            AssetAmount::from_scaled(126, Some(19), QuantityDomain::LedgerE18, Some(7)).unwrap();
        assert!(internal_transfer_amount_e18(&inexact, Some(19), 7).is_err());
    }

    #[test]
    fn internal_transfer_rejects_missing_amount_scale_before_transport() {
        let amount = AssetAmount::from_scaled(1, None, QuantityDomain::LedgerE18, Some(7)).unwrap();
        let err = internal_transfer_amount_e18(&amount, None, 7)
            .expect_err("missing scale must not silently mean e18");
        assert!(err.to_string().contains("amount scale is required"));
    }

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

        let err = client
            .internal_transfers
            .create(params.clone())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires exactly one"));

        let multiple = CreateInternalTransferParams {
            destination_account_id: Some("2".into()),
            destination_subaccount_id: Some("3".into()),
            idempotency_key: "multiple-destinations".into(),
            ..params.clone()
        };
        let err = client
            .internal_transfers
            .create(multiple)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires exactly one"));

        let empty_key = CreateInternalTransferParams {
            destination_account_id: Some("2".into()),
            idempotency_key: " ".into(),
            ..params.clone()
        };
        let err = client
            .internal_transfers
            .create(empty_key)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-empty idempotency_key"));

        let whitespace_destination = CreateInternalTransferParams {
            destination_smart_account_address: Some("   ".into()),
            ..params
        };
        let err = client
            .internal_transfers
            .create(whitespace_destination)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires exactly one"));
    }
}
