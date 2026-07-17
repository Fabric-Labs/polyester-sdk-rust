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
    ApiKeysService,
    crate::connect::auth::v1::ApiKeyServiceClient<crate::transport::SharedTransport>,
    true
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

impl InternalTransfersService {
    /// Create an internal transfer (signed). Prefer this over raw `connect_client` in tests.
    pub async fn create(
        &self,
        req: crate::proto::transfer::v1::CreateInternalTransferRequest,
    ) -> crate::errors::Result<crate::models::InternalTransferResult> {
        use super::unary;
        use crate::codecs::decode::internal_transfer_from_proto;
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

impl ApiKeysService {
    pub async fn list(
        &self,
        req: crate::proto::auth::v1::ListApiKeysRequest,
    ) -> crate::errors::Result<crate::models::ApiKeysList> {
        use crate::codecs::decode::api_keys_list_from_proto;
        let client = self.connect_client();
        let resp = {
            use super::unary;
            unary::await_auth(
                &self.ctx.factory,
                "/auth.v1.ApiKeyService/ListApiKeys",
                req,
                |req, opts| client.list_api_keys_with_options(req, opts),
            )
            .await?
            .into_owned()
        };
        Ok(api_keys_list_from_proto(&resp))
    }
}

impl PoliciesService {
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
}

impl SubAccountsService {
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

impl AddressBookService {
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
}
