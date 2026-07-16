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
    ) -> crate::errors::Result<crate::proto::transfer::v1::CreateInternalTransferResponse> {
        use super::unary;
        let client = self.connect_client();
        Ok(unary::await_auth(
            &self.ctx.factory,
            "/transfer.v1.InternalTransferService/CreateInternalTransfer",
            req,
            |req, opts| client.create_internal_transfer_with_options(req, opts),
        )
        .await?
        .into_owned())
    }
}

macro_rules! signed_unary {
    ($client:expr, $procedure:expr, $req:expr, $call:expr) => {{
        use super::unary;
        Ok(
            unary::await_auth(&$client.ctx.factory, $procedure, $req, $call)
                .await?
                .into_owned(),
        )
    }};
}

impl ApiKeysService {
    pub async fn list(
        &self,
        req: crate::proto::auth::v1::ListApiKeysRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ListApiKeysResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.ApiKeyService/ListApiKeys",
            req,
            |req, opts| client.list_api_keys_with_options(req, opts)
        )
    }
}

impl PoliciesService {
    pub async fn list_subaccount_policies(
        &self,
        req: crate::proto::auth::v1::ListSubaccountPoliciesRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ListSubaccountPoliciesResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.PolicyService/ListSubaccountPolicies",
            req,
            |req, opts| client.list_subaccount_policies_with_options(req, opts)
        )
    }

    pub async fn list_api_policies(
        &self,
        req: crate::proto::auth::v1::ListApiPoliciesRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ListApiPoliciesResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.PolicyService/ListApiPolicies",
            req,
            |req, opts| client.list_api_policies_with_options(req, opts)
        )
    }
}

impl SubAccountsService {
    pub async fn list(
        &self,
        req: crate::proto::auth::v1::ListSubaccountsRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ListSubaccountsResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.SubaccountService/ListSubaccounts",
            req,
            |req, opts| client.list_subaccounts_with_options(req, opts)
        )
    }
}

impl ResolveService {
    pub async fn resolve_account(
        &self,
        req: crate::proto::auth::v1::ResolveAccountRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ResolveAccountResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.ResolveService/ResolveAccount",
            req,
            |req, opts| client.resolve_account_with_options(req, opts)
        )
    }
}

impl AddressBookService {
    pub async fn list_books(
        &self,
        req: crate::proto::auth::v1::ListAddressBooksRequest,
    ) -> crate::errors::Result<crate::proto::auth::v1::ListAddressBooksResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/auth.v1.AddressBookService/ListAddressBooks",
            req,
            |req, opts| client.list_address_books_with_options(req, opts)
        )
    }
}

impl LifecycleService {
    pub async fn list_flows(
        &self,
        req: crate::proto::chain::lifecycle::v1::ListFlowsRequest,
    ) -> crate::errors::Result<crate::proto::chain::lifecycle::v1::ListFlowsResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/chain.lifecycle.v1.LifecycleReadService/ListFlows",
            req,
            |req, opts| client.list_flows_with_options(req, opts)
        )
    }
}

impl GuardSignerService {
    pub async fn get_status(
        &self,
        req: crate::proto::chain::guard::v1::GetGuardSignerStatusRequest,
    ) -> crate::errors::Result<crate::proto::chain::guard::v1::GetGuardSignerStatusResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/chain.guard.v1.GuardSignerService/GetGuardSignerStatus",
            req,
            |req, opts| client.get_guard_signer_status_with_options(req, opts)
        )
    }
}

impl LayoutService {
    pub async fn get_layouts(
        &self,
        req: crate::proto::layout::v1::GetLayoutsRequest,
    ) -> crate::errors::Result<crate::proto::layout::v1::GetLayoutsResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/layout.v1.LayoutService/GetLayouts",
            req,
            |req, opts| client.get_layouts_with_options(req, opts)
        )
    }
}

impl PolychartService {
    pub async fn get_market_layers(
        &self,
        req: crate::proto::polychart::v1::GetMarketLayersRequest,
    ) -> crate::errors::Result<crate::proto::polychart::v1::GetMarketLayersResponse> {
        let client = self.connect_client();
        signed_unary!(
            self,
            "/polychart.v1.PolychartService/GetMarketLayers",
            req,
            |req, opts| client.get_market_layers_with_options(req, opts)
        )
    }
}
