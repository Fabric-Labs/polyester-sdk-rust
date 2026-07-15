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
