//! Policy subscriptions (`auth.v1.PolicyService` streams).

use super::{ServiceContext, scope};
use crate::errors::Result;

/// Realtime streams of the account's API-key and sub-account policies.
#[derive(Clone)]
pub struct PoliciesService {
    pub(crate) ctx: ServiceContext,
}

impl PoliciesService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }
}

impl PoliciesService {
    /// Subscribe to private subaccount policy updates; the same stream as
    /// [`Self::subscribe_subaccount_policies`].
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<crate::models::SubaccountPolicy>> {
        self.subscribe_subaccount_policies(account_id).await
    }

    /// Subscribe to the account's own sub-account policies
    /// (`private:auth:subaccount-policies:{account}:proto`). An API key needs
    /// `READ_SUBACCOUNT_POLICIES`; a token refusal carries that read's error
    /// code. Policies of sub-accounts the account reaches through a delegated
    /// role are on a separate stream for signed-in sessions only, which this
    /// API-key SDK does not subscribe to.
    pub async fn subscribe_subaccount_policies(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<crate::models::SubaccountPolicy>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:subaccount-policies:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(
                &channel,
                crate::codecs::decode::subaccount_policy_from_bytes,
            )
            .await
    }

    /// Subscribe to the account's own API-key policies
    /// (`private:auth:api-policies:{account}:proto`). An API key needs
    /// `READ_API_POLICIES`; a token refusal carries that read's error code.
    /// Policies of keys on sub-accounts administered through a delegated role
    /// are on a separate stream for signed-in sessions only.
    pub async fn subscribe_api_policies(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<crate::models::ApiPolicy>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:auth:api-policies:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::api_policy_from_bytes)
            .await
    }
}
