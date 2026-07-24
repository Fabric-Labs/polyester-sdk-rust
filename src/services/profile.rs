//! Profile service (Go `services/profile.go` parity).

use super::ServiceContext;
use crate::errors::Result;
use crate::models::AccountIdentity;

#[derive(Clone)]
pub struct ProfileService {
    ctx: ServiceContext,
}

impl ProfileService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Subscribe to public identity updates (requires `realtime` feature).
    pub async fn subscribe_identity(
        &self,
    ) -> Result<crate::realtime::TypedSubscription<AccountIdentity>> {
        self.ctx
            .realtime
            .subscribe_proto(
                "public:identity:updates:proto",
                crate::codecs::decode::account_identity_from_bytes,
            )
            .await
    }
}
