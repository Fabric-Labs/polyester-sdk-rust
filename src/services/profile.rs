//! Profile service (Go `services/profile.go` parity).

use super::ServiceContext;
use super::unary;
use crate::codecs::decode::{profile_from_proto, username_history_from_proto};
use crate::connect::auth::v1::ProfileServiceClient;
use crate::errors::Result;
use crate::models::{AccountIdentity, UserProfile, UsernameHistoryList};
use crate::proto::auth::v1::{GetProfileRequest, GetUsernameHistoryRequest, UserProfilePatch};

#[derive(Clone)]
pub struct ProfileService {
    ctx: ServiceContext,
}

impl ProfileService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> ProfileServiceClient<crate::transport::SharedTransport> {
        ProfileServiceClient::new(
            self.ctx.factory.transport(true),
            self.ctx.factory.connect_config(true),
        )
    }

    pub async fn get(&self) -> Result<UserProfile> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ProfileService/GetProfile",
            GetProfileRequest::default(),
            |req, opts| client.get_profile_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(profile_from_proto(&resp))
    }

    pub async fn update(
        &self,
        username: &str,
        bio: &str,
        website: &str,
        twitter: &str,
        avatar_url: &str,
    ) -> Result<UserProfile> {
        let req = UserProfilePatch {
            username: Some(username.to_owned()),
            bio: Some(bio.to_owned()),
            website: Some(website.to_owned()),
            twitter: Some(twitter.to_owned()),
            avatar_url: Some(avatar_url.to_owned()),
            ..Default::default()
        };
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ProfileService/UpdateProfile",
            req,
            |req, opts| client.update_profile_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(profile_from_proto(&resp))
    }

    pub async fn get_username_history(&self) -> Result<UsernameHistoryList> {
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/auth.v1.ProfileService/GetUsernameHistory",
            GetUsernameHistoryRequest::default(),
            |req, opts| client.get_username_history_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(username_history_from_proto(&resp))
    }

    /// Subscribe to public identity updates (requires `realtime` feature).
    #[cfg(feature = "realtime")]
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
