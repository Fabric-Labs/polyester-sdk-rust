//! Server build version (`system.v1.SystemService`): public, no credentials.

use super::unary;
use crate::codecs::decode::server_version_from_proto;
use crate::connect::system::v1::SystemServiceClient;
use crate::errors::Result;
use crate::models::ServerVersion;
use crate::proto::system::v1::GetVersionRequest;
use crate::transport::Factory;

/// Read the build the API host serves; see [`crate::Client::get_version`].
pub(crate) async fn get_version(factory: &Factory) -> Result<ServerVersion> {
    let client = SystemServiceClient::new(factory.transport(), factory.connect_config());
    // "/system.v1.SystemService/GetVersion"
    let resp = unary::await_public(client.get_version(GetVersionRequest::default()))
        .await?
        .into_owned();
    Ok(server_version_from_proto(&resp))
}
