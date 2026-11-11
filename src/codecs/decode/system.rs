//! Server build version decoder.

use crate::models::ServerVersion;
use crate::proto::system::v1::GetVersionResponse;

pub fn server_version_from_proto(msg: &GetVersionResponse) -> ServerVersion {
    ServerVersion {
        version: msg.version.clone(),
        commit: msg.commit.clone(),
        environment: msg.environment.clone(),
    }
}
