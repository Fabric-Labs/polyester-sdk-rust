//! Guard signer decoders.

use crate::models::GuardSignerStatus;
use crate::proto::chain::guard::v1::{
    GetGuardSignerStatusResponse, GuardSignerStatus as ProtoStatus,
};

pub fn guard_signer_status_from_proto(msg: &ProtoStatus) -> GuardSignerStatus {
    let status = if msg.initialized {
        "initialized"
    } else {
        "uninitialized"
    };
    GuardSignerStatus {
        signer_wallet: msg.signer_address.clone(),
        status: status.into(),
    }
}

pub fn status_from_proto(msg: &GetGuardSignerStatusResponse) -> Option<GuardSignerStatus> {
    msg.status.as_option().map(guard_signer_status_from_proto)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_initialized_label() {
        let msg = GetGuardSignerStatusResponse {
            status: ProtoStatus {
                signer_address: "0xsigner".into(),
                initialized: true,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let status = status_from_proto(&msg).expect("status");
        assert_eq!(status.signer_wallet, "0xsigner");
        assert_eq!(status.status, "initialized");
    }

    #[test]
    fn status_uninitialized_label() {
        let msg = GetGuardSignerStatusResponse {
            status: ProtoStatus {
                initialized: false,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let status = status_from_proto(&msg).expect("status");
        assert_eq!(status.status, "uninitialized");
    }
}
