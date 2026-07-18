//! Guard signer decoders.

use crate::models::{
    BatchSignProtectedActionsResult, CreateGuardSignerWalletResult, ExportGuardSignerWalletResult,
    GuardApproval, GuardSignerStatus, RotateGuardSignerWalletResult,
};
use crate::proto::chain::guard::v1::{
    BatchSignProtectedActionsResponse, CreateGuardSignerWalletResponse,
    ExportGuardSignerWalletResponse, GetGuardSignerStatusResponse, GuardApproval as ProtoApproval,
    GuardSignerStatus as ProtoStatus, RotateGuardSignerWalletResponse, SignProtectedActionResponse,
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

pub fn create_wallet_from_proto(
    msg: &CreateGuardSignerWalletResponse,
) -> CreateGuardSignerWalletResult {
    CreateGuardSignerWalletResult {
        signer_wallet: msg.signer_address.clone(),
    }
}

pub fn guard_approval_from_proto(msg: &ProtoApproval) -> GuardApproval {
    GuardApproval {
        approval_id: String::new(),
        signature: hex::encode(&msg.signature),
    }
}

pub fn sign_protected_action_from_proto(
    msg: &SignProtectedActionResponse,
) -> Option<GuardApproval> {
    msg.approval.as_option().map(guard_approval_from_proto)
}

pub fn batch_sign_from_proto(
    msg: &BatchSignProtectedActionsResponse,
) -> BatchSignProtectedActionsResult {
    BatchSignProtectedActionsResult {
        approvals: msg
            .approvals
            .iter()
            .map(guard_approval_from_proto)
            .collect(),
    }
}

pub fn rotate_wallet_from_proto(
    msg: &RotateGuardSignerWalletResponse,
) -> RotateGuardSignerWalletResult {
    RotateGuardSignerWalletResult {
        signer_wallet: msg.new_signer_address.clone(),
    }
}

pub fn export_wallet_from_proto(
    msg: &ExportGuardSignerWalletResponse,
) -> ExportGuardSignerWalletResult {
    ExportGuardSignerWalletResult {
        encrypted_private_key: msg.private_key.clone(),
    }
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
    fn create_and_sign_decode() {
        let created = create_wallet_from_proto(&CreateGuardSignerWalletResponse {
            signer_address: "0xabc".into(),
            ..Default::default()
        });
        assert_eq!(created.signer_wallet, "0xabc");

        let signed = sign_protected_action_from_proto(&SignProtectedActionResponse {
            approval: ProtoApproval {
                signature: vec![0xde, 0xad],
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .expect("approval");
        assert_eq!(signed.signature, "dead");
    }
}
