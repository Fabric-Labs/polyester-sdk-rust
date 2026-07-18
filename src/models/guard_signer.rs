//! Guard signer models (Go `models/guard_signer.go` thin parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardSignerStatus {
    pub signer_wallet: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardApproval {
    pub approval_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateGuardSignerWalletResult {
    pub signer_wallet: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportGuardSignerWalletResult {
    pub encrypted_private_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotateGuardSignerWalletResult {
    pub signer_wallet: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSignProtectedActionsResult {
    pub approvals: Vec<GuardApproval>,
}
