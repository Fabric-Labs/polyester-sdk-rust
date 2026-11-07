//! Guard signer models (Go `models/guard_signer.go` thin parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardSignerStatus {
    pub signer_wallet: String,
    pub status: String,
}

/// A signed protected-action approval. Submit `nonce_space` and
/// `deadline_unix` unchanged with `signature`; they are the signed values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardApproval {
    pub approval_id: String,
    pub signature: String,
    /// Nonce space the approval was signed in. Normal flows use 0.
    pub nonce_space: u64,
    /// Approval deadline as a unix time in seconds.
    pub deadline_unix: u64,
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
    /// Signed approval needed to submit the rotation; `None` when the API
    /// returns none.
    pub approval: Option<GuardApproval>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSignProtectedActionsResult {
    pub approvals: Vec<GuardApproval>,
}
