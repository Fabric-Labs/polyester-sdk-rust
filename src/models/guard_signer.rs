//! Guard signer models (Go `models/guard_signer.go` thin parity).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardSignerStatus {
    pub signer_wallet: String,
    pub status: String,
}
