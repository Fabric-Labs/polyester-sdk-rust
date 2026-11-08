//! Guard signer models (Go `models/guard_signer.go` thin parity).

use std::fmt;

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

/// The exported guard signer wallet key.
///
/// [`Debug`] redacts `encrypted_private_key`, the private key exactly as the
/// API returns it, so logging the result cannot leak it. Read the field
/// explicitly when you need the key.
#[derive(Clone, PartialEq, Eq)]
pub struct ExportGuardSignerWalletResult {
    pub encrypted_private_key: String,
}

impl fmt::Debug for ExportGuardSignerWalletResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportGuardSignerWalletResult")
            .field("encrypted_private_key", &"[REDACTED]")
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// An obviously fake marker standing in for an exported private key.
    const FAKE_EXPORTED_KEY: &str = "fake-exported-guard-key-marker";

    fn assert_redacted(path: &str, rendered: &str, markers: &[&str]) {
        for marker in markers {
            assert!(
                !rendered.contains(marker),
                "{path} leaked key material: {rendered}"
            );
        }
        assert!(rendered.contains("[REDACTED]"), "{path}: {rendered}");
    }

    #[test]
    fn export_result_debug_redacts_private_key() {
        let result = ExportGuardSignerWalletResult {
            encrypted_private_key: FAKE_EXPORTED_KEY.into(),
        };
        assert_eq!(result.encrypted_private_key, FAKE_EXPORTED_KEY);
        for (path, rendered) in [
            ("debug", format!("{result:?}")),
            ("pretty debug", format!("{result:#?}")),
            ("option", format!("{:?}", Some(&result))),
            ("vec", format!("{:#?}", vec![result.clone()])),
        ] {
            assert_redacted(path, &rendered, &[FAKE_EXPORTED_KEY]);
        }
    }
}
