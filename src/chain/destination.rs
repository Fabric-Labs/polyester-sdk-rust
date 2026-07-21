//! Withdraw destination encoding matching TypeScript polyester-features.

/// UTF-8 bytes of the normalized address (lowercase when not case-sensitive).
///
/// Matches TS `encodeWithdrawDestination` / `evmUtf8ToHex` (hex of UTF-8),
/// not a 20-byte pubkey decode.
pub fn encode_withdraw_destination(address: &str, is_case_sensitive: bool) -> Vec<u8> {
    if is_case_sensitive {
        address.as_bytes().to_vec()
    } else {
        address.to_ascii_lowercase().into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_case_when_sensitive() {
        let address = "Tb1QCaseSensitiveAddress";
        assert_eq!(
            encode_withdraw_destination(address, true),
            address.as_bytes()
        );
    }

    #[test]
    fn lowercases_when_insensitive() {
        let address = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD";
        assert_eq!(
            encode_withdraw_destination(address, false),
            b"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
    }
}
