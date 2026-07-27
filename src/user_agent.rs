//! Stable HTTP User-Agent for Polyester SDK clients.
//!
//! hyper omits User-Agent by default. That accidental omission currently
//! dodges Cloudflare error 1010 ("browser signature banned"), but a WAF rule
//! change that bans missing/unknown signatures would break every Rust client
//! with an error that does not mention Cloudflare. Always send an explicit
//! Polyester identity instead.

use crate::VERSION;

/// Explicit SDK identity for edge WAF allowlisting.
pub fn user_agent() -> String {
    format!("polyester-sdk-rust/{VERSION}")
}

/// True when a response body looks like Cloudflare error 1010.
pub fn is_cloudflare_browser_ban(body: &str) -> bool {
    let lowered = body.to_ascii_lowercase();
    if lowered.contains("error code: 1010") || lowered.contains("error code 1010") {
        return true;
    }
    lowered.contains("attention required") && lowered.contains("cloudflare")
}

/// Explain a WAF block that is not an API authentication failure.
pub fn cloudflare_1010_message() -> String {
    format!(
        "Request blocked by edge WAF (Cloudflare error 1010: browser signature banned). \
         This is not an API authentication failure. \
         Retry with User-Agent '{}' (set automatically by this SDK).",
        user_agent()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_is_polyester_identity() {
        let ua = user_agent();
        assert!(ua.starts_with("polyester-sdk-rust/"));
        assert!(!ua.contains("hyper"));
    }

    #[test]
    fn detects_cloudflare_1010() {
        let body = "<!DOCTYPE html><title>Attention Required! | Cloudflare</title>error code: 1010";
        assert!(is_cloudflare_browser_ban(body));
        assert!(!is_cloudflare_browser_ban(
            r#"{"code":"permission_denied"}"#
        ));
    }
}
