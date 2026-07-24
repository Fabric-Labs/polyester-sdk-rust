//! Ed25519 API-key credentials and request signing.

use crate::errors::{Error, Result};
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub const API_KEY_ID_ENV: &str = "POLYESTER_API_KEY_ID";
pub const API_PRIVATE_KEY_ENV: &str = "POLYESTER_API_PRIVATE_KEY";
pub const ACCOUNT_ID_ENV: &str = "POLYESTER_ACCOUNT_ID";

pub const HEADER_KEY_ID: &str = "X-API-KEY-ID";
pub const HEADER_TIMESTAMP: &str = "X-API-TIMESTAMP";
pub const HEADER_SIGNATURE: &str = "X-API-SIGNATURE";

/// API-key authentication material.
#[derive(Clone)]
pub struct Credentials {
    pub key_id: String,
    signing_key: SigningKey,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("key_id", &self.key_id)
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

impl Credentials {
    pub fn new(key_id: impl Into<String>, private_key_hex: &str) -> Result<Self> {
        let key_id = key_id.into().trim().to_owned();
        if key_id.is_empty() {
            return Err(Error::auth("API key ID must not be empty"));
        }
        let seed = normalize_private_key(private_key_hex)?;
        let signing_key = SigningKey::from_bytes(&seed);
        Ok(Self {
            key_id,
            signing_key,
        })
    }

    /// Load credentials from explicit values and/or environment.
    pub fn load(
        api_key_id: Option<&str>,
        api_private_key: Option<&str>,
        from_env: bool,
    ) -> Result<Option<Self>> {
        let mut key_id = api_key_id.unwrap_or("").trim().to_owned();
        let mut private = api_private_key.unwrap_or("").trim().to_owned();
        if from_env {
            if key_id.is_empty() {
                key_id = std::env::var(API_KEY_ID_ENV)
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
            }
            if private.is_empty() {
                private = std::env::var(API_PRIVATE_KEY_ENV)
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
            }
        }
        if key_id.is_empty() && private.is_empty() {
            return Ok(None);
        }
        if key_id.is_empty() || private.is_empty() {
            let msg = if from_env {
                "Both POLYESTER_API_KEY_ID and POLYESTER_API_PRIVATE_KEY are required"
            } else {
                "Both api_key_id and api_private_key are required"
            };
            return Err(Error::auth(msg));
        }
        Ok(Some(Self::new(key_id, &private)?))
    }

    pub fn sign_request(
        &self,
        method: &str,
        raw_url: &str,
        body: &[u8],
        timestamp_ms: Option<&str>,
    ) -> BTreeMap<String, String> {
        let ts = timestamp_ms.map(str::to_owned).unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis()
                .to_string()
        });
        let canonical = canonical_signing_string(&ts, method, raw_url, body);
        let sig = self.signing_key.sign(canonical.as_bytes());
        let mut headers = BTreeMap::new();
        headers.insert(HEADER_KEY_ID.to_owned(), self.key_id.clone());
        headers.insert(HEADER_TIMESTAMP.to_owned(), ts);
        headers.insert(HEADER_SIGNATURE.to_owned(), hex::encode(sig.to_bytes()));
        headers
    }
}

/// Accept a 64-char hex Ed25519 seed (32 bytes).
pub fn normalize_private_key(value: &str) -> Result<[u8; 32]> {
    let private = hex::decode(value.trim())
        .map_err(|_| Error::auth("API private key must be a valid hex string or raw bytes"))?;
    if private.len() != 32 {
        return Err(Error::auth(
            "Ed25519 API private key must be exactly 32 bytes",
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&private);
    Ok(seed)
}

pub fn account_id_from_env() -> Option<String> {
    let v = std::env::var(ACCOUNT_ID_ENV).ok()?.trim().to_owned();
    if v.is_empty() { None } else { Some(v) }
}

/// RFC 3986 unreserved characters that must remain literal in query components.
///
/// Matches Python `urllib.parse.quote(..., safe="")` and Go `url.QueryEscape`
/// (with `+` normalized to `%20`): `ALPHA / DIGIT / "-" / "." / "_" / "~"`.
/// Using `percent_encoding::NON_ALPHANUMERIC` alone is wrong — it encodes `-` as
/// `%2D`, which breaks API-key signatures for channels like `api-keys`.
const QUERY_COMPONENT_ASCII_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Percent-encode a query component for Polyester API-key canonicalization.
pub fn encode_query_component(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, QUERY_COMPONENT_ASCII_SET).to_string()
}

/// Sort and percent-encode query parameters (Python `quote(safe="")` parity).
pub fn canonical_query(raw_url: &str) -> String {
    let Ok(parsed) = url::Url::parse(raw_url) else {
        return String::new();
    };
    let mut pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .into_iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                encode_query_component(&k),
                encode_query_component(&v)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn canonical_signing_string(
    timestamp_ms: &str,
    method: &str,
    raw_url: &str,
    body: &[u8],
) -> String {
    let pathname = match url::Url::parse(raw_url) {
        Ok(u) => {
            let path = u.path();
            if path.is_empty() {
                "/".to_owned()
            } else {
                path.to_owned()
            }
        }
        Err(_) => "/".to_owned(),
    };
    let sum = Sha256::digest(body);
    [
        timestamp_ms,
        &method.to_uppercase(),
        &pathname,
        &canonical_query(raw_url),
        &hex::encode(sum),
    ]
    .join("\n")
}

pub fn request_url(api_base: &str, procedure: &str) -> String {
    let base = api_base.trim_end_matches('/');
    let proc = if procedure.starts_with('/') {
        procedure.to_owned()
    } else {
        format!("/{procedure}")
    };
    format!("{base}{proc}")
}

/// Generate a fresh Ed25519 keypair (hex seed + public key).
pub fn generate_ed25519_keypair() -> (String, String) {
    use ed25519_dalek::SigningKey;
    use rand_core::OsRng;
    let signing = SigningKey::generate(&mut OsRng);
    let seed = signing.to_bytes();
    let public = signing.verifying_key().to_bytes();
    (hex::encode(seed), hex::encode(public))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_query_sorts_and_encodes_values() {
        let got = canonical_query("https://api.example.test/path?b=2&a=hello world");
        assert_eq!(got, "a=hello%20world&b=2");
    }

    #[test]
    fn encode_query_component_preserves_rfc3986_unreserved() {
        assert_eq!(encode_query_component("api-keys"), "api-keys");
        assert_eq!(encode_query_component("a_b.c~d-e"), "a_b.c~d-e");
        assert_eq!(encode_query_component("hello world"), "hello%20world");
        assert_eq!(encode_query_component("a+b"), "a%2Bb");
        assert_eq!(
            encode_query_component("private:auth:api-keys:account:proto"),
            "private%3Aauth%3Aapi-keys%3Aaccount%3Aproto"
        );
    }

    #[test]
    fn canonical_query_preserves_hyphens_in_channel_param() {
        let url =
            "https://api.example.test/v1/rt/subscribe?channel=private:auth:api-keys:account:proto";
        assert_eq!(
            canonical_query(url),
            "channel=private%3Aauth%3Aapi-keys%3Aaccount%3Aproto"
        );
    }

    #[test]
    fn canonical_query_shared_vectors() {
        // Cross-language parity vectors (Python quote(safe="") / Go QueryEscape+%20).
        // Note: bare `+` in a query string is form-decoded as space before re-encoding.
        let cases = [
            (
                "https://api.example.test/x?z=1&a=hello world&m=a+b",
                "a=hello%20world&m=a%20b&z=1",
            ),
            (
                "https://api.example.test/x?z=1&a=hello%20world&m=a%2Bb",
                "a=hello%20world&m=a%2Bb&z=1",
            ),
            ("https://api.example.test/x?b=&a=1", "a=1&b="),
            ("https://api.example.test/x?a=1&a=2&b=0", "a=1&a=2&b=0"),
            (
                "https://api.example.test/x?path=foo/bar&name=a_b.c~d-e",
                "name=a_b.c~d-e&path=foo%2Fbar",
            ),
            (
                "https://api.example.test/x?msg=%E2%9C%93&plain=ok",
                "msg=%E2%9C%93&plain=ok",
            ),
        ];
        for (url, want) in cases {
            assert_eq!(canonical_query(url), want, "url={url}");
        }
    }

    #[test]
    fn canonical_signing_string_matches_contract() {
        let got = canonical_signing_string(
            "123",
            "post",
            "https://api.example.test/foo/bar?b=2&a=1",
            b"{}",
        );
        let want = "123\nPOST\n/foo/bar\na=1&b=2\n44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
        assert_eq!(got, want);
    }

    #[test]
    fn canonical_string_empty_body() {
        let s = canonical_signing_string(
            "1700000000000",
            "POST",
            "https://api-devnet.polyester.ai/orders.v1.OrdersService/CreateOrder",
            b"",
        );
        let expected_hash = hex::encode(Sha256::digest(b""));
        assert!(s.contains(&expected_hash));
        assert!(s.starts_with("1700000000000\nPOST\n/orders.v1.OrdersService/CreateOrder\n\n"));
    }

    #[test]
    fn sign_request_returns_polyester_headers() {
        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("key_123", &seed).unwrap();
        let headers =
            creds.sign_request("POST", "https://api.example.test/foo", b"{}", Some("123"));
        assert_eq!(headers.get(HEADER_KEY_ID).unwrap(), "key_123");
        assert_eq!(headers.get(HEADER_TIMESTAMP).unwrap(), "123");
        assert_eq!(headers.get(HEADER_SIGNATURE).unwrap().len(), 128);
    }

    #[test]
    fn round_trip_credentials() {
        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("ak_test", &seed).unwrap();
        let headers = creds.sign_request(
            "POST",
            "https://api-devnet.polyester.ai/orders.v1.OrdersService/CreateOrder",
            b"{}",
            Some("1"),
        );
        assert_eq!(headers.get(HEADER_KEY_ID).unwrap(), "ak_test");
        assert_eq!(headers.get(HEADER_TIMESTAMP).unwrap(), "1");
        assert_eq!(headers.get(HEADER_SIGNATURE).unwrap().len(), 128);
    }

    #[test]
    fn credentials_reject_empty_key_id() {
        let (seed, _) = generate_ed25519_keypair();
        let err = Credentials::new("  ", &seed).unwrap_err();
        assert!(matches!(err, Error::Auth(_)));
    }

    #[test]
    fn load_credentials_requires_both() {
        let err = Credentials::load(Some("ak_test"), Some(""), false).unwrap_err();
        assert!(matches!(err, Error::Auth(_)));
    }

    #[test]
    fn load_credentials_none_when_empty() {
        assert!(Credentials::load(None, None, false).unwrap().is_none());
    }

    #[test]
    fn request_url_joins_base_and_procedure() {
        assert_eq!(
            request_url(
                "https://api.example.test/",
                "orders.v1.OrdersService/CreateOrder"
            ),
            "https://api.example.test/orders.v1.OrdersService/CreateOrder"
        );
        assert_eq!(
            request_url("https://api.example.test", "/auth.v1.AuthService/Me"),
            "https://api.example.test/auth.v1.AuthService/Me"
        );
    }
}
