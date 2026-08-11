//! Ed25519 API-key credentials and request signing.

use crate::errors::{Error, Result};
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const API_KEY_ID_ENV: &str = "POLYESTER_API_KEY_ID";
pub const API_PRIVATE_KEY_ENV: &str = "POLYESTER_API_PRIVATE_KEY";
pub const ACCOUNT_ID_ENV: &str = "POLYESTER_ACCOUNT_ID";

pub const HEADER_KEY_ID: &str = "X-API-KEY-ID";
pub const HEADER_TIMESTAMP: &str = "X-API-TIMESTAMP";
pub const HEADER_SIGNATURE: &str = "X-API-SIGNATURE";

/// Maximum amount an automatically allocated signing timestamp may lead the
/// local wall clock. The API accepts a 10-second freshness window; keeping the
/// client ceiling at 5 seconds leaves room for clock and network skew.
pub const MAX_SIGNING_FUTURE_SKEW_MS: u64 = 5_000;
const MAX_SIGNING_BACKPRESSURE: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct SigningTimestampAllocator {
    last_timestamp_ms: AtomicU64,
    async_gate: tokio::sync::Mutex<()>,
}

impl SigningTimestampAllocator {
    fn next(&self) -> Result<u64> {
        loop {
            let now = timestamp_ms_from(SystemTime::now())?;
            let ceiling = now
                .checked_add(MAX_SIGNING_FUTURE_SKEW_MS)
                .ok_or_else(|| Error::transport("signing timestamp ceiling overflow"))?;
            let observed = self.last_timestamp_ms.load(Ordering::Acquire);
            let candidate = if observed < now {
                now
            } else {
                observed
                    .checked_add(1)
                    .ok_or_else(|| Error::transport("signing timestamp sequence exhausted"))?
            };

            if candidate <= ceiling {
                if self
                    .last_timestamp_ms
                    .compare_exchange_weak(observed, candidate, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(candidate);
                }
                continue;
            }

            return Err(signing_capacity_error(candidate - ceiling));
        }
    }

    async fn next_async(&self) -> Result<u64> {
        // Serialize only timestamp allocation, not hashing, signing, or I/O.
        // Tokio's mutex queues waiters without blocking an executor thread.
        let started = Instant::now();
        let _gate = tokio::time::timeout(MAX_SIGNING_BACKPRESSURE, self.async_gate.lock())
            .await
            .map_err(|_| signing_capacity_error(1))?;
        loop {
            match self.next() {
                Ok(timestamp) => return Ok(timestamp),
                Err(Error::RateLimit { retry_after, .. })
                    if started.elapsed() < MAX_SIGNING_BACKPRESSURE =>
                {
                    let wait = Duration::from_secs_f64(retry_after.unwrap_or(0.001).max(0.001));
                    let remaining = MAX_SIGNING_BACKPRESSURE.saturating_sub(started.elapsed());
                    tokio::time::sleep(wait.min(remaining)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn signing_capacity_error(wait_ms: u64) -> Error {
    Error::RateLimit {
        message: "signing timestamp capacity exhausted; retry after clock advances".to_owned(),
        retry_after: Some(wait_ms.max(1) as f64 / 1_000.0),
        detail: None,
    }
}

fn allocator_for_key(key_id: &str) -> Arc<SigningTimestampAllocator> {
    static ALLOCATORS: OnceLock<Mutex<HashMap<String, Arc<SigningTimestampAllocator>>>> =
        OnceLock::new();
    let mut allocators = ALLOCATORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = allocators.get(key_id) {
        return existing.clone();
    }
    let allocator = Arc::new(SigningTimestampAllocator::default());
    allocators.insert(key_id.to_owned(), allocator.clone());
    allocator
}

/// API-key authentication material.
///
/// Credentials with the same key id share timestamp allocation within this
/// process. The signing protocol has no cross-process nonce, so use one API key
/// per process to avoid duplicate tuples across independent processes.
#[derive(Clone)]
pub struct Credentials {
    pub key_id: String,
    signing_key: SigningKey,
    timestamp_allocator: Arc<SigningTimestampAllocator>,
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
        let timestamp_allocator = allocator_for_key(&key_id);
        Ok(Self {
            key_id,
            signing_key,
            timestamp_allocator,
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
    ) -> Result<BTreeMap<String, String>> {
        let ts = match timestamp_ms {
            Some(value) => value.to_owned(),
            None => self.timestamp_allocator.next()?.to_string(),
        };
        let canonical = canonical_signing_string(&ts, method, raw_url, body)?;
        let sig = self.signing_key.sign(canonical.as_bytes());
        let mut headers = BTreeMap::new();
        headers.insert(HEADER_KEY_ID.to_owned(), self.key_id.clone());
        headers.insert(HEADER_TIMESTAMP.to_owned(), ts);
        headers.insert(HEADER_SIGNATURE.to_owned(), hex::encode(sig.to_bytes()));
        Ok(headers)
    }

    /// Sign exact business-payload bytes with this API key's Ed25519 key.
    ///
    /// This is distinct from HTTP request authentication. Callers must use a
    /// protocol-defined deterministic encoding and submit the same bytes'
    /// logical message unchanged.
    pub fn sign_payload(&self, payload: &[u8]) -> Vec<u8> {
        self.signing_key.sign(payload).to_bytes().to_vec()
    }

    /// Sign a request without blocking an async executor when the timestamp
    /// uniqueness window is temporarily full.
    ///
    /// All SDK network paths use this method. Callers using [`Self::sign_request`]
    /// directly receive a retryable capacity error instead of a blocking sleep.
    pub async fn sign_request_async(
        &self,
        method: &str,
        raw_url: &str,
        body: &[u8],
        timestamp_ms: Option<&str>,
    ) -> Result<BTreeMap<String, String>> {
        let ts = match timestamp_ms {
            Some(value) => value.to_owned(),
            None => self.timestamp_allocator.next_async().await?.to_string(),
        };
        let canonical = canonical_signing_string(&ts, method, raw_url, body)?;
        let sig = self.signing_key.sign(canonical.as_bytes());
        let mut headers = BTreeMap::new();
        headers.insert(HEADER_KEY_ID.to_owned(), self.key_id.clone());
        headers.insert(HEADER_TIMESTAMP.to_owned(), ts);
        headers.insert(HEADER_SIGNATURE.to_owned(), hex::encode(sig.to_bytes()));
        Ok(headers)
    }
}

fn timestamp_ms_from(now: SystemTime) -> Result<u64> {
    let elapsed = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::transport("system clock is before UNIX_EPOCH"))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| Error::transport("Unix timestamp milliseconds exceed u64 range"))
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
fn canonical_query_from_url(parsed: &url::Url) -> String {
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

pub fn canonical_query(raw_url: &str) -> Result<String> {
    let parsed = url::Url::parse(raw_url)
        .map_err(|err| Error::validation(format!("invalid signing URL: {err}")))?;
    Ok(canonical_query_from_url(&parsed))
}

pub fn canonical_signing_string(
    timestamp_ms: &str,
    method: &str,
    raw_url: &str,
    body: &[u8],
) -> Result<String> {
    let parsed = url::Url::parse(raw_url)
        .map_err(|err| Error::validation(format!("invalid signing URL: {err}")))?;
    let pathname = {
        let path = parsed.path();
        if path.is_empty() {
            "/".to_owned()
        } else {
            path.to_owned()
        }
    };
    let sum = Sha256::digest(body);
    Ok([
        timestamp_ms,
        &method.to_uppercase(),
        &pathname,
        &canonical_query_from_url(&parsed),
        &hex::encode(sum),
    ]
    .join("\n"))
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
        let got = canonical_query("https://api.example.test/path?b=2&a=hello world").unwrap();
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
            canonical_query(url).unwrap(),
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
            assert_eq!(canonical_query(url).unwrap(), want, "url={url}");
        }
    }

    #[test]
    fn canonical_signing_string_matches_contract() {
        let got = canonical_signing_string(
            "123",
            "post",
            "https://api.example.test/foo/bar?b=2&a=1",
            b"{}",
        )
        .unwrap();
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
        )
        .unwrap();
        let expected_hash = hex::encode(Sha256::digest(b""));
        assert!(s.contains(&expected_hash));
        assert!(s.starts_with("1700000000000\nPOST\n/orders.v1.OrdersService/CreateOrder\n\n"));
    }

    #[test]
    fn sign_request_returns_polyester_headers() {
        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("key_123", &seed).unwrap();
        let headers = creds
            .sign_request("POST", "https://api.example.test/foo", b"{}", Some("123"))
            .unwrap();
        assert_eq!(headers.get(HEADER_KEY_ID).unwrap(), "key_123");
        assert_eq!(headers.get(HEADER_TIMESTAMP).unwrap(), "123");
        assert_eq!(headers.get(HEADER_SIGNATURE).unwrap().len(), 128);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ten_thousand_identical_requests_get_unique_bounded_auth_tuples_without_blocking_runtime()
     {
        use std::collections::HashSet;

        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("key_123", &seed).unwrap();
        let before = timestamp_ms_from(SystemTime::now()).unwrap();
        let ticker = tokio::spawn(async {
            let mut previous = tokio::time::Instant::now();
            let mut largest_gap = Duration::ZERO;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let now = tokio::time::Instant::now();
                largest_gap = largest_gap.max(now.duration_since(previous));
                previous = now;
            }
            largest_gap
        });
        // Join in chunks so CPU-bound Ed25519 work cannot starve the ticker for
        // the whole 10k burst on a current-thread runtime. The allocator itself
        // must still yield under backpressure between chunks.
        let mut headers = Vec::with_capacity(10_000);
        for chunk_start in (0..10_000).step_by(250) {
            let chunk_end = (chunk_start + 250).min(10_000);
            let handles = (chunk_start..chunk_end)
                .map(|_| {
                    let creds = creds.clone();
                    tokio::spawn(async move {
                        let headers = creds
                            .sign_request_async("POST", "https://api.example.test/foo", b"{}", None)
                            .await
                            .unwrap();
                        let observed_at_ms = timestamp_ms_from(SystemTime::now()).unwrap();
                        (headers, observed_at_ms)
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                headers.push(handle.await.unwrap());
            }
            tokio::task::yield_now().await;
        }
        let largest_timer_gap = ticker.await.unwrap();
        assert_eq!(headers.len(), 10_000);
        assert!(
            largest_timer_gap < Duration::from_secs(1),
            "signing backpressure stalled the current-thread runtime for {largest_timer_gap:?}"
        );
        let mut timestamps = HashSet::with_capacity(headers.len());
        let mut signatures = HashSet::with_capacity(headers.len());
        for (item, observed_at_ms) in headers {
            let timestamp = item[HEADER_TIMESTAMP].parse::<u64>().unwrap();
            assert!(timestamp >= before);
            assert!(timestamp <= observed_at_ms + MAX_SIGNING_FUTURE_SKEW_MS);
            assert!(
                timestamps.insert(timestamp),
                "duplicate timestamp {timestamp}"
            );
            assert!(
                signatures.insert(item[HEADER_SIGNATURE].clone()),
                "duplicate signature for timestamp {timestamp}"
            );
        }
        assert_eq!(timestamps.len(), 10_000);
        assert_eq!(signatures.len(), 10_000);
    }

    #[tokio::test]
    async fn independently_constructed_credentials_share_process_timestamp_allocator() {
        let (seed, _) = generate_ed25519_keypair();
        let first = Credentials::new("shared-key", &seed).unwrap();
        let second = Credentials::new("shared-key", &seed).unwrap();
        assert!(Arc::ptr_eq(
            &first.timestamp_allocator,
            &second.timestamp_allocator
        ));
        let (first, second) = tokio::join!(
            first.sign_request_async("POST", "https://api.test/order", b"{}", None),
            second.sign_request_async("POST", "https://api.test/order", b"{}", None)
        );
        assert_ne!(
            first.unwrap()[HEADER_TIMESTAMP],
            second.unwrap()[HEADER_TIMESTAMP]
        );

        let transient = Credentials::new("process-lifetime-key", &seed).unwrap();
        let before_drop = transient
            .sign_request("POST", "https://api.test/order", b"{}", None)
            .unwrap()[HEADER_TIMESTAMP]
            .parse::<u64>()
            .unwrap();
        drop(transient);
        let reconstructed = Credentials::new("process-lifetime-key", &seed).unwrap();
        let after_drop = reconstructed
            .sign_request("POST", "https://api.test/order", b"{}", None)
            .unwrap()[HEADER_TIMESTAMP]
            .parse::<u64>()
            .unwrap();
        assert!(after_drop > before_drop);
    }

    #[test]
    fn synchronous_signing_capacity_returns_retryable_error_without_sleeping() {
        let allocator = SigningTimestampAllocator::default();
        // Seed far beyond the skew ceiling so wall-clock advance between store
        // and next() cannot reopen capacity under parallel CI load.
        allocator
            .last_timestamp_ms
            .store(u64::MAX - 2, Ordering::Release);
        let started = Instant::now();
        let error = allocator.next().unwrap_err();
        assert!(matches!(error, Error::RateLimit { .. }));
        assert!(error.is_retryable());
        assert!(error.retry_after().is_some_and(|value| value > 0.0));
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "synchronous capacity handling blocked for {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn round_trip_credentials() {
        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("ak_test", &seed).unwrap();
        let headers = creds
            .sign_request(
                "POST",
                "https://api-devnet.polyester.ai/orders.v1.OrdersService/CreateOrder",
                b"{}",
                Some("1"),
            )
            .unwrap();
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
    fn malformed_private_key_error_does_not_disclose_secret() {
        let malformed = "do-not-echo-this-private-key";
        let err = Credentials::new("ak_test", malformed).unwrap_err();
        let rendered = err.to_string();
        assert!(matches!(err, Error::Auth(_)));
        assert!(!rendered.contains(malformed));
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

    #[test]
    fn signing_rejects_unparseable_urls() {
        let (seed, _) = generate_ed25519_keypair();
        let creds = Credentials::new("ak_test", &seed).unwrap();
        let err = creds
            .sign_request("POST", "not a url", b"{}", Some("1"))
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(canonical_query("not a url").is_err());
        assert!(canonical_signing_string("1", "POST", "not a url", b"").is_err());
    }

    #[test]
    fn pre_epoch_clock_is_an_error_not_a_panic() {
        let before_epoch = UNIX_EPOCH
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            timestamp_ms_from(before_epoch),
            Err(Error::Transport(_))
        ));
    }
}
