//! Whether a subscription the server ended may resume.
//!
//! The server can end a subscription to a sub-account's channels after access
//! was granted again. A fresh read of `ListSubaccounts` decides: listed with a
//! role means subscribe again with a fresh token. Requests are paced per client
//! so many subscriptions ending together cannot flood the list or token paths.

use super::{Client, auth, lock_unpoisoned};
use crate::codecs::scalars::id_to_u64;
use crate::errors::{Error, Result, SubscriptionEndReason};
use buffa::Message;
use rand_core::{OsRng, RngCore};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

pub(super) const SUBACCOUNTS_CHANNEL_PREFIX: &str = "private:auth:subaccounts:";
const SUBACCOUNTS_LIST_LABEL: &str = "list sub-accounts";
const MAX_SUBACCOUNT_LIST_BYTES: usize = 4 * 1024 * 1024;
/// A server unsubscribe code at or above this asks the client to subscribe
/// again; a lower one ends the subscription.
const UNSUBSCRIBE_TEMPORARY_FROM: u32 = 2500;
/// The private channels a sub-account's access governs. The ID follows, then
/// an optional `:proto`.
const SUBACCOUNT_CHANNEL_PREFIXES: [&str; 6] = [
    "private:ledger:balances:",
    "private:ledger:transfers:",
    "private:spot:trades:",
    "private:spot:triggers:events:",
    "private:spot:triggers:",
    "private:chain:lifecycle:flows:",
];

/// Bounds on resuming subscriptions the server ended.
#[derive(Clone, Copy)]
pub(super) struct ResubscribeTiming {
    /// Hard limit on attempts without staying up for `stable_after`.
    pub max_attempts: u32,
    /// First per-subscription delay; it doubles per attempt.
    pub initial_backoff: Duration,
    /// How long a resumed subscription must stay up to earn a fresh budget.
    pub stable_after: Duration,
    /// Paces attempts across one client; each costs two token requests.
    pub attempt_spacing: Duration,
    /// Paces `ListSubaccounts` across one client.
    pub read_interval: Duration,
}

impl Default for ResubscribeTiming {
    fn default() -> Self {
        Self {
            max_attempts: 6,
            initial_backoff: Duration::from_millis(500),
            stable_after: Duration::from_secs(60),
            attempt_spacing: Duration::from_millis(200),
            read_interval: Duration::from_secs(1),
        }
    }
}

/// The server's unsubscribe for one connection.
#[derive(Clone)]
pub(super) struct ServerUnsubscribe {
    pub code: u32,
    pub reason: String,
}

impl ServerUnsubscribe {
    /// The error ending a connection: a temporary code asks for an ordinary
    /// reconnect; any other enters the resume decision.
    pub fn into_connection_error(self, channel: &str) -> Error {
        if self.code >= UNSUBSCRIBE_TEMPORARY_FROM {
            return Error::realtime(format!(
                "server asked to subscribe to {channel:?} again (code {}: {})",
                self.code, self.reason
            ));
        }
        self.stop(channel, SubscriptionEndReason::NotResubscribable, None)
    }

    pub fn stop(
        &self,
        channel: &str,
        reason: SubscriptionEndReason,
        cause: Option<Error>,
    ) -> Error {
        Error::SubscriptionEnded {
            channel: channel.to_owned(),
            code: self.code,
            server_reason: self.reason.clone(),
            reason,
            cause: cause.map(Box::new),
        }
    }
}

/// The sub-account ID of a channel its access governs.
pub(super) fn subaccount_channel_id(channel: &str) -> Option<u64> {
    let rest = SUBACCOUNT_CHANNEL_PREFIXES
        .iter()
        .find_map(|prefix| channel.strip_prefix(prefix))?;
    let rest = rest.strip_suffix(":proto").unwrap_or(rest);
    if rest.is_empty() || rest.contains(':') {
        return None;
    }
    id_to_u64(rest, "account_id").ok()
}

/// One subscription's bounded budget for resuming.
pub(super) struct ResubscribeAttempt {
    pub ended: ServerUnsubscribe,
    pub ready: bool,
    pub last: Option<Error>,
    ready_at: Option<Instant>,
    attempts: u32,
    backoff_cap: Duration,
}

impl ResubscribeAttempt {
    pub fn note_ready(&mut self, at: Instant) {
        self.ready = true;
        self.ready_at = Some(at);
        self.last = None;
    }

    fn next_delay(&mut self) -> Duration {
        let half = self.backoff_cap / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX).max(1);
        let delay = half + Duration::from_nanos(OsRng.next_u64() % spread);
        self.backoff_cap = self.backoff_cap.saturating_mul(2);
        delay
    }
}

type ListedRead = watch::Receiver<Option<Result<Arc<HashSet<u64>>>>>;

#[derive(Default)]
struct AccessState {
    removed: HashSet<u64>,
    next_read: Option<ListedRead>,
    last_read: Option<Instant>,
    next_attempt_at: Option<Instant>,
}

/// What one client knows about sub-account access, and the pacing of asking.
pub(super) struct SubaccountAccess {
    timing: ResubscribeTiming,
    state: Mutex<AccessState>,
}

impl SubaccountAccess {
    pub fn new(timing: ResubscribeTiming) -> Self {
        Self {
            timing,
            state: Mutex::new(AccessState::default()),
        }
    }

    /// The budget for a new unsubscribe: kept unless the subscription stayed up.
    pub fn attempt_after(
        &self,
        current: Option<ResubscribeAttempt>,
        ended: ServerUnsubscribe,
    ) -> ResubscribeAttempt {
        let keep = current.filter(|attempt| {
            !attempt.ready
                || attempt
                    .ready_at
                    .is_none_or(|at| at.elapsed() < self.timing.stable_after)
        });
        match keep {
            Some(mut attempt) => {
                attempt.ended = ended;
                attempt.ready = false;
                attempt
            }
            None => ResubscribeAttempt {
                ended,
                ready: false,
                last: None,
                ready_at: None,
                attempts: 0,
                backoff_cap: self.timing.initial_backoff,
            },
        }
    }

    /// Apply one `auth.v1.Subaccount` update, in arrival order.
    pub fn note_update(&self, payload: &[u8]) {
        let Ok(update) = crate::proto::auth::v1::Subaccount::decode_from_slice(payload) else {
            return;
        };
        if update.id == 0 {
            return;
        }
        let mut state = lock_unpoisoned(&self.state);
        if update.removed {
            state.removed.insert(update.id);
        } else {
            state.removed.remove(&update.id);
        }
    }

    fn seen_removed(&self, id: u64) -> bool {
        lock_unpoisoned(&self.state).removed.contains(&id)
    }

    /// Wait until `channel` may be subscribed again. Returns the error that ends
    /// the subscription, or `None` to subscribe; a stop request returns `None`
    /// and the caller's loop exits.
    pub async fn await_resume(
        &self,
        client: &Client,
        channel: &str,
        attempt: &mut ResubscribeAttempt,
        stop: &mut watch::Receiver<bool>,
    ) -> Option<Error> {
        let Some(id) = subaccount_channel_id(channel).filter(|_| client.credentials.is_some())
        else {
            return Some(attempt.ended.stop(
                channel,
                SubscriptionEndReason::NotResubscribable,
                None,
            ));
        };
        loop {
            if self.seen_removed(id) {
                return Some(attempt.ended.stop(
                    channel,
                    SubscriptionEndReason::AccessRemoved,
                    None,
                ));
            }
            if attempt.attempts >= self.timing.max_attempts {
                let last = attempt.last.take();
                return Some(attempt.ended.stop(
                    channel,
                    SubscriptionEndReason::RetriesExhausted,
                    last,
                ));
            }
            attempt.attempts += 1;
            let delay = attempt.next_delay();
            if !sleep_or_stop(delay, stop).await {
                return None;
            }
            if self.seen_removed(id) {
                return Some(attempt.ended.stop(
                    channel,
                    SubscriptionEndReason::AccessRemoved,
                    None,
                ));
            }
            let read = tokio::select! {
                _ = stop.changed() => return None,
                read = self.read(client) => read,
            };
            match read {
                Err(err) if auth::is_refusal(&err) => {
                    return Some(attempt.ended.stop(
                        channel,
                        SubscriptionEndReason::ListRefused,
                        Some(err),
                    ));
                }
                Err(err) => attempt.last = Some(err),
                // A removal that arrived during the read is newer than its answer.
                Ok(_) if self.seen_removed(id) => {
                    return Some(attempt.ended.stop(
                        channel,
                        SubscriptionEndReason::AccessRemoved,
                        None,
                    ));
                }
                Ok(listed) if !listed.contains(&id) => {
                    return Some(attempt.ended.stop(
                        channel,
                        SubscriptionEndReason::NotListed,
                        None,
                    ));
                }
                Ok(_) => {
                    let wait = self.reserve_attempt();
                    if sleep_or_stop(wait, stop).await && self.seen_removed(id) {
                        return Some(attempt.ended.stop(
                            channel,
                            SubscriptionEndReason::AccessRemoved,
                            None,
                        ));
                    }
                    return None;
                }
            }
        }
    }

    /// Take the client's next attempt slot and return the wait for it.
    fn reserve_attempt(&self) -> Duration {
        let now = Instant::now();
        let mut state = lock_unpoisoned(&self.state);
        let slot = state.next_attempt_at.map_or(now, |at| at.max(now));
        state.next_attempt_at = Some(slot + self.timing.attempt_spacing);
        slot - now
    }

    /// Listed sub-accounts from a request started after this call, shared with
    /// concurrent callers and paced across the client.
    async fn read(&self, client: &Client) -> Result<Arc<HashSet<u64>>> {
        let mut read = {
            let mut state = lock_unpoisoned(&self.state);
            match &state.next_read {
                Some(read) => read.clone(),
                None => {
                    let (tx, read) = watch::channel(None);
                    state.next_read = Some(read.clone());
                    let wait = state
                        .last_read
                        .map(|at| {
                            (at + self.timing.read_interval)
                                .saturating_duration_since(Instant::now())
                        })
                        .unwrap_or_default();
                    let access = Arc::clone(&client.access);
                    let client = client.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(wait).await;
                        {
                            // Callers from here on need a read that starts after them.
                            let mut state = lock_unpoisoned(&access.state);
                            state.next_read = None;
                            state.last_read = Some(Instant::now());
                        }
                        let listed = fetch_listed(&client).await.map(Arc::new);
                        let _ = tx.send(Some(listed));
                    });
                    read
                }
            }
        };
        let listed = read
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::realtime(format!("{SUBACCOUNTS_LIST_LABEL}: read stopped")))?;
        listed.clone().unwrap_or_else(|| {
            Err(Error::realtime(format!(
                "{SUBACCOUNTS_LIST_LABEL}: no result"
            )))
        })
    }
}

async fn sleep_or_stop(delay: Duration, stop: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = stop.changed() => !*stop.borrow(),
        _ = tokio::time::sleep(delay) => true,
    }
}

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    subaccounts: Vec<ListedSubaccount>,
}

#[derive(Deserialize)]
struct ListedSubaccount {
    id: serde_json::Value,
    #[serde(default)]
    role: serde_json::Value,
    #[serde(default)]
    removed: bool,
}

/// The sub-accounts this key can access with a role.
async fn fetch_listed(client: &Client) -> Result<HashSet<u64>> {
    let creds = client
        .credentials
        .as_ref()
        .ok_or_else(|| Error::auth("private channel requires credentials"))?;
    crate::transport::validate_http_url(&client.api_url, client.allow_insecure_http)?;
    let url = format!(
        "{}/v1/auth/subaccounts",
        client.api_url.trim_end_matches('/')
    );
    let body = auth::signed_get(
        creds,
        &url,
        SUBACCOUNTS_LIST_LABEL,
        MAX_SUBACCOUNT_LIST_BYTES,
        client.timeout,
        client.allow_insecure_http,
    )
    .await?;
    let invalid = || Error::realtime(format!("{SUBACCOUNTS_LIST_LABEL}: invalid response"));
    let payload: ListResponse = serde_json::from_slice(&body).map_err(|_| invalid())?;
    let mut listed = HashSet::with_capacity(payload.subaccounts.len());
    for entry in payload.subaccounts {
        let id = match &entry.id {
            // The list reports IDs in decimal; a decimal with no zero digit also
            // reads as base58, so decimal comes first.
            serde_json::Value::String(id) => match id.parse::<u64>() {
                Ok(id) => id,
                Err(_) => id_to_u64(id, "id").map_err(|_| invalid())?,
            },
            serde_json::Value::Number(id) => id.as_u64().ok_or_else(invalid)?,
            _ => return Err(invalid()),
        };
        if !entry.removed && has_role(&entry.role) {
            listed.insert(id);
        }
    }
    Ok(listed)
}

fn has_role(role: &serde_json::Value) -> bool {
    match role {
        serde_json::Value::String(role) => {
            !role.is_empty() && role != "SUBACCOUNT_ROLE_UNSPECIFIED"
        }
        serde_json::Value::Number(role) => role.as_u64() != Some(0),
        _ => false,
    }
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/hardening_support/centrifugo.rs"]
mod centrifugo;

#[cfg(test)]
mod tests {
    use super::centrifugo::{ApiMock, Centrifugo, centrifugo_publication, unsubscribe_frame};
    use super::*;
    use crate::auth::Credentials;
    use crate::codecs::scalars::format_id;
    use crate::proto::auth::v1::{Subaccount, SubaccountRole};
    use crate::realtime::TypedSubscription;

    const SUB_ID: u64 = 2_305_843_009_213_693_953;
    const LIST: &str = "/v1/auth/subaccounts";
    const SUBSCRIBE: &str = "/v1/rt/subscribe";
    const CONNECT: &str = "/v1/rt/token";
    const WAIT: Duration = Duration::from_secs(10);
    const FAST: ResubscribeTiming = ResubscribeTiming {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        stable_after: Duration::from_secs(3600),
        attempt_spacing: Duration::from_millis(1),
        read_interval: Duration::from_millis(1),
    };

    fn channel(id: u64) -> String {
        format!("private:ledger:balances:{}:proto", format_id(id))
    }

    fn listed_body(ids: &[u64]) -> String {
        serde_json::json!({"subaccounts": ids
            .iter()
            .map(|id| serde_json::json!({"id": id.to_string(), "role": "VIEWER"}))
            .collect::<Vec<_>>()})
        .to_string()
    }

    fn respond(ids: Vec<u64>) -> impl Fn(&str) -> (u16, String) + Send + Sync + 'static {
        move |path| match path {
            CONNECT | SUBSCRIBE => (200, r#"{"token":"t"}"#.to_owned()),
            LIST => (200, listed_body(&ids)),
            _ => (404, String::new()),
        }
    }

    fn refuse(
        refused_path: &'static str,
        status: u16,
        code: &'static str,
        ids: Vec<u64>,
    ) -> impl Fn(&str) -> (u16, String) + Send + Sync + 'static {
        let ok = respond(ids);
        move |path| {
            if path == refused_path {
                (
                    status,
                    serde_json::json!({"code": code, "message": "no"}).to_string(),
                )
            } else {
                ok(path)
            }
        }
    }

    fn client(ws: &Centrifugo, api: &ApiMock, timing: ResubscribeTiming) -> Client {
        let creds = Credentials::new(
            "ak_test",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let mut client = Client::new(ws.url.clone(), api.url.clone(), Some(creds), None)
            .allow_insecure_http(true);
        client.access = Arc::new(SubaccountAccess::new(timing));
        client
    }

    async fn ended(sub: &mut TypedSubscription<Vec<u8>>) -> Error {
        let closed =
            tokio::time::timeout(WAIT, async { while sub.recv().await.is_some() {} }).await;
        assert!(
            closed.is_ok(),
            "subscription stayed open after the server ended it"
        );
        sub.err().expect("ended with an error")
    }

    fn reason_of(err: &Error) -> SubscriptionEndReason {
        match err {
            Error::SubscriptionEnded { reason, .. } => *reason,
            other => panic!("expected SubscriptionEnded, got {other:?}"),
        }
    }

    fn end(ws: &Centrifugo, channel: &str) {
        ws.push(
            channel,
            unsubscribe_frame(channel, 2000, "server unsubscribe"),
        );
    }

    #[tokio::test]
    async fn stale_unsubscribe_while_listed_resumes_with_fresh_token() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        assert_eq!(api.count(SUBSCRIBE), 1);
        assert_eq!(
            api.count(LIST),
            0,
            "a healthy subscribe must not read the list"
        );

        end(&ws, &channel);
        ws.await_subscribed(&channel).await;
        assert_eq!(api.count(SUBSCRIBE), 2, "a fresh token");
        assert_eq!(api.count(LIST), 1);
        ws.push(&channel, centrifugo_publication(b"live again"));
        let row = tokio::time::timeout(WAIT, sub.recv()).await.unwrap();
        assert_eq!(row.as_deref(), Some(&b"live again"[..]));
        assert!(sub.take_resubscribed(), "resuming raises the gap signal");
        assert_eq!(sub.resubscribes(), 1);
        assert!(sub.err().is_none(), "{:?}", sub.err());
    }

    #[tokio::test]
    async fn removed_from_the_list_does_not_resubscribe() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        api.set_respond(respond(vec![SUB_ID + 1]));

        end(&ws, &channel);
        let err = ended(&mut sub).await;
        assert_eq!(reason_of(&err), SubscriptionEndReason::NotListed);
        assert_eq!(api.count(LIST), 1, "the decision reads once");
        assert_eq!(api.count(SUBSCRIBE), 1, "no new token");
    }

    #[tokio::test]
    async fn listed_without_a_role_does_not_resubscribe() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        let ok = respond(vec![]);
        api.set_respond(move |path| {
            if path == LIST {
                (
                    200,
                    serde_json::json!({"subaccounts": [
                        {"id": SUB_ID.to_string(), "role": "SUBACCOUNT_ROLE_UNSPECIFIED"}
                    ]})
                    .to_string(),
                )
            } else {
                ok(path)
            }
        });
        end(&ws, &channel);
        assert_eq!(
            reason_of(&ended(&mut sub).await),
            SubscriptionEndReason::NotListed
        );
    }

    #[tokio::test]
    async fn seen_removal_stops_without_calls_and_a_later_grant_clears_it() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let updates = "private:auth:subaccounts:root:proto".to_owned();
        let mut stream = client.subscribe_raw(&updates).await.unwrap();
        ws.await_subscribed(&updates).await;
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        let tokens = api.count(SUBSCRIBE);

        let publish = |update: Subaccount| {
            ws.push(&updates, centrifugo_publication(&update.encode_to_vec()));
        };
        publish(Subaccount {
            id: SUB_ID,
            removed: true,
            ..Default::default()
        });
        tokio::time::timeout(WAIT, stream.recv())
            .await
            .unwrap()
            .unwrap();
        end(&ws, &channel);
        assert_eq!(
            reason_of(&ended(&mut sub).await),
            SubscriptionEndReason::AccessRemoved
        );
        assert_eq!(api.count(LIST), 0, "a seen removal needs no read");
        assert_eq!(api.count(SUBSCRIBE), tokens, "and no token");

        publish(Subaccount {
            id: SUB_ID,
            role: SubaccountRole::VIEWER.into(),
            ..Default::default()
        });
        tokio::time::timeout(WAIT, stream.recv())
            .await
            .unwrap()
            .unwrap();
        let again = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        end(&ws, &channel);
        ws.await_subscribed(&channel).await;
        assert_eq!(api.count(LIST), 1);
        assert!(again.err().is_none());
    }

    #[tokio::test]
    async fn refused_token_stops_with_the_refusal() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        api.set_respond(refuse(
            SUBSCRIBE,
            403,
            "SUBACCOUNT_READ_FORBIDDEN",
            vec![SUB_ID],
        ));

        end(&ws, &channel);
        let err = ended(&mut sub).await;
        assert_eq!(reason_of(&err), SubscriptionEndReason::TokenRefused);
        let Error::SubscriptionEnded {
            cause: Some(cause), ..
        } = &err
        else {
            panic!("{err:?}");
        };
        assert!(
            matches!(**cause, Error::PermissionDenied { status: 403, .. }),
            "{cause:?}"
        );
        assert_eq!(api.count(SUBSCRIBE), 2, "exactly one refused attempt");
    }

    #[tokio::test]
    async fn refused_list_stops_and_says_why() {
        for status in [401u16, 403] {
            let ws = Centrifugo::spawn().await;
            let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
            let client = client(&ws, &api, FAST);
            let channel = channel(SUB_ID);
            let mut sub = client.subscribe_raw(&channel).await.unwrap();
            ws.await_subscribed(&channel).await;
            api.set_respond(refuse(
                LIST,
                status,
                "AUTH_SUBACCOUNT_ACCESS_DENIED",
                vec![SUB_ID],
            ));

            end(&ws, &channel);
            let err = ended(&mut sub).await;
            assert_eq!(
                reason_of(&err),
                SubscriptionEndReason::ListRefused,
                "{status}"
            );
            let Error::SubscriptionEnded {
                cause: Some(cause), ..
            } = &err
            else {
                panic!("{err:?}");
            };
            assert!(auth::is_refusal(cause), "{cause:?}");
            let message = err.to_string();
            assert!(
                message.contains("server ended realtime subscription"),
                "{message}"
            );
            assert!(message.contains("list_refused"), "{message}");
            assert_eq!(api.count(LIST), 1);
            assert_eq!(api.count(SUBSCRIBE), 1);
        }
    }

    #[tokio::test]
    async fn temporary_list_failure_retries_then_resumes() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        let ok = respond(vec![SUB_ID]);
        let first = Arc::new(Mutex::new(true));
        api.set_respond(move |path| {
            if path == LIST && std::mem::take(&mut *first.lock().unwrap()) {
                (503, "{}".to_owned())
            } else {
                ok(path)
            }
        });
        end(&ws, &channel);
        ws.await_subscribed(&channel).await;
        assert_eq!(api.count(LIST), 2);
        assert!(sub.err().is_none(), "{:?}", sub.err());
    }

    #[tokio::test]
    async fn list_failures_have_a_hard_limit() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        let ok = respond(vec![SUB_ID]);
        api.set_respond(move |path| {
            if path == LIST {
                (503, "{}".to_owned())
            } else {
                ok(path)
            }
        });
        end(&ws, &channel);
        let err = ended(&mut sub).await;
        assert_eq!(reason_of(&err), SubscriptionEndReason::RetriesExhausted);
        assert!(matches!(
            &err,
            Error::SubscriptionEnded { cause: Some(_), .. }
        ));
        assert_eq!(api.count(LIST), FAST.max_attempts as usize);
    }

    #[tokio::test]
    async fn repeated_stale_unsubscribes_have_a_hard_limit() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        ws.after_subscribe(unsubscribe_frame(&channel, 2000, "server unsubscribe"));
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        let err = ended(&mut sub).await;
        assert_eq!(reason_of(&err), SubscriptionEndReason::RetriesExhausted);
        let limit = FAST.max_attempts as usize;
        assert_eq!(api.count(SUBSCRIBE), limit + 1, "initial plus the limit");
        assert_eq!(
            ws.connects.load(std::sync::atomic::Ordering::SeqCst),
            limit + 1,
            "the scenario must repeat to the limit"
        );
    }

    #[tokio::test]
    async fn revocation_burst_cannot_flood_requests() {
        const BURST: u64 = 25;
        let timing = ResubscribeTiming {
            max_attempts: 6,
            initial_backoff: Duration::from_millis(5),
            stable_after: Duration::from_secs(3600),
            attempt_spacing: Duration::from_millis(40),
            read_interval: Duration::from_millis(200),
        };
        let ids: Vec<u64> = (0..BURST).map(|i| SUB_ID + i).collect();
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(ids.clone())).await;
        let client = client(&ws, &api, timing);
        let mut subs = Vec::new();
        for id in &ids {
            subs.push(client.subscribe_raw(&channel(*id)).await.unwrap());
            ws.await_subscribed(&channel(*id)).await;
        }
        let tokens = api.count(CONNECT) + api.count(SUBSCRIBE);
        let started = std::time::Instant::now();
        for id in &ids {
            end(&ws, &channel(*id));
        }
        ws.await_subscribes(BURST as usize).await;
        let elapsed = started.elapsed();
        assert!(subs.iter().all(|sub| sub.err().is_none()));
        let max_reads = (elapsed.as_millis() / timing.read_interval.as_millis()) as usize + 2;
        assert!(
            api.count(LIST) <= max_reads && api.count(LIST) < BURST as usize,
            "list reads {} in {elapsed:?}, allowed {max_reads}",
            api.count(LIST)
        );
        assert!(
            elapsed >= timing.attempt_spacing * (BURST as u32 - 1),
            "{BURST} attempts took {elapsed:?}, faster than the pacing allows"
        );
        assert_eq!(
            api.count(CONNECT) + api.count(SUBSCRIBE) - tokens,
            2 * BURST as usize,
            "one connection and one subscribe token each"
        );
    }

    /// Such a decimal is also valid base58: it must read as the decimal reported.
    #[tokio::test]
    async fn listed_decimal_id_without_a_zero_digit_still_matches() {
        const ID: u64 = 2_305_843_111_111_111_111;
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(ID);
        let sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        end(&ws, &channel);
        ws.await_subscribed(&channel).await;
        assert!(sub.err().is_none(), "{:?}", sub.err());
    }

    #[tokio::test]
    async fn channels_no_access_governs_stop_without_requests() {
        for channel in [
            "private:auth:api-keys:root:proto",
            "public:spot:trades:1:proto",
        ] {
            let ws = Centrifugo::spawn().await;
            let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
            let client = client(&ws, &api, FAST);
            let mut sub = client.subscribe_raw(channel).await.unwrap();
            ws.await_subscribed(channel).await;
            let tokens = api.count(SUBSCRIBE);
            end(&ws, channel);
            assert_eq!(
                reason_of(&ended(&mut sub).await),
                SubscriptionEndReason::NotResubscribable
            );
            assert_eq!(api.count(LIST), 0);
            assert_eq!(api.count(SUBSCRIBE), tokens);
        }
    }

    #[tokio::test]
    async fn temporary_unsubscribe_code_reconnects_without_reading_the_list() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let client = client(&ws, &api, FAST);
        let channel = channel(SUB_ID);
        let _sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        ws.push(
            &channel,
            unsubscribe_frame(&channel, 2500, "insufficient state"),
        );
        ws.await_subscribed(&channel).await;
        assert_eq!(api.count(LIST), 0);
    }

    #[tokio::test]
    async fn a_read_cannot_answer_a_decision_made_after_it_started() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![])).await;
        let client = client(
            &ws,
            &api,
            ResubscribeTiming {
                read_interval: Duration::from_millis(50),
                ..FAST
            },
        );
        // The first read stays open while a later decision asks: only a read of
        // its own, started after it asked, can reach the server as a second call.
        api.hold(LIST);
        let first = {
            let client = client.clone();
            tokio::spawn(async move { client.access.read(&client).await })
        };
        let reaches = async |calls: usize| {
            tokio::time::timeout(WAIT, async {
                while api.count(LIST) < calls {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the read reaches the server");
        };
        reaches(1).await;
        let second = {
            let client = client.clone();
            tokio::spawn(async move { client.access.read(&client).await })
        };
        reaches(2).await;
        api.release();
        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_ok());
        assert_eq!(api.count(LIST), 2, "a later decision needs its own read");
    }

    /// A removal during the list or the pacing wait must win over the older
    /// answer. The removed false controls prove the same barriers still resume.
    async fn removal_while_deciding(pacing: bool, removed: bool) {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let timing = if pacing {
            ResubscribeTiming {
                attempt_spacing: Duration::from_millis(400),
                ..FAST
            }
        } else {
            FAST
        };
        let client = client(&ws, &api, timing);
        let updates = "private:auth:subaccounts:root:proto".to_owned();
        let mut stream = client.subscribe_raw(&updates).await.unwrap();
        ws.await_subscribed(&updates).await;
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        let tokens = api.count(SUBSCRIBE);
        // Occupy the slot so the resume must wait for the next one.
        let occupied = pacing.then(|| {
            client.access.reserve_attempt();
            lock_unpoisoned(&client.access.state).next_attempt_at
        });
        if !pacing {
            api.hold(LIST);
        }

        end(&ws, &channel);
        tokio::time::timeout(WAIT, async {
            loop {
                let at_barrier = match occupied {
                    Some(occupied) => {
                        lock_unpoisoned(&client.access.state).next_attempt_at > occupied
                    }
                    None => api.count(LIST) == 1,
                };
                if at_barrier {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the resume reaches its barrier");
        assert_eq!(
            api.count(SUBSCRIBE),
            tokens,
            "no token before the barrier opens"
        );

        let update = if removed {
            Subaccount {
                id: SUB_ID,
                removed: true,
                ..Default::default()
            }
        } else {
            Subaccount {
                id: SUB_ID,
                role: SubaccountRole::VIEWER.into(),
                ..Default::default()
            }
        };
        ws.push(&updates, centrifugo_publication(&update.encode_to_vec()));
        tokio::time::timeout(WAIT, stream.recv())
            .await
            .unwrap()
            .unwrap();
        api.release();

        if removed {
            assert_eq!(
                reason_of(&ended(&mut sub).await),
                SubscriptionEndReason::AccessRemoved
            );
            assert_eq!(api.count(SUBSCRIBE), tokens, "no token after the removal");
        } else {
            ws.await_subscribed(&channel).await;
            assert_eq!(api.count(SUBSCRIBE), tokens + 1);
            assert!(sub.err().is_none(), "{:?}", sub.err());
        }
    }

    #[tokio::test]
    async fn removal_during_the_list_stops_before_any_token() {
        removal_while_deciding(false, true).await;
    }

    #[tokio::test]
    async fn still_granted_during_the_list_resumes() {
        removal_while_deciding(false, false).await;
    }

    #[tokio::test]
    async fn removal_during_pacing_stops_before_any_token() {
        removal_while_deciding(true, true).await;
    }

    #[tokio::test]
    async fn still_granted_during_pacing_resumes() {
        removal_while_deciding(true, false).await;
    }

    /// Keeps a connection alive with protocol pings. Confirms connect and
    /// subscribe only after `confirm_after` pings, if given.
    async fn spawn_pinging_server(
        confirm_after: Option<usize>,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use futures_util::{SinkExt, StreamExt};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let pings = Arc::new(AtomicUsize::new(0));
        let sent = pings.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let sent = sent.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(
                        socket,
                        |_: &_,
                         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            response.headers_mut().insert(
                                "sec-websocket-protocol",
                                "centrifuge-protobuf".parse().unwrap(),
                            );
                            Ok(response)
                        },
                    )
                    .await
                    else {
                        return;
                    };
                    let mut count = 0;
                    let mut answered = 0u32;
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(20)) => {
                                // A zero-length reply is the protocol's ping.
                                if ws.send(WsMessage::Binary(vec![0u8].into())).await.is_err() {
                                    return;
                                }
                                sent.fetch_add(1, Ordering::SeqCst);
                                count += 1;
                            }
                            message = ws.next() => {
                                let Some(Ok(message)) = message else { return };
                                // Connect and subscribe commands, not pongs.
                                if message.len() > 1 {
                                    answered += 1;
                                }
                            }
                        }
                        if confirm_after.is_some_and(|after| count >= after) && answered >= 1 {
                            for id in 1..=2 {
                                let reply = super::centrifugo::centrifugo_ok_reply(id);
                                if ws.send(WsMessage::Binary(reply.into())).await.is_err() {
                                    return;
                                }
                                if id == 1 {
                                    let _ = ws.next().await;
                                }
                            }
                            while let Some(Ok(_)) = ws.next().await {}
                            return;
                        }
                    }
                });
            }
        });
        (url, task, pings)
    }

    #[tokio::test]
    async fn ping_only_server_fails_setup_at_one_deadline() {
        let (url, task, pings) = spawn_pinging_server(None).await;
        let mut client = Client::new(url, "http://127.0.0.1:1", None, None);
        client.setup_deadline = Duration::from_millis(400);
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            client.subscribe_raw("public:spot:trades:1:proto"),
        )
        .await;
        let elapsed = started.elapsed();
        task.abort();
        let err = match outcome {
            Err(_) => panic!("setup stayed open on a server that only pings"),
            Ok(Err(err)) => err,
            Ok(Ok(_)) => panic!("the server never confirmed"),
        };
        assert!(
            matches!(
                &err,
                Error::RealtimeSetupTimeout { channel, deadline }
                    if channel == "public:spot:trades:1:proto"
                        && *deadline == Duration::from_millis(400)
            ),
            "{err:?}"
        );
        assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
        assert!(
            pings.load(std::sync::atomic::Ordering::SeqCst) >= 3,
            "the server must have kept the connection alive"
        );
        assert!(!err.is_retryable());
    }

    /// Pings before the confirmation do not fail a setup within its deadline.
    #[tokio::test]
    async fn pings_then_confirmation_within_the_deadline_still_subscribes() {
        let (url, task, pings) = spawn_pinging_server(Some(5)).await;
        let mut client = Client::new(url, "http://127.0.0.1:1", None, None);
        client.setup_deadline = Duration::from_secs(20);
        let sub = tokio::time::timeout(
            Duration::from_secs(25),
            client.subscribe_raw("public:spot:trades:1:proto"),
        )
        .await
        .expect("setup must finish")
        .expect("setup must succeed");
        assert!(pings.load(std::sync::atomic::Ordering::SeqCst) >= 5);
        assert!(sub.is_alive());
        task.abort();
    }

    /// The default keeps its margin over a longer configured HTTP timeout, which
    /// covers each of the two token requests.
    #[test]
    fn setup_deadline_covers_a_longer_http_timeout() {
        let default = Client::new("ws://127.0.0.1:1", "http://127.0.0.1:1", None, None);
        assert_eq!(default.setup_deadline, Duration::from_secs(30));
        let slow = Client::with_timeout(
            "ws://127.0.0.1:1",
            "http://127.0.0.1:1",
            None,
            None,
            Duration::from_secs(60),
        );
        assert_eq!(slow.setup_deadline, Duration::from_secs(120));
    }

    /// A resume attempt stuck in setup counts against the hard limit, so a server
    /// that only pings cannot hold a resuming subscription forever either.
    #[tokio::test]
    async fn resume_attempts_stuck_in_setup_exhaust_the_limit() {
        let ws = Centrifugo::spawn().await;
        let api = ApiMock::spawn(respond(vec![SUB_ID])).await;
        let mut client = client(&ws, &api, FAST);
        client.setup_deadline = Duration::from_millis(150);
        let channel = channel(SUB_ID);
        let mut sub = client.subscribe_raw(&channel).await.unwrap();
        ws.await_subscribed(&channel).await;
        ws.stop_confirming();
        end(&ws, &channel);
        let err = ended(&mut sub).await;
        assert_eq!(reason_of(&err), SubscriptionEndReason::RetriesExhausted);
        let Error::SubscriptionEnded {
            cause: Some(cause), ..
        } = &err
        else {
            panic!("{err:?}");
        };
        assert!(
            matches!(**cause, Error::RealtimeSetupTimeout { .. }),
            "{cause:?}"
        );
    }

    #[test]
    fn subaccount_channel_ids() {
        let id = format_id(SUB_ID);
        for prefix in SUBACCOUNT_CHANNEL_PREFIXES {
            for suffix in ["", ":proto"] {
                assert_eq!(
                    subaccount_channel_id(&format!("{prefix}{id}{suffix}")),
                    Some(SUB_ID),
                    "{prefix}{suffix}"
                );
            }
        }
        for channel in [
            format!("private:auth:subaccounts:{id}:proto"),
            format!("private:spot:orders:{id}"),
            "public:spot:trades:1:proto".to_owned(),
            "private:ledger:balances:".to_owned(),
            format!("private:ledger:balances:{id}:other"),
        ] {
            assert_eq!(subaccount_channel_id(&channel), None, "{channel}");
        }
    }
}
