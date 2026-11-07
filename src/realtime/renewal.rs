//! Token renewal for one private Centrifugo connection.
//!
//! The connection and subscription tokens are renewed before they expire,
//! from the same token endpoints, and a failed renewal is handled as the
//! private order stream handles one: it is reported, and the server ending the
//! subscription or the connection then drives the usual resubscribe.

use super::auth;
use super::protocol;
use crate::auth::Credentials;
use crate::errors::{Error, Result};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RenewalKind {
    Connection = 0,
    Subscription = 1,
}

impl RenewalKind {
    fn label(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Subscription => "subscription",
        }
    }
}

/// Where a renewal takes its fresh token.
#[derive(Clone)]
pub(super) struct TokenSource {
    pub credentials: Option<Credentials>,
    pub api_url: String,
    pub channel: String,
    pub timeout: Duration,
    pub allow_insecure_http: bool,
}

impl TokenSource {
    async fn fetch(self, kind: RenewalKind) -> Result<String> {
        let creds = self
            .credentials
            .as_ref()
            .ok_or_else(|| Error::auth("private channel requires credentials"))?;
        match kind {
            RenewalKind::Connection => {
                auth::fetch_connection_token(
                    creds,
                    &self.api_url,
                    self.timeout,
                    self.allow_insecure_http,
                )
                .await
            }
            RenewalKind::Subscription => {
                auth::fetch_subscription_token(
                    creds,
                    &self.api_url,
                    &self.channel,
                    self.timeout,
                    self.allow_insecure_http,
                )
                .await
            }
        }
    }
}

pub(super) type FetchedToken = (RenewalKind, Result<String>);

/// Renewal state of one connection. It lives and dies with the connection:
/// dropping it aborts any token fetch in flight, so a renewal is only ever
/// sent on the connection that scheduled it.
pub(super) struct TokenRenewer {
    source: TokenSource,
    /// The length of one TTL unit: a second, shortened in tests.
    unit: Duration,
    due: [Option<Instant>; 2],
    fetches: [Option<tokio::task::JoinHandle<()>>; 2],
    pending: HashMap<u32, RenewalKind>,
    next_id: u32,
    fetched: mpsc::UnboundedSender<FetchedToken>,
}

impl Drop for TokenRenewer {
    fn drop(&mut self) {
        for fetch in self.fetches.iter_mut().filter_map(Option::take) {
            fetch.abort();
        }
    }
}

impl TokenRenewer {
    /// Renewal command IDs start at `first_id`. Fetched tokens arrive on the
    /// returned receiver; pass each to [`Self::command`].
    pub fn new(
        source: TokenSource,
        unit: Duration,
        first_id: u32,
    ) -> (Self, mpsc::UnboundedReceiver<FetchedToken>) {
        let (fetched, receiver) = mpsc::unbounded_channel();
        (
            Self {
                source,
                unit,
                due: [None, None],
                fetches: [None, None],
                pending: HashMap::new(),
                next_id: first_id,
                fetched,
            },
            receiver,
        )
    }

    /// Renews `kind`'s token `ttl` units from now when the server said it
    /// expires (`expiry` is `Some(ttl)`).
    pub fn schedule(&mut self, kind: RenewalKind, expiry: Option<u32>) {
        if let Some(ttl) = expiry {
            self.due[kind as usize] = Some(Instant::now() + self.unit.saturating_mul(ttl));
        }
    }

    /// When the next renewal is due, if any is scheduled.
    pub fn next_due(&self) -> Option<Instant> {
        self.due.iter().flatten().min().copied()
    }

    /// Starts a token fetch for every renewal that is due.
    pub fn start_due(&mut self) {
        let now = Instant::now();
        for kind in [RenewalKind::Connection, RenewalKind::Subscription] {
            let slot = kind as usize;
            if self.due[slot].is_some_and(|due| due <= now) {
                self.due[slot] = None;
                let source = self.source.clone();
                let fetched = self.fetched.clone();
                if let Some(previous) = self.fetches[slot].replace(tokio::spawn(async move {
                    let _ = fetched.send((kind, source.fetch(kind).await));
                })) {
                    previous.abort();
                }
            }
        }
    }

    /// The command that presents a fetched token; its reply goes to
    /// [`Self::take_reply`].
    pub fn command(&mut self, kind: RenewalKind, token: &str) -> Vec<u8> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(3);
        self.pending.insert(id, kind);
        match kind {
            RenewalKind::Connection => protocol::refresh_command(id, token),
            RenewalKind::Subscription => {
                protocol::sub_refresh_command(id, &self.source.channel, token)
            }
        }
    }

    /// Takes the answer to a renewal: schedules the next one, or returns the
    /// refusal to report. `None` when `id` answers no renewal.
    pub fn take_reply(
        &mut self,
        id: u32,
        error: Option<&protocol::ProtoError>,
        expiry: Option<u32>,
    ) -> Option<std::result::Result<(), Error>> {
        let kind = self.pending.remove(&id)?;
        Some(match error {
            // The server ends what it no longer accepts; that drives recovery.
            Some(error) => Err(Error::realtime(format!(
                "realtime {} token renewal refused: {}",
                kind.label(),
                super::centrifugo_protocol_error_message(error)
            ))),
            None => {
                self.schedule(kind, expiry);
                Ok(())
            }
        })
    }
}
