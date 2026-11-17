//! A shared bound on the WebSocket upgrades many sessions make at once.
//!
//! Every [`crate::tradews`] session and [`crate::marketstream`] stream
//! reconnects on its own, with jittered backoff and at least the delay a
//! refused upgrade asked for with `Retry-After`. When thousands of them drop
//! together, the jitter spreads their reconnects out but does not bound how
//! many upgrades are in flight at once. A [`ReconnectLimiter`], created once and
//! given to the config of every session that shares it, does.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::errors::{Error, Result};

/// Caps how many WebSocket upgrades the sessions that share it have in flight
/// at once, across [`crate::tradews::Config::reconnect_limiter`] and
/// [`crate::marketstream::Config::reconnect_limiter`].
///
/// A session takes one of its slots for an upgrade only, after its backoff and
/// any `Retry-After` wait have run out, and gives the slot back as soon as the
/// upgrade ends: accepted, refused, failed or timed out. It never holds a slot
/// while it waits to reconnect or while it is connected. The first connection
/// of a session ([`crate::tradews::connect`], [`crate::marketstream::connect`])
/// takes a slot the same way, so the cap holds for a process that starts
/// thousands of sessions at once as well as for one whose sessions all drop.
///
/// Sessions waiting for a slot get one in the order they asked (first in,
/// first out), so none starves. The wait for a slot comes after the session's
/// own wait and is never counted toward it: a server's `Retry-After` is never
/// shortened. Closing a session that waits for a slot ends the wait, and it
/// makes no upgrade afterwards; dropping a `connect` future that waits does the
/// same.
///
/// Cloning it is cheap and every clone shares the same slots. A session
/// configured without one connects as soon as its own wait ends.
///
/// ```
/// use polyester::ReconnectLimiter;
///
/// let limiter = ReconnectLimiter::new(64)?;
/// assert_eq!(limiter.max_concurrent_upgrades(), 64);
/// assert_eq!(limiter.upgrades_in_flight(), 0);
/// # Ok::<(), polyester::Error>(())
/// ```
#[derive(Clone)]
pub struct ReconnectLimiter {
    slots: Arc<Semaphore>,
    max: usize,
}

impl ReconnectLimiter {
    /// A limiter that lets at most `max_concurrent_upgrades` upgrades run at
    /// once. Zero, or more than tokio's semaphore holds
    /// ([`Semaphore::MAX_PERMITS`]), is [`Error::Validation`].
    pub fn new(max_concurrent_upgrades: usize) -> Result<Self> {
        if max_concurrent_upgrades == 0 || max_concurrent_upgrades > Semaphore::MAX_PERMITS {
            return Err(Error::validation(format!(
                "ReconnectLimiter max_concurrent_upgrades must be from 1 to {}",
                Semaphore::MAX_PERMITS
            )));
        }
        Ok(Self {
            slots: Arc::new(Semaphore::new(max_concurrent_upgrades)),
            max: max_concurrent_upgrades,
        })
    }

    /// The most upgrades that run at once.
    pub fn max_concurrent_upgrades(&self) -> usize {
        self.max
    }

    /// How many upgrades hold a slot now.
    pub fn upgrades_in_flight(&self) -> usize {
        self.max - self.slots.available_permits()
    }

    /// Waits for a slot, first in, first out. Cancel-safe: dropping the
    /// future leaves the queue and takes no slot.
    pub(crate) async fn acquire(&self) -> UpgradeSlot {
        let permit = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("a ReconnectLimiter's semaphore is never closed");
        UpgradeSlot { _permit: permit }
    }
}

impl std::fmt::Debug for ReconnectLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectLimiter")
            .field("max_concurrent_upgrades", &self.max)
            .field("upgrades_in_flight", &self.upgrades_in_flight())
            .finish()
    }
}

/// One upgrade's slot; dropping it gives the slot back.
pub(crate) struct UpgradeSlot {
    _permit: OwnedSemaphorePermit,
}

/// A slot of `limiter` for one upgrade, waited for in turn; at once, and
/// `None`, without a limiter. Cancel-safe.
pub(crate) async fn upgrade_slot(limiter: Option<&ReconnectLimiter>) -> Option<UpgradeSlot> {
    match limiter {
        Some(limiter) => Some(limiter.acquire().await),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::ReconnectLimiter;

    #[test]
    fn zero_or_too_many_slots_are_refused() {
        assert!(matches!(
            ReconnectLimiter::new(0),
            Err(crate::Error::Validation(_))
        ));
        assert!(matches!(
            ReconnectLimiter::new(tokio::sync::Semaphore::MAX_PERMITS + 1),
            Err(crate::Error::Validation(_))
        ));
        assert!(ReconnectLimiter::new(1).is_ok());
    }

    #[tokio::test]
    async fn slots_are_shared_by_clones_and_given_back_on_drop() {
        let limiter = ReconnectLimiter::new(2).unwrap();
        let clone = limiter.clone();
        let a = limiter.acquire().await;
        let b = clone.acquire().await;
        assert_eq!(limiter.upgrades_in_flight(), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), clone.acquire())
                .await
                .is_err(),
            "a third upgrade waits"
        );
        drop(a);
        assert_eq!(clone.upgrades_in_flight(), 1);
        drop(b);
        assert_eq!(limiter.upgrades_in_flight(), 0);
    }

    #[tokio::test]
    async fn waiters_get_slots_first_in_first_out() {
        let limiter = ReconnectLimiter::new(1).unwrap();
        let held = limiter.acquire().await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..8 {
            let limiter = limiter.clone();
            let order = Arc::clone(&order);
            waiters.push(tokio::spawn(async move {
                let _slot = limiter.acquire().await;
                order.lock().unwrap().push(i);
                tokio::task::yield_now().await;
            }));
            // On this single-threaded runtime the waiter runs, and joins the
            // queue, before the next one is spawned.
            tokio::task::yield_now().await;
        }
        drop(held);
        for w in waiters {
            w.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), (0..8).collect::<Vec<_>>());
        assert_eq!(limiter.upgrades_in_flight(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_wait_takes_no_slot() {
        let limiter = ReconnectLimiter::new(1).unwrap();
        let held = limiter.acquire().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), limiter.acquire())
                .await
                .is_err()
        );
        drop(held);
        assert_eq!(limiter.upgrades_in_flight(), 0);
        let _again = limiter.acquire().await;
        assert_eq!(limiter.upgrades_in_flight(), 1);
    }
}
