//! Which sessions currently have a connected socket. Deliberately in memory,
//! never a DB column: a crash must not leave a session stuck "live".

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct LiveRegistry {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    live: Mutex<HashSet<Uuid>>,
    released: Notify,
}

/// The session stayed live for the whole wait.
#[derive(Debug, PartialEq, Eq)]
pub struct Busy;

impl LiveRegistry {
    pub fn is_live(&self, id: Uuid) -> bool {
        self.inner.live.lock().unwrap().contains(&id)
    }

    pub fn try_claim(&self, id: Uuid) -> Option<LiveGuard> {
        let fresh = self.inner.live.lock().unwrap().insert(id);
        fresh.then(|| LiveGuard {
            registry: self.clone(),
            id,
        })
    }

    /// Claims `id`, waiting up to `wait` for a previous holder to release it.
    /// A reconnect usually races the old connection's final flush, which
    /// takes about one decode.
    pub async fn claim(&self, id: Uuid, wait: Duration) -> Result<LiveGuard, Busy> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Register for the wake-up *before* checking, so a release
            // between the check and the await is not missed.
            let released = self.inner.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(guard) = self.try_claim(id) {
                return Ok(guard);
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return Err(Busy);
            }
        }
    }
}

#[must_use = "dropping the guard immediately releases the session"]
pub struct LiveGuard {
    registry: LiveRegistry,
    id: Uuid,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.registry.inner.live.lock().unwrap().remove(&self.id);
        self.registry.inner.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claim_is_exclusive_until_the_guard_drops() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let guard = reg.try_claim(id).expect("first claim");
        assert!(reg.is_live(id));
        assert!(reg.try_claim(id).is_none());
        drop(guard);
        assert!(!reg.is_live(id));
        assert!(reg.try_claim(id).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_claim_acquires_once_released() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let guard = reg.try_claim(id).unwrap();
        let waiter = {
            let reg = reg.clone();
            tokio::spawn(async move { reg.claim(id, Duration::from_secs(5)).await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(guard);
        assert_eq!(waiter.await.unwrap(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_claim_times_out_as_busy() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let _guard = reg.try_claim(id).unwrap();
        assert!(matches!(
            reg.claim(id, Duration::from_secs(5)).await,
            Err(Busy)
        ));
    }

    #[tokio::test]
    async fn other_sessions_are_independent() {
        let reg = LiveRegistry::default();
        let _a = reg.try_claim(Uuid::now_v7()).unwrap();
        assert!(reg.try_claim(Uuid::now_v7()).is_some());
    }
}
