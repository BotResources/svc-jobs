//! The claims a pod holds open, by runner type, and how they are woken.
//!
//! A Job queued for a type wakes ONE held claim of that type — the oldest — so
//! a burst of claims does not stampede the ledger for every Job. A claim woken
//! but leaving without taking the wake (it was served by its own pass, or its
//! wait ended) passes the wake on to the next one, so no Job is left waiting
//! with claims parked beside it. A retry falling due is one more Job to take:
//! the pod keeps ONE timer per type, armed for the earliest retry it knows
//! of, and wakes one claim when it fires. Facts that change what every claim
//! of a type may do — the fleet of the type moved — wake them all.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use bc_jobs::domain::keys::RunnerTypeKey;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::Instant;

#[derive(Default)]
pub struct ClaimWaiters {
    held: Mutex<HashMap<RunnerTypeKey, VecDeque<Weak<Waiter>>>>,
    retry_timers: Mutex<HashMap<RunnerTypeKey, RetryTimer>>,
}

struct RetryTimer {
    due: Instant,
    task: AbortHandle,
}

pub struct Waiter {
    notify: Notify,
    woken: AtomicBool,
}

/// One held claim, registered until dropped.
pub struct HeldClaim {
    waiters: Arc<ClaimWaiters>,
    runner_type: RunnerTypeKey,
    waiter: Arc<Waiter>,
}

impl HeldClaim {
    /// Parks until this claim is woken. The wake is consumed.
    pub async fn woken(&self) {
        self.waiter.notify.notified().await;
        self.waiter.woken.store(false, Ordering::SeqCst);
    }
}

impl Drop for HeldClaim {
    fn drop(&mut self) {
        if self.waiter.woken.swap(false, Ordering::SeqCst) {
            self.waiters.wake_one(&self.runner_type);
        }
    }
}

impl ClaimWaiters {
    pub fn hold(self: &Arc<Self>, runner_type: &RunnerTypeKey) -> HeldClaim {
        let waiter = Arc::new(Waiter {
            notify: Notify::new(),
            woken: AtomicBool::new(false),
        });
        self.lock()
            .entry(runner_type.clone())
            .or_default()
            .push_back(Arc::downgrade(&waiter));
        HeldClaim {
            waiters: Arc::clone(self),
            runner_type: runner_type.clone(),
            waiter,
        }
    }

    pub fn is_held(&self, runner_type: &RunnerTypeKey) -> bool {
        let mut held = self.lock();
        let Some(queue) = held.get_mut(runner_type) else {
            return false;
        };
        queue.retain(|waiter| waiter.strong_count() > 0);
        !queue.is_empty()
    }

    pub fn is_any_held(&self) -> bool {
        self.lock()
            .values()
            .any(|queue| queue.iter().any(|waiter| waiter.strong_count() > 0))
    }

    pub fn wake_one(&self, runner_type: &RunnerTypeKey) {
        let mut held = self.lock();
        let Some(queue) = held.get_mut(runner_type) else {
            return;
        };
        while let Some(candidate) = queue.pop_front() {
            if let Some(waiter) = candidate.upgrade() {
                waiter.woken.store(true, Ordering::SeqCst);
                waiter.notify.notify_one();
                return;
            }
        }
    }

    pub fn wake_all(&self, runner_type: &RunnerTypeKey) {
        let woken: Vec<Arc<Waiter>> = self
            .lock()
            .remove(runner_type)
            .into_iter()
            .flatten()
            .filter_map(|waiter| waiter.upgrade())
            .collect();
        for waiter in woken {
            waiter.woken.store(true, Ordering::SeqCst);
            waiter.notify.notify_one();
        }
    }

    /// Arms the type's retry timer for `due_in` from now, unless it is already
    /// armed for an earlier moment. When it fires, one held claim of the type
    /// is woken; a claim parking afterwards arms it again for the next retry.
    pub fn arm_retry(self: &Arc<Self>, runner_type: &RunnerTypeKey, due_in: Duration) {
        let due = Instant::now() + due_in;
        let mut timers = self.timers();
        if timers
            .get(runner_type)
            .is_some_and(|armed| armed.due <= due && !armed.task.is_finished())
        {
            return;
        }
        let waiters = Arc::downgrade(self);
        let key = runner_type.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep_until(due).await;
            if let Some(waiters) = waiters.upgrade() {
                waiters.fired(&key, due);
            }
        })
        .abort_handle();
        if let Some(replaced) = timers.insert(runner_type.clone(), RetryTimer { due, task }) {
            replaced.task.abort();
        }
    }

    /// Whether a retry timer of the type is armed and not yet fired.
    pub fn has_retry_timer(&self, runner_type: &RunnerTypeKey) -> bool {
        self.timers()
            .get(runner_type)
            .is_some_and(|armed| !armed.task.is_finished())
    }

    fn fired(&self, runner_type: &RunnerTypeKey, due: Instant) {
        {
            let mut timers = self.timers();
            if timers
                .get(runner_type)
                .is_some_and(|armed| armed.due == due)
            {
                timers.remove(runner_type);
            }
        }
        self.wake_one(runner_type);
    }

    pub fn wake_everyone(&self) {
        let types: Vec<RunnerTypeKey> = self.lock().keys().cloned().collect();
        for runner_type in types {
            self.wake_all(&runner_type);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunnerTypeKey, VecDeque<Weak<Waiter>>>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn timers(&self) -> std::sync::MutexGuard<'_, HashMap<RunnerTypeKey, RetryTimer>> {
        self.retry_timers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn analyst() -> RunnerTypeKey {
        RunnerTypeKey::new("analyst").unwrap()
    }

    async fn is_woken(claim: &HeldClaim) -> bool {
        tokio::time::timeout(Duration::from_millis(50), claim.woken())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn a_queued_job_wakes_the_oldest_held_claim_only() {
        // Given: two claims held for one type
        let waiters = Arc::new(ClaimWaiters::default());
        let first = waiters.hold(&analyst());
        let second = waiters.hold(&analyst());
        // When: one job is queued
        waiters.wake_one(&analyst());
        // Then: the oldest claim alone is woken
        assert!(is_woken(&first).await);
        assert!(!is_woken(&second).await);
    }

    #[tokio::test]
    async fn a_claim_leaving_without_its_wake_passes_it_on() {
        // Given: two claims held, the first woken
        let waiters = Arc::new(ClaimWaiters::default());
        let first = waiters.hold(&analyst());
        let second = waiters.hold(&analyst());
        waiters.wake_one(&analyst());
        // When: the first leaves without consuming the wake
        drop(first);
        // Then: the second is woken in its place
        assert!(is_woken(&second).await);
    }

    #[tokio::test]
    async fn a_wake_landing_before_the_claim_parks_is_not_lost() {
        // Given: a claim registered but not yet parked
        let waiters = Arc::new(ClaimWaiters::default());
        let claim = waiters.hold(&analyst());
        // When: it is woken before it awaits
        waiters.wake_one(&analyst());
        // Then: parking returns at once
        assert!(is_woken(&claim).await);
    }

    #[tokio::test]
    async fn a_fleet_change_wakes_every_claim_of_its_type_only() {
        // Given: two claims for one type and one for another
        let waiters = Arc::new(ClaimWaiters::default());
        let first = waiters.hold(&analyst());
        let second = waiters.hold(&analyst());
        let other = waiters.hold(&RunnerTypeKey::new("scribe").unwrap());
        // When: the type's fleet moves
        waiters.wake_all(&analyst());
        // Then: both its claims wake, the other type's does not
        assert!(is_woken(&first).await);
        assert!(is_woken(&second).await);
        assert!(!is_woken(&other).await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_falling_due_wakes_one_claim_through_one_timer() {
        // Given: two claims held for one type, and two retries armed
        let waiters = Arc::new(ClaimWaiters::default());
        let first = waiters.hold(&analyst());
        let second = waiters.hold(&analyst());
        waiters.arm_retry(&analyst(), Duration::from_secs(5));
        waiters.arm_retry(&analyst(), Duration::from_secs(9));
        // Then: one timer stands, for the earliest retry
        assert!(waiters.has_retry_timer(&analyst()));
        // When: it fires
        tokio::time::sleep(Duration::from_secs(6)).await;
        // Then: one claim is woken, the other keeps waiting, and the timer is spent
        assert!(is_woken(&first).await);
        assert!(!is_woken(&second).await);
        assert!(!waiters.has_retry_timer(&analyst()));
    }
}
