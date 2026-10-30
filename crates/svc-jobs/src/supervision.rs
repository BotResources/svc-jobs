//! Background tasks kept alive for as long as the process lives, and the pod's
//! readiness derived from them.
//!
//! Every supervised task is down from the moment one of its attempts ends until
//! a later attempt signals [`Established`]; the pod is ready only once boot is
//! complete and no task (nor declared dependency) is down. A lost task is
//! restarted after the wait its [`RestartPolicy`] gives, again and again, never
//! giving up: a bus outage of any length ends with the pod ready again on its
//! own. Liveness is not involved — a dependency outage must take the pod out of
//! rotation, never get it killed.

mod observability;
mod policy;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use br_util_axum_readiness::ReadinessHandle;

use crate::error::ServiceError;

pub use policy::RestartPolicy;
use policy::Spread;

#[derive(Clone)]
pub struct Supervisor {
    readiness: ReadinessHandle,
    policy: RestartPolicy,
    booted: Arc<AtomicBool>,
    /// What keeps the pod out of rotation, and since when.
    down: Arc<Mutex<BTreeMap<&'static str, Instant>>>,
}

/// Handed to each attempt of a task: the attempt signals it once it is bound to
/// its source of work, which is what readiness waits for.
pub struct Established {
    supervisor: Supervisor,
    name: &'static str,
    attempt: u64,
}

impl Established {
    pub fn signal(self) {
        observability::bound(self.name);
        let down_since = self.supervisor.bring_up(self.name);
        if self.attempt > 1 {
            let down_for_ms = down_since.map_or(0, |since| milliseconds(since.elapsed()));
            tracing::info!(
                task = self.name,
                attempt = self.attempt,
                down_for_ms,
                "a background task is bound to its source of work again"
            );
        }
    }
}

impl Supervisor {
    pub fn new(readiness: ReadinessHandle, policy: RestartPolicy) -> Self {
        observability::describe();
        Self {
            readiness,
            policy,
            booted: Arc::new(AtomicBool::new(false)),
            down: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn boot_complete(&self) {
        self.booted.store(true, Ordering::SeqCst);
        self.publish_readiness();
    }

    pub fn spawn<F, Fut>(&self, name: &'static str, attempt: F)
    where
        F: Fn(Established) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
    {
        self.spawn_with_teardown(name, attempt, || {});
    }

    pub fn spawn_with_teardown<F, Fut, T>(&self, name: &'static str, attempt: F, teardown: T)
    where
        F: Fn(Established) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
        T: Fn() + Send + 'static,
    {
        observability::spawned(name);
        self.mark_down(name);
        let supervisor = self.clone();
        tokio::spawn(async move { supervisor.supervise(name, attempt, teardown).await });
    }

    /// Runs attempt after attempt of one task. Never returns: there is no
    /// restart budget, so an outage longer than any budget still ends with the
    /// task bound again, and the pod ready, without an operator.
    async fn supervise<F, Fut, T>(&self, name: &'static str, run: F, teardown: T)
    where
        F: Fn(Established) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
        T: Fn() + Send + 'static,
    {
        let mut consecutive_restarts = 0u32;
        let mut attempt = 1u64;
        loop {
            let started = Instant::now();
            let established = Established {
                supervisor: self.clone(),
                name,
                attempt,
            };
            let outcome = tokio::spawn(run(established)).await;
            teardown();
            observability::lost(name);
            self.mark_down(name);
            if started.elapsed() >= self.policy.stability {
                consecutive_restarts = 0;
            }
            let wait = self.policy.delay(consecutive_restarts, Spread::draw());
            report_loss(name, attempt, wait, outcome);
            tokio::time::sleep(wait).await;
            consecutive_restarts = consecutive_restarts.saturating_add(1);
            attempt = attempt.saturating_add(1);
            observability::restarted(name);
        }
    }

    pub fn mark_down(&self, name: &'static str) {
        self.down_names().entry(name).or_insert_with(Instant::now);
        self.publish_readiness();
    }

    pub fn mark_up(&self, name: &'static str) {
        self.bring_up(name);
    }

    /// Clears `name` from what keeps the pod down, and says since when it was.
    fn bring_up(&self, name: &'static str) -> Option<Instant> {
        let down_since = self.down_names().remove(name);
        self.publish_readiness();
        down_since
    }

    fn down_names(&self) -> std::sync::MutexGuard<'_, BTreeMap<&'static str, Instant>> {
        self.down.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish_readiness(&self) {
        let down: Vec<&str> = self.down_names().keys().copied().collect();
        if down.is_empty() {
            if self.booted.load(Ordering::SeqCst) {
                self.readiness.set_ready();
            }
        } else {
            self.readiness
                .set_not_ready(format!("background tasks are down: {}", down.join(", ")));
        }
    }
}

fn report_loss(
    name: &'static str,
    attempt: u64,
    wait: Duration,
    outcome: Result<Result<(), ServiceError>, tokio::task::JoinError>,
) {
    let next_delay_ms = milliseconds(wait);
    match outcome {
        Ok(Ok(())) => tracing::warn!(
            task = name,
            attempt,
            next_delay_ms,
            "a background task returned — its source of work ended, which never happens while \
             the service is healthy; the pod is NOT READY until it binds again, restarting it"
        ),
        Ok(Err(error)) => tracing::warn!(
            task = name,
            attempt,
            next_delay_ms,
            error = %error,
            "a background task lost its source of work; the pod is NOT READY until it binds \
             again, restarting it"
        ),
        Err(join) => tracing::error!(
            task = name,
            attempt,
            next_delay_ms,
            error = %join,
            "a background task panicked; the pod is NOT READY until it binds again, restarting it"
        ),
    }
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
