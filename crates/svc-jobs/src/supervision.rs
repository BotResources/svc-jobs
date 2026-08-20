use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use br_util_axum_readiness::ReadinessHandle;

use crate::error::ServiceError;

#[derive(Clone, Copy)]
pub struct RestartPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub budget: u32,
    pub stability: Duration,
}

impl RestartPolicy {
    fn backoff(&self, consecutive_restarts: u32) -> Duration {
        let factor = 1u32 << consecutive_restarts.min(16);
        self.initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }
}

#[derive(Clone)]
pub struct Supervisor {
    readiness: ReadinessHandle,
    policy: RestartPolicy,
    booted: Arc<AtomicBool>,
    down: Arc<Mutex<BTreeSet<&'static str>>>,
}

impl Supervisor {
    pub fn new(readiness: ReadinessHandle, policy: RestartPolicy) -> Self {
        Self {
            readiness,
            policy,
            booted: Arc::new(AtomicBool::new(false)),
            down: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    pub fn boot_complete(&self) {
        self.booted.store(true, Ordering::SeqCst);
        self.publish_readiness();
    }

    pub fn spawn<F, Fut>(&self, name: &'static str, attempt: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
    {
        self.spawn_with_teardown(name, attempt, || {});
    }

    pub fn spawn_with_teardown<F, Fut, T>(&self, name: &'static str, attempt: F, teardown: T)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
        T: Fn() + Send + 'static,
    {
        let supervisor = self.clone();
        tokio::spawn(async move { supervisor.supervise(name, attempt, teardown).await });
    }

    async fn supervise<F, Fut, T>(&self, name: &'static str, attempt: F, teardown: T)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
        T: Fn() + Send + 'static,
    {
        let mut consecutive_restarts = 0u32;
        loop {
            let started = Instant::now();
            report_death(name, tokio::spawn(attempt()).await);
            teardown();
            self.mark_down(name);
            if started.elapsed() >= self.policy.stability {
                consecutive_restarts = 0;
            }
            if consecutive_restarts >= self.policy.budget {
                tracing::error!(
                    task = name,
                    budget = self.policy.budget,
                    "a background task exhausted its restart budget; this pod stays NOT READY \
                     until an operator restarts it"
                );
                return;
            }
            tokio::time::sleep(self.policy.backoff(consecutive_restarts)).await;
            consecutive_restarts += 1;
            self.mark_up(name);
        }
    }

    fn mark_down(&self, name: &'static str) {
        self.down_names().insert(name);
        self.publish_readiness();
    }

    pub fn dependency_down(&self, name: &'static str) {
        self.mark_down(name);
    }

    fn mark_up(&self, name: &'static str) {
        self.down_names().remove(name);
        self.publish_readiness();
    }

    pub fn dependency_up(&self, name: &'static str) {
        self.mark_up(name);
    }

    fn down_names(&self) -> std::sync::MutexGuard<'_, BTreeSet<&'static str>> {
        self.down.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish_readiness(&self) {
        let down: Vec<&str> = self.down_names().iter().copied().collect();
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

fn report_death(
    name: &'static str,
    outcome: Result<Result<(), ServiceError>, tokio::task::JoinError>,
) {
    match outcome {
        Ok(Ok(())) => tracing::error!(
            task = name,
            "a background task returned; its source of work ended, which never happens while the \
             service is healthy"
        ),
        Ok(Err(error)) => tracing::error!(task = name, error = %error, "a background task failed"),
        Err(join) => tracing::error!(task = name, error = %join, "a background task panicked"),
    }
}

#[cfg(test)]
mod tests;
