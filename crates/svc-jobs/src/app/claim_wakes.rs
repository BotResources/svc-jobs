//! Routes the pod's durable facts to the claims it holds open: each fact wakes
//! only the claims of the runner type it concerns, and a fact no held claim
//! can care about is dropped without touching the database.

use std::sync::Arc;

use bc_jobs::event::job::JobEvent;
use tokio::sync::broadcast::error::RecvError;

use super::Jobs;
use crate::error::ServiceError;
use crate::stream::{Fact, Hub};
use crate::supervision::Established;

pub async fn route(
    jobs: Arc<Jobs>,
    hub: Hub,
    established: Established,
) -> Result<(), ServiceError> {
    let mut facts = hub.subscribe();
    established.signal();
    loop {
        let fact = match facts.recv().await {
            Ok(fact) => fact,
            Err(RecvError::Lagged(_)) => {
                jobs.waiters.wake_everyone();
                continue;
            }
            Err(RecvError::Closed) => {
                jobs.waiters.wake_everyone();
                facts = hub.subscribe();
                continue;
            }
        };
        if let Err(error) = wake(&jobs, &fact).await {
            tracing::warn!(error = %error, "a fact could not be routed to the held claims");
            jobs.waiters.wake_everyone();
        }
    }
}

async fn wake(jobs: &Jobs, fact: &Fact) -> Result<(), ServiceError> {
    match fact {
        Fact::Job { job_id, event, .. } => match event.as_ref() {
            JobEvent::JobQueued(queued) => {
                jobs.waiters.wake_one(&queued.runner_type);
                Ok(())
            }
            JobEvent::RetryScheduled(_)
            | JobEvent::JobPriorityForced(_)
            | JobEvent::JobPriorityRecalculated(_) => {
                if !jobs.waiters.is_any_held() {
                    return Ok(());
                }
                let Some(runner_type) = jobs.store.runner_type_of(*job_id).await? else {
                    return Ok(());
                };
                if !jobs.waiters.is_held(&runner_type) {
                    return Ok(());
                }
                if matches!(event.as_ref(), JobEvent::RetryScheduled(_)) {
                    // Every held claim of the type re-reads when the next retry falls due.
                    jobs.waiters.wake_all(&runner_type);
                } else {
                    jobs.waiters.wake_one(&runner_type);
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Fact::Fleet { event, .. } => {
            jobs.waiters.wake_all(event.runner_type());
            Ok(())
        }
        Fact::Log { .. } | Fact::User { .. } => Ok(()),
    }
}
