pub mod admin;
pub mod backstop;
pub mod create;
pub mod dispatch;
pub mod environment;
pub mod integration;
pub mod logs;
pub mod presence;
pub mod resolve;
pub mod run_facts;
pub mod write;

use std::sync::Arc;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::policy::{RetryPolicy, ServiceLimits};
use bc_jobs::event::job::JobEvent;
use bc_jobs::policies::affordances::{
    descendant_affordances_changed, predecessor_affordances_changed,
};
use bc_jobs::ports::environment::{Clock, IdFactory, JitterSource};
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::transport::{RunTrigger, RunnerTransport};
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use uuid::Uuid;

use crate::db::PgStore;
use crate::error::ServiceError;
use write::JobChange;

pub const SERVICE_ACTOR: Uuid = Uuid::from_u128(0x019f8137_e784_7320_87f3_13074aacc4d4);

#[derive(Clone)]
pub struct Jobs {
    pub store: PgStore,
    pub transport: Arc<dyn RunnerTransport>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdFactory>,
    pub jitter: Arc<dyn JitterSource>,
    pub limits: ServiceLimits,
    pub retry: RetryPolicy,
}

pub fn service_metadata() -> EventMetadata {
    EventMetadata::new(
        Actor::Service(ServiceAccountId::from(SERVICE_ACTOR)),
        Uuid::now_v7(),
    )
}

impl Jobs {
    pub async fn load(&self, job_id: JobId) -> Result<Option<Job>, ServiceError> {
        Ok(JobReader::load(&self.store, job_id).await?)
    }

    pub async fn require(&self, job_id: JobId) -> Result<Job, ServiceError> {
        self.load(job_id).await?.ok_or(ServiceError::JobNotFound)
    }

    pub async fn commit(
        &self,
        changes: Vec<JobChange>,
        metadata: &EventMetadata,
    ) -> Result<(), ServiceError> {
        let at = self.clock.now();
        let effects: Vec<(JobId, Vec<JobEvent>)> = changes
            .iter()
            .map(|change| (change.job_id, change.events.clone()))
            .collect();
        write::commit_job_changes(&self.store, changes, metadata, at).await?;
        for (job_id, events) in effects {
            self.after_commit(job_id, &events, metadata).await?;
        }
        Ok(())
    }

    async fn after_commit(
        &self,
        job_id: JobId,
        events: &[JobEvent],
        metadata: &EventMetadata,
    ) -> Result<(), ServiceError> {
        let Some(job) = self.load(job_id).await? else {
            return Ok(());
        };
        self.transport_effects(&job, events).await;
        self.affordance_followups(&job, events, metadata).await
    }

    async fn transport_effects(&self, job: &Job, events: &[JobEvent]) {
        for event in events {
            let outcome = match event {
                JobEvent::RunDispatched(fact) => match job.find_run(fact.run_id) {
                    Ok(run) => {
                        self.transport
                            .dispatch(&RunTrigger {
                                job_id: job.id(),
                                run_id: fact.run_id,
                                runner_type: job.runner_type().clone(),
                                config: job.config().cloned(),
                                attempt: run.attempt_number(),
                                triggered_by: job.triggered_by().cloned(),
                            })
                            .await
                    }
                    Err(error) => Err(bc_jobs::ports::PortError::StoredStateRejected(error)),
                },
                JobEvent::RunCancellationRequested(fact) => {
                    self.transport
                        .request_stop(fact.run_id, &fact.reason_code)
                        .await
                }
                JobEvent::RunCancelled(fact) => {
                    match job.find_run(fact.run_id).map(|run| run.has_started()) {
                        Ok(false) => {
                            self.transport
                                .withdraw_trigger(fact.run_id, job.runner_type())
                                .await
                        }
                        _ => Ok(()),
                    }
                }
                _ => Ok(()),
            };
            if let Err(error) = outcome {
                tracing::error!(
                    job_id = %job.id().as_uuid(),
                    event = event.event_type(),
                    error = %error,
                    "runner transport effect failed after commit"
                );
            }
        }
    }

    async fn affordance_followups(
        &self,
        job: &Job,
        events: &[JobEvent],
        metadata: &EventMetadata,
    ) -> Result<(), ServiceError> {
        if !events.iter().any(settles_job) || !job.is_terminal() {
            return Ok(());
        }
        let children = JobReader::load_children(&self.store, job.id()).await?;
        let mut followups: Vec<JobChange> = Vec::new();
        if let Some(event) = predecessor_affordances_changed(job) {
            let predecessor = event.job_id();
            let before = self.load(predecessor).await?;
            followups.push(JobChange::new(predecessor, before, vec![event]));
        }
        for event in descendant_affordances_changed(job, &children) {
            let child = event.job_id();
            let before = children
                .iter()
                .find(|candidate| candidate.id() == child)
                .cloned();
            followups.push(JobChange::new(child, before, vec![event]));
        }
        if followups.is_empty() {
            return Ok(());
        }
        followups.sort_by_key(|change| change.job_id.as_uuid());
        write::commit_job_changes(&self.store, followups, metadata, self.clock.now()).await
    }
}

fn settles_job(event: &JobEvent) -> bool {
    matches!(
        event,
        JobEvent::JobCompleted(_) | JobEvent::JobFailed(_) | JobEvent::JobCancelled(_)
    )
}
