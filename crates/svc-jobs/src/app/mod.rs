pub mod acting;
pub mod admin;
pub mod backstop;
pub mod catch_up;
pub mod claim;
pub mod claim_observability;
pub mod claim_waiters;
pub mod claim_wakes;
pub mod create;
pub mod directory;
pub mod dispatch;
pub mod environment;
pub mod fingerprint;
pub mod fleet;
pub mod followups;
pub mod integration;
pub mod logs;
pub mod presence;
pub mod priority;
pub mod reclaim;
pub mod reconcile;
pub mod resolve;
pub mod resolve_v3;
pub mod restart;
pub mod run_end;
pub mod run_facts;
pub mod runner;
pub mod runner_type_catalog;
pub mod runner_type_impacts;
pub mod runner_type_lifecycle;
pub mod runner_type_settings;
pub mod tree_directory;
pub mod user_max_priorities;
pub mod user_max_priorities_live;
pub mod write;

use std::sync::Arc;

use bc_jobs::JobsError;
use bc_jobs::domain::actions::fleet::RetirementWindow;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::policy::{RetryPolicy, ServiceLimits};
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::environment::{Clock, IdFactory, JitterSource};
use bc_jobs::ports::fleet::{DispatchModeOfferWriter, RunnerTypeCatalogWriter};
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::transport::{RunTrigger, RunnerTransport};
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use uuid::Uuid;

use crate::config::ClaimSettings;
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
    pub catalog: Arc<dyn RunnerTypeCatalogWriter>,
    pub dispatch_modes: Arc<dyn DispatchModeOfferWriter>,
    pub claims: ClaimSettings,
    pub waiters: Arc<claim_waiters::ClaimWaiters>,
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
        let may_wake_a_restart = changes.iter().any(|change| {
            change
                .events
                .iter()
                .any(|event| matches!(event, JobEvent::RunCompleted(_) | JobEvent::JobFailed(_)))
        });
        let effects: Vec<(JobId, Vec<JobEvent>)> = changes
            .iter()
            .map(|change| (change.job_id, change.events.clone()))
            .collect();
        write::commit_job_changes(
            &self.store,
            write::WriteContext {
                ids: self.ids.as_ref(),
                metadata,
                at,
                limits: &self.limits,
            },
            changes,
        )
        .await?;
        let mut ended = Vec::new();
        for (job_id, events) in effects {
            self.after_commit(job_id, &events).await?;
            ended.extend(run_end::ended_runs(job_id, &events));
        }
        for run in &ended {
            Box::pin(run_end::cancel_jobs_requested_by(self, run, metadata)).await?;
        }
        if may_wake_a_restart {
            Box::pin(self.start_due_restarts()).await;
        }
        Ok(())
    }

    pub async fn start_due_restarts(&self) {
        if let Err(error) = restart::process_wakes(self).await {
            tracing::warn!(error = %error, "starting the due restarts failed; the sweep retries");
        }
    }

    async fn after_commit(&self, job_id: JobId, events: &[JobEvent]) -> Result<(), ServiceError> {
        let Some(job) = self.load(job_id).await? else {
            return Ok(());
        };
        self.transport_effects(&job, events).await;
        Ok(())
    }

    pub fn retirement_window(&self) -> RetirementWindow {
        RetirementWindow {
            evaluated_at: self.clock.now(),
            quiet_period: self.limits.retirement_quiet_period(),
        }
    }

    async fn transport_effects(&self, job: &Job, events: &[JobEvent]) {
        for event in events {
            let outcome = match event {
                JobEvent::RunDispatched(fact) if fact.claimed_by.is_some() => Ok(()),
                JobEvent::RunDispatched(fact) => match job.find_run(fact.run_id) {
                    Ok(run) => match RunTrigger::of_job(job, fact.run_id, run.attempt_number()) {
                        Ok(trigger) => self.transport.dispatch(&trigger).await,
                        Err(JobsError::PassportRequired) => {
                            if let Err(error) =
                                Box::pin(reclaim::without_passport(self, job, fact.run_id)).await
                            {
                                tracing::error!(
                                    job_id = %job.id().as_uuid(),
                                    error = %error,
                                    "a run with no Passport to send could not be failed"
                                );
                            }
                            Ok(())
                        }
                        Err(error) => Err(bc_jobs::ports::PortError::StoredStateRejected(error)),
                    },
                    Err(error) => Err(bc_jobs::ports::PortError::StoredStateRejected(error)),
                },
                JobEvent::RunCancellationRequested(fact) => {
                    self.transport
                        .request_stop(fact.run_id, &fact.reason_code)
                        .await
                }
                JobEvent::RunCancelled(fact) => {
                    match job
                        .find_run(fact.run_id)
                        .map(|run| run.is_held_by_an_instance())
                    {
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
}
