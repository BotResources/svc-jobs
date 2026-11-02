mod inline;

use bc_jobs::JobsError;
use bc_jobs::domain::ids::{JobId, ResolutionId, RunId};
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::auto_finish::AutoFinishReader;
use bc_jobs::ports::job::DUE_WORK_BATCH;
use br_core_events::{Actor, EventMetadata, ServiceAccountId};

use self::inline::{Finished, finish_within_budget};
use super::write::JobChange;
use super::{Jobs, SERVICE_ACTOR, retry};
use crate::error::{Contention, ServiceError};

const ATTEMPTS: usize = 3;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Triggers {
    pub completed_runs: Vec<RunId>,
    pub stemming_jobs: Vec<JobId>,
}

impl Triggers {
    pub fn of(effects: &[(JobId, Vec<JobEvent>)]) -> Self {
        let mut triggers = Self::default();
        for (job_id, events) in effects {
            for event in events {
                match event {
                    JobEvent::RunCompleted(fact) => {
                        triggers.completed_runs.push(fact.run_id);
                        triggers.stemming_jobs.push(*job_id);
                    }
                    JobEvent::JobFailed(_) | JobEvent::JobCancelled(_) => {
                        triggers.stemming_jobs.push(*job_id);
                    }
                    _ => {}
                }
            }
        }
        triggers
    }

    pub fn is_empty(&self) -> bool {
        self.completed_runs.is_empty() && self.stemming_jobs.is_empty()
    }
}

pub async fn after_commit(jobs: &Jobs, triggers: &Triggers, cause: &EventMetadata) {
    if triggers.is_empty() {
        return;
    }
    let candidates = match candidates_of(jobs, triggers).await {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "looking for the called jobs to finish failed; the next sweep looks again"
            );
            return;
        }
    };
    finish_within_budget(candidates, DUE_WORK_BATCH, |called| {
        finish_called_job(jobs, called, cause)
    })
    .await;
}

async fn candidates_of(jobs: &Jobs, triggers: &Triggers) -> Result<Vec<JobId>, ServiceError> {
    let mut requesters: Vec<RunId> = triggers.completed_runs.clone();
    requesters.extend(jobs.store.acts_for_all(&triggers.completed_runs).await?);
    let mut candidates = AutoFinishReader::called_by_runs(&jobs.store, &requesters).await?;
    candidates.extend(
        AutoFinishReader::called_jobs_stemming_to(&jobs.store, &triggers.stemming_jobs).await?,
    );
    candidates.sort_by_key(|job| job.as_uuid());
    candidates.dedup();
    Ok(candidates)
}

pub async fn finish_called_job(
    jobs: &Jobs,
    called: JobId,
    cause: &EventMetadata,
) -> Result<Finished, ServiceError> {
    match retry::on_write_race(ATTEMPTS, || finish_once(jobs, called, cause)).await {
        Err(ServiceError::Contended(contention)) => {
            tracing::info!(
                job_id = %called.as_uuid(),
                "finishing a called job was contended; the next end of a run or the next sweep decides again"
            );
            Ok(match contention {
                Contention::LockTimeout => Finished::TreeBusy,
                Contention::WriteRace => Finished::Decided,
            })
        }
        Err(ServiceError::Domain(JobsError::JobAlreadyTerminal { .. } | JobsError::JobDeleted)) => {
            Ok(Finished::Decided)
        }
        outcome => outcome.map(|()| Finished::Decided),
    }
}

async fn finish_once(
    jobs: &Jobs,
    called: JobId,
    cause: &EventMetadata,
) -> Result<(), ServiceError> {
    let Some(facts) = AutoFinishReader::facts_of(&jobs.store, called).await? else {
        return Ok(());
    };
    let plan = match facts.decide() {
        Ok(plan) => plan,
        Err(waiting) => {
            tracing::debug!(
                job_id = %called.as_uuid(),
                ?waiting,
                "a called job is not finished by rule yet"
            );
            return Ok(());
        }
    };
    let result = facts
        .called
        .finish(plan.finish_command(ResolutionId::new(jobs.ids.next())?))?;
    let mut changes = vec![
        JobChange::new(called, Some(facts.called.clone()), result.events)
            .finishing_by_rule(plan.premise().clone()),
    ];
    for restart in facts
        .restart_jobs
        .iter()
        .filter(|restart| plan.restart_jobs_to_finish().contains(&restart.id()))
    {
        let result = restart.finish(plan.finish_command(ResolutionId::new(jobs.ids.next())?))?;
        changes.push(JobChange::new(
            restart.id(),
            Some(restart.clone()),
            result.events,
        ));
    }
    jobs.commit(changes, &finish_metadata(jobs, cause)).await
}

fn finish_metadata(jobs: &Jobs, cause: &EventMetadata) -> EventMetadata {
    let own = EventMetadata::new(
        Actor::Service(ServiceAccountId::from(SERVICE_ACTOR)),
        jobs.ids.next(),
    );
    match cause.causation_id {
        Some(causation) => own.with_causation(causation),
        None => own,
    }
}
