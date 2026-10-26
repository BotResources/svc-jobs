//! Serving the claim of one Instance of a `CLAIM` RunnerType.
//!
//! The claim is held open until a Job is waiting for the type, then that Job is
//! reserved to the Instance and its Run created in the same transaction that
//! locked it — `FOR UPDATE SKIP LOCKED`, so the claims served by several pods
//! never reserve one Job twice. A claim repeated under the same identity finds
//! the Run already reserved to it. At the end of the wait the answer is empty.

use std::time::Duration;

use bc_jobs::commands::job::dispatch::ReserveRun;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::ids::{ClaimId, JobId, RunId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::transport::RunTrigger;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

use super::write::{self, JobChange};
use super::{Jobs, service_metadata};
use crate::db::PgStore;
use crate::db::claim::ClaimedBefore;
use crate::error::ServiceError;
use crate::stream::{Fact, Hub};

/// What a claim is answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimAnswer {
    Served(RunTrigger),
    /// Nothing was waiting during the wait.
    Empty,
    /// The claim was refused, under a stable code.
    Refused(&'static str),
}

/// A claim identity whose Run was already started or settled: it named one
/// slot, and that slot was spent.
pub const CLAIM_SPENT: &str = "claim_spent";

const MINIMUM_WAKE: Duration = Duration::from_millis(250);

pub struct ClaimRequest {
    pub runner_type: String,
    pub instance_key: String,
    pub claim_id: Uuid,
    pub wait: Duration,
}

pub async fn serve(jobs: &Jobs, hub: &Hub, request: ClaimRequest) -> ClaimAnswer {
    match answer(jobs, hub, request).await {
        Ok(answer) => answer,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "a claim could not be served; it is answered empty and the instance claims again"
            );
            ClaimAnswer::Empty
        }
    }
}

enum Reservation {
    Reserved(RunTrigger),
    NothingWaiting,
    PassedOver(JobId),
}

async fn answer(
    jobs: &Jobs,
    hub: &Hub,
    request: ClaimRequest,
) -> Result<ClaimAnswer, ServiceError> {
    let (Ok(key), Ok(instance_key)) = (
        RunnerTypeKey::new(&request.runner_type),
        InstanceKey::new(&request.instance_key),
    ) else {
        return Ok(ClaimAnswer::Refused(
            bc_jobs::JobsError::InstanceUnknown {
                instance_key: request.instance_key,
            }
            .code(),
        ));
    };
    let claim_id = match ClaimId::new(request.claim_id) {
        Ok(claim_id) => claim_id,
        Err(refusal) => return Ok(ClaimAnswer::Refused(refusal.code())),
    };
    let deadline = Instant::now() + request.wait.min(jobs.claims.max_wait);
    let mut facts = hub.subscribe();
    let mut passed_over: Vec<Uuid> = Vec::new();
    loop {
        let Some(runner_type) = FleetReader::load(&jobs.store, &key).await? else {
            return Ok(ClaimAnswer::Refused(
                bc_jobs::JobsError::InstanceUnknown {
                    instance_key: instance_key.as_str().to_owned(),
                }
                .code(),
            ));
        };
        if let Err(refusal) = runner_type.guard_claim(&instance_key) {
            return Ok(ClaimAnswer::Refused(refusal.code()));
        }
        match jobs
            .store
            .claimed_before(&key, &instance_key, claim_id)
            .await?
        {
            ClaimedBefore::Pending { job_id, run_id } => {
                return Ok(ClaimAnswer::Served(trigger_of(jobs, job_id, run_id).await?));
            }
            ClaimedBefore::Spent => return Ok(ClaimAnswer::Refused(CLAIM_SPENT)),
            ClaimedBefore::Nothing => {}
        }
        match reserve(jobs, &runner_type, &instance_key, claim_id, &passed_over).await {
            Ok(Reservation::Reserved(trigger)) => return Ok(ClaimAnswer::Served(trigger)),
            Ok(Reservation::PassedOver(job_id)) => {
                passed_over.push(job_id.as_uuid());
                if Instant::now() >= deadline {
                    return Ok(ClaimAnswer::Empty);
                }
                continue;
            }
            Ok(Reservation::NothingWaiting) => {}
            // Another claim or write moved the job between the selection and the
            // commit: the next pass sees it settled — a repeat of this very claim
            // finds the run it reserved.
            Err(ServiceError::Contended) if Instant::now() < deadline => continue,
            Err(ServiceError::Contended) => return Ok(ClaimAnswer::Empty),
            Err(other) => return Err(other),
        }
        if !wait_for_work(jobs, &key, &mut facts, hub, deadline).await? {
            return Ok(ClaimAnswer::Empty);
        }
    }
}

async fn reserve(
    jobs: &Jobs,
    runner_type: &RunnerType,
    instance_key: &InstanceKey,
    claim_id: ClaimId,
    passed_over: &[Uuid],
) -> Result<Reservation, ServiceError> {
    let at = jobs.clock.now();
    let mut tx = jobs.store.begin().await?;
    let Some(job_id) =
        PgStore::lock_next_claimable(&mut tx, runner_type.key(), at, passed_over).await?
    else {
        return Ok(Reservation::NothingWaiting);
    };
    let job = PgStore::load_in(&mut tx, job_id)
        .await?
        .ok_or(ServiceError::Contended)?;
    let run_id = RunId::new(jobs.ids.next())?;
    let decided = job.reserve_run(
        ReserveRun {
            run_id,
            at,
            max_run_duration: runner_type.max_run_duration(),
            instance: RunnerInstanceReference::new(runner_type.key().clone(), instance_key.clone()),
            claim_id,
        },
        &jobs.limits,
    );
    let result = match decided {
        Ok(result) => result,
        Err(refusal) => {
            tracing::debug!(
                job_id = %job_id.as_uuid(),
                code = refusal.code(),
                "a claimable job was refused by the domain; the claim moves to the next one"
            );
            return Ok(Reservation::PassedOver(job_id));
        }
    };
    let Some(attempt) = result.events.iter().find_map(|event| match event {
        JobEvent::RunDispatched(fact) => Some(fact.attempt_number),
        _ => None,
    }) else {
        return Ok(Reservation::PassedOver(job_id));
    };
    write::commit_job_changes_in(
        &mut tx,
        jobs.ids.as_ref(),
        vec![JobChange::new(job_id, Some(job.clone()), result.events)],
        &service_metadata(),
        at,
    )
    .await?;
    tx.commit().await?;
    Ok(Reservation::Reserved(RunTrigger {
        job_id,
        run_id,
        runner_type: job.runner_type().clone(),
        config: job.config().cloned(),
        attempt,
        triggered_by: job.triggered_by().cloned(),
    }))
}

async fn trigger_of(jobs: &Jobs, job_id: JobId, run_id: RunId) -> Result<RunTrigger, ServiceError> {
    let job = jobs.require(job_id).await?;
    let run = job.find_run(run_id)?;
    Ok(RunTrigger {
        job_id,
        run_id,
        runner_type: job.runner_type().clone(),
        config: job.config().cloned(),
        attempt: run.attempt_number(),
        triggered_by: job.triggered_by().cloned(),
    })
}

/// Parks the claim until something could make a Job of the type claimable — a
/// Job queued, a retry falling due, the fleet of the type moving — or until
/// the deadline. `false` at the deadline.
async fn wait_for_work(
    jobs: &Jobs,
    key: &RunnerTypeKey,
    facts: &mut tokio::sync::broadcast::Receiver<Fact>,
    hub: &Hub,
    deadline: Instant,
) -> Result<bool, ServiceError> {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        let wake = match jobs.store.next_retry_due_for(key).await? {
            Some(due) => match (due - jobs.clock.now()).to_std() {
                Ok(remaining) => deadline.min(now + remaining.max(MINIMUM_WAKE)),
                // A retry already due that the claim could not take — refused by the
                // domain, or taken by another claim — is looked at again after the
                // floor, never in a spin.
                Err(_) => deadline.min(now + MINIMUM_WAKE),
            },
            None => deadline,
        };
        tokio::select! {
            () = sleep_until(wake) => return Ok(Instant::now() < deadline),
            fact = facts.recv() => match fact {
                Ok(fact) if may_make_work_claimable(&fact, key) => return Ok(true),
                Ok(_) => {}
                Err(RecvError::Lagged(_)) => return Ok(true),
                Err(RecvError::Closed) => {
                    *facts = hub.subscribe();
                    return Ok(true);
                }
            },
        }
    }
}

fn may_make_work_claimable(fact: &Fact, key: &RunnerTypeKey) -> bool {
    match fact {
        Fact::Job { event, .. } => match event.as_ref() {
            JobEvent::JobQueued(queued) => &queued.runner_type == key,
            JobEvent::RetryScheduled(_) => true,
            _ => false,
        },
        Fact::Fleet { event, .. } => event.runner_type() == key,
        _ => false,
    }
}
