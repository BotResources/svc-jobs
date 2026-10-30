use std::time::Duration;

use bc_jobs::JobsError;
use bc_jobs::commands::job::dispatch::ReserveRun;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::ids::{ClaimId, JobId, RunId};
use bc_jobs::domain::job::claim::RecalledClaim;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::transport::RunTrigger;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

use super::claim_waiters::HeldClaim;
use super::write::{self, JobChange, WriteContext};
use super::{Jobs, service_metadata};
use crate::db::PgStore;
use crate::db::claim::NextClaimable;
use crate::error::ServiceError;

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimAnswer {
    Served(Box<RunTrigger>),
    Empty,
    Refused(JobsError),
    Spent,
    Malformed,
    Unavailable,
}

const RESERVATION_MARGIN: Duration = Duration::from_millis(500);

pub struct ClaimRequest {
    pub runner_type: String,
    pub instance_key: String,
    pub claim_id: Uuid,
    pub wait: Duration,
}

pub async fn serve(jobs: &Jobs, request: ClaimRequest) -> ClaimAnswer {
    match answer(jobs, request).await {
        Ok(answer) => answer,
        Err(ServiceError::Domain(refusal)) => ClaimAnswer::Refused(refusal),
        Err(error) => {
            tracing::warn!(
                error = %error,
                "a claim could not be served; the instance repeats it after the back-off"
            );
            ClaimAnswer::Unavailable
        }
    }
}

enum Reservation {
    Reserved(Box<RunTrigger>),
    NothingWaiting,
    PassedOver(JobId),
}

async fn answer(jobs: &Jobs, request: ClaimRequest) -> Result<ClaimAnswer, ServiceError> {
    let (Ok(key), Ok(instance_key), Ok(claim_id)) = (
        RunnerTypeKey::new(&request.runner_type),
        InstanceKey::new(&request.instance_key),
        ClaimId::new(request.claim_id),
    ) else {
        return Ok(ClaimAnswer::Malformed);
    };
    let wait = request.wait.min(jobs.claims.max_wait);
    let deadline = Instant::now() + wait;
    let last_reservation = deadline - RESERVATION_MARGIN.min(wait / 5);
    let mut passed_over: Vec<Uuid> = Vec::new();
    let mut first_pass = true;
    loop {
        if !first_pass && Instant::now() >= last_reservation {
            return Ok(ClaimAnswer::Empty);
        }
        first_pass = false;
        let Some(runner_type) = FleetReader::load(&jobs.store, &key).await? else {
            return Ok(ClaimAnswer::Refused(JobsError::InstanceUnknown {
                instance_key: instance_key.as_str().to_owned(),
            }));
        };
        if let Err(refusal) = runner_type.guard_claim(&instance_key) {
            return Ok(ClaimAnswer::Refused(refusal));
        }
        if let Some(answer) = recalled(jobs, &key, &instance_key, claim_id).await? {
            return Ok(answer);
        }
        let held = jobs.waiters.hold(&key);
        match reserve(jobs, &runner_type, &instance_key, claim_id, &passed_over).await {
            Ok(Reservation::Reserved(trigger)) => {
                drop(held);
                jobs.waiters.wake_one(&key);
                return Ok(ClaimAnswer::Served(trigger));
            }
            Ok(Reservation::PassedOver(job_id)) => {
                passed_over.push(job_id.as_uuid());
                continue;
            }
            Ok(Reservation::NothingWaiting) => {}
            Err(error) if error.is_write_race() => {
                jobs.waiters.wake_one(&key);
                continue;
            }
            Err(other) => return Err(other),
        }
        if !wait_for_work(jobs, &key, &held, deadline).await? {
            return Ok(ClaimAnswer::Empty);
        }
    }
}

async fn recalled(
    jobs: &Jobs,
    key: &RunnerTypeKey,
    instance_key: &InstanceKey,
    claim_id: ClaimId,
) -> Result<Option<ClaimAnswer>, ServiceError> {
    let Some((job_id, run_id)) = jobs
        .store
        .reserved_by_claim(key, instance_key, claim_id)
        .await?
    else {
        return Ok(None);
    };
    let job = jobs.require(job_id).await?;
    Ok(Some(match job.recall_claim(run_id)? {
        RecalledClaim::Pending { run_id, attempt } => {
            match RunTrigger::of_job(&job, run_id, attempt) {
                Ok(trigger) => ClaimAnswer::Served(Box::new(trigger)),
                Err(JobsError::PassportRequired) => {
                    super::reclaim::without_passport(jobs, &job, run_id).await?;
                    ClaimAnswer::Spent
                }
                Err(other) => return Err(other.into()),
            }
        }
        RecalledClaim::Spent => ClaimAnswer::Spent,
    }))
}

async fn reserve(
    jobs: &Jobs,
    runner_type: &RunnerType,
    instance_key: &InstanceKey,
    claim_id: ClaimId,
    passed_over: &[Uuid],
) -> Result<Reservation, ServiceError> {
    let mut tx = jobs.store.begin().await?;
    match write::guard_free_capacity(&mut tx, runner_type.key()).await {
        Ok(()) => {}
        Err(ServiceError::Domain(JobsError::NoFreeCapacity { .. })) => {
            return Ok(Reservation::NothingWaiting);
        }
        Err(other) => return Err(other),
    }
    let at = PgStore::now_in(&mut tx).await?;
    let job_id =
        match PgStore::lock_next_claimable(&mut tx, runner_type.key(), at, passed_over).await? {
            NextClaimable::NothingWaiting => return Ok(Reservation::NothingWaiting),
            NextClaimable::HeldElsewhere(job_id) => return Ok(Reservation::PassedOver(job_id)),
            NextClaimable::Locked(job_id) => job_id,
        };
    let job = PgStore::load_in(&mut tx, job_id)
        .await?
        .ok_or_else(ServiceError::write_race)?;
    let run_id = RunId::new(jobs.ids.next())?;
    let decided = job.reserve_run(
        ReserveRun {
            run_id,
            at,
            max_run_duration: runner_type
                .max_run_duration_or(MaxRunDuration::from_delta(jobs.limits.max_run_duration())?),
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
        WriteContext {
            ids: jobs.ids.as_ref(),
            metadata: &service_metadata(),
            at,
            limits: &jobs.limits,
        },
        vec![JobChange::new(job_id, Some(job.clone()), result.events)],
    )
    .await?;
    tx.commit().await?;
    match RunTrigger::of_job(&job, run_id, attempt) {
        Ok(trigger) => Ok(Reservation::Reserved(Box::new(trigger))),
        Err(JobsError::PassportRequired) => {
            let stored = jobs.require(job_id).await?;
            super::reclaim::without_passport(jobs, &stored, run_id).await?;
            Ok(Reservation::PassedOver(job_id))
        }
        Err(other) => Err(other.into()),
    }
}

async fn wait_for_work(
    jobs: &Jobs,
    key: &RunnerTypeKey,
    held: &HeldClaim,
    deadline: Instant,
) -> Result<bool, ServiceError> {
    if Instant::now() >= deadline {
        return Ok(false);
    }
    if !jobs.waiters.has_retry_timer(key)
        && let Some(due_in) = jobs.store.next_retry_due_in(key).await?
    {
        jobs.waiters.arm_retry(key, due_in);
    }
    tokio::select! {
        () = sleep_until(deadline) => {}
        () = held.woken() => {}
    }
    Ok(Instant::now() < deadline)
}
