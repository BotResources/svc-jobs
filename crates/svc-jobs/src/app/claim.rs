//! Serving the claim of one Instance of a `CLAIM` RunnerType.
//!
//! The claim is held open until a Job is waiting for the type, then that Job is
//! reserved to the Instance and its Run created in the same transaction that
//! locked it — `FOR UPDATE SKIP LOCKED`, so the claims served by several pods
//! never reserve one Job twice. A claim repeated under the same identity is
//! answered with the Run already reserved to it. At the end of the wait the
//! answer is empty.

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

/// What a claim is answered with, in the service's terms; the runner
/// transport words it for the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimAnswer {
    Served(Box<RunTrigger>),
    /// Nothing was waiting during the wait.
    Empty,
    /// The domain refused to serve this Instance of this type.
    Refused(JobsError),
    /// The identity already served a Run that was started or settled.
    Spent,
    /// The claim names no valid type, Instance or identity.
    Malformed,
    /// The pod could not serve the claim — its store failed; nothing was
    /// reserved for it.
    Unavailable,
}

/// The longest a reservation may start before the claim's deadline: a Run is
/// never reserved for a claim whose reply would arrive after its Instance gave
/// up waiting.
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
                // Taking the head of a (root, level) uncovers the next Job of
                // that pair — a claim parked because every head was held looks
                // again.
                drop(held);
                jobs.waiters.wake_one(&key);
                return Ok(ClaimAnswer::Served(trigger));
            }
            Ok(Reservation::PassedOver(job_id)) => {
                passed_over.push(job_id.as_uuid());
                continue;
            }
            Ok(Reservation::NothingWaiting) => {}
            // Another claim or write moved the job between the selection and the
            // commit; the job this pass locked and rolled back may still wait, so
            // another held claim of the type is woken to look again. A repeat of
            // this very claim on another pod finds its run on the next pass.
            Err(ServiceError::Contended) => {
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

/// A claim repeated under an identity that already reserved a Run: the
/// domain decides whether that Run is handed back or the identity was spent.
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
            ClaimAnswer::Served(Box::new(RunTrigger::of_job(&job, run_id, attempt)))
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
    let at = PgStore::now_in(&mut tx).await?;
    let job_id =
        match PgStore::lock_next_claimable(&mut tx, runner_type.key(), at, passed_over).await? {
            NextClaimable::NothingWaiting => return Ok(Reservation::NothingWaiting),
            NextClaimable::HeldElsewhere(job_id) => return Ok(Reservation::PassedOver(job_id)),
            NextClaimable::Locked(job_id) => job_id,
        };
    let job = PgStore::load_in(&mut tx, job_id)
        .await?
        .ok_or(ServiceError::Contended)?;
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
    Ok(Reservation::Reserved(Box::new(RunTrigger::of_job(
        &job, run_id, attempt,
    ))))
}

/// Parks the claim until it is woken — a Job queued for the type, the type's
/// retry timer firing, the fleet of the type moving — or the deadline. The
/// pod keeps one retry timer per type: the first claim parking while none is
/// armed arms it for the next retry, by the database clock. `false` at the
/// deadline.
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
