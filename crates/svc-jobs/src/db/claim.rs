//! The reads a claim needs: the next claimable Job, locked so no other pod's
//! claim takes it, and the Run already reserved to a claim being repeated.

use bc_jobs::domain::ids::{ClaimId, JobId, RunId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::dispatch_order::{CLAIM_CANDIDATES, NEXT_CLAIMABLE};
use super::hydrate::unavailable;

impl PgStore {
    /// The database clock, read once per claim pass: the selection, the
    /// reservation and its facts all carry this instant.
    pub async fn now_in(tx: &mut PgConnection) -> Result<DateTime<Utc>, PortError> {
        sqlx::query_scalar("SELECT now()")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)
    }

    /// The next Job of `runner_type` waiting at `at`, in dispatch order, its
    /// waiting row and its Job row locked for this transaction. A row another
    /// transaction holds is skipped, never waited for: two claims never
    /// reserve the same Job, whatever the number of pods, and a claim never
    /// queues behind a writer. `passed_over` are the Jobs this claim already
    /// found the domain refusing.
    pub async fn lock_next_claimable(
        tx: &mut PgConnection,
        runner_type: &RunnerTypeKey,
        at: DateTime<Utc>,
        passed_over: &[Uuid],
    ) -> Result<NextClaimable, PortError> {
        let found: Option<Uuid> = sqlx::query_scalar(NEXT_CLAIMABLE)
            .bind(at)
            .bind(runner_type.as_str())
            .bind(passed_over)
            .bind(CLAIM_CANDIDATES)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        let Some(job_id) = found else {
            return Ok(NextClaimable::NothingWaiting);
        };
        // The Job row is taken the same way: a writer holding it (a
        // cancellation, a force) is not waited for — that Job is left to the
        // next pass, which finds it waiting or gone.
        let locked: Option<Uuid> =
            sqlx::query_scalar("SELECT id::uuid FROM jobs WHERE id = $1 FOR UPDATE SKIP LOCKED")
                .bind(job_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        let job_id = JobId::new(job_id).map_err(PortError::from)?;
        Ok(match locked {
            Some(_) => NextClaimable::Locked(job_id),
            None => NextClaimable::HeldElsewhere(job_id),
        })
    }

    /// The Job and Run an earlier claim under this identity reserved, if any.
    pub async fn reserved_by_claim(
        &self,
        runner_type: &RunnerTypeKey,
        instance_key: &InstanceKey,
        claim_id: ClaimId,
    ) -> Result<Option<(JobId, RunId)>, PortError> {
        let row = sqlx::query(
            "SELECT r.job_id::uuid AS job_id, r.id::uuid AS run_id \
             FROM run_reservations rr \
             JOIN runs r ON r.id = rr.run_id \
             JOIN runner_instances ri ON ri.id = rr.instance_id \
             JOIN runner_types rt ON rt.id = ri.runner_type_id \
             WHERE rt.type_key = $1 AND ri.instance_key = $2 AND rr.claim_id = $3",
        )
        .bind(runner_type.as_str())
        .bind(instance_key.as_str())
        .bind(claim_id.as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        row.map(|row| -> Result<(JobId, RunId), bc_jobs::JobsError> {
            Ok((
                JobId::new(row.get("job_id"))?,
                RunId::new(row.get("run_id"))?,
            ))
        })
        .transpose()
        .map_err(PortError::from)
    }

    pub async fn runner_type_of(&self, job_id: JobId) -> Result<Option<RunnerTypeKey>, PortError> {
        let key: Option<String> = sqlx::query_scalar(
            "SELECT rt.type_key FROM jobs j JOIN runner_types rt ON rt.id = j.runner_type_id \
             WHERE j.id = $1",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        key.map(RunnerTypeKey::new)
            .transpose()
            .map_err(PortError::from)
    }

    /// How long until the next retry of `runner_type` falls due, by the
    /// database clock — `None` when none is scheduled ahead.
    pub async fn next_retry_due_in(
        &self,
        runner_type: &RunnerTypeKey,
    ) -> Result<Option<std::time::Duration>, PortError> {
        let millis: Option<f64> = sqlx::query_scalar(
            "SELECT (EXTRACT(EPOCH FROM (min(w.due_at) - now())) * 1000)::float8 FROM waiting_jobs w \
             WHERE w.due_at > now() \
               AND w.runner_type_id = (SELECT rt.id FROM runner_types rt WHERE rt.type_key = $1)",
        )
        .bind(runner_type.as_str())
        .fetch_one(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(millis.map(|millis| std::time::Duration::from_millis(millis.max(0.0) as u64)))
    }
}

/// The outcome of looking for the next claimable Job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextClaimable {
    NothingWaiting,
    /// Both its rows are locked by this transaction.
    Locked(JobId),
    /// Its waiting row was free but a writer holds the Job.
    HeldElsewhere(JobId),
}
