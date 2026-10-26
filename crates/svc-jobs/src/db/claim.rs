//! The reads a claim needs: the next claimable Job, locked so no other pod's
//! claim takes it, and the Run already reserved to a claim being repeated.

use bc_jobs::domain::ids::{ClaimId, JobId, RunId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::dispatch_order::{CLAIMED_SCOPE, ORDER_BY, ordered_waiting};
use super::hydrate::unavailable;
use super::jobs::{NO_ACTIVE_RUN, NOT_RESOLVED, UNCONSUMED_RETRY};

/// What a repeated claim finds under its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedBefore {
    /// Nothing: the claim is new.
    Nothing,
    /// A Run reserved to it and not yet started — the reply that was lost.
    Pending { job_id: JobId, run_id: RunId },
    /// A Run its Instance already started or that already settled: the
    /// identity was spent.
    Spent,
}

impl PgStore {
    /// The next Job of `runner_type` waiting at `at`, in dispatch order, locked
    /// for this transaction. A Job another transaction holds is skipped, never
    /// waited for: two claims never reserve the same Job, whatever the number
    /// of pods. `passed_over` are the Jobs this claim already found the domain
    /// refusing.
    pub async fn lock_next_claimable(
        tx: &mut PgConnection,
        runner_type: &RunnerTypeKey,
        at: DateTime<Utc>,
        passed_over: &[Uuid],
    ) -> Result<Option<JobId>, PortError> {
        let sql = format!(
            "{} SELECT j.id::uuid AS id FROM jobs j JOIN ordered o ON o.id = j.id \
             WHERE NOT (j.id = ANY($3)) {ORDER_BY} \
             FOR UPDATE OF j SKIP LOCKED LIMIT 1",
            ordered_waiting(CLAIMED_SCOPE),
        );
        let found: Option<Uuid> = sqlx::query_scalar(&sql)
            .bind(at)
            .bind(runner_type.as_str())
            .bind(passed_over)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        found.map(JobId::new).transpose().map_err(PortError::from)
    }

    pub async fn claimed_before(
        &self,
        runner_type: &RunnerTypeKey,
        instance_key: &InstanceKey,
        claim_id: ClaimId,
    ) -> Result<ClaimedBefore, PortError> {
        let row = sqlx::query(
            "SELECT r.job_id::uuid AS job_id, r.id::uuid AS run_id, \
                (rs.run_id IS NOT NULL OR t.run_id IS NOT NULL) AS spent \
             FROM run_reservations rr \
             JOIN runs r ON r.id = rr.run_id \
             JOIN runner_instances ri ON ri.id = rr.instance_id \
             JOIN runner_types rt ON rt.id = ri.runner_type_id \
             LEFT JOIN run_starts rs ON rs.run_id = r.id \
             LEFT JOIN run_terminals t ON t.run_id = r.id \
             WHERE rt.type_key = $1 AND ri.instance_key = $2 AND rr.claim_id = $3",
        )
        .bind(runner_type.as_str())
        .bind(instance_key.as_str())
        .bind(claim_id.as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            return Ok(ClaimedBefore::Nothing);
        };
        if row.get::<bool, _>("spent") {
            return Ok(ClaimedBefore::Spent);
        }
        Ok(ClaimedBefore::Pending {
            job_id: JobId::new(row.get("job_id"))?,
            run_id: RunId::new(row.get("run_id"))?,
        })
    }

    /// The earliest moment a retry of `runner_type` becomes due, so a held
    /// claim wakes for it without polling.
    pub async fn next_retry_due_for(
        &self,
        runner_type: &RunnerTypeKey,
    ) -> Result<Option<DateTime<Utc>>, PortError> {
        let sql = format!(
            "SELECT min(due.due_at) AS due_at FROM jobs j \
             JOIN LATERAL ({UNCONSUMED_RETRY}) due ON true \
             WHERE {NOT_RESOLVED} AND {NO_ACTIVE_RUN} \
               AND j.runner_type_id = (SELECT rt.id FROM runner_types rt WHERE rt.type_key = $1)"
        );
        let row = sqlx::query(&sql)
            .bind(runner_type.as_str())
            .fetch_one(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(row.get("due_at"))
    }
}
