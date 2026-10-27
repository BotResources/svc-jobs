//! Repairing the access paths from the facts they derive from.
//!
//! `waiting_jobs` and `unstarted_pushed_runs` are maintained in the
//! transaction of every fact a 0.4 pod records. During a rolling update a 0.3
//! pod keeps recording facts — admissions, dispatches, retries — without
//! knowing either table, and a Job it leaves waiting would never be offered to
//! dispatch. The repair recomputes both paths for a set of Jobs from the
//! ledger and writes the difference: missing rows inserted, rows of Jobs that
//! no longer wait (or triggers since taken) deleted, drifted rows corrected.
//! It is idempotent and safe beside live writers. The drift is found by a
//! read that takes no lock; only the Jobs actually drifted are then locked —
//! with `SKIP LOCKED`, so a Job a writer holds is left to the writer and to
//! the next sweep — and repaired. A correct row is never locked by the
//! repair: a claim skipping locked rows never passes over a Job the repair
//! merely looked at.

use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::lineage::root_of;

/// How many Jobs one repair transaction covers.
pub const REPAIR_BATCH: usize = 500;

/// The facts that make a Job start or stop waiting, or publish, take or
/// settle a trigger.
const PATH_FACTS: [&str; 13] = [
    "JobQueued",
    "RunDispatched",
    "RunStarted",
    "RunCompleted",
    "RunFailed",
    "RunCancelled",
    "RetryScheduled",
    "JobCompleted",
    "JobFailed",
    "JobCancelled",
    "JobDeleted",
    "JobPriorityForced",
    "JobPriorityRecalculated",
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Repaired {
    pub waiting_removed: i64,
    pub waiting_written: i64,
    pub triggers_removed: i64,
    pub triggers_added: i64,
}

impl Repaired {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn add(&mut self, other: Self) {
        self.waiting_removed += other.waiting_removed;
        self.waiting_written += other.waiting_written;
        self.triggers_removed += other.triggers_removed;
        self.triggers_added += other.triggers_added;
    }
}

impl PgStore {
    /// Every Job whose access paths may be wrong: the unresolved ones, and
    /// those either path still names. Read once, at boot.
    pub async fn every_job_on_a_path(&self) -> Result<Vec<Uuid>, PortError> {
        sqlx::query_scalar(
            "SELECT j.id::uuid FROM jobs j \
             WHERE NOT EXISTS (SELECT 1 FROM job_resolutions r WHERE r.job_id = j.id) \
               AND NOT EXISTS (SELECT 1 FROM job_deletions d WHERE d.job_id = j.id) \
             UNION SELECT w.job_id::uuid FROM waiting_jobs w \
             UNION SELECT u.job_id::uuid FROM unstarted_pushed_runs u",
        )
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)
    }

    /// The Jobs that recorded a path fact since `since`, whichever pod
    /// recorded it.
    pub async fn jobs_with_path_facts_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<Uuid>, PortError> {
        sqlx::query_scalar(
            "SELECT DISTINCT e.aggregate_id::uuid FROM domain_events e \
             WHERE e.aggregate_type = 'Job' AND e.event_type = ANY($1) \
               AND e.occurred_at >= $2",
        )
        .bind(&PATH_FACTS[..])
        .bind(since)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)
    }

    /// Repairs both access paths for `jobs`, batch by batch: the drift is
    /// read without a lock, and only the drifted Jobs are locked and fixed.
    pub async fn repair_access_paths(&self, jobs: &[Uuid]) -> Result<Repaired, PortError> {
        let mut repaired = Repaired::default();
        for batch in jobs.chunks(REPAIR_BATCH) {
            let drifted: Vec<Uuid> = sqlx::query_scalar(&drift_sql())
                .bind(batch)
                .fetch_all(self.pool())
                .await
                .map_err(unavailable)?;
            if drifted.is_empty() {
                continue;
            }
            let mut tx = self.begin().await?;
            repaired.add(repair_batch(&mut tx, &drifted).await?);
            tx.commit().await.map_err(unavailable)?;
        }
        Ok(repaired)
    }

    /// Forgets the turns of roots that have nothing waiting any more and were
    /// last served before `cutoff`: a root that waits again later starts as
    /// one never served.
    pub async fn purge_idle_turns(&self, cutoff: DateTime<Utc>) -> Result<u64, PortError> {
        let done = sqlx::query(
            "DELETE FROM root_dispatch_turns t \
             WHERE t.last_served_at < $1 \
               AND NOT EXISTS (SELECT 1 FROM waiting_jobs w \
                    WHERE w.runner_type_id = t.runner_type_id \
                      AND w.root_job_id = t.root_job_id)",
        )
        .bind(cutoff)
        .execute(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(done.rows_affected())
    }
}

async fn repair_batch(tx: &mut PgConnection, batch: &[Uuid]) -> Result<Repaired, PortError> {
    let locked: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id::uuid FROM jobs WHERE id = ANY($1) ORDER BY id FOR UPDATE SKIP LOCKED",
    )
    .bind(batch)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    if locked.is_empty() {
        return Ok(Repaired::default());
    }
    let waiting = format!(
        "WITH {desired}, \
         removed AS ( \
            DELETE FROM waiting_jobs w WHERE w.job_id = ANY($1) \
              AND NOT EXISTS (SELECT 1 FROM desired d WHERE d.job_id = w.job_id) \
            RETURNING w.job_id), \
         written AS ( \
            INSERT INTO waiting_jobs (job_id, runner_type_id, root_job_id, \
                effective_priority, created_at, due_at) \
            SELECT job_id, runner_type_id, root_job_id, effective_priority, created_at, due_at \
            FROM desired \
            ON CONFLICT (job_id) DO UPDATE SET runner_type_id = EXCLUDED.runner_type_id, \
                root_job_id = EXCLUDED.root_job_id, \
                effective_priority = EXCLUDED.effective_priority, due_at = EXCLUDED.due_at \
            WHERE (waiting_jobs.runner_type_id, waiting_jobs.root_job_id, \
                    waiting_jobs.effective_priority, waiting_jobs.due_at) \
                IS DISTINCT FROM (EXCLUDED.runner_type_id, EXCLUDED.root_job_id, \
                    EXCLUDED.effective_priority, EXCLUDED.due_at) \
            RETURNING job_id) \
         SELECT (SELECT count(*) FROM removed), (SELECT count(*) FROM written)",
        desired = desired_waiting(),
    );
    let (waiting_removed, waiting_written): (i64, i64) = sqlx::query_as(&waiting)
        .bind(&locked)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    let (triggers_removed, triggers_added): (i64, i64) = sqlx::query_as(&format!(
        "WITH {}, \
         removed AS ( \
            DELETE FROM unstarted_pushed_runs u WHERE u.job_id = ANY($1) \
              AND NOT EXISTS (SELECT 1 FROM desired_runs d WHERE d.run_id = u.run_id) \
            RETURNING u.run_id), \
         added AS ( \
            INSERT INTO unstarted_pushed_runs (run_id, job_id, runner_type_id, dispatched_at) \
            SELECT run_id, job_id, runner_type_id, dispatched_at FROM desired_runs \
            ON CONFLICT (run_id) DO NOTHING \
            RETURNING run_id) \
         SELECT (SELECT count(*) FROM removed), (SELECT count(*) FROM added)",
        desired_triggers(),
    ))
    .bind(&locked)
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(Repaired {
        waiting_removed,
        waiting_written,
        triggers_removed,
        triggers_added,
    })
}

/// The waiting rows the ledger implies for the Jobs in `$1`, as the CTEs `c`
/// and `desired`.
fn desired_waiting() -> String {
    format!(
        "c AS (SELECT * FROM jobs WHERE id = ANY($1)), \
         desired AS ( \
            SELECT c.id AS job_id, c.runner_type_id, {root} AS root_job_id, \
                c.effective_priority, c.created_at, retry.due_at \
            FROM c \
            LEFT JOIN LATERAL ( \
                SELECT s.due_at FROM run_retry_schedules s \
                JOIN runs fr ON fr.id = s.failed_run_id \
                LEFT JOIN runs dr ON dr.automatic_retry_schedule_id = s.id \
                    AND dr.withdrawn_reason IS NULL \
                WHERE fr.job_id = c.id AND dr.id IS NULL \
                ORDER BY s.due_at DESC LIMIT 1) retry ON true \
            WHERE NOT EXISTS (SELECT 1 FROM job_resolutions r WHERE r.job_id = c.id) \
              AND NOT EXISTS (SELECT 1 FROM job_deletions d WHERE d.job_id = c.id) \
              AND NOT EXISTS (SELECT 1 FROM runs r \
                    LEFT JOIN run_terminals t ON t.run_id = r.id \
                    WHERE r.job_id = c.id AND t.run_id IS NULL) \
              AND (retry.due_at IS NOT NULL OR NOT EXISTS (SELECT 1 FROM runs r \
                    WHERE r.job_id = c.id AND r.withdrawn_reason IS NULL)))",
        root = root_of("c"),
    )
}

/// The outstanding triggers the ledger implies for the Jobs in `$1`, as the
/// CTE `desired_runs`.
fn desired_triggers() -> &'static str {
    "desired_runs AS ( \
        SELECT r.id AS run_id, r.job_id, j.runner_type_id, r.dispatched_at \
        FROM runs r JOIN jobs j ON j.id = r.job_id \
        WHERE r.job_id = ANY($1) \
          AND NOT EXISTS (SELECT 1 FROM run_starts s WHERE s.run_id = r.id) \
          AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id = r.id) \
          AND NOT EXISTS (SELECT 1 FROM run_reservations rr WHERE rr.run_id = r.id))"
}

/// The Jobs of `$1` whose access paths differ from what the ledger implies:
/// a waiting row missing, stale or drifted, a trigger row missing or stale.
/// A plain read — nothing is locked.
fn drift_sql() -> String {
    format!(
        "WITH {waiting}, {triggers} \
         SELECT d.job_id::uuid FROM desired d \
            LEFT JOIN waiting_jobs w ON w.job_id = d.job_id \
            WHERE w.job_id IS NULL \
               OR (w.runner_type_id, w.root_job_id, w.effective_priority, w.due_at) \
                  IS DISTINCT FROM (d.runner_type_id, d.root_job_id, d.effective_priority, \
                    d.due_at) \
         UNION SELECT w.job_id::uuid FROM waiting_jobs w \
            WHERE w.job_id = ANY($1) \
              AND NOT EXISTS (SELECT 1 FROM desired d WHERE d.job_id = w.job_id) \
         UNION SELECT dr.job_id::uuid FROM desired_runs dr \
            WHERE NOT EXISTS (SELECT 1 FROM unstarted_pushed_runs u WHERE u.run_id = dr.run_id) \
         UNION SELECT u.job_id::uuid FROM unstarted_pushed_runs u \
            WHERE u.job_id = ANY($1) \
              AND NOT EXISTS (SELECT 1 FROM desired_runs dr WHERE dr.run_id = u.run_id)",
        waiting = desired_waiting(),
        triggers = desired_triggers(),
    )
}
