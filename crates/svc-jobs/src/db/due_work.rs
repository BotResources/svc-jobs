use async_trait::async_trait;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::{DUE_WORK_BATCH, DispatchWindow, DueWorkReader};
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::dispatch_order::pushed_in_order;
use super::hydrate::unavailable;
use super::jobs::{NO_ACTIVE_RUN, NOT_RESOLVED, UNCONSUMED_RETRY};

#[async_trait]
impl DueWorkReader for PgStore {
    async fn jobs_awaiting_dispatch(
        &self,
        at: DateTime<Utc>,
        windows: &[DispatchWindow],
    ) -> Result<Vec<Job>, PortError> {
        if windows.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = windows
            .iter()
            .map(|window| window.runner_type.as_str().to_owned())
            .collect();
        let free: Vec<i32> = windows
            .iter()
            .map(|window| i32::try_from(window.free).unwrap_or(i32::MAX))
            .collect();
        let ids: Vec<Uuid> = sqlx::query_scalar(&pushed_in_order())
            .bind(at)
            .bind(i64::try_from(DUE_WORK_BATCH).unwrap_or(i64::MAX))
            .bind(keys)
            .bind(free)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let mut jobs = self.load_batch(&ids).await?;
        jobs.sort_by_key(|job| {
            ids.iter()
                .position(|id| *id == job.id().as_uuid())
                .unwrap_or(usize::MAX)
        });
        Ok(jobs)
    }

    async fn jobs_with_outrun_runs(
        &self,
        now: DateTime<Utc>,
        service_default: TimeDelta,
    ) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT j.id::uuid AS id FROM jobs j \
             WHERE NOT EXISTS (SELECT 1 FROM job_deletions jd WHERE jd.job_id = j.id) \
               AND EXISTS (SELECT 1 FROM runs r \
                JOIN run_starts rs ON rs.run_id = r.id \
                LEFT JOIN run_terminals t ON t.run_id = r.id \
                WHERE r.job_id = j.id AND t.run_id IS NULL \
                  AND rs.started_at + make_interval(secs => \
                      coalesce(r.max_run_duration_seconds, $2)::double precision) < $1) \
             LIMIT {DUE_WORK_BATCH}"
        );
        let rows = sqlx::query(&sql)
            .bind(now)
            .bind(service_default.num_seconds())
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
        self.load_batch(&ids).await
    }

    async fn jobs_with_lost_claims(&self, cutoff: DateTime<Utc>) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT DISTINCT r.job_id::uuid AS id FROM run_reservations rr \
             JOIN runs r ON r.id = rr.run_id \
             WHERE rr.settled_at IS NULL AND rr.reserved_at < $1 \
             LIMIT {DUE_WORK_BATCH}"
        );
        let ids = self.ids(&sql, cutoff).await?;
        self.load_batch(&ids).await
    }

    async fn jobs_with_stranded_triggers(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT DISTINCT u.job_id::uuid AS id FROM unstarted_pushed_runs u \
             JOIN registered_runner_types reg ON reg.runner_type_id = u.runner_type_id \
                AND reg.dispatch_mode = 'CLAIM' \
             WHERE greatest(u.dispatched_at, \
                    coalesce(reg.dispatch_mode_since, reg.registered_at)) < $1 \
             LIMIT {DUE_WORK_BATCH}"
        );
        let ids = self.ids(&sql, cutoff).await?;
        self.load_batch(&ids).await
    }

    async fn jobs_idle_since(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT j.id::uuid AS id FROM jobs j \
             JOIN LATERAL (SELECT max(activity.moment) AS last_activity FROM ( \
                    SELECT r.dispatched_at AS moment FROM runs r WHERE r.job_id = j.id \
                    UNION ALL \
                    SELECT rs.started_at FROM run_starts rs \
                        JOIN runs r ON r.id = rs.run_id WHERE r.job_id = j.id \
                    UNION ALL \
                    SELECT t.occurred_at FROM run_terminals t \
                        JOIN runs r ON r.id = t.run_id WHERE r.job_id = j.id \
                    UNION ALL \
                    SELECT s.started_at FROM steps s \
                        JOIN runs r ON r.id = s.run_id WHERE r.job_id = j.id \
                    UNION ALL \
                    SELECT sc.due_at FROM run_retry_schedules sc \
                        JOIN runs r ON r.id = sc.failed_run_id WHERE r.job_id = j.id \
                    UNION ALL \
                    SELECT j.created_at) activity) idle ON true \
             WHERE {NOT_RESOLVED} AND {NO_ACTIVE_RUN} \
               AND EXISTS (SELECT 1 FROM runs r \
                    WHERE r.job_id = j.id AND r.withdrawn_reason IS NULL) \
               AND NOT EXISTS ({UNCONSUMED_RETRY}) \
               AND idle.last_activity < $1 \
             LIMIT {DUE_WORK_BATCH}"
        );
        let ids = self.ids(&sql, at).await?;
        self.load_batch(&ids).await
    }
}
