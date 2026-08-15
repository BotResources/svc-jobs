use async_trait::async_trait;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::{DueWorkReader, JobReader};
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, Row, Transaction};
use uuid::Uuid;

use super::PgStore;
use super::hydrate::{load_many, unavailable};

const NO_ACTIVE_RUN: &str = "NOT EXISTS (SELECT 1 FROM runs r \
     LEFT JOIN run_terminals t ON t.run_id = r.id \
     WHERE r.job_id = j.id AND t.run_id IS NULL)";

const NOT_RESOLVED: &str = "NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
     AND NOT EXISTS (SELECT 1 FROM job_deletions jd WHERE jd.job_id = j.id)";

const UNCONSUMED_RETRY: &str = "SELECT s.due_at FROM run_retry_schedules s \
     JOIN runs fr ON fr.id = s.failed_run_id \
     LEFT JOIN runs dr ON dr.automatic_retry_schedule_id = s.id \
     WHERE fr.job_id = j.id AND dr.id IS NULL";

impl PgStore {
    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>, PortError> {
        self.pool().begin().await.map_err(unavailable)
    }

    pub async fn lock_job(tx: &mut PgConnection, job_id: JobId) -> Result<Option<Job>, PortError> {
        let locked: Option<Uuid> =
            sqlx::query_scalar("SELECT id::uuid FROM jobs WHERE id = $1 FOR UPDATE")
                .bind(job_id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        match locked {
            None => Ok(None),
            Some(id) => Ok(load_many(&mut *tx, &[id]).await?.pop()),
        }
    }

    pub async fn load_in(tx: &mut PgConnection, job_id: JobId) -> Result<Option<Job>, PortError> {
        Ok(load_many(&mut *tx, &[job_id.as_uuid()]).await?.pop())
    }

    pub async fn load_batch(&self, ids: &[Uuid]) -> Result<Vec<Job>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load_many(&mut connection, ids).await
    }

    pub async fn next_retry_due_at(&self) -> Result<Option<DateTime<Utc>>, PortError> {
        let sql = format!(
            "SELECT min(due.due_at) AS due_at FROM jobs j \
             JOIN LATERAL ({UNCONSUMED_RETRY}) due ON true \
             WHERE {NOT_RESOLVED} AND {NO_ACTIVE_RUN}"
        );
        let row = sqlx::query(&sql)
            .fetch_one(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(row.get("due_at"))
    }

    pub async fn declaring_actor(&self, job_id: JobId) -> Result<Option<Uuid>, PortError> {
        let found: Option<String> = sqlx::query_scalar(
            "SELECT metadata->>'actor_id' FROM domain_events \
             WHERE aggregate_type = 'Job' AND aggregate_id = $1 AND event_type = 'JobQueued' \
             ORDER BY aggregate_version LIMIT 1",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(found.and_then(|raw| Uuid::parse_str(&raw).ok()))
    }

    pub async fn job_of_run(&self, run_id: Uuid) -> Result<Option<JobId>, PortError> {
        let found: Option<Uuid> = sqlx::query_scalar("SELECT job_id::uuid FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(self.pool())
            .await
            .map_err(unavailable)?;
        found.map(JobId::new).transpose().map_err(PortError::from)
    }

    pub async fn runs_terminal_before(
        &self,
        run_ids: &[Uuid],
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<Uuid>, PortError> {
        let rows = sqlx::query(
            "SELECT run_id::uuid AS run_id FROM run_terminals \
             WHERE run_id = ANY($1) AND occurred_at <= $2",
        )
        .bind(run_ids)
        .bind(cutoff)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(rows.iter().map(|row| row.get("run_id")).collect())
    }

    pub async fn active_jobs_of_type(&self, runner_type: &str) -> Result<Vec<Job>, PortError> {
        let rows = sqlx::query(
            "SELECT j.id::uuid AS id FROM jobs j \
             JOIN runner_types rt ON rt.id = j.runner_type_id \
             WHERE rt.type_key = $1 \
               AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id)",
        )
        .bind(runner_type)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
        self.load_batch(&ids).await
    }

    async fn ids(&self, sql: &str, at: DateTime<Utc>) -> Result<Vec<Uuid>, PortError> {
        let rows = sqlx::query(sql)
            .bind(at)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(rows.iter().map(|row| row.get("id")).collect())
    }
}

#[async_trait]
impl JobReader for PgStore {
    async fn load(&self, id: JobId) -> Result<Option<Job>, PortError> {
        Ok(self.load_batch(&[id.as_uuid()]).await?.pop())
    }

    async fn load_children(&self, id: JobId) -> Result<Vec<Job>, PortError> {
        let rows = sqlx::query("SELECT id::uuid AS id FROM jobs WHERE parent_job_id = $1")
            .bind(id.as_uuid())
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
        self.load_batch(&ids).await
    }

    async fn load_descendants(&self, id: JobId) -> Result<Vec<Job>, PortError> {
        let rows = sqlx::query(
            "WITH RECURSIVE descendants AS ( \
                SELECT id FROM jobs WHERE parent_job_id = $1 \
                UNION ALL \
                SELECT child.id FROM jobs child \
                JOIN descendants d ON child.parent_job_id = d.id) \
             SELECT id::uuid AS id FROM descendants",
        )
        .bind(id.as_uuid())
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
        self.load_batch(&ids).await
    }

    async fn load_active_for_source(
        &self,
        source: &SourceReference,
    ) -> Result<Option<Job>, PortError> {
        let row = sqlx::query(
            "SELECT j.id::uuid AS id FROM jobs j \
             JOIN source_entities se ON se.id = j.source_entity_id \
             JOIN producers p ON p.id = se.producer_id \
             WHERE p.bc_key = $1 AND se.external_id = $2 \
               AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
             ORDER BY j.created_at DESC, j.id DESC LIMIT 1",
        )
        .bind(source.bc().as_str())
        .bind(source.entity_id().as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        match row {
            None => Ok(None),
            Some(row) => Ok(self.load_batch(&[row.get("id")]).await?.pop()),
        }
    }
}

#[async_trait]
impl DueWorkReader for PgStore {
    async fn jobs_awaiting_dispatch(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT j.id::uuid AS id FROM jobs j \
             WHERE {NOT_RESOLVED} AND {NO_ACTIVE_RUN} \
               AND EXISTS (SELECT 1 FROM runner_instances ri \
                   JOIN runner_presence_sessions s ON s.instance_id = ri.id \
                       AND s.disconnected_at IS NULL \
                   WHERE ri.runner_type_id = j.runner_type_id) \
               AND (NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id) \
                    OR EXISTS ({UNCONSUMED_RETRY} AND s.due_at <= $1)) \
             ORDER BY j.created_at, j.id LIMIT 200"
        );
        let ids = self.ids(&sql, at).await?;
        self.load_batch(&ids).await
    }

    async fn jobs_with_outrun_runs(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError> {
        let sql = format!(
            "SELECT j.id::uuid AS id FROM jobs j \
             WHERE {NOT_RESOLVED} AND EXISTS (SELECT 1 FROM runs r \
                JOIN run_starts rs ON rs.run_id = r.id \
                LEFT JOIN run_terminals t ON t.run_id = r.id \
                WHERE r.job_id = j.id AND t.run_id IS NULL AND rs.started_at < $1) \
             LIMIT 200"
        );
        let ids = self.ids(&sql, at).await?;
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
               AND EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id) \
               AND NOT EXISTS ({UNCONSUMED_RETRY}) \
               AND idle.last_activity < $1 \
             LIMIT 200"
        );
        let ids = self.ids(&sql, at).await?;
        self.load_batch(&ids).await
    }
}
