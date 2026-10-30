use async_trait::async_trait;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::JobReader;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, Row, Transaction};
use uuid::Uuid;

use super::PgStore;
use super::dispatch_order::PUSHED_TYPES;
use super::hydrate::{load_many, unavailable};

pub(crate) const NO_ACTIVE_RUN: &str = "NOT EXISTS (SELECT 1 FROM runs r \
     LEFT JOIN run_terminals t ON t.run_id = r.id \
     WHERE r.job_id = j.id AND t.run_id IS NULL)";

pub(crate) const NOT_RESOLVED: &str = "NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
     AND NOT EXISTS (SELECT 1 FROM job_deletions jd WHERE jd.job_id = j.id)";

const ACTIVE_FOR_SOURCE_SQL: &str = "SELECT j.id::uuid AS id FROM jobs j \
     JOIN source_entities se ON se.id = j.source_entity_id \
     JOIN producers p ON p.id = se.producer_id \
     WHERE p.bc_key = $1 AND se.external_id = $2 \
       AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
     ORDER BY j.created_at DESC, j.id DESC LIMIT 1";

pub(crate) const UNCONSUMED_RETRY: &str = "SELECT s.due_at FROM run_retry_schedules s \
     JOIN runs fr ON fr.id = s.failed_run_id \
     LEFT JOIN runs dr ON dr.automatic_retry_schedule_id = s.id \
        AND dr.withdrawn_reason IS NULL \
     WHERE fr.job_id = j.id AND dr.id IS NULL";

impl PgStore {
    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>, PortError> {
        self.pool().begin().await.map_err(unavailable)
    }

    pub async fn lock_job_row(
        tx: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Option<Uuid>, PortError> {
        sqlx::query_scalar("SELECT id::uuid FROM jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)
    }

    pub async fn child_ids(tx: &mut PgConnection, job_id: JobId) -> Result<Vec<Uuid>, PortError> {
        let rows = sqlx::query("SELECT id::uuid AS id FROM jobs WHERE parent_job_id = $1")
            .bind(job_id.as_uuid())
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        Ok(rows.iter().map(|row| row.get("id")).collect())
    }

    pub async fn active_job_for_source_in(
        tx: &mut PgConnection,
        source: &SourceReference,
    ) -> Result<Option<Uuid>, PortError> {
        sqlx::query_scalar(ACTIVE_FOR_SOURCE_SQL)
            .bind(source.bc().as_str())
            .bind(source.entity_id().as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)
    }

    pub async fn load_in(tx: &mut PgConnection, job_id: JobId) -> Result<Option<Job>, PortError> {
        Ok(load_many(&mut *tx, &[job_id.as_uuid()]).await?.pop())
    }

    pub async fn load_batch(&self, ids: &[Uuid]) -> Result<Vec<Job>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load_many(&mut connection, ids).await
    }

    pub async fn next_retry_due_at(
        &self,
        types: &[String],
    ) -> Result<Option<DateTime<Utc>>, PortError> {
        let sql = format!(
            "SELECT min(w.due_at) AS due_at FROM waiting_jobs w \
             WHERE w.due_at IS NOT NULL AND w.runner_type_id IN ({PUSHED_TYPES}) \
               AND w.runner_type_id IN (SELECT id FROM runner_types WHERE type_key = ANY($1))"
        );
        let row = sqlx::query(&sql)
            .bind(types)
            .fetch_one(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(row.get("due_at"))
    }

    pub async fn initial_job_of(
        tx: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Option<JobId>, PortError> {
        let sql = format!(
            "SELECT ({})::uuid FROM jobs j WHERE j.id = $1",
            super::lineage::root_of("j")
        );
        let found: Option<Uuid> = sqlx::query_scalar(&sql)
            .bind(job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        found.map(JobId::new).transpose().map_err(PortError::from)
    }

    pub async fn current_runs_by_type(
        &self,
    ) -> Result<std::collections::HashMap<String, u32>, PortError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT rt.type_key, count(*) FROM current_runs c \
             JOIN runner_types rt ON rt.id = c.runner_type_id GROUP BY rt.type_key",
        )
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|(key, count)| (key, u32::try_from(count).unwrap_or(u32::MAX)))
            .collect())
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

    pub async fn jobs_requested_by_run(&self, run_id: RunId) -> Result<Vec<JobId>, PortError> {
        let rows: Vec<Uuid> =
            sqlx::query_scalar("SELECT id::uuid FROM jobs WHERE requester ->> 'Run' = $1")
                .bind(run_id.as_uuid().to_string())
                .fetch_all(self.pool())
                .await
                .map_err(unavailable)?;
        rows.into_iter()
            .map(|id| JobId::new(id).map_err(PortError::from))
            .collect()
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
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT j.id::uuid AS id FROM jobs j \
             JOIN runner_types rt ON rt.id = j.runner_type_id \
             WHERE rt.type_key = $1 \
               AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id)",
        )
        .bind(runner_type)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        self.load_batch(&ids).await
    }

    pub(super) async fn ids(&self, sql: &str, at: DateTime<Utc>) -> Result<Vec<Uuid>, PortError> {
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
        let found: Option<Uuid> = sqlx::query_scalar(ACTIVE_FOR_SOURCE_SQL)
            .bind(source.bc().as_str())
            .bind(source.entity_id().as_uuid())
            .fetch_optional(self.pool())
            .await
            .map_err(unavailable)?;
        match found {
            None => Ok(None),
            Some(id) => Ok(self.load_batch(&[id]).await?.pop()),
        }
    }
}
