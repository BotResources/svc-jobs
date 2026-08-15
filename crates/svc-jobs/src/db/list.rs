use bc_jobs::domain::job::Job;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

const LIST_SQL: &str = "SELECT j.id::uuid AS id, j.created_at FROM jobs j \
     JOIN job_states st ON st.job_id = j.id \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN producers p ON p.id = j.producer_id \
     LEFT JOIN source_entities se ON se.id = j.source_entity_id \
     LEFT JOIN producers sp ON sp.id = se.producer_id \
     WHERE ($1::text[] IS NULL OR st.status = ANY($1)) \
       AND ($2::text[] IS NULL OR rt.type_key = ANY($2)) \
       AND ($3::text[] IS NULL OR coalesce(p.bc_key, sp.bc_key) = ANY($3)) \
       AND (CASE $4::text \
              WHEN 'ONLY_DELETED' THEN st.is_deleted \
              WHEN 'INCLUDE_DELETED' THEN true \
              ELSE NOT st.is_deleted END) \
       AND ($5::timestamptz IS NULL OR (j.created_at, j.id::uuid) < ($5, $6::uuid)) \
     ORDER BY j.created_at DESC, j.id DESC LIMIT $7";

#[derive(Clone, Default)]
pub struct JobFilter {
    pub statuses: Option<Vec<String>>,
    pub runner_types: Option<Vec<String>>,
    pub producers: Option<Vec<String>>,
    pub deleted: Option<String>,
}

pub struct JobPage {
    pub jobs: Vec<Job>,
    pub has_next_page: bool,
}

pub fn cursor_of(job: &Job) -> String {
    format!(
        "{}|{}",
        job.created_at().timestamp_micros(),
        job.id().as_uuid()
    )
}

fn parse_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (micros, id) = cursor.split_once('|')?;
    let micros: i64 = micros.parse().ok()?;
    Some((
        DateTime::from_timestamp_micros(micros)?,
        Uuid::parse_str(id).ok()?,
    ))
}

impl PgStore {
    pub async fn matching_ids(
        &self,
        filter: &JobFilter,
        first: i64,
        after: Option<&str>,
    ) -> Result<(Vec<Uuid>, bool), PortError> {
        let limit = first.clamp(1, 200);
        let (at, id) = after
            .and_then(parse_cursor)
            .map_or((None, None), |(at, id)| (Some(at), Some(id)));
        let rows = sqlx::query(LIST_SQL)
            .bind(filter.statuses.as_deref())
            .bind(filter.runner_types.as_deref())
            .bind(filter.producers.as_deref())
            .bind(filter.deleted.as_deref())
            .bind(at)
            .bind(id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let more = rows.len() as i64 > limit;
        let ids = rows
            .iter()
            .take(limit as usize)
            .map(|row| row.get::<Uuid, _>("id"))
            .collect();
        Ok((ids, more))
    }

    pub async fn job_page(
        &self,
        filter: &JobFilter,
        first: i64,
        after: Option<&str>,
    ) -> Result<JobPage, PortError> {
        let (ids, has_next_page) = self.matching_ids(filter, first, after).await?;
        let mut jobs = self.load_batch(&ids).await?;
        jobs.sort_by(|left, right| {
            right
                .created_at()
                .cmp(&left.created_at())
                .then(right.id().as_uuid().cmp(&left.id().as_uuid()))
        });
        Ok(JobPage {
            jobs,
            has_next_page,
        })
    }

    pub async fn matches_filter(
        &self,
        job_id: Uuid,
        filter: &JobFilter,
    ) -> Result<bool, PortError> {
        let row = sqlx::query(LIST_SQL_MATCH)
            .bind(filter.statuses.as_deref())
            .bind(filter.runner_types.as_deref())
            .bind(filter.producers.as_deref())
            .bind(filter.deleted.as_deref())
            .bind(job_id)
            .fetch_optional(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(row.is_some())
    }
}

const LIST_SQL_MATCH: &str = "SELECT j.id::uuid AS id FROM jobs j \
     JOIN job_states st ON st.job_id = j.id \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN producers p ON p.id = j.producer_id \
     LEFT JOIN source_entities se ON se.id = j.source_entity_id \
     LEFT JOIN producers sp ON sp.id = se.producer_id \
     WHERE ($1::text[] IS NULL OR st.status = ANY($1)) \
       AND ($2::text[] IS NULL OR rt.type_key = ANY($2)) \
       AND ($3::text[] IS NULL OR coalesce(p.bc_key, sp.bc_key) = ANY($3)) \
       AND (CASE $4::text \
              WHEN 'ONLY_DELETED' THEN st.is_deleted \
              WHEN 'INCLUDE_DELETED' THEN true \
              ELSE NOT st.is_deleted END) \
       AND j.id = $5";
