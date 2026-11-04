//! The Jobs of a source reference: a producer's bounded context and its entity.
//!
//! A source reference names the work a producer asked for an entity of its own.
//! Since 0.8 it is no uniqueness key: any number of Jobs, terminal or not, may
//! carry the same reference, and the administrator reads them all here, newest
//! first, through the `jobs_source_entity_created` index.

use bc_jobs::domain::job::Job;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::list::{JobPage, parse_cursor};

const SOURCE_WINDOW_SQL: &str = "SELECT j.id::uuid AS id FROM jobs j \
     JOIN source_entities se ON se.id = j.source_entity_id \
     JOIN producers p ON p.id = se.producer_id \
     WHERE p.bc_key = $1 AND se.external_id = $2 \
       AND ($3::timestamptz IS NULL OR (j.created_at, j.id::uuid) < ($3, $4::uuid)) \
     ORDER BY j.created_at DESC, j.id DESC LIMIT $5";

const NEWEST_LIVE_OF_SOURCE_SQL: &str = "SELECT j.id::uuid AS id FROM jobs j \
     JOIN source_entities se ON se.id = j.source_entity_id \
     JOIN producers p ON p.id = se.producer_id \
     WHERE p.bc_key = $1 AND se.external_id = $2 \
       AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
     ORDER BY j.created_at DESC, j.id DESC LIMIT 1";

impl PgStore {
    /// One page of every Job of `source`, soft-deleted ones included, newest
    /// first: two statements whatever the page size (the window, then one
    /// batched hydration).
    pub async fn source_page(
        &self,
        source: &SourceReference,
        first: i64,
        after: Option<&str>,
    ) -> Result<JobPage, PortError> {
        let limit = first.clamp(1, 200);
        let (at, id): (Option<DateTime<Utc>>, Option<Uuid>) = after
            .and_then(parse_cursor)
            .map_or((None, None), |(at, id)| (Some(at), Some(id)));
        let rows = sqlx::query(SOURCE_WINDOW_SQL)
            .bind(source.bc().as_str())
            .bind(source.entity_id().as_uuid())
            .bind(at)
            .bind(id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let has_next_page = rows.len() as i64 > limit;
        let ids: Vec<Uuid> = rows
            .iter()
            .take(limit as usize)
            .map(|row| row.get("id"))
            .collect();
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

    /// The newest non-terminal Job of `source`, if any — what the deprecated
    /// `jobsJobBySource` answers with now that a source may carry several.
    pub async fn newest_live_job_of_source(
        &self,
        source: &SourceReference,
    ) -> Result<Option<Job>, PortError> {
        let found: Option<Uuid> = sqlx::query_scalar(NEWEST_LIVE_OF_SOURCE_SQL)
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
