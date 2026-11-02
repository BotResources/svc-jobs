mod content;
mod events;
mod orphans;
mod tree;

use async_trait::async_trait;
use bc_jobs::domain::ids::{EventId, JobId};
use bc_jobs::domain::job::scrub::events_of_a_scrub;
use bc_jobs::ports::PortError;
use bc_jobs::ports::scrub::{DueTree, OrphanUploads, ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::{DateTime, Utc};

use super::PgStore;
use super::apply::apply_job_events;
use super::hydrate::{load_many, unavailable};
use super::tree_lock::lock_tree;

#[async_trait]
impl TreeScrubs for PgStore {
    async fn trees_due_for_scrub(
        &self,
        due_before: DateTime<Utc>,
        batch: usize,
        after: Option<DueTree>,
    ) -> Result<Vec<DueTree>, PortError> {
        tree::due_trees(self.pool(), due_before, batch, after).await
    }

    async fn scrub_tree(
        &self,
        initial_job_id: JobId,
        scrub: TreeScrub<'_>,
    ) -> Result<ScrubOutcome, PortError> {
        let at = scrub.at;
        let mut tx = self.begin().await?;
        lock_tree(&mut tx, initial_job_id).await?;
        let jobs = tree::job_ids_of_tree(&mut tx, initial_job_id).await?;
        let facts = tree::facts_of_tree(&mut tx, initial_job_id, &jobs).await?;
        let standing = scrub.retention.scrub_standing(&facts, at);
        if !standing.is_due() {
            return Ok(ScrubOutcome::Standing(standing));
        }
        let runs = tree::run_ids_of(&mut tx, &jobs).await?;
        let loaded = load_many(&mut tx, &jobs).await?;
        let events = events_of_a_scrub(&loaded, at);

        content::erase_request_data(&mut tx, &jobs, at).await?;
        content::erase_results(&mut tx, &jobs, &runs).await?;
        content::erase_mail_content(&mut tx, initial_job_id, at).await?;
        content::erase_items(&mut tx, initial_job_id, &runs).await?;
        content::erase_saved_state(&mut tx, &runs).await?;
        content::erase_run_records(&mut tx, &runs).await?;
        events::rewrite_domain_events(&mut tx, &jobs).await?;
        events::rewrite_published_outbox(&mut tx, &jobs).await?;
        sqlx::query("INSERT INTO tree_scrubs (initial_job_id, scrubbed_at) VALUES ($1, $2)")
            .bind(initial_job_id.as_uuid())
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        for event in events {
            let event_id = EventId::new(scrub.ids.next())?;
            let job_id = event.job_id();
            apply_job_events(&mut tx, job_id, &[(event_id, event)], scrub.metadata, at).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(ScrubOutcome::Scrubbed { jobs: jobs.len() })
    }

    async fn erase_orphan_uploads(
        &self,
        uploaded_before: DateTime<Utc>,
        batch: usize,
    ) -> Result<OrphanUploads, PortError> {
        orphans::erase_orphan_uploads(self.pool(), uploaded_before, batch).await
    }
}
