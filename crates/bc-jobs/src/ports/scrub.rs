use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use crate::domain::ids::JobId;
use crate::domain::policy::{Retention, ScrubStanding};
use crate::ports::PortError;
use crate::ports::environment::IdFactory;

#[derive(Clone, Copy)]
pub struct TreeScrub<'a> {
    pub retention: Retention,
    pub at: DateTime<Utc>,
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DueTree {
    pub initial_job_id: JobId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrubOutcome {
    Scrubbed { jobs: usize },
    Standing(ScrubStanding),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanUploads {
    pub payloads: u64,
    pub attachments: u64,
}

#[async_trait]
pub trait TreeScrubs: Send + Sync {
    async fn trees_due_for_scrub(
        &self,
        due_before: DateTime<Utc>,
        batch: usize,
        after: Option<DueTree>,
    ) -> Result<Vec<DueTree>, PortError>;

    async fn scrub_tree(
        &self,
        initial_job_id: JobId,
        scrub: TreeScrub<'_>,
    ) -> Result<ScrubOutcome, PortError>;

    async fn erase_orphan_uploads(
        &self,
        uploaded_before: DateTime<Utc>,
        batch: usize,
    ) -> Result<OrphanUploads, PortError>;
}
