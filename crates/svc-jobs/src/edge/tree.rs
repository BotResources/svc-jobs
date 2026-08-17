use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::job::JobReader;
use uuid::Uuid;

use crate::db::PgStore;
use crate::error::ServiceError;

#[derive(Default)]
pub struct JobTree {
    children: HashMap<Uuid, Vec<Arc<Job>>>,
}

impl JobTree {
    pub async fn rooted_at(store: &PgStore, root: JobId) -> Result<Self, ServiceError> {
        let descendants = JobReader::load_descendants(store, root).await?;
        let mut children: HashMap<Uuid, Vec<Arc<Job>>> = HashMap::new();
        for job in descendants {
            let Some(parent) = job.parent_job_id() else {
                continue;
            };
            children
                .entry(parent.as_uuid())
                .or_default()
                .push(Arc::new(job));
        }
        for siblings in children.values_mut() {
            siblings.sort_by_key(|job| (job.created_at(), job.id()));
        }
        Ok(Self { children })
    }

    pub fn descendant_ids(&self) -> HashSet<Uuid> {
        self.children
            .values()
            .flatten()
            .map(|job| job.id().as_uuid())
            .collect()
    }

    pub fn children_of(&self, job_id: JobId) -> &[Arc<Job>] {
        self.children
            .get(&job_id.as_uuid())
            .map_or(&[], Vec::as_slice)
    }
}
