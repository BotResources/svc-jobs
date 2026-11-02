use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::requester::Requester;
use bc_jobs::event::job_facts::JobQueued;
use contract_jobs::event_v2 as wire;
use uuid::Uuid;

use super::{Lineage, published, requester_ref};
use crate::app::integration::Published;
use crate::error::ServiceError;

pub fn queued_again(job: &Job) -> Result<Published, ServiceError> {
    queued(
        job.id().as_uuid(),
        Lineage::of_job(job),
        job.runner_type().as_str(),
        requester_ref(job.requester(), job.producer().as_str()),
        None,
        job.predecessor_job_id().map(|id: JobId| id.as_uuid()),
    )
}

fn queued(
    job_id: Uuid,
    lineage: Lineage,
    runner_type: &str,
    requester: wire::RequesterRef,
    restart_of_job_id: Option<Uuid>,
    retry_of_job_id: Option<Uuid>,
) -> Result<Published, ServiceError> {
    published(
        contract_jobs::evt_job_queued_v2_coords(),
        contract_jobs::event::EVENT_TYPE_QUEUED,
        wire::EVENT_V2_SCHEMA_VERSION,
        &wire::JobQueuedV2 {
            job_id,
            parent_job_id: lineage.parent_job_id,
            initial_job_id: lineage.initial_job_id,
            runner_type: runner_type.to_owned(),
            requester,
            restart_of_job_id,
            retry_of_job_id,
        },
    )
}

pub(super) fn of_fact(fact: &JobQueued) -> Result<Published, ServiceError> {
    let job_id = fact.job_id.as_uuid();
    let lineage = Lineage {
        parent_job_id: fact.parent_job_id.map_or(job_id, |id: JobId| id.as_uuid()),
        initial_job_id: fact.initial_job_id.map_or(job_id, |id: JobId| id.as_uuid()),
    };
    let requester = fact.requester.unwrap_or(Requester::Service(None));
    queued(
        job_id,
        lineage,
        fact.runner_type.as_str(),
        requester_ref(&requester, fact.producer.as_str()),
        fact.restart_of_job_id.map(|id: JobId| id.as_uuid()),
        fact.predecessor_job_id.map(|id: JobId| id.as_uuid()),
    )
}
