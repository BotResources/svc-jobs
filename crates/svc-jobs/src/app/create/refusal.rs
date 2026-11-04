use bc_jobs::JobsError;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::call_notices::CallNotices;
use br_core_events::EventMetadata;
use contract_jobs::command_v3::CreateJobV3;
use contract_jobs::event::{REASON_ID_REUSE, REASON_MALFORMED_PAYLOAD};
use serde_json::{Value, json};

use super::declare;
use crate::app::Jobs;
use crate::app::integration::{Lineage, plain_message};
use crate::app::write::{self, CreationRefusal, RefusalNotice};
use crate::error::ServiceError;

pub async fn reject_malformed(
    jobs: &Jobs,
    job_id: uuid::Uuid,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    write::publish_rejection(
        &jobs.store,
        jobs.ids.as_ref(),
        CreationRefusal {
            job_id,
            lineage: Lineage::declared(job_id, None, None),
            reason_code: REASON_MALFORMED_PAYLOAD,
            params: json!({ "jobId": job_id }),
            notice: None,
        },
        metadata,
        jobs.clock.now(),
    )
    .await?;
    Ok(())
}

pub(super) fn caller_of(requester: &Requester) -> Option<RunId> {
    match requester {
        Requester::Run(run_id) => Some(*run_id),
        Requester::Service(_) => None,
    }
}

pub(super) async fn reject(
    jobs: &Jobs,
    wire: &CreateJobV3,
    refusal: &JobsError,
    metadata: &EventMetadata,
    caller: Option<RunId>,
) -> Result<(), ServiceError> {
    let reason_code = published_code(refusal);
    let woke = write::publish_rejection(
        &jobs.store,
        jobs.ids.as_ref(),
        CreationRefusal {
            job_id: wire.job_id,
            lineage: Lineage::declared(wire.job_id, wire.parent_job_id, wire.initial_job_id),
            reason_code,
            params: rejection_params(wire, refusal),
            notice: refusal_notice(wire, reason_code, metadata, caller),
        },
        metadata,
        jobs.clock.now(),
    )
    .await?;
    if woke {
        jobs.start_due_restarts().await;
    }
    Ok(())
}

fn refusal_notice(
    wire: &CreateJobV3,
    reason_code: &str,
    metadata: &EventMetadata,
    caller: Option<RunId>,
) -> Option<RefusalNotice> {
    let caller_run = caller?;
    let call = wire.mode.map(|mode| declare::call(wire, mode));
    let told = CallNotices::of(&Requester::Run(caller_run), call.as_ref())?
        .refusal_notice(wire.job_id, refusal_words(wire.job_id, reason_code))?;
    Some(RefusalNotice {
        told,
        command: metadata.causation_id,
    })
}

fn refusal_words(job_id: uuid::Uuid, reason_code: &str) -> String {
    format!(
        "Job {job_id} was not created: {}",
        plain_message(reason_code)
    )
}

fn published_code(refusal: &JobsError) -> &'static str {
    match refusal {
        JobsError::JobIdConflict { .. } => REASON_ID_REUSE,
        other => other.code(),
    }
}

fn rejection_params(wire: &CreateJobV3, refusal: &JobsError) -> Value {
    let mut params = refusal.params();
    if let Some(fields) = params.as_object_mut() {
        fields.insert("jobId".to_owned(), json!(wire.job_id));
        if let Some(entity_id) = wire.source_entity_id {
            fields.insert("sourceEntityId".to_owned(), json!(entity_id));
        }
        if let Some(bc) = &wire.source_bc {
            fields.insert("sourceBc".to_owned(), json!(bc));
        }
    }
    params
}
