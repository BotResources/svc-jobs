use bc_jobs::commands::job::run_start_request::StartRequestFact;
use bc_jobs::domain::ids::{RunId, StartId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::domain::run::start_refusal::StartRefusal;
use bc_jobs::ports::transport::AnnouncedInstance;
use contract_jobs::runner_surface as wire;

use super::write::JobChange;
use super::{Jobs, catch_up, retry, service_metadata};
use crate::error::ServiceError;

const ATTEMPTS: usize = 5;

struct Parsed {
    run_id: RunId,
    start_id: StartId,
    instance: RunnerInstanceReference,
}

pub async fn started(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::StartedRequest,
) -> Option<wire::SurfaceReply> {
    let Some(parsed) = parse(runner_type, &request) else {
        return Some(refused(StartRefusal::Malformed));
    };
    if let Err(error) = catch_up::apply_acknowledged_start(jobs, parsed.run_id).await {
        tracing::warn!(error = %error, "the acknowledged start of a run could not be applied");
        return None;
    }
    match hold(jobs, &parsed).await {
        Ok(Ok(())) => Some(wire::SurfaceReply::accepted()),
        Ok(Err(refusal)) => Some(refused(refusal)),
        Err(error) => {
            tracing::warn!(error = %error, "a started request could not be settled");
            None
        }
    }
}

fn parse(runner_type: &str, request: &wire::StartedRequest) -> Option<Parsed> {
    if request.version != wire::SURFACE_WIRE_VERSION {
        return None;
    }
    Some(Parsed {
        run_id: RunId::new(request.run_id).ok()?,
        start_id: StartId::new(request.start_id).ok()?,
        instance: RunnerInstanceReference::new(
            RunnerTypeKey::new(runner_type).ok()?,
            InstanceKey::new(&request.instance_key).ok()?,
        ),
    })
}

async fn hold(jobs: &Jobs, parsed: &Parsed) -> Result<Result<(), StartRefusal>, ServiceError> {
    let instance_present = jobs
        .transport
        .announces(&AnnouncedInstance {
            runner_type: parsed.instance.runner_type().clone(),
            instance_key: parsed.instance.instance_key().clone(),
        })
        .await?;
    retry::on_write_race(ATTEMPTS, || hold_once(jobs, parsed, instance_present)).await
}

async fn hold_once(
    jobs: &Jobs,
    parsed: &Parsed,
    instance_present: bool,
) -> Result<Result<(), StartRefusal>, ServiceError> {
    let Some(job_id) = jobs.store.job_of_run(parsed.run_id.as_uuid()).await? else {
        return Ok(Err(StartRefusal::RunUnknown));
    };
    let Some(job) = jobs.load(job_id).await? else {
        return Ok(Err(StartRefusal::RunUnknown));
    };
    let decision = job.record_run_started_request(StartRequestFact {
        run_id: parsed.run_id,
        instance: parsed.instance.clone(),
        start_id: parsed.start_id,
        instance_present,
    });
    let result = match decision {
        Ok(result) => result,
        Err(refusal) => return Ok(Err(refusal)),
    };
    if result.events.is_empty() {
        return Ok(Ok(()));
    }
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events)],
        &service_metadata(),
    )
    .await?;
    Ok(Ok(()))
}

fn refused(refusal: StartRefusal) -> wire::SurfaceReply {
    wire::SurfaceReply::refused(reason(refusal), refusal.message())
}

fn reason(refusal: StartRefusal) -> wire::SurfaceReason {
    match refusal {
        StartRefusal::RunUnknown => wire::SurfaceReason::RunUnknown,
        StartRefusal::InstanceUnknown => wire::SurfaceReason::InstanceUnknown,
        StartRefusal::RunHeld => wire::SurfaceReason::RunHeld,
        StartRefusal::RunReservedElsewhere => wire::SurfaceReason::RunReservedElsewhere,
        StartRefusal::RunNotPending => wire::SurfaceReason::RunNotPending,
        StartRefusal::Malformed => wire::SurfaceReason::Malformed,
    }
}
