use br_core_integration::{Aggregate, Bc, CommandCoords, CoordError, EventCoords, PastFact, Verb};

pub const SERVICE_KEY: &str = "jobs";

pub const CMD_JOB_CANCEL_V1: &str = "integration.cmd.jobs.job.cancel.v1";
pub const CMD_JOB_CREATE_V1: &str = "integration.cmd.jobs.job.create.v1";
pub const CMD_JOB_FAIL_V1: &str = "integration.cmd.jobs.job.fail.v1";
pub const CMD_JOB_FINISH_V1: &str = "integration.cmd.jobs.job.finish.v1";
pub const CMD_JOBS_TRIGGER_RUNNER_TYPE: &str = "jobs.trigger.{runner_type}";

pub const EVT_JOB_CANCELLED_V1: &str = "integration.evt.jobs.job.cancelled.v1";
pub const EVT_JOB_COMPLETED_V1: &str = "integration.evt.jobs.job.completed.v1";
pub const EVT_JOB_CREATION_REJECTED_V1: &str = "integration.evt.jobs.job.creation_rejected.v1";
pub const EVT_JOB_FAILED_V1: &str = "integration.evt.jobs.job.failed.v1";
pub const EVT_JOB_PLAN_DECLARED_V1: &str = "integration.evt.jobs.job.plan_declared.v1";
pub const EVT_JOB_QUEUED_V1: &str = "integration.evt.jobs.job.queued.v1";
pub const EVT_JOB_STARTED_V1: &str = "integration.evt.jobs.job.started.v1";
pub const EVT_JOB_STEP_STARTED_V1: &str = "integration.evt.jobs.job.step_started.v1";
pub const EVT_JOBS_LOG_RUNNER_TYPE: &str = "jobs.log.{runner_type}";
pub const EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED: &str = "jobs.status.{runner_type}.completed";
pub const EVT_JOBS_STATUS_RUNNER_TYPE_FAILED: &str = "jobs.status.{runner_type}.failed";
pub const EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED: &str =
    "jobs.status.{runner_type}.plan_declared";
pub const EVT_JOBS_STATUS_RUNNER_TYPE_STARTED: &str = "jobs.status.{runner_type}.started";
pub const EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED: &str = "jobs.status.{runner_type}.step_started";

pub const KV_RUN_ID: &str = "{run_id}";
pub const KV_RUNNER_TYPE_INSTANCE_KEY: &str = "{runner_type}.{instance_key}";

pub fn cmd_job_cancel_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("cancel")?,
        version: 1,
    })
}

pub fn cmd_job_create_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("create")?,
        version: 1,
    })
}

pub fn cmd_job_fail_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("fail")?,
        version: 1,
    })
}

pub fn cmd_job_finish_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("finish")?,
        version: 1,
    })
}

pub fn evt_job_cancelled_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("cancelled")?,
        version: 1,
    })
}

pub fn evt_job_completed_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("completed")?,
        version: 1,
    })
}

pub fn evt_job_creation_rejected_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("creation_rejected")?,
        version: 1,
    })
}

pub fn evt_job_failed_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("failed")?,
        version: 1,
    })
}

pub fn evt_job_plan_declared_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("plan_declared")?,
        version: 1,
    })
}

pub fn evt_job_queued_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("queued")?,
        version: 1,
    })
}

pub fn evt_job_started_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("started")?,
        version: 1,
    })
}

pub fn evt_job_step_started_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("step_started")?,
        version: 1,
    })
}

#[cfg(test)]
mod tests;
