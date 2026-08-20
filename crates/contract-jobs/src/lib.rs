pub mod catalog;
pub mod command;
pub mod event;
#[cfg(feature = "integration")]
mod integration;
pub mod runner;
pub mod runner_transport;
pub mod segment;

#[cfg(feature = "integration")]
pub use integration::*;

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
pub const KV_JOBS_RUNNER_TYPE: &str = "jobs.runner_type.{runner_type}";

#[cfg(test)]
mod tests;
