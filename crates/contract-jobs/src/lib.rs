//! Published language for `svc-jobs` — the subjects and prefixes the
//! Services registry declares this service offers.
//!
//! Scaffolded by runkit from major 0, and hand-owned from here on. Every
//! literal below carries an offer's `natural_key` **verbatim**: the platform
//! already agreed on those strings, and a crate that rebuilds them from parts is
//! a crate that can disagree with the registry without anybody noticing. The
//! constant names are derived from the strings; the strings are never derived
//! from the names. That derivation (`integration.cmd.{bc}.task.create.v1` →
//! `CMD_TASK_CREATE_V1`) is runkit's own convention rather than a platform
//! standard — no BR doctrine fixes one — so rename freely, but rename the
//! coordinate constructor with it.
//!
//! **Literals and coordinates, both.** `br-util-nats-fabric` takes coordinates
//! everywhere and a subject string nowhere: `publish_command`, `publish_event`,
//! `run_commands`, `run_events` and `ensure_*_durable` all want a
//! `&CommandCoords` / `&EventCoords`. So each subject appears twice — as the
//! `const` that cannot fail and states what the registry declared, and as the
//! constructor the Fabric can actually be called with. Coordinates are fallible
//! by construction (`Bc::new` and its siblings validate segments and reject `.`
//! and wildcards), which is why they are functions. The test module asserts the
//! two render identically: edit one without the other and this crate goes red
//! rather than quietly addressing a different subject.
//!
//! A KV prefix is a key, not a subject, so it carries no coordinates. A subject
//! that does not parse as the v1 grammar carries none either — it gets its
//! literal alone, and that is worth chasing down in the registry.
//!
//! **No payload types.** The registry describes what an offer carries in prose,
//! so the field lists are not machine-derivable, and a guessed struct is worse
//! than none: consumers compile against it. Write the serde DTOs here, one per
//! subject, from each offer's specification in the Services registry (service
//! `jobs`, major 0). They are the `T` of the canonical envelopes
//! `IntegrationEvent<T>` / `IntegrationCommand<T>` from `br-core-integration`,
//! which already carry `id`, `type`, `version`, `timestamp` and an
//! `EventMetadata { actor, correlation_id, causation_id }` — never redeclare
//! those on a payload. Async **command** contracts are owned by the receiver —
//! this crate — and **event** contracts by the producer, which for the subjects
//! below is also this crate.

use br_core_integration::{Aggregate, Bc, CommandCoords, CoordError, EventCoords, PastFact, Verb};

/// The manifest service key `svc-jobs` declares to the Identity scope
/// registry. Single source of truth: the boot declaration and every in-service
/// scope gate reference this one const, so the declared value cannot drift from
/// the value the gate enforces.
pub const SERVICE_KEY: &str = "jobs";

// Integration commands this service accepts. A command contract is owned by
// its receiver, so the payloads these subjects carry are this crate's to define.
/// `integration.cmd.jobs.job.cancel.v1` — an integration command offered on the bus.
pub const CMD_JOB_CANCEL_V1: &str = "integration.cmd.jobs.job.cancel.v1";
/// `integration.cmd.jobs.job.create.v1` — an integration command offered on the bus.
pub const CMD_JOB_CREATE_V1: &str = "integration.cmd.jobs.job.create.v1";
/// `integration.cmd.jobs.job.fail.v1` — an integration command offered on the bus.
pub const CMD_JOB_FAIL_V1: &str = "integration.cmd.jobs.job.fail.v1";
/// `integration.cmd.jobs.job.finish.v1` — an integration command offered on the bus.
pub const CMD_JOB_FINISH_V1: &str = "integration.cmd.jobs.job.finish.v1";
/// `jobs.trigger.{runner_type}` — an integration command offered on the bus.
pub const CMD_JOBS_TRIGGER_RUNNER_TYPE: &str = "jobs.trigger.{runner_type}";

// Integration events this service publishes. An event contract is owned by its
// producer, so the payloads these subjects carry are this crate's to define.
/// `integration.evt.jobs.job.cancelled.v1` — an integration event offered on the bus.
pub const EVT_JOB_CANCELLED_V1: &str = "integration.evt.jobs.job.cancelled.v1";
/// `integration.evt.jobs.job.completed.v1` — an integration event offered on the bus.
pub const EVT_JOB_COMPLETED_V1: &str = "integration.evt.jobs.job.completed.v1";
/// `integration.evt.jobs.job.creation_rejected.v1` — an integration event offered on the bus.
pub const EVT_JOB_CREATION_REJECTED_V1: &str = "integration.evt.jobs.job.creation_rejected.v1";
/// `integration.evt.jobs.job.failed.v1` — an integration event offered on the bus.
pub const EVT_JOB_FAILED_V1: &str = "integration.evt.jobs.job.failed.v1";
/// `integration.evt.jobs.job.plan_declared.v1` — an integration event offered on the bus.
pub const EVT_JOB_PLAN_DECLARED_V1: &str = "integration.evt.jobs.job.plan_declared.v1";
/// `integration.evt.jobs.job.queued.v1` — an integration event offered on the bus.
pub const EVT_JOB_QUEUED_V1: &str = "integration.evt.jobs.job.queued.v1";
/// `integration.evt.jobs.job.started.v1` — an integration event offered on the bus.
pub const EVT_JOB_STARTED_V1: &str = "integration.evt.jobs.job.started.v1";
/// `integration.evt.jobs.job.step_started.v1` — an integration event offered on the bus.
pub const EVT_JOB_STEP_STARTED_V1: &str = "integration.evt.jobs.job.step_started.v1";
/// `jobs.log.{runner_type}` — an integration event offered on the bus.
pub const EVT_JOBS_LOG_RUNNER_TYPE: &str = "jobs.log.{runner_type}";
/// `jobs.status.{runner_type}.completed` — an integration event offered on the bus.
pub const EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED: &str = "jobs.status.{runner_type}.completed";
/// `jobs.status.{runner_type}.failed` — an integration event offered on the bus.
pub const EVT_JOBS_STATUS_RUNNER_TYPE_FAILED: &str = "jobs.status.{runner_type}.failed";
/// `jobs.status.{runner_type}.plan_declared` — an integration event offered on the bus.
pub const EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED: &str =
    "jobs.status.{runner_type}.plan_declared";
/// `jobs.status.{runner_type}.started` — an integration event offered on the bus.
pub const EVT_JOBS_STATUS_RUNNER_TYPE_STARTED: &str = "jobs.status.{runner_type}.started";
/// `jobs.status.{runner_type}.step_started` — an integration event offered on the bus.
pub const EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED: &str = "jobs.status.{runner_type}.step_started";

// Published-language KV prefixes. Other services read under these keys; the value
// shape stored there is this crate's to define.
/// `{run_id}` — a published-language KV key prefix.
pub const KV_RUN_ID: &str = "{run_id}";
/// `{runner_type}.{instance_key}` — a published-language KV key prefix.
pub const KV_RUNNER_TYPE_INSTANCE_KEY: &str = "{runner_type}.{instance_key}";

/// Coordinates for [`CMD_JOB_CANCEL_V1`] — the form `Fabric::publish_command` and
/// `Fabric::run_commands` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn cmd_job_cancel_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("cancel")?,
        version: 1,
    })
}

/// Coordinates for [`CMD_JOB_CREATE_V1`] — the form `Fabric::publish_command` and
/// `Fabric::run_commands` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn cmd_job_create_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("create")?,
        version: 1,
    })
}

/// Coordinates for [`CMD_JOB_FAIL_V1`] — the form `Fabric::publish_command` and
/// `Fabric::run_commands` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn cmd_job_fail_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("fail")?,
        version: 1,
    })
}

/// Coordinates for [`CMD_JOB_FINISH_V1`] — the form `Fabric::publish_command` and
/// `Fabric::run_commands` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn cmd_job_finish_v1_coords() -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        verb: Verb::new("finish")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_CANCELLED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_cancelled_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("cancelled")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_COMPLETED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_completed_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("completed")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_CREATION_REJECTED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_creation_rejected_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("creation_rejected")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_FAILED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_failed_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("failed")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_PLAN_DECLARED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_plan_declared_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("plan_declared")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_QUEUED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_queued_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("queued")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_STARTED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_started_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("started")?,
        version: 1,
    })
}

/// Coordinates for [`EVT_JOB_STEP_STARTED_V1`] — the form `Fabric::publish_event` and
/// `Fabric::run_events` take. A function rather than a const because the
/// segment newtypes validate, and validation returns `Result`.
pub fn evt_job_step_started_v1_coords() -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new("job")?,
        fact: PastFact::new("step_started")?,
        version: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every subject this crate names, paired with the constant naming it.
    /// Generated alongside the constants: extend it when you add one.
    const SUBJECTS: &[(&str, &str)] = &[
        ("CMD_JOB_CANCEL_V1", CMD_JOB_CANCEL_V1),
        ("CMD_JOB_CREATE_V1", CMD_JOB_CREATE_V1),
        ("CMD_JOB_FAIL_V1", CMD_JOB_FAIL_V1),
        ("CMD_JOB_FINISH_V1", CMD_JOB_FINISH_V1),
        ("EVT_JOB_CANCELLED_V1", EVT_JOB_CANCELLED_V1),
        ("EVT_JOB_COMPLETED_V1", EVT_JOB_COMPLETED_V1),
        ("EVT_JOB_CREATION_REJECTED_V1", EVT_JOB_CREATION_REJECTED_V1),
        ("EVT_JOB_FAILED_V1", EVT_JOB_FAILED_V1),
        ("EVT_JOB_PLAN_DECLARED_V1", EVT_JOB_PLAN_DECLARED_V1),
        ("EVT_JOB_QUEUED_V1", EVT_JOB_QUEUED_V1),
        ("EVT_JOB_STARTED_V1", EVT_JOB_STARTED_V1),
        ("EVT_JOB_STEP_STARTED_V1", EVT_JOB_STEP_STARTED_V1),
        ("EVT_JOBS_LOG_RUNNER_TYPE", EVT_JOBS_LOG_RUNNER_TYPE),
        (
            "EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED",
            EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED,
        ),
        (
            "EVT_JOBS_STATUS_RUNNER_TYPE_FAILED",
            EVT_JOBS_STATUS_RUNNER_TYPE_FAILED,
        ),
        (
            "EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED",
            EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED,
        ),
        (
            "EVT_JOBS_STATUS_RUNNER_TYPE_STARTED",
            EVT_JOBS_STATUS_RUNNER_TYPE_STARTED,
        ),
        (
            "EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED",
            EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED,
        ),
        ("CMD_JOBS_TRIGGER_RUNNER_TYPE", CMD_JOBS_TRIGGER_RUNNER_TYPE),
        ("KV_RUN_ID", KV_RUN_ID),
        ("KV_RUNNER_TYPE_INSTANCE_KEY", KV_RUNNER_TYPE_INSTANCE_KEY),
    ];

    fn rendered_command(coords: &CommandCoords) -> String {
        format!(
            "integration.cmd.{}.{}.{}.v{}",
            coords.receiver.as_str(),
            coords.aggregate.as_str(),
            coords.verb.as_str(),
            coords.version
        )
    }

    fn rendered_event(coords: &EventCoords) -> String {
        format!(
            "integration.evt.{}.{}.{}.v{}",
            coords.producer.as_str(),
            coords.aggregate.as_str(),
            coords.fact.as_str(),
            coords.version
        )
    }

    /// The coordinates and the literal are two renderings of one subject.
    /// This is what stops them drifting apart when either is edited.
    #[test]
    fn every_coordinate_renders_its_declared_subject() {
        assert_eq!(
            rendered_command(&cmd_job_cancel_v1_coords().unwrap()),
            CMD_JOB_CANCEL_V1
        );
        assert_eq!(
            rendered_command(&cmd_job_create_v1_coords().unwrap()),
            CMD_JOB_CREATE_V1
        );
        assert_eq!(
            rendered_command(&cmd_job_fail_v1_coords().unwrap()),
            CMD_JOB_FAIL_V1
        );
        assert_eq!(
            rendered_command(&cmd_job_finish_v1_coords().unwrap()),
            CMD_JOB_FINISH_V1
        );
        assert_eq!(
            rendered_event(&evt_job_cancelled_v1_coords().unwrap()),
            EVT_JOB_CANCELLED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_completed_v1_coords().unwrap()),
            EVT_JOB_COMPLETED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_creation_rejected_v1_coords().unwrap()),
            EVT_JOB_CREATION_REJECTED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_failed_v1_coords().unwrap()),
            EVT_JOB_FAILED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_plan_declared_v1_coords().unwrap()),
            EVT_JOB_PLAN_DECLARED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_queued_v1_coords().unwrap()),
            EVT_JOB_QUEUED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_started_v1_coords().unwrap()),
            EVT_JOB_STARTED_V1
        );
        assert_eq!(
            rendered_event(&evt_job_step_started_v1_coords().unwrap()),
            EVT_JOB_STEP_STARTED_V1
        );
    }

    #[test]
    fn the_service_key_is_the_registered_service_name() {
        assert_eq!(SERVICE_KEY, "jobs");
    }

    #[test]
    fn no_two_constants_name_the_same_subject() {
        for (index, (name, subject)) in SUBJECTS.iter().enumerate() {
            assert!(!subject.is_empty(), "{name} names an empty subject");
            for (other_name, other_subject) in &SUBJECTS[index + 1..] {
                assert_ne!(
                    subject, other_subject,
                    "{name} and {other_name} name the same subject"
                );
            }
        }
    }
}
