use br_core_integration::{Aggregate, Bc, CommandCoords, CoordError, EventCoords, PastFact, Verb};

fn command(aggregate: &str, verb: &str, version: u8) -> Result<CommandCoords, CoordError> {
    Ok(CommandCoords {
        receiver: Bc::new("jobs")?,
        aggregate: Aggregate::new(aggregate)?,
        verb: Verb::new(verb)?,
        version,
    })
}

fn event(aggregate: &str, fact: &str, version: u8) -> Result<EventCoords, CoordError> {
    Ok(EventCoords {
        producer: Bc::new("jobs")?,
        aggregate: Aggregate::new(aggregate)?,
        fact: PastFact::new(fact)?,
        version,
    })
}

pub fn cmd_job_cancel_v1_coords() -> Result<CommandCoords, CoordError> {
    command("job", "cancel", 1)
}

pub fn cmd_job_cancel_v2_coords() -> Result<CommandCoords, CoordError> {
    command("job", "cancel", 2)
}

pub fn cmd_job_cancel_v3_coords() -> Result<CommandCoords, CoordError> {
    command("job", "cancel", 3)
}

pub fn cmd_job_create_v1_coords() -> Result<CommandCoords, CoordError> {
    command("job", "create", 1)
}

pub fn cmd_job_create_v2_coords() -> Result<CommandCoords, CoordError> {
    command("job", "create", 2)
}

pub fn cmd_job_create_v3_coords() -> Result<CommandCoords, CoordError> {
    command("job", "create", 3)
}

pub fn cmd_job_fail_v1_coords() -> Result<CommandCoords, CoordError> {
    command("job", "fail", 1)
}

pub fn cmd_job_fail_v2_coords() -> Result<CommandCoords, CoordError> {
    command("job", "fail", 2)
}

pub fn cmd_job_fail_v3_coords() -> Result<CommandCoords, CoordError> {
    command("job", "fail", 3)
}

pub fn cmd_job_finish_v1_coords() -> Result<CommandCoords, CoordError> {
    command("job", "finish", 1)
}

pub fn cmd_job_finish_v2_coords() -> Result<CommandCoords, CoordError> {
    command("job", "finish", 2)
}

pub fn cmd_job_finish_v3_coords() -> Result<CommandCoords, CoordError> {
    command("job", "finish", 3)
}

pub fn evt_job_cancel_accepted_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "cancel_accepted", 1)
}

pub fn evt_job_cancelled_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "cancelled", 1)
}

pub fn evt_job_cancelled_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "cancelled", 2)
}

pub fn evt_job_command_rejected_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "command_rejected", 1)
}

pub fn evt_job_completed_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "completed", 1)
}

pub fn evt_job_completed_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "completed", 2)
}

pub fn evt_job_creation_rejected_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "creation_rejected", 1)
}

pub fn evt_job_creation_rejected_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "creation_rejected", 2)
}

pub fn evt_job_failed_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "failed", 1)
}

pub fn evt_job_failed_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "failed", 2)
}

pub fn evt_run_item_arrived_v1_coords() -> Result<EventCoords, CoordError> {
    event("run", "item_arrived", 1)
}

pub fn evt_run_item_arrived_v2_coords() -> Result<EventCoords, CoordError> {
    event("run", "item_arrived", 2)
}

pub fn evt_job_plan_declared_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "plan_declared", 1)
}

pub fn evt_job_plan_declared_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "plan_declared", 2)
}

pub fn evt_job_queued_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "queued", 1)
}

pub fn evt_job_queued_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "queued", 2)
}

pub fn evt_job_started_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "started", 1)
}

pub fn evt_job_started_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "started", 2)
}

pub fn evt_job_step_started_v1_coords() -> Result<EventCoords, CoordError> {
    event("job", "step_started", 1)
}

pub fn evt_job_step_started_v2_coords() -> Result<EventCoords, CoordError> {
    event("job", "step_started", 2)
}

#[cfg(test)]
mod tests;
