use br_core_integration::{Aggregate, Bc, CommandCoords, CoordError, EventCoords, PastFact, Verb};

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
