//! A platform administrator setting a RunnerType's dispatch mode or maximum
//! Run duration.

use bc_jobs::commands::FleetCommandResult;
use bc_jobs::commands::fleet::{set_dispatch_mode, set_max_run_duration};
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::dispatch_mode::DispatchMode;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::fleet::FleetReader;

use super::admin::administrator_metadata;
use super::{Jobs, write};
use crate::error::ServiceError;

pub async fn dispatch_mode(
    jobs: &Jobs,
    actor: KnownUser,
    runner_type: String,
    mode: DispatchMode,
) -> Result<(), ServiceError> {
    configure(jobs, actor, runner_type, |known| {
        set_dispatch_mode(known, mode)
    })
    .await
}

pub async fn max_run_duration(
    jobs: &Jobs,
    actor: KnownUser,
    runner_type: String,
    seconds: i64,
) -> Result<(), ServiceError> {
    let duration = MaxRunDuration::from_seconds(seconds)?;
    configure(jobs, actor, runner_type, |known| {
        set_max_run_duration(known, duration)
    })
    .await
}

async fn configure<F>(
    jobs: &Jobs,
    actor: KnownUser,
    runner_type: String,
    decide: F,
) -> Result<(), ServiceError>
where
    F: Fn(&RunnerType) -> Result<FleetCommandResult, bc_jobs::JobsError>,
{
    let key = RunnerTypeKey::new(runner_type)?;
    let known = FleetReader::load(&jobs.store, &key)
        .await?
        .ok_or(ServiceError::RunnerTypeNotFound)?;
    let result = decide(&known)?;
    write::commit_fleet_events(
        &jobs.store,
        jobs.ids.as_ref(),
        write::FleetChange {
            runner_type_id: known.id(),
            runner_type: &key,
            decided_on: Some(&known),
            events: &result.events,
        },
        &administrator_metadata(&actor),
        jobs.clock.now(),
    )
    .await
}
