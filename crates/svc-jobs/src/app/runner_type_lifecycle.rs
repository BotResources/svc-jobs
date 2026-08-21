use bc_jobs::commands::FleetCommandResult;
use bc_jobs::commands::fleet::{deprecate, reactivate, retire};
use bc_jobs::domain::actions::fleet::RetirementWindow;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::fleet::FleetReader;

use super::admin::administrator_metadata;
use super::{Jobs, runner_type_catalog, write};
use crate::error::ServiceError;

#[derive(Debug, Clone, Copy)]
pub enum RunnerTypeAction {
    Deprecate,
    Reactivate,
    Retire,
}

pub async fn change(
    jobs: &Jobs,
    actor: KnownUser,
    runner_type: String,
    action: RunnerTypeAction,
) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(runner_type)?;
    let known = FleetReader::load(&jobs.store, &key)
        .await?
        .ok_or(ServiceError::RunnerTypeNotFound)?;
    let (result, revalidate_within) = decide(jobs, &known, action).await?;
    write::commit_fleet(
        &jobs.store,
        jobs.ids.as_ref(),
        write::FleetChange {
            runner_type_id: known.id(),
            runner_type: &key,
            decided_on: Some(&known),
            events: &result.events,
        },
        revalidate_within,
        &administrator_metadata(&actor),
        jobs.clock.now(),
    )
    .await?;
    runner_type_catalog::project_committed(jobs, &key, &result.events).await;
    Ok(())
}

async fn decide(
    jobs: &Jobs,
    known: &RunnerType,
    action: RunnerTypeAction,
) -> Result<(FleetCommandResult, Option<RetirementWindow>), ServiceError> {
    Ok(match action {
        RunnerTypeAction::Deprecate => (deprecate(known)?, None),
        RunnerTypeAction::Reactivate => (reactivate(known)?, None),
        RunnerTypeAction::Retire => {
            let window = jobs.retirement_window();
            let facts = jobs.store.decision_facts(known.key(), window).await?;
            (retire(known, facts)?, Some(window))
        }
    })
}
