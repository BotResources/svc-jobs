use bc_jobs::domain::actions::fleet::guard_accept_job;
use bc_jobs::domain::directory::caller_address::guard_caller_address;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::event::job::JobEvent;

use super::job_changes::JobChange;
use super::routing::RoutedRunnerType;
use crate::app::fingerprint;
use crate::db::fleet::capacity;
use crate::db::{PgStore, apply};
use crate::error::ServiceError;

pub async fn guard_free_capacity(
    tx: &mut sqlx::PgConnection,
    runner_type: &RunnerTypeKey,
) -> Result<(), ServiceError> {
    let Some(capacity) = capacity::lock_and_read(tx, runner_type).await? else {
        return Err(ServiceError::Contended);
    };
    capacity
        .runner_type
        .guard_free_capacity(capacity.current_runs)?;
    Ok(())
}

pub(super) fn guard_runner_types_still_accept_jobs(
    routed: &[RoutedRunnerType<'_>],
) -> Result<(), ServiceError> {
    for runner_type in routed {
        if let Some(lifecycle) = runner_type.lifecycle {
            guard_accept_job(lifecycle, runner_type.key)?;
        }
    }
    Ok(())
}

pub(super) async fn guard_descendants_unchanged(
    tx: &mut sqlx::PgConnection,
    change: &JobChange,
) -> Result<(), ServiceError> {
    let Some(decided_on) = &change.descendants_premise else {
        return Ok(());
    };
    let mut current = PgStore::cancel_reach_in(tx, &[change.job_id.as_uuid()]).await?;
    current.sort_unstable();
    if &current == decided_on {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

pub(super) fn guard_decision_still_holds(
    decided_on: Option<&Job>,
    locked: Option<&Job>,
) -> Result<(), ServiceError> {
    let (Some(decided_on), Some(locked)) = (decided_on, locked) else {
        return Err(ServiceError::Contended);
    };
    if fingerprint::of_job(decided_on) == fingerprint::of_job(locked) {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

pub(super) async fn claim_source(
    tx: &mut sqlx::PgConnection,
    job_id: JobId,
    source: &SourceReference,
) -> Result<(), ServiceError> {
    apply::refs::lock_source_entity(tx, source.bc(), source.entity_id().as_uuid()).await?;
    match PgStore::active_job_for_source_in(tx, source).await? {
        Some(active) if active != job_id.as_uuid() => Err(ServiceError::Domain(
            bc_jobs::JobsError::SourceAlreadyActive {
                active_job_id: active,
            },
        )),
        _ => Ok(()),
    }
}

pub(super) async fn guard_free_capacity_of_every_type(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<(), ServiceError> {
    let mut types: Vec<&RunnerTypeKey> = changes
        .iter()
        .filter_map(|change| change.free_capacity_of.as_ref())
        .collect();
    types.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    types.dedup();
    for runner_type in types {
        guard_free_capacity(&mut *tx, runner_type).await?;
    }
    Ok(())
}

fn ends_a_run(event: &JobEvent) -> bool {
    matches!(
        event,
        JobEvent::RunCompleted(_) | JobEvent::RunFailed(_) | JobEvent::RunCancelled(_)
    )
}

pub(super) async fn lock_directory_trees(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<(), ServiceError> {
    let mut trees: Vec<JobId> = Vec::new();
    for change in changes {
        if let Some(restart) = &change.restart {
            trees.push(restart.tree);
        }
        if let Some(premise) = &change.caller_address {
            trees.extend(crate::db::directory::tree_of_run(tx, premise.caller).await?);
        }
        if !change.removes_directory && !change.events.iter().any(ends_a_run) {
            continue;
        }
        let tree = match &change.before {
            Some(job) => Some(job.initial_job_id()),
            None => PgStore::initial_job_of(tx, change.job_id).await?,
        };
        trees.extend(tree);
    }
    trees.sort_by_key(|tree| tree.as_uuid());
    trees.dedup();
    for tree in trees {
        crate::db::directory::lock_tree(tx, tree).await?;
    }
    Ok(())
}

pub(super) async fn guard_restarts_still_due(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<(), ServiceError> {
    for restart in changes.iter().filter_map(|change| change.restart.as_ref()) {
        if !crate::db::restart::restart_still_due(tx, restart.origin_run).await? {
            return Err(ServiceError::RestartNotDue);
        }
        crate::db::restart::consume_wake(tx, restart.origin_run).await?;
    }
    Ok(())
}

pub(super) async fn guard_caller_addresses_still_held(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<(), ServiceError> {
    for premise in changes.iter().filter_map(|change| change.caller_address) {
        let holder = match premise.address {
            Some(address) => crate::db::directory::address_holder_in(tx, address).await?,
            None => None,
        };
        guard_caller_address(premise.mode, premise.address, premise.caller, holder)?;
    }
    Ok(())
}
