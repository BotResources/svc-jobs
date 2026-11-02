use std::collections::{BTreeSet, HashMap};

use bc_jobs::domain::directory::removal::DirectoryRemoval;
use bc_jobs::domain::ids::{EventId, JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::event::job::JobEvent;
use bc_jobs::event::user::UserEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::change::JobChange;
use super::erasure::erase_what_a_cancel_leaves_behind;
use super::guards::{
    claim_source, guard_caller_addresses_still_held, guard_decision_still_holds,
    guard_descendants_unchanged, guard_free_capacity_of_every_type, guard_restarts_still_due,
    guard_runner_types_still_accept_jobs, lock_directory_trees,
};
use super::notices::tell_callers;
use super::premise::guard_priority_premise_still_holds;
use super::rederive;
use super::routing::resolve_runner_type_routes;
use crate::app::{followups, integration};
use crate::db::removal::removed_with_jobs;
use crate::db::{PgStore, apply, hydrate};
use crate::error::ServiceError;

#[derive(Clone, Copy)]
pub struct WriteContext<'a> {
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
    pub limits: &'a ServiceLimits,
}

pub async fn commit_job_changes(
    store: &PgStore,
    context: WriteContext<'_>,
    changes: Vec<JobChange>,
) -> Result<(), ServiceError> {
    if changes.iter().all(|change| change.is_empty()) {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    commit_job_changes_in(&mut tx, context, changes).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn commit_job_changes_in(
    tx: &mut sqlx::PgConnection,
    context: WriteContext<'_>,
    changes: Vec<JobChange>,
) -> Result<(), ServiceError> {
    let WriteContext {
        ids,
        metadata,
        at,
        limits,
    } = context;
    let changes: Vec<JobChange> = changes
        .into_iter()
        .filter(|change| !change.is_empty())
        .collect();
    if changes.is_empty() {
        return Ok(());
    }
    guard_free_capacity_of_every_type(&mut *tx, &changes).await?;
    lock_directory_trees(&mut *tx, &changes).await?;
    guard_caller_addresses_still_held(&mut *tx, &changes).await?;
    guard_restarts_still_due(&mut *tx, &changes).await?;
    let held = followups::rows_to_lock(&mut *tx, &changes).await?;
    for change in &changes {
        guard_priority_premise_still_holds(&mut *tx, change.priority_premise).await?;
    }
    let cascade_root = changes
        .iter()
        .find(|change| change.descendants_premise.is_some())
        .map(|change| change.job_id.as_uuid());
    let locked = lock_root_first(&mut *tx, cascade_root, held.ids()).await?;
    for change in changes.iter().filter(|change| change.before.is_some()) {
        guard_decision_still_holds(change.before.as_ref(), locked.get(&change.job_id.as_uuid()))?;
    }
    for change in &changes {
        guard_descendants_unchanged(&mut *tx, change).await?;
    }
    let removal = removal_of(&mut *tx, &changes).await?;
    let routed = resolve_runner_type_routes(&mut *tx, ids, &changes).await?;
    guard_runner_types_still_accept_jobs(&routed)?;
    for change in &changes {
        apply_change(&mut *tx, ids, change, metadata, at, limits, &removal).await?;
    }
    for followup in followups::of_settled_jobs(&mut *tx, &changes, &held).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at, limits, &removal).await?;
    }
    for followup in rederive::of_jobs_back_to_waiting(&mut *tx, &changes, limits).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at, limits, &removal).await?;
    }
    let removed_runs: Vec<Uuid> = removal.runs().map(|run| run.as_uuid()).collect();
    if !removed_runs.is_empty() {
        apply::directory::remove_job_subtree(tx, &removed_runs, at).await?;
    }
    Ok(())
}

pub async fn record_user_events_in(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    user_id: KnownUserId,
    events: Vec<UserEvent>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let mut recorded = Vec::with_capacity(events.len());
    for event in events {
        recorded.push((EventId::new(ids.next())?, event));
    }
    apply::apply_user_events(tx, user_id, &recorded, metadata, at).await?;
    Ok(())
}

async fn lock_root_first(
    tx: &mut sqlx::PgConnection,
    root: Option<Uuid>,
    rows: &BTreeSet<Uuid>,
) -> Result<HashMap<Uuid, Job>, ServiceError> {
    let mut held = Vec::with_capacity(rows.len());
    let ordered = root
        .filter(|root| rows.contains(root))
        .into_iter()
        .chain(rows.iter().copied().filter(|row| Some(*row) != root));
    for row in ordered {
        if let Some(locked) = PgStore::lock_job_row(&mut *tx, JobId::new(row)?).await? {
            held.push(locked);
        }
    }
    Ok(hydrate::load_map(tx, &held).await?)
}

async fn removal_of(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<DirectoryRemoval, ServiceError> {
    let roots: Vec<Uuid> = changes
        .iter()
        .filter(|change| change.removes_directory)
        .map(|change| change.job_id.as_uuid())
        .collect();
    Ok(removed_with_jobs(tx, &roots).await?)
}

async fn apply_change(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    change: &JobChange,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
    limits: &ServiceLimits,
    removal: &DirectoryRemoval,
) -> Result<(), ServiceError> {
    if let Some(source) = &change.claimed_source {
        claim_source(tx, change.job_id, source).await?;
    }
    let recorded: Vec<(EventId, JobEvent)> = change
        .events
        .iter()
        .map(|event| Ok((EventId::new(ids.next())?, event.clone())))
        .collect::<Result<_, bc_jobs::JobsError>>()?;
    let metadata = change.metadata.as_ref().unwrap_or(metadata);
    apply::apply_job_events(tx, change.job_id, &recorded, metadata, at).await?;
    if let Some(command_id) = change
        .remembered_command
        .filter(|_| change.resolves_the_job())
    {
        apply::command_memory::remember(tx, change.job_id, command_id).await?;
    }
    for (event_id, event) in &recorded {
        let published =
            integration::of_event(event, change.before.as_ref(), change.note.as_deref())?;
        for (position, published) in published.into_iter().enumerate() {
            let event_id = if position == 0 {
                event_id.as_uuid()
            } else {
                ids.next()
            };
            let record = integration::record(published, ids.next(), event_id, metadata, at)?;
            stage(&mut *tx, &record)
                .await
                .map_err(|error| ServiceError::Infra(error.to_string()))?;
        }
    }
    for answer in &change.answers {
        let record = integration::record(answer.clone(), ids.next(), ids.next(), metadata, at)?;
        stage(&mut *tx, &record)
            .await
            .map_err(|error| ServiceError::Infra(error.to_string()))?;
    }
    tell_callers(
        tx,
        ids,
        &change.events,
        metadata,
        at,
        limits.inline_payload_max_bytes(),
        removal,
    )
    .await?;
    erase_what_a_cancel_leaves_behind(tx, &change.events, limits, at).await
}
