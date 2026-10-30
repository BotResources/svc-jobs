use std::collections::{BTreeMap, BTreeSet, HashMap};

use bc_jobs::domain::actions::fleet::guard_accept_job;
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::ids::{EventId, JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::event::job::JobEvent;
use bc_jobs::event::user::UserEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::premise::{PriorityPremise, guard_priority_premise_still_holds};
use super::rederive;
use crate::app::{fingerprint, followups, integration};
use crate::db::fleet::{capacity, load};
use crate::db::{PgStore, apply, hydrate};
use crate::error::ServiceError;

pub struct JobChange {
    pub job_id: JobId,
    pub before: Option<Job>,
    pub events: Vec<JobEvent>,
    pub note: Option<String>,
    pub claimed_source: Option<SourceReference>,
    pub metadata: Option<EventMetadata>,
    pub answers: Vec<integration::Published>,
    pub priority_premise: Option<PriorityPremise>,
    pub descendants_premise: Option<Vec<Uuid>>,
    pub free_capacity_of: Option<RunnerTypeKey>,
    pub removes_directory: bool,
}

impl JobChange {
    pub fn new(job_id: JobId, before: Option<Job>, events: Vec<JobEvent>) -> Self {
        Self {
            job_id,
            before,
            events,
            note: None,
            claimed_source: None,
            metadata: None,
            answers: Vec::new(),
            priority_premise: None,
            descendants_premise: None,
            free_capacity_of: None,
            removes_directory: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.answers.is_empty() && !self.removes_directory
    }

    pub fn removing_directory(mut self) -> Self {
        self.removes_directory = true;
        self
    }

    pub fn requiring_free_capacity(mut self, runner_type: RunnerTypeKey) -> Self {
        self.free_capacity_of = Some(runner_type);
        self
    }

    pub fn over_descendants(mut self, descendants: impl IntoIterator<Item = JobId>) -> Self {
        let mut ids: Vec<Uuid> = descendants.into_iter().map(|id| id.as_uuid()).collect();
        ids.sort_unstable();
        self.descendants_premise = Some(ids);
        self
    }

    pub fn on_priority_premise(mut self, premise: Option<PriorityPremise>) -> Self {
        self.priority_premise = premise;
        self
    }

    pub fn answering(mut self, answer: integration::Published) -> Self {
        self.answers.push(answer);
        self
    }

    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_note(mut self, note: Option<String>) -> Self {
        self.note = note;
        self
    }

    pub fn claiming_source(mut self, source: Option<SourceReference>) -> Self {
        self.claimed_source = source;
        self
    }
}

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
    for change in &changes {
        if let Some(runner_type) = &change.free_capacity_of {
            guard_free_capacity(&mut *tx, runner_type).await?;
        }
    }
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
    let routed = resolve_runner_type_routes(&mut *tx, ids, &changes).await?;
    guard_runner_types_still_accept_jobs(&routed)?;
    for change in &changes {
        apply_change(&mut *tx, ids, change, metadata, at).await?;
    }
    for followup in followups::of_settled_jobs(&mut *tx, &changes, &held).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at).await?;
    }
    for followup in rederive::of_jobs_back_to_waiting(&mut *tx, &changes, limits).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at).await?;
    }
    Ok(())
}

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

async fn apply_change(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    change: &JobChange,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
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
    if change.removes_directory {
        apply::directory::remove_job_subtree(tx, change.job_id.as_uuid(), at).await?;
    }
    Ok(())
}

pub struct RoutedRunnerType<'a> {
    key: &'a RunnerTypeKey,
    lifecycle: Option<RunnerTypeLifecycle>,
}

async fn resolve_runner_type_routes<'a>(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    changes: &'a [JobChange],
) -> Result<Vec<RoutedRunnerType<'a>>, ServiceError> {
    let mut claimed: BTreeMap<&str, &RunnerTypeKey> = BTreeMap::new();
    for change in changes {
        for event in &change.events {
            if let JobEvent::JobQueued(fact) = event {
                claimed.insert(fact.runner_type.as_str(), &fact.runner_type);
            }
        }
    }
    let mut routed = Vec::with_capacity(claimed.len());
    for key in claimed.into_values() {
        let route = load::upsert_route(&mut *tx, ids.next(), key).await?;
        routed.push(RoutedRunnerType {
            key,
            lifecycle: load::registered_lifecycle(&mut *tx, route).await?,
        });
    }
    Ok(routed)
}

fn guard_runner_types_still_accept_jobs(
    routed: &[RoutedRunnerType<'_>],
) -> Result<(), ServiceError> {
    for runner_type in routed {
        if let Some(lifecycle) = runner_type.lifecycle {
            guard_accept_job(lifecycle, runner_type.key)?;
        }
    }
    Ok(())
}

async fn guard_descendants_unchanged(
    tx: &mut sqlx::PgConnection,
    change: &JobChange,
) -> Result<(), ServiceError> {
    let Some(decided_on) = &change.descendants_premise else {
        return Ok(());
    };
    let mut current = PgStore::descendant_ids_in(tx, &[change.job_id.as_uuid()]).await?;
    current.sort_unstable();
    if &current == decided_on {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

fn guard_decision_still_holds(
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

async fn claim_source(
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
