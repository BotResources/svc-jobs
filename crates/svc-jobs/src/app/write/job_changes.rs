use std::collections::{BTreeSet, HashMap};

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

use super::guards::{
    claim_source, guard_decision_still_holds, guard_descendants_unchanged,
    guard_free_capacity_of_every_type, guard_restarts_still_due,
    guard_runner_types_still_accept_jobs, lock_directory_trees,
};
use super::notices::tell_callers;
use super::premise::{PriorityPremise, guard_priority_premise_still_holds};
use super::rederive;
use super::routing::resolve_runner_type_routes;
use crate::app::{followups, integration};
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
    pub restart: Option<RestartPremise>,
}

#[derive(Debug, Clone, Copy)]
pub struct RestartPremise {
    pub origin_run: Uuid,
    pub tree: JobId,
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
            restart: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.answers.is_empty() && !self.removes_directory
    }

    pub fn restarting(mut self, origin_run: Uuid, tree: JobId) -> Self {
        self.restart = Some(RestartPremise { origin_run, tree });
        self
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
    guard_free_capacity_of_every_type(&mut *tx, &changes).await?;
    lock_directory_trees(&mut *tx, &changes).await?;
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
    let routed = resolve_runner_type_routes(&mut *tx, ids, &changes).await?;
    guard_runner_types_still_accept_jobs(&routed)?;
    for change in &changes {
        apply_change(&mut *tx, ids, change, metadata, at, limits).await?;
    }
    for followup in followups::of_settled_jobs(&mut *tx, &changes, &held).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at, limits).await?;
    }
    for followup in rederive::of_jobs_back_to_waiting(&mut *tx, &changes, limits).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at, limits).await?;
    }
    for change in changes.iter().filter(|change| change.removes_directory) {
        apply::directory::remove_job_subtree(tx, change.job_id.as_uuid(), at).await?;
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

async fn apply_change(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    change: &JobChange,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
    limits: &ServiceLimits,
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
    tell_callers(
        tx,
        ids,
        &change.events,
        metadata,
        at,
        limits.inline_payload_max_bytes(),
    )
    .await?;
    Ok(())
}
