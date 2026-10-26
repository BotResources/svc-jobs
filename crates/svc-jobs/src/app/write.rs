use std::collections::{BTreeMap, BTreeSet, HashMap};

use bc_jobs::domain::actions::fleet::{RetirementWindow, guard_accept_job};
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::ids::{EventId, JobId, KnownUserId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::priority::{Priority, PriorityBasis, UserMaxPriority};
use bc_jobs::domain::references::SourceReference;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::event::job::JobEvent;
use bc_jobs::event::user::UserEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::{fingerprint, followups, integration};
use crate::db::fleet::load;
use crate::db::{PgStore, apply, hydrate};
use crate::error::ServiceError;

pub struct JobChange {
    pub job_id: JobId,
    pub before: Option<Job>,
    pub events: Vec<JobEvent>,
    pub note: Option<String>,
    pub claimed_source: Option<SourceReference>,
    pub priority_premise: Option<PriorityPremise>,
}

/// The fact an admission derived its effective priority from. It is read
/// again under lock before the admission commits: a maximum changed or a
/// parent forced in between makes the decision stale, never silently wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityPremise {
    UserMaximum {
        user_id: KnownUserId,
        maximum: Option<Priority>,
    },
    ParentEffective {
        parent_job_id: JobId,
        effective: Priority,
    },
}

impl PriorityPremise {
    /// The premise of an admission on `basis`: the parent it inherited from,
    /// or the stored maximum of the user it runs for. A Job that runs for
    /// nobody is capped by the service default alone, which nothing changes
    /// at run time.
    pub fn of(
        basis: PriorityBasis,
        parent: Option<&Job>,
        user: Option<&UserMaxPriority>,
    ) -> Option<Self> {
        match (basis, parent) {
            (PriorityBasis::InheritedFromParent { parent_effective }, Some(parent)) => {
                Some(Self::ParentEffective {
                    parent_job_id: parent.id(),
                    effective: parent_effective,
                })
            }
            _ => user.map(|user| Self::UserMaximum {
                user_id: user.user_id(),
                maximum: user.maximum(),
            }),
        }
    }
}

impl JobChange {
    pub fn new(job_id: JobId, before: Option<Job>, events: Vec<JobEvent>) -> Self {
        Self {
            job_id,
            before,
            events,
            note: None,
            claimed_source: None,
            priority_premise: None,
        }
    }

    pub fn on_priority_premise(mut self, premise: Option<PriorityPremise>) -> Self {
        self.priority_premise = premise;
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

pub async fn commit_job_changes(
    store: &PgStore,
    ids: &dyn IdFactory,
    changes: Vec<JobChange>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if changes.iter().all(|change| change.events.is_empty()) {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    commit_job_changes_in(&mut tx, ids, changes, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}

/// The same write inside a transaction the caller owns and commits — a claim
/// that locked its Job with the selection, a change of a user's maximum that
/// locked the user first.
pub async fn commit_job_changes_in(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    changes: Vec<JobChange>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let changes: Vec<JobChange> = changes
        .into_iter()
        .filter(|change| !change.events.is_empty())
        .collect();
    if changes.is_empty() {
        return Ok(());
    }
    let held = followups::rows_to_lock(&mut *tx, &changes).await?;
    let locked = lock_in_ascending_order(&mut *tx, held.ids()).await?;
    for change in changes.iter().filter(|change| change.before.is_some()) {
        guard_decision_still_holds(change.before.as_ref(), locked.get(&change.job_id.as_uuid()))?;
    }
    for change in &changes {
        guard_priority_premise_still_holds(&mut *tx, change.priority_premise).await?;
    }
    let routed = resolve_runner_type_routes(&mut *tx, ids, &changes).await?;
    guard_runner_types_still_accept_jobs(&routed)?;
    for change in &changes {
        apply_change(&mut *tx, ids, change, metadata, at).await?;
    }
    for followup in followups::of_settled_jobs(&mut *tx, &changes, &held).await? {
        apply_change(&mut *tx, ids, &followup, metadata, at).await?;
    }
    Ok(())
}

async fn guard_priority_premise_still_holds(
    tx: &mut sqlx::PgConnection,
    premise: Option<PriorityPremise>,
) -> Result<(), ServiceError> {
    let holds = match premise {
        None => true,
        Some(PriorityPremise::UserMaximum { user_id, maximum }) => {
            PgStore::user_max_priority_in(tx, user_id).await? == maximum
        }
        Some(PriorityPremise::ParentEffective {
            parent_job_id,
            effective,
        }) => PgStore::effective_priority_in(tx, parent_job_id).await? == Some(effective),
    };
    if holds {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

/// A change of a user's maximum and the recalculations it carries, in one
/// transaction. The user is locked first: an admission that read the old
/// maximum fails its premise check instead of committing a stale level.
pub struct UserChange {
    pub user_id: KnownUserId,
    /// The maximum stored when the change was decided.
    pub decided_on: Option<Priority>,
    pub events: Vec<UserEvent>,
    pub recalculations: Vec<JobChange>,
}

pub async fn commit_user_change(
    store: &PgStore,
    ids: &dyn IdFactory,
    change: UserChange,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if change.events.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    if PgStore::lock_user_max_priority(&mut tx, change.user_id).await? != change.decided_on {
        return Err(ServiceError::Contended);
    }
    let mut recorded = Vec::with_capacity(change.events.len());
    for event in change.events {
        recorded.push((EventId::new(ids.next())?, event));
    }
    apply::apply_user_events(&mut tx, change.user_id, &recorded, metadata, at).await?;
    commit_job_changes_in(&mut tx, ids, change.recalculations, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}

async fn lock_in_ascending_order(
    tx: &mut sqlx::PgConnection,
    rows: &BTreeSet<Uuid>,
) -> Result<HashMap<Uuid, Job>, ServiceError> {
    let mut held = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(locked) = PgStore::lock_job_row(&mut *tx, JobId::new(*row)?).await? {
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
    apply::apply_job_events(tx, change.job_id, &recorded, metadata, at).await?;
    for (event_id, event) in &recorded {
        if let Some(published) =
            integration::of_event(event, change.before.as_ref(), change.note.as_deref())?
        {
            let record =
                integration::record(published, ids.next(), event_id.as_uuid(), metadata, at)?;
            stage(&mut *tx, &record)
                .await
                .map_err(|error| ServiceError::Infra(error.to_string()))?;
        }
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

pub async fn publish_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    job_id: Uuid,
    reason_code: &str,
    params: serde_json::Value,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let published = integration::rejection(job_id, reason_code, params)?;
    let record = integration::record(published, ids.next(), ids.next(), metadata, at)?;
    let mut tx = store.begin().await?;
    stage(&mut *tx, &record)
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    tx.commit().await?;
    Ok(())
}

pub struct FleetChange<'a> {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: &'a RunnerTypeKey,
    pub decided_on: Option<&'a RunnerType>,
    pub events: &'a [FleetEvent],
}

pub async fn commit_fleet_events(
    store: &PgStore,
    ids: &dyn IdFactory,
    change: FleetChange<'_>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    commit_fleet(store, ids, change, None, metadata, at).await
}

pub async fn commit_fleet(
    store: &PgStore,
    ids: &dyn IdFactory,
    change: FleetChange<'_>,
    revalidate_retirement_within: Option<RetirementWindow>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if change.events.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    let locked =
        PgStore::lock_runner_type(&mut tx, change.runner_type_id, change.runner_type).await?;
    if fingerprint::of_fleet(change.decided_on) != fingerprint::of_fleet(locked.as_ref()) {
        return Err(ServiceError::Contended);
    }
    if let Some(window) = revalidate_retirement_within {
        let locked = locked.as_ref().ok_or(ServiceError::Contended)?;
        let facts =
            PgStore::runner_type_decision_facts_in(&mut tx, change.runner_type, window).await?;
        locked.guard_retire(facts)?;
    }
    let mut recorded = Vec::with_capacity(change.events.len());
    for event in change.events {
        recorded.push((EventId::new(ids.next())?, event.clone()));
    }
    apply::apply_fleet_events(&mut tx, change.runner_type_id, &recorded, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}
