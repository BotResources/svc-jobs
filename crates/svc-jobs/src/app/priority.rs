//! Priority gestures: a super user forcing a Job, or setting and clearing the
//! maximum of a user, and a user reading their own maximum.
//!
//! A gesture that cascades decides its cascade on locked state, inside the
//! transaction that writes it: the Jobs it reaches are locked in id order
//! first, and their descendants read and locked until none is missing — a
//! child admitted meanwhile takes a share lock on its lineage, so it either
//! committed before the lock and is reached, or waits for the gesture and
//! inherits the new level. Each Job is then re-decided on its locked state: one
//! that stopped waiting is skipped, never aborting the gesture.

use std::collections::BTreeSet;

use bc_jobs::JobsError;
use bc_jobs::commands::UserCommandResult;
use bc_jobs::commands::job::priority::ForcePriority;
use bc_jobs::commands::user::{
    ClearMaxPriority, SetMaxPriority, clear_max_priority, set_max_priority,
};
use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::{JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::priority::{Priority, UserMaxPriority};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::policies::priority::{forcing_cascade, recalculate_for_user};
use sqlx::PgConnection;
use uuid::Uuid;

use super::Jobs;
use super::admin::administrator_metadata;
use super::write::{self, JobChange};
use crate::db::PgStore;
use crate::error::ServiceError;

/// A concurrent write moving one of the Jobs a gesture reaches is retried
/// against fresh state this many times before the gesture reports a conflict.
const ATTEMPTS: usize = 5;

pub async fn force(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    job_id: Uuid,
    priority: i64,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(job_id)?;
    let command = ForcePriority {
        priority: Priority::new(priority)?,
        forced_by: actor.clone(),
    };
    let metadata = administrator_metadata(&actor);
    retried(|| async {
        let at = jobs.clock.now();
        let mut tx = jobs.store.begin().await?;
        let tree = lock_trees(&mut tx, &[job_id.as_uuid()]).await?;
        let loaded = PgStore::load_jobs_in(&mut tx, &tree).await?;
        let job = loaded
            .iter()
            .find(|found| found.id() == job_id)
            .cloned()
            .ok_or(ServiceError::JobNotFound)?;
        let result = job.force_priority(command.clone(), principal)?;
        let descendants: Vec<Job> = loaded
            .into_iter()
            .filter(|found| found.id() != job_id)
            .collect();
        let mut changes = vec![JobChange::new(job_id, Some(job.clone()), result.events)];
        for order in forcing_cascade(&job, &descendants) {
            let Some(descendant) = descendants.iter().find(|found| found.id() == order.job_id)
            else {
                continue;
            };
            let cascaded =
                descendant.force_priority_as_descendant(command.clone(), order.originating_job_id);
            changes.push(JobChange::new(
                order.job_id,
                Some(descendant.clone()),
                cascaded.events,
            ));
        }
        write::commit_job_changes_in(&mut tx, context(jobs, &metadata, at), changes).await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}

pub async fn set_max(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    user_id: Uuid,
    max_priority: i64,
) -> Result<(), ServiceError> {
    let max_priority = Priority::new(max_priority)?;
    let set_by = actor.clone();
    change_max(jobs, actor, KnownUserId::new(user_id)?, |current| {
        set_max_priority(
            current,
            SetMaxPriority {
                max_priority,
                set_by: set_by.clone(),
            },
            principal,
        )
    })
    .await
}

pub async fn clear_max(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    user_id: Uuid,
) -> Result<(), ServiceError> {
    let cleared_by = actor.clone();
    change_max(jobs, actor, KnownUserId::new(user_id)?, |current| {
        clear_max_priority(
            current,
            ClearMaxPriority {
                cleared_by: cleared_by.clone(),
            },
            principal,
        )
    })
    .await
}

/// The maximum that applies to a user — their own, or the service default —
/// as the user surface shows it, in its snapshot and its live changes alike.
pub fn applicable(user: &UserMaxPriority, jobs: &Jobs) -> Priority {
    user.applicable(&jobs.limits)
}

pub async fn applicable_max(jobs: &Jobs, user_id: Uuid) -> Result<Priority, ServiceError> {
    let user = jobs
        .store
        .user_max_priority(KnownUserId::new(user_id)?)
        .await?;
    Ok(applicable(&user, jobs))
}

async fn change_max<F>(
    jobs: &Jobs,
    actor: KnownUser,
    user_id: KnownUserId,
    decide: F,
) -> Result<(), ServiceError>
where
    F: Fn(&UserMaxPriority) -> Result<UserCommandResult, JobsError>,
{
    let metadata = administrator_metadata(&actor);
    retried(|| async {
        let at = jobs.clock.now();
        let mut tx = jobs.store.begin().await?;
        let current = UserMaxPriority::new(
            user_id,
            PgStore::lock_user_max_priority(&mut tx, user_id).await?,
        );
        let result = decide(&current)?;
        let Some(resulting) = result.events.last().map(|event| event.resulting_maximum()) else {
            return Ok(());
        };
        let roots = PgStore::waiting_roots_of_in(&mut tx, user_id).await?;
        let tree = lock_trees(&mut tx, &roots).await?;
        let loaded = PgStore::load_jobs_in(&mut tx, &tree).await?;
        let (root_jobs, descendants): (Vec<Job>, Vec<Job>) = loaded
            .into_iter()
            .partition(|job| roots.contains(&job.id().as_uuid()));
        let recalculations: Vec<JobChange> = recalculate_for_user(
            &UserMaxPriority::new(user_id, resulting),
            &root_jobs,
            &descendants,
            &jobs.limits,
        )
        .into_iter()
        .map(|recalculation| {
            let before = root_jobs
                .iter()
                .chain(&descendants)
                .find(|job| job.id() == recalculation.job_id)
                .cloned();
            JobChange::new(recalculation.job_id, before, recalculation.result.events)
        })
        .collect();
        write::record_user_events_in(
            &mut tx,
            jobs.ids.as_ref(),
            user_id,
            result.events,
            &metadata,
            at,
        )
        .await?;
        write::commit_job_changes_in(&mut tx, context(jobs, &metadata, at), recalculations).await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}

/// Locks `roots` and every descendant of theirs, in id order, until no
/// descendant is missing: a child admitted before a parent was locked is read
/// on the next round, and none can be admitted under a locked parent. Returns
/// every Job locked, roots first.
async fn lock_trees(tx: &mut PgConnection, roots: &[Uuid]) -> Result<Vec<Uuid>, ServiceError> {
    let mut locked: BTreeSet<Uuid> = BTreeSet::new();
    let mut order: Vec<Uuid> = PgStore::lock_jobs_in(tx, roots).await?;
    locked.extend(order.iter().copied());
    loop {
        let descendants = PgStore::descendant_ids_in(tx, &order).await?;
        let missing: Vec<Uuid> = descendants
            .into_iter()
            .filter(|id| !locked.contains(id))
            .collect();
        if missing.is_empty() {
            return Ok(order);
        }
        let newly = PgStore::lock_jobs_in(tx, &missing).await?;
        locked.extend(newly.iter().copied());
        order.extend(newly);
    }
}

fn context<'a>(
    jobs: &'a Jobs,
    metadata: &'a br_core_events::EventMetadata,
    at: chrono::DateTime<chrono::Utc>,
) -> write::WriteContext<'a> {
    write::WriteContext {
        ids: jobs.ids.as_ref(),
        metadata,
        at,
        limits: &jobs.limits,
    }
}

async fn retried<F, Fut>(mut attempt: F) -> Result<(), ServiceError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), ServiceError>>,
{
    let mut outcome = Err(ServiceError::Contended);
    for _ in 0..ATTEMPTS {
        outcome = attempt().await;
        if !matches!(outcome, Err(ServiceError::Contended)) {
            return outcome;
        }
    }
    outcome
}
