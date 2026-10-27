use std::collections::HashSet;
use std::sync::Arc;

use async_graphql::Result;
use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::JobId;
use bc_jobs::event::job::JobEvent;
use br_util_graphql::Affordance;
use futures::Stream;
use uuid::Uuid;

use crate::edge::error::edge_error;
use crate::edge::project;
use crate::edge::state::EdgeState;
use crate::edge::tree::JobTree;
use crate::edge::types::event::of_domain;
use crate::edge::types::job::GqlJobDetail;
use crate::edge::types::stream::{GqlJobDelta, GqlJobSnapshot, GqlJobStreamMessage};
use crate::stream::Fact;

use super::{FACT_BATCH, RESET_ABOVE, cursor, next_fact_batch};

pub fn job_stream(
    state: EdgeState,
    job_id: JobId,
    principal: PrincipalFacts,
) -> impl Stream<Item = GqlJobStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        let mut current = match projection(&state, job_id, &principal).await {
            Ok(Some(projected)) => {
                yield snapshot_of(&projected);
                projected
            }
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(error = ?error, "the job snapshot could not be built");
                return;
            }
        };
        while let Some(batch) = next_fact_batch(&mut facts, FACT_BATCH).await {
            let mut descendants = current.tree.descendant_ids();
            let mut concerning = Vec::new();
            for fact in batch {
                let Fact::Job { event_id, occurred_at, job_id: carried, event } = fact else {
                    continue;
                };
                if !concerns_the_tree(&event, carried, job_id, &descendants) {
                    continue;
                }
                descendants.insert(carried.as_uuid());
                if let Some(event) = of_domain(event_id, occurred_at, &event) {
                    concerning.push((carried, event_id, event));
                }
            }
            if concerning.is_empty() {
                continue;
            }
            let refreshed = if concerning.len() > RESET_ABOVE {
                projection(&state, job_id, &principal).await.map(|found| found.map(Refresh::Reset))
            } else {
                touched(&state, job_id, &principal, &current, &concerning)
                    .await
                    .map(|found| found.map(Refresh::Deltas))
            };
            match refreshed {
                Ok(Some(Refresh::Reset(projected))) => {
                    yield snapshot_of(&projected);
                    current = projected;
                }
                Ok(Some(Refresh::Deltas(projected))) => {
                    for (_, event_id, event) in concerning {
                        yield GqlJobStreamMessage::Delta(GqlJobDelta {
                            cursor: cursor(event_id),
                            event,
                            job: GqlJobDetail {
                                job: Arc::clone(&projected.job),
                                tree: Arc::clone(&projected.tree),
                            },
                            affordances: projected.affordances.clone(),
                        });
                    }
                    current = projected;
                }
                Ok(None) => return,
                Err(error) => tracing::warn!(error = ?error, "job deltas could not be built"),
            }
        }
    }
}

enum Refresh {
    Reset(Projection),
    Deltas(Projection),
}

fn snapshot_of(projected: &Projection) -> GqlJobStreamMessage {
    GqlJobStreamMessage::Snapshot(GqlJobSnapshot {
        cursor: cursor(Uuid::now_v7()),
        job: GqlJobDetail {
            job: Arc::clone(&projected.job),
            tree: Arc::clone(&projected.tree),
        },
        affordances: projected.affordances.clone(),
    })
}

/// The projection after a batch, reloading only the Jobs the batch touched:
/// the root (and its affordances) when it is one of them, each touched
/// descendant in place in the tree. `None` when the root is gone.
async fn touched(
    state: &EdgeState,
    root: JobId,
    principal: &PrincipalFacts,
    current: &Projection,
    concerning: &[(JobId, Uuid, crate::edge::types::event::GqlJobEvent)],
) -> Result<Option<Projection>> {
    let mut ids: Vec<Uuid> = concerning.iter().map(|(job, _, _)| job.as_uuid()).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut loaded = state.store.load_batch(&ids).await.map_err(edge_error)?;
    let reloaded_root = loaded
        .iter()
        .position(|job| job.id() == root)
        .map(|at| loaded.remove(at));
    let (job, affordances) = match reloaded_root {
        Some(job) => {
            let parent = project::parent_of(&state.store, &job).await?;
            let affordances = project::affordances_of(&job, parent.as_deref(), principal)?;
            (Arc::new(job), affordances)
        }
        None if ids.contains(&root.as_uuid()) => return Ok(None),
        None => (Arc::clone(&current.job), current.affordances.clone()),
    };
    Ok(Some(Projection {
        job,
        tree: Arc::new(current.tree.with_replaced(loaded)),
        affordances,
    }))
}

fn concerns_the_tree(
    event: &JobEvent,
    carried: JobId,
    root: JobId,
    descendants: &HashSet<Uuid>,
) -> bool {
    carried == root
        || descendants.contains(&carried.as_uuid())
        || joins_the_tree(event, root, descendants)
}

fn joins_the_tree(event: &JobEvent, root: JobId, descendants: &HashSet<Uuid>) -> bool {
    let JobEvent::JobQueued(queued) = event else {
        return false;
    };
    queued
        .parent_job_id
        .is_some_and(|parent| parent == root || descendants.contains(&parent.as_uuid()))
}

struct Projection {
    job: Arc<bc_jobs::domain::job::Job>,
    tree: Arc<JobTree>,
    affordances: Vec<Affordance>,
}

async fn projection(
    state: &EdgeState,
    job_id: JobId,
    principal: &PrincipalFacts,
) -> Result<Option<Projection>> {
    let Some(job) = state.jobs.load(job_id).await.map_err(edge_error)? else {
        return Ok(None);
    };
    let parent = project::parent_of(&state.store, &job).await?;
    let affordances = project::affordances_of(&job, parent.as_deref(), principal)?;
    let tree = JobTree::rooted_at(&state.store, job_id)
        .await
        .map_err(edge_error)?;
    Ok(Some(Projection {
        job: Arc::new(job),
        tree: Arc::new(tree),
        affordances,
    }))
}
