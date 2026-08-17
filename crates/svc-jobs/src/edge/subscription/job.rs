use std::collections::HashSet;
use std::sync::Arc;

use async_graphql::Result;
use bc_jobs::domain::ids::JobId;
use bc_jobs::event::job::JobEvent;
use br_util_graphql::Affordance;
use chrono::{DateTime, Utc};
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

use super::{cursor, next_unbroken_fact};

pub fn job_stream(
    state: EdgeState,
    job_id: JobId,
) -> impl Stream<Item = GqlJobStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        let mut descendants = match snapshot(&state, job_id).await {
            Ok(Some((message, descendants))) => {
                yield message;
                descendants
            }
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(error = ?error, "the job snapshot could not be built");
                return;
            }
        };
        while let Some(fact) = next_unbroken_fact(&mut facts).await {
            let Fact::Job { event_id, occurred_at, job_id: carried, event } = fact else {
                continue;
            };
            if !concerns_the_tree(&event, carried, job_id, &descendants) {
                continue;
            }
            match delta(&state, job_id, event_id, occurred_at, &event).await {
                Ok(Some((message, refreshed))) => {
                    descendants = refreshed;
                    yield message;
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(error = ?error, "a job delta could not be built"),
            }
        }
    }
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

async fn snapshot(
    state: &EdgeState,
    job_id: JobId,
) -> Result<Option<(GqlJobStreamMessage, HashSet<Uuid>)>> {
    let Some(projected) = projection(state, job_id).await? else {
        return Ok(None);
    };
    let descendants = projected.tree.descendant_ids();
    Ok(Some((
        GqlJobStreamMessage::Snapshot(GqlJobSnapshot {
            cursor: cursor(Uuid::now_v7()),
            job: GqlJobDetail {
                job: projected.job,
                tree: projected.tree,
            },
            affordances: projected.affordances,
        }),
        descendants,
    )))
}

async fn delta(
    state: &EdgeState,
    job_id: JobId,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    event: &JobEvent,
) -> Result<Option<(GqlJobStreamMessage, HashSet<Uuid>)>> {
    let Some(carried) = of_domain(event_id, occurred_at, event) else {
        return Ok(None);
    };
    let Some(projected) = projection(state, job_id).await? else {
        return Ok(None);
    };
    let descendants = projected.tree.descendant_ids();
    Ok(Some((
        GqlJobStreamMessage::Delta(GqlJobDelta {
            cursor: cursor(event_id),
            event: carried,
            job: GqlJobDetail {
                job: projected.job,
                tree: projected.tree,
            },
            affordances: projected.affordances,
        }),
        descendants,
    )))
}

struct Projection {
    job: Arc<bc_jobs::domain::job::Job>,
    tree: Arc<JobTree>,
    affordances: Vec<Affordance>,
}

async fn projection(state: &EdgeState, job_id: JobId) -> Result<Option<Projection>> {
    let Some(job) = state.jobs.load(job_id).await.map_err(edge_error)? else {
        return Ok(None);
    };
    let parent = project::parent_of(&state.store, &job).await?;
    let affordances = project::affordances_of(&job, parent.as_deref())?;
    let tree = JobTree::rooted_at(&state.store, job_id)
        .await
        .map_err(edge_error)?;
    Ok(Some(Projection {
        job: Arc::new(job),
        tree: Arc::new(tree),
        affordances,
    }))
}
