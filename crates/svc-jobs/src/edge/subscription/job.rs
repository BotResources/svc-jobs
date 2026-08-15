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
use crate::edge::types::event::of_domain;
use crate::edge::types::job::GqlJobDetail;
use crate::edge::types::stream::{GqlJobDelta, GqlJobSnapshot, GqlJobStreamMessage};
use crate::stream::Fact;

use super::{cursor, next_fact};

pub fn job_stream(
    state: EdgeState,
    job_id: JobId,
) -> impl Stream<Item = GqlJobStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        match snapshot(&state, job_id).await {
            Ok(Some(message)) => yield message,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(error = ?error, "the job snapshot could not be built");
                return;
            }
        }
        while let Some(fact) = next_fact(&mut facts).await {
            let Fact::Job { event_id, occurred_at, job_id: carried, event } = fact else {
                continue;
            };
            if carried != job_id {
                continue;
            }
            match delta(&state, job_id, event_id, occurred_at, &event).await {
                Ok(Some(message)) => yield message,
                Ok(None) => {}
                Err(error) => tracing::warn!(error = ?error, "a job delta could not be built"),
            }
        }
    }
}

async fn snapshot(state: &EdgeState, job_id: JobId) -> Result<Option<GqlJobStreamMessage>> {
    let Some((job, affordances)) = projection(state, job_id).await? else {
        return Ok(None);
    };
    Ok(Some(GqlJobStreamMessage::Snapshot(GqlJobSnapshot {
        cursor: cursor(Uuid::now_v7()),
        job: GqlJobDetail { job },
        affordances,
    })))
}

async fn delta(
    state: &EdgeState,
    job_id: JobId,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    event: &JobEvent,
) -> Result<Option<GqlJobStreamMessage>> {
    let Some(carried) = of_domain(event_id, occurred_at, event) else {
        return Ok(None);
    };
    let Some((job, affordances)) = projection(state, job_id).await? else {
        return Ok(None);
    };
    Ok(Some(GqlJobStreamMessage::Delta(GqlJobDelta {
        cursor: cursor(event_id),
        event: carried,
        job: GqlJobDetail { job },
        affordances,
    })))
}

async fn projection(
    state: &EdgeState,
    job_id: JobId,
) -> Result<Option<(Arc<bc_jobs::domain::job::Job>, Vec<Affordance>)>> {
    let Some(job) = state.jobs.load(job_id).await.map_err(edge_error)? else {
        return Ok(None);
    };
    let parent = project::parent_of(&state.store, &job).await?;
    let affordances = project::affordances_of(&job, parent.as_deref())?;
    Ok(Some((Arc::new(job), affordances)))
}
