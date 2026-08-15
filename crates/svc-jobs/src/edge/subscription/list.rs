use std::collections::HashSet;

use async_graphql::Result;
use bc_jobs::event::job::JobEvent;
use br_util_graphql::{Connection, Edge, PageInfo};
use chrono::{DateTime, Utc};
use futures::Stream;
use uuid::Uuid;

use crate::db::list::{self, JobFilter};
use crate::edge::error::edge_error;
use crate::edge::project;
use crate::edge::state::EdgeState;
use crate::edge::types::event::of_domain;
use crate::edge::types::job::GqlJobSummaryView;
use crate::edge::types::stream::{GqlJobsDelta, GqlJobsSnapshot, GqlJobsStreamMessage};
use crate::stream::Fact;

use super::{cursor, next_unbroken_fact};

pub struct Window {
    pub filter: JobFilter,
    pub first: i64,
    pub after: Option<String>,
}

pub fn list_stream(
    state: EdgeState,
    window: Window,
) -> impl Stream<Item = GqlJobsStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        let mut present: HashSet<Uuid>;
        match page(&state, &window).await {
            Ok((connection, ids)) => {
                present = ids;
                yield GqlJobsStreamMessage::Snapshot(GqlJobsSnapshot {
                    cursor: cursor(Uuid::now_v7()),
                    jobs: connection,
                });
            }
            Err(error) => {
                tracing::warn!(error = ?error, "the job list snapshot could not be built");
                return;
            }
        }
        while let Some(fact) = next_unbroken_fact(&mut facts).await {
            let Fact::Job { event_id, occurred_at, job_id, event } = fact else {
                continue;
            };
            match delta(
                &state,
                &window,
                &mut present,
                job_id.as_uuid(),
                event_id,
                occurred_at,
                &event,
            )
            .await
            {
                Ok(Some(message)) => yield message,
                Ok(None) => {}
                Err(error) => tracing::warn!(error = ?error, "a list delta could not be built"),
            }
        }
    }
}

async fn delta(
    state: &EdgeState,
    window: &Window,
    present: &mut HashSet<Uuid>,
    job_id: Uuid,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    event: &JobEvent,
) -> Result<Option<GqlJobsStreamMessage>> {
    let Some(carried) = of_domain(event_id, occurred_at, event) else {
        return Ok(None);
    };
    let matches = state
        .store
        .matches_filter(job_id, &window.filter)
        .await
        .map_err(edge_error)?;
    if !matches && !present.contains(&job_id) {
        return Ok(None);
    }
    let (ids, page_info) = window_of(state, window).await?;
    let (upserted, removed_ids) = if matches {
        present.insert(job_id);
        (upsert(state, job_id).await?, vec![])
    } else {
        present.remove(&job_id);
        (vec![], vec![job_id])
    };
    present.retain(|id| ids.contains(id) || *id == job_id);
    Ok(Some(GqlJobsStreamMessage::Delta(GqlJobsDelta {
        cursor: cursor(event_id),
        event: carried,
        upserted,
        removed_ids,
        page_info,
    })))
}

async fn window_of(state: &EdgeState, window: &Window) -> Result<(HashSet<Uuid>, PageInfo)> {
    let matched = state
        .store
        .matching_window(&window.filter, window.first, window.after.as_deref())
        .await
        .map_err(edge_error)?;
    let page_info = PageInfo {
        has_next_page: matched.has_next_page,
        has_previous_page: false,
        start_cursor: matched.entries.first().map(|(_, cursor)| cursor.clone()),
        end_cursor: matched.entries.last().map(|(_, cursor)| cursor.clone()),
    };
    Ok((matched.ids().into_iter().collect(), page_info))
}

async fn upsert(state: &EdgeState, job_id: Uuid) -> Result<Vec<GqlJobSummaryView>> {
    let jobs = state
        .store
        .load_batch(&[job_id])
        .await
        .map_err(edge_error)?;
    project::summary_views(&state.store, jobs).await
}

async fn page(
    state: &EdgeState,
    window: &Window,
) -> Result<(Connection<GqlJobSummaryView>, HashSet<Uuid>)> {
    let page = state
        .store
        .job_page(&window.filter, window.first, window.after.as_deref())
        .await
        .map_err(edge_error)?;
    let ids: HashSet<Uuid> = page.jobs.iter().map(|job| job.id().as_uuid()).collect();
    let cursors: Vec<String> = page.jobs.iter().map(list::cursor_of).collect();
    let edges: Vec<Edge<GqlJobSummaryView>> = project::summary_views(&state.store, page.jobs)
        .await?
        .into_iter()
        .zip(cursors)
        .map(|(view, cursor)| Edge::new(view, cursor))
        .collect();
    Ok((Connection::forward(edges, page.has_next_page), ids))
}
