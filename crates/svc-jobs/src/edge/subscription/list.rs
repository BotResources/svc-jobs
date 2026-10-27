use std::collections::{HashMap, HashSet};

use async_graphql::Result;
use br_util_graphql::{Connection, Edge, PageInfo};
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

use super::{FACT_BATCH, RESET_ABOVE, cursor, next_fact_batch};

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
        match snapshot(&state, &window).await {
            Ok((message, ids)) => {
                present = ids;
                yield message;
            }
            Err(error) => {
                tracing::warn!(error = ?error, "the job list snapshot could not be built");
                return;
            }
        }
        while let Some(batch) = next_fact_batch(&mut facts, FACT_BATCH).await {
            let carried: Vec<Carried> = batch.iter().filter_map(Carried::of).collect();
            if carried.is_empty() {
                continue;
            }
            match deltas(&state, &window, &mut present, carried).await {
                Ok(Deltas::Each(messages)) => {
                    for message in messages {
                        yield message;
                    }
                }
                Ok(Deltas::Reset) => match snapshot(&state, &window).await {
                    Ok((message, ids)) => {
                        present = ids;
                        yield message;
                    }
                    Err(error) => {
                        tracing::warn!(error = ?error, "the job list could not be reset");
                        return;
                    }
                },
                Err(error) => tracing::warn!(error = ?error, "list deltas could not be built"),
            }
        }
    }
}

/// One Job fact a list may carry.
struct Carried {
    job_id: Uuid,
    event: crate::edge::types::event::GqlJobEvent,
    event_id: Uuid,
}

impl Carried {
    fn of(fact: &Fact) -> Option<Self> {
        let Fact::Job {
            event_id,
            occurred_at,
            job_id,
            event,
        } = fact
        else {
            return None;
        };
        Some(Self {
            job_id: job_id.as_uuid(),
            event: of_domain(*event_id, *occurred_at, event)?,
            event_id: *event_id,
        })
    }
}

enum Deltas {
    Each(Vec<GqlJobsStreamMessage>),
    Reset,
}

async fn snapshot(
    state: &EdgeState,
    window: &Window,
) -> Result<(GqlJobsStreamMessage, HashSet<Uuid>)> {
    let (connection, ids) = page(state, window).await?;
    Ok((
        GqlJobsStreamMessage::Snapshot(GqlJobsSnapshot {
            cursor: cursor(Uuid::now_v7()),
            jobs: connection,
        }),
        ids,
    ))
}

/// The deltas of one batch: the window and the touched Jobs are read once for
/// the whole batch, then each fact is carried with the current view of its
/// Job. A batch touching more Jobs of the list than `RESET_ABOVE` resets it.
async fn deltas(
    state: &EdgeState,
    window: &Window,
    present: &mut HashSet<Uuid>,
    carried: Vec<Carried>,
) -> Result<Deltas> {
    let mut touched: Vec<Uuid> = carried.iter().map(|fact| fact.job_id).collect();
    touched.sort_unstable();
    touched.dedup();
    let matched = state
        .store
        .matching_among(&touched, &window.filter)
        .await
        .map_err(edge_error)?;
    let relevant: Vec<&Carried> = carried
        .iter()
        .filter(|fact| matched.contains(&fact.job_id) || present.contains(&fact.job_id))
        .collect();
    if relevant.is_empty() {
        return Ok(Deltas::Each(vec![]));
    }
    if relevant.len() > RESET_ABOVE {
        return Ok(Deltas::Reset);
    }
    let (ids, page_info) = window_of(state, window).await?;
    let wanted: Vec<Uuid> = touched
        .iter()
        .copied()
        .filter(|id| matched.contains(id))
        .collect();
    let views: HashMap<Uuid, GqlJobSummaryView> = upserts(state, &wanted)
        .await?
        .into_iter()
        .map(|view| (view.job.id().as_uuid(), view))
        .collect();
    let mut messages = Vec::with_capacity(relevant.len());
    for fact in relevant {
        let (upserted, removed_ids) = match views.get(&fact.job_id) {
            Some(view) => {
                present.insert(fact.job_id);
                (vec![view.clone()], vec![])
            }
            None => {
                present.remove(&fact.job_id);
                (vec![], vec![fact.job_id])
            }
        };
        messages.push(GqlJobsStreamMessage::Delta(GqlJobsDelta {
            cursor: cursor(fact.event_id),
            event: fact.event.clone(),
            upserted,
            removed_ids,
            page_info: page_info.clone(),
        }));
    }
    present.retain(|id| ids.contains(id) || views.contains_key(id));
    Ok(Deltas::Each(messages))
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

async fn upserts(state: &EdgeState, job_ids: &[Uuid]) -> Result<Vec<GqlJobSummaryView>> {
    if job_ids.is_empty() {
        return Ok(vec![]);
    }
    let jobs = state.store.load_batch(job_ids).await.map_err(edge_error)?;
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
