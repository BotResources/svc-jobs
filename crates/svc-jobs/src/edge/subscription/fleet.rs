use async_graphql::Result;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::event::job::JobEvent;
use bc_jobs::policies::fleet::fleet_signals;
use chrono::{DateTime, Utc};
use futures::Stream;
use uuid::Uuid;

use crate::edge::error::edge_error;
use crate::edge::project;
use crate::edge::state::EdgeState;
use crate::edge::types::enums::GqlFleetEventKind;
use crate::edge::types::fleet::GqlFleetEvent;
use crate::edge::types::stream::{GqlFleetSnapshot, GqlFleetStreamMessage, GqlRunnerTypeDelta};
use crate::stream::Fact;

use super::{cursor, next_fact};

pub fn fleet_stream(
    state: EdgeState,
    runner_type: Option<String>,
) -> impl Stream<Item = GqlFleetStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        match snapshot(&state, runner_type.as_deref()).await {
            Ok(message) => yield message,
            Err(error) => {
                tracing::warn!(error = ?error, "the fleet snapshot could not be built");
                return;
            }
        }
        while let Some(fact) = next_fact(&mut facts).await {
            let messages = match fact {
                Fact::Fleet { event_id, occurred_at, event } => {
                    fleet_deltas(&state, runner_type.as_deref(), event_id, occurred_at, &event).await
                }
                Fact::Job { event_id, occurred_at, job_id, event } => {
                    job_deltas(
                        &state,
                        runner_type.as_deref(),
                        event_id,
                        occurred_at,
                        job_id,
                        &event,
                    )
                    .await
                }
                Fact::Log { .. } => Ok(vec![]),
            };
            match messages {
                Ok(messages) => {
                    for message in messages {
                        yield message;
                    }
                }
                Err(error) => tracing::warn!(error = ?error, "a fleet delta could not be built"),
            }
        }
    }
}

async fn snapshot(state: &EdgeState, runner_type: Option<&str>) -> Result<GqlFleetStreamMessage> {
    Ok(GqlFleetStreamMessage::Snapshot(GqlFleetSnapshot {
        cursor: cursor(Uuid::now_v7()),
        runner_types: project::fleet_views(&state.store, runner_type).await?,
    }))
}

async fn fleet_deltas(
    state: &EdgeState,
    watched: Option<&str>,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    event: &FleetEvent,
) -> Result<Vec<GqlFleetStreamMessage>> {
    let key = event.runner_type().clone();
    if watched.is_some_and(|watched| watched != key.as_str()) {
        return Ok(vec![]);
    }
    let kind = match event {
        FleetEvent::RunnerTypeRegistered(_) => GqlFleetEventKind::RunnerTypeRegistered,
        FleetEvent::InstanceConnected(_) => GqlFleetEventKind::InstanceConnected,
        FleetEvent::InstanceStatusReported(_) => GqlFleetEventKind::InstanceStatusReported,
        FleetEvent::InstanceDisconnected(_) => GqlFleetEventKind::InstanceDisconnected,
    };
    let carried = GqlFleetEvent {
        id: event_id,
        kind,
        occurred_at,
        runner_type: key.as_str().to_owned(),
        instance_key: event
            .instance_key()
            .map(|instance| instance.as_str().to_owned()),
        job_id: None,
        run_id: None,
    };
    Ok(vec![delta(state, &key, carried).await?])
}

async fn job_deltas(
    state: &EdgeState,
    watched: Option<&str>,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    job_id: JobId,
    event: &JobEvent,
) -> Result<Vec<GqlFleetStreamMessage>> {
    let signals = fleet_signals(event);
    if signals.is_empty() {
        return Ok(vec![]);
    }
    let Some(job) = state.jobs.load(job_id).await.map_err(edge_error)? else {
        return Ok(vec![]);
    };
    let key = job.runner_type().clone();
    if watched.is_some_and(|watched| watched != key.as_str()) {
        return Ok(vec![]);
    }
    let run_id = run_of(event);
    let mut messages = Vec::with_capacity(signals.len());
    for signal in signals {
        let carried = GqlFleetEvent {
            id: event_id,
            kind: signal.into(),
            occurred_at,
            runner_type: key.as_str().to_owned(),
            instance_key: None,
            job_id: Some(job_id.as_uuid()),
            run_id,
        };
        messages.push(delta(state, &key, carried).await?);
    }
    Ok(messages)
}

fn run_of(event: &JobEvent) -> Option<Uuid> {
    match event {
        JobEvent::RunStarted(fact) => Some(fact.run_id.as_uuid()),
        JobEvent::RunCompleted(fact) => Some(fact.run_id.as_uuid()),
        JobEvent::RunFailed(fact) => Some(fact.run_id.as_uuid()),
        JobEvent::RunCancelled(fact) => Some(fact.run_id.as_uuid()),
        _ => None,
    }
}

async fn delta(
    state: &EdgeState,
    key: &RunnerTypeKey,
    event: GqlFleetEvent,
) -> Result<GqlFleetStreamMessage> {
    let mut views = project::fleet_views(&state.store, Some(key.as_str())).await?;
    let view = views.pop().ok_or_else(|| {
        async_graphql::Error::new("the fleet projection of a live runner type is always present")
    })?;
    Ok(GqlFleetStreamMessage::Delta(GqlRunnerTypeDelta {
        cursor: cursor(event.id),
        event,
        runner_type: view.runner_type,
        affordances: view.affordances,
    }))
}
