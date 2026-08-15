use async_graphql::Result;
use bc_jobs::domain::ids::{JobId, RunId};
use futures::Stream;
use uuid::Uuid;

use crate::db::logs::{LogWindow, cursor_of};
use crate::edge::error::edge_error;
use crate::edge::log_page::log_connection;
use crate::edge::state::EdgeState;
use crate::edge::types::log::log_of;
use crate::edge::types::stream::{GqlJobLogSnapshot, GqlJobLogStreamMessage, GqlRunLogAppended};
use crate::stream::Fact;

use super::{cursor, next_fact};

pub fn log_stream(
    state: EdgeState,
    job_id: JobId,
    run_id: Option<RunId>,
    last: i64,
    before: Option<String>,
) -> impl Stream<Item = GqlJobLogStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        match snapshot(&state, job_id, run_id, last, before.as_deref()).await {
            Ok(message) => yield message,
            Err(error) => {
                tracing::warn!(error = ?error, "the log tail snapshot could not be built");
                return;
            }
        }
        while let Some(fact) = next_fact(&mut facts).await {
            let Fact::Log { log_id, job_id: carried, run_id: carried_run } = fact else {
                continue;
            };
            if carried != job_id || run_id.is_some_and(|wanted| wanted != carried_run) {
                continue;
            }
            match appended(&state, log_id).await {
                Ok(Some(message)) => yield message,
                Ok(None) => {}
                Err(error) => tracing::warn!(error = ?error, "a log message could not be built"),
            }
        }
    }
}

async fn snapshot(
    state: &EdgeState,
    job_id: JobId,
    run_id: Option<RunId>,
    last: i64,
    before: Option<&str>,
) -> Result<GqlJobLogStreamMessage> {
    let page = state
        .store
        .log_page(
            job_id,
            run_id,
            LogWindow {
                first: None,
                last: Some(last),
            },
            None,
            before,
        )
        .await
        .map_err(edge_error)?;
    Ok(GqlJobLogStreamMessage::Snapshot(GqlJobLogSnapshot {
        cursor: cursor(Uuid::now_v7()),
        logs: log_connection(page),
    }))
}

async fn appended(
    state: &EdgeState,
    log_id: bc_jobs::domain::ids::RunLogId,
) -> Result<Option<GqlJobLogStreamMessage>> {
    let Some((job_id, line)) = state.store.log_line(log_id).await.map_err(edge_error)? else {
        return Ok(None);
    };
    Ok(Some(GqlJobLogStreamMessage::Appended(GqlRunLogAppended {
        cursor: cursor_of(&line),
        log: log_of(job_id, &line),
    })))
}
