pub mod fleet;
pub mod job;
pub mod list;
pub mod logs;
pub mod priority;

use async_graphql::{Context, Result, Subscription};
use bc_jobs::domain::ids::{JobId, RunId};
use futures::Stream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use super::error::edge_error;
use super::state::{EdgeState, administrator, own_user, principal};
use super::types::input::{GqlConnectionWindowInput, GqlJobFilterInput, GqlLogTailWindowInput};
use super::types::priority::GqlMyMaxPriorityStreamMessage;
use super::types::stream::{
    GqlFleetStreamMessage, GqlJobLogStreamMessage, GqlJobStreamMessage, GqlJobsStreamMessage,
};
use crate::stream::Fact;

pub struct SubscriptionRoot;

#[Subscription(name = "Subscription")]
impl SubscriptionRoot {
    async fn jobs_changed(
        &self,
        ctx: &Context<'_>,
        filter: Option<GqlJobFilterInput>,
        window: GqlConnectionWindowInput,
    ) -> Result<impl Stream<Item = GqlJobsStreamMessage>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?.clone();
        Ok(list::list_stream(
            state,
            list::Window {
                filter: filter.unwrap_or_default().into_filter(),
                first: i64::from(window.first),
                after: window.after,
            },
        ))
    }

    async fn jobs_job_changed(
        &self,
        ctx: &Context<'_>,
        job_id: Uuid,
    ) -> Result<impl Stream<Item = GqlJobStreamMessage>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?.clone();
        Ok(job::job_stream(
            state,
            JobId::new(job_id).map_err(edge_error)?,
            principal(ctx)?,
        ))
    }

    async fn jobs_job_log_tail(
        &self,
        ctx: &Context<'_>,
        job_id: Uuid,
        run_id: Option<Uuid>,
        window: GqlLogTailWindowInput,
    ) -> Result<impl Stream<Item = GqlJobLogStreamMessage>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?.clone();
        Ok(logs::log_stream(
            state,
            JobId::new(job_id).map_err(edge_error)?,
            run_id.map(RunId::new).transpose().map_err(edge_error)?,
            i64::from(window.last),
            window.before,
        ))
    }

    async fn jobs_fleet_changed(
        &self,
        ctx: &Context<'_>,
        runner_type: Option<String>,
    ) -> Result<impl Stream<Item = GqlFleetStreamMessage>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?.clone();
        Ok(fleet::fleet_stream(state, runner_type))
    }

    /// The caller's own maximum priority: a snapshot, then every change. Open
    /// to any active human; nothing else of Jobs is readable this way.
    async fn jobs_my_max_priority_changed(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = GqlMyMaxPriorityStreamMessage>> {
        let user_id = own_user(ctx)?;
        let state = ctx.data::<EdgeState>()?.clone();
        Ok(priority::my_max_priority_stream(state, user_id))
    }
}

pub fn cursor(event_id: Uuid) -> String {
    event_id.to_string()
}

pub async fn next_unbroken_fact(facts: &mut Receiver<Fact>) -> Option<Fact> {
    match facts.recv().await {
        Ok(fact) => Some(fact),
        Err(RecvError::Lagged(missed)) => {
            tracing::warn!(
                missed,
                "a subscriber fell behind the durable fact stream; its subscription ends so the \
                 client reconnects onto a fresh snapshot"
            );
            None
        }
        Err(RecvError::Closed) => None,
    }
}
