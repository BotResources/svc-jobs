use async_graphql::{Context, Object, Result};
use bc_jobs::domain::ids::SourceEntityId;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::keys::ProducerKey;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::ports::job::JobReader;
use br_util_graphql::{Connection, Edge, EdgeError};
use uuid::Uuid;

use super::error::edge_error;
use super::project;
use super::state::{EdgeState, administrator};
use super::types::fleet::GqlRunnerTypeView;
use super::types::input::GqlJobFilterInput;
use super::types::job::{GqlJobSummaryView, GqlJobView};
use super::types::log::GqlRunLog;
use crate::db::list;
use crate::db::logs::LogWindow;
use crate::edge::log_page::log_connection;

pub struct QueryRoot;

#[Object(name = "Query")]
impl QueryRoot {
    async fn jobs(
        &self,
        ctx: &Context<'_>,
        filter: Option<GqlJobFilterInput>,
        #[graphql(default = 50)] first: i32,
        after: Option<String>,
    ) -> Result<Connection<GqlJobSummaryView>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let filter = filter.unwrap_or_default().into_filter();
        let page = state
            .store
            .job_page(&filter, i64::from(first), after.as_deref())
            .await
            .map_err(edge_error)?;
        let cursors: Vec<String> = page.jobs.iter().map(list::cursor_of).collect();
        let views = project::summary_views(&state.store, page.jobs)
            .await?
            .into_iter()
            .zip(cursors)
            .map(|(view, cursor)| Edge::new(view, cursor))
            .collect();
        Ok(Connection::forward(views, page.has_next_page))
    }

    async fn jobs_job(&self, ctx: &Context<'_>, id: Uuid) -> Result<GqlJobView> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let job_id = JobId::new(id).map_err(edge_error)?;
        let job = JobReader::load(&state.store, job_id)
            .await
            .map_err(edge_error)?
            .ok_or_else(|| {
                async_graphql::Error::from(EdgeError::not_found().with_reason("job_not_found"))
            })?;
        project::view_of_job(&state.store, job).await
    }

    async fn jobs_job_by_source(
        &self,
        ctx: &Context<'_>,
        source_bc: String,
        source_entity_id: Uuid,
    ) -> Result<Option<GqlJobView>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let source = SourceReference::new(
            ProducerKey::new(source_bc).map_err(edge_error)?,
            SourceEntityId::new(source_entity_id).map_err(edge_error)?,
        );
        let found = JobReader::load_active_for_source(&state.store, &source)
            .await
            .map_err(edge_error)?;
        match found {
            None => Ok(None),
            Some(job) => Ok(Some(project::view_of_job(&state.store, job).await?)),
        }
    }

    // the arguments are the sealed SDL's own connection arguments, not a signature we chose
    #[allow(clippy::too_many_arguments)]
    async fn jobs_logs(
        &self,
        ctx: &Context<'_>,
        job_id: Uuid,
        run_id: Option<Uuid>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Connection<GqlRunLog>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let job_id = JobId::new(job_id).map_err(edge_error)?;
        let run_id = run_id.map(RunId::new).transpose().map_err(edge_error)?;
        if first.is_some() && last.is_some() {
            return Err(async_graphql::Error::from(
                EdgeError::bad_user_input().with_reason("first_and_last_together"),
            ));
        }
        let page = state
            .store
            .log_page(
                job_id,
                run_id,
                LogWindow {
                    first: first.map(i64::from),
                    last: last.map(i64::from),
                },
                after.as_deref(),
                before.as_deref(),
            )
            .await
            .map_err(edge_error)?;
        Ok(log_connection(page))
    }

    async fn jobs_fleet(
        &self,
        ctx: &Context<'_>,
        runner_type: Option<String>,
    ) -> Result<Vec<GqlRunnerTypeView>> {
        administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        project::fleet_views(state, runner_type.as_deref()).await
    }
}
