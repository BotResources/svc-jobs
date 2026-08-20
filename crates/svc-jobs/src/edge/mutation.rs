use async_graphql::{Context, Object, Result};
use br_util_graphql::MutationResult;

use super::error::of_service;
use super::state::{EdgeState, administrator};
use super::types::input::{
    GqlCancelJobInput, GqlDeleteJobInput, GqlManualRetryJobInput, GqlRunnerTypeInput,
};
use crate::app::admin::{self, ManualRetryInput, RunnerTypeAction};

pub struct MutationRoot;

#[Object(name = "Mutation")]
impl MutationRoot {
    async fn jobs_cancel_job(
        &self,
        ctx: &Context<'_>,
        input: GqlCancelJobInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        admin::cancel(&state.jobs, actor, input.id, input.job_id)
            .await
            .map_err(|error| async_graphql::Error::from(of_service(error)))?;
        Ok(MutationResult::ok())
    }

    async fn jobs_manual_retry_job(
        &self,
        ctx: &Context<'_>,
        input: GqlManualRetryJobInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        admin::manual_retry(
            &state.jobs,
            actor,
            ManualRetryInput {
                manual_retry_id: input.id,
                job_id: input.job_id,
                successor_job_id: input.successor_job_id,
                failed_resolution_id: input.failed_resolution_id,
            },
        )
        .await
        .map_err(|error| async_graphql::Error::from(of_service(error)))?;
        Ok(MutationResult::ok())
    }

    async fn jobs_delete_job(
        &self,
        ctx: &Context<'_>,
        input: GqlDeleteJobInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        admin::delete(&state.jobs, actor, input.job_id)
            .await
            .map_err(|error| async_graphql::Error::from(of_service(error)))?;
        Ok(MutationResult::ok())
    }

    async fn jobs_deprecate_runner_type(
        &self,
        ctx: &Context<'_>,
        input: GqlRunnerTypeInput,
    ) -> Result<MutationResult> {
        change_runner_type(ctx, input, RunnerTypeAction::Deprecate).await
    }

    async fn jobs_reactivate_runner_type(
        &self,
        ctx: &Context<'_>,
        input: GqlRunnerTypeInput,
    ) -> Result<MutationResult> {
        change_runner_type(ctx, input, RunnerTypeAction::Reactivate).await
    }

    async fn jobs_retire_runner_type(
        &self,
        ctx: &Context<'_>,
        input: GqlRunnerTypeInput,
    ) -> Result<MutationResult> {
        change_runner_type(ctx, input, RunnerTypeAction::Retire).await
    }
}

async fn change_runner_type(
    ctx: &Context<'_>,
    input: GqlRunnerTypeInput,
    action: RunnerTypeAction,
) -> Result<MutationResult> {
    let actor = administrator(ctx)?;
    let state = ctx.data::<EdgeState>()?;
    admin::change_runner_type(&state.jobs, actor, input.runner_type, action)
        .await
        .map_err(|error| async_graphql::Error::from(of_service(error)))?;
    Ok(MutationResult::ok())
}
