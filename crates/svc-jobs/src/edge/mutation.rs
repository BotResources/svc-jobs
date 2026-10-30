use async_graphql::{Context, Object, Result};
use br_util_graphql::MutationResult;

use super::error::of_service;
use super::state::{EdgeState, actor, administrator, principal};
use super::types::input::{
    GqlCancelJobInput, GqlClearUserMaxPriorityInput, GqlDeleteJobInput, GqlForceJobPriorityInput,
    GqlManualRetryJobInput, GqlRunnerTypeInput, GqlSetRunnerTypeDispatchModeInput,
    GqlSetRunnerTypeMaxRunDurationInput, GqlSetUserMaxPriorityInput,
};
use crate::app::admin::{self, ManualRetryInput};
use crate::app::runner_type_lifecycle::{self, RunnerTypeAction};
use crate::app::{priority, runner_type_settings};
use crate::error::ServiceError;

#[derive(Default)]
pub struct MutationRoot;

#[Object]
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

    async fn jobs_set_runner_type_dispatch_mode(
        &self,
        ctx: &Context<'_>,
        input: GqlSetRunnerTypeDispatchModeInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            runner_type_settings::dispatch_mode(
                &state.jobs,
                actor,
                input.runner_type,
                input.dispatch_mode.into(),
            )
            .await,
        )
    }

    async fn jobs_set_runner_type_max_run_duration(
        &self,
        ctx: &Context<'_>,
        input: GqlSetRunnerTypeMaxRunDurationInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            runner_type_settings::max_run_duration(
                &state.jobs,
                actor,
                input.runner_type,
                i64::from(input.max_run_duration_seconds),
            )
            .await,
        )
    }

    /// Returns a RunnerType to the service default maximum Run duration.
    async fn jobs_clear_runner_type_max_run_duration(
        &self,
        ctx: &Context<'_>,
        input: GqlRunnerTypeInput,
    ) -> Result<MutationResult> {
        let actor = administrator(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            runner_type_settings::clear_run_duration(&state.jobs, actor, input.runner_type).await,
        )
    }

    /// Super users only: decided by the domain on the caller's principal.
    async fn jobs_set_user_max_priority(
        &self,
        ctx: &Context<'_>,
        input: GqlSetUserMaxPriorityInput,
    ) -> Result<MutationResult> {
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            priority::set_max(
                &state.jobs,
                actor(ctx)?,
                &principal(ctx)?,
                input.user_id,
                i64::from(input.max_priority),
            )
            .await,
        )
    }

    /// Super users only: decided by the domain on the caller's principal.
    async fn jobs_clear_user_max_priority(
        &self,
        ctx: &Context<'_>,
        input: GqlClearUserMaxPriorityInput,
    ) -> Result<MutationResult> {
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            priority::clear_max(&state.jobs, actor(ctx)?, &principal(ctx)?, input.user_id).await,
        )
    }

    /// Super users only: decided by the domain on the caller's principal.
    async fn jobs_force_job_priority(
        &self,
        ctx: &Context<'_>,
        input: GqlForceJobPriorityInput,
    ) -> Result<MutationResult> {
        let state = ctx.data::<EdgeState>()?;
        acknowledged(
            priority::force(
                &state.jobs,
                actor(ctx)?,
                &principal(ctx)?,
                input.job_id,
                i64::from(input.priority),
            )
            .await,
        )
    }
}

fn acknowledged(outcome: Result<(), ServiceError>) -> Result<MutationResult> {
    outcome.map_err(|error| async_graphql::Error::from(of_service(error)))?;
    Ok(MutationResult::ok())
}

async fn change_runner_type(
    ctx: &Context<'_>,
    input: GqlRunnerTypeInput,
    action: RunnerTypeAction,
) -> Result<MutationResult> {
    let actor = administrator(ctx)?;
    let state = ctx.data::<EdgeState>()?;
    runner_type_lifecycle::change(&state.jobs, actor, input.runner_type, action)
        .await
        .map_err(|error| async_graphql::Error::from(of_service(error)))?;
    Ok(MutationResult::ok())
}
