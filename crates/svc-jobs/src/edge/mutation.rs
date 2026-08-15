use async_graphql::{Context, Object, Result};
use br_util_graphql::MutationResult;

use super::error::of_service;
use super::state::{EdgeState, administrator};
use super::types::input::{GqlCancelJobInput, GqlDeleteJobInput, GqlManualRetryJobInput};
use crate::app::admin::{self, ManualRetryInput};

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
}
