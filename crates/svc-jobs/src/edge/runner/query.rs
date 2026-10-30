use async_graphql::{Context, Object, Result};
use uuid::Uuid;

use super::guard::runner_caller;
use super::input::GqlWorkInput;
use super::mail_output::{GqlAttachmentResult, GqlMailResult, GqlUnreadResult};
use super::output::{GqlContent, GqlContentResult, GqlSavedStateResult};
use crate::app::runner;
use crate::edge::error::edge_error;
use crate::edge::state::EdgeState;

#[derive(Default)]
pub struct RunnerQuery;

#[Object]
impl RunnerQuery {
    async fn jobs_payload(
        &self,
        ctx: &Context<'_>,
        work: GqlWorkInput,
    ) -> Result<GqlContentResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::fetch_payload(&state.jobs, &work.into())
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(payload) => {
                GqlContentResult::found(GqlContent::new(payload.message, payload.attachments))
            }
            Err(refusal) => GqlContentResult::refused(&refusal),
        })
    }

    async fn jobs_attachment(
        &self,
        ctx: &Context<'_>,
        work: GqlWorkInput,
        mail_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<GqlAttachmentResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::fetch_attachment(&state.jobs, &work.into(), mail_id, attachment_id)
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(attachment) => GqlAttachmentResult::found(attachment),
            Err(refusal) => GqlAttachmentResult::refused(&refusal),
        })
    }

    async fn jobs_unread(
        &self,
        ctx: &Context<'_>,
        work: GqlWorkInput,
        address: Option<Uuid>,
    ) -> Result<GqlUnreadResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::unread(&state.jobs, &work.into(), address)
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(items) => GqlUnreadResult::found(items),
            Err(refusal) => GqlUnreadResult::refused(&refusal),
        })
    }

    async fn jobs_mail(
        &self,
        ctx: &Context<'_>,
        work: GqlWorkInput,
        mail_id: Uuid,
        address: Uuid,
    ) -> Result<GqlMailResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::read_mail(&state.jobs, &work.into(), mail_id, address)
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(read) => GqlMailResult::found(read),
            Err(refusal) => GqlMailResult::refused(&refusal),
        })
    }

    async fn jobs_saved_state(
        &self,
        ctx: &Context<'_>,
        work: GqlWorkInput,
    ) -> Result<GqlSavedStateResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::read_saved_state(&state.jobs, &work.into())
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(saved) => GqlSavedStateResult::found(saved),
            Err(refusal) => GqlSavedStateResult::refused(&refusal),
        })
    }
}
