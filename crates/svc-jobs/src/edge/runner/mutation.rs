use async_graphql::{Context, Object, Result};
use bc_jobs::domain::ownership::ActorRef;
use br_core_auth::Passport;
use br_util_graphql::EdgeError;

use super::guard::runner_caller;
use super::input::{
    GqlReadResultInput, GqlSaveStateInput, GqlSendMailInput, GqlUploadAttachmentInput,
    GqlUploadPayloadInput, GqlUploadResultInput,
};
use super::mail_output::{GqlRestartRefused, GqlSendMailResult};
use super::output::{GqlContent, GqlContentResult, GqlRunnerResult};
use crate::app::runner::{self, AttachmentInput, SendMailInput, Uploader};
use crate::edge::error::edge_error;
use crate::edge::state::EdgeState;
use bc_jobs::domain::runner::views::StoredResult;

#[derive(Default)]
pub struct RunnerMutation;

fn uploader_of(passport: &Passport, work: Option<super::input::GqlWorkInput>) -> Result<Uploader> {
    match (work, passport) {
        (Some(work), _) => Ok(Uploader::Run(work.into())),
        (None, Passport::Service { .. }) => {
            Ok(Uploader::Service(ActorRef::new(passport.actor_id())))
        }
        (None, _) => Err(EdgeError::forbidden().into()),
    }
}

#[Object]
impl RunnerMutation {
    async fn jobs_upload_payload(
        &self,
        ctx: &Context<'_>,
        input: GqlUploadPayloadInput,
    ) -> Result<GqlRunnerResult> {
        let uploader = uploader_of(runner_caller(ctx)?, input.work)?;
        let state = ctx.data::<EdgeState>()?;
        let (message, attachments) = input.content.into_parts();
        let outcome = runner::upload_payload(
            &state.jobs,
            &uploader,
            input.for_job_id,
            message,
            attachments,
        )
        .await
        .map_err(edge_error)?;
        Ok(match outcome {
            Ok(_) => GqlRunnerResult::ok(),
            Err(refusal) => GqlRunnerResult::refused(&refusal),
        })
    }

    async fn jobs_upload_result(
        &self,
        ctx: &Context<'_>,
        input: GqlUploadResultInput,
    ) -> Result<GqlRunnerResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let (message, attachments) = input.answer.into_parts();
        let outcome = runner::hand_over_result(
            &state.jobs,
            &input.work.into(),
            StoredResult {
                message,
                attachments,
            },
        )
        .await
        .map_err(edge_error)?;
        Ok(match outcome {
            Ok(_) => GqlRunnerResult::ok(),
            Err(refusal) => GqlRunnerResult::refused(&refusal),
        })
    }

    async fn jobs_read_result(
        &self,
        ctx: &Context<'_>,
        input: GqlReadResultInput,
    ) -> Result<GqlContentResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome =
            runner::read_result(&state.jobs, &input.work.into(), input.result_id, input.keep)
                .await
                .map_err(edge_error)?;
        Ok(match outcome {
            Ok(answer) => {
                GqlContentResult::found(GqlContent::new(answer.message, answer.attachments))
            }
            Err(refusal) => GqlContentResult::refused(&refusal),
        })
    }

    async fn jobs_upload_attachment(
        &self,
        ctx: &Context<'_>,
        input: GqlUploadAttachmentInput,
    ) -> Result<GqlRunnerResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::upload_attachment(
            &state.jobs,
            AttachmentInput {
                work: input.work.into(),
                mail_id: input.mail_id,
                attachment_id: input.attachment_id,
                from: input.from_address,
                name: input.name,
                content_type: input.content_type,
                content: input.content,
            },
        )
        .await
        .map_err(edge_error)?;
        Ok(match outcome {
            Ok(()) => GqlRunnerResult::ok(),
            Err(refusal) => GqlRunnerResult::refused(&refusal),
        })
    }

    async fn jobs_send_mail(
        &self,
        ctx: &Context<'_>,
        input: GqlSendMailInput,
    ) -> Result<GqlSendMailResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::send_mail(
            &state.jobs,
            SendMailInput {
                work: input.work.into(),
                mail_id: input.mail_id,
                from: input.from_address,
                author: input.author,
                source_conversation_id: input.source_conversation_id,
                subject: input.subject,
                body: input.body,
                to: input.to,
                cc: input.cc,
                attachments: input
                    .attachments
                    .into_iter()
                    .map(|attachment| (attachment.attachment_id, attachment.name))
                    .collect(),
            },
        )
        .await
        .map_err(edge_error)?;
        Ok(match outcome {
            Ok(sent) => GqlSendMailResult {
                success: true,
                restart_refused: sent
                    .refused_restarts
                    .iter()
                    .map(|refused| GqlRestartRefused {
                        address: refused.address.as_uuid(),
                        reason: refused.code().to_owned(),
                        message: refused.message().to_owned(),
                    })
                    .collect(),
                ..GqlSendMailResult::default()
            },
            Err(refusal) => GqlSendMailResult::refused(&refusal),
        })
    }

    async fn jobs_save_state(
        &self,
        ctx: &Context<'_>,
        input: GqlSaveStateInput,
    ) -> Result<GqlRunnerResult> {
        runner_caller(ctx)?;
        let state = ctx.data::<EdgeState>()?;
        let outcome = runner::save_state(&state.jobs, &input.work.into(), input.state.0)
            .await
            .map_err(edge_error)?;
        Ok(match outcome {
            Ok(()) => GqlRunnerResult::ok(),
            Err(refusal) => GqlRunnerResult::refused(&refusal),
        })
    }
}
