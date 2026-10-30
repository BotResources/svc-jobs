use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::JobCall;
use bc_jobs::domain::mail::inbox::{Notice, NoticeType};
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::call_notices::{CallNotices, NoticePlan};
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::PgConnection;

use crate::app::integration::message::failure_message;
use crate::db::call_notices::{
    Delivering, NoticeFor, insert_notice, queue_position, result_of_run,
};
use crate::db::hydrate::unavailable;
use crate::db::restart::wake_run;
use crate::error::ServiceError;

pub async fn tell_callers(
    tx: &mut PgConnection,
    ids: &dyn IdFactory,
    events: &[JobEvent],
    metadata: &EventMetadata,
    at: DateTime<Utc>,
    inline_max_bytes: usize,
) -> Result<(), ServiceError> {
    for event in events {
        let job_id = event.job_id();
        let Some(notices) = call_notices_of(&mut *tx, job_id).await? else {
            continue;
        };
        for plan in notices.plan(event) {
            let Some(notice) =
                build(&mut *tx, plan, event, job_id, &notices, inline_max_bytes).await?
            else {
                continue;
            };
            let wakes = notice.notice_type == NoticeType::JobFailed;
            insert_notice(
                &mut *tx,
                NoticeFor {
                    run_id: notices.caller_run,
                    address: notices.address,
                },
                &notice,
                Delivering { ids, metadata, at },
            )
            .await?;
            if wakes {
                wake_run(&mut *tx, notices.caller_run.as_uuid(), at).await?;
            }
        }
    }
    Ok(())
}

async fn call_notices_of(
    tx: &mut PgConnection,
    job_id: JobId,
) -> Result<Option<CallNotices>, ServiceError> {
    let row: Option<(Option<serde_json::Value>, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT requester, call FROM jobs WHERE id = $1")
            .bind(job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
    let Some((Some(requester), Some(call))) = row else {
        return Ok(None);
    };
    let requester: Requester = serde_json::from_value(requester)?;
    let call: JobCall = serde_json::from_value(call)?;
    Ok(CallNotices::of(&requester, Some(&call)))
}

async fn build(
    tx: &mut PgConnection,
    plan: NoticePlan,
    event: &JobEvent,
    job_id: JobId,
    notices: &CallNotices,
    inline_max_bytes: usize,
) -> Result<Option<Notice>, ServiceError> {
    let bare = |notice_type| Notice {
        notice_type,
        job_id,
        position: None,
        failure_report: None,
        message: None,
        answer: None,
        result_id: None,
    };
    Ok(Some(match plan {
        NoticePlan::Accepted => bare(NoticeType::Accepted),
        NoticePlan::Started => bare(NoticeType::Started),
        NoticePlan::QueuePosition => Notice {
            position: queue_position(&mut *tx, job_id).await?,
            ..bare(NoticeType::QueuePosition)
        },
        NoticePlan::RunCompleted { run_id } => {
            run_completed(tx, job_id, run_id, notices, inline_max_bytes, bare).await?
        }
        NoticePlan::JobFailed => match event {
            JobEvent::JobFailed(fact) => {
                let report = fact.report.as_ref();
                Notice {
                    failure_report: report.map(|report| {
                        json!({
                            "kind": report.kind().as_db_str(),
                            "reason_code": report.reason_code().as_str(),
                            "params": report.params(),
                            "diagnostic": report.diagnostic(),
                        })
                    }),
                    message: Some(failure_message(
                        job_id.as_uuid(),
                        fact.failure_cause.as_db_str(),
                        report.map(|report| report.reason_code().as_str()),
                        report.is_some_and(|report| report.kind().is_retryable()),
                        fact.caused_by_run_id.map(|run| run.as_uuid()),
                    )),
                    ..bare(NoticeType::JobFailed)
                }
            }
            _ => return Ok(None),
        },
    }))
}

async fn run_completed(
    tx: &mut PgConnection,
    job_id: JobId,
    run_id: RunId,
    notices: &CallNotices,
    inline_max_bytes: usize,
    bare: impl Fn(NoticeType) -> Notice,
) -> Result<Notice, ServiceError> {
    let base = bare(NoticeType::RunCompleted);
    if notices.address.is_some() {
        return Ok(base);
    }
    let Some(kept) = result_of_run(tx, job_id, run_id).await? else {
        return Ok(base);
    };
    let answer = json!({ "message": kept.message, "attachments": kept.attachments });
    let within = serde_json::to_vec(&answer).is_ok_and(|bytes| bytes.len() <= inline_max_bytes);
    Ok(Notice {
        answer: within.then_some(answer),
        result_id: Some(kept.result_id),
        ..base
    })
}
