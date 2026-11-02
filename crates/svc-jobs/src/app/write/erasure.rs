use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::event::job::JobEvent;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::db::erasure::{await_request_data_erasure, erase_request_data, erase_results_of_job};
use crate::error::ServiceError;

pub async fn erase_what_a_cancel_leaves_behind(
    tx: &mut PgConnection,
    events: &[JobEvent],
    limits: &ServiceLimits,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    for event in events {
        let JobEvent::JobCancelled(fact) = event else {
            continue;
        };
        let job_id = fact.job_id.as_uuid();
        erase_results_of_job(&mut *tx, job_id).await?;
        if limits.retention().erases_cancelled_request_data_at_once() {
            erase_request_data(&mut *tx, job_id, at).await?;
        } else {
            await_request_data_erasure(&mut *tx, job_id).await?;
        }
    }
    Ok(())
}
