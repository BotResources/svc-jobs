use uuid::Uuid;

use super::change::JobChange;
use crate::db::auto_finish::premise_now;
use crate::error::ServiceError;

pub(super) async fn guard_finish_premise_still_holds(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
) -> Result<(), ServiceError> {
    for change in changes {
        let Some(decided_on) = &change.finish_premise else {
            continue;
        };
        let now = premise_now(tx, change.job_id.as_uuid()).await?;
        let restart_jobs: Vec<Uuid> = decided_on
            .restart_jobs
            .iter()
            .map(|job| job.as_uuid())
            .collect();
        let restarts_due: Vec<Uuid> = decided_on
            .restarts_due
            .iter()
            .map(|run| run.as_uuid())
            .collect();
        if now.restart_jobs != restart_jobs || now.pending_wakes != restarts_due {
            return Err(ServiceError::write_race());
        }
    }
    Ok(())
}
