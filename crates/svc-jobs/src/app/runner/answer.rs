use bc_jobs::domain::ids::{ResultId, ResultId as Id};
use bc_jobs::domain::runner::answer::ResultRefusal;
use uuid::Uuid;

use super::{Jobs, Outcome, WorkIds, started};
use crate::db::answers::StoredResult;

pub async fn hand_over_result(
    jobs: &Jobs,
    work: &WorkIds,
    answer: StoredResult,
) -> Outcome<ResultId> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .hand_over_result(work.named(), &answer, jobs.ids.as_ref(), jobs.clock.now())
        .await?)
}

pub async fn read_result(
    jobs: &Jobs,
    work: &WorkIds,
    result_id: Uuid,
    keep: bool,
) -> Outcome<StoredResult> {
    started(jobs, work).await?;
    let Ok(result_id) = Id::new(result_id) else {
        return Ok(Err(ResultRefusal::ResultUnknown.into()));
    };
    Ok(jobs
        .store
        .read_result(work.named(), result_id, keep, jobs.clock.now())
        .await?)
}
