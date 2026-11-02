use std::future::Future;

use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::job::{CancelledRequestData, DUE_WORK_BATCH};

use crate::app::Jobs;
use crate::error::ServiceError;

pub(super) async fn erase_cancelled_request_data(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let due_before = jobs
        .limits
        .retention()
        .cancelled_request_data_due_before(now);
    let awaiting = jobs
        .store
        .cancelled_jobs_awaiting_erasure(due_before, DUE_WORK_BATCH)
        .await?;
    erase_each(awaiting, |job_id| async move {
        jobs.store
            .erase_request_data_of(job_id, now)
            .await
            .map(drop)
            .map_err(ServiceError::from)
    })
    .await
}

async fn erase_each<F, Fut>(awaiting: Vec<JobId>, mut erase: F) -> Result<(), ServiceError>
where
    F: FnMut(JobId) -> Fut,
    Fut: Future<Output = Result<(), ServiceError>>,
{
    let mut first_failure = None;
    for job_id in awaiting {
        if let Err(error) = erase(job_id).await {
            tracing::warn!(
                job_id = %job_id.as_uuid(),
                error = %error,
                "erasing the request data of a cancelled Job failed; the other Jobs go on and the \
                 next sweep tries this one again"
            );
            first_failure.get_or_insert(error);
        }
    }
    first_failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;

    fn job(seed: u128) -> JobId {
        JobId::new(Uuid::from_u128((seed << 64) | 0x7000_8000_0000_0000_0000)).unwrap()
    }

    #[tokio::test]
    async fn a_job_that_fails_to_erase_does_not_stop_the_jobs_behind_it() {
        let (first, poisoned, last) = (job(1), job(2), job(3));
        let erased = Mutex::new(Vec::new());

        let outcome = erase_each(vec![first, poisoned, last], |job_id| {
            let erased = &erased;
            async move {
                if job_id == poisoned {
                    return Err(ServiceError::Infra("poisoned".to_owned()));
                }
                erased.lock().unwrap().push(job_id);
                Ok(())
            }
        })
        .await;

        assert_eq!(*erased.lock().unwrap(), vec![first, last]);
        assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "poisoned"));
    }

    #[tokio::test]
    async fn the_first_failure_is_the_one_reported() {
        let outcome = erase_each(vec![job(1), job(2)], |job_id| async move {
            Err(ServiceError::Infra(format!("{}", job_id.as_uuid())))
        })
        .await;

        assert!(
            matches!(outcome, Err(ServiceError::Infra(reason)) if reason == job(1).as_uuid().to_string())
        );
    }
}
