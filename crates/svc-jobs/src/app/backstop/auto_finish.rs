use std::future::Future;

use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::auto_finish::AutoFinishReader;
use bc_jobs::ports::job::DUE_WORK_BATCH;

use crate::app::auto_finish::finish_called_job;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn finish_called_jobs(jobs: &Jobs) -> Result<(), ServiceError> {
    each_candidate(&jobs.store, DUE_WORK_BATCH, |called| async move {
        finish_called_job(jobs, called, &service_metadata())
            .await
            .map(drop)
    })
    .await
}

async fn each_candidate<R, F, Fut>(
    reader: &R,
    page: usize,
    mut visit: F,
) -> Result<(), ServiceError>
where
    R: AutoFinishReader,
    F: FnMut(JobId) -> Fut,
    Fut: Future<Output = Result<(), ServiceError>>,
{
    let mut after = None;
    let mut first_failure = None;
    loop {
        let batch = reader.candidates_after(after, page).await?;
        let Some(last) = batch.last().copied() else {
            break;
        };
        after = Some(last);
        let reached_the_end = batch.len() < page;
        for called in batch {
            if let Err(error) = visit(called).await {
                tracing::warn!(
                    job_id = %called.as_uuid(),
                    error = %error,
                    "finishing a called job failed; the other jobs go on and the next sweep decides again"
                );
                first_failure.get_or_insert(error);
            }
        }
        if reached_the_end {
            break;
        }
    }
    first_failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bc_jobs::domain::auto_finish::AutoFinishFacts;
    use bc_jobs::domain::ids::RunId;
    use bc_jobs::ports::PortError;
    use uuid::Uuid;

    use super::*;

    struct Candidates(Vec<JobId>);

    #[async_trait]
    impl AutoFinishReader for Candidates {
        async fn facts_of(&self, _: JobId) -> Result<Option<AutoFinishFacts>, PortError> {
            Ok(None)
        }

        async fn candidates_after(
            &self,
            after: Option<JobId>,
            limit: usize,
        ) -> Result<Vec<JobId>, PortError> {
            Ok(self
                .0
                .iter()
                .copied()
                .filter(|job| after.is_none_or(|after| job.as_uuid() > after.as_uuid()))
                .take(limit)
                .collect())
        }

        async fn called_by_runs(&self, _: &[RunId]) -> Result<Vec<JobId>, PortError> {
            Ok(Vec::new())
        }

        async fn called_jobs_stemming_to(&self, _: &[JobId]) -> Result<Vec<JobId>, PortError> {
            Ok(Vec::new())
        }
    }

    fn jobs(count: u128) -> Vec<JobId> {
        (1..=count)
            .map(|n| {
                JobId::new(Uuid::from_u128(
                    0x0190_0000_0000_7000_8000_0000_0000_0000 + n,
                ))
                .expect("a job id")
            })
            .collect()
    }

    async fn visited(count: u128, page: usize, failing: Option<JobId>) -> (Vec<JobId>, bool) {
        let seen = Mutex::new(Vec::new());
        let outcome = each_candidate(&Candidates(jobs(count)), page, |job| {
            seen.lock().expect("the log").push(job);
            let result = if Some(job) == failing {
                Err(ServiceError::Infra("down".to_owned()))
            } else {
                Ok(())
            };
            async move { result }
        })
        .await;
        (seen.into_inner().expect("the log"), outcome.is_err())
    }

    #[tokio::test]
    async fn the_sweep_reads_the_candidates_page_after_page_until_none_is_left() {
        assert_eq!(visited(7, 3, None).await, (jobs(7), false));
        assert_eq!(visited(6, 3, None).await, (jobs(6), false));
        assert_eq!(visited(0, 3, None).await, (Vec::new(), false));
    }

    #[tokio::test]
    async fn a_failing_candidate_does_not_stop_the_sweep_and_is_reported() {
        assert_eq!(visited(7, 3, Some(jobs(4)[3])).await, (jobs(7), true));
    }
}
