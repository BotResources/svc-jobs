use std::future::Future;

use bc_jobs::domain::ids::JobId;

use crate::error::ServiceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finished {
    Decided,
    TreeBusy,
}

pub async fn finish_within_budget<F, Fut>(candidates: Vec<JobId>, budget: usize, mut finish: F)
where
    F: FnMut(JobId) -> Fut,
    Fut: Future<Output = Result<Finished, ServiceError>>,
{
    if candidates.len() > budget {
        tracing::info!(
            candidates = candidates.len(),
            budget,
            "more called jobs than one end of a run finishes; the next sweep decides the others"
        );
    }
    for called in candidates.into_iter().take(budget) {
        match finish(called).await {
            Ok(Finished::Decided) => {}
            Ok(Finished::TreeBusy) => {
                tracing::info!(
                    job_id = %called.as_uuid(),
                    "a Tree is busy; the next end of a run or the next sweep decides the other called jobs"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %called.as_uuid(),
                    error = %error,
                    "finishing a called job failed; the next end of a run or the next sweep decides again"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;

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

    async fn visited(
        candidates: Vec<JobId>,
        budget: usize,
        outcome: impl Fn(JobId) -> Result<Finished, ServiceError>,
    ) -> Vec<JobId> {
        let seen = Mutex::new(Vec::new());
        finish_within_budget(candidates, budget, |job| {
            seen.lock().expect("the log").push(job);
            let result = outcome(job);
            async move { result }
        })
        .await;
        seen.into_inner().expect("the log")
    }

    #[tokio::test]
    async fn the_end_of_a_run_finishes_no_more_called_jobs_than_the_budget_and_leaves_the_rest_to_the_sweep()
     {
        let seen = visited(jobs(7), 3, |_| Ok(Finished::Decided)).await;
        assert_eq!(seen, jobs(3));
    }

    #[tokio::test]
    async fn the_end_of_a_run_stops_at_the_first_busy_tree() {
        let seen = visited(jobs(7), 7, |job| {
            if job == jobs(3)[2] {
                Ok(Finished::TreeBusy)
            } else {
                Ok(Finished::Decided)
            }
        })
        .await;
        assert_eq!(seen, jobs(3));
    }

    #[tokio::test]
    async fn a_failed_called_job_does_not_stop_the_others() {
        let seen = visited(jobs(4), 4, |job| {
            if job == jobs(2)[1] {
                Err(ServiceError::Infra("down".to_owned()))
            } else {
                Ok(Finished::Decided)
            }
        })
        .await;
        assert_eq!(seen, jobs(4));
    }
}
