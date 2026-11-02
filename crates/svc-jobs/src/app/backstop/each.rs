use std::future::Future;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;

use crate::error::ServiceError;

pub(super) async fn each_job<F, Fut>(
    jobs: Vec<JobId>,
    what: &'static str,
    work: F,
) -> Result<(), ServiceError>
where
    F: FnMut(JobId) -> Fut,
    Fut: Future<Output = Result<(), ServiceError>>,
{
    each_entry(jobs, |job_id| *job_id, what, work).await
}

pub(super) async fn each_loaded_job<F, Fut>(
    jobs: Vec<Job>,
    what: &'static str,
    work: F,
) -> Result<(), ServiceError>
where
    F: FnMut(Job) -> Fut,
    Fut: Future<Output = Result<(), ServiceError>>,
{
    each_entry(jobs, Job::id, what, work).await
}

async fn each_entry<T, F, Fut>(
    entries: Vec<T>,
    id_of: impl Fn(&T) -> JobId,
    what: &'static str,
    mut work: F,
) -> Result<(), ServiceError>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Result<(), ServiceError>>,
{
    let mut first_failure = None;
    for entry in entries {
        let job_id = id_of(&entry);
        if let Err(error) = work(entry).await {
            tracing::warn!(
                job_id = %job_id.as_uuid(),
                error = %error,
                what,
                "a sweep step failed for one entry; the other entries go on and the next sweep \
                 tries this one again"
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
    async fn an_entry_that_fails_does_not_stop_the_entries_behind_it() {
        let (first, poisoned, last) = (job(1), job(2), job(3));
        let done = Mutex::new(Vec::new());

        let outcome = each_job(vec![first, poisoned, last], "test", |job_id| {
            let done = &done;
            async move {
                if job_id == poisoned {
                    return Err(ServiceError::Infra("poisoned".to_owned()));
                }
                done.lock().unwrap().push(job_id);
                Ok(())
            }
        })
        .await;

        assert_eq!(*done.lock().unwrap(), vec![first, last]);
        assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "poisoned"));
    }

    #[tokio::test]
    async fn the_first_failure_is_the_one_reported() {
        let outcome = each_job(vec![job(1), job(2)], "test", |job_id| async move {
            Err(ServiceError::Infra(format!("{}", job_id.as_uuid())))
        })
        .await;

        assert!(
            matches!(outcome, Err(ServiceError::Infra(reason)) if reason == job(1).as_uuid().to_string())
        );
    }

    #[tokio::test]
    async fn a_loaded_job_that_fails_does_not_stop_the_jobs_behind_it() {
        use bc_jobs::fixtures::JobBuilder;

        let jobs: Vec<Job> = (0..3).map(|_| JobBuilder::new().build()).collect();
        let poisoned = jobs[1].id();
        let expected = vec![jobs[0].id(), jobs[2].id()];
        let done = Mutex::new(Vec::new());

        let outcome = each_loaded_job(jobs, "test", |loaded| {
            let done = &done;
            async move {
                if loaded.id() == poisoned {
                    return Err(ServiceError::Infra("poisoned".to_owned()));
                }
                done.lock().unwrap().push(loaded.id());
                Ok(())
            }
        })
        .await;

        assert_eq!(*done.lock().unwrap(), expected);
        assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "poisoned"));
    }
}
