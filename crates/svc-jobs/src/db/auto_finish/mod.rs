mod candidates;
mod evidence;
mod family;

use async_trait::async_trait;
use bc_jobs::commands::job::auto_finish::AutoFinishFacts;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::requester::Requester;
use bc_jobs::ports::PortError;
use bc_jobs::ports::auto_finish::AutoFinishReader;
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::{load_many, unavailable};
use evidence::{AnswerSource, answers_of, caller_runs_of};
use family::{family_of, runs_of};

pub struct FinishPremiseNow {
    pub restart_jobs: Vec<Uuid>,
    pub pending_wakes: Vec<Uuid>,
}

pub async fn premise_now(
    tx: &mut PgConnection,
    called: Uuid,
) -> Result<FinishPremiseNow, PortError> {
    let family = family_of(tx, called).await?;
    Ok(FinishPremiseNow {
        restart_jobs: family.restart_jobs,
        pending_wakes: family.pending_wakes,
    })
}

#[async_trait]
impl AutoFinishReader for PgStore {
    async fn facts_of(&self, called: JobId) -> Result<Option<AutoFinishFacts>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        let tx = &mut *connection;
        let Some(job) = load_many(tx, &[called.as_uuid()]).await?.pop() else {
            return Ok(None);
        };
        let (Requester::Run(caller_run), Some(call), false) =
            (job.requester(), job.call().copied(), job.is_terminal())
        else {
            return Ok(Some(AutoFinishFacts {
                called: job,
                restart_jobs: Vec::new(),
                restarts_due: Vec::new(),
                answers: Vec::new(),
                caller_runs: Vec::new(),
            }));
        };
        let caller_run = caller_run.as_uuid();
        let family = family_of(tx, called.as_uuid()).await?;
        let mut family_jobs = family.restart_jobs.clone();
        family_jobs.push(called.as_uuid());
        let family_runs = runs_of(tx, &family_jobs).await?;
        let answers = answers_of(
            tx,
            &AnswerSource {
                mode: call.mode,
                caller_run,
                caller_address: call.caller_address,
                family_jobs,
                family_runs,
            },
        )
        .await?;
        let caller_runs = caller_runs_of(tx, caller_run).await?;
        let restart_jobs = load_many(tx, &family.restart_jobs).await?;
        let restarts_due = family
            .pending_wakes
            .into_iter()
            .map(|run| RunId::new(run).map_err(PortError::from))
            .collect::<Result<_, _>>()?;
        Ok(Some(AutoFinishFacts {
            called: job,
            restart_jobs,
            restarts_due,
            answers,
            caller_runs,
        }))
    }

    async fn candidates_after(
        &self,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, PortError> {
        candidates::candidates_after(self.pool(), after, limit).await
    }

    async fn called_by_runs(&self, runs: &[RunId]) -> Result<Vec<JobId>, PortError> {
        candidates::called_by_runs(self.pool(), runs).await
    }

    async fn called_jobs_stemming_to(&self, job: JobId) -> Result<Vec<JobId>, PortError> {
        candidates::called_jobs_stemming_to(self.pool(), job).await
    }
}
