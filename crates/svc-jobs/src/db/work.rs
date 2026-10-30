use bc_jobs::domain::directory::registration::RunStanding;
use bc_jobs::domain::ids::{AgentId, JobId, RunId};
use bc_jobs::domain::runner::work::{WorkFacts, WorkRefusal, decide_work};
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::directory::{lock_tree, standing};
use super::hydrate::unavailable;

#[derive(Debug, Clone, Copy)]
pub struct WorkNamed<'a> {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub instance_key: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkContext {
    pub run_id: RunId,
    pub job_id: JobId,
    pub initial_job_id: JobId,
}

pub async fn require_work(
    tx: &mut PgConnection,
    named: WorkNamed<'_>,
    lock_the_tree: bool,
) -> Result<Result<WorkContext, WorkRefusal>, PortError> {
    let (Ok(job_id), Ok(run_id)) = (JobId::new(named.job_id), RunId::new(named.run_id)) else {
        return Ok(Err(WorkRefusal::Malformed));
    };
    let mut current = standing(tx, run_id, None, named.instance_key).await?;
    if lock_the_tree && let RunStanding::Started { initial_job_id } = current {
        lock_tree(tx, initial_job_id).await?;
        current = standing(tx, run_id, None, named.instance_key).await?;
    }
    let job_of_run: Option<Uuid> =
        sqlx::query_scalar("SELECT job_id::uuid FROM runs WHERE id = $1")
            .bind(run_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
    let facts = WorkFacts {
        standing: current,
        job_of_run: job_of_run.map(JobId::new).transpose()?,
    };
    Ok(
        decide_work(job_id, facts).map(|initial_job_id| WorkContext {
            run_id,
            job_id,
            initial_job_id,
        }),
    )
}

pub async fn require_run(
    tx: &mut PgConnection,
    run_id: Uuid,
    instance_key: &str,
    runner_type: Option<&str>,
) -> Result<Result<(RunId, JobId), WorkRefusal>, PortError> {
    let Ok(run_id) = RunId::new(run_id) else {
        return Ok(Err(WorkRefusal::Malformed));
    };
    let current = standing(tx, run_id, runner_type, instance_key).await?;
    match current {
        RunStanding::Started { initial_job_id } => Ok(Ok((run_id, initial_job_id))),
        RunStanding::Unknown => Ok(Err(WorkRefusal::RunUnknown)),
        RunStanding::NotStarted => Ok(Err(WorkRefusal::RunNotStarted)),
        RunStanding::Ended => Ok(Err(WorkRefusal::RunEnded)),
        RunStanding::Removed => Ok(Err(WorkRefusal::RunRemoved)),
        RunStanding::StartedOnAnotherInstance => Ok(Err(WorkRefusal::InstanceMismatch)),
    }
}

pub async fn addresses_of_run(
    tx: &mut PgConnection,
    run_id: RunId,
) -> Result<Vec<AgentId>, PortError> {
    let found: Vec<Uuid> = sqlx::query_scalar(
        "SELECT agent_id FROM directory_agents WHERE run_id = $1 AND has_inbox ORDER BY agent_id",
    )
    .bind(run_id.as_uuid())
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(found.into_iter().map(AgentId::new).collect())
}
