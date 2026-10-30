use bc_jobs::domain::directory::registration::RunStanding;
use bc_jobs::domain::ids::{AgentId, JobId, RunId};
use bc_jobs::domain::runner::work::{RunRefusal, WorkFacts, WorkNamed, WorkRefusal, decide_work};
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::directory::{lock_tree, standing};
use super::hydrate::unavailable;
use super::saved_states::saved_state_run_in;

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
) -> Result<Result<(RunId, JobId), RunRefusal>, PortError> {
    let Ok(run_id) = RunId::new(run_id) else {
        return Ok(Err(RunRefusal::Malformed));
    };
    let current = standing(tx, run_id, runner_type, instance_key).await?;
    match current {
        RunStanding::Started { initial_job_id } => Ok(Ok((run_id, initial_job_id))),
        RunStanding::Unknown => Ok(Err(RunRefusal::RunUnknown)),
        RunStanding::NotStarted => Ok(Err(RunRefusal::RunNotStarted)),
        RunStanding::Ended => Ok(Err(RunRefusal::RunEnded)),
        RunStanding::Removed => Ok(Err(RunRefusal::RunRemoved)),
        RunStanding::StartedOnAnotherInstance => Ok(Err(RunRefusal::InstanceMismatch)),
    }
}

pub async fn addresses_of_run(
    tx: &mut PgConnection,
    run_id: RunId,
) -> Result<Vec<AgentId>, PortError> {
    let mut found: Vec<Uuid> = sqlx::query_scalar(
        "SELECT agent_id FROM directory_agents WHERE run_id = $1 AND has_inbox ORDER BY agent_id",
    )
    .bind(run_id.as_uuid())
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    if let Some(state_run) = saved_state_run_in(tx, run_id).await? {
        let saved: Vec<Uuid> = sqlx::query_scalar(
            "SELECT agent_id FROM saved_state_agents WHERE run_id = $1 AND has_inbox",
        )
        .bind(state_run.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        found.extend(saved);
    }
    found.sort_unstable();
    found.dedup();
    Ok(found.into_iter().map(AgentId::new).collect())
}
