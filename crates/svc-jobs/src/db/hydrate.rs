use std::collections::HashMap;

use bc_jobs::JobsError;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::{Job, JobState};
use bc_jobs::domain::keys::{ProducerKey, RunnerTypeKey};
use bc_jobs::domain::ownership::JobOwner;
use bc_jobs::domain::run::{Run, RunState};
use bc_jobs::ports::PortError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::hydrate_sql::{
    CANCELLATIONS_SQL, DELETIONS_SQL, MANUAL_RETRIES_SQL, PLANS_SQL, RESERVATIONS_SQL,
    RESOLUTIONS_SQL, RUNS_SQL, SCHEDULES_SQL, STARTS_SQL, STEPS_SQL, TERMINALS_SQL, jobs_sql,
};
use super::rows;

#[derive(Default)]
struct RunParts {
    reservations: HashMap<RunId, bc_jobs::domain::run::parts::RunReservation>,
    starts: HashMap<RunId, bc_jobs::domain::run::parts::RunStart>,
    terminals: HashMap<RunId, bc_jobs::domain::run::parts::RunTerminal>,
    schedules: HashMap<RunId, bc_jobs::domain::run::parts::RetrySchedule>,
    plans: HashMap<RunId, bc_jobs::domain::run::plan::RunPlan>,
    steps: HashMap<RunId, Vec<bc_jobs::domain::run::step::Step>>,
    cancellations: HashMap<RunId, bc_jobs::domain::run::parts::RunCancellationRequest>,
}

pub async fn load_map(
    executor: &mut PgConnection,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Job>, PortError> {
    Ok(load_many(executor, ids)
        .await?
        .into_iter()
        .map(|job| (job.id().as_uuid(), job))
        .collect())
}

pub async fn load_many(executor: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Job>, PortError> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let parts = load_run_parts(&mut *executor, ids).await?;
    let mut runs: HashMap<JobId, Vec<Run>> = HashMap::new();
    for row in fetch(&mut *executor, RUNS_SQL, ids).await? {
        let id = RunId::new(row.get("id"))?;
        let job_id = JobId::new(row.get("job_id"))?;
        let schedule_id = row
            .get::<Option<Uuid>, _>("automatic_retry_schedule_id")
            .map(bc_jobs::domain::ids::RetryScheduleId::new)
            .transpose()?;
        let run = Run::hydrate(RunState {
            id,
            attempt_number: rows::attempt(&row, "attempt_number")?,
            dispatched_at: row.get("dispatched_at"),
            automatic_retry_schedule_id: schedule_id,
            max_duration: rows::max_run_duration(row.get("max_run_duration_seconds"))?,
            reservation: parts.reservations.get(&id).cloned(),
            start: parts.starts.get(&id).cloned(),
            terminal: parts.terminals.get(&id).cloned(),
            plan: parts.plans.get(&id).cloned(),
            steps: parts.steps.get(&id).cloned().unwrap_or_default(),
            retry_schedule: parts.schedules.get(&id).copied(),
            cancellation_request: parts.cancellations.get(&id).cloned(),
        })?;
        runs.entry(job_id).or_default().push(run);
    }

    let mut resolutions = HashMap::new();
    for row in fetch(&mut *executor, RESOLUTIONS_SQL, ids).await? {
        let (job_id, resolution) = rows::resolution(&row)?;
        resolutions.insert(job_id, resolution);
    }
    let mut deletions = HashMap::new();
    for row in fetch(&mut *executor, DELETIONS_SQL, ids).await? {
        let (job_id, deletion) = rows::deletion(&row)?;
        deletions.insert(job_id, deletion);
    }
    let mut manual_retries = HashMap::new();
    for row in fetch(&mut *executor, MANUAL_RETRIES_SQL, ids).await? {
        let (job_id, record) = rows::manual_retry(&row)?;
        manual_retries.insert(job_id, record);
    }

    let mut jobs = Vec::new();
    for row in fetch(&mut *executor, &jobs_sql(), ids).await? {
        let id = JobId::new(row.get("id"))?;
        let producer = ProducerKey::new(row.get::<String, _>("producer"))?;
        let parent_job_id = row
            .get::<Option<Uuid>, _>("parent_job_id")
            .map(JobId::new)
            .transpose()?;
        let owner = match (
            parent_job_id,
            row.get::<Option<String>, _>("parent_runner_type"),
        ) {
            (Some(parent_job_id), Some(runner_type)) => JobOwner::Runner {
                parent_job_id,
                runner_type: RunnerTypeKey::new(runner_type)?,
            },
            (None, _) => JobOwner::Producer(producer.clone()),
            (Some(_), None) => {
                return Err(PortError::from(JobsError::CorruptState {
                    reason_code: "parent_job_not_loaded",
                }));
            }
        };
        jobs.push(Job::hydrate(JobState {
            id,
            runner_type: RunnerTypeKey::new(row.get::<String, _>("runner_type"))?,
            producer,
            config: rows::config(row.get("config"))?,
            parent_job_id,
            predecessor_job_id: row
                .get::<Option<Uuid>, _>("predecessor_job_id")
                .map(JobId::new)
                .transpose()?,
            triggered_by: rows::optional_known_user(
                row.get("triggered_by_id"),
                row.get("triggered_by_name"),
            )?,
            source_entity_id: rows::source_entity(row.get("source_entity_id"))?,
            max_attempts: rows::max_attempts(row.get("max_attempts"))?,
            priority: rows::job_priority(&row)?,
            created_at: row.get("created_at"),
            owner,
            runs: runs.remove(&id).unwrap_or_default(),
            resolution: resolutions.remove(&id),
            deletion: deletions.remove(&id),
            manual_retry: manual_retries.remove(&id),
            initial_job_id: rows::initial_job(id, row.get("root_job_id"))?,
            requester: rows::json_part(row.get("requester"))?
                .unwrap_or(bc_jobs::domain::requester::Requester::Service(None)),
            passport: rows::json_part(row.get("passport"))?,
            payload: rows::json_part(row.get("payload"))?,
            call: rows::json_part(row.get("call"))?,
            restart_of_run_id: row
                .get::<Option<Uuid>, _>("restart_of_run_id")
                .map(RunId::new)
                .transpose()?,
        })?);
    }
    Ok(jobs)
}

async fn load_run_parts(executor: &mut PgConnection, ids: &[Uuid]) -> Result<RunParts, PortError> {
    let mut parts = RunParts::default();
    for row in fetch(&mut *executor, RESERVATIONS_SQL, ids).await? {
        let (run_id, reservation) = rows::reservation(&row)?;
        parts.reservations.insert(run_id, reservation);
    }
    for row in fetch(&mut *executor, STARTS_SQL, ids).await? {
        let (run_id, start) = rows::start(&row)?;
        parts.starts.insert(run_id, start);
    }
    for row in fetch(&mut *executor, TERMINALS_SQL, ids).await? {
        let (run_id, terminal) = rows::terminal(&row)?;
        parts.terminals.insert(run_id, terminal);
    }
    for row in fetch(&mut *executor, SCHEDULES_SQL, ids).await? {
        let (run_id, schedule) = rows::retry_schedule(&row)?;
        parts.schedules.insert(run_id, schedule);
    }
    for row in fetch(&mut *executor, STEPS_SQL, ids).await? {
        let (run_id, step) = rows::step(&row)?;
        parts.steps.entry(run_id).or_default().push(step);
    }
    for row in fetch(&mut *executor, CANCELLATIONS_SQL, ids).await? {
        let (run_id, request) = rows::cancellation_request(&row)?;
        parts.cancellations.insert(run_id, request);
    }
    let mut current: Option<(RunId, Uuid, i32, chrono::DateTime<chrono::Utc>, Vec<_>)> = None;
    for row in fetch(&mut *executor, PLANS_SQL, ids).await? {
        let run_id = RunId::new(row.get("run_id"))?;
        let item = rows::plan_item(&row)?;
        match current.as_mut() {
            Some((open, _, _, _, items)) if *open == run_id => items.push(item),
            _ => {
                if let Some((open, id, number, declared_at, items)) = current.take() {
                    parts
                        .plans
                        .insert(open, rows::plan(id, number, declared_at, items)?);
                }
                current = Some((
                    run_id,
                    row.get("id"),
                    row.get("declaration_number"),
                    row.get("declared_at"),
                    vec![item],
                ));
            }
        }
    }
    if let Some((open, id, number, declared_at, items)) = current.take() {
        parts
            .plans
            .insert(open, rows::plan(id, number, declared_at, items)?);
    }
    Ok(parts)
}

async fn fetch(
    executor: &mut PgConnection,
    sql: &str,
    ids: &[Uuid],
) -> Result<Vec<sqlx::postgres::PgRow>, PortError> {
    sqlx::query(sql)
        .bind(ids)
        .fetch_all(&mut *executor)
        .await
        .map_err(unavailable)
}

const RETRYABLE_SQLSTATES: [&str; 2] = ["40P01", "40001"];

pub fn is_write_race(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database| {
        database.is_unique_violation()
            || database
                .code()
                .is_some_and(|code| RETRYABLE_SQLSTATES.contains(&code.as_ref()))
    })
}

pub fn unavailable(error: sqlx::Error) -> PortError {
    if is_write_race(&error) {
        return PortError::ConcurrentModification;
    }
    PortError::Unavailable {
        detail: error.to_string(),
    }
}
