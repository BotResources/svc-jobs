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

use super::rows;

const JOBS_SQL: &str = "SELECT j.id::uuid AS id, rt.type_key AS runner_type, \
     coalesce(p.bc_key, sp.bc_key) AS producer, j.config, \
     j.parent_job_id::uuid AS parent_job_id, j.predecessor_job_id::uuid AS predecessor_job_id, \
     j.triggered_by_id::uuid AS triggered_by_id, tu.display_name AS triggered_by_name, \
     se.external_id::uuid AS source_entity_id, j.max_attempts, j.created_at, \
     parent_rt.type_key AS parent_runner_type \
     FROM jobs j \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN producers p ON p.id = j.producer_id \
     LEFT JOIN source_entities se ON se.id = j.source_entity_id \
     LEFT JOIN producers sp ON sp.id = se.producer_id \
     LEFT JOIN known_users tu ON tu.id = j.triggered_by_id \
     LEFT JOIN jobs parent ON parent.id = j.parent_job_id \
     LEFT JOIN runner_types parent_rt ON parent_rt.id = parent.runner_type_id \
     WHERE j.id = ANY($1)";

const RUNS_SQL: &str = "SELECT id::uuid AS id, job_id::uuid AS job_id, attempt_number, \
     dispatched_at, automatic_retry_schedule_id::uuid AS automatic_retry_schedule_id \
     FROM runs WHERE job_id = ANY($1) ORDER BY job_id, attempt_number";

const STARTS_SQL: &str = "SELECT rs.run_id::uuid AS run_id, rs.started_at, rt.type_key, \
     ri.instance_key FROM run_starts rs \
     JOIN runs r ON r.id = rs.run_id \
     JOIN runner_instances ri ON ri.id = rs.instance_id \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     WHERE r.job_id = ANY($1)";

const TERMINALS_SQL: &str = "SELECT t.run_id::uuid AS run_id, t.kind::text AS kind, t.occurred_at, \
     t.failure_kind::text AS failure_kind, t.reason_code, t.params, t.diagnostic, \
     EXTRACT(EPOCH FROM t.retry_after_hint)::bigint AS retry_after_seconds \
     FROM run_terminals t JOIN runs r ON r.id = t.run_id WHERE r.job_id = ANY($1)";

const SCHEDULES_SQL: &str = "SELECT s.id::uuid AS id, s.failed_run_id::uuid AS failed_run_id, s.due_at \
     FROM run_retry_schedules s JOIN runs r ON r.id = s.failed_run_id WHERE r.job_id = ANY($1)";

const PLANS_SQL: &str = "SELECT d.id::uuid AS id, d.run_id::uuid AS run_id, d.declaration_number, \
     d.declared_at, i.step_index, i.label \
     FROM run_plan_declarations d \
     JOIN runs r ON r.id = d.run_id \
     JOIN run_plan_items i ON i.declaration_id = d.id \
     WHERE r.job_id = ANY($1) AND d.declaration_number = ( \
        SELECT max(x.declaration_number) FROM run_plan_declarations x WHERE x.run_id = d.run_id) \
     ORDER BY d.run_id, i.step_index";

const STEPS_SQL: &str = "SELECT s.run_id::uuid AS run_id, s.step_index, s.label, s.started_at \
     FROM steps s JOIN runs r ON r.id = s.run_id WHERE r.job_id = ANY($1) \
     ORDER BY s.run_id, s.step_index";

const CANCELLATIONS_SQL: &str = "SELECT c.run_id::uuid AS run_id, c.requested_at, c.reason_code, \
     c.requested_by_id::uuid AS requested_by_id, u.display_name, \
     c.originating_job_id::uuid AS originating_job_id \
     FROM run_cancellation_requests c JOIN runs r ON r.id = c.run_id \
     LEFT JOIN known_users u ON u.id = c.requested_by_id WHERE r.job_id = ANY($1)";

const RESOLUTIONS_SQL: &str = "SELECT DISTINCT ON (job_id) job_id::uuid AS job_id, id::uuid AS id, \
     kind::text AS kind, occurred_at, failure_cause::text AS failure_cause, \
     caused_by_run_id::uuid AS caused_by_run_id \
     FROM job_resolutions WHERE job_id = ANY($1) ORDER BY job_id, occurred_at DESC, id DESC";

const DELETIONS_SQL: &str = "SELECT d.job_id::uuid AS job_id, d.deleted_by_id::uuid AS deleted_by_id, \
     u.display_name, d.deleted_at FROM job_deletions d \
     JOIN known_users u ON u.id = d.deleted_by_id WHERE d.job_id = ANY($1)";

const MANUAL_RETRIES_SQL: &str = "SELECT m.id::uuid AS id, \
     m.predecessor_job_id::uuid AS predecessor_job_id, \
     m.failed_resolution_id::uuid AS failed_resolution_id, \
     m.successor_job_id::uuid AS successor_job_id, \
     m.requested_by_id::uuid AS requested_by_id, u.display_name, m.requested_at, \
     (settled.id IS NOT NULL) AS successor_settled \
     FROM manual_retries m \
     JOIN known_users u ON u.id = m.requested_by_id \
     LEFT JOIN LATERAL (SELECT jr.id FROM job_resolutions jr \
        WHERE jr.job_id = m.successor_job_id LIMIT 1) settled ON true \
     WHERE m.predecessor_job_id = ANY($1)";

#[derive(Default)]
struct RunParts {
    starts: HashMap<RunId, bc_jobs::domain::run::parts::RunStart>,
    terminals: HashMap<RunId, bc_jobs::domain::run::parts::RunTerminal>,
    schedules: HashMap<RunId, bc_jobs::domain::run::parts::RetrySchedule>,
    plans: HashMap<RunId, bc_jobs::domain::run::plan::RunPlan>,
    steps: HashMap<RunId, Vec<bc_jobs::domain::run::step::Step>>,
    cancellations: HashMap<RunId, bc_jobs::domain::run::parts::RunCancellationRequest>,
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
    for row in fetch(&mut *executor, JOBS_SQL, ids).await? {
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
            created_at: row.get("created_at"),
            owner,
            runs: runs.remove(&id).unwrap_or_default(),
            resolution: resolutions.remove(&id),
            deletion: deletions.remove(&id),
            manual_retry: manual_retries.remove(&id),
        })?);
    }
    Ok(jobs)
}

async fn load_run_parts(executor: &mut PgConnection, ids: &[Uuid]) -> Result<RunParts, PortError> {
    let mut parts = RunParts::default();
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

pub fn unavailable(error: sqlx::Error) -> PortError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        return PortError::ConcurrentModification;
    }
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_check_violation)
    {
        return PortError::Refused {
            detail: error.to_string(),
        };
    }
    PortError::Unavailable {
        detail: error.to_string(),
    }
}
