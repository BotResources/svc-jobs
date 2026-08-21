use std::collections::HashMap;

use bc_jobs::domain::actions::fleet::{RetirementWindow, RunnerTypeDecisionFacts};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::{DecidableRunnerType, FleetProjectionSource};
use uuid::Uuid;

use super::{decision, load};
use crate::db::PgStore;
use crate::db::hydrate::{self, unavailable};

pub async fn projection_source(
    store: &PgStore,
    key: Option<&RunnerTypeKey>,
    window: RetirementWindow,
) -> Result<FleetProjectionSource, PortError> {
    let mut tx = store.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let runner_types = load::load_types(&mut tx, key, corruption_policy(key)).await?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT j.id::uuid FROM jobs j JOIN runner_types rt ON rt.id = j.runner_type_id \
         WHERE NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
           AND ($1::text IS NULL OR rt.type_key = $1)",
    )
    .bind(key.map(RunnerTypeKey::as_str))
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    let active_jobs = hydrate::load_many(&mut tx, &ids).await?;
    let keys: Vec<RunnerTypeKey> = runner_types
        .iter()
        .map(|runner_type| runner_type.key().clone())
        .collect();
    let mut terminals = decision::terminals_for(&mut tx, &keys).await?;
    tx.commit().await.map_err(unavailable)?;
    let non_terminal_job_counts = non_terminal_job_counts(&active_jobs);
    Ok(FleetProjectionSource {
        runner_types: runner_types
            .into_iter()
            .map(|runner_type| {
                let latest_terminal_run_at = terminals.remove(runner_type.key()).ok_or(
                    bc_jobs::JobsError::CorruptState {
                        reason_code: "runner_type_without_decision_facts",
                    },
                )?;
                Ok(DecidableRunnerType {
                    decision_facts: RunnerTypeDecisionFacts {
                        non_terminal_job_count: non_terminal_job_counts
                            .get(runner_type.key())
                            .copied()
                            .unwrap_or(0),
                        latest_terminal_run_at,
                        window,
                    },
                    runner_type,
                })
            })
            .collect::<Result<_, PortError>>()?,
        active_jobs,
    })
}

fn corruption_policy(key: Option<&RunnerTypeKey>) -> load::CorruptionPolicy {
    match key {
        Some(_) => load::CorruptionPolicy::FailTheLoad,
        None => load::CorruptionPolicy::IsolateTheRunnerType,
    }
}

fn non_terminal_job_counts(active_jobs: &[Job]) -> HashMap<RunnerTypeKey, u32> {
    let mut counts: HashMap<RunnerTypeKey, u32> = HashMap::new();
    for job in active_jobs {
        *counts.entry(job.runner_type().clone()).or_default() += 1;
    }
    counts
}
