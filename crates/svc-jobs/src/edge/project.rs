use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::Result;
use bc_jobs::domain::actions::Affordance as DomainAffordance;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use br_util_graphql::Affordance;
use uuid::Uuid;

use super::error::edge_error;
use super::tree::JobTree;
use super::types::fleet::{GqlRunnerTypeView, view_of};
use super::types::job::{GqlJobSummaryView, GqlJobView};
use crate::db::PgStore;

const UNSTATED_BLOCK: &str = "unstated";

pub fn affordance_list(domain: Vec<DomainAffordance>) -> Vec<Affordance> {
    domain.iter().map(affordance).collect()
}

fn affordance(source: &DomainAffordance) -> Affordance {
    match (source.allowed(), source.reason_code()) {
        (true, _) => Affordance::allow(source.action()),
        (false, None) => Affordance::block(source.action(), UNSTATED_BLOCK),
        (false, Some(reason_code)) => {
            let blocked = Affordance::block(source.action(), reason_code);
            match source.params().and_then(|params| params.as_object()) {
                None => blocked,
                Some(fields) => blocked.with_params(
                    fields
                        .iter()
                        .map(|(key, value)| (key.clone(), rendered(value))),
                ),
            }
        }
    }
}

fn rendered(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

pub fn affordances_of(job: &Job, parent: Option<&Job>) -> Result<Vec<Affordance>> {
    let context = match (job.parent_job_id(), parent) {
        (Some(_), Some(parent)) => ParentContext::Loaded(parent),
        _ => ParentContext::Root,
    };
    let computed = job.affordances(context).map_err(edge_error)?;
    Ok(affordance_list(computed))
}

pub async fn view_of_job(store: &PgStore, job: Job) -> Result<GqlJobView> {
    let parent = parent_of(store, &job).await?;
    let tree = JobTree::rooted_at(store, job.id())
        .await
        .map_err(edge_error)?;
    Ok(GqlJobView {
        job: Arc::new(job),
        parent,
        tree: Arc::new(tree),
    })
}

pub async fn parent_of(store: &PgStore, job: &Job) -> Result<Option<Arc<Job>>> {
    let Some(parent_id) = job.parent_job_id() else {
        return Ok(None);
    };
    Ok(JobReader::load(store, parent_id)
        .await
        .map_err(edge_error)?
        .map(Arc::new))
}

pub async fn summary_views(store: &PgStore, jobs: Vec<Job>) -> Result<Vec<GqlJobSummaryView>> {
    let parents = parents_of(store, &jobs).await?;
    Ok(jobs
        .into_iter()
        .map(|job| GqlJobSummaryView {
            parent: job
                .parent_job_id()
                .and_then(|id| parents.get(&id.as_uuid()).cloned()),
            job: Arc::new(job),
        })
        .collect())
}

pub async fn parents_of(store: &PgStore, jobs: &[Job]) -> Result<HashMap<Uuid, Arc<Job>>> {
    let ids: Vec<Uuid> = jobs
        .iter()
        .filter_map(|job| job.parent_job_id().map(|id| id.as_uuid()))
        .collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let loaded = store.load_batch(&ids).await.map_err(edge_error)?;
    Ok(loaded
        .into_iter()
        .map(|parent| (parent.id().as_uuid(), Arc::new(parent)))
        .collect())
}

pub async fn fleet_views(
    store: &PgStore,
    runner_type: Option<&str>,
) -> Result<Vec<GqlRunnerTypeView>> {
    let watched = runner_type
        .map(RunnerTypeKey::new)
        .transpose()
        .map_err(edge_error)?;
    let live = FleetReader::load_all(store).await.map_err(edge_error)?;
    let jobs = match &watched {
        Some(key) => store
            .active_jobs_of_type(key.as_str())
            .await
            .map_err(edge_error)?,
        None => store.active_jobs().await.map_err(edge_error)?,
    };
    let keys = match &watched {
        Some(key) => vec![key.clone()],
        None => known_keys(store, &live).await?,
    };
    Ok(keys
        .iter()
        .map(|key| {
            let known = live.iter().find(|runner_type| runner_type.key() == key);
            view_of(key, known, &jobs)
        })
        .collect())
}

async fn known_keys(store: &PgStore, live: &[RunnerType]) -> Result<Vec<RunnerTypeKey>> {
    let mut keys: Vec<RunnerTypeKey> = live
        .iter()
        .map(|runner_type| runner_type.key().clone())
        .collect();
    for key in store
        .runner_type_keys_with_jobs()
        .await
        .map_err(edge_error)?
    {
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys.sort();
    Ok(keys)
}
