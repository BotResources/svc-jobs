use async_graphql::SimpleObject;
use bc_jobs::domain::actions::fleet::unregistered_affordances;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::view::{FleetView, fleet_view, unregistered_fleet_view};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use br_util_graphql::Affordance;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::GqlFleetEventKind;
use crate::edge::project::affordance_list;

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsRunnerInstance")]
pub struct GqlRunnerInstance {
    pub instance_key: String,
    pub version: String,
    pub reported_status: String,
    pub capacity: i32,
    pub is_busy: bool,
    pub current_run_ids: Vec<Uuid>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsRunnerType")]
pub struct GqlRunnerType {
    pub type_key: String,
    pub instances: Vec<GqlRunnerInstance>,
    pub is_available: bool,
    pub total_capacity: i32,
    pub busy_instance_count: i32,
    pub idle_instance_count: i32,
    pub waiting_job_count: i32,
    pub executing_job_count: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsRunnerTypeView")]
pub struct GqlRunnerTypeView {
    pub runner_type: GqlRunnerType,
    pub affordances: Vec<Affordance>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsFleetEvent")]
pub struct GqlFleetEvent {
    pub id: Uuid,
    pub kind: GqlFleetEventKind,
    pub occurred_at: DateTime<Utc>,
    pub runner_type: String,
    pub instance_key: Option<String>,
    pub job_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
}

fn count(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

pub fn view_of(key: &RunnerTypeKey, known: Option<&RunnerType>, jobs: &[Job]) -> GqlRunnerTypeView {
    let projection = match known {
        Some(runner_type) => fleet_view(runner_type, jobs),
        None => unregistered_fleet_view(key, jobs),
    };
    GqlRunnerTypeView {
        runner_type: runner_type_of(key, known, &projection),
        affordances: affordance_list(match known {
            Some(runner_type) => runner_type.affordances(),
            None => unregistered_affordances(key),
        }),
    }
}

fn runner_type_of(
    key: &RunnerTypeKey,
    known: Option<&RunnerType>,
    projection: &FleetView,
) -> GqlRunnerType {
    GqlRunnerType {
        type_key: key.as_str().to_owned(),
        instances: projection
            .instances()
            .iter()
            .map(|load| {
                let announced = known
                    .and_then(|runner_type| runner_type.instance(load.instance().instance_key()));
                GqlRunnerInstance {
                    instance_key: load.instance().instance_key().as_str().to_owned(),
                    version: announced
                        .map(|live| live.version().as_str().to_owned())
                        .unwrap_or_default(),
                    reported_status: announced
                        .map(|live| live.reported_status().as_str().to_owned())
                        .unwrap_or_default(),
                    capacity: count(load.capacity().get()),
                    is_busy: load.is_busy(),
                    current_run_ids: load
                        .current_run_ids()
                        .iter()
                        .map(|run| run.as_uuid())
                        .collect(),
                }
            })
            .collect(),
        is_available: known.is_some_and(RunnerType::is_available),
        total_capacity: count(projection.total_capacity()),
        busy_instance_count: count(projection.busy_instance_count()),
        idle_instance_count: count(projection.idle_instance_count()),
        waiting_job_count: count(projection.waiting_job_count()),
        executing_job_count: count(projection.executing_job_count()),
    }
}
