use async_graphql::SimpleObject;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::view::FleetView;
use br_util_graphql::Affordance;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::{GqlFleetEventKind, GqlRunnerTypeLifecycle};
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
    pub lifecycle: GqlRunnerTypeLifecycle,
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

pub fn view_of(view: crate::app::fleet::RunnerTypeView) -> GqlRunnerTypeView {
    let known = &view.runner_type;
    GqlRunnerTypeView {
        runner_type: runner_type_of(known, &view.fleet),
        affordances: affordance_list(view.affordances),
    }
}

fn runner_type_of(known: &RunnerType, projection: &FleetView) -> GqlRunnerType {
    GqlRunnerType {
        type_key: known.key().as_str().to_owned(),
        lifecycle: known.lifecycle().into(),
        instances: projection
            .instances()
            .iter()
            .map(|load| {
                let announced = known.instance(load.instance().instance_key());
                GqlRunnerInstance {
                    instance_key: load.instance().instance_key().as_str().to_owned(),
                    version: announced
                        .map(|live| live.version().as_str().to_owned())
                        .unwrap_or_default(),
                    reported_status: announced
                        .map(|live| live.reported_status().as_str().to_owned())
                        .unwrap_or_default(),
                    capacity: load.capacity().get_i32(),
                    is_busy: load.is_busy(),
                    current_run_ids: load
                        .current_run_ids()
                        .iter()
                        .map(|run| run.as_uuid())
                        .collect(),
                }
            })
            .collect(),
        is_available: known.is_available(),
        total_capacity: count(projection.total_capacity()),
        busy_instance_count: count(projection.busy_instance_count()),
        idle_instance_count: count(projection.idle_instance_count()),
        waiting_job_count: count(projection.waiting_job_count()),
        executing_job_count: count(projection.executing_job_count()),
    }
}
