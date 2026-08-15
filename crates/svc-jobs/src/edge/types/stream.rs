use async_graphql::{SimpleObject, Union};
use br_util_graphql::{Affordance, Connection, PageInfo};
use uuid::Uuid;

use super::event::GqlJobEvent;
use super::fleet::{GqlFleetEvent, GqlRunnerType, GqlRunnerTypeView};
use super::job::{GqlJobDetail, GqlJobSummaryView};
use super::log::GqlRunLog;

#[derive(SimpleObject)]
#[graphql(name = "JobsJobsSnapshot")]
pub struct GqlJobsSnapshot {
    pub cursor: String,
    pub jobs: Connection<GqlJobSummaryView>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobsDelta")]
pub struct GqlJobsDelta {
    pub cursor: String,
    pub event: GqlJobEvent,
    pub upserted: Vec<GqlJobSummaryView>,
    pub removed_ids: Vec<Uuid>,
    pub page_info: PageInfo,
}

#[derive(Union)]
#[graphql(name = "JobsJobsStreamMessage")]
pub enum GqlJobsStreamMessage {
    Snapshot(GqlJobsSnapshot),
    Delta(GqlJobsDelta),
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobSnapshot")]
pub struct GqlJobSnapshot {
    pub cursor: String,
    pub job: GqlJobDetail,
    pub affordances: Vec<Affordance>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobDelta")]
pub struct GqlJobDelta {
    pub cursor: String,
    pub event: GqlJobEvent,
    pub job: GqlJobDetail,
    pub affordances: Vec<Affordance>,
}

#[derive(Union)]
#[graphql(name = "JobsJobStreamMessage")]
pub enum GqlJobStreamMessage {
    Snapshot(GqlJobSnapshot),
    Delta(GqlJobDelta),
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobLogSnapshot")]
pub struct GqlJobLogSnapshot {
    pub cursor: String,
    pub logs: Connection<GqlRunLog>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunLogAppended")]
pub struct GqlRunLogAppended {
    pub cursor: String,
    pub log: GqlRunLog,
}

#[derive(Union)]
#[graphql(name = "JobsJobLogStreamMessage")]
pub enum GqlJobLogStreamMessage {
    Snapshot(GqlJobLogSnapshot),
    Appended(GqlRunLogAppended),
}

#[derive(SimpleObject)]
#[graphql(name = "JobsFleetSnapshot")]
pub struct GqlFleetSnapshot {
    pub cursor: String,
    pub runner_types: Vec<GqlRunnerTypeView>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunnerTypeDelta")]
pub struct GqlRunnerTypeDelta {
    pub cursor: String,
    pub event: GqlFleetEvent,
    pub runner_type: GqlRunnerType,
    pub affordances: Vec<Affordance>,
}

#[derive(Union)]
#[graphql(name = "JobsFleetStreamMessage")]
pub enum GqlFleetStreamMessage {
    Snapshot(GqlFleetSnapshot),
    Delta(GqlRunnerTypeDelta),
}
