use async_graphql::SimpleObject;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::log::RunLogLine;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::GqlRunLogLevel;

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsRunLog")]
pub struct GqlRunLog {
    pub id: Uuid,
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub step_index: Option<i32>,
    pub level: GqlRunLogLevel,
    pub message: String,
    pub logged_at: DateTime<Utc>,
}

pub fn log_of(job_id: JobId, line: &RunLogLine) -> GqlRunLog {
    GqlRunLog {
        id: line.id().as_uuid(),
        job_id: job_id.as_uuid(),
        run_id: line.run_id().as_uuid(),
        step_index: line
            .step_index()
            .map(|index| i32::try_from(index.get()).unwrap_or(i32::MAX)),
        level: line.level().into(),
        message: line.message().as_str().to_owned(),
        logged_at: line.logged_at(),
    }
}
