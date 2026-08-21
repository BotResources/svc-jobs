use async_graphql::InputObject;
use uuid::Uuid;

use super::enums::{GqlDeletedFilter, GqlJobStatus};
use crate::db::list::JobFilter;

#[derive(InputObject, Clone, Default)]
#[graphql(name = "JobsJobFilterInput")]
pub struct GqlJobFilterInput {
    pub statuses: Option<Vec<GqlJobStatus>>,
    pub runner_types: Option<Vec<String>>,
    pub producers: Option<Vec<String>>,
    #[graphql(default_with = "Some(GqlDeletedFilter::ExcludeDeleted)")]
    pub deleted: Option<GqlDeletedFilter>,
}

impl GqlJobFilterInput {
    pub fn into_filter(self) -> JobFilter {
        JobFilter {
            statuses: self.statuses.map(|statuses| {
                statuses
                    .into_iter()
                    .map(|status| status.as_db_str().to_owned())
                    .collect()
            }),
            runner_types: self.runner_types,
            producers: self.producers,
            deleted: Some(
                self.deleted
                    .unwrap_or(GqlDeletedFilter::ExcludeDeleted)
                    .as_db_str()
                    .to_owned(),
            ),
        }
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "JobsConnectionWindowInput")]
pub struct GqlConnectionWindowInput {
    #[graphql(default = 50)]
    pub first: i32,
    pub after: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "JobsLogTailWindowInput")]
pub struct GqlLogTailWindowInput {
    #[graphql(default = 200)]
    pub last: i32,
    pub before: Option<String>,
}

#[derive(InputObject)]
#[graphql(name = "JobsCancelJobInput")]
pub struct GqlCancelJobInput {
    pub id: Uuid,
    pub job_id: Uuid,
}

#[derive(InputObject)]
#[graphql(name = "JobsManualRetryJobInput")]
pub struct GqlManualRetryJobInput {
    pub id: Uuid,
    pub job_id: Uuid,
    pub successor_job_id: Uuid,
    pub failed_resolution_id: Uuid,
}

#[derive(InputObject)]
#[graphql(name = "JobsDeleteJobInput")]
pub struct GqlDeleteJobInput {
    pub job_id: Uuid,
}

#[derive(InputObject)]
#[graphql(name = "JobsRunnerTypeInput")]
pub struct GqlRunnerTypeInput {
    pub runner_type: String,
}
