use async_graphql::{SimpleObject, Union};
use bc_jobs::domain::priority::UserMaxPriorityView;
use br_util_graphql::Affordance;
use uuid::Uuid;

use crate::edge::project::affordance_list;

/// The maximum priority that applies to the caller: their own, or the service
/// default.
#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsMyMaxPriority")]
pub struct GqlMyMaxPriority {
    pub max_priority: i32,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsMyMaxPrioritySnapshot")]
pub struct GqlMyMaxPrioritySnapshot {
    pub cursor: String,
    pub max_priority: i32,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsMyMaxPriorityDelta")]
pub struct GqlMyMaxPriorityDelta {
    pub cursor: String,
    pub max_priority: i32,
}

#[derive(Union)]
#[graphql(name = "JobsMyMaxPriorityStreamMessage")]
pub enum GqlMyMaxPriorityStreamMessage {
    Snapshot(GqlMyMaxPrioritySnapshot),
    Delta(GqlMyMaxPriorityDelta),
}

/// A user's maximum priority as an administrator reads it: the level that
/// applies, whether it is the service default rather than the user's own, and
/// the caller's affordances of setting and clearing it.
#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsUserMaxPriority")]
pub struct GqlUserMaxPriority {
    pub user_id: Uuid,
    pub max_priority: i32,
    pub is_default: bool,
    pub affordances: Vec<Affordance>,
}

impl From<UserMaxPriorityView> for GqlUserMaxPriority {
    fn from(view: UserMaxPriorityView) -> Self {
        Self {
            user_id: view.user_id.as_uuid(),
            max_priority: i32::from(view.max_priority.get()),
            is_default: view.is_default,
            affordances: affordance_list(view.affordances),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "JobsUserMaxPrioritiesSnapshot")]
pub struct GqlUserMaxPrioritiesSnapshot {
    pub cursor: String,
    pub user_max_priorities: Vec<GqlUserMaxPriority>,
}

/// The complete new view of one watched user, replacing the one sent before.
#[derive(SimpleObject)]
#[graphql(name = "JobsUserMaxPriorityDelta")]
pub struct GqlUserMaxPriorityDelta {
    pub cursor: String,
    pub user_max_priority: GqlUserMaxPriority,
}

#[derive(Union)]
#[graphql(name = "JobsUserMaxPrioritiesStreamMessage")]
pub enum GqlUserMaxPrioritiesStreamMessage {
    Snapshot(GqlUserMaxPrioritiesSnapshot),
    Delta(GqlUserMaxPriorityDelta),
}
