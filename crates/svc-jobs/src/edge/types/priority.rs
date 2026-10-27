use async_graphql::{SimpleObject, Union};

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
