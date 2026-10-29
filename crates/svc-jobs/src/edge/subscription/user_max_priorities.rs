use std::sync::Arc;

use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::priority::UserSelection;
use futures::{Stream, StreamExt};
use uuid::Uuid;

use crate::app::Jobs;
use crate::app::user_max_priorities_live::{MaximaMessage, watch};
use crate::edge::types::priority::{
    GqlUserMaxPrioritiesSnapshot, GqlUserMaxPrioritiesStreamMessage, GqlUserMaxPriority,
    GqlUserMaxPriorityDelta,
};
use crate::stream::Hub;

use super::cursor;

/// The live read mapped to its transport types. The hub is subscribed here,
/// before the snapshot is read.
pub fn user_max_priorities_stream(
    jobs: Arc<Jobs>,
    hub: &Hub,
    principal: PrincipalFacts,
    users: UserSelection,
) -> impl Stream<Item = GqlUserMaxPrioritiesStreamMessage> + Send {
    watch(jobs, hub.subscribe(), principal, users).map(|message| match message {
        MaximaMessage::Snapshot(views) => {
            GqlUserMaxPrioritiesStreamMessage::Snapshot(GqlUserMaxPrioritiesSnapshot {
                cursor: cursor(Uuid::now_v7()),
                user_max_priorities: views.into_iter().map(GqlUserMaxPriority::from).collect(),
            })
        }
        MaximaMessage::Upsert { event_id, view } => {
            GqlUserMaxPrioritiesStreamMessage::Delta(GqlUserMaxPriorityDelta {
                cursor: cursor(event_id),
                user_max_priority: GqlUserMaxPriority::from(view),
            })
        }
    })
}
