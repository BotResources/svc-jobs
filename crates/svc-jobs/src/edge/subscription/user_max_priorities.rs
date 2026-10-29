use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::priority::UserSelection;
use futures::Stream;
use uuid::Uuid;

use crate::app::user_max_priorities;
use crate::app::user_max_priorities_live::WatchedMaxima;
use crate::edge::state::EdgeState;
use crate::edge::types::priority::{
    GqlUserMaxPrioritiesSnapshot, GqlUserMaxPrioritiesStreamMessage, GqlUserMaxPriority,
    GqlUserMaxPriorityDelta,
};
use crate::stream::Fact;

use super::{cursor, next_unbroken_fact};

pub fn user_max_priorities_stream(
    state: EdgeState,
    principal: PrincipalFacts,
    users: UserSelection,
) -> impl Stream<Item = GqlUserMaxPrioritiesStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        let snapshot = match user_max_priorities::snapshot(&state.jobs, &principal, &users).await {
            Ok(views) => views,
            Err(error) => {
                tracing::warn!(error = %error, "the users' maxima snapshot could not be built");
                return;
            }
        };
        let mut watched = WatchedMaxima::opened_on(principal, users, &snapshot);
        yield GqlUserMaxPrioritiesStreamMessage::Snapshot(GqlUserMaxPrioritiesSnapshot {
            cursor: cursor(Uuid::now_v7()),
            user_max_priorities: snapshot.into_iter().map(GqlUserMaxPriority::from).collect(),
        });
        while let Some(fact) = next_unbroken_fact(&mut facts).await {
            let Fact::User { event_id, event, .. } = fact else {
                continue;
            };
            if let Some(view) = watched.upsert_for(&state.jobs, &event) {
                yield GqlUserMaxPrioritiesStreamMessage::Delta(GqlUserMaxPriorityDelta {
                    cursor: cursor(event_id),
                    user_max_priority: GqlUserMaxPriority::from(view),
                });
            }
        }
    }
}
