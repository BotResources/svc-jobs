use bc_jobs::domain::priority::UserMaxPriority;
use futures::Stream;
use uuid::Uuid;

use crate::app::priority;
use crate::edge::state::EdgeState;
use crate::edge::types::priority::{
    GqlMyMaxPriorityDelta, GqlMyMaxPrioritySnapshot, GqlMyMaxPriorityStreamMessage,
};
use crate::stream::Fact;

use super::{cursor, next_unbroken_fact};

pub fn my_max_priority_stream(
    state: EdgeState,
    user_id: Uuid,
) -> impl Stream<Item = GqlMyMaxPriorityStreamMessage> + Send {
    let mut facts = state.hub.subscribe();
    async_stream::stream! {
        // The revision the snapshot was read at: a change committed before the
        // read but delivered after it is no later than it, and is dropped.
        let mut sent = match priority::applicable_max_revised(&state.jobs, user_id).await {
            Ok((maximum, revision)) => {
                yield GqlMyMaxPriorityStreamMessage::Snapshot(GqlMyMaxPrioritySnapshot {
                    cursor: cursor(Uuid::now_v7()),
                    max_priority: i32::from(maximum.get()),
                });
                revision
            }
            Err(error) => {
                tracing::warn!(error = %error, "the maximum priority snapshot could not be built");
                return;
            }
        };
        while let Some(fact) = next_unbroken_fact(&mut facts).await {
            let Fact::User {
                event_id,
                user_id: carried,
                revision,
                event,
            } = fact
            else {
                continue;
            };
            if carried.as_uuid() != user_id || revision <= sent {
                continue;
            }
            sent = revision;
            let maximum = priority::applicable(
                &UserMaxPriority::new(carried, event.resulting_maximum()),
                &state.jobs,
            );
            yield GqlMyMaxPriorityStreamMessage::Delta(GqlMyMaxPriorityDelta {
                cursor: cursor(event_id),
                max_priority: i32::from(maximum.get()),
            });
        }
    }
}
