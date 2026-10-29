//! The live side of the administrator's read of users' maximum priorities: a
//! snapshot equal to the read, then the complete new view of a watched user
//! each time their maximum changes.
//!
//! What was last sent is delivery state, kept per subscription and never
//! stored: a subscriber that loses it reconnects onto a fresh snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::KnownUserId;
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::domain::priority::{
    MaxPriorityRevision, UserMaxPriority, UserMaxPriorityView, UserSelection,
};
use bc_jobs::ports::user::{RevisedUserMaxPriority, UserMaxPriorityReader};
use futures::Stream;
use tokio::sync::broadcast::Receiver;
use uuid::Uuid;

use super::Jobs;
use crate::stream::{Fact, next_unbroken_fact};

/// One change of a user's maximum, as the durable fact stream delivers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxPriorityChange {
    pub user_id: KnownUserId,
    pub revision: MaxPriorityRevision,
    pub maximum: UserMaxPriority,
}

pub enum MaximaMessage {
    /// The current views of every watched user, in the selection's order.
    Snapshot(Vec<UserMaxPriorityView>),
    /// The complete new view of one watched user, replacing the one sent
    /// before; `event_id` names the change that produced it.
    Upsert {
        event_id: Uuid,
        view: UserMaxPriorityView,
    },
}

/// The views a subscriber was last sent, and the revision each was read at.
pub struct WatchedMaxima {
    principal: PrincipalFacts,
    users: UserSelection,
    sent: HashMap<KnownUserId, (MaxPriorityRevision, UserMaxPriorityView)>,
}

impl WatchedMaxima {
    /// Opens the window on what the store held for the watched users, and
    /// returns the snapshot to send.
    pub fn open(
        principal: PrincipalFacts,
        users: UserSelection,
        read: &[RevisedUserMaxPriority],
        limits: &ServiceLimits,
    ) -> (Self, Vec<UserMaxPriorityView>) {
        let views: Vec<UserMaxPriorityView> = read
            .iter()
            .map(|found| found.maximum.view(&principal, limits))
            .collect();
        let sent = read
            .iter()
            .zip(&views)
            .map(|(found, view)| (view.user_id, (found.revision, view.clone())))
            .collect();
        (
            Self {
                principal,
                users,
                sent,
            },
            views,
        )
    }

    /// The view to push once `change` applies: the complete new view of a
    /// watched user — or nothing when the user is not watched, when the change
    /// is no later than what was already sent (a late or repeated delivery), or
    /// when it leaves the view as it was.
    pub fn upsert_for(
        &mut self,
        change: &MaxPriorityChange,
        limits: &ServiceLimits,
    ) -> Option<UserMaxPriorityView> {
        if !self.users.contains(change.user_id) {
            return None;
        }
        if let Some((sent_revision, _)) = self.sent.get(&change.user_id)
            && change.revision <= *sent_revision
        {
            return None;
        }
        let view = change.maximum.view(&self.principal, limits);
        let previous = self
            .sent
            .insert(change.user_id, (change.revision, view.clone()));
        match previous {
            Some((_, sent)) if sent == view => None,
            _ => Some(view),
        }
    }
}

/// The live read of the selected users' maxima: the snapshot, then one
/// upsert per change that alters a watched view. `facts` must be subscribed
/// before this is called, so no change committed after the snapshot read is
/// missed; one committed before it is recognised by its revision and dropped.
pub fn watch(
    jobs: Arc<Jobs>,
    mut facts: Receiver<Fact>,
    principal: PrincipalFacts,
    users: UserSelection,
) -> impl Stream<Item = MaximaMessage> + Send {
    async_stream::stream! {
        let read = match UserMaxPriorityReader::load_selected(&jobs.store, &users).await {
            Ok(read) => read,
            Err(error) => {
                tracing::warn!(error = %error, "the users' maxima snapshot could not be built");
                return;
            }
        };
        let (mut watched, snapshot) = WatchedMaxima::open(principal, users, &read, &jobs.limits);
        yield MaximaMessage::Snapshot(snapshot);
        while let Some(fact) = next_unbroken_fact(&mut facts).await {
            let Fact::User { event_id, user_id, revision, event } = fact else {
                continue;
            };
            let change = MaxPriorityChange {
                user_id,
                revision,
                maximum: UserMaxPriority::new(user_id, event.resulting_maximum()),
            };
            if let Some(view) = watched.upsert_for(&change, &jobs.limits) {
                yield MaximaMessage::Upsert { event_id, view };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bc_jobs::domain::access::{PlatformRole, PrincipalKind};
    use bc_jobs::domain::actions::user::CLEAR_MAX_PRIORITY;
    use bc_jobs::domain::priority::Priority;

    fn super_user() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, PlatformRole::SuperUser)
    }

    fn user() -> KnownUserId {
        KnownUserId::new(Uuid::now_v7()).unwrap()
    }

    fn revision(value: i64) -> MaxPriorityRevision {
        MaxPriorityRevision::new(value).unwrap()
    }

    fn change(user_id: KnownUserId, at: i64, maximum: Option<i64>) -> MaxPriorityChange {
        MaxPriorityChange {
            user_id,
            revision: revision(at),
            maximum: UserMaxPriority::new(
                user_id,
                maximum.map(|level| Priority::new(level).unwrap()),
            ),
        }
    }

    /// A window watching `a`, opened on `a` capped at 5 at revision 2.
    fn watching(a: KnownUserId) -> WatchedMaxima {
        let read = [RevisedUserMaxPriority {
            maximum: UserMaxPriority::new(a, Some(Priority::new(5).unwrap())),
            revision: revision(2),
        }];
        let (watched, snapshot) = WatchedMaxima::open(
            super_user(),
            UserSelection::one(a),
            &read,
            &ServiceLimits::default(),
        );
        assert_eq!(snapshot.len(), 1);
        watched
    }

    #[test]
    fn a_change_of_an_unwatched_user_pushes_nothing() {
        // Given: a window watching A
        let a = user();
        let mut watched = watching(a);
        // When/Then: a change of another user reaches nobody here
        assert_eq!(
            watched.upsert_for(&change(user(), 9, Some(1)), &ServiceLimits::default()),
            None
        );
    }

    #[test]
    fn a_late_or_repeated_change_pushes_nothing() {
        // Given: a window opened on A at revision 2
        let a = user();
        let mut watched = watching(a);
        let limits = ServiceLimits::default();
        // When: the change that produced revision 1 arrives after the snapshot, then revision 3
        // arrives twice
        let late = watched.upsert_for(&change(a, 1, Some(4)), &limits);
        let newer = watched.upsert_for(&change(a, 3, Some(2)), &limits);
        let repeated = watched.upsert_for(&change(a, 3, Some(2)), &limits);
        // Then: only the newer change is pushed, once
        assert_eq!(
            late, None,
            "a change older than the snapshot never rolls it back"
        );
        assert_eq!(newer.map(|view| view.max_priority.get()), Some(2));
        assert_eq!(repeated, None);
    }

    #[test]
    fn a_later_change_that_leaves_the_view_as_it_was_pushes_nothing() {
        // Given: a window opened on A capped at 5
        let a = user();
        let mut watched = watching(a);
        // When/Then: a later change to the same maximum pushes nothing
        assert_eq!(
            watched.upsert_for(&change(a, 3, Some(5)), &ServiceLimits::default()),
            None
        );
    }

    #[test]
    fn setting_then_clearing_pushes_each_complete_view() {
        // Given: a window opened on A capped at 5
        let a = user();
        let mut watched = watching(a);
        let limits = ServiceLimits::default();
        // When: A is set to 4, then cleared
        let set = watched.upsert_for(&change(a, 3, Some(4)), &limits).unwrap();
        let cleared = watched.upsert_for(&change(a, 4, None), &limits).unwrap();
        // Then: each push is the whole view — level, origin and affordances
        assert_eq!((set.max_priority.get(), set.is_default), (4, false));
        assert_eq!((cleared.max_priority.get(), cleared.is_default), (3, true));
        let clear = cleared
            .affordances
            .iter()
            .find(|affordance| affordance.action() == CLEAR_MAX_PRIORITY)
            .unwrap();
        assert_eq!(clear.reason_code(), Some("no_max_priority_set"));
    }
}
