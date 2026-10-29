//! The live side of the administrator's read of users' maximum priorities:
//! which watched view a change of maximum replaces.
//!
//! The views last sent are delivery state, kept per subscription and never
//! stored: a subscriber that loses them reconnects onto a fresh snapshot.

use std::collections::HashMap;

use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::KnownUserId;
use bc_jobs::domain::priority::{UserMaxPriority, UserMaxPriorityView, UserSelection};
use bc_jobs::event::user::UserEvent;

use super::Jobs;

pub struct WatchedMaxima {
    principal: PrincipalFacts,
    users: UserSelection,
    sent: HashMap<KnownUserId, UserMaxPriorityView>,
}

impl WatchedMaxima {
    /// The window a subscriber watches, opened on the snapshot it was sent.
    pub fn opened_on(
        principal: PrincipalFacts,
        users: UserSelection,
        snapshot: &[UserMaxPriorityView],
    ) -> Self {
        Self {
            principal,
            users,
            sent: snapshot
                .iter()
                .map(|view| (view.user_id, view.clone()))
                .collect(),
        }
    }

    /// The view to push once `change` applies: the complete new view of a
    /// watched user, or nothing when the user is not watched or their view is
    /// the one already sent — so a repeated change is harmless.
    pub fn upsert_for(&mut self, jobs: &Jobs, change: &UserEvent) -> Option<UserMaxPriorityView> {
        let user_id = change.user_id();
        if !self.users.contains(user_id) {
            return None;
        }
        // A change of maximum carries the user's whole maximum: the view is
        // projected from it, through the snapshot's own projection, with no
        // re-read.
        let view = UserMaxPriority::new(user_id, change.resulting_maximum())
            .view(&self.principal, &jobs.limits);
        if self.sent.get(&view.user_id) == Some(&view) {
            return None;
        }
        self.sent.insert(view.user_id, view.clone());
        Some(view)
    }
}
